# Fetchgraph Tracer: observed-first replay cases, bundles и реплей

> Этот документ описывает актуальный формат трейса и реплея:
> - `log.py` — контракт логгера событий и хелпер `log_replay_case`
> - `runtime.py` — `ReplayContext`, `REPLAY_HANDLERS`, `run_case`, `load_case_bundle`
> - `handlers/*` — обработчики (регистрация в `REPLAY_HANDLERS`)
> - `export.py` — экспорт `replay_case` → case bundle (`*.case.json`)

---

## 1) Зачем это нужно

Трейсер решает две задачи:

1) **Observed-first логирование**
   В рантайме пишется **input + observed outcome** (успех) **или** `observed_error` (ошибка) + зависимости для реплея. `expected` не логируется.

2) **Реплей и регрессии**
   Из `events.jsonl` экспортируются **case bundles** (root case + extras/resources + source). Реплей работает без LLM и внешних сервисов.

---

## 2) Ключевые понятия

### Event stream (JSONL)
События пишутся как JSONL: одна строка = одно событие.

Пример общего вида:
```json
{"timestamp":"2026-01-25T00:00:00Z","run_id":"abcd1234","type":"...","...": "..."}
```

### Replay case (v2)
Событие `type="replay_case"`, `v=2`:
- `id`: идентификатор обработчика
- `meta`: опциональные метаданные (например `spec_idx`, `provider`)
- `input`: вход для реплея
- `input.provider_info_snapshot`: минимальный snapshot провайдера (например `selectors_schema`), чтобы реплей был детерминированным без extras
- **ровно одно** из `observed` или `observed_error`
- `requires`: опциональный список зависимостей `[{"kind":"extra"|"resource","id":"..."}]`

Пример:
```json
{
  "type": "replay_case",
  "v": 2,
  "id": "plan_normalize.spec_v1",
  "meta": {"spec_idx": 0, "provider": "sql"},
  "input": {"spec": {...}, "options": {...}, "provider_info_snapshot": {"name": "sql", "selectors_schema": {...}}},
  "observed": {"out_spec": {...}},
  "requires": [{"kind": "extra", "id": "planner_input_v1"}]
}
```

### Extras / Resources
- **Extras**: события с `type="planner_input"`, ключуются по `id`.
- **Resources**: события с `type="replay_resource"`, ключуются по `id`.
  - Файлы указываются в `data_ref.file` (относительный путь внутри `run_dir`).

---

## 3) Контракт логгера и хелпер

### `EventLoggerLike`
```py
class EventLoggerLike(Protocol):
    def emit(self, event: dict) -> None: ...
```

### `log_replay_case`
Хелпер формирует событие v2 и валидирует:
- `id` не пустой
- `input` — dict
- XOR `observed` / `observed_error`
- `requires` — список `{kind,id}`

Пример:
```py
from fetchgraph.tracer import log_replay_case

log_replay_case(
    logger=event_log,
    id="plan_normalize.spec_v1",
    meta={"spec_idx": i, "provider": spec.provider},
    input={
        "spec": spec.model_dump(),
        "options": options.model_dump(),
        "provider_info_snapshot": {
            "name": spec.provider,
            "selectors_schema": provider_info.selectors_schema,
        },
    },
    observed={"out_spec": out_spec},
)
```

Если есть файлы, логируйте отдельные события:
```py
event_log.emit({
  "type": "replay_resource",
  "id": "catalog_csv",
  "data_ref": {"file": "artifacts/catalog.csv"}
})
```

---

## 4) Replay runtime

### Регистрация обработчиков
Обработчики регистрируются через side-effect импорта.
Рекомендуемый способ:
```py
import fetchgraph.tracer.handlers  # noqa: F401
```

### `ReplayContext`
```py
@dataclass(frozen=True)
class ReplayContext:
    resources: dict[str, dict]
    extras: dict[str, dict]
    base_dir: Path | None = None

    def resolve_resource_path(self, resource_path: str | Path) -> Path: ...
```

### `run_case`
```py
from fetchgraph.tracer.runtime import run_case

out = run_case(root_case, ctx)
```

### `load_case_bundle`
```py
from fetchgraph.tracer.runtime import load_case_bundle

root, ctx = load_case_bundle(Path(".../case.case.json"))
```

---

## 5) Export case bundles

### Python API
```py
from fetchgraph.tracer.export import export_replay_case_bundle, export_replay_case_bundles

path = export_replay_case_bundle(
    events_path=Path(".../events.jsonl"),
    out_dir=Path("tests/fixtures/replay_cases"),
    replay_id="plan_normalize.spec_v1",
    spec_idx=0,
    provider="sql",
    run_dir=Path(".../run_dir"),
    allow_bad_json=False,
    overwrite=False,
)
```

Все совпадения:
```py
paths = export_replay_case_bundles(
    events_path=Path(".../events.jsonl"),
    out_dir=Path("tests/fixtures/replay_cases"),
    replay_id="plan_normalize.spec_v1",
    allow_bad_json=True,
    overwrite=True,
)
```

### Layout ресурсов
Файлы копируются в:
```
resources/<fixture_stem>/<resource_id>/<relative_path>
```

---

## 6) CLI

### tracer CLI
```bash
fetchgraph-tracer export-case-bundle \
  --events path/to/events.jsonl \
  --out tests/fixtures/replay_cases \
  --id plan_normalize.spec_v1 \
  --spec-idx 0 \
  --run-dir path/to/run_dir \
  --allow-bad-json \
  --overwrite
```

Разрешение источника events:
- `--events` — явный путь к events/trace файлу (самый высокий приоритет).
- `--run-id` или `--case-dir` — выбрать конкретный запуск/кейс.
- `--tag` — выбрать самый свежий кейс с events, совпадающий с тегом.
- по умолчанию — самый свежий кейс с events.

Доступные форматы events: `events.jsonl`, `events.ndjson`, `trace.jsonl`, `trace.ndjson`, `traces/events.jsonl`, `traces/trace.jsonl`.

Примеры:
```bash
# По RUN_ID
fetchgraph-tracer export-case-bundle \
  --case agg_003 \
  --data _demo_data/shop \
  --run-id 20260125_160601_retail_cases \
  --id plan_normalize.spec_v1 \
  --out tests/fixtures/replay_cases

# По TAG
fetchgraph-tracer export-case-bundle \
  --case agg_003 \
  --data _demo_data/shop \
  --tag known_bad \
  --id plan_normalize.spec_v1 \
  --out tests/fixtures/replay_cases
```

### Makefile
```bash
make tracer-export REPLAY_ID=plan_normalize.spec_v1 CASE=agg_003 \
  REPLAY_IDATA=_demo_data/shop TRACER_OUT_DIR=tests/fixtures/replay_cases

# Явный events
make tracer-export REPLAY_ID=plan_normalize.spec_v1 EVENTS=path/to/events.jsonl \
  TRACER_OUT_DIR=tests/fixtures/replay_cases RUN_DIR=path/to/run_dir OVERWRITE=1

# Фильтр по TAG
make tracer-export REPLAY_ID=plan_normalize.spec_v1 CASE=agg_003 TAG=known_bad \
  TRACER_OUT_DIR=tests/fixtures/replay_cases

# Явный run/case
make tracer-export REPLAY_ID=plan_normalize.spec_v1 CASE=agg_003 RUN_ID=20260125_160601_retail_cases \
  TRACER_OUT_DIR=tests/fixtures/replay_cases
```

---

## 7) Тестовый workflow

### known_bad
```py
import pytest
from pydantic import ValidationError

import fetchgraph.tracer.handlers  # noqa: F401
from fetchgraph.tracer.runtime import load_case_bundle, run_case
from fetchgraph.tracer.validators import validate_plan_normalize_spec_v1

@pytest.mark.known_bad
@pytest.mark.parametrize("case_path", [...])
def test_known_bad(case_path):
    root, ctx = load_case_bundle(case_path)
    out = run_case(root, ctx)
    with pytest.raises((AssertionError, ValidationError)):
        validate_plan_normalize_spec_v1(out)
```

### Green (explicit expected)
Если рядом лежит `*.expected.json`, сравниваем его с результатом; иначе можно свериться с `root["observed"]`.

---

## 8) Короткая памятка

1) Логируйте `replay_case` через `log_replay_case` (observed-first).
2) Extras/Resources логируйте отдельными событиями (`planner_input`, `replay_resource`).
3) Экспортируйте bundle через `export_replay_case_bundle(s)`.
4) В тестах грузите bundle через `load_case_bundle` и запускайте `run_case`.

Примечание: `log_replay_point` оставлен как deprecated alias, используйте `log_replay_case`.
