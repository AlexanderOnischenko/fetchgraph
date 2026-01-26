# Fetchgraph Tracer: observed-first replay cases, bundles и реплей

> Этот документ описывает актуальный формат трейса и реплея:
> - `log.py` — контракт логгера событий и хелпер `log_replay_case`
> - `runtime.py` — `ReplayContext`, `REPLAY_HANDLERS`, `run_case`, `load_case_bundle`
> - `handlers/*` — обработчики (регистрация в `REPLAY_HANDLERS`)
> - `export.py` — экспорт `replay_case` → case bundle (`*.case.json`)

> Актуально для консольной команды `fetchgraph-tracer` и модулей `fetchgraph.tracer/*` + `fetchgraph.replay/*`.
> Путь к фикстурам по умолчанию: `tests/fixtures/replay_cases/{fixed,known_bad}`.

---

## 0) Зачем это нужно

Трейсер решает две задачи:

1) **Observed-first логирование**
   В рантайме пишется **input + observed outcome** (успех) **или** `observed_error` (ошибка) + зависимости для реплея. `expected` не логируется.

2) **Реплей и регрессии**
   Из `events.jsonl` экспортируются **case bundles** (root case + extras/resources + source). Реплей работает без LLM и внешних сервисов.

---

## 1) Термины и базовые сущности

### 1.1 Event stream (JSONL)

Трейс — это JSONL/NDJSON: одна строка = одно событие.

События, которые важны для реплея:

- `type="replay_case"` — корневое событие “кейса реплея” (v2).
- `type="planner_input"` — “extra” (например, входы планировщика), адресуется по `id`.
- `type="replay_resource"` — “resource” (например, файл), адресуется по `id`.

### 1.2 Replay case (v2)

Схема корневого события:

- `type="replay_case"`, `v=2`
- `id` — идентификатор обработчика (namespace вида `something.v1`)
- `meta` — опционально (например, `{spec_idx, provider}`)
- `input` — dict, вход для обработчика
  - `input.provider_info_snapshot` — минимальный snapshot провайдера (например, `selectors_schema`), чтобы реплей был детерминированным
- **ровно одно** из:
  - `observed` — dict (observed outcome)
  - `observed_error` — dict (observed error), если во время “наблюдения” упало
- `requires` — опционально: список зависимостей
  - v2-формат: `[{"kind":"extra"|"resource","id":"..."}]`
  - legacy-формат допускается в экспорте: `["id1","id2"]` (будет нормализован при экспорте)

Пример:

```json
{
  "type": "replay_case",
  "v": 2,
  "id": "plan_normalize.spec_v1",
  "meta": {"spec_idx": 0, "provider": "sql"},
  "input": {
    "spec": {"provider": "sql", "selectors": {"q": "..." }},
    "options": {"lowercase": true},
    "provider_info_snapshot": {"name": "sql", "selectors_schema": {"type":"object","properties":{}}}
  },
  "observed": {"out_spec": {"provider": "sql", "selectors": {"q": "..."}}},
  "requires": [{"kind":"extra","id":"planner_input_v1"}]
}
```

### 1.3 Resources и extras

- **Extras**: события `type="planner_input"`, индексируются по `id`.
- **Resources**: события `type="replay_resource"`, индексируются по `id`.
  - Если ресурс указывает `data_ref.file`, это **относительный путь** внутри `run_dir` во время экспорта.
  - При экспорте файлы копируются в fixture-layout (см. ниже) и `data_ref.file` переписывается на новый относительный путь.

---

## 2) Контракт логирования (observed-first)

### 2.1 EventLoggerLike

Трейсер принимает любой logger, который умеет:

```py
class EventLoggerLike(Protocol):
    def emit(self, event: dict) -> None: ...
```

### 2.2 log_replay_case

`log_replay_case(logger=..., id=..., input=..., observed=.../observed_error=..., requires=..., meta=...)`

Валидация на входе:

- `id` — непустая строка
- `input` — dict
- XOR: `observed` / `observed_error`
- `requires` — список `{kind,id}`

**Рекомендация:** логируйте `provider_info_snapshot` (см. `fetchgraph.replay.snapshots`), чтобы реплей не зависел от внешних данных.

---

## 3) Replay runtime

### 3.1 Регистрация обработчиков

`REPLAY_HANDLERS` — dict `{replay_id: handler(input, ctx) -> dict}`.

Обработчики регистрируются через side-effect импорта, рекомендуемый способ в тестах/скриптах:

```py
import fetchgraph.tracer.handlers  # noqa: F401
```

### 3.2 ReplayContext

```py
@dataclass(frozen=True)
class ReplayContext:
    resources: dict[str, dict]
    extras: dict[str, dict]
    base_dir: Path | None

    def resolve_resource_path(self, resource_path: str | Path) -> Path:
        # относительные пути резолвятся относительно base_dir
```

### 3.3 run_case и load_case_bundle

```py
from fetchgraph.tracer.runtime import load_case_bundle, run_case

root, ctx = load_case_bundle(Path(".../*.case.json"))
out = run_case(root, ctx)
```

---

## 4) Case bundle (fixture) format

Экспорт создает JSON файл:

- `schema="fetchgraph.tracer.case_bundle"`, `v=1`
- `root` — replay_case (как в events)
- `resources` — dict ресурсов по id
- `extras` — dict extras по id
- `source` — метаданные (минимум: `events_path`, `line`, опционально `run_id`, `timestamp`, `case_id`)

### 4.1 Layout фикстур и ресурсов

Рекомендуемый layout:

```
tests/fixtures/replay_cases/
  fixed/
    <stem>.case.json
    <stem>.expected.json          # для green кейсов
    resources/<stem>/<resource_id>/...
  known_bad/
    <stem>.case.json
    resources/<stem>/<resource_id>/...
```

Где `<stem>` — имя фикстуры (обычно совпадает с именем `.case.json` без расширения).

При экспорте файлы ресурсов копируются в:

```
resources/<stem>/<resource_id>/<relative_path_from_run_dir>
```

С коллизиями экспорт падает fail-fast (чтобы не смешивать фикстуры из разных прогонов).

---

## 5) Export API (Python)

```py
from fetchgraph.tracer.export import export_replay_case_bundle, export_replay_case_bundles

# один bundle
path = export_replay_case_bundle(
    events_path=Path("./events.jsonl"),
    out_dir=Path("tests/fixtures/replay_cases/known_bad"),
    replay_id="plan_normalize.spec_v1",
    spec_idx=0,            # фильтр по meta.spec_idx (опционально)
    provider="sql",        # фильтр по meta.provider (опционально)
    run_dir=Path("./run_dir"),  # обязателен, если есть file-resources
    allow_bad_json=False,
    overwrite=False,
    selection_policy="latest",  # latest|first|last|by-timestamp|by-line
    select_index=None,          # 1-based индекс среди replay_case матчей
    require_unique=False,
)

# все совпадения
paths = export_replay_case_bundles(
    events_path=Path("./events.jsonl"),
    out_dir=Path("tests/fixtures/replay_cases/known_bad"),
    replay_id="plan_normalize.spec_v1",
    allow_bad_json=True,
    overwrite=True,
)
```

---

## 6) CLI: `fetchgraph-tracer`

Команда подключена как `fetchgraph-tracer = fetchgraph.tracer.cli:main`.

### 6.1 export-case-bundle

> Примечание: в старом/экспериментальном синтаксисе мог встречаться флаг `--replay-id`; актуальный флаг — `--id`.


Экспорт одного или нескольких case bundles из events:

```bash
fetchgraph-tracer export-case-bundle   --out tests/fixtures/replay_cases/known_bad   --id plan_normalize.spec_v1   --events path/to/events.jsonl   --run-dir path/to/run_dir   --overwrite   --allow-bad-json
```

#### 6.1.1 Как выбирается events/run_dir

Приоритет разрешения:

1) `--events` — явный путь к events/trace файлу.  
   `run_dir` берется из `--case-dir` или `--run-dir` (если нужны file-resources).

2) Иначе (auto-resolve через `.runs`):
   - обязателен `--case <CASE_ID>` и `--data <DATA_DIR>`
   - дальше выбираем конкретный run/case:
     - `--case-dir <PATH>` — явный путь к кейсу
     - `--run-dir <PATH>` — явный путь к run (кейс выбирается из `run_dir/cases`)
     - либо “самый свежий” по стратегии `--pick-run` (+ опционально `--tag`)

Поддерживаемые имена файлов events (быстрый поиск):  
`events.jsonl`, `events.ndjson`, `trace.jsonl`, `trace.ndjson`, `traces/events.jsonl`, `traces/trace.jsonl`.

Если ни один из стандартных путей не найден — делается fallback-поиск по `*.jsonl/*.ndjson` в глубину до 3 уровней (кроме `resources/`) и выбирается самый “тяжелый” файл.

#### 6.1.2 Run selection (auto-resolve) flags

- `--case <CASE_ID>` — id кейса (например `agg_003`)
- `--data <DATA_DIR>` — директория с `.runs`
- `--runs-subdir <REL>` — где искать `runs` относительно `DATA_DIR` (default `.runs/runs`)
- `--tag <TAG>` — фильтр по tag (если теги ведутся)
- `--pick-run <MODE>` — стратегия выбора запуска:
  - `latest_non_missed`
  - `latest_with_replay` (default; требует `--id`)
- `--select-index <N>` — выбрать конкретный run-candidate (1-based)
- `--list-matches` — вывести список кандидатов run/case и выйти
- `--debug` — детальный вывод кандидатов (или `DEBUG=1`)

Полезно:
- `--print-resolve` — распечатать, во что именно разрешилось (`run_dir`, `case_dir`, `events_path`, `selection_method`).

#### 6.1.3 Replay-case selection flags (когда в events много replay_case)

- `--spec-idx <INT>` — фильтр по `meta.spec_idx`
- `--provider <NAME>` — фильтр по `meta.provider` (case-insensitive)
- `--list-replay-matches` — вывести найденные replay_case entries и выйти
- `--require-unique` — упасть, если матчей > 1
- `--select <POLICY>` — политика выбора:
  - `latest` (по timestamp, fallback по line)
  - `first`
  - `last`
  - `by-timestamp`
  - `by-line`
- `--replay-select-index <N>` — выбрать конкретный replay_case матч (1-based)
- `--all` — экспортировать **все** матчинг replay_case (игнорируя single-selection)

---

### 6.2 Управление фикстурами (fixture tools)

#### 6.2.1 fixture-ls

Показать кандидатов (по умолчанию bucket=known_bad):

```bash
fetchgraph-tracer fixture-ls --case-id agg_003
fetchgraph-tracer fixture-ls --bucket fixed --pattern "plan_normalize.*"
```

#### 6.2.2 fixture-green

“Позеленить” кейс: перенести из `known_bad` → `fixed` и записать `*.expected.json` из `root.observed`.

```bash
# выбрать по case_id (если несколько — используется --select/--select-index)
fetchgraph-tracer fixture-green --case-id agg_003 --validate

# или явно указать файл
fetchgraph-tracer fixture-green --case tests/fixtures/replay_cases/known_bad/<stem>.case.json --validate
```

Флаги:
- `--overwrite-expected` — перезаписать `*.expected.json`, если уже есть
- `--validate` — после перемещения прогнать `run_case()` и сравнить с expected
- `--git auto|on|off` — перемещения/удаления через git (если доступно)
- `--dry-run` — только печать действий
- `--select/--select-index/--require-unique` — выбор среди нескольких кандидатов

> Важно: `fixture-green` требует `root.observed`. Если в кейсе только `observed_error`, сначала нужно переэкспортировать bundle после фикса (чтобы был observed).

#### 6.2.3 fixture-demote

Перенести `fixed` → `known_bad` (например, если баг вернулся или кейс решили держать как backlog):

```bash
fetchgraph-tracer fixture-demote --case-id agg_003
```

Флаги:
- `--overwrite` — перезаписать существующие target-фикстуры
- `--all` — применить ко всем матчам
- остальные — как в green (select/dry-run/git)

#### 6.2.4 fixture-fix

Переименовать stem фикстуры (case + expected + resources):

```bash
fetchgraph-tracer fixture-fix --bucket fixed --name old_stem --new-name new_stem
```

#### 6.2.5 fixture-migrate

Нормализовать layout ресурсов/путей внутри bundle (полезно при изменении схемы ресурсов):

```bash
fetchgraph-tracer fixture-migrate --bucket all --case-id agg_003 --dry-run
```

#### 6.2.6 fixture-rm

Удалить фикстуры (cases/resources):

```bash
# удалить конкретный stem в known_bad
fetchgraph-tracer fixture-rm --bucket known_bad --name "<stem>"

# удалить по case_id (опционально --all, иначе выберет один по --select)
fetchgraph-tracer fixture-rm --bucket all --case-id agg_003 --scope both --all
```

Флаги:
- `--scope cases|resources|both`
- `--pattern <glob>` — матчить по stem/пути
- `--all` — применить ко всем матчам
- `--git auto|on|off`, `--dry-run`, `--select*`

---

## 7) Make targets (DX)

Makefile предоставляет врапперы (см. `make help`):

### 7.1 Экспорт

```bash
# обычный export (auto-resolve через .runs)
make tracer-export REPLAY_ID=plan_normalize.spec_v1 CASE=agg_003   DATA=_demo_data/shop TRACER_OUT_DIR=tests/fixtures/replay_cases/known_bad

# явный events + run_dir
make tracer-export REPLAY_ID=plan_normalize.spec_v1 CASE=agg_003 EVENTS=path/to/events.jsonl   RUN_DIR=path/to/run_dir TRACER_OUT_DIR=tests/fixtures/replay_cases/known_bad OVERWRITE=1
```

### 7.2 Список кандидатов (auto-resolve)

> В Makefile `DATA` используется как основной параметр; внутри таргетов он пробрасывается как `REPLAY_IDATA` (alias).


```bash
make tracer-ls CASE=agg_003 DATA=_demo_data/shop TAG=known_bad
```

### 7.3 Запуск known_bad

```bash
make known-bad
make known-bad-one NAME=<fixture_stem>
```

### 7.4 Управление фикстурами

```bash
# promote known_bad -> fixed
make fixture-green CASE=agg_003 VALIDATE=1

# list
make fixture-ls CASE=agg_003 BUCKET=known_bad

# remove
make fixture-rm BUCKET=known_bad CASE=agg_003 ALL=1 DRY=1

# rename stem
make fixture-fix BUCKET=fixed NAME=old NEW_NAME=new

# migrate
make fixture-migrate BUCKET=all CASE=agg_003 DRY=1

# demote fixed -> known_bad
make fixture-demote CASE=agg_003 OVERWRITE=1
```

---

## 8) Тестовый workflow и CI

### 8.1 Buckets: fixed vs known_bad

- `fixed/` — **регрессионные** кейсы: должны проходить и обычно имеют `.expected.json`.
- `known_bad/` — **backlog/TDD-like** кейсы: могут падать или возвращать невалидный результат.
  В pytest они должны идти под маркером `known_bad`, чтобы их легко исключать из CI.

Рекомендуемый CI фильтр: `-m "not known_bad"` (и при необходимости `not slow`).

### 8.2 Диагностика known_bad

Для подробной диагностики можно включить окружение:

- `FETCHGRAPH_REPLAY_DEBUG=1` — больше деталей в выводе
- `FETCHGRAPH_REPLAY_TRUNCATE=<N>` — лимит тримминга больших блоков
- `FETCHGRAPH_REPLAY_META_TRUNCATE=<N>` — лимит для meta
- `FETCHGRAPH_RULE_TRACE_TAIL=<N>` — сколько элементов `diag.rule_trace` показывать

---

## 9) Как добавить новый replay-case

1) Выберите `replay_id` (строка) и зафиксируйте его в коде.
2) Добавьте handler в `fetchgraph/tracer/handlers/...` и зарегистрируйте в `REPLAY_HANDLERS`.
3) При “наблюдении” логируйте:
   - `replay_case` через `log_replay_case(...)`
   - `planner_input` / `replay_resource` отдельными событиями, и добавляйте их в `requires`
4) Добавьте валидатор (если нужен) для fixed/regression тестов.
5) Проверьте, что экспорт реплея воспроизводим (нет внешних сервисов, нет LLM).

---

## 10) Совместимость и заметки

- `log_replay_point` оставлен как deprecated alias; используйте `log_replay_case`.
- Экспорт поддерживает legacy `requires=["id1","id2"]`, но желательно писать v2-формат `[{kind,id}]`.
