# Fetchgraph Tracer: Event Log, Replay Points и фикстуры

> Этот документ описывает “трейсер” (событийный лог + механизм воспроизведения/реплея) по коду из приложенных модулей:
> - `log.py` — контракт логгера событий и хелпер `log_replay_point`
> - `runtime.py` — `ReplayContext` и реестр `REPLAY_HANDLERS`
> - `snapshots.py` — снапшоты для `ProviderInfo` / каталога провайдеров
> - `plan_normalize.py` — пример replay-обработчика `plan_normalize.spec_v1`
> - `export.py` — экспорт replay-point’ов в фикстуры / бандлы (копирование ресурсов)

---

## 1) Зачем это нужно

Трейсер решает две связанные задачи:

1) **Диагностика и воспроизводимость**  
   В рантайме Fetchgraph можно записывать “точки реплея” (replay points) — маленькие, детерминированные фрагменты вычислений: вход → ожидаемый выход (+ метаданные).

2) **Автоматическая генерация регрессионных тестов (fixtures)**  
   Из потока событий (`events.jsonl`) можно автоматически выгрузить фикстуры и гонять “реплей” без LLM/внешних зависимостей: просто вызвать нужный обработчик из `REPLAY_HANDLERS` и сравнить результат с `expected`.

---

## 2) Ключевые понятия

### Event stream (JSONL)
События пишутся в файл (или другой sink) как **JSON Lines**: *одна JSON-строка = одно событие*.

Пример общего вида (как минимум это делает `EventLogger` из demo runner’а):
```json
{"timestamp":"2026-01-25T00:00:00Z","run_id":"abcd1234","type":"...","...": "..."}
```

### Replay point
Событие типа `replay_point` описывает собтыие, которое можно воспроизвести:
- `id` — строковый идентификатор точки (обычно совпадает с ключом обработчика в `REPLAY_HANDLERS`)
- `meta` — фильтруемые метаданные (например `spec_idx`, `provider`)
- `input` — входные данные для реплея
- `expected` — ожидаемый результат
- `requires` *(опционально)* — список зависимостей (ресурсы/доп. события), необходимых для реплея

Минимальный пример:
```json
{
  "type": "replay_point",
  "v": 1,
  "id": "plan_normalize.spec_v1",
  "meta": {"spec_idx": 0, "provider": "sql"},
  "input": {"spec": {...}, "options": {...}},
  "expected": {"out_spec": {...}, "notes_last": "..."},
  "requires": ["planner_input_v1"]
}
```

### Replay resource / extras
`export.py` умеет подтягивать зависимости из event stream двух типов:
- `type="replay_resource"` — ресурсы (часто файлы), которые могут понадобиться при реплее
- `type="planner_input"` — дополнительные входы/контекст (“extras”), которые обработчик может использовать

Важно: для `extras` ключом является `event["id"]` (например `"planner_input_v1"`), а обработчики потом читают это из `ctx.extras[...]`.

---

## 3) Ответственность модулей и классов

### `EventLoggerLike` (`log.py`)
Минимальный контракт “куда писать события”:

```py
class EventLoggerLike(Protocol):
    def emit(self, event: Dict[str, object]) -> None: ...
```

Идея: рантайм может писать события в “настоящий” `EventLog`/`EventLogger`, а тесты/утилиты — принимать любой sink, который реализует `emit`.

### `log_replay_point` (`log.py`)
Унифицированный способ записать `replay_point`:

- гарантирует базовую форму события (`type`, `v`, `id`, `meta`, `input`, `expected`)
- опционально добавляет: `requires`, `diag`, `note`, `error`, `extra`

Рекомендуемый паттерн: **для всех реплейных точек использовать только этот хелпер**, чтобы формат не “расползался”.

---

### `ReplayContext` и `REPLAY_HANDLERS` (`runtime.py`)

#### `ReplayContext`
Контейнер для зависимостей и контекста реплея:
```py
@dataclass(frozen=True)
class ReplayContext:
    resources: Dict[str, dict] = ...
    extras: Dict[str, dict] = ...
    base_dir: Path | None = None

    def resolve_resource_path(self, resource_path: str | Path) -> Path:
        ...
```

- `resources`: “словарь ресурсов” по id (обычно события `replay_resource`)
- `extras`: “словарь доп. данных” по id (обычно события `planner_input`)
- `base_dir`: база для резолва относительных путей ресурсов (полезно для бандлов фикстур)

#### `REPLAY_HANDLERS`
Глобальный реестр обработчиков:
```py
REPLAY_HANDLERS: Dict[str, Callable[[dict, ReplayContext], dict]] = {}
```

Ключ: `replay_point.id`  
Значение: функция `handler(input_dict, ctx) -> output_dict`

---

### `snapshots.py`
Снапшоты данных о провайдерах, чтобы реплей был более стабильным:

- `snapshot_provider_info(info: ProviderInfo) -> Dict[str, object]`
- `snapshot_provider_catalog(provider_catalog: Mapping[str, object]) -> Dict[str, object]`

Смысл: вместо того чтобы зависеть от “живых” объектов, сохраняем стабильный JSON-слепок.

---

### Пример обработчика: `plan_normalize.spec_v1` (`plan_normalize.py`)
Функция:
```py
def replay_plan_normalize_spec_v1(inp: dict, ctx: ReplayContext) -> dict: ...
REPLAY_HANDLERS["plan_normalize.spec_v1"] = replay_plan_normalize_spec_v1
```

Что делает (по коду):
1) Берёт `inp["spec"]` и `inp["options"]`
2) Строит `PlanNormalizerOptions`
3) Вытаскивает правила нормализации из `normalizer_rules` или `normalizer_registry`
4) Пытается восстановить `ProviderInfo` из `ctx.extras["planner_input_v1"]["input"]["provider_catalog"][provider]`
   - если не получилось — создаёт `ProviderInfo(name=provider, capabilities=[])`
5) Собирает `PlanNormalizer` и нормализует один `ContextFetchSpec`
6) Возвращает:
   - `out_spec` (provider/mode/selectors)
   - `notes_last` (последняя заметка нормализатора)

---

### Экспорт фикстур: `export.py`

#### Что делает `export.py`
1) Читает `events.jsonl` построчно (`iter_events`)
2) Находит `replay_point` с нужным `id` (+ фильтры `spec_idx`/`provider`)
3) Опционально подтягивает зависимости из `requires`:
   - `replay_resource` события → `ctx.resources`
   - `planner_input` события → `ctx.extras`
4) Пишет фикстуру в `out_dir`:
   - **простая фикстура** (`write_fixture`) — только replay_point + `source`
   - **bundle** (`write_bundle`) — root replay_point + resources + extras + `source`,
     и при необходимости копирует файлы ресурсов в структуру `resources/<fixture_stem>/...`

#### Имена фикстур
Имя вычисляется стабильно:
```py
fixture_name = f"{event_id}__{sha256(event_id + canonical_json(input))[:8]}.json"
```

---

## 4) Как этим пользоваться

### 4.1 В рантайме: как логировать replay-point’ы

1) Подготовить объект, реализующий `EventLoggerLike.emit(...)`.

Пример (упрощённый) — JSONL writer:
```py
from pathlib import Path
import json, datetime

class JsonlEventLog:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: dict) -> None:
        payload = {"timestamp": datetime.datetime.utcnow().isoformat() + "Z", **event}
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
```

2) В точке, где хотите получать реплейный тест, вызвать `log_replay_point(...)`:

```py
from fetchgraph.tracer import log_replay_point  # путь зависит от того, где пакет лежит у вас

log_replay_point(
    logger=event_log,
    id="plan_normalize.spec_v1",
    meta={"spec_idx": i, "provider": spec.provider},
    input={"spec": spec.model_dump(), "options": options.model_dump(), "normalizer_rules": rules},
    expected={"out_spec": out_spec, "notes_last": notes_last},
    requires=["planner_input_v1"],  # если обработчику нужен контекст
)
```

3) Если есть внешние файлы/ресурсы, которые нужно будет копировать в bundle — записывайте отдельные события `replay_resource`
(формат в коде не зафиксирован типами, но `export.py` ожидает хотя бы `type="replay_resource"`, `id=str`, и опционально `data_ref.file`):
```py
event_log.emit({
  "type": "replay_resource",
  "id": "catalog_csv",
  "data_ref": {"file": "relative/path/to/catalog.csv"}
})
```

---

### 4.2 Реплей: как выполнить фикстуру

1) Импортировать модули обработчиков, чтобы они зарегистрировались в `REPLAY_HANDLERS`  
(сейчас регистрация — через side-effect: модуль при импорте пишет в dict).

Например:
```py
from fetchgraph.tracer.runtime import REPLAY_HANDLERS, ReplayContext
import fetchgraph.tracer.plan_normalize  # важно: импорт модуля регистрирует обработчик
```

2) Загрузить фикстуру (или bundle) и построить `ReplayContext`.

- Для простой фикстуры:
```py
fixture = json.load(open(path, "r", encoding="utf-8"))
ctx = ReplayContext(resources={}, extras={}, base_dir=path.parent)
handler = REPLAY_HANDLERS[fixture["id"]]
out = handler(fixture["input"], ctx)
assert out == fixture["expected"]
```

- Для bundle:
```py
bundle = json.load(open(path, "r", encoding="utf-8"))
root = bundle["root"]
ctx = ReplayContext(
  resources=bundle.get("resources", {}),
  extras=bundle.get("extras", {}),
  base_dir=path.parent,
)
handler = REPLAY_HANDLERS[root["id"]]
out = handler(root["input"], ctx)
assert out == root["expected"]
```

---

### 4.3 Экспорт фикстур из events.jsonl

Python API (как в `export.py`):

- **Одна фикстура**:
```py
from fetchgraph.tracer.export import export_replay_fixture
export_replay_fixture(
    events_path=Path("_demo_data/.../events.jsonl"),
    out_dir=Path("tests/fixtures/replay"),
    replay_id="plan_normalize.spec_v1",
    spec_idx=0,
    provider="sql",
    with_requires=True,
    run_dir=Path("_demo_data/.../run_dir"),
)
```

- **Все совпадения (если один `id` встречается много раз)**:
```py
from fetchgraph.tracer.export import export_replay_fixtures
paths = export_replay_fixtures(
    events_path=...,
    out_dir=...,
    replay_id="plan_normalize.spec_v1",
    with_requires=False,
)
```

---

## 5) Рекомендации по pytest-фикстурам

Минимальный удобный слой (совет):

- `load_replay_fixture(path) -> (root_event, ctx)`
- `run_replay(root_event, ctx) -> out`

Чтобы каждый тест был “одной строкой”.

Пример (псевдокод):
```py
@pytest.mark.parametrize("fixture_path", glob("tests/fixtures/replay/*.json"))
def test_replay_fixture(fixture_path):
    root, ctx = load_fixture_and_ctx(fixture_path)
    handler = REPLAY_HANDLERS[root["id"]]
    assert handler(root["input"], ctx) == root["expected"]
```

---

## 6) “Короткая памятка” для разработчика

1) В коде, где хотите регрессию, используйте `log_replay_point(...)`.
2) Если реплей требует контекст/файлы:
   - логируйте `planner_input` (extras) и/или `replay_resource`
   - добавляйте их `id` в `requires`
3) Для тестов:
   - экспортируйте фикстуры из `events.jsonl` через `export.py`
   - импортируйте модули обработчиков (чтобы заполнить `REPLAY_HANDLERS`)
   - грузите фикстуру, строите `ReplayContext`, вызывайте handler, сравнивайте с `expected`
