# Demo QA harness

Пример интерактивного стенда для fetchgraph. Расположен в `examples/demo_qa` и не входит в пакетную сборку.

## Генерация данных

```bash
python -m examples.demo_qa.cli gen --out demo_data --rows 1000 --seed 42
```

Команда создаст четыре CSV, `schema.json`, `meta.json` и `stats.json`.

## Конфигурация LLM (pydantic-settings)

Порядок источников: CLI overrides > env vars > `.env.demo_qa` > `demo_qa.toml` > дефолты.

### Файл demo_qa.toml
См. шаблон `examples/demo_qa/demo_qa.toml.example`.
Автопоиск: `--config`, затем `<DATA_DIR>/demo_qa.toml`, затем `examples/demo_qa/demo_qa.toml`.
`llm.api_key` можно опустить: при инициализации LLM используется `OPENAI_API_KEY`, а при его отсутствии — строка `"unused"`.

### .env.demo_qa
Пример:
```
DEMO_QA_LLM__API_KEY=env:OPENAI_API_KEY
DEMO_QA_LLM__BASE_URL=http://localhost:8000/v1
```

### Env vars напрямую
```
export DEMO_QA_LLM__API_KEY=sk-...
export DEMO_QA_LLM__BASE_URL=http://localhost:8000/v1
```
Если не задавать `DEMO_QA_LLM__API_KEY` и не выставлять `OPENAI_API_KEY`, LLM-клиент подставит `"unused"` и не упадёт.

### Зависимости демо
* Требуется Python 3.11+ (используется стандартный `tomllib`).
```
pip install -e .[demo]
# или
pip install -r examples/demo_qa/requirements.txt
```
`examples/` не включается в пакет/PyPI, зависимости опциональны.

## Чат

### OpenAI / совместимый прокси
1. Скопируйте `examples/demo_qa/demo_qa.toml.example` в удобное место и укажите
   при необходимости `llm.api_key` (можно `env:OPENAI_API_KEY`; если не указать, возьмётся `OPENAI_API_KEY` или `"unused"`),
   `base_url` (формат `http://host:port/v1`), модели и температуры.
2. Запустите чат с указанием конфига:
```bash
python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.json --config path/to/demo_qa.toml
```

Флаг `--enable-semantic` строит семантический индекс, если передана модель эмбеддингов.

## Batch

Запустить пакетный прогон вопросов из файла кейсов (`cases.jsonl` или `cases.json`).

Поддерживаемые форматы:

* **JSONL**: по одному JSON-объекту на строку.
* **JSON**: массив объектов.

Поля кейса: `id`, `question`, опционально `expected`/`expected_regex`/`expected_contains` и `skip`.

```bash
python -m examples.demo_qa.cli batch \
  --data demo_data \
  --schema demo_data/schema.json \
  --cases cases.jsonl \
  --out results.jsonl
```

Что сохраняется:

* Артефакты по кейсам по умолчанию пишутся в `<data>/.runs/runs/<timestamp>_<cases_stem>/cases/<id>_<runid>/` (`plan.json`, `context.json`, `answer.txt`, `raw_synth.txt`, `error.txt`).
* `results.jsonl` содержит по строке на кейс, рядом сохраняется `summary.json` с агрегацией статусов.
* Без `--out` результаты складываются в `<data>/.runs/runs/<timestamp>_<cases_stem>/results.jsonl`, а `runs/latest.txt` указывает на последнюю папку запуска, `runs/latest_results.txt` — на путь к results.
* При `Ctrl-C` сохраняются частичные результаты: уже пройденные кейсы попадают в `results.jsonl/summary.json`, а прогон помечается как `interrupted`.

Ключевые флаги:

* `--fail-on (error|bad|unchecked|any|skipped)`, `--max-fails`, `--fail-fast`, `--require-assert` — остановка/код выхода (0/1/2) и строгость проверок.
* `--only-failed` / `--only-failed-from PATH` — перепрогон только плохих кейсов (baseline = latest либо явно заданный results).
* `--only-failed-effective` — перепрогон только плохих кейсов относительно effective snapshot для `--tag` (без ручного поиска baseline).
* `--only-missed` / `--only-missed-from PATH` — “добить” только те кейсы, которые отсутствуют в baseline results (удобно после Ctrl-C).
* `--only-missed-effective` — “добить” кейсы, которых нет в effective snapshot для `--tag`.
* `--tag TAG` / `--note "..."` — пометить прогон как часть эксперимента. Для `--tag` поддерживается “effective snapshot”: результаты по тегу накапливаются инкрементально, так что `--only-failed/--only-missed` по тегу корректно работают даже после частичных прогонов.
* `--plan-only` — строить планы без выполнения.

Команды уровня кейса:

* `python -m examples.demo_qa.cli case run <id> --cases ...` — прогнать один кейс.
* `python -m examples.demo_qa.cli case open <id> --data ...` — открыть папку артефактов кейса.

Отчёты и история:

* `python -m examples.demo_qa.cli stats --data <DATA_DIR> --last 10` — последние прогоны.
* `python -m examples.demo_qa.cli report tag --data <DATA_DIR> --tag <TAG>` — сводка по “effective” результатам тега.
* `python -m examples.demo_qa.cli report run --data <DATA_DIR> --run runs/latest` — сводка по конкретному run.
* `python -m examples.demo_qa.cli history case <id> --data <DATA_DIR> [--tag <TAG>]` — история по кейсу.
* `python -m examples.demo_qa.cli compare --base <PATH> --new <PATH> [--out ... --junit ... --format md|table|json --color auto|always|never]` — сравнить два результата по путям (табличный и JSON форматы удобны для CI/терминала).
* `python -m examples.demo_qa.cli compare --data <DATA_DIR> --base-tag <TAG1> --new-tag <TAG2> [...]` — сравнить “effective snapshot” двух тегов без явных путей (работает и для неполных прогонов).

### Удобные алиасы (bash/zsh)

Добавьте в `~/.bashrc` или `~/.zshrc` и перезапустите shell.

```bash
# 1) Настройте свои дефолты под проект/датасет
export DQ_DATA="./_demo_data/shop"
export DQ_SCHEMA="$DQ_DATA/schema.json"
export DQ_CASES="./examples/demo_qa/cases/retail_cases.json"
export DQ_OUT="$DQ_DATA/.runs/results.jsonl"
export DQ_TAG="retail-iter1"

# 2) Базовая команда
dq() { python -m examples.demo_qa.cli "$@"; }

# 3) Самые частые сценарии
dq-batch()  { dq batch  --data "$DQ_DATA" --schema "$DQ_SCHEMA" --cases "$DQ_CASES" --out "$DQ_OUT" "$@"; }
dq-failed() { dq-batch --only-failed "$@"; }
dq-missed() { dq-batch --only-missed "$@"; }
dq-failed-effective()  { dq-batch --tag "$DQ_TAG" --only-failed-effective "$@"; }
dq-missed-effective()  { dq-batch --tag "$DQ_TAG" --only-missed-effective "$@"; }

# Tagged (effective) workflow
dq-batch-tag()  { dq-batch --tag "$DQ_TAG" "$@"; }
dq-failed-tag() { dq-batch --tag "$DQ_TAG" --only-failed "$@"; }
dq-missed-tag() { dq-batch --tag "$DQ_TAG" --only-missed "$@"; }

# Отчёты
dq-stats()   { dq stats  --data "$DQ_DATA" "$@"; }
dq-report()  { dq report tag --data "$DQ_DATA" --tag "$DQ_TAG" "$@"; }
dq-run()     { dq report run --data "$DQ_DATA" --run "${1:-runs/latest}"; }
dq-hist()    { dq history case "$1" --data "$DQ_DATA" --tag "$DQ_TAG" "${@:2}"; }
dq-compare() { dq compare --base "$1" --new "$2" "${@:3}"; }
dq-compare-tag() { dq compare --data "$DQ_DATA" --base-tag "${1:-baseline}" --new-tag "$2" "${@:3}"; }

# Дебаг кейса
dq-case()    { dq case run "$1" --cases "$DQ_CASES" --data "$DQ_DATA" --schema "$DQ_SCHEMA" "${@:2}"; }
dq-open()    { dq case open "$1" --data "$DQ_DATA" "${@:2}"; }
```

Минимальный набор, если не хочется “тегов”:

```bash
dq() { python -m examples.demo_qa.cli "$@"; }
dq-batch()  { dq batch  --data "$DQ_DATA" --schema "$DQ_SCHEMA" --cases "$DQ_CASES" --out "$DQ_OUT" "$@"; }
dq-failed() { dq-batch --only-failed "$@"; }
dq-missed() { dq-batch --only-missed "$@"; }
dq-stats()  { dq stats --data "$DQ_DATA" --last 10; }
```


## Local proxy

Для OpenAI-совместимых серверов (например, LM Studio) укажите `base_url` с `.../v1` и
любым ключом доступа, если прокси не проверяет его. Запуск:

```bash
python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.json --config path/to/demo_qa.toml
```

Большинство OpenAI-совместимых сервисов ожидают конечную точку `/v1` в `base_url`.
