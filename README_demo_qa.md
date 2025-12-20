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

### Зависимости демо
```
pip install -e .[demo]
# или
pip install -r examples/demo_qa/requirements.txt
```
`examples/` не включается в пакет/PyPI, зависимости опциональны.

## Чат

### OpenAI / совместимый прокси
1. Скопируйте `examples/demo_qa/demo_qa.toml.example` в удобное место и укажите
   `llm.api_key` (можно `env:OPENAI_API_KEY` или любое значение, если прокси не проверяет ключ),
   `base_url` (формат `http://host:port/v1`), модели и температуры.
2. Запустите чат с указанием конфига:
```bash
python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.json --config path/to/demo_qa.toml
```

Флаг `--enable-semantic` строит семантический индекс, если передана модель эмбеддингов.

## Batch

Запустить пакетный прогон вопросов из `cases.jsonl` (по одному JSON на строку, поля `id`, `question`, опционально `expected`/`expected_regex`/`expected_contains` и `skip`):

```bash
python -m examples.demo_qa.cli batch \
  --data demo_data \
  --schema demo_data/schema.json \
  --cases cases.jsonl \
  --out results.jsonl
```

* Артефакты по умолчанию пишутся в `<data>/.runs/runs/<timestamp>_<cases_stem>/cases/<id>_<runid>/` (`plan.json`, `context.json`, `answer.txt`, `raw_synth.txt`, `error.txt`).
* `results.jsonl` содержит по строке на кейс, рядом сохраняется `summary.json` с агрегацией статусов и, при наличии `--compare-to`, diff по прогрессу.
* Флаги `--fail-on (error|bad|unchecked|any|skipped)`, `--max-fails`, `--fail-fast`, `--require-assert`, `--compare-to`, `--only-failed-from/--only-failed` и `--plan-only` управляют выбором кейсов, остановкой и кодом выхода (0/1/2).
* Без `--out` результаты складываются в `<artifacts_dir>/runs/<timestamp>_<cases_stem>/results.jsonl`, а `runs/latest.txt` указывает на последнюю папку запуска.
* Быстрый фокус на упавших: `--only-failed` возьмёт `runs/latest/results.jsonl`, `--show-artifacts` печатает пути, репро-команды выводятся для каждого FAIL.
* Команды уровня кейса: `demo_qa case run <id> --cases ...` и `demo_qa case open <id> --run runs/latest` для быстрого воспроизведения.
## Local proxy

Для OpenAI-совместимых серверов (например, LM Studio) укажите `base_url` с `.../v1` и
любым ключом доступа, если прокси не проверяет его. Запуск:

```bash
python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.json --config path/to/demo_qa.toml
```

Большинство OpenAI-совместимых сервисов ожидают конечную точку `/v1` в `base_url`.
