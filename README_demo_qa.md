# Demo QA harness

Пример интерактивного стенда для fetchgraph. Расположен в `examples/demo_qa` и не входит в пакетную сборку.

## Генерация данных

```bash
python -m examples.demo_qa.cli gen --out demo_data --rows 1000 --seed 42
```

Команда создаст четыре CSV, `schema.yaml`, `meta.json` и `stats.json`.

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
python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.yaml --config path/to/demo_qa.toml
```

Флаг `--enable-semantic` строит семантический индекс, если передана модель эмбеддингов.

## Batch-запуск

Команда для пакетного прогона набора вопросов:

```bash
python -m examples.demo_qa.cli batch \
  --data demo_data \
  --schema demo_data/schema.yaml \
  --config path/to/demo_qa.toml \
  --cases cases.jsonl \
  --out results.jsonl
```

Флаги:
- `--artifacts-dir` — куда складывать артефакты (`plan.json`, `context.json`, `answer.txt`).
- `--fail-on {error,mismatch,any}`, `--max-fails`, `--fail-fast` — управление выходным кодом и остановкой.
- `--llm-cache {off,record,replay}` и `--llm-cache-file` — кэширование ответов LLM для офлайн-прогонов.

Рядом с `results.jsonl` пишется `summary.json` с агрегатами, а в stdout выводится компактная таблица по кейсам.

## Local proxy

Для OpenAI-совместимых серверов (например, LM Studio) укажите `base_url` с `.../v1` и
любым ключом доступа, если прокси не проверяет его. Запуск:

```bash
python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.yaml --config path/to/demo_qa.toml
```

Большинство OpenAI-совместимых сервисов ожидают конечную точку `/v1` в `base_url`.
