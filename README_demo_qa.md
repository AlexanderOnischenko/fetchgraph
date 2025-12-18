# Demo QA harness

Пример интерактивного стенда для fetchgraph. Расположен в `examples/demo_qa` и не входит в пакетную сборку.

## Генерация данных

```bash
python -m examples.demo_qa.cli gen --out demo_data --rows 1000 --seed 42
```

Команда создаст четыре CSV, `schema.yaml`, `meta.json` и `stats.json`.

## Конфигурация LLM

Настройки читаются из TOML-файла `demo_qa.toml` (см. шаблон `examples/demo_qa/demo_qa.toml.example`).
Приоритет поиска:

1. Явный путь через `--config`.
2. `<DATA_DIR>/demo_qa.toml` (рядом с данными, если указан `--data`).
3. `examples/demo_qa/demo_qa.toml` (локальный файл разработчика).
4. Если файл не найден, используется провайдер `mock` по умолчанию.

Поверх файла можно задать переменные окружения: `DEMO_QA_LLM_PROVIDER`,
`DEMO_QA_OPENAI_API_KEY`/`OPENAI_API_KEY`, `DEMO_QA_OPENAI_BASE_URL`,
`DEMO_QA_OPENAI_PLAN_MODEL`, `DEMO_QA_OPENAI_SYNTH_MODEL`,
`DEMO_QA_OPENAI_PLAN_TEMPERATURE`, `DEMO_QA_OPENAI_SYNTH_TEMPERATURE`,
`DEMO_QA_OPENAI_TIMEOUT`, `DEMO_QA_OPENAI_RETRIES`, `DEMO_QA_MOCK_PLAN_FIXTURE`,
`DEMO_QA_MOCK_SYNTH_TEMPLATE`. Значение вида `env:VAR_NAME` в конфиге будет
прочитано из переменной окружения `VAR_NAME`.

Флаг `--llm` в `chat` перекрывает `llm.provider` из конфига.

## Чат

### Mock LLM (по умолчанию)
```bash
python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.yaml
```
Если конфиг не найден, будет использован моковый адаптер.

### OpenAI / совместимый прокси
1. Скопируйте `examples/demo_qa/demo_qa.toml.example` в удобное место и укажите
   `provider = "openai"`, `llm.openai.api_key` (можно `env:OPENAI_API_KEY`),
   `base_url`, модели и температуры.
2. Запустите чат с указанием конфига:
```bash
python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.yaml --config path/to/demo_qa.toml
```

Флаг `--llm openai` тоже включает OpenAI-профиль, даже если провайдер в конфиге другой.

Флаг `--enable-semantic` строит семантический индекс, если передана модель эмбеддингов.

## Регрессионные кейсы

```bash
python -m examples.demo_qa.cli run-cases --data demo_data --schema demo_data/schema.yaml
```

Прогон использует моковый LLM и вычисляет ожидания напрямую через pandas, поэтому не требует сети.

## Local proxy

Для OpenAI-совместимых серверов (например, LM Studio) укажите `base_url` с `.../v1` и
любым ключом доступа, если прокси не проверяет его. Запуск:

```bash
python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.yaml --config path/to/demo_qa.toml
```

Большинство OpenAI-совместимых сервисов ожидают конечную точку `/v1` в `base_url`.
