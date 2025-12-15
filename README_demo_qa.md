# Demo QA harness

Пример интерактивного стенда для fetchgraph. Расположен в `examples/demo_qa` и не входит в пакетную сборку.

## Генерация данных

```bash
python -m examples.demo_qa.cli gen --out demo_data --rows 1000 --seed 42
```

Команда создаст четыре CSV, `schema.yaml`, `meta.json` и `stats.json`.

## Чат

### Mock LLM
```bash
python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.yaml --llm mock
```

### OpenAI
```bash
OPENAI_API_KEY=... python -m examples.demo_qa.cli chat --data demo_data --schema demo_data/schema.yaml --llm openai
```

Флаг `--enable-semantic` строит семантический индекс, если передана модель эмбеддингов.

## Регрессионные кейсы

```bash
python -m examples.demo_qa.cli run-cases --data demo_data --schema demo_data/schema.yaml
```

Прогон использует моковый LLM и вычисляет ожидания напрямую через pandas, поэтому не требует сети.
