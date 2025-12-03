from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from fetchgraph.core import TaskProfile, create_generic_agent
from fetchgraph.relational_schema import build_sql_provider_from_schema

from .schema import RETAIL_SCHEMA


# Простейшая заглушка LLM, чтобы пример запускался без внешних зависимостей.
# В реальном проекте сюда подставляется настоящий LLMInvoke (OpenAI, GigaChat и т.д.).
def dummy_llm(prompt: str, *, sender: str) -> str:
    print(f"\n----- LLM ({sender}) prompt preview -----\n{prompt[:800]}\n----- end prompt -----\n")
    return '{"required_context": [], "context_plan": [], "adr_queries": [], "constraints": []}'


def _load_csv_into_sql(engine, data_dir: Path) -> None:
    with engine.begin() as conn:
        for ent in RETAIL_SCHEMA.entities:
            if not ent.source:
                continue
            csv_path = data_dir / ent.source
            df = pd.read_csv(csv_path)
            table_name = Path(ent.source).stem
            df.to_sql(table_name, conn, if_exists="replace", index=False)


def main() -> None:
    base_dir = Path(__file__).parent
    data_dir = base_dir / "data"

    # 1) Создаём SQL-движок и загружаем туда те же CSV, что использует Pandas-демо
    engine = create_engine("sqlite:///:memory:")
    _load_csv_into_sql(engine, data_dir)

    # 2) Собираем провайдер SQL на основе той же SchemaConfig
    retail_provider = build_sql_provider_from_schema(engine, RETAIL_SCHEMA)

    # 3) Показываем описание провайдера (идентично Pandas-варианту)
    info = retail_provider.describe()
    print("=== ProviderInfo.name ===")
    print(info.name)
    print("\n=== ProviderInfo.description ===")
    print(info.description)
    print("\n=== ProviderInfo.capabilities ===")
    print(info.capabilities)
    print("\n=== ProviderInfo.examples ===")
    for ex in info.examples or []:
        print("-", ex)

    # 4) Собираем агента и прогоняем тестовый вопрос
    task_profile = TaskProfile(
        task_name="Аналитика розничных заказов (SQL)",
        goal=(
            "Отвечать на вопросы аналитика по данным интернет-магазина: "
            "кто что покупает, какие товары популярны, как распределяется выручка."
        ),
        output_format="Текстовое объяснение на русском + по возможности простая таблица в Markdown.",
        acceptance_criteria=[
            "Ответ опирается на факты из контекста провайдера retail_orders.",
            "При аггрегатах явно указаны фильтры, период и единицы измерения.",
        ],
        constraints=[
            "Не придумывать несуществующие поля и сущности, использовать только описанные в схеме.",
        ],
        focus_hints=[
            "Если вопрос про популярность товаров, используй order_items и products.",
            "Если вопрос про эффективность клиентов, используй customers и orders.",
        ],
    )

    agent = create_generic_agent(
        llm_invoke=dummy_llm,
        providers={"retail_orders": retail_provider},
        saver=lambda feature, parsed: print(f"\n[SAVER] {feature} => {parsed}"),
        task_profile=task_profile,
    )

    question = "Какие товары чаще всего покупают клиенты сегмента b2b?"
    result = agent.run(question)
    print("\n=== Agent result ===")
    print(result)


if __name__ == "__main__":
    main()
