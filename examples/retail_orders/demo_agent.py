# examples/retail_orders/demo_agent.py
from __future__ import annotations

from pathlib import Path
from fetchgraph.relational_schema import SchemaConfig  # только для типа, не обязательно

from .schema import build_retail_provider, RETAIL_SCHEMA


def main() -> None:
    base_dir = Path(__file__).parent
    data_dir = base_dir / "data"

    # 1) Собираем провайдер розничной схемы
    retail_provider = build_retail_provider(data_dir)

    # 2) Показываем описание провайдера, которое уйдёт в промпт планирования
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

    # 3) В реальном приложении сюда можно подключить BaseGraphAgent из README,
    #    make_llm_plan_generic/make_llm_synth_generic и настоящий LLM. Этот
    #    пример ограничивается описанием провайдера, чтобы не тянуть лишних
    #    зависимостей.


if __name__ == "__main__":
    main()
