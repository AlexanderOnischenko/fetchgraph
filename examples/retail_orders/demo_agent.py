# examples/retail_orders/demo_agent.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from fetchgraph.core import create_generic_agent, TaskProfile
from fetchgraph.protocols import LLMInvoke
from fetchgraph.relational.schema import SchemaConfig  # только для типа, не обязательно

from .schema import build_retail_provider, RETAIL_SCHEMA


# Простейшая заглушка LLM, чтобы пример запускался без внешних зависимостей.
# В реальном проекте сюда подставляется настоящий LLMInvoke (OpenAI, GigaChat и т.д.).
def dummy_llm(prompt: str, sender: str) -> str:
    print(f"\n----- LLM ({sender}) prompt preview -----\n{prompt[:800]}\n----- end prompt -----\n")
    # Возвращаем заведомо неправильный план/ответ — это только демонстрация интеграции.
    return '{"required_context": [], "context_plan": [], "adr_queries": [], "constraints": []}'


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

    # 3) Собираем агента (используем dummy_llm, чтобы показать wiring)
    task_profile = TaskProfile(
        task_name="Аналитика розничных заказов",
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
        llm_invoke=dummy_llm,                # в реальном сценарии сюда приходит настоящий LLMInvoke
        providers={"retail_orders": retail_provider},
        saver=lambda feature, parsed: print(f"\n[SAVER] {feature} => {parsed}"),
        task_profile=task_profile,
    )

    # 4) Пробуем прогнать один запрос (feature_name ~= запрос пользователя)
    question = "Какие товары чаще всего покупают клиенты сегмента b2b?"
    result = agent.run(question)
    print("\n=== Agent result ===")
    print(result)


if __name__ == "__main__":
    main()
