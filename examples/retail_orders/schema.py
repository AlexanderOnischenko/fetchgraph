# examples/retail_orders/schema.py
from __future__ import annotations

from pathlib import Path

from fetchgraph.relational.schema import (
    ColumnConfig,
    EntityConfig,
    RelationConfig,
    SchemaConfig,
    build_pandas_provider_from_schema,
)

RETAIL_SCHEMA = SchemaConfig(
    name="retail_orders",
    label="Интернет-магазин: клиенты, заказы и товары",
    description=(
        "Домен интернет-магазина: клиенты, товары, заказы и позиции заказов. "
        "Подходит для вопросов вида «какие товары чаще всего покупают», "
        "«какие клиенты приносят больше выручки», «детали конкретного заказа»."
    ),
    planning_hints=[
        "Для обзора схемы сначала запроси op=\"schema\".",
        "Для текстового поиска по названиям товаров или именам клиентов используй op=\"semantic_only\".",
        "Для табличных запросов используй op=\"query\" с root_entity и relations.",
        "Аггрегаты (суммы и количество) делай через group_by и aggregations в RelationalQuery.",
    ],
    entities=[
        # Клиенты
        EntityConfig(
            name="customers",
            label="Клиент",
            source="customers.csv",
            columns=[
                ColumnConfig("customer_id", type="int", pk=True),
                ColumnConfig("name", semantic=True),
                ColumnConfig("email"),
                ColumnConfig("segment"),
            ],
            semantic_text_fields=["name"],
            planning_hint="Справочник клиентов. Основной ключ customer_id. "
                          "Поле segment можно использовать для группировок.",
        ),
        # Товары
        EntityConfig(
            name="products",
            label="Товар",
            source="products.csv",
            columns=[
                ColumnConfig("product_id", type="int", pk=True),
                ColumnConfig("name", semantic=True),
                ColumnConfig("category"),
                ColumnConfig("price", type="float"),
            ],
            semantic_text_fields=["name"],
            planning_hint="Справочник товаров. Используй category и price для фильтров и аггрегатов.",
        ),
        # Заказы (шапка)
        EntityConfig(
            name="orders",
            label="Заказ",
            source="orders.csv",
            columns=[
                ColumnConfig("order_id", type="int", pk=True),
                ColumnConfig("customer_id", type="int"),
                ColumnConfig("order_date"),
                ColumnConfig("status"),
                ColumnConfig("total_amount", type="float"),
            ],
            planning_hint="Шапка заказа. Связан с customers по customer_id.",
        ),
        # Позиции заказа
        EntityConfig(
            name="order_items",
            label="Позиция заказа",
            source="order_items.csv",
            columns=[
                ColumnConfig("order_item_id", type="int", pk=True),
                ColumnConfig("order_id", type="int"),
                ColumnConfig("product_id", type="int"),
                ColumnConfig("quantity", type="int"),
                ColumnConfig("unit_price", type="float"),
            ],
            planning_hint="Строки заказов, связывают orders и products.",
        ),
    ],
    relations=[
        # Клиент -> заказы
        RelationConfig(
            name="customer_orders",
            from_entity="customers",
            from_column="customer_id",
            to_entity="orders",
            to_column="customer_id",
            cardinality="1_to_many",
            semantic_hint="какие заказы оформил клиент",
            planning_hint="Используй, чтобы получить все заказы клиента или агрегировать заказы по клиентам.",
        ),
        # Заказ -> позиции
        RelationConfig(
            name="order_items_rel",
            from_entity="orders",
            from_column="order_id",
            to_entity="order_items",
            to_column="order_id",
            cardinality="1_to_many",
            semantic_hint="какие товары входят в заказ",
            planning_hint="Связь для проваливания из заказов в отдельные позиции.",
        ),
        # Товар -> позиции (обратная связь many_to_1 в другом направлении)
        RelationConfig(
            name="product_items",
            from_entity="products",
            from_column="product_id",
            to_entity="order_items",
            to_column="product_id",
            cardinality="1_to_many",
            semantic_hint="в каких позициях заказов встречается товар",
            planning_hint="Используй для аналитики спроса по товарам.",
        ),
    ],
    # Можно явно подсказать планировщику примеры селекторов (иначе он сгенерит авто)
    examples=[
        # 1) Схема
        '{"op": "schema"}',
        # 2) Простая выборка клиентов
        '{"op": "query", "root_entity": "customers", "select": [{"expr": "customers.customer_id"}, {"expr": "customers.name"}], "limit": 5}',
        # 3) Семантический поиск по товарам
        '{"op": "semantic_only", "entity": "products", "query": "набор для кофе", "mode": "filter", "top_k": 20}',
        # 4) Заказы с деталями клиентов
        '{"op": "query", "root_entity": "orders", "relations": ["customer_orders"], "limit": 20}',
    ],
)


def build_retail_provider(data_dir: str | Path):
    """
    Высокоуровневый билдер: из RETAIL_SCHEMA собирает PandasRelationalDataProvider.
    """
    return build_pandas_provider_from_schema(data_dir, RETAIL_SCHEMA)
