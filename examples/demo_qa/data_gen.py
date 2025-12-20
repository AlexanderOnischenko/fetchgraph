from __future__ import annotations

import json
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd

from fetchgraph.relational.schema import ColumnConfig, EntityConfig, RelationConfig, SchemaConfig


@dataclass
class GeneratedDataset:
    customers: pd.DataFrame
    products: pd.DataFrame
    orders: pd.DataFrame
    order_items: pd.DataFrame


CITY_CHOICES = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Philadelphia",
    "Phoenix",
    "San Antonio",
    "San Diego",
]
SEGMENTS = ["consumer", "corporate", "home_office"]
PRODUCT_CATEGORIES = [
    "electronics",
    "furniture",
    "office_supplies",
    "toys",
    "outdoors",
    "books",
]
CHANNELS = ["online", "retail", "partner", "phone"]
ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]


# --------------------------- generation ---------------------------


def _random_date(start: datetime, end: datetime, rng: random.Random) -> datetime:
    delta = end - start
    seconds = rng.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def generate_dataset(rows: int = 1000, seed: int | None = None) -> GeneratedDataset:
    rng = random.Random(seed)

    customers = pd.DataFrame(
        {
            "customer_id": range(1, rows + 1),
            "name": [f"Customer {i}" for i in range(1, rows + 1)],
            "city": [rng.choice(CITY_CHOICES) for _ in range(rows)],
            "segment": [rng.choice(SEGMENTS) for _ in range(rows)],
            "signup_date": [
                _random_date(datetime(2021, 1, 1), datetime(2024, 1, 1), rng).date()
                for _ in range(rows)
            ],
        }
    )

    product_rows = max(200, rows // 3)
    products = pd.DataFrame(
        {
            "product_id": range(1, product_rows + 1),
            "name": [f"Product {i}" for i in range(1, product_rows + 1)],
            "category": [rng.choice(PRODUCT_CATEGORIES) for _ in range(product_rows)],
            "price": [round(rng.uniform(5, 500), 2) for _ in range(product_rows)],
            "in_stock": [rng.randint(0, 500) for _ in range(product_rows)],
        }
    )

    orders = []
    for oid in range(1, rows + 1):
        customer_id = rng.randint(1, rows)
        order_date = _random_date(datetime(2022, 1, 1), datetime(2024, 6, 1), rng)
        channel = rng.choice(CHANNELS)
        status = rng.choice(ORDER_STATUSES)
        orders.append(
            {
                "order_id": oid,
                "customer_id": customer_id,
                "order_date": order_date.date(),
                "status": status,
                "channel": channel,
                "order_total": 0.0,  # filled later
            }
        )
    orders_df = pd.DataFrame(orders)

    order_items_records: List[Dict[str, object]] = []
    for order in orders:
        item_count = rng.randint(1, 4)
        total = 0.0
        for _ in range(item_count):
            product_id = rng.randint(1, product_rows)
            quantity = rng.randint(1, 5)
            unit_price = float(products.loc[product_id - 1, "price"])
            line_total = round(unit_price * quantity, 2)
            total += line_total
            order_items_records.append(
                {
                    "order_item_id": len(order_items_records) + 1,
                    "order_id": order["order_id"],
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )
        orders_df.loc[orders_df["order_id"] == order["order_id"], "order_total"] = round(total, 2)

    order_items_df = pd.DataFrame(order_items_records)

    return GeneratedDataset(
        customers=customers,
        products=products,
        orders=orders_df,
        order_items=order_items_df,
    )


# --------------------------- schema ---------------------------


def default_schema(enable_semantic: bool = False) -> SchemaConfig:
    semantic_fields = {
        "customers": ["name", "city"] if enable_semantic else [],
        "products": ["name", "category"] if enable_semantic else [],
        "orders": [] if not enable_semantic else ["status", "channel"],
        "order_items": [],
    }

    entities = [
        EntityConfig(
            name="customers",
            label="Customers",
            source="customers.csv",
            semantic_text_fields=semantic_fields["customers"],
            planning_hint="Customer demographics and signup information.",
            columns=[
                ColumnConfig("customer_id", type="int", pk=True),
                ColumnConfig("name", type="text"),
                ColumnConfig("city", type="text"),
                ColumnConfig("segment", type="text"),
                ColumnConfig("signup_date", type="date"),
            ],
        ),
        EntityConfig(
            name="products",
            label="Products",
            source="products.csv",
            semantic_text_fields=semantic_fields["products"],
            planning_hint="Catalog of products and pricing.",
            columns=[
                ColumnConfig("product_id", type="int", pk=True),
                ColumnConfig("name", type="text"),
                ColumnConfig("category", type="text"),
                ColumnConfig("price", type="float"),
                ColumnConfig("in_stock", type="int"),
            ],
        ),
        EntityConfig(
            name="orders",
            label="Orders",
            source="orders.csv",
            semantic_text_fields=semantic_fields["orders"],
            planning_hint="Orders placed by customers across channels.",
            columns=[
                ColumnConfig("order_id", type="int", pk=True),
                ColumnConfig("customer_id", type="int"),
                ColumnConfig("order_date", type="date"),
                ColumnConfig("status", type="text"),
                ColumnConfig("channel", type="text"),
                ColumnConfig("order_total", type="float"),
            ],
        ),
        EntityConfig(
            name="order_items",
            label="Order Items",
            source="order_items.csv",
            semantic_text_fields=semantic_fields["order_items"],
            planning_hint="Line items for each order.",
            columns=[
                ColumnConfig("order_item_id", type="int", pk=True),
                ColumnConfig("order_id", type="int"),
                ColumnConfig("product_id", type="int"),
                ColumnConfig("quantity", type="int"),
                ColumnConfig("unit_price", type="float"),
                ColumnConfig("line_total", type="float"),
            ],
        ),
    ]

    relations = [
        RelationConfig(
            name="orders_to_customers",
            from_entity="orders",
            from_column="customer_id",
            to_entity="customers",
            to_column="customer_id",
            cardinality="many_to_1",
            planning_hint="Orders reference the customer placing them.",
        ),
        RelationConfig(
            name="items_to_orders",
            from_entity="order_items",
            from_column="order_id",
            to_entity="orders",
            to_column="order_id",
            cardinality="many_to_1",
            planning_hint="Order items belong to a single order.",
        ),
        RelationConfig(
            name="items_to_products",
            from_entity="order_items",
            from_column="product_id",
            to_entity="products",
            to_column="product_id",
            cardinality="many_to_1",
            planning_hint="Order items reference a product.",
        ),
    ]

    return SchemaConfig(
        name="demo_qa",
        label="Demo QA",
        description="Synthetic commerce dataset for fetchgraph demo QA.",
        planning_hints=[
            "Prefer aggregates instead of returning raw rows.",
            "Use joins only when relevant to the question.",
        ],
        entities=entities,
        relations=relations,
    )


# --------------------------- persistence ---------------------------


def save_dataset(dataset: GeneratedDataset, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    dataset.customers.to_csv(out_dir / "customers.csv", index=False)
    dataset.products.to_csv(out_dir / "products.csv", index=False)
    dataset.orders.to_csv(out_dir / "orders.csv", index=False)
    dataset.order_items.to_csv(out_dir / "order_items.csv", index=False)


def save_schema(schema: SchemaConfig, path: Path) -> None:
    schema_dict = asdict(schema)
    with path.open("w", encoding="utf-8") as f:
        json.dump(schema_dict, f, ensure_ascii=False, indent=2)


@dataclass
class MetaInfo:
    seed: int | None
    rows: int
    created_at: str
    version: str = "1.0"


def write_meta(meta_path: Path, meta: MetaInfo) -> None:
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(asdict(meta), f, indent=2)


# --------------------------- validation ---------------------------


def validate_dataset(dataset: GeneratedDataset, expected_rows: int) -> None:
    assert len(dataset.customers) == expected_rows, "Unexpected customer count"
    assert len(dataset.orders) == expected_rows, "Unexpected order count"
    assert dataset.orders["customer_id"].isin(dataset.customers["customer_id"]).all(), "Broken FK orders.customer_id"
    assert dataset.order_items["order_id"].isin(dataset.orders["order_id"]).all(), "Broken FK order_items.order_id"
    assert dataset.order_items["product_id"].isin(dataset.products["product_id"]).all(), "Broken FK order_items.product_id"


# --------------------------- public API ---------------------------


def generate_and_save(out_dir: Path, *, rows: int = 1000, seed: int | None = None, enable_semantic: bool = False) -> None:
    dataset = generate_dataset(rows=rows, seed=seed)
    validate_dataset(dataset, rows)
    save_dataset(dataset, out_dir)
    schema = default_schema(enable_semantic=enable_semantic)
    save_schema(schema, out_dir / "schema.json")
    meta = MetaInfo(seed=seed, rows=rows, created_at=datetime.utcnow().isoformat())
    write_meta(out_dir / "meta.json", meta)

    # Simple statistics
    stats = {
        "orders": {
            "min_date": str(dataset.orders["order_date"].min()),
            "max_date": str(dataset.orders["order_date"].max()),
            "status_counts": dataset.orders["status"].value_counts().to_dict(),
        }
    }
    with (out_dir / "stats.json").open("w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)


__all__ = [
    "generate_and_save",
    "generate_dataset",
    "default_schema",
]
