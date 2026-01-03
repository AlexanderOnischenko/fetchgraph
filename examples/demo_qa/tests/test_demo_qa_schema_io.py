from __future__ import annotations

from pathlib import Path

from examples.demo_qa.data_gen import generate_and_save
from examples.demo_qa.schema_io import load_schema


def test_generate_and_load_schema_json(tmp_path: Path) -> None:
    out_dir = tmp_path / "demo_data"
    generate_and_save(out_dir, rows=5, seed=123)

    schema_path = out_dir / "schema.json"
    assert schema_path.exists()

    schema = load_schema(schema_path)

    assert schema.name == "demo_qa"
    assert {e.name for e in schema.entities} >= {"customers", "products", "orders", "order_items"}
    assert {r.name for r in schema.relations} >= {"orders_to_customers", "items_to_orders", "items_to_products"}
