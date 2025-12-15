from __future__ import annotations

import json
from typing import Any, Dict

import pytest

from fetchgraph.core.catalog import MAX_PROVIDERS_CATALOG_CHARS
from fetchgraph.core.context import provider_catalog_text
from fetchgraph.core.models import ProviderInfo, SelectorDialectInfo
from fetchgraph.core.selector_dialects import compile_selectors, QUERY_SKETCH_DSL_ID
from fetchgraph.relational.models import (
    ColumnDescriptor,
    EntityDescriptor,
    RelationDescriptor,
    RelationJoin,
    SchemaRequest,
    SemanticOnlyRequest,
    RelationalQuery,
)
from fetchgraph.relational.providers.base import RelationalDataProvider


class DummyCompactProvider:
    name = "dummy"
    entities = []
    relations = []

    def describe(self) -> ProviderInfo:
        huge_schema: Dict[str, Any] = {
            "oneOf": [
                {
                    "properties": {
                        "op": {"const": "huge"},
                        "payload": {"enum": [f"v{i}" for i in range(50)]},
                    },
                    "required": ["op"],
                    "$defs": {"noise": ["x" * 100 for _ in range(10)]},
                }
            ],
            "$defs": {"more": ["y" * 100 for _ in range(10)]},
        }

        return ProviderInfo(
            name="dummy",
            description="A provider with a huge schema that should be summarized",
            capabilities=["test"],
            selector_dialects=[
                SelectorDialectInfo(
                    id="dummy@v1",
                    description="dummy dialect",
                    envelope_example=json.dumps({"$dsl": "dummy@v1", "payload": {}}),
                )
            ],
            selectors_schema=huge_schema,
        )


class MiniRelProvider(RelationalDataProvider):
    def __init__(self):
        entities = [
            EntityDescriptor(
                name="customer",
                columns=[
                    ColumnDescriptor(name="id", role="primary_key"),
                    ColumnDescriptor(name="name", semantic=True),
                ],
            ),
            EntityDescriptor(
                name="order",
                columns=[
                    ColumnDescriptor(name="id", role="primary_key"),
                    ColumnDescriptor(name="customer_id", role="foreign_key"),
                ],
            ),
        ]
        relations = [
            RelationDescriptor(
                name="order_customer",
                from_entity="order",
                to_entity="customer",
                join=RelationJoin(
                    from_entity="order",
                    from_column="customer_id",
                    to_entity="customer",
                    to_column="id",
                ),
            )
        ]
        super().__init__(name="mini_rel", entities=entities, relations=relations)

    def _handle_schema(self):  # pragma: no cover - not used in tests
        raise NotImplementedError

    def _handle_semantic_only(self, req):  # pragma: no cover - not used in tests
        raise NotImplementedError

    def _handle_query(self, req):  # pragma: no cover - not used in tests
        raise NotImplementedError


def test_provider_catalog_compact_limits_and_order():
    providers = {"dummy": DummyCompactProvider()}

    catalog = provider_catalog_text(providers)

    assert "$defs" not in catalog
    assert len(catalog) <= MAX_PROVIDERS_CATALOG_CHARS
    selector_idx = catalog.index("selector_dialects:")
    summary_idx = catalog.index("selectors_schema_summary")
    assert selector_idx < summary_idx


def test_relational_examples_validate_and_dsl_compiles():
    provider = MiniRelProvider()
    info = provider.describe()

    for ex in info.examples:
        data = json.loads(ex)
        op = data.get("op")
        if op == "schema":
            SchemaRequest.model_validate(data)
        elif op == "semantic_only":
            SemanticOnlyRequest.model_validate(data)
        elif op == "query":
            RelationalQuery.model_validate(data)
        else:
            pytest.fail(f"Unknown op in example: {op}")

    compiled = compile_selectors(
        provider,
        {
            "$dsl": QUERY_SKETCH_DSL_ID,
            "payload": {"from": "customer", "where": [["name", "Marketing"]], "take": 1},
        },
    )
    assert compiled.get("op") == "query"
