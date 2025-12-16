from __future__ import annotations

import json
from typing import Any, Dict

import pytest

from fetchgraph.core.catalog import (
    MAX_PROVIDER_BLOCK_CHARS,
    MAX_PROVIDERS_CATALOG_CHARS,
)
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
from fetchgraph.relational.schema import (
    ColumnConfig,
    EntityConfig,
    RelationConfig,
    SchemaConfig,
)


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
    assert "planning_hints:" not in catalog  # no hints configured


def test_provider_catalog_includes_simple_schema_summary():
    class SimpleSchemaProvider:
        name = "simple"
        entities = []
        relations = []

        def describe(self) -> ProviderInfo:
            return ProviderInfo(
                name=self.name,
                selectors_schema={
                    "type": "object",
                    "properties": {
                        "op": {"enum": ["simple"]},
                        "payload": {"type": "object"},
                    },
                    "required": ["op", "payload"],
                },
            )

    catalog = provider_catalog_text({"simple": SimpleSchemaProvider()})
    assert "selectors_schema_summary" in catalog
    assert "\"op\"" in catalog
    assert "\"payload\"" in catalog


def test_provider_catalog_uses_compact_digest_and_prefers_dsl_note():
    provider = MiniRelProvider()

    catalog = provider_catalog_text({"mini": provider})

    assert "preferred_selectors" not in catalog
    assert "selector_dialects" not in catalog
    assert "selectors_digest" not in catalog
    assert "selectors_schema" not in catalog


def test_provider_catalog_truncation_enforces_cap():
    class LongCatalogProvider(DummyCompactProvider):
        def __init__(self, idx: int):
            self.idx = idx

        def describe(self) -> ProviderInfo:
            info = super().describe()
            info.name = f"dummy_{self.idx}"
            info.description = "x" * (MAX_PROVIDER_BLOCK_CHARS * 2)
            return info

    providers = {f"p{i}": LongCatalogProvider(i) for i in range(5)}

    catalog = provider_catalog_text(providers)

    assert len(catalog) <= MAX_PROVIDERS_CATALOG_CHARS
    assert "truncated" not in catalog


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

    relation_example = None
    for ex in info.examples:
        data = json.loads(ex)
        if data.get("op") == "query" and data.get("relations"):
            relation_example = data
            break

    assert relation_example is not None
    assert relation_example["relations"] == ["order_customer"]
    assert relation_example["filters"]["entity"] == "customer"
    assert relation_example["filters"]["op"] == "ilike"
    assert "select" in relation_example

    compiled = compile_selectors(
        provider,
        {
            "$dsl": QUERY_SKETCH_DSL_ID,
            "payload": {"from": "customer", "where": [["name", "Marketing"]], "take": 1},
        },
    )
    assert compiled.get("op") == "query"


def test_selector_dialect_envelope_example_includes_get_and_where():
    provider = MiniRelProvider()
    info = provider.describe()

    dialect = info.selector_dialects[0]
    payload = json.loads(dialect.envelope_example)["payload"]

    assert payload.get("get")
    assert payload.get("where") and len(payload["where"][0]) == 3
    assert payload.get("with")


def test_relational_describe_digest_contains_ops_and_rules():
    provider = MiniRelProvider()
    info = provider.describe()

    digest = info.selectors_digest
    assert digest["ops"]["query"]["required"]
    assert digest["rules"]["filters"]["comparison_ops"]
    assert digest["rules"]["aggregations"]["ops"]
    assert digest["entities"]["preview"][0]["columns_preview"]["preview"]
    assert digest["relations"]["preview"]


def test_provider_catalog_shows_planning_hints_and_no_inline_truncation():
    provider = MiniRelProvider()
    schema_config = SchemaConfig(
        name="fbsem",
        planning_hints=["Use schema op first"],
        entities=[
            EntityConfig(
                name="customer",
                label="Customer",
                columns=[ColumnConfig(name="id", pk=True), ColumnConfig(name="name")],
                planning_hint="Customer master data",
            )
        ],
        relations=[
            RelationConfig(
                name="order_customer",
                from_entity="order",
                from_column="customer_id",
                to_entity="customer",
                to_column="id",
                cardinality="many_to_1",
                planning_hint="Join orders to customers",
            )
        ],
    )
    provider._schema_config = schema_config  # type: ignore[attr-defined]

    catalog = provider_catalog_text({"fbsem": provider})

    assert "planning_hints:" in catalog
    assert "Use schema op first" in catalog
    assert "Customer master data" in catalog
    assert "Join orders to customers" in catalog
    assert "truncated" not in catalog


def test_catalog_order_preserves_examples_and_dialects_under_truncation():
    class VerboseProvider(DummyCompactProvider):
        def describe(self) -> ProviderInfo:
            info = super().describe()
            info.description = "desc " * 5000
            info.examples = [json.dumps({"op": "schema"})]
            return info

    catalog = provider_catalog_text({"verbose": VerboseProvider()})
    assert "examples" in catalog
    assert "selector_dialects" not in catalog


def test_entities_preview_always_contains_pk_and_semantic_fields():
    class PkLateProvider(MiniRelProvider):
        def __init__(self):
            extra_columns = [ColumnDescriptor(name=f"col_{i}") for i in range(30)]
            semantic_column = ColumnDescriptor(name="meaningful_text", semantic=True)
            pk_column = ColumnDescriptor(name="late_pk", role="primary_key")
            entity = EntityDescriptor(
                name="wide", columns=extra_columns + [semantic_column, pk_column]
            )
            relations = []
            RelationalDataProvider.__init__(
                self, name="late", entities=[entity], relations=relations
            )

    provider = PkLateProvider()
    digest = provider.describe().selectors_digest
    preview_cols = digest["entities"]["preview"][0]["columns_preview"]["preview"]
    assert "late_pk" in preview_cols
    assert "meaningful_text" in preview_cols
