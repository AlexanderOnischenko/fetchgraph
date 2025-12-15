import pytest

from fetchgraph.core.selector_dialects import QUERY_SKETCH_DSL_ID, compile_selectors
from fetchgraph.relational.models import ColumnDescriptor, EntityDescriptor, RelationDescriptor, RelationJoin
from fetchgraph.relational.providers.base import RelationalDataProvider


class DummyProvider(RelationalDataProvider):
    def _handle_schema(self):  # pragma: no cover - not used
        raise NotImplementedError

    def _handle_semantic_only(self, req):  # pragma: no cover - not used
        raise NotImplementedError

    def _handle_query(self, req):  # pragma: no cover - not used
        return {"ok": True, "selectors": req.model_dump()}


def make_provider(include_root_field: bool = False) -> DummyProvider:
    entities = [
        EntityDescriptor(
            name="fbs",
            columns=[
                ColumnDescriptor(name="id"),
                *( [ColumnDescriptor(name="system_name")] if include_root_field else []),
            ],
        ),
        EntityDescriptor(
            name="as",
            columns=[ColumnDescriptor(name="id"), ColumnDescriptor(name="system_name")],
        ),
        EntityDescriptor(
            name="env",
            columns=[ColumnDescriptor(name="id"), ColumnDescriptor(name="name")],
        ),
    ]
    relations = [
        RelationDescriptor(
            name="fbs_as",
            from_entity="fbs",
            to_entity="as",
            join=RelationJoin(from_entity="fbs", from_column="id", to_entity="as", to_column="id"),
        ),
        RelationDescriptor(
            name="as_env",
            from_entity="as",
            to_entity="env",
            join=RelationJoin(from_entity="as", from_column="id", to_entity="env", to_column="id"),
        ),
    ]

    return DummyProvider("rel", entities, relations)


def test_query_sketch_compiles_and_adds_relations():
    provider = make_provider()
    selectors = {
        "$dsl": QUERY_SKETCH_DSL_ID,
        "payload": {"from": "fbs", "where": [["system_name", "contains", "ЕСП"]], "take": 10},
    }

    compiled = compile_selectors(provider, selectors)

    assert compiled["op"] == "query"
    assert compiled["relations"] == ["fbs_as"]
    assert compiled["filters"]["field"] == "fbs_as.system_name"


def test_native_and_dsl_envelope_conflict():
    provider = make_provider()
    selectors = {"op": "query", "$dsl": QUERY_SKETCH_DSL_ID}

    with pytest.raises(ValueError):
        compile_selectors(provider, selectors)
