import pytest

from fetchgraph.core.selector_dialects import QUERY_SKETCH_DSL_ID, compile_selectors
from fetchgraph.dsl import Clause, NormalizedQuerySketch, SchemaRegistry, WhereExpr, bind_query_sketch
from fetchgraph.dsl.resolution_policy import ResolutionPolicy
from fetchgraph.relational.models import ColumnDescriptor, EntityDescriptor, RelationDescriptor, RelationJoin
from fetchgraph.relational.providers.base import RelationalDataProvider


class DummyProvider(RelationalDataProvider):
    def _handle_schema(self):  # pragma: no cover - not used in these tests
        raise NotImplementedError

    def _handle_semantic_only(self, req):  # pragma: no cover - not used in these tests
        raise NotImplementedError

    def _handle_query(self, req):  # pragma: no cover - not used in these tests
        raise NotImplementedError


def make_entities(*, include_root_system: bool = True):
    return [
        EntityDescriptor(
            name="fbs",
            columns=[
                ColumnDescriptor(name="id"),
                *( [ColumnDescriptor(name="system_name")] if include_root_system else [] ),
                ColumnDescriptor(name="status"),
            ],
        ),
        EntityDescriptor(
            name="as",
            columns=[ColumnDescriptor(name="id"), ColumnDescriptor(name="system_name")],
        ),
        EntityDescriptor(
            name="env",
            columns=[ColumnDescriptor(name="id"), ColumnDescriptor(name="system_name"), ColumnDescriptor(name="name")],
        ),
    ]


def make_relations():
    return [
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
        RelationDescriptor(
            name="fbs_env",
            from_entity="fbs",
            to_entity="env",
            join=RelationJoin(from_entity="fbs", from_column="id", to_entity="env", to_column="id"),
        ),
    ]


def test_auto_join_field_resolution_adds_with_and_rewrites_path():
    entities = make_entities(include_root_system=False)
    relations = [make_relations()[0]]
    provider = DummyProvider("rel", entities, relations)

    selectors = {
        "$dsl": QUERY_SKETCH_DSL_ID,
        "payload": {"from": "fbs", "where": [["system_name", "ЕСП"]], "take": 10},
    }

    compiled = compile_selectors(provider, selectors)

    assert compiled["relations"] == ["fbs_as"]
    assert compiled["filters"]["field"] == "fbs_as.system_name"


def test_root_field_wins_over_joined_field():
    entities = make_entities(include_root_system=True)
    relations = [make_relations()[0]]
    provider = DummyProvider("rel", entities, relations)

    selectors = {
        "$dsl": QUERY_SKETCH_DSL_ID,
        "payload": {"from": "fbs", "where": [["system_name", "ЕСП"]], "take": 10},
    }

    compiled = compile_selectors(provider, selectors)

    assert compiled["relations"] == []
    assert compiled["filters"]["field"] == "system_name"


def test_ambiguous_field_ask_strategy_errors():
    entities = make_entities(include_root_system=False)
    relations = make_relations()
    registry = SchemaRegistry(entities, relations)

    sketch = NormalizedQuerySketch(
        from_="fbs",
        where=WhereExpr(all=[Clause(path="system_name", op="is", value="x")]),
        get=["*"],
        with_=[],
        take=10,
    )

    with pytest.raises(ValueError) as exc:
        _, diags = bind_query_sketch(sketch, registry, ResolutionPolicy(ambiguity_strategy="ask"))
        if diags.has_errors():
            codes = ", ".join(d.code for d in diags.errors())
            raise ValueError(codes)

    assert "DSL_BIND_AMBIGUOUS_FIELD" in str(exc.value)
