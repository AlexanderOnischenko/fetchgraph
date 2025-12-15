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


def make_entities(include_root_field: bool = False):
    return [
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
            columns=[
                ColumnDescriptor(name="id"),
                ColumnDescriptor(name="system_name"),
                ColumnDescriptor(name="name"),
            ],
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


def test_query_sketch_auto_join_unqualified_field():
    provider = DummyProvider("rel", make_entities(include_root_field=False), [make_relations()[0]])

    selectors = {
        "$dsl": QUERY_SKETCH_DSL_ID,
        "payload": {"from": "fbs", "where": [["system_name", "contains", "ЕСП"]], "take": 10},
    }

    compiled = compile_selectors(provider, selectors)

    assert compiled["relations"] == ["fbs_as"]
    assert compiled["filters"]["field"] == "fbs_as.system_name"


def test_query_sketch_multi_hop_adds_all_relations():
    relations = make_relations()[:2]
    provider = DummyProvider("rel", make_entities(include_root_field=False), relations)

    selectors = {
        "$dsl": QUERY_SKETCH_DSL_ID,
        "payload": {"from": "fbs", "where": [["name", "is", "prod"]], "take": 5},
    }

    compiled = compile_selectors(provider, selectors)

    assert compiled["relations"] == ["fbs_as", "as_env"]
    assert compiled["filters"]["field"] == "as_env.name"


def test_query_sketch_ambiguity_ask_returns_error():
    entities = make_entities(include_root_field=False)
    relations = make_relations()
    registry = SchemaRegistry(entities, relations)
    sketch = NormalizedQuerySketch(
        from_="fbs",
        where=WhereExpr(all=[Clause(path="system_name", op="is", value="x")]),
        get=["*"],
        with_=[],
        take=10,
    )

    _, diags = bind_query_sketch(sketch, registry, ResolutionPolicy(ambiguity_strategy="ask"))

    assert diags.has_errors()
    assert any(err.code == "DSL_BIND_AMBIGUOUS_FIELD" for err in diags.errors())
