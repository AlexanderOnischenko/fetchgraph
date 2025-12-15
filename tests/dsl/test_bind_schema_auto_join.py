from fetchgraph.dsl import Clause, NormalizedQuerySketch, SchemaRegistry, WhereExpr, bind_query_sketch
from fetchgraph.dsl.resolution_policy import ResolutionPolicy
from fetchgraph.relational.models import ColumnDescriptor, EntityDescriptor, RelationDescriptor, RelationJoin


def make_registry(
    include_root_field: bool = False,
    include_env_direct: bool = False,
    include_as_name: bool = False,
) -> SchemaRegistry:
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
            columns=[
                ColumnDescriptor(name="id"),
                ColumnDescriptor(name="system_name"),
                *( [ColumnDescriptor(name="name")] if include_as_name else []),
            ],
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

    if include_env_direct:
        relations.append(
            RelationDescriptor(
                name="fbs_env",
                from_entity="fbs",
                to_entity="env",
                join=RelationJoin(from_entity="fbs", from_column="id", to_entity="env", to_column="id"),
            )
        )

    return SchemaRegistry(entities, relations)


def make_sketch(path: str) -> NormalizedQuerySketch:
    return NormalizedQuerySketch(
        from_="fbs",
        where=WhereExpr(all=[Clause(path=path, op="contains", value="ЕСП")]),
        get=["*"],
        with_=[],
        take=10,
    )


def test_auto_join_for_unqualified_field():
    registry = make_registry(include_root_field=False)
    sketch = make_sketch("system_name")

    bound, diags = bind_query_sketch(sketch, registry, ResolutionPolicy())

    assert not diags.has_errors()
    assert bound.with_ == ["fbs_as"]
    clause = bound.where.all[0]
    assert clause.field.raw == "fbs_as.system_name"


def test_multi_hop_auto_join_adds_all_relations():
    registry = make_registry(include_root_field=False)
    sketch = NormalizedQuerySketch(
        from_="fbs",
        where=WhereExpr(all=[Clause(path="name", op="is", value="prod")]),
        get=["*"],
        with_=[],
        take=5,
    )

    bound, diags = bind_query_sketch(sketch, registry, ResolutionPolicy())

    assert not diags.has_errors()
    assert bound.with_ == ["fbs_as", "as_env"]
    clause = bound.where.all[0]
    assert clause.field.raw == "as_env.name"


def test_ambiguity_with_ask_strategy_reports_error():
    registry = make_registry(
        include_root_field=False, include_env_direct=True, include_as_name=True
    )
    sketch = make_sketch("name")

    _, diags = bind_query_sketch(sketch, registry, ResolutionPolicy(ambiguity_strategy="ask"))

    assert diags.has_errors()
    assert any(err.code == "DSL_BIND_AMBIGUOUS_FIELD" for err in diags.errors())
