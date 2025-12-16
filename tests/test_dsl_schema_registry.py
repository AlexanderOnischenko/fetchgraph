from fetchgraph.dsl import FieldCandidate, SchemaRegistry
from fetchgraph.relational.models import ColumnDescriptor, EntityDescriptor, RelationDescriptor, RelationJoin


def build_registry() -> SchemaRegistry:
    fbs = EntityDescriptor(
        name="fbs",
        columns=[
            ColumnDescriptor(name="id", type="uuid"),
            ColumnDescriptor(name="bc_name", type="text"),
        ],
    )
    as_ = EntityDescriptor(
        name="as",
        columns=[
            ColumnDescriptor(name="id", type="uuid"),
            ColumnDescriptor(name="system_name", type="text"),
        ],
    )
    env = EntityDescriptor(
        name="env",
        columns=[
            ColumnDescriptor(name="id", type="uuid"),
            ColumnDescriptor(name="name", type="text"),
        ],
    )

    fbs_as = RelationDescriptor(
        name="fbs_as",
        from_entity="fbs",
        to_entity="as",
        join=RelationJoin(from_entity="fbs", from_column="id", to_entity="as", to_column="id"),
    )
    as_env = RelationDescriptor(
        name="as_env",
        from_entity="as",
        to_entity="env",
        join=RelationJoin(from_entity="as", from_column="id", to_entity="env", to_column="id"),
    )

    return SchemaRegistry(entities=[fbs, as_, env], relations=[fbs_as, as_env])


def test_find_entities_with_field():
    registry = build_registry()
    assert registry.find_entities_with_field("system_name") == ["as"]


def test_find_paths_direct_and_limited_depth():
    registry = build_registry()
    assert registry.find_paths("fbs", "as", max_depth=1) == [("fbs_as",)]
    assert registry.find_paths("fbs", "env", max_depth=1) == []


def test_find_paths_two_hop():
    registry = build_registry()
    assert registry.find_paths("fbs", "env", max_depth=2) == [("fbs_as", "as_env")]


def test_field_candidates_deterministic_and_paths():
    registry = build_registry()
    candidates_first = registry.field_candidates(root="fbs", field="system_name", max_depth=2)
    candidates_second = registry.field_candidates(root="fbs", field="system_name", max_depth=2)

    assert candidates_first == candidates_second
    assert len(candidates_first) == 1

    candidate = candidates_first[0]
    assert candidate.entity == "as"
    assert candidate.field == "system_name"
    assert candidate.field_type == "text"
    assert candidate.join_path == ("fbs_as",)
