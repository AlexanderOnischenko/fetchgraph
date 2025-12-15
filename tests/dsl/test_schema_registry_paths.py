from fetchgraph.dsl import SchemaRegistry
from fetchgraph.relational.models import ColumnDescriptor, EntityDescriptor, RelationDescriptor, RelationJoin


def make_registry() -> SchemaRegistry:
    fbs = EntityDescriptor(
        name="fbs",
        columns=[ColumnDescriptor(name="id"), ColumnDescriptor(name="name")],
    )
    as_ = EntityDescriptor(
        name="as",
        columns=[ColumnDescriptor(name="id"), ColumnDescriptor(name="system_name")],
    )
    env = EntityDescriptor(
        name="env",
        columns=[ColumnDescriptor(name="id"), ColumnDescriptor(name="name")],
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


def test_root_to_root_returns_empty_path():
    registry = make_registry()
    assert registry.find_paths("fbs", "fbs", max_depth=0) == [()]


def test_paths_are_bidirectional_and_deterministic():
    registry = make_registry()

    forward = registry.find_paths("fbs", "as", max_depth=1)
    backward = registry.find_paths("as", "fbs", max_depth=1)

    assert forward == [("fbs_as",)]
    assert backward == [("fbs_as",)]

    assert forward == registry.find_paths("fbs", "as", max_depth=1)
    assert backward == registry.find_paths("as", "fbs", max_depth=1)


def test_two_hop_paths_ordered():
    registry = make_registry()
    paths = registry.find_paths("fbs", "env", max_depth=2)
    assert paths == [("fbs_as", "as_env")]
    assert paths == registry.find_paths("fbs", "env", max_depth=2)
