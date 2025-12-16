import pytest

from fetchgraph.schema import (
    AmbiguousField,
    ProviderSchema,
    ResolutionPolicy,
    UnknownField,
    UnknownRelation,
    bind_selectors,
)
from fetchgraph.schema.registry import SchemaRegistry
from fetchgraph.schema.types import EntityDescriptor, FieldDescriptor, RelationDescriptor


class DummyProvider:
    def __init__(self, schema: ProviderSchema):
        self.schema = schema
        self.describe_calls = 0
        self.name = "dummy"

    def describe_schema(self):
        self.describe_calls += 1
        return self.schema


@pytest.fixture
def simple_schema():
    return ProviderSchema(
        entities=[
            EntityDescriptor(
                name="fbs",
                fields=[FieldDescriptor(name="bc_guid"), FieldDescriptor(name="system_name"), FieldDescriptor(name="root_only")],
            ),
            EntityDescriptor(
                name="as",
                fields=[FieldDescriptor(name="system_name"), FieldDescriptor(name="extra")],
            ),
        ],
        relations=[
            RelationDescriptor(name="fbs_as", from_entity="fbs", to_entity="as"),
        ],
    )


def test_registry_caches_describe(simple_schema):
    prov = DummyProvider(simple_schema)
    local_registry = SchemaRegistry()

    first = local_registry.get_or_describe(prov)
    second = local_registry.get_or_describe(prov)

    assert first is second
    assert prov.describe_calls == 1


def test_qualified_field_ok(simple_schema):
    selectors = {"op": "query", "root_entity": "fbs", "select": [{"expr": "fbs.bc_guid"}]}
    bound, diag = bind_selectors(simple_schema, selectors)
    assert bound["select"][0]["expr"] == "fbs.bc_guid"
    assert diag == []


def test_qualified_field_bad_relation(simple_schema):
    selectors = {"op": "query", "root_entity": "fbs", "relations": ["unknown"], "select": [{"expr": "unknown.field"}]}
    with pytest.raises(UnknownRelation):
        bind_selectors(simple_schema, selectors)


def test_unqualified_resolves_root(simple_schema):
    selectors = {"op": "query", "root_entity": "fbs", "select": [{"expr": "bc_guid"}]}
    bound, diag = bind_selectors(simple_schema, selectors)
    assert bound["select"][0]["expr"] == "fbs.bc_guid"
    assert any(d["kind"] == "bound_field" for d in diag)


def test_unqualified_resolves_declared_relation(simple_schema):
    schema = ProviderSchema(
        entities=[
            EntityDescriptor(
                name="fbs",
                fields=[FieldDescriptor(name="bc_guid"), FieldDescriptor(name="root_only")],
            ),
            EntityDescriptor(
                name="as",
                fields=[FieldDescriptor(name="system_name"), FieldDescriptor(name="extra")],
            ),
        ],
        relations=[RelationDescriptor(name="fbs_as", from_entity="fbs", to_entity="as")],
    )
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "relations": ["fbs_as"],
        "select": [{"expr": "system_name"}],
    }
    bound, _ = bind_selectors(schema, selectors)
    assert bound["select"][0]["expr"] == "fbs_as.system_name"


def test_unqualified_triggers_auto_add(simple_schema):
    selectors = {"op": "query", "root_entity": "fbs", "select": [{"expr": "extra"}]}
    bound, diag = bind_selectors(simple_schema, selectors)
    assert "fbs_as" in bound.get("relations", [])
    assert bound["select"][0]["expr"] == "fbs_as.extra"
    assert any(d["kind"] == "auto_add_relation" for d in diag)


def test_ambiguity_ask(simple_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "relations": ["fbs_as"],
        "select": [{"expr": "system_name"}],
    }
    with pytest.raises(AmbiguousField) as exc:
        bind_selectors(simple_schema, selectors)
    assert "fbs.system_name" in exc.value.candidates
    assert "fbs_as.system_name" in exc.value.candidates


def test_ambiguity_best(simple_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "relations": ["fbs_as"],
        "select": [{"expr": "system_name"}],
    }
    bound, diag = bind_selectors(simple_schema, selectors, policy=ResolutionPolicy(ambiguity_strategy="best"))
    assert bound["select"][0]["expr"] == "fbs.system_name"
    assert any(d["kind"] == "ambiguous_field_best_effort" for d in diag)
