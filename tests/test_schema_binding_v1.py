import pytest

from fetchgraph.schema import (
    AmbiguousField,
    ProviderSchema,
    RelationNotFromRoot,
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

    def describe_schema(self) -> object:
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
                label="system",
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


def test_registry_coerces_schema_result(simple_schema):
    class SchemaResult:
        def __init__(self, entities, relations):
            self.entities = entities
            self.relations = relations

    prov = DummyProvider(simple_schema)
    prov.describe_schema = lambda: SchemaResult(simple_schema.entities, simple_schema.relations)
    local_registry = SchemaRegistry()

    schema = local_registry.get_or_describe(prov)

    assert isinstance(schema, ProviderSchema)
    assert prov.describe_calls == 0


def test_registry_uses_provider_key(simple_schema):
    prov = DummyProvider(simple_schema)
    local_registry = SchemaRegistry()

    first = local_registry.get_or_describe(prov, provider_key="fbsem")
    second = local_registry.get_or_describe(prov, provider_key="fbsem")

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


def test_qualified_relation_auto_add(simple_schema):
    selectors = {"op": "query", "root_entity": "fbs", "select": [{"expr": "fbs_as.system_name"}]}
    bound, diag = bind_selectors(simple_schema, selectors)
    assert "fbs_as" in bound.get("relations", [])
    assert bound["select"][0]["expr"] == "fbs_as.system_name"
    assert any(d["kind"] == "auto_add_relation" for d in diag)


def test_entity_name_in_qualified_field_maps_to_relation(simple_schema):
    selectors = {"op": "query", "root_entity": "fbs", "select": [{"expr": "as.system_name"}]}
    bound, diag = bind_selectors(simple_schema, selectors)
    assert bound["select"][0]["expr"] == "fbs_as.system_name"
    assert "fbs_as" in bound.get("relations", [])
    assert any(d["kind"] == "auto_add_relation" for d in diag)


def test_qualifier_label_maps_to_relation(simple_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "relations": ["fbs_as"],
        "select": [{"expr": "system.system_name"}],
    }

    bound, diag = bind_selectors(simple_schema, selectors)

    assert bound["select"][0]["expr"] == "fbs_as.system_name"
    assert any(d["kind"] == "mapped_qualifier_label_to_relation" for d in diag)


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


def test_unknown_qualifier_drop(simple_schema):
    selectors = {"op": "query", "root_entity": "fbs", "select": [{"expr": "unknown.bc_guid"}]}
    bound, diag = bind_selectors(
        simple_schema,
        selectors,
        policy=ResolutionPolicy(unknown_qualifier_strategy="drop"),
    )

    assert bound["select"][0]["expr"] == "fbs.bc_guid"
    assert any(d["kind"] == "ignored_unknown_qualifier" for d in diag)


def test_order_by_binding(simple_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "select": [{"expr": "bc_guid"}],
        "order_by": [{"field": "system_name", "direction": "asc"}],
    }
    bound, diag = bind_selectors(simple_schema, selectors)
    assert bound["order_by"][0]["field"] == "fbs.system_name"
    assert any(d["kind"] == "bound_field" for d in diag)


def test_filters_binding_diagnostics(simple_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "filters": {"type": "comparison", "field": "system_name", "op": "eq", "value": 1},
    }
    bound, diag = bind_selectors(simple_schema, selectors)
    assert bound["filters"]["field"] == "fbs.system_name"
    assert any(d.get("context") == "filters" for d in diag if d.get("kind") == "bound_field")


def test_filter_entity_triggers_relation_auto_add(simple_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "filters": {
            "type": "comparison",
            "entity": "as",
            "field": "extra",
            "op": "eq",
            "value": 1,
        },
    }

    bound, diag = bind_selectors(simple_schema, selectors)

    assert "fbs_as" in bound.get("relations", [])
    assert bound["filters"]["field"] == "fbs_as.extra"
    assert any(d["kind"] == "auto_add_relation" for d in diag)


def test_relation_validation_against_root(simple_schema):
    bad_schema = ProviderSchema(
        entities=simple_schema.entities,
        relations=simple_schema.relations
        + [RelationDescriptor(name="other", from_entity="other_root", to_entity="as")],
    )
    selectors = {"op": "query", "root_entity": "fbs", "relations": ["other"], "select": [{"expr": "extra"}]}
    with pytest.raises(RelationNotFromRoot):
        bind_selectors(bad_schema, selectors)
