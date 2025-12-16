import pytest

from fetchgraph.schema import (
    ProviderSchema,
    ResolutionPolicy,
    UnknownRelation,
    bind_selectors,
)
from fetchgraph.schema.types import EntityDescriptor, FieldDescriptor, RelationDescriptor


@pytest.fixture
def qualifier_schema():
    return ProviderSchema(
        entities=[
            EntityDescriptor(
                name="fbs",
                fields=[
                    FieldDescriptor(name="system_name"),
                ],
            ),
            EntityDescriptor(
                name="as",
                label="system",
                fields=[FieldDescriptor(name="system_name")],
            ),
        ],
        relations=[RelationDescriptor(name="fbs_as", from_entity="fbs", to_entity="as")],
    )


def test_label_maps_to_relation(qualifier_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "relations": ["fbs_as"],
        "select": [{"expr": "system.system_name"}],
    }

    bound, diag = bind_selectors(qualifier_schema, selectors)

    assert bound["select"][0]["expr"] == "fbs_as.system_name"
    assert any(d["kind"] == "mapped_qualifier_label_to_relation" for d in diag)


def test_entity_name_maps_to_relation_and_auto_add(qualifier_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "select": [{"expr": "as.system_name"}],
    }

    bound, diag = bind_selectors(qualifier_schema, selectors)

    assert "fbs_as" in bound.get("relations", [])
    assert bound["select"][0]["expr"] == "fbs_as.system_name"
    assert any(d["kind"] == "auto_add_relation" for d in diag)


def test_unknown_qualifier_errors(qualifier_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "select": [{"expr": "unknown.system_name"}],
    }

    with pytest.raises(UnknownRelation):
        bind_selectors(
            qualifier_schema,
            selectors,
            policy=ResolutionPolicy(unknown_qualifier_strategy="error"),
        )


def test_unknown_qualifier_drops_to_root(qualifier_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "select": [{"expr": "unknown.system_name"}],
    }

    bound, diag = bind_selectors(
        qualifier_schema,
        selectors,
        policy=ResolutionPolicy(unknown_qualifier_strategy="drop"),
    )

    assert bound["select"][0]["expr"] == "fbs.system_name"
    assert any(d["kind"] == "ignored_unknown_qualifier" for d in diag)


def test_filter_entity_field_combination(qualifier_schema):
    selectors = {
        "op": "query",
        "root_entity": "fbs",
        "relations": ["fbs_as"],
        "filters": {
            "type": "comparison",
            "entity": "system",
            "field": "system_name",
            "op": "eq",
            "value": "x",
        },
    }

    bound, diag = bind_selectors(qualifier_schema, selectors)

    assert "entity" not in bound["filters"]
    assert bound["filters"]["field"] == "fbs_as.system_name"
    assert any(d["kind"] == "bound_entity_field" for d in diag)
