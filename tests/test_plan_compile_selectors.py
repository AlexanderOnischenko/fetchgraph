import pytest

from fetchgraph.core.models import ContextFetchSpec, Plan
from fetchgraph.core.selector_dialects import QUERY_SKETCH_DSL_ID
from fetchgraph.plan_compile import compile_plan_selectors
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
    ]
    relations = [
        RelationDescriptor(
            name="fbs_as",
            from_entity="fbs",
            to_entity="as",
            join=RelationJoin(from_entity="fbs", from_column="id", to_entity="as", to_column="id"),
        )
    ]
    return DummyProvider("rel", entities, relations)


def test_compile_plan_selectors_compiles_dsl_envelope():
    provider = make_provider()
    plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel",
                selectors={
                    "$dsl": QUERY_SKETCH_DSL_ID,
                    "payload": {
                        "from": "fbs",
                        "where": [["system_name", "contains", "ЕСП"]],
                        "take": 10,
                    },
                },
            )
        ]
    )

    compiled = compile_plan_selectors(plan, {"rel": provider})

    selectors = compiled.context_plan[0].selectors
    assert selectors["op"] == "query"
    assert selectors["root_entity"] == "fbs"
    assert selectors["relations"] == ["fbs_as"]
    assert selectors["filters"]["field"] == "fbs_as.system_name"


def test_compile_plan_selectors_rejects_op_and_dsl():
    provider = make_provider()
    plan = Plan(
        context_plan=[
            ContextFetchSpec(provider="rel", selectors={"op": "query", "$dsl": QUERY_SKETCH_DSL_ID})
        ]
    )

    with pytest.raises(ValueError):
        compile_plan_selectors(plan, {"rel": provider})


def test_compile_plan_selectors_rejects_unknown_native_entities_and_relations():
    provider = make_provider()
    plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel",
                selectors={"op": "query", "root_entity": "NOPE", "relations": ["NOPE_REL"]},
            )
        ]
    )

    with pytest.raises(ValueError):
        compile_plan_selectors(plan, {"rel": provider})


def test_compile_plan_selectors_validates_schema_request_against_provider():
    provider = make_provider()
    plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel",
                selectors={"op": "schema", "entities": ["NOPE"], "relations": ["fbs_as", "NOPE_REL"]},
            )
        ]
    )

    with pytest.raises(ValueError):
        compile_plan_selectors(plan, {"rel": provider})
