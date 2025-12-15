import pytest

from fetchgraph.core.models import ContextFetchSpec, Plan
from fetchgraph.core.selector_dialects import QUERY_SKETCH_DSL_ID
from fetchgraph.plan_compile import compile_plan_selectors
from fetchgraph.relational.models import ColumnDescriptor, EntityDescriptor, RelationDescriptor, RelationJoin
from fetchgraph.relational.providers.base import RelationalDataProvider


class DummyProvider(RelationalDataProvider):
    def _handle_schema(self):  # pragma: no cover - not used here
        raise NotImplementedError

    def _handle_semantic_only(self, req):  # pragma: no cover - not used here
        raise NotImplementedError

    def _handle_query(self, req):  # pragma: no cover - not used here
        raise NotImplementedError


def make_provider():
    entities = [
        EntityDescriptor(
            name="fbs",
            columns=[ColumnDescriptor(name="id")],
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
                        "where": [["system_name", "ЕСП"]],
                        "take": 100,
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


def test_compile_plan_selectors_rejects_conflicting_selectors():
    provider = make_provider()
    plan = Plan(
        context_plan=[
            ContextFetchSpec(provider="rel", selectors={"op": "query", "$dsl": QUERY_SKETCH_DSL_ID})
        ]
    )

    with pytest.raises(ValueError):
        compile_plan_selectors(plan, {"rel": provider})


def test_compile_plan_selectors_rejects_unknown_root_entity():
    provider = make_provider()
    plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel",
                selectors={
                    "op": "query",
                    "root_entity": "NOPE",
                },
            )
        ]
    )

    with pytest.raises(ValueError):
        compile_plan_selectors(plan, {"rel": provider})


def test_compile_plan_selectors_rejects_unknown_relation():
    provider = make_provider()
    plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel",
                selectors={
                    "op": "query",
                    "root_entity": "fbs",
                    "relations": ["NOPE_REL"],
                },
            )
        ]
    )

    with pytest.raises(ValueError):
        compile_plan_selectors(plan, {"rel": provider})
