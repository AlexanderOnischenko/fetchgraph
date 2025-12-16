import json
import pytest

from fetchgraph.core.models import ContextFetchSpec, Plan
from fetchgraph.plan_compile import compile_plan_selectors
from fetchgraph.core.selector_dialects import QUERY_SKETCH_DSL_ID
from fetchgraph.core.selectors import coerce_selectors_to_native
from fetchgraph.relational.models import (
    ColumnDescriptor,
    EntityDescriptor,
    QueryResult,
    RelationDescriptor,
    RelationJoin,
)
from fetchgraph.relational.providers.base import RelationalDataProvider


class DummyProvider(RelationalDataProvider):
    def _handle_schema(self):  # pragma: no cover - not used
        raise NotImplementedError

    def _handle_semantic_only(self, req):  # pragma: no cover - not used
        raise NotImplementedError

    def _handle_query(self, req):  # pragma: no cover - not used
        return QueryResult(rows=[])


def make_provider(include_root_field: bool = False) -> DummyProvider:
    entities = [
        EntityDescriptor(
            name="fbs",
            columns=[
                ColumnDescriptor(name="id"),
                *([ColumnDescriptor(name="system_name")] if include_root_field else []),
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


def test_planner_native_selectors_validate():
    provider = make_provider(include_root_field=True)
    plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel",
                selectors={
                    "op": "query",
                    "root_entity": "as",
                    "relations": ["fbs_as"],
                    "select": [{"expr": "system_name"}],
                    "filters": {
                        "type": "comparison",
                        "entity": "as",
                        "field": "system_name",
                        "op": "ilike",
                        "value": "%ЕСП%",
                    },
                },
            )
        ]
    )

    compiled = compile_plan_selectors(plan, {"rel": provider})
    selectors = compiled.context_plan[0].selectors

    assert selectors["op"] == "query"
    assert selectors["filters"]["op"] == "ilike"
    assert selectors["select"] == [{"expr": "system_name"}]


def test_repair_fields_and_filters_list():
    provider = make_provider(include_root_field=True)
    plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel",
                selectors={
                    "op": "query",
                    "root_entity": "as",
                    "fields": ["system_name"],
                    "filters": [
                        {"field": "system_name", "op": "ilike", "value": "%ЕСП%"}
                    ],
                },
            )
        ]
    )

    compiled = compile_plan_selectors(plan, {"rel": provider})
    selectors = compiled.context_plan[0].selectors

    assert selectors["select"] == [{"expr": "system_name"}]
    assert selectors["filters"]["type"] == "logical"
    assert selectors["filters"]["clauses"][0]["type"] == "comparison"


def test_planner_rejects_dsl_envelope():
    provider = make_provider()
    plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel",
                selectors={"$dsl": QUERY_SKETCH_DSL_ID, "payload": {"from": "fbs"}},
            )
        ]
    )

    with pytest.raises(ValueError) as err:
        compile_plan_selectors(plan, {"rel": provider})

    assert "native selectors" in str(err.value)


def test_rejects_subqueries_and_relation_roots():
    provider = make_provider()
    bad_subquery_plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel",
                selectors={"op": "query", "root_entity": "fbs", "$subquery": {}},
            )
        ]
    )

    with pytest.raises(ValueError):
        compile_plan_selectors(bad_subquery_plan, {"rel": provider})

    relation_root_plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel", selectors={"op": "query", "root_entity": "fbs_as"}
            )
        ]
    )

    with pytest.raises(ValueError):
        compile_plan_selectors(relation_root_plan, {"rel": provider})


def test_compile_plan_selectors_validates_schema_request_against_provider():
    provider = make_provider()
    plan = Plan(
        context_plan=[
            ContextFetchSpec(
                provider="rel", selectors={"op": "schema", "entities": ["NOPE"], "relations": ["fbs_as", "NOPE_REL"]}
            )
        ]
    )

    with pytest.raises(ValueError):
        compile_plan_selectors(plan, {"rel": provider})


def test_cli_accepts_sketch_and_compiles_to_native():
    provider = make_provider()
    selectors = coerce_selectors_to_native(
        provider,
        {"$dsl": QUERY_SKETCH_DSL_ID, "payload": {"from": "fbs", "where": [["system_name", "%ESP%"]]}},
        planner_mode=False,
    )

    assert selectors["op"] == "query"
    assert selectors["root_entity"] == "fbs"
    assert "relations" in selectors
    assert json.dumps(selectors)
