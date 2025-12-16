import json

import pytest

from fetchgraph.core.context import BaseGraphAgent, ContextPacker
from fetchgraph.core.models import ContextFetchSpec, Plan, RefetchDecision
from fetchgraph.core.selector_dialects import QUERY_SKETCH_DSL_ID
from fetchgraph.plan_compile import compile_plan_selectors as real_compile_plan_selectors
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


@pytest.fixture
def provider() -> DummyProvider:
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


def test_refetch_merges_and_compiles_dsl(monkeypatch, provider):
    calls = {"compile": 0, "fetched_selectors": None}

    def compile_spy(plan, providers, **kwargs):
        calls["compile"] += 1
        return real_compile_plan_selectors(plan, providers, **kwargs)

    monkeypatch.setattr("fetchgraph.core.context.compile_plan_selectors", compile_spy)

    agent = BaseGraphAgent(
        llm_plan=lambda feature, ctx: json.dumps({"context_plan": []}),
        llm_synth=lambda feature, ctx, plan: "",
        domain_parser=lambda raw: raw,
        saver=lambda feature, parsed: None,
        providers={"rel": provider},
        verifiers=[],
        packer=ContextPacker(max_tokens=1000, summarizer_llm=lambda text: text),
        llm_refetch=None,
    )

    def fake_refetch(feature_name, ctx_text, plan):
        decision = RefetchDecision(
            stop=False,
            add_specs=[
                ContextFetchSpec(
                    provider="rel",
                    selectors={
                        "$dsl": QUERY_SKETCH_DSL_ID,
                        "payload": {"from": "fbs", "where": [["system_name", "contains", "ЕСП"]], "take": 5},
                    },
                )
            ],
        )
        return decision.model_dump_json()

    agent.llm_refetch = fake_refetch
    agent.max_refetch_iters = 1

    def fake_fetch(feature_name, plan):
        calls["fetched_selectors"] = plan.context_plan[0].selectors
        return {}

    monkeypatch.setattr(agent, "_fetch", fake_fetch)

    ctx, updated_plan = agent._assess_refetch_loop("feature", {}, Plan())

    assert calls["compile"] == 1
    assert calls["fetched_selectors"]["op"] == "query"
    assert calls["fetched_selectors"]["relations"] == ["fbs_as"]
    assert ctx == {}
    assert updated_plan.context_plan[0].selectors["filters"]["field"] == "fbs_as.system_name"
