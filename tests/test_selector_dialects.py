import json

from fetchgraph.core.context import BaseGraphAgent, ContextPacker
from fetchgraph.core.models import ContextFetchSpec, Plan, ProviderInfo
from fetchgraph.core.selector_dialects import (
    QUERY_SKETCH_DSL_ID,
    compile_selectors,
)
from fetchgraph.core.protocols import ContextProvider, SupportsDescribe
from fetchgraph.relational.models import ComparisonFilter, EntityDescriptor
from fetchgraph.relational.providers.base import RelationalDataProvider


class DummyRelationalProvider(RelationalDataProvider):
    def _handle_schema(self):  # pragma: no cover - not used in tests
        raise NotImplementedError

    def _handle_semantic_only(self, req):  # pragma: no cover - not used in tests
        raise NotImplementedError

    def _handle_query(self, req):  # pragma: no cover - not used in tests
        return req


def test_selector_dialect_query_sketch_compiles_to_relational_query():
    provider = DummyRelationalProvider("rel", [EntityDescriptor(name="streams")], [])
    selectors = {
        "$dsl": QUERY_SKETCH_DSL_ID,
        "payload": '{ from: streams, where: [["status", "active"]], limit: 5 }',
    }

    compiled = compile_selectors(provider, selectors)

    assert compiled["op"] == "query"
    assert compiled["root_entity"] == "streams"
    assert compiled["limit"] == 5
    assert compiled["filters"] == ComparisonFilter(
        entity=None, field="status", op="=", value="active"
    ).model_dump()


class RecordingProvider(ContextProvider, SupportsDescribe):
    name = "rec"

    def __init__(self):
        self.last_selectors = None

    def fetch(self, feature_name: str, selectors=None, **kwargs):
        self.last_selectors = selectors or {}
        return {"feature": feature_name, "selectors": self.last_selectors}

    def serialize(self, obj) -> str:
        return json.dumps(obj)

    def describe(self) -> ProviderInfo:  # pragma: no cover - trivial
        return ProviderInfo(name=self.name)


def test_agent_fetch_compiles_envelope_before_provider_fetch():
    provider = RecordingProvider()
    agent = BaseGraphAgent(
        llm_plan=None,
        llm_synth=lambda feature_name, ctx, plan: "",  # not used
        domain_parser=lambda raw: raw,
        saver=lambda name, obj: None,
        providers={"rec": provider},
        verifiers=[],
        packer=ContextPacker(max_tokens=1000, summarizer_llm=lambda text: text),
    )

    plan = Plan(context_plan=[
        ContextFetchSpec(
            provider="rec",
            selectors={
                "$dsl": QUERY_SKETCH_DSL_ID,
                "payload": '{ from: streams, where: [["status", "active"]] }',
            },
        )
    ])

    agent._fetch("demo", plan)

    assert provider.last_selectors["op"] == "query"
    assert "$dsl" not in provider.last_selectors
