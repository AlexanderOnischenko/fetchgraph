import json
from typing import Any

import pytest

from fetchgraph.core.context import BaseGraphAgent, ContextPacker
from fetchgraph.core.models import (
    ContextFetchSpec,
    Plan,
    ProviderInfo,
    SelectorDialectInfo,
)
from fetchgraph.core.selector_dialects import (
    QUERY_SKETCH_DSL_ID,
    compile_selectors,
)
from fetchgraph.core.protocols import ContextProvider, SupportsDescribe
from fetchgraph.relational.models import ColumnDescriptor, ComparisonFilter, EntityDescriptor, QueryResult
from fetchgraph.relational.providers.base import RelationalDataProvider


class DummyRelationalProvider(RelationalDataProvider):
    def _handle_schema(self):  # pragma: no cover - not used in tests
        raise NotImplementedError

    def _handle_semantic_only(self, req):  # pragma: no cover - not used in tests
        raise NotImplementedError

    def _handle_query(self, req):  # pragma: no cover - not used in tests
        return QueryResult()


def test_selector_dialect_query_sketch_compiles_object_payload_to_relational_query():
    provider = DummyRelationalProvider(
        "rel",
        [EntityDescriptor(name="streams", columns=[ColumnDescriptor(name="status")])],
        [],
    )
    selectors = {
        "$dsl": QUERY_SKETCH_DSL_ID,
        "payload": {"from": "streams", "where": [["status", "active"]], "take": 5},
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
        self.last_selectors: dict[str, Any] = {}
        self.entities = [EntityDescriptor(name="streams", columns=[ColumnDescriptor(name="status")])]
        self.relations = []

    def fetch(self, feature_name: str, selectors=None, **kwargs):
        self.last_selectors = selectors or {}
        return {"feature": feature_name, "selectors": self.last_selectors}

    def serialize(self, obj) -> str:
        return json.dumps(obj)

    def describe(self) -> ProviderInfo:  # pragma: no cover - trivial
        return ProviderInfo(
            name=self.name,
            selector_dialects=[
                SelectorDialectInfo(
                    id=QUERY_SKETCH_DSL_ID,
                    description="",
                    envelope_example="",
                )
            ],
        )


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


class LegacyProvider(ContextProvider):
    name = "legacy"

    def fetch(self, feature_name: str, selectors=None, **kwargs):  # pragma: no cover
        return {}

    def serialize(self, obj) -> str:  # pragma: no cover
        return json.dumps(obj)


def test_compile_selectors_rejects_missing_describe_support():
    provider = LegacyProvider()
    selectors = {"$dsl": QUERY_SKETCH_DSL_ID, "payload": "{ from: streams }"}

    with pytest.raises(ValueError) as exc:
        compile_selectors(provider, selectors)

    assert "$dsl" in str(exc.value)


def test_compile_selectors_rejects_conflicting_op_and_dsl():
    provider = RecordingProvider()
    selectors = {
        "$dsl": QUERY_SKETCH_DSL_ID,
        "op": "query",
        "payload": "{ from: streams }",
    }

    with pytest.raises(ValueError) as exc:
        compile_selectors(provider, selectors)

    assert "op" in str(exc.value)


def test_compile_selectors_requires_string_dsl_id():
    provider = RecordingProvider()
    selectors = {"$dsl": 123, "payload": {"from": "streams"}}

    with pytest.raises(ValueError) as exc:
        compile_selectors(provider, selectors)

    assert "string id" in str(exc.value)


class DescribingWithoutDialect(ContextProvider, SupportsDescribe):
    name = "no_dialect"
    entities = []
    relations = []

    def fetch(self, feature_name: str, selectors=None, **kwargs):  # pragma: no cover
        return {}

    def serialize(self, obj) -> str:  # pragma: no cover
        return json.dumps(obj)

    def describe(self) -> ProviderInfo:  # pragma: no cover - trivial
        return ProviderInfo(name=self.name)


def test_compile_selectors_requires_declared_dialect_support():
    provider = DescribingWithoutDialect()
    selectors = {"$dsl": QUERY_SKETCH_DSL_ID, "payload": {"from": "streams"}}

    with pytest.raises(ValueError) as exc:
        compile_selectors(provider, selectors)

    assert QUERY_SKETCH_DSL_ID in str(exc.value)


class MultiDialectProvider(RecordingProvider):
    def describe(self) -> ProviderInfo:  # pragma: no cover - trivial
        return ProviderInfo(
            name=self.name,
            selector_dialects=[
                SelectorDialectInfo(id=QUERY_SKETCH_DSL_ID, description="", envelope_example=""),
                SelectorDialectInfo(id="other@v1", description="", envelope_example=""),
            ],
        )


def test_compile_selectors_accepts_object_envelope_with_id_and_payload():
    provider = RecordingProvider()
    selectors = {"$dsl": {"id": QUERY_SKETCH_DSL_ID, "payload": {"from": "streams", "where": []}}}

    compiled = compile_selectors(provider, selectors)

    assert compiled["op"] == "query"
    assert compiled["root_entity"] == "streams"


def test_compile_selectors_accepts_object_envelope_and_infers_single_dialect():
    provider = RecordingProvider()
    selectors = {"$dsl": {"payload": {"from": "streams", "where": []}}}

    compiled = compile_selectors(provider, selectors)

    assert compiled["op"] == "query"
    assert compiled["root_entity"] == "streams"


def test_compile_selectors_missing_dsl_id_with_multiple_dialects_errors():
    provider = MultiDialectProvider()
    selectors = {"$dsl": {"payload": {"from": "streams", "where": []}}}

    with pytest.raises(ValueError) as exc:
        compile_selectors(provider, selectors)

    assert "missing $dsl.id" in str(exc.value)
