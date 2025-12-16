import json

import pytest

from fetchgraph import (
    BaseGraphAgent,
    ContextPacker,
    ProviderInfo,
    TaskProfile,
    coerce_selectors_to_native,
    make_llm_plan_generic,
)
from fetchgraph.core.context import provider_catalog_text


class DummyProvider:
    name = "dummy"

    def fetch(self, feature_name, selectors=None, **kwargs):
        return selectors or {}

    def serialize(self, obj):
        return json.dumps(obj)

    def describe(self):
        return ProviderInfo(name=self.name, examples=['{"op":"schema"}'])


class SketchyProvider(DummyProvider):
    def describe(self):
        return ProviderInfo(
            name=self.name,
            examples=['{"op":"query","root_entity":"demo"}'],
            sketch_examples=['{"$dsl":{"payload":{"op":"query"}}}'],
        )


def test_planner_prompt_no_sketch_markers():
    captured = []
    profile = TaskProfile(task_name="Demo")
    providers = {"dummy": DummyProvider()}

    def fake_llm(prompt: str, sender: str):
        captured.append(prompt)
        return "{}"

    plan = make_llm_plan_generic(fake_llm, profile, providers)
    plan("feature", {})

    prompt = captured[0]
    assert "$dsl" not in prompt
    assert "selector_dialects" not in prompt
    assert "payload" not in prompt


def test_planner_accepts_native_only():
    provider = DummyProvider()
    plan_json = json.dumps(
        {
            "context_plan": [
                {
                    "provider": provider.name,
                    "mode": "full",
                    "selectors": {"$dsl": {"payload": {"op": "schema"}}},
                }
            ]
        }
    )
    agent = BaseGraphAgent(
        llm_plan=lambda feature_name, lite_ctx: plan_json,
        llm_synth=lambda feature_name, ctx, plan: "",
        domain_parser=lambda raw: raw,
        saver=lambda feature_name, parsed: None,
        providers={provider.name: provider},
        verifiers=[],
        packer=ContextPacker(max_tokens=1000, summarizer_llm=lambda text: text),
        baseline=[],
        max_retries=0,
        task_profile=TaskProfile(task_name="Demo"),
        allow_sketch=False,
    )

    with pytest.raises(ValueError):
        agent.run("feature")


def test_cli_accepts_sketch_optional():
    provider_info = ProviderInfo(name="demo")
    selectors = {"$dsl": {"payload": {"op": "schema", "limit": 5}}}

    native = coerce_selectors_to_native(selectors, provider_info, allow_sketch=True)

    assert native == {"op": "schema", "limit": 5}


def test_describe_planner_view_contains_native_only_examples():
    catalog = provider_catalog_text({"demo": SketchyProvider()})

    assert "$dsl" not in catalog
    assert "sketch_examples" not in catalog
