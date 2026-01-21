from fetchgraph.core.context import BaseGraphAgent, ContextPacker
from fetchgraph.core.models import BaselineSpec, ContextFetchSpec, Plan


def _make_agent(baseline):
    packer = ContextPacker(max_tokens=10, summarizer_llm=lambda text: text)
    return BaseGraphAgent(
        llm_plan=None,
        llm_synth=lambda feature_name, ctx, plan: "",
        domain_parser=lambda raw: raw,
        saver=lambda name, obj: None,
        providers={},
        verifiers=[],
        packer=packer,
        baseline=baseline,
    )


def _get_spec(specs, provider):
    return next(spec for spec in specs if spec.provider == provider)


def test_merge_baseline_preserved_when_context_plan_missing():
    baseline_spec = ContextFetchSpec(
        provider="X", selectors={"a": 1}, mode="filter"
    )
    agent = _make_agent([BaselineSpec(spec=baseline_spec)])
    plan = Plan(required_context=["X"])

    merged = agent._merge_baseline_with_plan(plan)

    merged_spec = _get_spec(merged, "X")
    assert merged_spec.model_dump() == baseline_spec.model_dump()


def test_merge_baseline_preserved_when_context_plan_empty():
    baseline_spec = ContextFetchSpec(
        provider="X", selectors={"a": 1}, mode="filter"
    )
    agent = _make_agent([BaselineSpec(spec=baseline_spec)])
    plan = Plan(required_context=["X"], context_plan=[])

    merged = agent._merge_baseline_with_plan(plan)

    merged_spec = _get_spec(merged, "X")
    assert merged_spec.model_dump() == baseline_spec.model_dump()


def test_merge_required_context_materializes_without_baseline():
    agent = _make_agent(baseline=[])
    plan = Plan(required_context=["X"])

    merged = agent._merge_baseline_with_plan(plan)

    merged_spec = _get_spec(merged, "X")
    assert merged_spec.provider == "X"
    assert merged_spec.mode == "full"
    assert merged_spec.selectors in ({}, None)
    assert getattr(merged_spec, "max_tokens", None) is None


def test_required_context_materializes_even_when_context_plan_non_empty():
    agent = _make_agent(baseline=[])
    plan = Plan(
        required_context=["X"],
        context_plan=[ContextFetchSpec(provider="A", mode="full")],
    )

    merged = agent._merge_baseline_with_plan(plan)
    providers = {spec.provider for spec in merged}

    assert "A" in providers
    assert "X" in providers
