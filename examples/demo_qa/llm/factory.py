from __future__ import annotations

from pathlib import Path

from fetchgraph.core.protocols import LLMInvoke

from ..settings import DemoQASettings
from .mock_adapter import MockLLM
from .openai_adapter import OpenAILLM


def build_llm(settings: DemoQASettings) -> LLMInvoke:
    if settings.llm.provider == "mock":
        plan_responses = None
        if settings.llm.mock.plan_fixture:
            plan_responses = MockLLM.load_plan_fixture(Path(settings.llm.mock.plan_fixture))
        return MockLLM(plan_responses=plan_responses, synth_template=settings.llm.mock.synth_template)

    return OpenAILLM(
        api_key=settings.llm.openai.api_key,
        base_url=settings.llm.openai.base_url,
        plan_model=settings.llm.openai.plan_model,
        synth_model=settings.llm.openai.synth_model,
        plan_temperature=settings.llm.openai.plan_temperature,
        synth_temperature=settings.llm.openai.synth_temperature,
        timeout_s=settings.llm.openai.timeout_s,
        retries=settings.llm.openai.retries,
    )


__all__ = ["build_llm"]
