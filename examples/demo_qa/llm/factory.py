from __future__ import annotations

from fetchgraph.core.protocols import LLMInvoke

from ..settings import DemoQASettings
from .openai_adapter import OpenAILLM


def build_llm(settings: DemoQASettings) -> LLMInvoke:
    return OpenAILLM(
        api_key=settings.llm.api_key,
        base_url=settings.llm.base_url,
        plan_model=settings.llm.plan_model,
        synth_model=settings.llm.synth_model,
        plan_temperature=settings.llm.plan_temperature,
        synth_temperature=settings.llm.synth_temperature,
        timeout_s=settings.llm.timeout_s,
        retries=settings.llm.retries,
        require_api_key=settings.llm.require_api_key,
    )


__all__ = ["build_llm"]
