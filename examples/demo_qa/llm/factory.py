from __future__ import annotations

from fetchgraph.core.protocols import LLMInvoke

from ..settings import DemoQASettings
from .openai_adapter import OpenAILLM


def build_llm(settings: DemoQASettings) -> LLMInvoke:
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
