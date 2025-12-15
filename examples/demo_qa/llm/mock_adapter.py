from __future__ import annotations

from typing import Dict, Optional

from fetchgraph.core.protocols import LLMInvoke


class MockLLM(LLMInvoke):
    """Deterministic LLM adapter used for tests and CI."""

    def __init__(self, *, plan_responses: Optional[Dict[str, str]] = None, synth_template: str | None = None):
        self.plan_responses = plan_responses or {}
        self.synth_template = synth_template or "Mock synthesis for: {question}"

    def __call__(self, prompt: str, /, sender: str) -> str:  # type: ignore[override]
        if sender == "generic_plan":
            for key, value in self.plan_responses.items():
                if key.lower() in prompt.lower():
                    return value
            return self.plan_responses.get("default", "{}")
        if sender == "generic_synth":
            return self.synth_template.format(question=prompt)
        return ""


__all__ = ["MockLLM"]
