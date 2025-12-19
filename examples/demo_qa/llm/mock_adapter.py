from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from fetchgraph.core.protocols import LLMInvoke


class MockLLM(LLMInvoke):
    """Deterministic LLM adapter used for tests and CI."""

    def __init__(self, *, plan_responses: Optional[Dict[str, str]] = None, synth_template: str | None = None):
        self.plan_responses = plan_responses or {}
        self.synth_template = synth_template or "Mock synthesis for: {question}"

    @staticmethod
    def load_plan_fixture(path: Path) -> Dict[str, str]:
        import json

        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("Mock plan fixture must be a JSON object mapping patterns to responses")
        return {str(k): str(v) for k, v in data.items()}

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
