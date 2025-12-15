from __future__ import annotations

import os
from typing import Optional

from fetchgraph.core.protocols import LLMInvoke


class OpenAILLM(LLMInvoke):
    """Thin wrapper around the OpenAI ChatCompletions API."""

    def __init__(self, model: Optional[str] = None):
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY is not set. Provide it via environment variable.")
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    def __call__(self, prompt: str, /, sender: str) -> str:  # type: ignore[override]
        try:
            import openai
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("openai package is required for OpenAILLM") from exc

        client = openai.OpenAI(api_key=self.api_key)
        resp = client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        return resp.choices[0].message.content or ""


__all__ = ["OpenAILLM"]
