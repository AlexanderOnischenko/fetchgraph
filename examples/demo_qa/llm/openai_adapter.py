from __future__ import annotations

import logging
import os
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

from fetchgraph.core.protocols import LLMInvoke


class OpenAILLM(LLMInvoke):
    """Thin wrapper around the OpenAI ChatCompletions API."""

    logger = logging.getLogger(__name__)

    def __init__(
        self,
        *,
        api_key: str | None,
        base_url: str | None = None,
        plan_model: str,
        synth_model: str,
        plan_temperature: float = 0.0,
        synth_temperature: float = 0.2,
        timeout_s: float | None = None,
        retries: int | None = None,
        require_api_key: bool = True,
    ):
        try:
            import openai
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("openai package is required for OpenAILLM") from exc

        resolved_key = self._resolve_api_key(api_key, require_api_key)
        validated_base = self._validate_base_url(base_url)
        normalized_base = validated_base.rstrip("/") if validated_base else None

        self.client = openai.OpenAI(api_key=resolved_key, base_url=normalized_base)
        self.plan_model = plan_model
        self.synth_model = synth_model
        self.plan_temperature = plan_temperature
        self.synth_temperature = synth_temperature
        self.timeout_s = timeout_s
        self.retries = retries
        if normalized_base:
            endpoint = f"{normalized_base}/chat/completions"
            self.logger.info("OpenAILLM using endpoint %s", endpoint)

    def _resolve_api_key(self, api_key: str | None, require_api_key: bool) -> str | None:
        if api_key:
            if api_key.startswith("env:"):
                env_var = api_key.split(":", 1)[1]
                value = os.getenv(env_var)
                if value:
                    return value
                if require_api_key:
                    raise RuntimeError(
                        f"Environment variable {env_var} referenced in config but not set."
                    )
                return None
            return api_key

        if require_api_key:
            raise RuntimeError("OpenAI provider selected but llm.api_key is missing.")
        return None

    def _validate_base_url(self, base_url: str | None) -> str | None:
        if base_url in (None, ""):
            return None
        parsed = urlparse(base_url)
        if not (parsed.scheme and parsed.netloc):
            raise RuntimeError(f"Invalid base_url for OpenAI provider: {base_url!r}.")
        return base_url

    def _select_model(self, sender: str) -> Tuple[str, float]:
        if sender == "generic_plan":
            return self.plan_model, self.plan_temperature
        if sender == "generic_synth":
            return self.synth_model, self.synth_temperature
        return self.plan_model, self.plan_temperature

    def __call__(self, prompt: str, /, sender: str) -> str:  # type: ignore[override]
        model, temperature = self._select_model(sender)
        client = self.client
        options: Dict[str, Any] = {}
        if self.timeout_s is not None:
            options["timeout"] = self.timeout_s
        if self.retries is not None:
            options["max_retries"] = self.retries
        if options:
            client = self.client.with_options(**options)

        if not model:
            raise RuntimeError("Model must be provided for OpenAI calls.")
        payload: Dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
        }

        resp = client.chat.completions.create(**payload)
        return resp.choices[0].message.content or ""


__all__ = ["OpenAILLM"]
