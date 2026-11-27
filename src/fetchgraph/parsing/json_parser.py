"""Fetchgraph adapter over the independently versioned llm-output-repair API.

The library owns syntactic repair and crash capture; Fetchgraph retains key
selection, domain parsing and Plan validation.
"""
from __future__ import annotations

from typing import Any, Generic, Optional, TypeVar, cast

from llm_output_repair import CrashRecorder, parse_json, repair_json

from ..core.models import RawLLMOutput
from .exceptions import OutputParserException
from .repair_runtime import recorder_for

T = TypeVar("T")


class JsonParser(Generic[T]):
    """Decode LLM JSON without losing the raw response needed for regression.

    TOLERANT preserves support for arrays and legacy extraction heuristics.
    SAFE/STRICT demand object roots; SAFE rejects ambiguous or lossy repairs.
    """

    def __init__(
        self,
        key: Optional[str] = None,
        *,
        policy: str = "tolerant",
        recorder: CrashRecorder | None = None,
    ) -> None:
        if policy not in ("strict", "safe", "tolerant"):
            raise ValueError("policy must be strict, safe, or tolerant")
        self.key = key
        self.policy = policy
        self.recorder = recorder

    def to_model(self, data: Any) -> T:
        return cast(T, data)

    def parse(self, raw: RawLLMOutput) -> T:
        # Pass EXACT original text; do not strip/fence-extract before recording.
        # Recorder initialization is opt-in, controlled by the host application.
        recorder = self.recorder if self.recorder is not None else recorder_for("json")
        try:
            if self.policy == "tolerant":
                data: Any = repair_json(raw.text, recorder=recorder)
            else:
                data = parse_json(
                    raw.text,
                    policy=self.policy,
                    expected_type=dict,
                    recorder=recorder,
                ).value
        except Exception as exc:
            # Keep the existing public Fetchgraph exception surface; do not
            # copy model output/exception text into logs.
            raise OutputParserException(
                f"Invalid JSON output ({type(exc).__name__})"
            ) from exc

        if self.key:
            if not isinstance(data, dict) or self.key not in data:
                raise OutputParserException(
                    f"Во входном JSON нет ключа '{self.key}'"
                )
            data = {self.key: data[self.key]}
        return self.to_model(data)
