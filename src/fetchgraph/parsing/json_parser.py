from __future__ import annotations

import ast
import json
import re
from typing import Any, Dict, Generic, Optional, TypeVar, cast

from ..models import RawLLMOutput
from .exceptions import OutputParserException
from .extract_json import extract_json

T = TypeVar("T")


class JsonParser(Generic[T]):
    """Parse LLM output into JSON with optional key extraction."""

    def __init__(self, key: Optional[str] = None):
        self.key = key

    def _extract_block(self, text: str) -> str:
        try:
            blk = extract_json(text)
            if blk:
                return blk
        except Exception:
            pass

        fenced = re.search(r"```json\s*(.*?)\s*```", text, flags=re.IGNORECASE | re.DOTALL)
        if fenced:
            return fenced.group(1)

        start = text.find("{")
        if start != -1:
            end = text.rfind("}")
            if end != -1 and end > start:
                return text[start : end + 1]

        return text

    def _loads_tolerant(self, s: str) -> Any:
        try:
            return json.loads(s)
        except Exception:
            try:
                return ast.literal_eval(s)
            except Exception as exc:
                raise OutputParserException(f"Invalid JSON output: {exc}") from exc

    def to_model(self, data: Any) -> T:
        return cast(T, data)

    def parse(self, raw: RawLLMOutput) -> T:
        text = raw.text.strip()
        block = self._extract_block(text)
        data: Any = self._loads_tolerant(block)

        if self.key:
            if not isinstance(data, dict) or self.key not in data:
                raise OutputParserException(f"Во входном JSON нет ключа '{self.key}'")
            data = {self.key: data[self.key]}

        return self.to_model(data)
