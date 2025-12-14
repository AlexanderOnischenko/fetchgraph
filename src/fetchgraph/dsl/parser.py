from __future__ import annotations

import json
import re
from json import JSONDecodeError
from typing import Any, Dict, Tuple

from .ast import QuerySketch
from .diagnostics import Diagnostics, Severity


def _normalize_input(src: str) -> str:
    text = src.strip()
    if not text.startswith("{"):
        text = "{" + text
        if not text.endswith("}"):
            text = text + "}"
    # Quote unquoted keys
    text = re.sub(r'(^|[,{]\s*)([A-Za-z_]\w*)\s*:', r'\1"\2":', text)
    # Quote bare word values (except true/false/null)
    def _quote_value(match: re.Match[str]) -> str:
        value = match.group(1)
        if value in {"true", "false", "null"}:
            return f": {value}"
        return f': "{value}"'

    text = re.sub(r':\s*([A-Za-z_][\w-]*)\s*(?=[,}])', _quote_value, text)
    # Replace single-quoted strings with double quotes
    text = re.sub(r"'([^'\\]*(?:\\.[^'\\]*)*)'", r'"\1"', text)
    # Remove trailing commas
    text = re.sub(r',\s*(?=[}\]])', '', text)
    return text


def _parse_jsonish(text: str) -> Tuple[Dict[str, Any], Diagnostics]:
    diagnostics = Diagnostics()
    try:
        return json.loads(text), diagnostics
    except JSONDecodeError:
        fixed = _normalize_input(text)
        try:
            return json.loads(fixed), diagnostics
        except JSONDecodeError:
            diagnostics.add(
                code="DSL_PARSE_ERROR",
                message="Failed to parse QuerySketch input as JSON5-like structure",
                path="$",
                severity=Severity.ERROR,
            )
            return {}, diagnostics


def parse_query_sketch(src: str | Dict[str, Any]) -> Tuple[QuerySketch, Diagnostics]:
    """Parse QuerySketch source (dict or JSON5-ish string) into an AST."""

    if isinstance(src, dict):
        return QuerySketch(data=src), Diagnostics()

    if not isinstance(src, str):
        diagnostics = Diagnostics()
        diagnostics.add(
            code="DSL_PARSE_ERROR",
            message="Source must be a string or mapping",
            path="$",
            severity=Severity.ERROR,
        )
        return QuerySketch(data={}), diagnostics

    parsed, diagnostics = _parse_jsonish(src)
    return QuerySketch(data=parsed), diagnostics
