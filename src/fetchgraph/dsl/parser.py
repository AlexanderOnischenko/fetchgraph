from __future__ import annotations

import json
import re
from json import JSONDecodeError
from typing import Any, Dict, Tuple

from ..parsing.exceptions import OutputParserException
from ..parsing.json_parser import JsonParser
from .ast import QuerySketch
from .diagnostics import Diagnostics, Severity


def _normalize_input(src: str) -> str:
    text = src.strip()
    if not text.startswith("{"):
        text = "{" + text
        if not text.endswith("}"):
            text = text + "}"
    # Quote unquoted keys
    text = re.sub(r'(^|[,\{]\s*)([A-Za-z_]\w*)\s*:', r'\1"\2":', text)
    # Quote bare identifiers or operators inside arrays
    def _quote_array_value(match: re.Match[str]) -> str:
        prefix, value = match.groups()
        if value in {"true", "false", "null"}:
            return f"{prefix}{value}"
        return f'{prefix}"{value}"'

    text = re.sub(r'(\[|,)\s*([A-Za-z_][\w.-]*)\s*(?=[,\]])', _quote_array_value, text)
    text = re.sub(r'(\[|,)\s*([!<>=]{1,3})\s*(?=[,\]])', _quote_array_value, text)
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


class DslParser(JsonParser[QuerySketch]):
    """JSON5-ish parser for QuerySketch built atop the generic JsonParser."""

    def parse_query(self, src: str | Dict[str, Any]) -> Tuple[QuerySketch, Diagnostics]:
        diagnostics = Diagnostics()

        if isinstance(src, dict):
            return QuerySketch(data=src), diagnostics

        if not isinstance(src, str):
            diagnostics.add(
                code="DSL_PARSE_ERROR",
                message="Source must be a string or mapping",
                path="$",
                severity=Severity.ERROR,
            )
            return QuerySketch(data={}), diagnostics

        text = src.strip()
        block = self._extract_block(text)
        if ":" in text and ("{" not in block and ":" not in block):
            block = text

        def _load_candidates() -> Dict[str, Any]:
            try:
                return json.loads(block)
            except JSONDecodeError:
                pass

            try:
                return json.loads(_normalize_input(block))
            except JSONDecodeError:
                pass

            try:
                loaded = self._loads_tolerant(block)
            except OutputParserException:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                raise OutputParserException(str(exc)) from exc

            if isinstance(loaded, dict):
                return loaded
            raise OutputParserException("Parsed data is not an object")

        try:
            parsed = _load_candidates()
        except OutputParserException:
            diagnostics.add(
                code="DSL_PARSE_ERROR",
                message="Failed to parse QuerySketch input as JSON5-like structure",
                path="$",
                severity=Severity.ERROR,
            )
            return QuerySketch(data={}), diagnostics

        return QuerySketch(data=parsed), diagnostics


def parse_query_sketch(src: str | Dict[str, Any]) -> Tuple[QuerySketch, Diagnostics]:
    """Parse QuerySketch source (dict or JSON5-ish string) into an AST."""

    parser = DslParser()
    return parser.parse_query(src)
