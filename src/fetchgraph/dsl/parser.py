from __future__ import annotations

import json
import re
from json import JSONDecodeError
from typing import Any, Dict, List, Tuple

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

    # Quote unquoted keys quickly with a regex pass
    text = re.sub(r'(^|[,\{]\s*)([A-Za-z_]\w*)\s*:', r'\1"\2":', text)

    result: List[str] = []
    stack: List[dict] = []  # track context and expectations
    in_string = False
    quote_char = ""
    escape = False

    def _current_expect_value() -> bool:
        if not stack:
            return False
        top = stack[-1]
        return top.get("expecting_value", False)

    def _set_expect_value(flag: bool) -> None:
        if stack:
            stack[-1]["expecting_value"] = flag

    number_pattern = re.compile(r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$")

    i = 0
    while i < len(text):
        ch = text[i]

        if in_string:
            result.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote_char:
                in_string = False
                _set_expect_value(False)
            i += 1
            continue

        if ch in {'"', "'"}:
            in_string = True
            quote_char = ch
            result.append(ch)
            _set_expect_value(False)
            i += 1
            continue

        if ch == "{":
            stack.append({"type": "object", "expecting_key": True, "expecting_value": False})
            result.append(ch)
            i += 1
            continue

        if ch == "[":
            stack.append({"type": "array", "expecting_value": True})
            result.append(ch)
            i += 1
            continue

        if ch == "}":
            if stack:
                stack.pop()
            result.append(ch)
            i += 1
            continue

        if ch == "]":
            if stack:
                stack.pop()
            result.append(ch)
            i += 1
            continue

        if ch == ":":
            if stack and stack[-1].get("type") == "object":
                stack[-1]["expecting_key"] = False
                stack[-1]["expecting_value"] = True
            result.append(ch)
            i += 1
            continue

        if ch == ",":
            if stack:
                top = stack[-1]
                if top.get("type") == "array":
                    top["expecting_value"] = True
                elif top.get("type") == "object":
                    top["expecting_key"] = True
                    top["expecting_value"] = False
            result.append(ch)
            i += 1
            continue

        expecting_value = _current_expect_value()

        if expecting_value:
            if ch.isspace():
                result.append(ch)
                i += 1
                continue

            if ch in "<>!=":
                j = i
                while j < len(text) and text[j] in "<>!=":
                    j += 1
                token = text[i:j]
                result.append(f'"{token}"')
                _set_expect_value(False)
                i = j
                continue

            if ch.isalpha() or ch in {"_", "."}:
                j = i
                while j < len(text) and (text[j].isalnum() or text[j] in {"_", "."}):
                    j += 1
                token = text[i:j]
                if token in {"true", "false", "null"}:
                    token_str = token
                elif number_pattern.match(token):
                    token_str = token
                else:
                    token_str = f'"{token}"'
                result.append(token_str)
                _set_expect_value(False)
                i = j
                continue

            if ch.isdigit() or ch == "-":
                j = i
                while j < len(text) and (text[j].isdigit() or text[j] in "+-eE."):
                    j += 1
                token = text[i:j]
                if number_pattern.match(token):
                    result.append(token)
                else:
                    result.append(f'"{token}"')
                _set_expect_value(False)
                i = j
                continue

        result.append(ch)
        i += 1

    normalized = "".join(result)
    normalized = re.sub(r"'([^'\\]*(?:\\.[^'\\]*)*)'", r'"\1"', normalized)
    normalized = re.sub(r',\s*(?=[}\]])', '', normalized)
    return normalized


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
