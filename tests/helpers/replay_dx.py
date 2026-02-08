from __future__ import annotations

import json
import os
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def truncate(text: str, *, limit: int = 8000) -> str:
    if len(text) <= limit:
        return text
    remaining = len(text) - limit
    return f"{text[:limit]}...[truncated {remaining} chars]"


def format_json(obj: object, *, max_chars: int = 12_000, max_depth: int = 6) -> str:
    def _prune(value: object, depth: int) -> object:
        if depth <= 0:
            return "...(max depth reached)"
        if isinstance(value, dict):
            return {str(key): _prune(val, depth - 1) for key, val in value.items()}
        if isinstance(value, list):
            return [_prune(item, depth - 1) for item in value]
        if isinstance(value, tuple):
            return tuple(_prune(item, depth - 1) for item in value)
        return value

    text = json.dumps(
        _prune(obj, max_depth),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        default=str,
    )
    return truncate(text, limit=max_chars)


def format_rule_trace(diag: dict | None, *, tail: int = 30) -> str:
    if not diag:
        return ""
    rule_trace = diag.get("rule_trace")
    if not isinstance(rule_trace, list) or not rule_trace:
        return ""
    tail_items = rule_trace[-tail:] if tail > 0 else rule_trace
    return truncate(format_json(tail_items), limit=_int_env("FETCHGRAPH_REPLAY_TRUNCATE", 8000))


def build_rerun_hints(bundle_path: Path) -> list[str]:
    stem = bundle_path.stem
    return [
        f"pytest -k {stem} -m known_bad -vv",
        f"fetchgraph-tracer fixture-green --case {bundle_path}",
        f"fetchgraph-tracer replay --case {bundle_path} --debug",
    ]


def ids_from_path(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except ValueError:
        return path.stem


def truncate_limits() -> tuple[int, int]:
    return (
        _int_env("FETCHGRAPH_REPLAY_TRUNCATE", 12_000),
        _int_env("FETCHGRAPH_REPLAY_META_TRUNCATE", 8000),
    )


def rule_trace_tail() -> int:
    return _int_env("FETCHGRAPH_RULE_TRACE_TAIL", 30)


def debug_enabled() -> bool:
    return os.getenv("FETCHGRAPH_REPLAY_DEBUG") not in (None, "", "0", "false", "False")
