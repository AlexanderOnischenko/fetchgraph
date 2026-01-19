from __future__ import annotations

import re
from typing import Any, Dict, Optional

from .types import SelectorsDict

_AGG_REGEX = re.compile(r"^(?P<agg>[a-zA-Z_][\w]*)\s*\(\s*(?P<field>[^)]+)\s*\)$")


def normalize_relational_selectors(selectors: SelectorsDict) -> SelectorsDict:
    if not isinstance(selectors, dict):
        return selectors
    normalized = dict(selectors)
    if normalized.get("op") != "query":
        return normalized
    normalized["aggregations"] = _normalize_aggregations(normalized.get("aggregations"))
    normalized["group_by"] = _normalize_group_by(normalized.get("group_by"))
    normalized_filters = _normalize_filters(normalized.get("filters"))
    normalized["filters"] = normalized_filters
    normalized = _normalize_min_max_filter(normalized, normalized_filters)
    return normalized


def _normalize_aggregations(value: Any) -> Any:
    if not isinstance(value, list):
        return value
    normalized: list[Any] = []
    for item in value:
        if not isinstance(item, dict):
            normalized.append(item)
            continue
        entry = dict(item)
        if not entry.get("agg"):
            field = entry.get("field")
            parsed = _parse_agg_field(field)
            if parsed:
                agg, field_name = parsed
                entry.setdefault("agg", agg)
                entry["field"] = field_name
        normalized.append(entry)
    return normalized


def _parse_agg_field(value: Any) -> Optional[tuple[str, str]]:
    if not isinstance(value, str):
        return None
    match = _AGG_REGEX.match(value.strip())
    if not match:
        return None
    agg = match.group("agg").lower()
    field = match.group("field").strip()
    return agg, field


def _normalize_filters(value: Any) -> Any:
    if isinstance(value, list):
        clauses = _flatten_filter_clauses(value)
        if not clauses:
            return None
        if len(clauses) == 1:
            return clauses[0]
        return {"type": "logical", "op": "and", "clauses": clauses}
    if isinstance(value, dict) and "clauses" in value and "type" not in value:
        normalized = dict(value)
        normalized.setdefault("type", "logical")
        normalized.setdefault("op", "and")
        return normalized
    return value


def _normalize_min_max_filter(selectors: SelectorsDict, filters: Any) -> SelectorsDict:
    if not isinstance(filters, dict):
        return selectors
    if filters.get("type") != "comparison":
        return selectors
    op = filters.get("op")
    if not isinstance(op, str):
        return selectors
    op_lower = op.lower()
    if op_lower not in {"min", "max"}:
        return selectors
    if "value" in filters and filters.get("value") is not None:
        return selectors
    field = filters.get("field")
    if not isinstance(field, str) or not field.strip():
        return selectors
    aggregations = list(selectors.get("aggregations") or [])
    aggregations.append({"field": field, "agg": op_lower, "alias": f"{op_lower}_{field}"})
    normalized = dict(selectors)
    normalized["aggregations"] = _normalize_aggregations(aggregations)
    normalized["filters"] = None
    return normalized


def _normalize_group_by(value: Any) -> Any:
    if not isinstance(value, list):
        return value
    normalized: list[Any] = []
    for item in value:
        if not isinstance(item, dict):
            normalized.append(item)
            continue
        field = item.get("field")
        if not isinstance(field, str) or not field.strip():
            continue
        normalized.append(item)
    return normalized


def _flatten_filter_clauses(value: list[Any]) -> list[Any]:
    flattened: list[Any] = []
    for clause in value:
        if clause is None:
            continue
        if isinstance(clause, list):
            flattened.extend(_flatten_filter_clauses(clause))
        else:
            flattened.append(clause)
    return flattened
