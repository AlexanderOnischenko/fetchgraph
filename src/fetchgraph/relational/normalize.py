from __future__ import annotations

import re
from typing import Any, Dict, Optional
from collections.abc import Callable, MutableMapping

from .types import SelectorsDict

_AGG_REGEX = re.compile(r"^(?P<agg>[a-zA-Z_][\w]*)\s*\(\s*(?P<field>[^)]+)\s*\)$")

def _set_list_field(
    out: MutableMapping[str, Any],
    key: str,
    value: Any,
    normalizer: Callable[[Any], Any],
) -> None:
    """
    Нормализует поле, которое по контракту должно быть list.

    Правило:
      - если нормализатор вернул list -> ставим
      - иначе -> удаляем ключ (не оставляем None и не оставляем мусор)

    Это делает normalize_* "неухудшающим": не превращает отсутствующее поле в None
    и не создаёт гарантированную ошибку list_type.
    """
    normalized = normalizer(value)
    if isinstance(normalized, list):
        out[key] = normalized
    else:
        out.pop(key, None)


def normalize_relational_selectors(selectors: SelectorsDict) -> SelectorsDict:
    if not isinstance(selectors, dict):
        return selectors
    
    normalized: dict[str, Any] = dict(selectors)

    if normalized.get("op") != "query":
        return normalized
    
    _set_list_field(
        normalized, "aggregations", normalized.get("aggregations"), _normalize_aggregations
    )
    _set_list_field(normalized, "group_by", normalized.get("group_by"), _normalize_group_by)
    
    normalized_filters = _normalize_filters(normalized.get("filters"))
    normalized["filters"] = normalized_filters

    normalized = _normalize_min_max_filter(normalized, normalized_filters)

    return normalized


def _normalize_aggregations(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, list):
        value = [value]
    normalized: list[Any] = []
    for item in value:
        if item is None:
            continue
        if isinstance(item, str):
            parsed = _parse_agg_field(item)
            if parsed:
                agg, field_name = parsed
                normalized.append({"field": field_name, "agg": agg, "alias": None})
            continue
        if not isinstance(item, dict):
            continue
        entry = dict(item)
        if not entry.get("agg"):
            field = entry.get("field")
            parsed = _parse_agg_field(field)
            if parsed:
                agg, field_name = parsed
                entry.setdefault("agg", agg)
                entry["field"] = field_name
        if entry.get("field") and entry.get("agg"):
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
        # return normalized
        return _normalize_logical_filter(normalized)
    if isinstance(value, dict) and value.get("type") == "logical":
        return _normalize_logical_filter(value)
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
    if value is None:
        return []
    if not isinstance(value, list):
        if isinstance(value, (str, dict)):
            value = [value]
        else:
            return []
    normalized: list[dict[str, Any]] = []
    for item in value:
        if item is None:
            continue
        if isinstance(item, str):
            field = item.strip()
            if field:
                normalized.append({"field": field})
            continue
        if not isinstance(item, dict):
            continue
        field = item.get("field")
        if isinstance(field, str) and field.strip():
            entry = dict(item)
            entry["field"] = field.strip()
            normalized.append(entry)
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


def _normalize_logical_filter(value: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(value)
    op = normalized.get("op")
    if isinstance(op, str):
        normalized["op"] = op.lower()
    clauses = normalized.get("clauses")
    if isinstance(clauses, list):
        normalized["clauses"] = _flatten_filter_clauses(clauses)
    return normalized