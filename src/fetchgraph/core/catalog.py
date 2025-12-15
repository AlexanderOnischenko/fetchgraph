from __future__ import annotations

"""Utilities for compact, LLM-friendly provider catalogs."""

from typing import Any, Dict, Iterable, List, Tuple

MAX_PROVIDERS_CATALOG_CHARS = 16000
MAX_PROVIDER_BLOCK_CHARS = 3500
MAX_ENUM_ITEMS = 25
MAX_COLUMNS_PREVIEW = 20
MAX_ENTITIES_PREVIEW = 8
MAX_RELATIONS_PREVIEW = 8
MAX_EXAMPLES = 3
MAX_DIALECTS = 3


def compact_enum(values: Iterable[str], max_items: int = MAX_ENUM_ITEMS) -> Tuple[List[str], int]:
    values_list = list(values)
    if len(values_list) <= max_items:
        return values_list, 0
    return values_list[:max_items], len(values_list) - max_items


def summarize_selectors_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Return a compact summary of selectors schema.

    The summary intentionally omits heavy sections such as ``$defs`` and keeps only
    per-op required fields and short enum previews.
    """

    one_of = schema.get("oneOf")
    if not isinstance(one_of, list):
        return {}

    summary: Dict[str, Any] = {"oneOf": []}
    for variant in one_of:
        if not isinstance(variant, dict):
            continue

        props = variant.get("properties", {}) if isinstance(variant.get("properties"), dict) else {}
        op = None
        op_field = props.get("op")
        if isinstance(op_field, dict):
            op = op_field.get("const") or op_field.get("enum", [None])[0]

        required = variant.get("required", []) if isinstance(variant.get("required"), list) else []
        enums: Dict[str, Any] = {}
        optional: List[str] = []

        for key, spec in props.items():
            if key == "op":
                continue
            if isinstance(spec, dict):
                if "enum" in spec and isinstance(spec["enum"], list):
                    compacted, more = compact_enum(spec["enum"])
                    enums[key] = compacted + ([f"... (+{more} more)"] if more else [])
                else:
                    optional.append(key)

        summary["oneOf"].append(
            {
                "op": op,
                "required": required,
                "optional": optional,
                "enums": enums,
            }
        )

    return summary


__all__ = [
    "MAX_PROVIDERS_CATALOG_CHARS",
    "MAX_PROVIDER_BLOCK_CHARS",
    "MAX_ENUM_ITEMS",
    "MAX_COLUMNS_PREVIEW",
    "MAX_ENTITIES_PREVIEW",
    "MAX_RELATIONS_PREVIEW",
    "MAX_EXAMPLES",
    "MAX_DIALECTS",
    "compact_enum",
    "summarize_selectors_schema",
]
