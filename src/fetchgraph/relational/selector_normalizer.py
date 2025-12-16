from __future__ import annotations

"""Helpers for tolerating and normalizing slightly malformed selectors."""

from typing import Any, Dict


def _assert_no_subquery(obj: Any) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(key, str) and "$subquery" in key:
                raise ValueError(
                    "Subqueries ($subquery) are not supported; use joins via relations/with instead."
                )
            _assert_no_subquery(value)
    elif isinstance(obj, list):
        for item in obj:
            _assert_no_subquery(item)


def normalize_relational_selectors(
    provider: Any, selectors: Dict[str, Any], *, lenient_relation_root: bool = False
) -> Dict[str, Any]:
    """Normalize common near-miss shapes for relational selectors.

    - Converts ``fields: [..]`` into ``select: [{"expr": ...}]`` for ``op=query``.
    - Wraps ``filters: [{...}]`` lists into a logical ``and`` with comparison clauses.
    - Rejects any ``$subquery`` keys with an actionable error message.
    - Flags ``root_entity`` values that reference relations instead of entities.
    """

    if not isinstance(selectors, dict):
        return selectors

    _assert_no_subquery(selectors)

    op = selectors.get("op")
    if op != "query":
        return selectors

    normalized = dict(selectors)

    entity_names = {e.name for e in getattr(provider, "entities", []) or []}
    relation_names = {r.name for r in getattr(provider, "relations", []) or []}

    root = normalized.get("root_entity")
    if root and entity_names and root not in entity_names:
        known_entities = ", ".join(sorted(entity_names))
        if root in relation_names and not lenient_relation_root:
            raise ValueError(
                f"root_entity {root!r} refers to a relation, not an entity; choose one of: {known_entities} and join via relations/with."
            )
        if not lenient_relation_root:
            raise ValueError(
                f"Unknown root_entity {root!r}; valid entities: {known_entities}. Use relations/with to join related tables."
            )

    if "fields" in normalized and "select" not in normalized:
        fields = normalized.pop("fields")
        if isinstance(fields, list):
            normalized["select"] = [{"expr": f} for f in fields]

    filters_obj = normalized.get("filters")
    if isinstance(filters_obj, list) and filters_obj and all(
        isinstance(item, dict) and "type" not in item for item in filters_obj
    ):
        clauses = []
        for item in filters_obj:
            clause = {"type": "comparison"}
            clause.update(item)
            clauses.append(clause)
        normalized["filters"] = {"type": "logical", "op": "and", "clauses": clauses}

    return normalized


__all__ = ["normalize_relational_selectors"]
