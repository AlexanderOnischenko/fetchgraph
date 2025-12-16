from __future__ import annotations

from typing import Any, Dict, Mapping

from .selector_dialects import compile_selectors
from .protocols import ContextProvider
from ..relational.models import RelationalQuery, SchemaRequest, SemanticOnlyRequest
from ..relational.selector_normalizer import normalize_relational_selectors


def _validate_compiled(provider: ContextProvider, compiled: Dict[str, Any]) -> None:
    op = compiled.get("op") if isinstance(compiled, dict) else None

    entity_names = {e.name for e in getattr(provider, "entities", []) or []}
    relation_names = {r.name for r in getattr(provider, "relations", []) or []}

    if op == "query":
        RelationalQuery.model_validate(compiled)
        if entity_names:
            root = compiled.get("root_entity")
            if root not in entity_names:
                raise ValueError(
                    f"Unknown root_entity {root!r}; known entities: {sorted(entity_names)}"
                )
        if relation_names:
            missing_relations = [
                rel for rel in compiled.get("relations") or [] if rel not in relation_names
            ]
            if missing_relations:
                raise ValueError(
                    "Unknown relations: "
                    + ", ".join(sorted(missing_relations))
                    + f"; known relations: {sorted(relation_names)}"
                )
    elif op == "schema":
        SchemaRequest.model_validate(compiled)
        if entity_names:
            requested_entities = compiled.get("entities") or []
            missing_entities = [e for e in requested_entities if e not in entity_names]
            if missing_entities:
                raise ValueError(
                    "Unknown entities: "
                    + ", ".join(sorted(missing_entities))
                    + f"; known entities: {sorted(entity_names)}"
                )
        if relation_names:
            requested_relations = compiled.get("relations") or []
            missing_relations = [r for r in requested_relations if r not in relation_names]
            if missing_relations:
                raise ValueError(
                    "Unknown relations: "
                    + ", ".join(sorted(missing_relations))
                    + f"; known relations: {sorted(relation_names)}"
                )
    elif op == "semantic_only":
        SemanticOnlyRequest.model_validate(compiled)
        if entity_names:
            entity = compiled.get("entity")
            if entity not in entity_names:
                raise ValueError(
                    f"Unknown entity {entity!r}; known entities: {sorted(entity_names)}"
                )


def coerce_selectors_to_native(
    provider: ContextProvider, selectors: Mapping[str, Any] | None, *, planner_mode: bool = False
) -> Dict[str, Any]:
    if selectors is None:
        selectors = {}

    if not isinstance(selectors, Mapping):
        return selectors  # type: ignore[return-value]

    selectors = dict(selectors)
    has_dsl = "$dsl" in selectors
    has_payload_only = "payload" in selectors and "op" not in selectors

    if planner_mode and (has_dsl or has_payload_only):
        raise ValueError(
            "Planner must output native selectors without $dsl/payload; use provider's selectors_schema/digest."
        )

    compiled: Dict[str, Any] | Any = selectors
    if has_dsl:
        compiled = compile_selectors(provider, selectors)  # may raise

    compiled = normalize_relational_selectors(provider, compiled)
    if isinstance(compiled, dict):
        _validate_compiled(provider, compiled)
    return compiled


__all__ = ["coerce_selectors_to_native", "_validate_compiled"]
