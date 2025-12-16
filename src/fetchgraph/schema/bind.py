from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .policy import ResolutionPolicy
from .types import ProviderSchema


class UnknownRelation(Exception):
    def __init__(self, relation: str):
        super().__init__(f"Unknown relation alias: {relation}")
        self.relation = relation


class RelationNotFromRoot(Exception):
    def __init__(self, relation: str, root_entity: str, from_entity: str):
        super().__init__(
            f"Relation '{relation}' does not originate from root '{root_entity}' (from_entity={from_entity})"
        )
        self.relation = relation
        self.root_entity = root_entity
        self.from_entity = from_entity


class UnknownField(Exception):
    def __init__(self, field: str, *, root_entity: str, candidates: Optional[List[str]] = None):
        msg = f"Unknown field '{field}' for root '{root_entity}'"
        if candidates:
            msg += f" (candidates: {', '.join(candidates)})"
        super().__init__(msg)
        self.field = field
        self.root_entity = root_entity
        self.candidates = candidates or []


class AmbiguousField(Exception):
    def __init__(self, field: str, candidates: List[str]):
        msg = f"Ambiguous field '{field}' candidates: {', '.join(candidates)}"
        super().__init__(msg)
        self.field = field
        self.candidates = candidates


@dataclass
class _Candidate:
    source: str  # root | declared | auto_add
    relation_alias: Optional[str]
    entity_name: str
    field_name: str

    def label(self) -> str:
        alias = self.relation_alias or self.entity_name
        return f"{alias}.{self.field_name}"


def _pick_best_candidate(candidates: List[_Candidate]) -> _Candidate:
    priority = {"root": 0, "declared": 1, "auto_add": 2}
    best_pri = min(priority[c.source] for c in candidates)
    best = [c for c in candidates if priority[c.source] == best_pri]
    best.sort(key=lambda c: (c.entity_name if c.source == "root" else (c.relation_alias or c.entity_name), c.field_name))
    return best[0]


def _bind_field(
    field: str,
    *,
    root_entity: str,
    declared_relations: List[str],
    schema: ProviderSchema,
    policy: ResolutionPolicy,
    selectors_relations: List[str],
    diagnostics: List[Dict[str, Any]],
) -> Tuple[str, List[str]]:
    entity_index = schema.entity_index()
    relation_index = schema.relation_index()

    def _normalize_label(value: str) -> str:
        return " ".join(value.lower().split())

    if "." in field:
        alias, col = field.split(".", 1)
        target_entity: Optional[str] = None
        rel_alias = alias

        if alias == root_entity:
            target_entity = root_entity
        elif alias in declared_relations:
            rel = relation_index.get(alias)
            if rel is None:
                raise UnknownRelation(alias)
            target_entity = rel.to_entity
        else:
            rel = relation_index.get(alias)
            if rel:
                if rel.from_entity != root_entity:
                    raise RelationNotFromRoot(alias, root_entity, rel.from_entity)
                if policy.allow_auto_add_relations and rel.name not in selectors_relations:
                    selectors_relations.append(rel.name)
                    diagnostics.append(
                        {
                            "kind": "auto_add_relation",
                            "relation": rel.name,
                            "reason": f"qualified field used undeclared relation {alias}",
                        }
                    )
                rel_alias = rel.name
                target_entity = rel.to_entity
            else:
                matching_relations = [
                    r
                    for r in schema.relations
                    if r.to_entity == alias and r.from_entity == root_entity
                ]
                if len(matching_relations) == 1:
                    rel = matching_relations[0]
                    rel_alias = rel.name
                    target_entity = rel.to_entity
                    if rel_alias not in selectors_relations and policy.allow_auto_add_relations:
                        selectors_relations.append(rel_alias)
                        diagnostics.append(
                            {
                                "kind": "auto_add_relation",
                                "relation": rel_alias,
                                "reason": f"qualified field mapped entity '{alias}' to relation alias",
                            }
                        )
                elif len(matching_relations) > 1:
                    labels = [f"{r.name}.{col}" for r in matching_relations]
                    if policy.ambiguity_strategy == "best":
                        chosen_rel = sorted(matching_relations, key=lambda r: (r.name, r.to_entity))[0]
                        rel_alias = chosen_rel.name
                        target_entity = chosen_rel.to_entity
                        diagnostics.append(
                            {
                                "kind": "ambiguous_field_best_effort",
                                "field": field,
                                "chosen": f"{rel_alias}.{col}",
                                "candidates": labels,
                            }
                        )
                        if rel_alias not in selectors_relations and policy.allow_auto_add_relations:
                            selectors_relations.append(rel_alias)
                    else:
                        raise AmbiguousField(field, labels)

                if target_entity is None:
                    normalized_alias = _normalize_label(alias)
                    matching_entities = [
                        ent
                        for ent in schema.entities
                        if _normalize_label(ent.name) == normalized_alias
                        or (ent.label is not None and _normalize_label(ent.label) == normalized_alias)
                    ]

                    if len(matching_entities) == 1:
                        ent = matching_entities[0]
                        relations_to_entity = [
                            r for r in schema.relations if r.from_entity == root_entity and r.to_entity == ent.name
                        ]
                        if len(relations_to_entity) == 1:
                            rel = relations_to_entity[0]
                            rel_alias = rel.name
                            target_entity = rel.to_entity
                            if rel_alias not in selectors_relations and policy.allow_auto_add_relations:
                                selectors_relations.append(rel_alias)
                            diagnostics.append(
                                {
                                    "kind": "mapped_qualifier_label_to_relation",
                                    "qualifier": alias,
                                    "relation": rel_alias,
                                    "reason": "mapped qualifier label to relation",
                                }
                            )

        if target_entity is None:
            if policy.unknown_qualifier_strategy == "drop":
                diagnostics.append(
                    {
                        "kind": "ignored_unknown_qualifier",
                        "qualifier": alias,
                        "field": field,
                        "reason": "ignored_unknown_qualifier",
                    }
                )
                return _bind_field(
                    col,
                    root_entity=root_entity,
                    declared_relations=declared_relations,
                    schema=schema,
                    policy=policy,
                    selectors_relations=selectors_relations,
                    diagnostics=diagnostics,
                )
            raise UnknownRelation(alias)
        entity = entity_index.get(target_entity)
        if entity is None or col not in entity.field_names():
            raise UnknownField(field, root_entity=root_entity)
        bound = f"{rel_alias}.{col}"
        return bound, selectors_relations

    # Unqualified field
    candidates: List[_Candidate] = []

    root_entity_desc = entity_index.get(root_entity)
    if root_entity_desc and field in root_entity_desc.field_names():
        candidates.append(
            _Candidate(
                source="root",
                relation_alias=root_entity,
                entity_name=root_entity,
                field_name=field,
            )
        )

    for rel_alias in declared_relations:
        rel = relation_index.get(rel_alias)
        if rel is None:
            raise UnknownRelation(rel_alias)
        target_entity = entity_index.get(rel.to_entity)
        if target_entity and field in target_entity.field_names():
            candidates.append(
                _Candidate(
                    source="declared",
                    relation_alias=rel_alias,
                    entity_name=rel.to_entity,
                    field_name=field,
                )
            )

    if not candidates and policy.allow_auto_add_relations and policy.max_auto_join_depth >= 1:
        for rel in schema.relations:
            if rel.from_entity != root_entity:
                continue
            target_entity = entity_index.get(rel.to_entity)
            if target_entity and field in target_entity.field_names():
                candidates.append(
                    _Candidate(
                        source="auto_add",
                        relation_alias=rel.name,
                        entity_name=rel.to_entity,
                        field_name=field,
                    )
                )

    if not candidates:
        raise UnknownField(field, root_entity=root_entity)

    if len(candidates) > 1:
        labels = [c.label() for c in candidates]
        if policy.ambiguity_strategy == "best":
            chosen = _pick_best_candidate(candidates)
            diagnostics.append(
                {
                    "kind": "ambiguous_field_best_effort",
                    "field": field,
                    "chosen": chosen.label(),
                    "candidates": labels,
                }
            )
            if chosen.source == "auto_add" and chosen.relation_alias not in selectors_relations:
                selectors_relations.append(chosen.relation_alias or chosen.entity_name)
            return chosen.label(), selectors_relations
        raise AmbiguousField(field, labels)

    chosen = candidates[0]
    if chosen.source == "auto_add" and chosen.relation_alias not in selectors_relations:
        selectors_relations.append(chosen.relation_alias or chosen.entity_name)
        diagnostics.append(
            {
                "kind": "auto_add_relation",
                "relation": chosen.relation_alias,
                "reason": f"field {field} found on to_entity {chosen.entity_name}",
            }
        )
    return chosen.label(), selectors_relations


def _bind_filter(
    filt: Dict[str, Any],
    *,
    root_entity: str,
    declared_relations: List[str],
    schema: ProviderSchema,
    policy: ResolutionPolicy,
    selectors_relations: List[str],
    diagnostics: List[Dict[str, Any]],
) -> List[str]:
    op_type = filt.get("type")
    updated_relations = selectors_relations
    if op_type == "logical":
        clauses = filt.get("clauses", [])
        for clause in clauses:
            updated_relations = _bind_filter(
                clause,
                root_entity=root_entity,
                declared_relations=declared_relations,
                schema=schema,
                policy=policy,
                selectors_relations=updated_relations,
                diagnostics=diagnostics,
            )
    elif op_type == "comparison":
        entity_hint = filt.get("entity")
        if isinstance(entity_hint, str) and entity_hint and entity_hint != root_entity:
            relation_index = schema.relation_index()
            candidates = []
            rel_by_alias = relation_index.get(entity_hint)
            if rel_by_alias:
                if rel_by_alias.from_entity != root_entity:
                    raise RelationNotFromRoot(
                        entity_hint, root_entity, rel_by_alias.from_entity
                    )
                candidates = [rel_by_alias]
            else:
                candidates = [
                    rel
                    for rel in schema.relations
                    if rel.from_entity == root_entity and rel.to_entity == entity_hint
                ]

            if not candidates:
                raise UnknownRelation(entity_hint)

            chosen_rel = None
            if len(candidates) == 1:
                chosen_rel = candidates[0]
            else:
                labels = [r.name for r in candidates]
                if policy.ambiguity_strategy == "best":
                    chosen_rel = sorted(candidates, key=lambda r: (r.name, r.to_entity))[0]
                    diagnostics.append(
                        {
                            "kind": "ambiguous_field_best_effort",
                            "field": entity_hint,
                            "chosen": chosen_rel.name,
                            "candidates": labels,
                        }
                    )
                else:
                    raise AmbiguousField(entity_hint, labels)

            rel_alias = chosen_rel.name
            if rel_alias not in updated_relations:
                if not policy.allow_auto_add_relations:
                    raise UnknownRelation(rel_alias)
                updated_relations = list(updated_relations) + [rel_alias]
                diagnostics.append(
                    {
                        "kind": "auto_add_relation",
                        "relation": rel_alias,
                        "reason": "filter entity hinted relation",
                    }
                )

        field = filt.get("field")
        if isinstance(field, str):
            bound, updated_relations = _bind_field(
                field,
                root_entity=root_entity,
                declared_relations=declared_relations,
                schema=schema,
                policy=policy,
                selectors_relations=updated_relations,
                diagnostics=diagnostics,
            )
            filt["field"] = bound
            if bound != field:
                diagnostics.append(
                    {
                        "kind": "bound_field",
                        "from": field,
                        "to": bound,
                        "reason": "resolved against schema",
                        "context": "filters",
                    }
                )
    return updated_relations


def bind_selectors(
    schema: ProviderSchema,
    selectors: Dict[str, Any],
    policy: Optional[ResolutionPolicy] = None,
    capabilities: Optional[Iterable[str]] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Bind selectors to provider schema (v1).

    Returns a tuple of (updated selectors, diagnostics).
    """

    policy = policy or ResolutionPolicy()
    if selectors.get("op") != "query":
        return selectors, []

    updated = copy.deepcopy(selectors)
    diagnostics: List[Dict[str, Any]] = []

    root_entity = updated.get("root_entity")
    declared_relations: List[str] = list(updated.get("relations", []) or [])
    if not root_entity:
        return updated, diagnostics

    relation_index = schema.relation_index()
    for rel_alias in declared_relations:
        rel = relation_index.get(rel_alias)
        if rel is None:
            raise UnknownRelation(rel_alias)
        if rel.from_entity != root_entity:
            raise RelationNotFromRoot(rel_alias, root_entity, rel.from_entity)

    # Bind select expressions
    for sel in updated.get("select", []) or []:
        expr = sel.get("expr") if isinstance(sel, dict) else None
        if isinstance(expr, str):
            bound, declared_relations = _bind_field(
                expr,
                root_entity=root_entity,
                declared_relations=declared_relations,
                schema=schema,
                policy=policy,
                selectors_relations=declared_relations,
                diagnostics=diagnostics,
            )
            if bound != expr:
                diagnostics.append(
                    {
                        "kind": "bound_field",
                        "from": expr,
                        "to": bound,
                        "reason": "resolved against schema",
                    }
                )
            sel["expr"] = bound

    filt = updated.get("filters")
    if isinstance(filt, dict):
        declared_relations = _bind_filter(
            filt,
            root_entity=root_entity,
            declared_relations=declared_relations,
            schema=schema,
            policy=policy,
            selectors_relations=declared_relations,
            diagnostics=diagnostics,
        )

    for grp in updated.get("group_by", []) or []:
        field = grp.get("field") if isinstance(grp, dict) else None
        if isinstance(field, str):
            bound, declared_relations = _bind_field(
                field,
                root_entity=root_entity,
                declared_relations=declared_relations,
                schema=schema,
                policy=policy,
                selectors_relations=declared_relations,
                diagnostics=diagnostics,
            )
            if bound != field:
                diagnostics.append(
                    {
                        "kind": "bound_field",
                        "from": field,
                        "to": bound,
                        "reason": "resolved against schema",
                    }
                )
            grp["field"] = bound

    for agg in updated.get("aggregations", []) or []:
        field = agg.get("field") if isinstance(agg, dict) else None
        if isinstance(field, str):
            bound, declared_relations = _bind_field(
                field,
                root_entity=root_entity,
                declared_relations=declared_relations,
                schema=schema,
                policy=policy,
                selectors_relations=declared_relations,
                diagnostics=diagnostics,
            )
            if bound != field:
                diagnostics.append(
                    {
                        "kind": "bound_field",
                        "from": field,
                        "to": bound,
                        "reason": "resolved against schema",
                    }
                )
            agg["field"] = bound

    for ob in updated.get("order_by", []) or []:
        field = ob.get("field") if isinstance(ob, dict) else None
        if isinstance(field, str):
            bound, declared_relations = _bind_field(
                field,
                root_entity=root_entity,
                declared_relations=declared_relations,
                schema=schema,
                policy=policy,
                selectors_relations=declared_relations,
                diagnostics=diagnostics,
            )
            if bound != field:
                diagnostics.append(
                    {
                        "kind": "bound_field",
                        "from": field,
                        "to": bound,
                        "reason": "resolved against schema",
                    }
                )
            ob["field"] = bound

    updated["relations"] = declared_relations
    return updated, diagnostics
