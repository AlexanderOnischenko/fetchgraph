from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Tuple

from .ast import NormalizedQuerySketch
from .bind_noop import bound_from_normalized
from .bound import BoundClause, BoundWhereExpr, FieldRef, JoinPath, parse_field_ref
from .diagnostics import Diagnostics, Severity
from .resolution_policy import ResolutionPolicy
from .schema_registry import FieldCandidate, SchemaRegistry, _normalize_name


@dataclass
class _ResolutionResult:
    field_ref: FieldRef
    join_path: Tuple[str, ...]
    reason: str


_DEF_REASON_ROOT = "root"
_DEF_REASON_DECLARED = "declared"
_DEF_REASON_AUTO = "auto"


def bind_query_sketch(
    sketch: NormalizedQuerySketch,
    registry: SchemaRegistry,
    policy: ResolutionPolicy,
) -> tuple[BoundQuery, Diagnostics]:
    bound = bound_from_normalized(sketch)
    diagnostics = Diagnostics()

    bindings: List[dict] = []

    def record_binding(input_raw: str, result: _ResolutionResult, entity: str) -> None:
        bindings.append(
            {
                "input": input_raw,
                "output": result.field_ref.raw,
                "entity": entity,
                "join_path": list(result.join_path),
                "reason": result.reason,
            }
        )

    def add_relations(join_path: Iterable[str]) -> None:
        if not policy.allow_auto_add_relations:
            return
        for rel in join_path:
            if rel not in bound.with_:
                bound.with_.append(rel)

    def rewrite_field(field_ref: FieldRef, join_path: Tuple[str, ...], column_name: str, entity_name: str) -> FieldRef:
        if join_path:
            qualifier = join_path[-1]
            raw = f"{qualifier}.{column_name}"
        else:
            qualifier = None
            raw = column_name
        updated = parse_field_ref(raw)
        updated.entity = entity_name
        field_ref.raw = updated.raw
        field_ref.qualifier = updated.qualifier
        field_ref.field = updated.field
        field_ref.entity = updated.entity
        return field_ref

    def resolve_qualified(field_ref: FieldRef, qualifier: str) -> _ResolutionResult | None:
        norm_qualifier = _normalize_name(qualifier)
        if norm_qualifier in registry.relations_by_name:
            relation = registry.relations_by_name[norm_qualifier]
            paths = registry.find_paths(bound.from_, relation.from_entity, max_depth=policy.max_auto_join_depth)
            if not paths:
                diagnostics.add(
                    code="DSL_BIND_RELATION_PATH_NOT_FOUND",
                    message=f"No path from root {bound.from_!r} to relation source {relation.from_entity!r}",
                    path=field_ref.raw,
                    severity=Severity.ERROR,
                )
                return None
            join_path = paths[0] + (relation.name,)
            if not registry.has_field(relation.to_entity, field_ref.field):
                diagnostics.add(
                    code="DSL_BIND_FIELD_NOT_FOUND",
                    message=f"Field {field_ref.field!r} not found on entity {relation.to_entity!r}",
                    path=field_ref.raw,
                    severity=Severity.ERROR,
                )
                return None
            column = registry.field(relation.to_entity, field_ref.field)
            rewrite_field(field_ref, join_path, column.name, registry.entity(relation.to_entity).name)
            return _ResolutionResult(field_ref=field_ref, join_path=join_path, reason=_DEF_REASON_DECLARED)

        if not registry.has_entity(qualifier):
            diagnostics.add(
                code="DSL_BIND_UNKNOWN_QUALIFIER",
                message=f"Unknown qualifier {qualifier!r}",
                path=field_ref.raw,
                severity=Severity.ERROR,
            )
            return None

        paths = registry.find_paths(bound.from_, qualifier, max_depth=policy.max_auto_join_depth)
        if not paths:
            diagnostics.add(
                code="DSL_BIND_RELATION_PATH_NOT_FOUND",
                message=f"No path from root {bound.from_!r} to entity {qualifier!r}",
                path=field_ref.raw,
                severity=Severity.ERROR,
            )
            return None

        join_path = paths[0]
        if not registry.has_field(qualifier, field_ref.field):
            diagnostics.add(
                code="DSL_BIND_FIELD_NOT_FOUND",
                message=f"Field {field_ref.field!r} not found on entity {qualifier!r}",
                path=field_ref.raw,
                severity=Severity.ERROR,
            )
            return None
        column = registry.field(qualifier, field_ref.field)
        entity_name = registry.entity(qualifier).name
        rewrite_field(field_ref, join_path, column.name, entity_name)
        return _ResolutionResult(field_ref=field_ref, join_path=join_path, reason=_reason_for_path(join_path))

    def choose_best_candidate(field_ref: FieldRef) -> FieldCandidate | None:
        declared_with = bound.with_ if policy.prefer_declared_relations else None
        candidates = registry.field_candidates(
            bound.from_,
            field_ref.field,
            max_depth=policy.max_auto_join_depth,
            declared_with=declared_with,
        )
        if not candidates:
            diagnostics.add(
                code="DSL_BIND_FIELD_NOT_FOUND",
                message=f"Field {field_ref.field!r} not found on root or reachable entities",
                path=field_ref.raw,
                severity=Severity.ERROR,
            )
            return None

        best = candidates[0]
        if len(candidates) > 1:
            best_priority = _priority_key(best, declared_with)
            competing = [c for c in candidates if _priority_key(c, declared_with) == best_priority]
            if len(competing) > 1:
                message = (
                    f"Ambiguous field {field_ref.field!r}: candidates on "
                    + ", ".join(f"{c.entity} via {'/'.join(c.join_path) or 'root'}" for c in competing)
                )
                severity = Severity.ERROR if policy.ambiguity_strategy == "ask" else Severity.WARNING
                diagnostics.add(
                    code="DSL_BIND_AMBIGUOUS_FIELD",
                    message=message,
                    path=field_ref.raw,
                    severity=severity,
                )
                if policy.ambiguity_strategy == "ask":
                    return None
                best = competing[0]
        return best

    def resolve_unqualified(field_ref: FieldRef) -> _ResolutionResult | None:
        candidate = choose_best_candidate(field_ref)
        if candidate is None:
            return None

        reason = _reason_for_path(candidate.join_path)
        column = registry.field(candidate.entity, candidate.field)
        rewrite_field(field_ref, candidate.join_path, column.name, registry.entity(candidate.entity).name)
        return _ResolutionResult(field_ref=field_ref, join_path=candidate.join_path, reason=reason)

    def _reason_for_path(join_path: Tuple[str, ...]) -> str:
        if not join_path:
            return _DEF_REASON_ROOT
        declared_norm = tuple(_normalize_name(r) for r in bound.with_)
        normalized_path = tuple(_normalize_name(r) for r in join_path)
        if declared_norm and normalized_path[: len(declared_norm)] == declared_norm:
            return _DEF_REASON_DECLARED
        return _DEF_REASON_AUTO

    def process_field(field_ref: FieldRef, location: str) -> _ResolutionResult | None:
        input_raw = field_ref.raw
        if field_ref.raw == "*":
            return None

        if field_ref.qualifier is not None:
            result = resolve_qualified(field_ref, field_ref.qualifier)
        else:
            result = resolve_unqualified(field_ref)

        if result is None:
            return None

        add_relations(result.join_path)
        record_binding(input_raw, result, field_ref.entity or "")
        return result

    def process_clause(clause: BoundClause, location: str) -> None:
        result = process_field(clause.field, location)
        if result is not None:
            clause.join_path = JoinPath(list(result.join_path))

    def walk_where(expr: BoundWhereExpr, path_prefix: str) -> None:
        for idx, item in enumerate(expr.all):
            if isinstance(item, BoundClause):
                process_clause(item, f"{path_prefix}.all[{idx}]")
            else:
                walk_where(item, f"{path_prefix}.all[{idx}]")
        for idx, item in enumerate(expr.any):
            if isinstance(item, BoundClause):
                process_clause(item, f"{path_prefix}.any[{idx}]")
            else:
                walk_where(item, f"{path_prefix}.any[{idx}]")
        if expr.not_ is not None:
            if isinstance(expr.not_, BoundClause):
                process_clause(expr.not_, f"{path_prefix}.not")
            else:
                walk_where(expr.not_, f"{path_prefix}.not")

    for idx, field in enumerate(bound.get):
        process_field(field, f"get[{idx}]")

    walk_where(bound.where, "where")

    if bindings:
        bound.meta.setdefault("bindings", bindings)

    return bound, diagnostics


def _candidate_key(candidate: FieldCandidate, declared_with: List[str] | None) -> tuple:
    declared_norm = tuple(_normalize_name(r) for r in declared_with) if declared_with else tuple()
    normalized_path = tuple(_normalize_name(r) for r in candidate.join_path)
    declared_first = 0 if declared_norm and normalized_path[: len(declared_norm)] == declared_norm else 1
    return (
        len(candidate.join_path),
        declared_first,
        candidate.join_path,
        candidate.entity,
        candidate.field,
    )


def _priority_key(candidate: FieldCandidate, declared_with: List[str] | None) -> tuple[int, int]:
    declared_norm = tuple(_normalize_name(r) for r in declared_with) if declared_with else tuple()
    normalized_path = tuple(_normalize_name(r) for r in candidate.join_path)
    declared_first = 0 if declared_norm and normalized_path[: len(declared_norm)] == declared_norm else 1
    return (len(candidate.join_path), declared_first)
