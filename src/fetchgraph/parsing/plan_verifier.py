from __future__ import annotations

from typing import Dict, List, Sequence

from ..models import Plan
from ..protocols import ContextProvider, SupportsDescribe
from ..relational_models import (
    AggregationSpec,
    ComparisonFilter,
    FilterClause,
    GroupBySpec,
    LogicalFilter,
    RelationalQuery,
    SemanticClause,
)


class PlanVerifier:
    name = "plan_verifier"

    def check(
        self, plan: Plan, providers: Dict[str, ContextProvider]
    ) -> List[str]:
        errors: List[str] = []
        for spec in plan.context_plan or []:
            provider = providers.get(spec.provider)
            if provider is None:
                errors.append(f"Provider {spec.provider!r} not found for plan step")
                continue

            selectors = spec.selectors or {}
            if selectors.get("op") != "query":
                continue
            try:
                req = RelationalQuery.model_validate(selectors)
            except Exception as e:  # pragma: no cover - defensive
                errors.append(
                    f"[{spec.provider}] selectors are not a valid RelationalQuery: {e}"
                )
                continue

            caps = self._provider_capabilities(provider)

            if "root_scoped_filters" in caps:
                errors += self._check_filters(req.filters, req.root_entity, spec.provider)
            if "root_scoped_semantic" in caps:
                errors += self._check_semantic(req.semantic_clauses, req.root_entity, spec.provider)
            if "root_scoped_groupby" in caps:
                errors += self._check_group_by(req.group_by, req.root_entity, spec.provider)
            if "limited_aggregations" in caps:
                errors += self._check_aggregations(req.aggregations, spec.provider)

            verify = getattr(provider, "verify_query", None)
            if callable(verify):
                try:
                    verify_result = verify(req)
                    if verify_result:
                        errors.extend(list(verify_result))
                except Exception as e:  # pragma: no cover - defensive
                    errors.append(f"[{spec.provider}] verify_query failed: {e}")

        return errors

    @staticmethod
    def _provider_capabilities(provider: ContextProvider) -> set[str]:
        if isinstance(provider, SupportsDescribe):
            try:
                info = provider.describe()
                return set(info.capabilities or [])
            except Exception:
                return set()
        return set()

    @staticmethod
    def _check_filters(
        clause: FilterClause | None, root_entity: str, provider: str
    ) -> List[str]:
        errors: List[str] = []
        if clause is None:
            return errors
        if isinstance(clause, LogicalFilter):
            for c in clause.clauses:
                errors += PlanVerifier._check_filters(c, root_entity, provider)
            return errors
        if isinstance(clause, ComparisonFilter):
            if clause.entity and clause.entity != root_entity:
                errors.append(
                    f"[{provider}] filters must target root_entity={root_entity}, got entity={clause.entity}"
                )
            if "." in clause.field:
                entity_hint = clause.field.split(".", 1)[0]
                if entity_hint and entity_hint != root_entity:
                    errors.append(
                        f"[{provider}] filter field {clause.field!r} hints at entity {entity_hint!r}, expected root_entity={root_entity}"
                    )
            return errors
        errors.append(f"[{provider}] unsupported filter clause type {type(clause).__name__}")
        return errors

    @staticmethod
    def _check_semantic(
        clauses: Sequence[SemanticClause], root_entity: str, provider: str
    ) -> List[str]:
        errors: List[str] = []
        for clause in clauses or []:
            if clause.entity != root_entity:
                errors.append(
                    f"[{provider}] semantic_clauses must target root_entity={root_entity}, got entity={clause.entity}"
                )
        return errors

    @staticmethod
    def _check_group_by(
        group_by: Sequence[GroupBySpec], root_entity: str, provider: str
    ) -> List[str]:
        errors: List[str] = []
        for gb in group_by or []:
            if gb.entity and gb.entity != root_entity:
                errors.append(
                    f"[{provider}] group_by.entity must be empty or {root_entity}, got {gb.entity}"
                )
        return errors

    @staticmethod
    def _check_aggregations(
        aggs: Sequence[AggregationSpec], provider: str
    ) -> List[str]:
        errors: List[str] = []
        allowed = {"count", "count_distinct", "sum", "min", "max", "avg"}
        for agg in aggs or []:
            if agg.agg not in allowed:
                errors.append(
                    f"[{provider}] aggregation '{agg.agg}' is not supported; allowed: {sorted(allowed)}"
                )
        return errors


__all__ = ["PlanVerifier"]
