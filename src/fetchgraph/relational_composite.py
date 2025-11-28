from __future__ import annotations

"""Composite relational provider that delegates to child providers."""

from typing import Dict, List, Optional

from .json_types import SelectorsDict
from .relational_base import RelationalDataProvider
from .relational_models import (
    ComparisonFilter,
    EntityDescriptor,
    FilterClause,
    LogicalFilter,
    QueryResult,
    RelationDescriptor,
    RelationalQuery,
    SemanticOnlyRequest,
    SemanticOnlyResult,
)


class CompositeRelationalProvider(RelationalDataProvider):
    """Composite provider delegating to child relational providers."""

    def __init__(self, name: str, children: Dict[str, RelationalDataProvider]):
        entities: List[EntityDescriptor] = []
        relations: List[RelationDescriptor] = []
        for child in children.values():
            entities.extend(child.entities)
            relations.extend(child.relations)
        super().__init__(name=name, entities=entities, relations=relations)
        self.children = children

    def fetch(self, feature_name: str, selectors: Optional[SelectorsDict] = None, **kwargs):
        selectors = selectors or {}
        op = selectors.get("op")
        if op != "query":
            return super().fetch(feature_name, selectors, **kwargs)
        req = RelationalQuery.model_validate(selectors)
        child_name, target = self._choose_child(req)
        result = target.fetch(feature_name, selectors, **kwargs)
        if isinstance(result, QueryResult):
            result.meta.setdefault("provider", target.name)
            result.meta.setdefault("child_provider", child_name)
        return result

    def _choose_child(self, req: RelationalQuery) -> tuple[str, RelationalDataProvider]:
        involved_entities = {req.root_entity}
        if req.filters:
            involved_entities.update(self._collect_entities_from_filter(req.filters, req.root_entity))
        for clause in req.semantic_clauses:
            involved_entities.add(clause.entity)
        for grp in req.group_by:
            if grp.entity:
                involved_entities.add(grp.entity)
        candidates = [
            (name, prov)
            for name, prov in self.children.items()
            if all(e in getattr(prov, "_entity_index", {}) for e in involved_entities)
        ]
        if not candidates:
            raise NotImplementedError("Cross-provider joins are not supported yet")
        return candidates[0]

    def _collect_entities_from_filter(self, clause: FilterClause, root_entity: str) -> List[str]:
        if isinstance(clause, ComparisonFilter):
            if clause.entity:
                return [clause.entity]
            if "." in clause.field:
                return [clause.field.split(".", 1)[0]]
            return [root_entity]

        if isinstance(clause, LogicalFilter):
            entities: List[str] = []
            for sub in clause.clauses:
                entities.extend(self._collect_entities_from_filter(sub, root_entity))
            return entities

        return [root_entity]

    def _handle_semantic_only(self, req: SemanticOnlyRequest) -> SemanticOnlyResult:
        for child in self.children.values():
            if req.entity in getattr(child, "_entity_index", {}):
                return child._handle_semantic_only(req)
        raise KeyError(f"Entity '{req.entity}' not found in any child provider")

    def _handle_query(self, req: RelationalQuery):
        _, target = self._choose_child(req)
        return target._handle_query(req)

    def describe(self):
        info = super().describe()
        info.description = (
            info.description
            + " (Composite: routes requests to child providers; cross-provider joins are not supported)"
        )
        info.capabilities = sorted(set(info.capabilities + ["single_provider_routing"]))
        return info


__all__ = ["CompositeRelationalProvider"]
