from __future__ import annotations

"""Composite relational provider that delegates to child providers."""

from typing import Any, Dict, List, Optional, Set

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
    RowResult,
    SemanticOnlyRequest,
    SemanticOnlyResult,
)


class CompositeRelationalProvider(RelationalDataProvider):
    """Composite provider delegating to child relational providers."""

    def __init__(
        self,
        name: str,
        children: Dict[str, RelationalDataProvider],
        max_join_rows_per_batch: int = 1000,
        max_right_rows_per_batch: int = 5000,
        max_join_bytes: Optional[int] = None,
    ):
        entities: List[EntityDescriptor] = []
        relations: List[RelationDescriptor] = []
        for child in children.values():
            entities.extend(child.entities)
            relations.extend(child.relations)
        super().__init__(name=name, entities=entities, relations=relations)
        self.children = children
        self.max_join_rows_per_batch = max_join_rows_per_batch
        self.max_right_rows_per_batch = max_right_rows_per_batch
        self.max_join_bytes = max_join_bytes

        self._entity_to_provider: Dict[str, str] = {}
        for child_name, child in children.items():
            for ent in getattr(child, "_entity_index", {}):
                self._entity_to_provider[ent] = child_name

        self._relation_index: Dict[str, RelationDescriptor] = {
            rel.name: rel for rel in relations
        }
        self._cross_relations: Dict[str, RelationDescriptor] = {
            name: rel
            for name, rel in self._relation_index.items()
            if self._entity_to_provider.get(rel.from_entity)
            != self._entity_to_provider.get(rel.to_entity)
        }

    def fetch(self, feature_name: str, selectors: Optional[SelectorsDict] = None, **kwargs):
        selectors = selectors or {}
        op = selectors.get("op")
        if op != "query":
            return super().fetch(feature_name, selectors, **kwargs)
        req = RelationalQuery.model_validate(selectors)
        child_choice = self._choose_child(req)
        if child_choice is None:
            return self._execute_cross_provider_query(req, feature_name, **kwargs)
        child_name, target = child_choice
        result = target.fetch(feature_name, selectors, **kwargs)
        if isinstance(result, QueryResult):
            result.meta.setdefault("provider", target.name)
            result.meta.setdefault("child_provider", child_name)
        return result

    def _choose_child(
        self, req: RelationalQuery
    ) -> Optional[tuple[str, RelationalDataProvider]]:
        involved_entities = self._collect_involved_entities(req)
        for name, prov in self.children.items():
            if all(e in getattr(prov, "_entity_index", {}) for e in involved_entities):
                return name, prov
        return None

    # --- Cross-provider execution helpers ---
    def _execute_cross_provider_query(
        self, req: RelationalQuery, feature_name: str, **kwargs
    ) -> QueryResult:
        if req.group_by or req.aggregations:
            raise NotImplementedError("Aggregations/group by across providers are not supported")

        filter_entities: Set[str] = set()
        if req.filters:
            filter_entities.update(self._collect_entities_from_filter(req.filters, req.root_entity))
        for ent in filter_entities:
            if ent != req.root_entity and self._entity_to_provider.get(ent) != self._entity_to_provider.get(req.root_entity):
                raise NotImplementedError("Filters on non-root providers are not supported for cross-provider joins")
        for clause in req.semantic_clauses:
            if clause.entity != req.root_entity:
                raise NotImplementedError("Semantic clauses across providers are not supported")

        root_provider_name = self._entity_to_provider.get(req.root_entity)
        if not root_provider_name:
            raise KeyError(f"Root entity '{req.root_entity}' not found in any child provider")
        root_provider = self.children[root_provider_name]

        local_relations: List[str] = []
        cross_relation: Optional[RelationDescriptor] = None
        for idx, rel_name in enumerate(req.relations):
            rel = self._relation_index.get(rel_name)
            if not rel:
                raise KeyError(f"Relation '{rel_name}' not found")
            from_provider = self._entity_to_provider.get(rel.from_entity)
            to_provider = self._entity_to_provider.get(rel.to_entity)
            if from_provider == to_provider == root_provider_name and cross_relation is None:
                local_relations.append(rel_name)
                continue
            if from_provider != to_provider:
                if cross_relation is not None:
                    raise NotImplementedError("Multiple cross-provider relations are not supported")
                cross_relation = rel
                if idx < len(req.relations) - 1:
                    raise NotImplementedError("Relations after a cross-provider join are not supported")
                continue
            raise NotImplementedError("Relations involving non-root providers are not supported before crossing")

        if cross_relation is None:
            raise NotImplementedError("Cross-provider join boundary not found")

        remaining = req.limit
        offset = req.offset or 0
        all_rows: List[RowResult] = []
        while True:
            if remaining is not None and remaining <= 0:
                break
            batch_limit = self.max_join_rows_per_batch
            if remaining is not None:
                batch_limit = min(batch_limit, remaining)
            local_req = req.model_copy(
                update={
                    "relations": local_relations,
                    "offset": offset,
                    "limit": batch_limit,
                }
            )
            local_result = root_provider.fetch(
                feature_name, selectors=local_req.model_dump(), **kwargs
            )
            if not isinstance(local_result, QueryResult):
                raise TypeError("Expected QueryResult from child provider")
            left_rows = local_result.rows
            if len(left_rows) > self.max_join_rows_per_batch:
                raise MemoryError("Left join batch exceeds maximum allowed rows")
            if not left_rows:
                break

            joined_rows = self._join_batch_with_remote(
                left_rows, req, cross_relation, feature_name, **kwargs
            )

            for row in joined_rows:
                if remaining is not None and len(all_rows) >= req.limit:
                    break
                all_rows.append(row)
            if remaining is not None:
                remaining = req.limit - len(all_rows)
                if remaining <= 0:
                    break
            offset += len(left_rows)
            if len(left_rows) < batch_limit:
                break

        meta = {
            "composite": True,
            "root_provider": root_provider_name,
            "cross_relation": cross_relation.name,
        }
        return QueryResult(rows=all_rows, meta=meta)

    def _join_batch_with_remote(
        self,
        left_rows: List[RowResult],
        req: RelationalQuery,
        cross_relation: RelationDescriptor,
        feature_name: str,
        **kwargs,
    ) -> List[RowResult]:
        join_type = cross_relation.join.join_type
        if join_type not in {"inner", "left"}:
            raise NotImplementedError(f"Join type '{join_type}' is not supported for cross-provider joins")

        root_provider_name = self._entity_to_provider[req.root_entity]
        left_entity: str
        right_entity: str
        left_col: str
        right_col: str
        if self._entity_to_provider.get(cross_relation.join.from_entity) == root_provider_name:
            left_entity = cross_relation.join.from_entity
            right_entity = cross_relation.join.to_entity
            left_col = cross_relation.join.from_column
            right_col = cross_relation.join.to_column
        else:
            left_entity = cross_relation.join.to_entity
            right_entity = cross_relation.join.from_entity
            left_col = cross_relation.join.to_column
            right_col = cross_relation.join.from_column

        join_keys: List[Any] = []
        for row in left_rows:
            value = self._extract_value(row, left_entity, left_col)
            join_keys.append(value)
        unique_keys = list({k for k in join_keys if k is not None})

        right_provider_name = self._entity_to_provider.get(right_entity)
        if not right_provider_name:
            raise KeyError(f"Entity '{right_entity}' not found in any child provider")
        right_provider = self.children[right_provider_name]

        right_results: Dict[Any, List[RowResult]] = {}

        def chunks(iterable: List[Any], size: int):
            for i in range(0, len(iterable), size):
                yield iterable[i : i + size]

        for key_chunk in chunks(unique_keys, self.max_right_rows_per_batch):
            comparison = ComparisonFilter(entity=right_entity, field=right_col, op="in", value=key_chunk)
            remote_req = RelationalQuery(
                root_entity=right_entity,
                filters=comparison,
                relations=[],
                select=[],
                limit=min(self.max_right_rows_per_batch, len(key_chunk) if key_chunk else self.max_right_rows_per_batch),
                offset=0,
            )
            remote_result = right_provider.fetch(
                feature_name, selectors=remote_req.model_dump(), **kwargs
            )
            if not isinstance(remote_result, QueryResult):
                raise TypeError("Expected QueryResult from right provider")
            if len(remote_result.rows) > self.max_right_rows_per_batch:
                raise MemoryError("Right join batch exceeds maximum allowed rows")
            for row in remote_result.rows:
                key = self._extract_value(row, right_entity, right_col)
                right_results.setdefault(key, []).append(row)

        joined: List[RowResult] = []
        for row, key in zip(left_rows, join_keys):
            matches = right_results.get(key, [])
            if not matches:
                if join_type == "inner":
                    continue
                joined.append(row)
                continue
            for match in matches:
                related = {k: dict(v) for k, v in row.related.items()}
                related[right_entity] = match.data
                joined.append(
                    RowResult(
                        entity=row.entity,
                        data=dict(row.data),
                        related=related,
                    )
                )
        return joined

    def _extract_value(self, row: RowResult, entity: str, field: str) -> Any:
        if row.entity == entity:
            return row.data.get(field)
        return row.related.get(entity, {}).get(field)

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
        child_choice = self._choose_child(req)
        if child_choice is None:
            raise NotImplementedError("Cross-provider joins require full fetch handling")
        _, target = child_choice
        return target._handle_query(req)

    def _collect_involved_entities(self, req: RelationalQuery) -> Set[str]:
        involved_entities: Set[str] = {req.root_entity}
        if req.filters:
            involved_entities.update(
                self._collect_entities_from_filter(req.filters, req.root_entity)
            )
        for clause in req.semantic_clauses:
            involved_entities.add(clause.entity)
        for grp in req.group_by:
            if grp.entity:
                involved_entities.add(grp.entity)
        return involved_entities

    def describe(self):
        info = super().describe()
        info.description = info.description + (
            " (Composite: routes requests to child providers; limited cross-provider join support)"
        )
        info.capabilities = sorted(
            set(info.capabilities + ["single_provider_routing", "cross_provider_join"])
        )
        return info


__all__ = ["CompositeRelationalProvider"]
