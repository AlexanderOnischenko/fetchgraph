from __future__ import annotations

"""Composite relational provider that delegates to child providers."""

from typing import Any, Dict, List, Optional, Set, Tuple

from .json_types import SelectorsDict
from .relational_base import RelationalDataProvider
from .relational_models import (
    AggregationResult,
    AggregationSpec,
    ComparisonFilter,
    EntityDescriptor,
    FilterClause,
    GroupBySpec,
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
        # TODO: enforce max_join_bytes when estimating join materialization size

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

    def _plan_cross_provider(
        self, req: RelationalQuery
    ) -> Tuple[str, RelationalDataProvider, List[str], RelationDescriptor]:
        filter_entities: Set[str] = set()
        if req.filters:
            filter_entities.update(
                self._collect_entities_from_filter(req.filters, req.root_entity)
            )
        for ent in filter_entities:
            if ent != req.root_entity and self._entity_to_provider.get(ent) != self._entity_to_provider.get(req.root_entity):
                raise NotImplementedError(
                    "Filters on non-root providers are not supported for cross-provider joins"
                )
        for clause in req.semantic_clauses:
            if clause.entity != req.root_entity:
                raise NotImplementedError("Semantic clauses across providers are not supported")

        root_provider_name = self._entity_to_provider.get(req.root_entity)
        if not root_provider_name:
            raise KeyError(
                f"Root entity '{req.root_entity}' not found in any child provider"
            )
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
                    raise NotImplementedError(
                        "Multiple cross-provider relations are not supported"
                    )
                cross_relation = rel
                if idx < len(req.relations) - 1:
                    raise NotImplementedError(
                        "Relations after a cross-provider join are not supported"
                    )
                continue
            raise NotImplementedError(
                "Relations involving non-root providers are not supported before crossing"
            )

        if cross_relation is None:
            raise NotImplementedError("Cross-provider join boundary not found")
        return root_provider_name, root_provider, local_relations, cross_relation

    # --- Cross-provider execution helpers ---
    def _execute_cross_provider_query(
        self, req: RelationalQuery, feature_name: str, **kwargs
    ) -> QueryResult:
        (
            root_provider_name,
            root_provider,
            local_relations,
            cross_relation,
        ) = self._plan_cross_provider(req)

        if req.group_by or req.aggregations:
            return self._execute_cross_provider_aggregate(
                req,
                feature_name,
                root_provider_name,
                root_provider,
                local_relations,
                cross_relation,
                **kwargs,
            )

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
                    # Clear select to avoid pushing remote-field projections to
                    # the root provider; selection can be applied after join if
                    # needed.
                    "select": [],
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
            # NOTE: offset and limit are applied to the root-entity rows prior to
            # join expansion. Joined-row offsets are not currently supported for
            # cross-provider joins.
            offset += len(left_rows)
            if len(left_rows) < batch_limit:
                break

        meta = {
            "composite": True,
            "root_provider": root_provider_name,
            "cross_relation": cross_relation.name,
            "relations_used": req.relations,
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

        effective_cardinality = self._effective_cardinality(
            cross_relation, left_entity, right_entity
        )

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
        single_right: Dict[Any, RowResult] = {}

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
                limit=self._remote_limit_for_cardinality(
                    effective_cardinality, len(key_chunk)
                ),
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
                if effective_cardinality in {"1_to_1", "many_to_1"}:
                    if key in single_right:
                        raise ValueError(
                            f"Cardinality {effective_cardinality} violated for relation '{cross_relation.name}'"
                        )
                    single_right[key] = row
                else:
                    bucket = right_results.setdefault(key, [])
                    bucket.append(row)
                    if len(bucket) > self.max_right_rows_per_batch:
                        raise MemoryError(
                            "Right join batch exceeds maximum allowed rows for key"
                        )
            if effective_cardinality in {"1_to_1", "many_to_1"} and len(remote_result.rows) > len(key_chunk):
                raise ValueError(
                    f"Cardinality {effective_cardinality} violated for relation '{cross_relation.name}'"
                )

        joined: List[RowResult] = []
        for row, key in zip(left_rows, join_keys):
            if effective_cardinality in {"1_to_1", "many_to_1"}:
                match_list = [single_right[key]] if key in single_right else []
            else:
                match_list = right_results.get(key, [])
            matches = match_list
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

    def _execute_cross_provider_aggregate(
        self,
        req: RelationalQuery,
        feature_name: str,
        root_provider_name: str,
        root_provider: RelationalDataProvider,
        local_relations: List[str],
        cross_relation: RelationDescriptor,
        **kwargs,
    ) -> QueryResult:
        if req.group_by:
            for grp in req.group_by:
                if grp.entity and grp.entity != req.root_entity:
                    raise NotImplementedError(
                        "Cross-provider aggregations: group_by on non-root entities is not supported"
                    )
        for clause in req.semantic_clauses:
            if clause.entity != req.root_entity:
                raise NotImplementedError(
                    "Semantic clauses across providers are not supported"
                )

        default_count = bool(req.group_by and not req.aggregations)
        if req.group_by:
            group_state: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        else:
            group_state = {(): {}}

        def ensure_state(key: Tuple[Any, ...]):
            if key not in group_state:
                group_state[key] = {}
            return group_state[key]

        root_offset = 0
        while True:
            batch_limit = self.max_join_rows_per_batch
            local_req = req.model_copy(
                update={
                    "relations": local_relations,
                    "offset": root_offset,
                    "limit": batch_limit,
                    "group_by": [],
                    "aggregations": [],
                    "select": [],
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
                group_key = self._extract_group_key(row, req.group_by, req.root_entity)
                state = ensure_state(group_key)
                if default_count:
                    state["count"] = state.get("count", 0) + 1
                self._update_aggregations(state, row, req.aggregations, req.root_entity)

            root_offset += len(left_rows)
            if len(left_rows) < batch_limit:
                break

        meta = {
            "composite": True,
            "root_provider": root_provider_name,
            "cross_relation": cross_relation.name,
            "relations_used": req.relations,
        }
        if req.group_by:
            rows: List[RowResult] = []
            for key, state in group_state.items():
                data: Dict[str, Any] = {}
                for idx, grp in enumerate(req.group_by):
                    col_name = grp.alias or grp.field if hasattr(grp, "alias") else grp.field
                    if grp.entity and grp.entity != req.root_entity:
                        raise NotImplementedError(
                            "Cross-provider aggregations: group_by on non-root entities is not supported"
                        )
                    data[col_name] = key[idx]
                if default_count:
                    data["count"] = state.get("count", 0)
                data.update(self._finalize_aggregations(state, req.aggregations))
                rows.append(RowResult(entity=req.root_entity, data=data))
            if req.offset:
                rows = rows[req.offset :]
            if req.limit is not None:
                rows = rows[: req.limit]
            meta["group_by"] = [grp.field for grp in req.group_by]
            return QueryResult(rows=rows, meta=meta)

        aggregations = self._finalize_aggregations(
            group_state.get((), {}), req.aggregations
        )
        agg_results = {
            key: AggregationResult(key=key, value=value)
            for key, value in aggregations.items()
        }
        return QueryResult(aggregations=agg_results, meta=meta)

    def _extract_group_key(
        self, row: RowResult, group_by: List[GroupBySpec], root_entity: str
    ) -> Tuple[Any, ...]:
        if not group_by:
            return ()
        values: List[Any] = []
        for grp in group_by:
            if grp.entity and grp.entity != root_entity:
                raise NotImplementedError(
                    "Cross-provider aggregations: group_by on non-root entities is not supported"
                )
            ent, fld = self._resolve_field_entity(grp.field, grp.entity or root_entity)
            values.append(self._extract_value(row, ent, fld))
        return tuple(values)

    def _update_aggregations(
        self,
        state: Dict[str, Any],
        row: RowResult,
        aggregations: List[AggregationSpec],
        root_entity: str,
    ) -> None:
        for spec in aggregations:
            alias = spec.alias or f"{spec.agg}_{spec.field}"
            ent, field = self._resolve_field_entity(spec.field, root_entity)
            value = self._extract_value(row, ent, field)
            if spec.agg == "count":
                if value is not None:
                    state[alias] = state.get(alias, 0) + 1
            elif spec.agg == "count_distinct":
                seen = state.setdefault(alias, set())
                if value is not None:
                    seen.add(value)
            elif spec.agg == "sum":
                if value is not None:
                    state[alias] = state.get(alias, 0) + value
            elif spec.agg == "min":
                if value is None:
                    continue
                if alias not in state:
                    state[alias] = value
                else:
                    state[alias] = min(state[alias], value)
            elif spec.agg == "max":
                if value is None:
                    continue
                if alias not in state:
                    state[alias] = value
                else:
                    state[alias] = max(state[alias], value)
            elif spec.agg == "avg":
                if value is None:
                    continue
                total, count = state.get(alias, (0, 0))
                state[alias] = (total + value, count + 1)
            else:
                raise NotImplementedError(
                    f"Aggregation '{spec.agg}' is not supported across providers"
                )

    def _finalize_aggregations(
        self, state: Dict[str, Any], aggregations: List[AggregationSpec]
    ) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for spec in aggregations:
            alias = spec.alias or f"{spec.agg}_{spec.field}"
            if spec.agg == "count":
                results[alias] = state.get(alias, 0)
            elif spec.agg == "count_distinct":
                results[alias] = len(state.get(alias, set()))
            elif spec.agg == "sum":
                results[alias] = state.get(alias, 0)
            elif spec.agg == "min":
                results[alias] = state.get(alias)
            elif spec.agg == "max":
                results[alias] = state.get(alias)
            elif spec.agg == "avg":
                total, count = state.get(alias, (0, 0))
                results[alias] = total / count if count else None
            else:
                raise NotImplementedError(
                    f"Aggregation '{spec.agg}' is not supported across providers"
                )
        return results

    def _resolve_field_entity(self, field: str, root_entity: str) -> Tuple[str, str]:
        if "." in field:
            ent, fld = field.split(".", 1)
            return ent, fld
        return root_entity, field

    def _extract_value(self, row: RowResult, entity: str, field: str) -> Any:
        if row.entity == entity:
            return row.data.get(field)
        return row.related.get(entity, {}).get(field)

    def _effective_cardinality(
        self, relation: RelationDescriptor, left_entity: str, right_entity: str
    ) -> str:
        if (
            left_entity == relation.join.from_entity
            and right_entity == relation.join.to_entity
        ):
            return relation.cardinality
        if (
            left_entity == relation.join.to_entity
            and right_entity == relation.join.from_entity
        ):
            mapping = {
                "1_to_1": "1_to_1",
                "1_to_many": "many_to_1",
                "many_to_1": "1_to_many",
                "many_to_many": "many_to_many",
            }
            if relation.cardinality not in mapping:
                raise ValueError(
                    f"Unknown cardinality '{relation.cardinality}' for relation '{relation.name}'"
                )
            return mapping[relation.cardinality]
        raise ValueError(
            f"Relation '{relation.name}' does not connect entities {left_entity} and {right_entity}"
        )

    def _remote_limit_for_cardinality(self, cardinality: str, key_count: int) -> int:
        if cardinality in {"1_to_1", "many_to_1"}:
            limit = key_count if key_count else self.max_right_rows_per_batch
            return min(self.max_right_rows_per_batch, limit)
        return self.max_right_rows_per_batch

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
        for rel_name in req.relations:
            rel = self._relation_index.get(rel_name)
            if rel:
                involved_entities.add(rel.from_entity)
                involved_entities.add(rel.to_entity)
        for grp in req.group_by:
            if grp.entity:
                involved_entities.add(grp.entity)
        for agg in req.aggregations:
            if "." in agg.field:
                ent, _ = agg.field.split(".", 1)
                involved_entities.add(ent)
        for sel in req.select:
            expr = getattr(sel, "expr", None)
            if isinstance(expr, str) and "." in expr:
                ent, _ = expr.split(".", 1)
                involved_entities.add(ent)
        return involved_entities

    def describe(self):
        info = super().describe()
        info.description = info.description + (
            " (Composite: routes requests to child providers; limited cross-provider joins support)"
        )
        info.capabilities = sorted(
            set(
                info.capabilities
                + [
                    "single_provider_routing",
                    "cross_provider_join",
                    "cross_provider_aggregate",
                ]
            )
        )
        return info


__all__ = ["CompositeRelationalProvider"]
