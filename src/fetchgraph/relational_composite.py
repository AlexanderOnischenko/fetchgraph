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
        def _norm_entity(e: EntityDescriptor) -> Dict[str, Any]:
            d = e.model_dump()
            cols = d.get("columns") or []
            # column order should not make descriptors "different"
            d["columns"] = sorted(cols, key=lambda c: c.get("name", ""))
            return d

        def _norm_relation(r: RelationDescriptor) -> Dict[str, Any]:
            return r.model_dump()

        entity_by_name: Dict[str, EntityDescriptor] = {}
        entity_src: Dict[str, str] = {}
        relation_by_name: Dict[str, RelationDescriptor] = {}
        relation_src: Dict[str, str] = {}

        for child_name, child in children.items():
            for ent in child.entities:
                existing = entity_by_name.get(ent.name)
                if existing is None:
                    entity_by_name[ent.name] = ent
                    entity_src[ent.name] = child_name
                elif _norm_entity(existing) != _norm_entity(ent):
                    raise ValueError(
                        f"Entity descriptor conflict for '{ent.name}': "
                        f"'{entity_src[ent.name]}' vs '{child_name}'. "
                        "Ensure shared entities have identical schema (columns/roles/types)."
                    )

            for rel in child.relations:
                # nameless relations can't be referenced via selectors.relations anyway
                if not rel.name:
                    continue
                existing_rel = relation_by_name.get(rel.name)
                if existing_rel is None:
                    relation_by_name[rel.name] = rel
                    relation_src[rel.name] = child_name
                elif _norm_relation(existing_rel) != _norm_relation(rel):
                    raise ValueError(
                        f"Relation descriptor conflict for '{rel.name}': "
                        f"'{relation_src[rel.name]}' vs '{child_name}'. "
                        "Relation names must be unique across children (or have identical descriptors)."
                    )

        entities = list(entity_by_name.values())
        relations = list(relation_by_name.values())
        super().__init__(name=name, entities=entities, relations=relations)
        self.children = children
        self.max_join_rows_per_batch = max_join_rows_per_batch
        self.max_right_rows_per_batch = max_right_rows_per_batch
        self.max_join_bytes = max_join_bytes
        # TODO: enforce max_join_bytes when estimating join materialization size

        # NOTE: entities may be exposed by multiple children.
        self._entity_to_providers: Dict[str, Set[str]] = {}
        self._provider_entities: Dict[str, Set[str]] = {}
        self._provider_relations: Dict[str, Set[str]] = {}
        for child_name, child in children.items():
            ents = set(getattr(child, "_entity_index", {}).keys())
            self._provider_entities[child_name] = ents
            rels = {r.name for r in getattr(child, "relations", [])}
            self._provider_relations[child_name] = rels
            for ent in ents:
                self._entity_to_providers.setdefault(ent, set()).add(child_name)

        # Relation names must be globally unique by descriptor (conflicts are ambiguous).
        self._relation_index: Dict[str, RelationDescriptor] = {}
        for rel in relations:
            existing = self._relation_index.get(rel.name)
            if existing is not None and existing.model_dump() != rel.model_dump():
                raise ValueError(
                    f"Relation '{rel.name}' is defined multiple times with different descriptors. "
                    "Composite routing requires relation names to be unique across children."
                )
            self._relation_index[rel.name] = rel

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
        required_relations = set(req.relations)
        for name, prov in self.children.items():
            if not involved_entities.issubset(self._provider_entities.get(name, set())):
                continue
            if not required_relations.issubset(self._provider_relations.get(name, set())):
                continue
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
            if ent != req.root_entity:
                raise NotImplementedError(
                    "Filters on non-root providers are not supported for cross-provider joins"
                )
        for clause in req.semantic_clauses:
            if clause.entity != req.root_entity:
                raise NotImplementedError("Semantic clauses across providers are not supported")

        if not req.relations:
            raise NotImplementedError("Cross-provider join requires at least one relation")

        # Current limitation: a single cross-provider boundary, and it must be the
        # last relation in the chain.
        local_relations = list(req.relations[:-1])
        cross_relation_name = req.relations[-1]
        cross_relation = self._relation_index.get(cross_relation_name)
        if not cross_relation:
            raise KeyError(f"Relation '{cross_relation_name}' not found")

        def provider_supports_relation(provider_name: str, rel_name: str) -> bool:
            if rel_name not in self._provider_relations.get(provider_name, set()):
                return False
            rel = self._relation_index.get(rel_name)
            if not rel:
                return False
            ents = self._provider_entities.get(provider_name, set())
            return rel.from_entity in ents and rel.to_entity in ents

        # Choose a root provider that can execute all relations before the cross
        # boundary and contains all entities referenced by root-side filters.
        root_candidates: List[str] = []
        for provider_name in self.children.keys():
            ents = self._provider_entities.get(provider_name, set())
            if req.root_entity not in ents:
                continue
            if req.filters and not set(filter_entities).issubset(ents):
                continue
            ok = True
            for rel_name in local_relations:
                if not provider_supports_relation(provider_name, rel_name):
                    ok = False
                    break
            if ok:
                root_candidates.append(provider_name)

        if not root_candidates:
            raise NotImplementedError(
                "Cross-provider join planning failed: no child provider can execute the "
                "root-side relations prior to the cross boundary."
            )

        root_provider_name = root_candidates[0]
        root_provider = self.children[root_provider_name]
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
                left_rows, req, cross_relation, root_provider_name, feature_name, **kwargs
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
        projected_rows = self._apply_select_to_rows(
            all_rows, req.select, req.root_entity
        )
        return QueryResult(rows=projected_rows, meta=meta)

    def _join_batch_with_remote(
        self,
        left_rows: List[RowResult],
        req: RelationalQuery,
        cross_relation: RelationDescriptor,
        root_provider_name: str,
        feature_name: str,
        **kwargs,
    ) -> List[RowResult]:
        join_type = cross_relation.join.join_type
        if join_type not in {"inner", "left"}:
            raise NotImplementedError(f"Join type '{join_type}' is not supported for cross-provider joins")

        def _row_has_entity(row: RowResult, entity: str) -> bool:
            return row.entity == entity or entity in row.related

        # Determine join direction based on which entity is actually present on
        # the left side rows.
        from_ent = cross_relation.join.from_entity
        to_ent = cross_relation.join.to_entity
        has_from = any(_row_has_entity(r, from_ent) for r in left_rows)
        has_to = any(_row_has_entity(r, to_ent) for r in left_rows)

        if has_from:
            left_entity = from_ent
            right_entity = to_ent
            left_col = cross_relation.join.from_column
            right_col = cross_relation.join.to_column
        elif has_to:
            left_entity = to_ent
            right_entity = from_ent
            left_col = cross_relation.join.to_column
            right_col = cross_relation.join.from_column
        else:
            raise KeyError(
                f"Neither '{from_ent}' nor '{to_ent}' is present in left rows for cross join '{cross_relation.name}'"
            )

        effective_cardinality = self._effective_cardinality(
            cross_relation, left_entity, right_entity
        )

        join_keys: List[Any] = []
        for row in left_rows:
            value = self._extract_value(row, left_entity, left_col)
            join_keys.append(value)
        unique_keys = list({k for k in join_keys if k is not None})

        right_candidates = self._entity_to_providers.get(right_entity, set())
        if not right_candidates:
            raise KeyError(f"Entity '{right_entity}' not found in any child provider")

        # Prefer a provider different from the root provider when possible.
        right_provider_name: Optional[str] = None
        for name in self.children.keys():
            if name in right_candidates and name != root_provider_name:
                right_provider_name = name
                break
        if right_provider_name is None:
            # Fallback to root provider if it also exposes the entity, otherwise
            # take the first candidate in child order.
            if root_provider_name in right_candidates:
                right_provider_name = root_provider_name
            else:
                for name in self.children.keys():
                    if name in right_candidates:
                        right_provider_name = name
                        break
        assert right_provider_name is not None
        right_provider = self.children[right_provider_name]

        right_results: Dict[Any, List[RowResult]] = {}
        single_right: Dict[Any, RowResult] = {}

        def chunks(iterable: List[Any], size: int):
            for i in range(0, len(iterable), size):
                yield iterable[i : i + size]

        for key_chunk in chunks(unique_keys, self.max_right_rows_per_batch):
            comparison = ComparisonFilter(entity=right_entity, field=right_col, op="in", value=key_chunk)

            # --- 1) 1_to_1 / many_to_1: safe to fetch in one request (<= key_chunk)
            if effective_cardinality in {"1_to_1", "many_to_1"}:
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
                    if key in single_right:
                        raise ValueError(
                            f"Cardinality {effective_cardinality} violated for relation '{cross_relation.name}'"
                        )
                    single_right[key] = row
                if len(remote_result.rows) > len(key_chunk):
                    raise ValueError(
                        f"Cardinality {effective_cardinality} violated for relation '{cross_relation.name}'"
                    )
                continue

            # --- 2) 1_to_many / many_to_many: correctness-first, avoid silent truncation
            fast_limit = self.max_right_rows_per_batch
            fast_req = RelationalQuery(
                root_entity=right_entity,
                filters=comparison,
                relations=[],
                select=[],
                limit=fast_limit,
                offset=0,
            )
            fast_res = right_provider.fetch(
                feature_name, selectors=fast_req.model_dump(), **kwargs
            )
            if not isinstance(fast_res, QueryResult):
                raise TypeError("Expected QueryResult from right provider")
            if len(fast_res.rows) > self.max_right_rows_per_batch:
                raise MemoryError("Right join batch exceeds maximum allowed rows")

            # Fast path: if we didn't hit the cap, nothing can be truncated.
            if len(fast_res.rows) < fast_limit:
                for row in fast_res.rows:
                    key = self._extract_value(row, right_entity, right_col)
                    right_results.setdefault(key, []).append(row)
                continue

            # Slow path: we hit the cap -> may be truncated (single-key overflow OR multi-key sum overflow).
            counts = self._count_remote_matches_by_key(
                right_provider,
                feature_name=feature_name,
                entity=right_entity,
                key_field=right_col,
                keys=key_chunk,
                **kwargs,
            )
            groups = self._pack_keys_by_row_budget(key_chunk, counts, self.max_right_rows_per_batch)

            pk_field = self._primary_key_field(right_entity)
            if pk_field is None:
                raise NotImplementedError(
                    f"Overflow-safe cross-provider joins require a primary key for entity '{right_entity}'. "
                    "Mark a column with role='primary_key' in the schema."
                )

            for gkeys in groups:
                expected_sum = sum(int(counts.get(k, 0) or 0) for k in gkeys)
                if expected_sum <= 0:
                    continue

                # Single key with expected rows > budget -> page it key-by-key (correct but slow).
                if len(gkeys) == 1 and int(counts.get(gkeys[0], 0) or 0) > self.max_right_rows_per_batch:
                    k = gkeys[0]
                    rows = self._fetch_all_right_for_key(
                        right_provider,
                        feature_name=feature_name,
                        entity=right_entity,
                        key_field=right_col,
                        key_value=k,
                        expected_count=int(counts.get(k, 0) or 0),
                        pk_field=pk_field,
                        **kwargs,
                    )
                    for row in rows:
                        key = self._extract_value(row, right_entity, right_col)
                        right_results.setdefault(key, []).append(row)
                    continue

                # Group fetch where sum(expected) <= budget -> fetch exactly expected_sum rows.
                grp_filter = ComparisonFilter(entity=right_entity, field=right_col, op="in", value=gkeys)
                grp_req = RelationalQuery(
                    root_entity=right_entity,
                    filters=grp_filter,
                    relations=[],
                    select=[],
                    limit=expected_sum,
                    offset=0,
                )
                grp_res = right_provider.fetch(
                    feature_name, selectors=grp_req.model_dump(), **kwargs
                )
                if not isinstance(grp_res, QueryResult):
                    raise TypeError("Expected QueryResult from right provider (group fetch)")
                if len(grp_res.rows) > self.max_right_rows_per_batch:
                    raise MemoryError("Right join batch exceeds maximum allowed rows")

                tmp: Dict[Any, List[RowResult]] = {}
                for row in grp_res.rows:
                    key = self._extract_value(row, right_entity, right_col)
                    tmp.setdefault(key, []).append(row)

                # Validate per-key completeness; fallback to per-key paging if mismatch.
                bad_keys = [k for k in gkeys if len(tmp.get(k, [])) != int(counts.get(k, 0) or 0)]
                for k in bad_keys:
                    tmp[k] = self._fetch_all_right_for_key(
                        right_provider,
                        feature_name=feature_name,
                        entity=right_entity,
                        key_field=right_col,
                        key_value=k,
                        expected_count=int(counts.get(k, 0) or 0),
                        pk_field=pk_field,
                        **kwargs,
                    )

                for k, rows in tmp.items():
                    if not rows:
                        continue
                    right_results.setdefault(k, []).extend(rows)

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
                label = right_entity
                if label in related:
                    # Avoid overwriting when the same entity is already present
                    # in related (e.g. repeated joins). Prefer the relation name.
                    label = cross_relation.name or label
                    if label in related:
                        label = f"{right_entity}__remote"
                related[label] = match.data
                joined.append(
                    RowResult(
                        entity=row.entity,
                        data=dict(row.data),
                        related=related,
                    )
                )
        return joined

    def _primary_key_field(self, entity: str) -> Optional[str]:
        """Return primary key field name for entity if declared in schema (role=='primary_key')."""
        for e in self.entities:
            if e.name != entity:
                continue
            for c in e.columns or []:
                if getattr(c, "role", None) == "primary_key":
                    return c.name
        return None

    def _count_remote_matches_by_key(
        self,
        provider: RelationalDataProvider,
        *,
        feature_name: str,
        entity: str,
        key_field: str,
        keys: List[Any],
        **kwargs,
    ) -> Dict[Any, int]:
        """Return COUNT(*) per key_field for the given keys."""
        if not keys:
            return {}
        base = ComparisonFilter(entity=entity, field=key_field, op="in", value=keys)
        count_req = RelationalQuery(
            root_entity=entity,
            filters=base,
            relations=[],
            select=[],
            group_by=[GroupBySpec(field=key_field)],
            aggregations=[],
            # group_by output is bounded by len(keys), so no need to cap
            limit=None,
            offset=0,
        )
        res = provider.fetch(feature_name, selectors=count_req.model_dump(), **kwargs)
        if not isinstance(res, QueryResult):
            raise TypeError("Expected QueryResult from right provider (count query)")
        counts: Dict[Any, int] = {k: 0 for k in keys}
        for row in res.rows:
            k = row.data.get(key_field)
            cnt = row.data.get("count")
            if k in counts:
                try:
                    counts[k] = int(cnt) if cnt is not None else 0
                except (TypeError, ValueError):
                    counts[k] = 0
        return counts

    def _pack_keys_by_row_budget(
        self, keys: List[Any], counts: Dict[Any, int], budget: int
    ) -> List[List[Any]]:
        """
        Greedy pack keys into groups where sum(counts[key]) <= budget.
        Preserves the original key order for determinism.
        Keys with 0 expected rows are skipped (no need to fetch).
        Keys with count > budget are emitted as single-key groups (handled separately).
        """
        groups: List[List[Any]] = []
        cur: List[Any] = []
        cur_sum = 0
        for k in keys:
            c = int(counts.get(k, 0) or 0)
            if c <= 0:
                continue
            if c > budget:
                if cur:
                    groups.append(cur)
                    cur, cur_sum = [], 0
                groups.append([k])
                continue
            if cur and cur_sum + c > budget:
                groups.append(cur)
                cur, cur_sum = [k], c
            else:
                cur.append(k)
                cur_sum += c
        if cur:
            groups.append(cur)
        return groups

    def _fetch_all_right_for_key(
        self,
        provider: RelationalDataProvider,
        *,
        feature_name: str,
        entity: str,
        key_field: str,
        key_value: Any,
        expected_count: int,
        pk_field: str,
        **kwargs,
    ) -> List[RowResult]:
        """
        Fetch exactly expected_count rows for entity where key_field == key_value.
        Uses paging and validates completeness; never silently truncates.
        """
        if expected_count <= 0:
            return []
        page_limit = self.max_right_rows_per_batch
        seen: Set[Any] = set()
        out: List[RowResult] = []

        def _base_filter() -> ComparisonFilter:
            return ComparisonFilter(entity=entity, field=key_field, op="=", value=key_value)

        # 1) try offset paging first (fast path)
        offset = 0
        stagnant_pages = 0
        max_pages = (expected_count // page_limit) + 10  # safety
        pages = 0
        while len(seen) < expected_count and pages < max_pages:
            req = RelationalQuery(
                root_entity=entity,
                filters=_base_filter(),
                relations=[],
                select=[],
                limit=page_limit,
                offset=offset,
            )
            res = provider.fetch(feature_name, selectors=req.model_dump(), **kwargs)
            if not isinstance(res, QueryResult):
                raise TypeError("Expected QueryResult from right provider (paged fetch)")
            if not res.rows:
                break
            new = 0
            for row in res.rows:
                pk = self._extract_value(row, entity, pk_field)
                if pk in seen:
                    continue
                seen.add(pk)
                out.append(row)
                new += 1
            if new == 0:
                stagnant_pages += 1
            else:
                stagnant_pages = 0
            # If provider returns fewer than requested, we've hit the end under its ordering.
            if len(res.rows) < page_limit:
                break
            offset += page_limit
            pages += 1
            if stagnant_pages >= 2:
                break

        if len(seen) >= expected_count:
            return out

        # 2) fallback: exclude already seen PKs (robust even if ordering is unstable)
        # WARNING: can be slow / large filters, but this path is rare and correctness-first.
        def _exclude_filter() -> FilterClause:
            clauses: List[FilterClause] = [_base_filter()]
            seen_list = list(seen)
            # chunk NOT IN to avoid gigantic parameter lists
            chunk_size = 1000
            max_chunks = 100  # hard safety guard
            for i in range(0, len(seen_list), chunk_size):
                if (i // chunk_size) >= max_chunks:
                    raise RuntimeError(
                        f"Overflow join fetch for {entity}.{key_field}={key_value}: "
                        "too many exclusion chunks; consider adding a dedicated indexed ordering "
                        "or increasing batch strategy."
                    )
                chunk = seen_list[i : i + chunk_size]
                clauses.append(
                    ComparisonFilter(entity=entity, field=pk_field, op="not_in", value=chunk)
                )
            return LogicalFilter(op="and", clauses=clauses)

        while len(seen) < expected_count:
            req = RelationalQuery(
                root_entity=entity,
                filters=_exclude_filter(),
                relations=[],
                select=[],
                limit=page_limit,
                offset=0,
            )
            res = provider.fetch(feature_name, selectors=req.model_dump(), **kwargs)
            if not isinstance(res, QueryResult):
                raise TypeError("Expected QueryResult from right provider (exclude fetch)")
            if not res.rows:
                break
            for row in res.rows:
                pk = self._extract_value(row, entity, pk_field)
                if pk in seen:
                    continue
                seen.add(pk)
                out.append(row)

        if len(seen) != expected_count:
            raise RuntimeError(
                f"Overflow join fetch incomplete for {entity}.{key_field}={key_value}: "
                f"expected {expected_count} rows, got {len(seen)} unique PK rows. "
                "Refusing to return partial join results."
            )
        return out

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
                left_rows, req, cross_relation, root_provider_name, feature_name, **kwargs
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

    def _apply_select_to_rows(
        self, rows: List[RowResult], select: List, root_entity: str
    ) -> List[RowResult]:
        if not select:
            return rows
        projected: List[RowResult] = []
        for row in rows:
            data: Dict[str, Any] = {}
            related: Dict[str, Dict[str, Any]] = {}
            for expr in select:
                ent, fld = self._resolve_field_entity(expr.expr, root_entity)
                alias = expr.alias or fld
                value = self._extract_value(row, ent, fld)
                if ent == row.entity:
                    data[alias] = value
                else:
                    related.setdefault(ent, {})[alias] = value
            projected.append(RowResult(entity=row.entity, data=data, related=related))
        return projected

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
                return self._entities_for_reference(clause.entity, root_entity)
            if "." in clause.field:
                return self._entities_for_reference(clause.field.split(".", 1)[0], root_entity)
            return [root_entity]

        if isinstance(clause, LogicalFilter):
            entities: List[str] = []
            for sub in clause.clauses:
                entities.extend(self._collect_entities_from_filter(sub, root_entity))
            return entities

        return [root_entity]

    def _entities_for_reference(self, ref: str, root_entity: str) -> List[str]:
        entity_names = {e.name for e in self.entities}
        if ref in entity_names:
            return [ref]

        for rel in self.relations:
            if rel.name == ref:
                return [rel.from_entity, rel.to_entity]

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
