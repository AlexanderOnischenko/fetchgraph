from __future__ import annotations

"""SQL-backed relational provider that builds queries directly."""

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

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
    RelationalQuery,
    RowResult,
    RelationDescriptor,
    SelectExpr,
    SemanticClause,
    SemanticOnlyResult,
)
from .semantic_backend import SemanticBackend


class SqlRelationalDataProvider(RelationalDataProvider):
    """SQL-backed relational provider.

    The provider holds an existing DB-API 2.0 connection and translates
    :class:`RelationalQuery` selectors into SQL statements without using
    pandas. It mirrors the semantics of :class:`PandasRelationalDataProvider`
    including selector handling, semantic clauses, filters, and aggregations.

    Notes
    -----
    This provider assumes DB-API connections using ``paramstyle="qmark"``
    (``?`` placeholders), such as SQLite. Other paramstyles are not
    supported.
    """

    def __init__(
        self,
        name: str,
        entities: List[EntityDescriptor],
        relations: List[RelationDescriptor],
        connection,
        semantic_backend: Optional[SemanticBackend] = None,
        primary_keys: Optional[Mapping[str, str]] = None,
    ):
        super().__init__(name, entities, relations)
        self.connection = connection
        self.semantic_backend = semantic_backend
        self.primary_keys = primary_keys or {}
        self._entity_index: Dict[str, EntityDescriptor] = {e.name: e for e in entities}

    # --- helper methods ---
    def _pk_column(self, entity: str) -> Optional[str]:
        if entity in self.primary_keys:
            return self.primary_keys[entity]
        desc = self._entity_index.get(entity)
        if not desc:
            return None
        for col in desc.columns:
            if col.role == "primary_key":
                return col.name
        return None

    def _relation_by_name(self, name: str) -> RelationDescriptor:
        for rel in self.relations:
            if rel.name == name:
                return rel
        raise KeyError(f"Relation '{name}' not found")

    def _quote_ident(self, name: str) -> str:
        return f'"{name}"'

    def _column_ref(self, entity: str, column: str) -> str:
        return f"{self._quote_ident(entity)}.{self._quote_ident(column)}"

    def _lookup_alias(self, entity: str, table_aliases: Mapping[str, Tuple[str, str]]) -> str:
        if entity in table_aliases:
            return table_aliases[entity][1]

        matches = [alias for _, (ent, alias) in table_aliases.items() if ent == entity]
        if not matches:
            raise KeyError(f"Entity '{entity}' not joined in query")
        if len(matches) > 1:
            raise KeyError(
                f"Entity '{entity}' is joined multiple times; reference a relation name to disambiguate"
            )
        return matches[0]

    def _resolve_field(self, root_entity: str, field: str, entity: Optional[str]) -> Tuple[str, str]:
        ent = entity
        fld = field
        if ent is None and "." in field:
            ent, fld = field.split(".", 1)
        ent = ent or root_entity
        return ent, fld

    def _select_alias(self, entity: str, field: str, root_entity: str) -> str:
        return field if entity == root_entity else f"{entity}__{field}"

    def _build_comparison(self, column: str, op: str, value: Any, params: List[Any]) -> str:
        if op in {"=", "!=", ">", "<", ">=", "<="}:
            params.append(value)
            return f"{column} {op} ?"
        if op == "in":
            if not isinstance(value, (list, tuple)):
                raise TypeError("Values for 'in' operator must be a list or tuple")
            placeholders = ",".join("?" for _ in value)
            params.extend(value)
            return f"{column} IN ({placeholders})" if value else "1=0"
        if op == "not_in":
            if not isinstance(value, (list, tuple)):
                raise TypeError("Values for 'not_in' operator must be a list or tuple")
            placeholders = ",".join("?" for _ in value)
            params.extend(value)
            return f"{column} NOT IN ({placeholders})" if value else "1=1"
        if op == "like":
            params.append(f"%{value}%")
            return f"{column} LIKE ?"
        if op == "ilike":
            params.append(f"%{str(value).lower()}%")
            return f"LOWER({column}) LIKE ?"
        raise ValueError(f"Unsupported comparison operator: {op}")

    def _build_filters(
        self,
        clause: Optional[FilterClause],
        root_entity: str,
        table_aliases: Mapping[str, Tuple[str, str]],
        params: List[Any],
    ) -> Optional[str]:
        if clause is None:
            return None
        if isinstance(clause, ComparisonFilter):
            ent, fld = self._resolve_field(root_entity, clause.field, clause.entity)
            col = self._column_ref(self._lookup_alias(ent, table_aliases), fld)
            return self._build_comparison(col, clause.op, clause.value, params)
        if isinstance(clause, LogicalFilter):
            parts: List[str] = []
            for sub in clause.clauses:
                sub_sql = self._build_filters(sub, root_entity, table_aliases, params)
                if sub_sql:
                    parts.append(f"({sub_sql})")
            joiner = " AND " if clause.op == "and" else " OR "
            return joiner.join(parts) if parts else None
        return None

    def _build_semantic_clauses(
        self,
        clauses: List[SemanticClause],
        root_entity: str,
        table_aliases: Mapping[str, Tuple[str, str]],
    ) -> Tuple[List[str], Optional[str], List[Any], List[Any]]:
        if not clauses:
            return [], None, [], []
        if not self.semantic_backend:
            raise RuntimeError("Semantic backend is not configured")

        conditions: List[str] = []
        condition_params: List[Any] = []
        boost_params: List[Any] = []
        boost_exprs: List[str] = []

        for clause in clauses:
            pk = self._pk_column(clause.entity)
            if not pk:
                raise ValueError(f"Primary key not defined for entity '{clause.entity}'")
            if clause.entity not in self._entity_index:
                raise KeyError(f"Unknown entity '{clause.entity}' in semantic clause")
            matches = self.semantic_backend.search(clause.entity, clause.fields, clause.query, clause.top_k)
            if clause.threshold is not None:
                matches = [m for m in matches if m.score >= clause.threshold]
            match_ids = [m.id for m in matches]
            target_col = self._column_ref(self._lookup_alias(clause.entity, table_aliases), pk)

            if clause.mode == "filter":
                if match_ids:
                    placeholders = ",".join("?" for _ in match_ids)
                    conditions.append(f"{target_col} IN ({placeholders})")
                    condition_params.extend(match_ids)
                else:
                    conditions.append("1=0")
            elif clause.mode == "boost":
                if not match_ids:
                    continue
                cases = " ".join(["WHEN ? THEN ?" for _ in match_ids])
                case_expr = f"CASE {target_col} {cases} ELSE 0 END"
                for match in matches:
                    boost_params.extend([match.id, match.score])
                boost_exprs.append(case_expr)

        boost_order = None
        if boost_exprs:
            summed = " + ".join(f"({expr})" for expr in boost_exprs)
            boost_order = f"({summed}) DESC"

        return conditions, boost_order, condition_params, boost_params

    def _build_default_select(
        self, root_entity: str, table_aliases: Mapping[str, Tuple[str, str]]
    ) -> Tuple[List[str], List[str]]:
        select_parts: List[str] = []
        base_columns: List[str] = []
        entity_aliases: Dict[str, set[str]] = {}
        for ent, alias in table_aliases.values():
            entity_aliases.setdefault(ent, set()).add(alias)
        root_alias = self._lookup_alias(root_entity, table_aliases)
        for col in self._entity_index[root_entity].columns:
            select_parts.append(
                f"{self._column_ref(root_alias, col.name)} AS {self._quote_ident(col.name)}"
            )
            base_columns.append(col.name)

        seen_aliases = {root_alias}
        for ref, (entity, alias) in table_aliases.items():
            if ref == root_entity or alias in seen_aliases:
                continue
            seen_aliases.add(alias)
            desc = self._entity_index.get(entity)
            if not desc:
                continue
            label = entity if len(entity_aliases[entity]) == 1 else ref
            for col in desc.columns:
                aliased = self._select_alias(label, col.name, root_entity)
                select_parts.append(
                    f"{self._column_ref(alias, col.name)} AS {self._quote_ident(aliased)}"
                )
        return select_parts, base_columns

    def _build_select_from_expressions(
        self, req: RelationalQuery, table_aliases: Mapping[str, Tuple[str, str]]
    ) -> Tuple[List[str], List[str]]:
        select_parts: List[str] = []
        base_columns = [col.name for col in self._entity_index[req.root_entity].columns]
        for expr in req.select:
            ent, fld = self._resolve_field(req.root_entity, expr.expr, None)
            alias = self._select_alias(ent, fld, req.root_entity)
            target_alias = expr.alias or alias
            if ent not in table_aliases and ent not in self._entity_index:
                raise KeyError(f"Entity '{ent}' not joined in query")
            select_parts.append(
                f"{self._column_ref(self._lookup_alias(ent, table_aliases), fld)} AS {self._quote_ident(target_alias)}"
            )
        return select_parts, base_columns

    def _build_relations(self, req: RelationalQuery) -> Tuple[List[str], Dict[str, Tuple[str, str]]]:
        joins: List[str] = []
        table_aliases: Dict[str, Tuple[str, str]] = {req.root_entity: (req.root_entity, req.root_entity)}
        used_aliases = {req.root_entity}

        for rel_name in req.relations:
            relation = self._relation_by_name(rel_name)
            left_entity = right_entity = left_field = right_field = None

            if relation.from_entity in table_aliases or any(
                ent == relation.from_entity for ent, _ in table_aliases.values()
            ):
                left_entity = relation.from_entity
                right_entity = relation.to_entity
                left_field = relation.join.from_column
                right_field = relation.join.to_column
            elif relation.to_entity in table_aliases or any(
                ent == relation.to_entity for ent, _ in table_aliases.values()
            ):
                left_entity = relation.to_entity
                right_entity = relation.from_entity
                left_field = relation.join.to_column
                right_field = relation.join.from_column
            else:
                raise ValueError(f"Neither entity of relation '{relation.name}' present in query")

            base_alias = rel_name or right_entity
            right_alias = base_alias
            suffix = 2
            while right_alias in used_aliases:
                right_alias = f"{base_alias}_{suffix}"
                suffix += 1
            used_aliases.add(right_alias)

            table_aliases[rel_name] = (right_entity, right_alias)
            table_aliases.setdefault(right_entity, (right_entity, right_alias))

            join_type = relation.join.join_type.upper()
            if join_type == "OUTER":
                join_type = "FULL OUTER"
            joins.append(
                f"{join_type} JOIN {self._quote_ident(right_entity)} AS {self._quote_ident(right_alias)} "
                f"ON {self._column_ref(self._lookup_alias(left_entity, table_aliases), left_field)} = {self._column_ref(right_alias, right_field)}"
            )

        return joins, table_aliases

    def _build_aggregations(
        self, req: RelationalQuery, table_aliases: Mapping[str, Tuple[str, str]]
    ) -> Tuple[List[str], List[str]]:
        group_cols: List[str] = []
        select_parts: List[str] = []

        for g in req.group_by:
            ent, fld = self._resolve_field(req.root_entity, g.field, g.entity)
            col_ref = self._column_ref(self._lookup_alias(ent, table_aliases), fld)
            alias = self._select_alias(ent, fld, req.root_entity)
            select_parts.append(f"{col_ref} AS {self._quote_ident(alias)}")
            group_cols.append(col_ref)

        if req.aggregations:
            for spec in req.aggregations:
                ent, fld = self._resolve_field(req.root_entity, spec.field, None)
                col_ref = self._column_ref(self._lookup_alias(ent, table_aliases), fld)
                agg_func = spec.agg
                if agg_func == "count_distinct":
                    agg_expr = f"COUNT(DISTINCT {col_ref})"
                elif agg_func == "avg":
                    agg_expr = f"AVG({col_ref})"
                else:
                    agg_expr = f"{agg_func.upper()}({col_ref})"
                alias = spec.alias or f"{spec.agg}_{spec.field}"
                select_parts.append(f"{agg_expr} AS {self._quote_ident(alias)}")
        elif group_cols:
            select_parts.append("COUNT(*) AS \"count\"")

        return select_parts, group_cols

    # --- core handlers ---
    def _handle_query(self, req: RelationalQuery):
        joins, table_aliases = self._build_relations(req)

        if req.group_by or req.aggregations:
            return self._handle_aggregate_query(req, joins, table_aliases)

        if req.select:
            select_parts, base_columns = self._build_select_from_expressions(req, table_aliases)
        else:
            select_parts, base_columns = self._build_default_select(req.root_entity, table_aliases)

        conditions: List[str] = []
        params: List[Any] = []

        semantic_conditions, boost_order, semantic_params, boost_params = self._build_semantic_clauses(
            req.semantic_clauses, req.root_entity, table_aliases
        )
        conditions.extend(semantic_conditions)
        params.extend(semantic_params)

        filter_sql = self._build_filters(req.filters, req.root_entity, table_aliases, params)
        if filter_sql:
            conditions.append(filter_sql)

        if boost_params:
            params.extend(boost_params)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        order_clause = f"ORDER BY {boost_order}" if boost_order else ""
        limit_clause = f"LIMIT {req.limit}" if req.limit is not None else ""
        offset_clause = f"OFFSET {req.offset}" if req.offset else ""

        root_alias = self._lookup_alias(req.root_entity, table_aliases)
        sql_parts = [
            "SELECT",
            ", ".join(select_parts),
            "FROM",
            f"{self._quote_ident(req.root_entity)} AS {self._quote_ident(root_alias)}",
        ]
        if joins:
            sql_parts.append(" ".join(joins))
        if where_clause:
            sql_parts.append(where_clause)
        if order_clause:
            sql_parts.append(order_clause)
        if limit_clause:
            sql_parts.append(limit_clause)
        if offset_clause:
            sql_parts.append(offset_clause)

        sql = " ".join(part for part in sql_parts if part)
        cursor = self.connection.cursor()
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = [self._row_from_db(row, columns, base_columns, req.root_entity) for row in cursor.fetchall()]
        return QueryResult(rows=rows, meta={"relations_used": req.relations})

    def _handle_aggregate_query(
        self, req: RelationalQuery, joins: Sequence[str], table_aliases: Mapping[str, Tuple[str, str]]
    ):
        select_parts, group_cols = self._build_aggregations(req, table_aliases)
        conditions: List[str] = []
        params: List[Any] = []

        semantic_conditions, _, semantic_params, _ = self._build_semantic_clauses(
            req.semantic_clauses, req.root_entity, table_aliases
        )
        conditions.extend(semantic_conditions)
        params.extend(semantic_params)

        filter_sql = self._build_filters(req.filters, req.root_entity, table_aliases, params)
        if filter_sql:
            conditions.append(filter_sql)

        order_clause = ""

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        group_clause = f"GROUP BY {', '.join(group_cols)}" if group_cols else ""
        limit_clause = f"LIMIT {req.limit}" if req.limit is not None else ""
        offset_clause = f"OFFSET {req.offset}" if req.offset else ""

        root_alias = self._lookup_alias(req.root_entity, table_aliases)
        sql_parts = [
            "SELECT",
            ", ".join(select_parts),
            "FROM",
            f"{self._quote_ident(req.root_entity)} AS {self._quote_ident(root_alias)}",
        ]
        if joins:
            sql_parts.append(" ".join(joins))
        if where_clause:
            sql_parts.append(where_clause)
        if group_clause:
            sql_parts.append(group_clause)
        if order_clause:
            sql_parts.append(order_clause)
        if limit_clause:
            sql_parts.append(limit_clause)
        if offset_clause:
            sql_parts.append(offset_clause)

        sql = " ".join(part for part in sql_parts if part)
        cursor = self.connection.cursor()
        cursor.execute(sql, params)

        if group_cols:
            columns = [desc[0] for desc in cursor.description]
            rows = [
                RowResult(entity=req.root_entity, data=dict(zip(columns, row))) for row in cursor.fetchall()
            ]
            return QueryResult(rows=rows, meta={"group_by": group_cols, "relations_used": req.relations})

        row = cursor.fetchone()
        agg_results: Dict[str, AggregationResult] = {}
        if row is not None:
            for idx, col in enumerate(cursor.description):
                agg_results[col[0]] = AggregationResult(key=col[0], value=row[idx])
        return QueryResult(aggregations=agg_results, meta={"relations_used": req.relations})

    def _row_from_db(
        self, row: Sequence[Any], columns: Sequence[str], base_columns: List[str], root_entity: str
    ) -> RowResult:
        data_map = dict(zip(columns, row))
        data = {col: data_map[col] for col in base_columns if col in data_map}
        related: Dict[str, Dict[str, Any]] = {}
        for col_name, value in data_map.items():
            if "__" in col_name:
                ent, fld = col_name.split("__", 1)
                related.setdefault(ent, {})[fld] = value
        return RowResult(entity=root_entity, data=data, related=related)

    def _handle_semantic_only(self, req) -> SemanticOnlyResult:
        if not self.semantic_backend:
            raise RuntimeError("Semantic backend is not configured")
        matches = self.semantic_backend.search(req.entity, req.fields, req.query, req.top_k)
        return SemanticOnlyResult(matches=matches)


__all__ = ["SqlRelationalDataProvider"]

