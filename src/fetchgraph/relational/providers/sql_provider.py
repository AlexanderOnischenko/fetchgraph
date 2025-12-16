from __future__ import annotations

"""SQL-backed relational provider that builds queries directly."""

from dataclasses import dataclass
from typing import Any, DefaultDict, Dict, List, Mapping, Optional, Sequence, Tuple

from collections import defaultdict

from .base import RelationalDataProvider
from ..models import (
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
from ..semantic.backend import SemanticBackend


@dataclass(frozen=True)
class AliasInfo:
    entity: str
    alias: str
    key: str


@dataclass
class JoinAliasIndex:
    root_entity: str
    root_alias: str
    by_key: dict[str, AliasInfo]
    by_entity: dict[str, List[AliasInfo]]


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
        *,
        default_schema: Optional[str] = None,
        table_names: Optional[Mapping[str, str]] = None,
    ):
        super().__init__(name, entities, relations)
        self.connection = connection
        self.semantic_backend = semantic_backend
        self.primary_keys = primary_keys or {}
        self._entity_index: Dict[str, EntityDescriptor] = {e.name: e for e in entities}
        self.default_schema = default_schema
        self.table_names: Dict[str, str] = dict(table_names or {})

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
        matches = [rel for rel in self.relations if rel.name == name]
        if len(matches) == 1:
            return matches[0]
        if len(matches) == 0:
            raise KeyError(f"Relation '{name}' not found")
        raise ValueError(
            "Multiple relations share the same name; set distinct relation.name in schema."
        )

    def _quote_ident(self, name: str) -> str:
        return f'"{name}"'

    def _quote_table(self, name: str) -> str:
        if "." in name:
            return ".".join(self._quote_ident(part) for part in name.split("."))
        return self._quote_ident(name)

    def _table_name(self, entity: str) -> str:
        table = self.table_names.get(entity, entity)
        if self.default_schema and "." not in table:
            return f"{self.default_schema}.{table}"
        return table

    def _column_ref(self, entity: str, column: str) -> str:
        return f"{self._quote_ident(entity)}.{self._quote_ident(column)}"

    def _lookup_alias(self, identifier: str, index: JoinAliasIndex) -> str:
        if identifier == index.root_entity:
            return index.root_alias

        if identifier in index.by_key:
            return index.by_key[identifier].alias

        matches = index.by_entity.get(identifier, [])
        if len(matches) == 1:
            return matches[0].alias
        if len(matches) == 0:
            raise ValueError(f"Unknown entity/relation '{identifier}'")

        keys = [info.key for info in matches]
        raise ValueError(f"Ambiguous reference '{identifier}'. Use relation key: {keys}")

    def _resolve_field(self, root_entity: str, field: str, entity: Optional[str]) -> Tuple[str, str]:
        ent = entity
        fld = field
        if ent is None and "." in field:
            ent, fld = field.split(".", 1)
        ent = ent or root_entity
        return ent, fld

    def _select_alias(self, entity: str, field: str, root_entity: str) -> str:
        return field if entity == root_entity else f"{entity}__{field}"

    def _normalize_literal(self, value: Any) -> Any:
        """Normalize string literal(s) before binding to SQL params."""
        if isinstance(value, str):
            return self._normalize_string(value)
        if isinstance(value, (list, tuple, set)):
            return [self._normalize_string(v) if isinstance(v, str) else v for v in value]
        return value

    def _build_comparison(
        self,
        column: str,
        op: str,
        value: Any,
        params: List[Any],
        *,
        case_sensitive: bool,
    ) -> str:
        def _is_string_like(val: Any) -> bool:
            if isinstance(val, str):
                return True
            if isinstance(val, (list, tuple, set)):
                return all(isinstance(v, str) for v in val)
            return False

        soft_applicable = (not case_sensitive) and _is_string_like(value) and op in {
            "=",
            "!=",
            "in",
            "not_in",
            "like",
            "ilike",
        }

        if soft_applicable:
            norm_value = self._normalize_literal(value)
            norm_column = f"LOWER(TRIM({column}))"

            if op in {"=", "!=", "like", "ilike"} and not isinstance(norm_value, str):
                soft_applicable = False
            else:
                if op == "=":
                    params.append(norm_value)
                    params.append(f"%{norm_value}%")
                    return f"({norm_column} = ? OR {norm_column} LIKE ?)"
                if op == "!=":
                    params.append(norm_value)
                    params.append(f"%{norm_value}%")
                    return f"({norm_column} <> ? AND {norm_column} NOT LIKE ?)"
                if op == "in":
                    if not isinstance(norm_value, (list, tuple)):
                        raise TypeError("Values for 'in' operator must be a list or tuple")
                    placeholders = ",".join("?" for _ in norm_value)
                    params.extend(norm_value)
                    return f"{norm_column} IN ({placeholders})" if norm_value else "1=0"
                if op == "not_in":
                    if not isinstance(norm_value, (list, tuple)):
                        raise TypeError("Values for 'not_in' operator must be a list or tuple")
                    placeholders = ",".join("?" for _ in norm_value)
                    params.extend(norm_value)
                    return f"{norm_column} NOT IN ({placeholders})" if norm_value else "1=1"
                if op in {"like", "ilike"}:
                    pattern = f"%{norm_value}%"
                    params.append(pattern)
                    return f"{norm_column} LIKE ?"

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
        index: JoinAliasIndex,
        params: List[Any],
        *,
        case_sensitive: bool,
    ) -> Optional[str]:
        if clause is None:
            return None
        if isinstance(clause, ComparisonFilter):
            ent, fld = self._resolve_field(root_entity, clause.field, clause.entity)
            col = self._column_ref(self._lookup_alias(ent, index), fld)
            return self._build_comparison(col, clause.op, clause.value, params, case_sensitive=case_sensitive)
        if isinstance(clause, LogicalFilter):
            parts: List[str] = []
            for sub in clause.clauses:
                sub_sql = self._build_filters(sub, root_entity, index, params, case_sensitive=case_sensitive)
                if sub_sql:
                    parts.append(f"({sub_sql})")
            joiner = " AND " if clause.op == "and" else " OR "
            return joiner.join(parts) if parts else None
        return None

    def _build_semantic_clauses(
        self,
        clauses: List[SemanticClause],
        root_entity: str,
        index: JoinAliasIndex,
        *,
        aggregate: bool = False,
    ) -> Tuple[List[str], Optional[str], List[Any], List[Any]]:
        if not clauses:
            return [], None, [], []
        if not self.semantic_backend:
            raise RuntimeError("Semantic backend is not configured")

        conditions: List[str] = []
        condition_params: List[Any] = []
        score_params: List[Any] = []
        score_exprs: List[str] = []

        for clause in clauses:
            pk = self._pk_column(clause.entity)
            if not pk:
                raise ValueError(f"Primary key not defined for entity '{clause.entity}'")
            if clause.entity not in self._entity_index:
                raise KeyError(f"Unknown entity '{clause.entity}' in semantic clause")

            matches = self.semantic_backend.search(clause.entity, clause.fields, clause.query, clause.top_k)
            if clause.threshold is not None:
                matches = [m for m in matches if m.score >= clause.threshold]

            if not matches and clause.mode == "filter":
                conditions.append("1=0")
                continue
            if not matches:
                continue

            match_ids = [m.id for m in matches]
            target_col = self._column_ref(self._lookup_alias(clause.entity, index), pk)

            cases = " ".join(["WHEN ? THEN ?" for _ in match_ids])
            case_expr = f"CASE {target_col} {cases} ELSE 0 END"
            for match in matches:
                score_params.extend([match.id, match.score])
            score_exprs.append(case_expr)

            if clause.mode == "filter":
                placeholders = ",".join("?" for _ in match_ids)
                conditions.append(f"{target_col} IN ({placeholders})")
                condition_params.extend(match_ids)

        order_expr = None
        if score_exprs:
            summed = " + ".join(f"({expr})" for expr in score_exprs)
            scored = f"SUM({summed})" if aggregate else f"({summed})"
            order_expr = f"{scored} DESC"

        return conditions, order_expr, condition_params, score_params

    def _build_default_select(
        self, root_entity: str, index: JoinAliasIndex
    ) -> Tuple[List[str], List[str]]:
        select_parts: List[str] = []
        base_columns: List[str] = []

        root_alias = index.root_alias
        for col in self._entity_index[root_entity].columns:
            select_parts.append(
                f"{self._column_ref(root_alias, col.name)} AS {self._quote_ident(col.name)}"
            )
            base_columns.append(col.name)

        for info in index.by_key.values():
            desc = self._entity_index.get(info.entity)
            if not desc:
                continue
            if info.entity == index.root_entity:
                label = info.key
            else:
                label = info.entity if len(index.by_entity.get(info.entity, [])) == 1 else info.key
            for col in desc.columns:
                aliased = self._select_alias(label, col.name, root_entity)
                select_parts.append(
                    f"{self._column_ref(info.alias, col.name)} AS {self._quote_ident(aliased)}"
                )
        return select_parts, base_columns

    def _build_select_from_expressions(
        self, req: RelationalQuery, index: JoinAliasIndex
    ) -> Tuple[List[str], List[str], Dict[str, Tuple[str, str]]]:
        select_parts: List[str] = []
        base_columns: List[str] = []
        related_columns: Dict[str, Tuple[str, str]] = {}
        for expr in req.select:
            ent, fld = self._resolve_field(req.root_entity, expr.expr, None)
            alias = self._select_alias(ent, fld, req.root_entity)
            target_alias = expr.alias or alias
            if ent == req.root_entity:
                base_columns.append(target_alias)
            else:
                field_alias = expr.alias or fld
                related_columns[target_alias] = (ent, field_alias)
            select_parts.append(
                f"{self._column_ref(self._lookup_alias(ent, index), fld)} AS {self._quote_ident(target_alias)}"
            )
        return select_parts, base_columns, related_columns

    def _build_relations(self, req: RelationalQuery) -> Tuple[List[str], JoinAliasIndex]:
        joins: List[str] = []

        by_key: Dict[str, AliasInfo] = {}
        by_entity: DefaultDict[str, List[AliasInfo]] = defaultdict(list)

        root_alias = "t0"
        alias_counter = 1

        index = JoinAliasIndex(
            root_entity=req.root_entity, root_alias=root_alias, by_key=by_key, by_entity=by_entity
        )

        def _entity_present(entity: str) -> bool:
            return entity == index.root_entity or entity in index.by_entity

        for rel_name in req.relations:
            relation = self._relation_by_name(rel_name)

            if _entity_present(relation.from_entity):
                left_entity = relation.from_entity
                right_entity = relation.to_entity
                left_field = relation.join.from_column
                right_field = relation.join.to_column
            elif _entity_present(relation.to_entity):
                left_entity = relation.to_entity
                right_entity = relation.from_entity
                left_field = relation.join.to_column
                right_field = relation.join.from_column
            else:
                raise ValueError(f"Neither entity of relation '{relation.name}' present in query")

            key = relation.name or right_entity

            if not relation.name and len(by_entity.get(right_entity, [])) >= 1:
                raise ValueError(
                    f"Entity '{right_entity}' is joined multiple times; set distinct relation.name in schema and reference it in selectors."
                )

            if key in by_key:
                raise ValueError(f"Duplicate relation key '{key}' in joins")

            right_alias = f"t{alias_counter}"
            alias_counter += 1

            info = AliasInfo(entity=right_entity, alias=right_alias, key=key)
            by_key[key] = info
            by_entity[right_entity].append(info)

            join_type = relation.join.join_type.upper()
            if join_type == "OUTER":
                join_type = "FULL OUTER"
            joins.append(
                f"{join_type} JOIN {self._quote_table(self._table_name(right_entity))} AS {self._quote_ident(right_alias)} "
                f"ON {self._column_ref(self._lookup_alias(left_entity, index), left_field)} = {self._column_ref(right_alias, right_field)}"
            )

        return joins, JoinAliasIndex(
            root_entity=req.root_entity, root_alias=root_alias, by_key=by_key, by_entity=dict(by_entity)
        )

    def _build_aggregations(
        self, req: RelationalQuery, index: JoinAliasIndex
    ) -> Tuple[List[str], List[str]]:
        group_cols: List[str] = []
        select_parts: List[str] = []

        for g in req.group_by:
            ent, fld = self._resolve_field(req.root_entity, g.field, g.entity)
            col_ref = self._column_ref(self._lookup_alias(ent, index), fld)
            alias = g.alias or self._select_alias(ent, fld, req.root_entity)
            select_parts.append(f"{col_ref} AS {self._quote_ident(alias)}")
            group_cols.append(col_ref)

        if req.aggregations:
            for spec in req.aggregations:
                ent, fld = self._resolve_field(req.root_entity, spec.field, None)
                col_ref = self._column_ref(self._lookup_alias(ent, index), fld)
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
        joins, index = self._build_relations(req)

        if req.group_by or req.aggregations:
            return self._handle_aggregate_query(req, joins, index)

        related_columns: Dict[str, Tuple[str, str]] = {}

        if req.select:
            select_parts, base_columns, related_columns = self._build_select_from_expressions(req, index)
        else:
            select_parts, base_columns = self._build_default_select(req.root_entity, index)

        conditions: List[str] = []
        params: List[Any] = []

        semantic_conditions, semantic_order, semantic_params, score_params = self._build_semantic_clauses(
            req.semantic_clauses, req.root_entity, index, aggregate=False
        )
        conditions.extend(semantic_conditions)
        params.extend(semantic_params)

        filter_sql = self._build_filters(
            req.filters, req.root_entity, index, params, case_sensitive=req.case_sensitivity
        )
        if filter_sql:
            conditions.append(filter_sql)

        if score_params:
            params.extend(score_params)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        order_clause = f"ORDER BY {semantic_order}" if semantic_order else ""
        limit_clause = f"LIMIT {req.limit}" if req.limit is not None else ""
        offset_clause = f"OFFSET {req.offset}" if req.offset else ""

        root_alias = self._lookup_alias(req.root_entity, index)
        sql_parts = [
            "SELECT",
            ", ".join(select_parts),
            "FROM",
            f"{self._quote_table(self._table_name(req.root_entity))} AS {self._quote_ident(root_alias)}",
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
        rows = [
            self._row_from_db(row, columns, base_columns, req.root_entity, related_columns or None)
            for row in cursor.fetchall()
        ]
        return QueryResult(rows=rows, meta={"relations_used": req.relations})

    def _handle_aggregate_query(
        self, req: RelationalQuery, joins: Sequence[str], index: JoinAliasIndex
    ):
        select_parts, group_cols = self._build_aggregations(req, index)
        conditions: List[str] = []
        params: List[Any] = []

        semantic_conditions, semantic_order, semantic_params, score_params = self._build_semantic_clauses(
            req.semantic_clauses, req.root_entity, index, aggregate=True
        )
        conditions.extend(semantic_conditions)
        params.extend(semantic_params)

        filter_sql = self._build_filters(
            req.filters, req.root_entity, index, params, case_sensitive=req.case_sensitivity
        )
        if filter_sql:
            conditions.append(filter_sql)

        if score_params:
            params.extend(score_params)

        order_clause = f"ORDER BY {semantic_order}" if semantic_order else ""

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        group_clause = f"GROUP BY {', '.join(group_cols)}" if group_cols else ""
        limit_clause = f"LIMIT {req.limit}" if req.limit is not None else ""
        offset_clause = f"OFFSET {req.offset}" if req.offset else ""

        root_alias = self._lookup_alias(req.root_entity, index)
        sql_parts = [
            "SELECT",
            ", ".join(select_parts),
            "FROM",
            f"{self._quote_table(self._table_name(req.root_entity))} AS {self._quote_ident(root_alias)}",
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
        self,
        row: Sequence[Any],
        columns: Sequence[str],
        base_columns: List[str],
        root_entity: str,
        related_fields: Optional[Dict[str, Tuple[str, str]]] = None,
    ) -> RowResult:
        data_map = dict(zip(columns, row))
        data = {col: data_map[col] for col in base_columns if col in data_map}
        related: Dict[str, Dict[str, Any]] = {}
        for col_name, value in data_map.items():
            if col_name in base_columns:
                continue
            if related_fields and col_name in related_fields:
                ent, fld = related_fields[col_name]
                related.setdefault(ent, {})[fld] = value
                continue
            if col_name.startswith("__"):
                continue
            if "__" in col_name:
                ent, fld = col_name.split("__", 1)
                if ent:
                    related.setdefault(ent, {})[fld] = value
        return RowResult(entity=root_entity, data=data, related=related)

    def _handle_semantic_only(self, req) -> SemanticOnlyResult:
        if not self.semantic_backend:
            raise RuntimeError("Semantic backend is not configured")
        matches = self.semantic_backend.search(req.entity, req.fields, req.query, req.top_k)
        return SemanticOnlyResult(matches=matches)


__all__ = ["SqlRelationalDataProvider"]

