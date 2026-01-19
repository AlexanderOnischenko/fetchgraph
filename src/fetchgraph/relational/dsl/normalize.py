from __future__ import annotations

"""Normalization helpers for the relational DSL.

This module is intentionally decoupled from plan normalization. It is meant to
be used by the relational DSL parser/compiler pipeline to canonicalize LLM-
produced SQL-like queries before they are converted into structured selectors.
"""

from dataclasses import replace
import re
from typing import Dict, Iterable, Mapping, Optional, Sequence

from .ast import (
    ColumnRef,
    Comparison,
    Expression,
    Logical,
    OrderBy,
    SelectItem,
    SelectQuery,
    SelectStar,
)

ComparisonAliases = {
    "=": "=",
    "==": "=",
    "eq": "=",
    "equals": "=",
    "!=": "!=",
    "<>": "!=",
    "ne": "!=",
    "not_equals": "!=",
    "neq": "!=",
    "not equal": "!=",
    "not equal to": "!=",
    "<": "<",
    "lt": "<",
    "<=": "<=",
    "lte": "<=",
    ">": ">",
    "gt": ">",
    ">=": ">=",
    "gte": ">=",
}


def normalize_query(
    query: SelectQuery,
    *,
    table_columns: Optional[Mapping[str, Sequence[str]]] = None,
) -> SelectQuery:
    normalized = _normalize_query_structure(query)
    normalized = _normalize_where(normalized)
    normalized = _normalize_select(normalized, table_columns=table_columns)
    normalized = _normalize_order_by(normalized)
    return _resolve_aliases(normalized)


def _normalize_query_structure(query: SelectQuery) -> SelectQuery:
    normalized_table = _normalize_identifier(query.from_table)
    return replace(
        query,
        from_table=normalized_table,
        limit=_normalize_limit(query.limit),
        offset=_normalize_offset(query.offset),
    )


def _normalize_select(
    query: SelectQuery,
    *,
    table_columns: Optional[Mapping[str, Sequence[str]]],
) -> SelectQuery:
    normalized_select = [_normalize_select_item(item) for item in query.select]
    expanded_select = _expand_select_star(
        normalized_select,
        table=query.from_table,
        table_columns=table_columns,
    )
    return replace(query, select=expanded_select)


def _normalize_select_item(item: SelectItem) -> SelectItem:
    expr = item.expr
    if isinstance(expr, ColumnRef):
        expr = ColumnRef(table=_normalize_identifier(expr.table), name=_normalize_identifier(expr.name))
    alias = _normalize_identifier(item.alias) if item.alias else None
    return replace(item, expr=expr, alias=alias)


def _expand_select_star(
    items: Sequence[SelectItem],
    *,
    table: str,
    table_columns: Optional[Mapping[str, Sequence[str]]],
) -> list[SelectItem]:
    if not items:
        return list(items)
    if not _contains_select_star(items):
        return list(items)
    if not table_columns:
        return [item for item in items if not isinstance(item.expr, SelectStar)]
    columns = _resolve_columns(table, table_columns)
    expanded: list[SelectItem] = []
    for item in items:
        if isinstance(item.expr, SelectStar):
            expanded.extend(
                SelectItem(expr=ColumnRef(table=table, name=column), alias=None)
                for column in columns
            )
        else:
            expanded.append(item)
    return expanded


def _normalize_where(query: SelectQuery) -> SelectQuery:
    if query.where is None:
        return query
    return replace(query, where=_normalize_expression(query.where))


def _normalize_expression(expr: Expression) -> Expression:
    if isinstance(expr, ColumnRef):
        return ColumnRef(table=_normalize_identifier(expr.table), name=_normalize_identifier(expr.name))
    if isinstance(expr, Comparison):
        return Comparison(
            left=_normalize_expression(expr.left),
            op=_normalize_comparison(expr.op),
            right=_normalize_expression(expr.right),
        )
    if isinstance(expr, Logical):
        op = expr.op.lower()
        clauses: list[Expression] = []
        for clause in expr.clauses:
            normalized_clause = _normalize_expression(clause)
            if isinstance(normalized_clause, Logical) and normalized_clause.op == op:
                clauses.extend(normalized_clause.clauses)
            else:
                clauses.append(normalized_clause)
        return Logical(op=op, clauses=clauses)
    return expr


def _normalize_order_by(query: SelectQuery) -> SelectQuery:
    if not query.order_by:
        return query
    normalized = [
        OrderBy(
            column=ColumnRef(
                table=_normalize_identifier(item.column.table),
                name=_normalize_identifier(item.column.name),
            ),
            direction=_normalize_order_direction(item.direction),
        )
        for item in query.order_by
    ]
    return replace(query, order_by=normalized)


def _normalize_comparison(op: str) -> str:
    cleaned = op.strip().lower()
    return ComparisonAliases.get(cleaned, cleaned)


def _normalize_identifier(value: Optional[str]) -> str:
    if value is None:
        return ""
    cleaned = value.strip()
    if cleaned.startswith("`") and cleaned.endswith("`") and len(cleaned) > 1:
        cleaned = cleaned[1:-1]
    elif cleaned.startswith('"') and cleaned.endswith('"') and len(cleaned) > 1:
        cleaned = cleaned[1:-1]
    elif cleaned.startswith("[") and cleaned.endswith("]") and len(cleaned) > 1:
        cleaned = cleaned[1:-1]
    elif cleaned.startswith("'") and cleaned.endswith("'") and len(cleaned) > 1:
        cleaned = cleaned[1:-1]
    cleaned = re.sub(r"\s+", "_", cleaned)
    cleaned = re.sub(r"[^\w]", "", cleaned)
    return cleaned.lower()


def _normalize_order_direction(direction: str) -> str:
    cleaned = direction.strip().lower()
    if cleaned not in {"asc", "desc"}:
        return "asc"
    return cleaned


def _normalize_limit(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    return max(0, value)


def _normalize_offset(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    return max(0, value)


def _contains_select_star(items: Iterable[SelectItem]) -> bool:
    return any(isinstance(item.expr, SelectStar) for item in items)


def _resolve_columns(table: str, table_columns: Mapping[str, Sequence[str]]) -> list[str]:
    columns = list(table_columns.get(table, []))
    return [_normalize_identifier(col) for col in columns]


def _resolve_aliases(query: SelectQuery) -> SelectQuery:
    alias_map: Dict[str, ColumnRef] = {}
    for item in query.select:
        if item.alias and isinstance(item.expr, ColumnRef):
            alias_map[item.alias] = item.expr
    if not alias_map:
        return query
    normalized_where = _replace_aliases(query.where, alias_map) if query.where else None
    normalized_order_by = [
        _replace_order_by_alias(item, alias_map) for item in query.order_by
    ]
    return replace(query, where=normalized_where, order_by=normalized_order_by)


def _replace_aliases(expr: Expression, alias_map: Mapping[str, ColumnRef]) -> Expression:
    if isinstance(expr, ColumnRef):
        if not expr.table and expr.name in alias_map:
            return alias_map[expr.name]
        return expr
    if isinstance(expr, Comparison):
        return Comparison(
            left=_replace_aliases(expr.left, alias_map),
            op=expr.op,
            right=_replace_aliases(expr.right, alias_map),
        )
    if isinstance(expr, Logical):
        return Logical(op=expr.op, clauses=[_replace_aliases(c, alias_map) for c in expr.clauses])
    return expr


def _replace_order_by_alias(item: OrderBy, alias_map: Mapping[str, ColumnRef]) -> OrderBy:
    column = item.column
    if not column.table and column.name in alias_map:
        column = alias_map[column.name]
    return OrderBy(column=column, direction=item.direction)
