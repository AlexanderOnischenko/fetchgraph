from __future__ import annotations

from typing import List

from .ast import Clause, ClauseOrGroup, NormalizedQuerySketch, WhereExpr
from .bound import BoundClause, BoundQuery, BoundWhereExpr, JoinPath, parse_field_ref


def bound_from_normalized(sketch: NormalizedQuerySketch) -> BoundQuery:
    def convert_clause_or_group(item: ClauseOrGroup) -> BoundClause | BoundWhereExpr:
        if isinstance(item, Clause):
            return BoundClause(
                field=parse_field_ref(item.path),
                op=item.op,
                value=item.value,
                join_path=JoinPath([]),
            )
        return convert_where(item)

    def convert_where(expr: WhereExpr) -> BoundWhereExpr:
        converted_all: List[BoundClause | BoundWhereExpr] = [
            convert_clause_or_group(item) for item in expr.all
        ]
        converted_any: List[BoundClause | BoundWhereExpr] = [
            convert_clause_or_group(item) for item in expr.any
        ]
        converted_not = (
            convert_clause_or_group(expr.not_) if expr.not_ is not None else None
        )
        return BoundWhereExpr(all=converted_all, any=converted_any, not_=converted_not)

    bound_where = convert_where(sketch.where)
    bound_get = [parse_field_ref(field) for field in sketch.get]

    return BoundQuery(
        from_=sketch.from_,
        where=bound_where,
        get=bound_get,
        with_=list(sketch.with_),
        take=sketch.take,
        meta={},
    )


def normalized_from_bound(bound: BoundQuery) -> NormalizedQuerySketch:
    def convert_clause_or_group(
        item: BoundClause | BoundWhereExpr,
    ) -> ClauseOrGroup:
        if isinstance(item, BoundClause):
            return Clause(path=item.field.raw, op=item.op, value=item.value)
        return convert_where(item)

    def convert_where(expr: BoundWhereExpr) -> WhereExpr:
        converted_all: List[ClauseOrGroup] = [
            convert_clause_or_group(item) for item in expr.all
        ]
        converted_any: List[ClauseOrGroup] = [
            convert_clause_or_group(item) for item in expr.any
        ]
        converted_not = (
            convert_clause_or_group(expr.not_) if expr.not_ is not None else None
        )
        return WhereExpr(all=converted_all, any=converted_any, not_=converted_not)

    normalized_where = convert_where(bound.where)
    normalized_get = [field.raw for field in bound.get]

    return NormalizedQuerySketch(
        from_=bound.from_,
        where=normalized_where,
        get=normalized_get,
        with_=list(bound.with_),
        take=bound.take,
    )
