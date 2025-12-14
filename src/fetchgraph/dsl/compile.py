from __future__ import annotations

from typing import Any, Iterable, List, Optional, Tuple, Union

from .ast import Clause, ClauseOrGroup, NormalizedQuerySketch
from fetchgraph.relational.models import (
    ComparisonFilter,
    FilterClause,
    LogicalFilter,
    RelationalQuery,
    SelectExpr,
)


_MappedComparison = Tuple[str, Any]


def _map_op(op: str, value: Any) -> Union[FilterClause, List[_MappedComparison], _MappedComparison]:
    """Map DSL operator to provider operator or filter.

    Returns either a tuple representing comparison operator/value or a list of such tuples
    for compound operations. Between is converted to two comparisons combined later.
    """

    op = op.lower()
    if op == "is":
        return "=", value
    if op == "before":
        return "<", value
    if op == "after":
        return ">", value
    if op == "contains":
        return "ilike", value
    if op == "between":
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise ValueError("between operator expects a list or tuple with exactly two values")
        return [(">=", value[0]), ("<=", value[1])]

    if op in {"=", "!=", "<", ">", "<=", ">=", "in", "not_in", "like", "ilike"}:
        return op, value

    raise ValueError(f"Unsupported operator: {op}")


def _compile_clause(clause: Clause) -> FilterClause:
    mapped = _map_op(clause.op, clause.value)

    if isinstance(mapped, LogicalFilter):
        return mapped

    if isinstance(mapped, list):
        compiled = [
            ComparisonFilter(entity=None, field=clause.path, op=op, value=val)
            for op, val in mapped
        ]
        if len(compiled) == 1:
            return compiled[0]
        return LogicalFilter(op="and", clauses=compiled)

    op, value = mapped
    return ComparisonFilter(entity=None, field=clause.path, op=op, value=value)


def _invert_not_clause(clause: Clause) -> Clause:
    if clause.op == "=":
        return Clause(path=clause.path, op="!=", value=clause.value)
    if clause.op == "in":
        return Clause(path=clause.path, op="not_in", value=clause.value)
    if clause.op == "!=":
        return Clause(path=clause.path, op="=", value=clause.value)
    raise ValueError(f"NOT for op {clause.op} not supported yet")


def _compile_where(expr: ClauseOrGroup) -> FilterClause:
    if isinstance(expr, Clause):
        return _compile_clause(expr)

    compiled_all: List[FilterClause] = [_compile_where(item) for item in expr.all]
    compiled_any: List[FilterClause] = [_compile_where(item) for item in expr.any]

    all_filter: Optional[FilterClause] = None
    any_filter: Optional[FilterClause] = None

    if compiled_all:
        all_filter = compiled_all[0] if len(compiled_all) == 1 else LogicalFilter(op="and", clauses=compiled_all)

    if compiled_any:
        any_filter = compiled_any[0] if len(compiled_any) == 1 else LogicalFilter(op="or", clauses=compiled_any)

    not_filter: Optional[FilterClause] = None
    if expr.not_ is not None:
        if not isinstance(expr.not_, Clause):
            raise ValueError("NOT is only supported for simple comparisons")
        inverted = _invert_not_clause(expr.not_)
        not_filter = _compile_clause(inverted)

    clauses: List[FilterClause] = []
    if all_filter is not None:
        clauses.append(all_filter)
    if any_filter is not None:
        clauses.append(any_filter)
    if not_filter is not None:
        clauses.append(not_filter)

    if not clauses:
        raise ValueError("Empty where expression")
    if len(clauses) == 1:
        return clauses[0]
    return LogicalFilter(op="and", clauses=clauses)


def _collect_paths(expr: ClauseOrGroup) -> Iterable[str]:
    if isinstance(expr, Clause):
        yield expr.path
        return

    for item in expr.all:
        yield from _collect_paths(item)
    for item in expr.any:
        yield from _collect_paths(item)
    if expr.not_ is not None:
        yield from _collect_paths(expr.not_)


def _infer_relations(sketch: NormalizedQuerySketch) -> List[str]:
    relations = set(sketch.with_)

    def process_path(path: str) -> None:
        if "." not in path:
            return
        parts = path.split(".")
        if len(parts) == 2:
            rel, _ = parts
            if rel != sketch.from_:
                relations.add(rel)
            return
        raise ValueError(f"Multi-hop dotted paths are not supported yet: {path}")

    for path in _collect_paths(sketch.where):
        process_path(path)
    for field in sketch.get:
        process_path(field)

    return sorted(relations)


def compile_relational_query(sketch: NormalizedQuerySketch) -> RelationalQuery:
    filters: Optional[FilterClause] = None
    if sketch.where.all or sketch.where.any or sketch.where.not_ is not None:
        filters = _compile_where(sketch.where)

    select: List[SelectExpr] = []
    if sketch.get and "*" not in sketch.get:
        select = [SelectExpr(expr=field) for field in sketch.get]

    relations = _infer_relations(sketch)

    return RelationalQuery(
        root_entity=sketch.from_,
        select=select,
        filters=filters,
        relations=relations,
        limit=sketch.take,
        offset=0,
        case_sensitivity=False,
    )


def compile_relational_selectors(sketch: NormalizedQuerySketch) -> dict:
    return compile_relational_query(sketch).model_dump()
