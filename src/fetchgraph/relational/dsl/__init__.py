"""Relational DSL components (parsing, AST, compilation).

These utilities normalize SQL-like queries before compiling them into selectors;
they are separate from the plan normalizer used in the planning pipeline.
"""

from .ast import (
    ColumnRef,
    Comparison,
    Logical,
    OrderBy,
    SelectItem,
    SelectQuery,
    SelectStar,
    LiteralValue,
)
from .normalize import normalize_query

__all__ = [
    "ColumnRef",
    "Comparison",
    "Logical",
    "OrderBy",
    "SelectItem",
    "SelectQuery",
    "SelectStar",
    "LiteralValue",
    "normalize_query",
]
