from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional


@dataclass(frozen=True)
class ColumnRef:
    table: str
    name: str


@dataclass(frozen=True)
class LiteralValue:
    value: Any


@dataclass(frozen=True)
class Comparison:
    left: "Expression"
    op: str
    right: "Expression"


@dataclass(frozen=True)
class Logical:
    op: Literal["and", "or"]
    clauses: list["Expression"] = field(default_factory=list)


@dataclass(frozen=True)
class SelectStar:
    pass


@dataclass(frozen=True)
class SelectItem:
    expr: ColumnRef | SelectStar
    alias: Optional[str] = None


@dataclass(frozen=True)
class OrderBy:
    column: ColumnRef
    direction: Literal["asc", "desc"] = "asc"


Expression = ColumnRef | LiteralValue | Comparison | Logical


@dataclass(frozen=True)
class SelectQuery:
    select: list[SelectItem] = field(default_factory=list)
    from_table: str = ""
    where: Optional[Expression] = None
    order_by: list[OrderBy] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
