from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


@dataclass
class QuerySketch:
    """Parsed but not yet normalized query sketch."""

    data: Dict[str, Any]


@dataclass
class Clause:
    path: str
    op: str
    value: Any


ClauseOrGroup = Union[Clause, "WhereExpr"]


@dataclass
class WhereExpr:
    all: List[ClauseOrGroup] = field(default_factory=list)
    any: List[ClauseOrGroup] = field(default_factory=list)
    not_: Optional[ClauseOrGroup] = None


@dataclass
class NormalizedQuerySketch:
    from_: str
    where: WhereExpr
    get: List[str] = field(default_factory=list)
    with_: List[str] = field(default_factory=list)
    take: int = 0

    # TODO: normalize dotted paths
    # TODO: schema resolve (entity/field fuzzy match)
    # TODO: join inference
    # TODO: compile to internal plan
