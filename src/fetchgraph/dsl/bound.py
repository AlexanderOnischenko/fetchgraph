from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional, Union


@dataclass
class JoinPath:
    relations: List[str]


@dataclass
class FieldRef:
    raw: str
    qualifier: Optional[str]
    field: str
    entity: Optional[str] = None


@dataclass
class BoundClause:
    field: FieldRef
    op: str
    value: Any
    join_path: JoinPath = field(default_factory=lambda: JoinPath([]))


BoundClauseOrGroup = Union[BoundClause, "BoundWhereExpr"]


@dataclass
class BoundWhereExpr:
    all: List[BoundClauseOrGroup] = field(default_factory=list)
    any: List[BoundClauseOrGroup] = field(default_factory=list)
    not_: Optional[BoundClauseOrGroup] = None


@dataclass
class BoundQuery:
    from_: str
    where: BoundWhereExpr
    get: List[FieldRef]
    with_: List[str]
    take: int
    meta: dict = field(default_factory=dict)


def parse_field_ref(raw: str) -> FieldRef:
    if "." in raw:
        qualifier, rest = raw.split(".", 1)
        return FieldRef(raw=raw, qualifier=qualifier, field=rest)
    return FieldRef(raw=raw, qualifier=None, field=raw)
