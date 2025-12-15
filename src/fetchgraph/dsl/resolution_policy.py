from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ResolutionPolicy:
    max_auto_join_depth: int = 2
    allow_auto_add_relations: bool = True
    ambiguity_strategy: str = "best"
    prefer_declared_relations: bool = True
