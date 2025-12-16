from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ResolutionPolicy:
    allow_auto_add_relations: bool = True
    max_auto_join_depth: int = 1
    ambiguity_strategy: str = "ask"  # "ask" | "best"
