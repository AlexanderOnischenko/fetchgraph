from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict


@dataclass(frozen=True)
class ReplayContext:
    resources: Dict[str, dict] = field(default_factory=dict)
    extras: Dict[str, dict] = field(default_factory=dict)


REPLAY_HANDLERS: Dict[str, Callable[[dict, ReplayContext], dict]] = {}
