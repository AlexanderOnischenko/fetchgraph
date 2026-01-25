from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict


@dataclass(frozen=True)
class ReplayContext:
    resources: Dict[str, dict] = field(default_factory=dict)
    extras: Dict[str, dict] = field(default_factory=dict)
    base_dir: Path | None = None

    def resolve_resource_path(self, resource_path: str | Path) -> Path:
        path = Path(resource_path)
        if path.is_absolute() or self.base_dir is None:
            return path
        return self.base_dir / path


REPLAY_HANDLERS: Dict[str, Callable[[dict, ReplayContext], dict]] = {}
