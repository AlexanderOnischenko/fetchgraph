from __future__ import annotations

import json
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


def run_case(root: dict, ctx: ReplayContext) -> dict:
    handler = REPLAY_HANDLERS[root["id"]]
    return handler(root["input"], ctx)


def load_case_bundle(path: Path) -> tuple[dict, ReplayContext]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema") != "fetchgraph.tracer.case_bundle" or data.get("v") != 1:
        raise ValueError(f"Unsupported case bundle schema in {path}")
    root = data["root"]
    ctx = ReplayContext(
        resources=data.get("resources", {}),
        extras=data.get("extras", {}),
        base_dir=path.parent,
    )
    return root, ctx
