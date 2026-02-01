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
    fixture_stem: str | None = None

    def resolve_resource_path(self, resource_path: str | Path) -> Path:
        path = Path(resource_path)
        if path.is_absolute() or self.base_dir is None:
            return path
        resource_str = resource_path.as_posix() if isinstance(resource_path, Path) else str(resource_path)
        if resource_str.startswith("resources/"):
            return self.base_dir / path
        if self.fixture_stem and self.resources:
            for resource_id, resource in self.resources.items():
                if not isinstance(resource, dict):
                    continue
                data_ref = resource.get("data_ref")
                if not isinstance(data_ref, dict):
                    continue
                file_name = data_ref.get("file")
                if file_name == resource_str:
                    return self.base_dir / "resources" / self.fixture_stem / resource_id / Path(resource_str)
        return self.base_dir / path


REPLAY_HANDLERS: Dict[str, Callable[[dict, ReplayContext], dict]] = {}


def run_case(root: dict, ctx: ReplayContext) -> dict:
    replay_id = root["id"]
    if replay_id not in REPLAY_HANDLERS:
        raise KeyError(
            f"No handler for replay id={replay_id!r}. "
            "Did you import fetchgraph.tracer.handlers?"
        )
    handler = REPLAY_HANDLERS[replay_id]
    return handler(root["input"], ctx)


def load_case_bundle(path: Path) -> tuple[dict, ReplayContext]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema") != "fetchgraph.tracer.case_bundle" or data.get("v") != 1:
        raise ValueError(f"Unsupported case bundle schema in {path}")
    root = data["root"]
    fixture_stem = path.name.replace(".case.json", "")
    ctx = ReplayContext(
        resources=data.get("resources", {}),
        extras=data.get("extras", {}),
        base_dir=path.parent,
        fixture_stem=fixture_stem,
    )
    return root, ctx
