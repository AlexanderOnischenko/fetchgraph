from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict

from fetchgraph.utils.path_layout import (
    safe_join_under_validated,
    validate_run_relative_posix,
    validate_safe_path_segment,
)

_VALID_FIXTURE_BUCKETS = {"fixed", "known_bad"}


@dataclass(frozen=True)
class ReplayContext:
    resources: Dict[str, dict] = field(default_factory=dict)
    extras: Dict[str, dict] = field(default_factory=dict)
    base_dir: Path | None = None
    fixture_stem: str | None = None
    fixture_bucket_dir: Path | None = None

    def resolve_resource_path(self, resource_path: str | Path) -> Path:
        path = Path(resource_path)
        if self.base_dir is None:
            raise ValueError("base_dir is required for replay")
        if path.is_absolute():
            raise ValueError(f"resource path must be relative to base_dir: {path}")
        resource_str = resource_path.as_posix() if isinstance(resource_path, Path) else str(resource_path)
        rel_path = validate_run_relative_posix(resource_str)

        fixture_root = self.fixture_bucket_dir or self.base_dir
        fixture_stem = None
        if self.fixture_stem:
            fixture_stem = validate_run_relative_posix(self.fixture_stem).as_posix()

        if resource_str.startswith("resources/"):
            if not fixture_stem:
                raise ValueError("fixture_stem is required for resources/ paths")
            prefix = f"resources/{fixture_stem}/"
            if not resource_str.startswith(prefix):
                raise ValueError(f"resource path must be under {prefix}")
            return safe_join_under_validated(fixture_root, rel_path)

        if fixture_stem and self.resources:
            for resource_id, resource in self.resources.items():
                resource_id = validate_safe_path_segment(resource_id, what="resource_id")
                if not isinstance(resource, dict):
                    continue
                data_ref = resource.get("data_ref")
                if not isinstance(data_ref, dict):
                    continue
                file_name = data_ref.get("file")
                if file_name == resource_str:
                    fixture_rel = Path("resources") / fixture_stem / resource_id / rel_path
                    fixture_rel = validate_run_relative_posix(fixture_rel.as_posix())
                    return safe_join_under_validated(fixture_root, fixture_rel)

        return safe_join_under_validated(self.base_dir, rel_path)


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


def _infer_fixture_layout(path: Path) -> tuple[Path, str]:
    resolved = path.resolve()
    if not resolved.name.endswith(".case.json"):
        raise ValueError(f"Unsupported case bundle filename: {resolved}")

    parts = resolved.parts
    # Prefer canonical .../replay_cases/<bucket>/... anchor closest to the case file.
    for idx in range(len(parts) - 3, -1, -1):
        if parts[idx] != "replay_cases":
            continue
        bucket_idx = idx + 1
        if parts[bucket_idx] not in _VALID_FIXTURE_BUCKETS:
            continue
        bucket_dir = Path(*parts[: bucket_idx + 1])
        rel = resolved.relative_to(bucket_dir)
        if rel.parts and rel.parts[0] == "resources":
            continue
        return bucket_dir, rel.as_posix().removesuffix(".case.json")

    # Fallback: choose the left-most bucket segment to avoid nested "fixed/known_bad" stem collisions.
    for idx in range(len(parts) - 1):
        if parts[idx] not in _VALID_FIXTURE_BUCKETS:
            continue
        bucket_dir = Path(*parts[: idx + 1])
        rel = resolved.relative_to(bucket_dir)
        if rel.parts and rel.parts[0] == "resources":
            continue
        return bucket_dir, rel.as_posix().removesuffix(".case.json")

    return resolved.parent, resolved.name.removesuffix(".case.json")


def load_case_bundle(path: Path) -> tuple[dict, ReplayContext]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema") != "fetchgraph.tracer.case_bundle" or data.get("v") != 1:
        raise ValueError(f"Unsupported case bundle schema in {path}")
    root = data["root"]
    fixture_bucket_dir, fixture_stem = _infer_fixture_layout(path)
    ctx = ReplayContext(
        resources=data.get("resources", {}),
        extras=data.get("extras", {}),
        base_dir=path.resolve().parent,
        fixture_stem=fixture_stem,
        fixture_bucket_dir=fixture_bucket_dir,
    )
    return root, ctx
