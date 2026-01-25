from __future__ import annotations

from pathlib import Path

from ..runtime import REPLAY_HANDLERS, ReplayContext


def replay_resource_read(inp: dict, ctx: ReplayContext) -> dict:
    resource_id = inp.get("resource_id")
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("resource_id is required for resource.read")
    resource = ctx.resources.get(resource_id)
    if not isinstance(resource, dict):
        raise KeyError(f"Resource not found: {resource_id}")
    data_ref = resource.get("data_ref")
    if not isinstance(data_ref, dict):
        raise ValueError(f"Resource {resource_id} is missing data_ref")
    file_name = data_ref.get("file")
    if not isinstance(file_name, str) or not file_name:
        raise ValueError(f"Resource {resource_id} data_ref.file is required")
    resolved = ctx.resolve_resource_path(Path(file_name))
    return {"content": resolved.read_text(encoding="utf-8")}


REPLAY_HANDLERS["resource.read"] = replay_resource_read
