from __future__ import annotations

from fetchgraph.replay.runtime import REPLAY_HANDLERS, ReplayContext


def _resource_read_handler(payload: dict, ctx: ReplayContext) -> dict:
    resource_id = payload.get("resource_id")
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("resource_id is required")
    resource = ctx.resources.get(resource_id)
    if not isinstance(resource, dict):
        raise KeyError(f"Missing resource {resource_id!r}")
    data_ref = resource.get("data_ref")
    if not isinstance(data_ref, dict):
        raise ValueError("resource.data_ref must be a dict")
    file_name = data_ref.get("file")
    if not isinstance(file_name, str) or not file_name:
        raise ValueError("resource.data_ref.file must be a string")
    path = ctx.resolve_resource_path(file_name)
    text = path.read_text(encoding="utf-8")
    return {"text": text}


REPLAY_HANDLERS.setdefault("resource_read.v1", _resource_read_handler)
