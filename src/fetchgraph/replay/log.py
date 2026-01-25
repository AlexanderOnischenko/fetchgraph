from __future__ import annotations

from typing import Dict, Protocol


class EventLoggerLike(Protocol):
    def emit(self, event: Dict[str, object]) -> None: ...


def log_replay_case(
    logger: EventLoggerLike,
    *,
    id: str,
    input: dict,
    meta: dict | None = None,
    observed: dict | None = None,
    observed_error: dict | None = None,
    requires: list[dict] | None = None,
    note: str | None = None,
    diag: dict | None = None,
) -> None:
    if not isinstance(id, str) or not id.strip():
        raise ValueError("id must be a non-empty string")
    if not isinstance(input, dict):
        raise ValueError("input must be a dict")
    if (observed is None) == (observed_error is None):
        raise ValueError("exactly one of observed or observed_error must be set")
    if observed is not None and not isinstance(observed, dict):
        raise ValueError("observed must be a dict when provided")
    if observed_error is not None:
        if not isinstance(observed_error, dict):
            raise ValueError("observed_error must be a dict when provided")
        if not isinstance(observed_error.get("type"), str) or not observed_error.get("type"):
            raise ValueError("observed_error.type must be a non-empty string")
        if not isinstance(observed_error.get("message"), str) or not observed_error.get("message"):
            raise ValueError("observed_error.message must be a non-empty string")
    if meta is not None and not isinstance(meta, dict):
        raise ValueError("meta must be a dict when provided")
    if note is not None and not isinstance(note, str):
        raise ValueError("note must be a string when provided")
    if diag is not None and not isinstance(diag, dict):
        raise ValueError("diag must be a dict when provided")
    if requires is not None:
        if not isinstance(requires, list):
            raise ValueError("requires must be a list when provided")
        for req in requires:
            if not isinstance(req, dict):
                raise ValueError("requires entries must be dicts")
            kind = req.get("kind")
            if kind not in {"extra", "resource"}:
                raise ValueError("requires.kind must be 'extra' or 'resource'")
            req_id = req.get("id")
            if not isinstance(req_id, str) or not req_id:
                raise ValueError("requires.id must be a non-empty string")

    event: Dict[str, object] = {
        "type": "replay_case",
        "v": 2,
        "id": id,
        "input": input,
    }
    if meta is not None:
        event["meta"] = meta
    if observed is not None:
        event["observed"] = observed
    if observed_error is not None:
        event["observed_error"] = observed_error
    if requires:
        event["requires"] = requires
    if note is not None:
        event["note"] = note
    if diag is not None:
        event["diag"] = diag
    logger.emit(event)


def log_replay_point(
    logger: EventLoggerLike,
    *,
    id: str,
    meta: dict,
    input: dict,
    expected: dict,
    requires: list[str] | None = None,
    diag: dict | None = None,
    note: str | None = None,
) -> None:
    raise ValueError(
        "log_replay_point has been replaced by log_replay_case; "
        "log_replay_point no longer supports expected payloads."
    )
