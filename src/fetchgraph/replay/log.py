from __future__ import annotations

import warnings
from typing import Any, Dict, Protocol, TypedDict, cast

TRACE_LIMIT = 20_000


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
        trace_value = observed_error.get("trace")
        if not isinstance(trace_value, str) or not trace_value:
            raise ValueError("observed_error.trace must be a non-empty string")
        if len(trace_value) > TRACE_LIMIT:
            observed_error = {
                **observed_error,
                "trace": f"{trace_value[:TRACE_LIMIT]}...(truncated {len(trace_value) - TRACE_LIMIT} chars)",
            }
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


def log_replay_point(logger: EventLoggerLike, **kwargs: object) -> None:
    warnings.warn(
        "log_replay_point is deprecated; use log_replay_case",
        DeprecationWarning,
        stacklevel=2,
    )
    payload = dict(kwargs)
    if "observed" not in payload and "expected" in payload:
        payload["observed"] = payload.pop("expected")

    class _ReplayPointArgs(TypedDict, total=False):
        id: str
        input: dict
        meta: dict | None
        observed: dict | None
        observed_error: dict | None
        requires: list[dict] | None
        note: str | None
        diag: dict | None

    typed_payload: _ReplayPointArgs = {}
    for key in _ReplayPointArgs.__annotations__:
        if key in payload:
            typed_payload[key] = cast(Any, payload[key])
    log_replay_case(logger, **typed_payload)
