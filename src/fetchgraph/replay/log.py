from __future__ import annotations

from typing import Dict, Protocol


class EventLoggerLike(Protocol):
    def emit(self, event: Dict[str, object]) -> None: ...


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
    error: str | None = None,
    extra: dict | None = None,
) -> None:
    event: Dict[str, object] = {
        "type": "replay_point",
        "v": 1,
        "id": id,
        "meta": meta,
        "input": input,
        "expected": expected,
    }
    if requires is not None:
        event["requires"] = requires
    if diag is not None:
        event["diag"] = diag
    if note is not None:
        event["note"] = note
    if error is not None:
        event["error"] = error
    if extra:
        event.update(extra)
    logger.emit(event)
