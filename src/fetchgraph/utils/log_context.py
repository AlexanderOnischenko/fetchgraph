from __future__ import annotations

import logging
from contextvars import ContextVar

_run_id_var: ContextVar[str | None] = ContextVar("fetchgraph_run_id", default=None)


def set_run_id(run_id: str | None) -> None:
    _run_id_var.set(run_id)


def get_run_id(default: str | None = None) -> str | None:
    return _run_id_var.get(default)


class RunContextFilter(logging.Filter):
    """Populate logging records with run_id from context vars."""

    def __init__(self, default: str = "-") -> None:
        super().__init__()
        self.default = default

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - simple filter
        record.run_id = get_run_id(self.default) or self.default  # type: ignore[attr-defined]
        return True
