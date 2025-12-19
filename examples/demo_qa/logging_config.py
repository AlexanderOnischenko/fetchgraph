from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

from fetchgraph.utils import RunContextFilter, set_run_id


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - formatting logic
        payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "run_id": getattr(record, "run_id", "-"),
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def _make_formatter(jsonl: bool) -> logging.Formatter:
    if jsonl:
        return JsonFormatter()
    return logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s [run=%(run_id)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def configure_logging(
    *,
    level: str = "INFO",
    log_dir: Path | None,
    to_stderr: bool = False,
    jsonl: bool = False,
    run_id: str | None = None,
) -> Path | None:
    """Configure logging for demo_qa CLI."""
    if run_id:
        set_run_id(run_id)

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    handlers: list[logging.Handler] = []
    formatter = _make_formatter(jsonl)
    context_filter = RunContextFilter()
    log_file: Path | None = None

    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = "jsonl" if jsonl else "log"
        log_file = log_dir / f"demo_qa_{timestamp}.{suffix}"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=5_000_000,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(context_filter)
        handlers.append(file_handler)

    if to_stderr:
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(formatter)
        stream_handler.addFilter(context_filter)
        handlers.append(stream_handler)

    if not handlers:
        # Avoid silent drop due to NullHandler in library.
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(formatter)
        stream_handler.addFilter(context_filter)
        handlers.append(stream_handler)

    for handler in handlers:
        root.addHandler(handler)

    return log_file


__all__ = ["configure_logging"]
