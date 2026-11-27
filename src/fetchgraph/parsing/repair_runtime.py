"""Application-owned opt-in crash storage, shared by Fetchgraph JSON paths."""

from __future__ import annotations

import os
from pathlib import Path

from llm_output_repair import CrashRecorder

ENV_DUMP_DIR = "FETCHGRAPH_REPAIR_DUMP_DIR"


def recorder_for(stage: str) -> CrashRecorder | None:
    """Create a local recorder only when an absolute private path is configured.

    No default path, global singleton, telemetry, upload, or implicit opt-in.
    Invalid explicit configuration fails visibly instead of silently claiming
    that crash capture is active.
    """
    configured = os.environ.get(ENV_DUMP_DIR)
    if not configured:
        return None
    path = Path(configured).expanduser()
    if not path.is_absolute():
        raise ValueError(
            f"{ENV_DUMP_DIR} must name an absolute private directory"
        )
    return CrashRecorder(
        directory=path,
        capture="failed",
        source={"project": "fetchgraph", "stage": stage},
    )
