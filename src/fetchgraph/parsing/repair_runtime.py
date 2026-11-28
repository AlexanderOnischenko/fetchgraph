"""Fetchgraph-owned opt-in crash recording configuration.

The host application decides whether to collect dumps. The imported repair
library never reads Fetchgraph environment variables. Resolve settings when
a parser/retrieval engine is constructed, not during an LLM response.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from llm_output_repair import CrashRecorder

ENV_CAPTURE = "FETCHGRAPH_REPAIR_CRASH_CAPTURE"
ENV_DUMP_DIR = "FETCHGRAPH_REPAIR_DUMP_DIR"

log = logging.getLogger(__name__)


def recorder_for(stage: str) -> CrashRecorder | None:
    """Resolve the explicit off/failed mode and validate it before inference.

    A leftover dump path does not grant consent to capture. INVALID requested
    modes/directories fail at construction, not after an expensive model call.
    Returned recorders remain attached to their host parser/engine instance,
    so changing the process environment cannot flip capture mid-run.
    """
    mode = os.environ.get(ENV_CAPTURE, "off")
    if mode == "off":
        log.info("LLM repair crash capture: OFF")
        return None
    if mode != "failed":
        raise ValueError(f"{ENV_CAPTURE} must be 'off' or 'failed'")

    configured = os.environ.get(ENV_DUMP_DIR)
    if configured is None or not configured.strip():
        raise ValueError(f"{ENV_DUMP_DIR} is required when {ENV_CAPTURE}=failed")
    path = Path(configured).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{ENV_DUMP_DIR} must name an absolute private directory")

    recorder = CrashRecorder(
        directory=path,
        capture="failed",
        source={"project": "fetchgraph", "stage": stage},
    )
    log.info("LLM repair crash capture: FAILED storage=%s", path)
    return recorder
