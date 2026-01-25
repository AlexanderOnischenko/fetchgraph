from __future__ import annotations

import pytest

from fetchgraph.replay.runtime import ReplayContext, run_case


def test_run_case_missing_handler_message() -> None:
    root = {"id": "unknown.handler", "input": {}}
    ctx = ReplayContext()
    with pytest.raises(KeyError, match="Replay handler not registered.*Did you import fetchgraph.tracer.handlers\\?"):
        run_case(root, ctx)
