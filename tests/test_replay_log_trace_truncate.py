from __future__ import annotations

from fetchgraph.replay.log import TRACE_LIMIT, log_replay_case


class _Recorder:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def emit(self, event: dict) -> None:
        self.events.append(event)


def test_log_replay_case_truncates_trace() -> None:
    logger = _Recorder()
    trace = "A" * (TRACE_LIMIT + 500)
    log_replay_case(
        logger,
        id="plan_normalize.spec_v1",
        input={"spec": {"provider": "sql"}},
        observed_error={"type": "Boom", "message": "bad", "trace": trace},
    )
    assert logger.events
    observed_error = logger.events[0]["observed_error"]
    assert isinstance(observed_error, dict)
    truncated = observed_error["trace"]
    assert isinstance(truncated, str)
    assert len(truncated) < len(trace)
    assert "truncated" in truncated
