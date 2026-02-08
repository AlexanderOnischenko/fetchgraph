from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from fetchgraph.utils.path_layout import LayoutConfig, find_case_dirs, run_root_from_case_dir

from examples.demo_qa.runner import (
    Case,
    RunArtifacts,
    RunResult,
    RunTimings,
    _match_expected,
    diff_runs,
    run_one,
    summarize,
)


class _FailingRunner:
    def run_question(self, *args, **kwargs):
        raise RuntimeError("boom")


class _ArtifactRunner:
    def run_question(self, case, run_id, run_dir, **kwargs):
        return RunArtifacts(
            run_id=run_id,
            run_dir=run_dir,
            question=case.question,
            answer="ok",
            timings=RunTimings(total_s=0.01),
        )


def _read_events(path):
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_run_one_writes_events_on_early_failure(tmp_path) -> None:
    case = Case(id="case_fail", question="Q")
    runs_root = tmp_path / "runs"

    with pytest.raises(RuntimeError):
        run_one(case, _FailingRunner(), runs_root)

    run_roots = list(runs_root.iterdir())
    assert run_roots
    case_dirs = find_case_dirs(run_roots[0], case.id, LayoutConfig())
    assert case_dirs
    events_path = case_dirs[0] / "events.jsonl"
    assert events_path.exists()
    events = _read_events(events_path)
    assert any("run_started" in line for line in events)
    assert any("run_failed" in line for line in events)
    assert any("run_finished" in line for line in events)


def test_run_one_writes_events_on_late_failure(tmp_path, monkeypatch) -> None:
    case = Case(id="case_late", question="Q", expected="ok")
    runs_root = tmp_path / "runs"

    def _boom(*args, **kwargs):
        raise ValueError("late boom")

    monkeypatch.setattr("examples.demo_qa.runner._match_expected", _boom)

    with pytest.raises(ValueError):
        run_one(case, _ArtifactRunner(), runs_root)

    run_roots = list(runs_root.iterdir())
    assert run_roots
    case_dirs = find_case_dirs(run_roots[0], case.id, LayoutConfig())
    assert case_dirs
    events_path = case_dirs[0] / "events.jsonl"
    assert events_path.exists()
    events = _read_events(events_path)
    assert any("run_started" in line for line in events)
    assert any("run_failed" in line for line in events)


def test_run_one_with_events_disabled_does_not_write_file(tmp_path) -> None:
    case = Case(id="case_no_events", question="Q")
    runs_root = tmp_path / "runs"

    run_one(case, _ArtifactRunner(), runs_root, event_logger=None)

    run_roots = list(runs_root.iterdir())
    assert run_roots
    case_dirs = find_case_dirs(run_roots[0], case.id, LayoutConfig())
    assert case_dirs
    events_path = case_dirs[0] / "events.jsonl"
    assert not events_path.exists()


def test_run_one_writes_case_layout_and_events_payload(tmp_path) -> None:
    case = Case(id="case_layout", question="Q")
    runs_root = tmp_path / "runs"

    result = run_one(case, _ArtifactRunner(), runs_root)

    case_dir = Path(result.artifacts_dir)
    assert case_dir.name.startswith(f"{case.id}_")
    assert case_dir.parent.name.lower() == LayoutConfig().cases_dirname
    events_path = case_dir / "events.jsonl"
    assert events_path.exists()
    events = [json.loads(line) for line in _read_events(events_path)]
    assert any(event.get("artifacts_dir") == str(case_dir) for event in events)


def test_run_one_schema_snapshot_relative_to_run_root(tmp_path) -> None:
    case = Case(id="case_schema", question="Q")
    runs_root = tmp_path / "runs"
    schema_path = tmp_path / "schema.json"
    schema_path.write_text("{}", encoding="utf-8")

    result = run_one(case, _ArtifactRunner(), runs_root, schema_path=schema_path)

    case_dir = Path(result.artifacts_dir)
    events_path = case_dir / "events.jsonl"
    events = [json.loads(line) for line in _read_events(events_path)]
    replay_events = [event for event in events if event.get("type") == "replay_resource"]
    assert replay_events
    data_ref = replay_events[0]["data_ref"]["file"]
    run_root = run_root_from_case_dir(case_dir)
    assert (run_root / data_ref).exists()


def test_match_expected_unchecked_when_no_expectations() -> None:
    case = Case(id="c1", question="What is foo?")
    assert _match_expected(case, "anything") is None


def test_match_expected_coerces_non_string_expected_values() -> None:
    case = Case(id="c1", question="What is foo?", expected=cast(str, 42))

    mismatch = _match_expected(case, "43")
    assert mismatch is not None
    assert mismatch.passed is False
    assert "expected='42'" in (mismatch.detail or "")

    match = _match_expected(case, "42")
    assert match is not None
    assert match.passed is True


def test_match_expected_contains_pass_and_fail() -> None:
    case = Case(id="c2", question="Q", expected_contains="bar")

    match = _match_expected(case, "value BAR baz")
    assert match is not None
    assert match.passed is True

    mismatch = _match_expected(case, "value baz")
    assert mismatch is not None
    assert mismatch.passed is False
    assert "bar" in (mismatch.detail or "")

    missing_answer = _match_expected(case, None)
    assert missing_answer is not None
    assert missing_answer.passed is False
    assert missing_answer.detail == "no answer"


def test_match_expected_equals_is_case_insensitive() -> None:
    case = Case(id="c3", question="Q", expected="Alpha")

    match = _match_expected(case, "alpha")
    assert match is not None
    assert match.passed is True


def test_match_expected_list_comparison_normalizes_elements() -> None:
    case = Case(id="c4", question="Q", expected=["Foo", "Bar"])

    match = _match_expected(case, cast(str, ["foo", "bar"]))
    assert match is not None
    assert match.passed is True

    mismatch = _match_expected(case, cast(str, ["foo", "baz"]))
    assert mismatch is not None
    assert mismatch.passed is False


def test_diff_runs_tracks_regressions_and_improvements() -> None:
    baseline = [
        RunResult(
            id="ok_to_bad",
            question="",
            status="ok",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/tmp/ok",
            duration_ms=10,
            tags=[],
        ),
        RunResult(
            id="err_to_ok",
            question="",
            status="error",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/tmp/err",
            duration_ms=10,
            tags=[],
        ),
        RunResult(
            id="still_bad",
            question="",
            status="mismatch",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/tmp/ok2",
            duration_ms=10,
            tags=[],
        ),
        RunResult(
            id="missing_ok",
            question="",
            status="ok",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/tmp/miss-ok",
            duration_ms=10,
            tags=[],
        ),
        RunResult(
            id="missing_bad",
            question="",
            status="failed",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/tmp/miss-bad",
            duration_ms=10,
            tags=[],
        ),
    ]

    current = [
        RunResult(
            id="ok_to_bad",
            question="",
            status="mismatch",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/tmp/ok",
            duration_ms=10,
            tags=[],
        ),
        RunResult(
            id="err_to_ok",
            question="",
            status="ok",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/tmp/err",
            duration_ms=10,
            tags=[],
        ),
        RunResult(
            id="still_bad",
            question="",
            status="failed",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/tmp/ok2",
            duration_ms=10,
            tags=[],
        ),
        RunResult(
            id="new_ok",
            question="",
            status="ok",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/tmp/new",
            duration_ms=10,
            tags=[],
        ),
        RunResult(
            id="new_bad",
            question="",
            status="failed",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/tmp/newbad",
            duration_ms=10,
            tags=[],
        ),
    ]

    diff = diff_runs(baseline, current, fail_on="bad", require_assert=True)

    assert {row["id"] for row in diff["new_fail"]} == {"ok_to_bad", "new_bad", "missing_ok"}
    assert {row["id"] for row in diff["fixed"]} == {"err_to_ok"}
    assert {row["id"] for row in diff["still_fail"]} == {"still_bad", "missing_bad"}
    assert {"missing_ok", "missing_bad"} <= {row["id"] for row in diff["changed_status"]}
    assert diff["new_cases"] == ["new_bad", "new_ok"]


def test_summarize_counts_checked_and_unchecked() -> None:
    results = [
        RunResult(
            id="c1",
            question="",
            status="ok",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/a",
            duration_ms=10,
            tags=[],
        ),
        RunResult(
            id="c2",
            question="",
            status="unchecked",
            checked=False,
            reason=None,
            details=None,
            artifacts_dir="/b",
            duration_ms=5,
            tags=[],
        ),
        RunResult(
            id="c3",
            question="",
            status="mismatch",
            checked=True,
            reason=None,
            details=None,
            artifacts_dir="/c",
            duration_ms=7,
            tags=[],
        ),
    ]

    summary = summarize(results)
    assert summary["checked_ok"] == 1
    assert summary["unchecked_no_assert"] == 1  # counts unchecked separately
    assert summary["checked_total"] == 2
