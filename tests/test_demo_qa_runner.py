from __future__ import annotations

from examples.demo_qa.runner import Case, RunResult, _match_expected, diff_runs, summarize


def test_match_expected_unchecked_when_no_expectations() -> None:
    case = Case(id="c1", question="What is foo?")
    assert _match_expected(case, "anything") is None


def test_match_expected_contains_pass_and_fail() -> None:
    case = Case(id="c2", question="Q", expected_contains="bar")

    match = _match_expected(case, "value bar baz")
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
    ]

    diff = diff_runs(baseline, current, fail_on="bad", require_assert=True)

    assert {row["id"] for row in diff["new_fail"]} == {"ok_to_bad"}
    assert {row["id"] for row in diff["fixed"]} == {"err_to_ok"}
    assert {row["id"] for row in diff["still_fail"]} == {"still_bad"}
    assert diff["new_cases"] == ["new_ok"]


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
