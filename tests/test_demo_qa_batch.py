from __future__ import annotations

import itertools
import json
import os
import time
from pathlib import Path
from typing import cast

import pytest

from examples.demo_qa.batch import (
    _fingerprint_dir,
    _only_failed_selection,
    _only_missed_selection,
    bad_statuses,
    is_failure,
    render_markdown,
    write_results,
)
from examples.demo_qa.runs.coverage import _missed_case_ids
from examples.demo_qa.runs.layout import _latest_markers, _update_latest_markers
from examples.demo_qa.runner import DiffReport, RunResult, diff_runs


@pytest.mark.parametrize(
    "fail_on,require_assert",
    itertools.product(["bad", "error", "unchecked", "any", "skipped"], [False, True]),
)
def test_is_failure_matches_bad_statuses(fail_on: str, require_assert: bool) -> None:
    statuses = ["ok", "mismatch", "failed", "error", "unchecked", "plan_only", "skipped"]
    bad = bad_statuses(fail_on, require_assert)
    assert bad  # sanity check
    for status in statuses:
        assert is_failure(status, fail_on, require_assert) == (status in bad)


def test_render_markdown_uses_fail_policy() -> None:
    compare = cast(
        DiffReport,
        {
        "base_counts": {"ok": 0, "mismatch": 2, "error": 1, "failed": 0},
        "new_counts": {"ok": 1, "mismatch": 0, "error": 0, "failed": 0},
        "base_bad_total": 1,
        "new_bad_total": 0,
        "fail_on": "error",
        "require_assert": False,
        "new_fail": [],
        "fixed": [],
        "still_fail": [],
        "all_ids": [],
        },
    )
    report = render_markdown(compare, None)
    assert "- Base OK: 0, Bad: 1" in report
    assert "- New  OK: 1, Bad: 0" in report


def test_fingerprint_sensitive_to_file_changes(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    target = data / "file.txt"
    target.write_text("aaa", encoding="utf-8")
    first = _fingerprint_dir(data)

    target.write_text("bbb", encoding="utf-8")
    now = time.time() + 1
    os.utime(target, (now, now))
    second = _fingerprint_dir(data)

    assert first["hash"] != second["hash"]
    assert first["files_count"] == second["files_count"] == 1
    assert "files" not in first


def _mk_result(case_id: str, status: str) -> RunResult:
    return RunResult(
        id=case_id,
        question="q",
        status=status,
        checked=True,
        reason=None,
        details=None,
        artifacts_dir=f"/tmp/{case_id}",
        duration_ms=1000,
        tags=[],
    )


def test_compare_is_deterministic() -> None:
    base_results = [_mk_result("b", "ok"), _mk_result("a", "ok")]
    new_results = [_mk_result("a", "failed"), _mk_result("b", "ok")]

    first = diff_runs(base_results, new_results, fail_on="bad", require_assert=False)
    second = diff_runs(list(reversed(base_results)), list(reversed(new_results)), fail_on="bad", require_assert=False)

    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_write_results_is_deterministic(tmp_path: Path) -> None:
    out = tmp_path / "results.jsonl"
    res = _mk_result("a", "ok")

    write_results(out, [res])

    line = out.read_text(encoding="utf-8").strip()
    expected = json.dumps(res.to_json(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert line == expected


def test_missed_case_ids_diff_planned_and_executed() -> None:
    planned = ["a", "b", "c", "a"]
    executed = {_mk_result("b", "ok").id: _mk_result("b", "ok")}
    assert _missed_case_ids(planned, executed) == {"a", "c"}


def test_only_failed_selection_uses_overlay_and_baseline() -> None:
    baseline = {"a": _mk_result("a", "failed"), "b": _mk_result("b", "failed")}
    overlay = {"a": _mk_result("a", "ok"), "c": _mk_result("c", "failed")}

    selection, breakdown = _only_failed_selection(
        baseline,
        overlay,
        fail_on="bad",
        require_assert=False,
        artifacts_dir=None,
        anti_flake_passes=1,
    )

    assert selection == {"b", "c"}
    assert breakdown["healed"] == {"a"}
    assert breakdown["baseline_failures"] == {"a", "b"}
    assert breakdown["new_failures"] == {"c"}


def test_only_missed_selection_uses_overlay_executed() -> None:
    baseline = {"a": _mk_result("a", "ok")}
    overlay = {"c": _mk_result("c", "ok")}

    missed, breakdown = _only_missed_selection(["a", "b", "c"], baseline, overlay)

    assert missed == {"b"}
    assert breakdown["missed_base"] == {"b", "c"}
    assert breakdown["overlay_executed"] == {"c"}


def test_update_latest_markers_handles_tag(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "data" / ".runs"
    run_dir = artifacts_dir / "runs" / "20240101_cases"
    results_path = run_dir / "results.jsonl"
    run_dir.mkdir(parents=True)
    results_path.write_text("{}", encoding="utf-8")

    _update_latest_markers(run_dir, results_path, artifacts_dir, "feature/beta", results_complete=True)

    latest_default = _latest_markers(artifacts_dir, None)
    assert latest_default.complete.read_text(encoding="utf-8").strip() == str(run_dir)
    assert latest_default.results.read_text(encoding="utf-8").strip() == str(results_path)
    assert latest_default.any_run.read_text(encoding="utf-8").strip() == str(run_dir)
    assert latest_default.legacy_run.read_text(encoding="utf-8").strip() == str(run_dir)

    latest_tag = _latest_markers(artifacts_dir, "feature/beta")
    assert latest_tag.complete.read_text(encoding="utf-8").strip() == str(run_dir)
    assert latest_tag.results.read_text(encoding="utf-8").strip() == str(results_path)
    assert latest_tag.any_run.read_text(encoding="utf-8").strip() == str(run_dir)
    assert latest_tag.legacy_run.read_text(encoding="utf-8").strip() == str(run_dir)

    partial_dir = artifacts_dir / "runs" / "20240102_cases"
    partial_results = partial_dir / "results.jsonl"
    partial_dir.mkdir(parents=True)
    partial_results.write_text("{}", encoding="utf-8")

    _update_latest_markers(partial_dir, partial_results, artifacts_dir, "feature/beta", results_complete=False)

    refreshed_default = _latest_markers(artifacts_dir, None)
    assert refreshed_default.complete.read_text(encoding="utf-8").strip() == str(run_dir)
    assert refreshed_default.results.read_text(encoding="utf-8").strip() == str(results_path)
    assert refreshed_default.any_run.read_text(encoding="utf-8").strip() == str(partial_dir)
    assert refreshed_default.legacy_run.read_text(encoding="utf-8").strip() == str(run_dir)

    refreshed_tag = _latest_markers(artifacts_dir, "feature/beta")
    assert refreshed_tag.complete.read_text(encoding="utf-8").strip() == str(run_dir)
    assert refreshed_tag.results.read_text(encoding="utf-8").strip() == str(results_path)
    assert refreshed_tag.any_run.read_text(encoding="utf-8").strip() == str(partial_dir)
    assert refreshed_tag.legacy_run.read_text(encoding="utf-8").strip() == str(run_dir)
