from __future__ import annotations

import itertools
import json
import os
import time
from pathlib import Path
from typing import cast

import pytest

from examples.demo_qa.batch import (
    _consecutive_passes,
    _fingerprint_dir,
    _format_healed_explain,
    _only_failed_selection,
    _only_missed_selection,
    _planned_pool_from_meta,
    bad_statuses,
    is_failure,
    render_markdown,
    write_results,
)
from examples.demo_qa.runner import DiffReport, RunResult, diff_runs
from examples.demo_qa.runs.coverage import _missed_case_ids
from examples.demo_qa.runs.layout import _latest_markers, _update_latest_markers


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


def test_only_missed_ignores_overlay_when_scope_mismatches() -> None:
    baseline = {"A": _mk_result("A", "ok")}

    missed, breakdown = _only_missed_selection(
        ["A", "B", "C"],
        baseline,
        None,
        overlay_scope_hash="overlay_scope",
        overlay_scope_matches_current=False,
        overlay_ignored_reason="scope_mismatch",
    )

    assert missed == {"B", "C"}
    assert breakdown["missed_base"] == {"B", "C"}
    assert breakdown["overlay_executed"] == set()
    assert breakdown["overlay_scope_hash"] == "overlay_scope"
    assert breakdown["overlay_scope_matches_current"] is False
    assert breakdown["overlay_ignored_reason"] == "scope_mismatch"


def test_only_missed_applies_overlay_when_scope_matches() -> None:
    baseline = {"A": _mk_result("A", "ok")}
    overlay = {"B": _mk_result("B", "ok")}

    missed, breakdown = _only_missed_selection(
        ["A", "B", "C"],
        baseline,
        overlay,
        overlay_scope_hash="scope_current",
        overlay_scope_matches_current=True,
    )

    assert missed == {"C"}
    assert breakdown["missed_base"] == {"B", "C"}
    assert breakdown["overlay_executed"] == {"B"}
    assert breakdown["overlay_scope_hash"] == "scope_current"
    assert breakdown["overlay_scope_matches_current"] is True
    assert "overlay_ignored_reason" not in breakdown


def test_only_failed_strict_scope_ignores_overlay_pass(tmp_path: Path) -> None:
    baseline = {"A": _mk_result("A", "failed")}
    overlay = {"A": _mk_result("A", "ok")}
    overlay_meta = {"run_id": "overlay", "scope_hash": "scope_overlay", "ended_at": "2024-01-01T00:00:00Z"}

    selection, breakdown = _only_failed_selection(
        baseline,
        overlay,
        fail_on="bad",
        require_assert=False,
        artifacts_dir=tmp_path,
        tag="t1",
        scope_hash="scope_current",
        anti_flake_passes=1,
        strict_scope_history=True,
        overlay_run_meta=overlay_meta,
        overlay_run_path=tmp_path,
        explain_selection=True,
    )

    assert selection == {"A"}
    assert breakdown["healed"] == set()
    explain_lines = cast(list[str], breakdown.get("explain", []) or [])
    assert any("overlay_scope_matches_current=False" in line for line in explain_lines)


def test_only_failed_strict_scope_allows_overlay_pass_when_scope_matches(tmp_path: Path) -> None:
    baseline = {"A": _mk_result("A", "failed")}
    overlay = {"A": _mk_result("A", "ok")}
    overlay_meta = {"run_id": "overlay", "scope_hash": "scope_current", "ended_at": "2024-01-01T00:00:00Z"}

    selection, breakdown = _only_failed_selection(
        baseline,
        overlay,
        fail_on="bad",
        require_assert=False,
        artifacts_dir=tmp_path,
        tag="t1",
        scope_hash="scope_current",
        anti_flake_passes=1,
        strict_scope_history=True,
        overlay_run_meta=overlay_meta,
        overlay_run_path=tmp_path,
        explain_selection=True,
    )

    assert selection == set()
    assert breakdown["healed"] == {"A"}


def test_only_failed_explain_notes_scope_mismatch(tmp_path: Path) -> None:
    baseline = {"A": _mk_result("A", "failed")}
    overlay = {"A": _mk_result("A", "ok")}
    overlay_meta = {"run_id": "overlay", "scope_hash": "scope_other", "ended_at": "2024-01-01T00:00:00Z"}

    _, breakdown = _only_failed_selection(
        baseline,
        overlay,
        fail_on="bad",
        require_assert=False,
        artifacts_dir=tmp_path,
        tag="t1",
        scope_hash="scope_current",
        anti_flake_passes=1,
        strict_scope_history=True,
        overlay_run_meta=overlay_meta,
        overlay_run_path=tmp_path,
        explain_selection=True,
    )

    explain_lines = cast(list[str], breakdown.get("explain", []) or [])
    assert any("ignored due to strict scope mismatch" in line for line in explain_lines)


def test_anti_flake_requires_two_passes_without_double_count(tmp_path: Path) -> None:
    artifacts_dir = tmp_path
    case_id = "x1"
    history_dir = artifacts_dir / "runs" / "cases"
    history_dir.mkdir(parents=True)
    history_file = history_dir / f"{case_id}.jsonl"
    now = "2024-01-02T00:00:00Z"
    history_entries = [
        {"status": "ok", "scope_hash": "s", "run_id": "r1", "ts": now, "timestamp": now},
    ]
    history_file.write_text("\n".join(json.dumps(e, ensure_ascii=False) for e in history_entries), encoding="utf-8")
    overlay_entry = {"status": "ok", "scope_hash": "s", "run_id": "r1", "ts": now, "timestamp": now}
    healed, _ = _consecutive_passes(
        case_id,
        overlay_entry,
        artifacts_dir / "runs" / "cases" / f"{case_id}.jsonl",
        scope_hash="s",
        passes_required=2,
        fail_on="bad",
        require_assert=False,
        strict_scope_history=True,
    )
    assert healed is False


def test_anti_flake_order_independent(tmp_path: Path) -> None:
    artifacts_dir = tmp_path
    case_id = "case-1"
    history_dir = artifacts_dir / "runs" / "cases"
    history_dir.mkdir(parents=True)
    history_file = history_dir / f"{case_id}.jsonl"
    entries = [
        {"status": "ok", "scope_hash": "s", "run_id": "r2", "ts": "2024-01-03T00:00:00Z"},
        {"status": "failed", "scope_hash": "s", "run_id": "r1", "ts": "2024-01-01T00:00:00Z"},
        {"status": "ok", "scope_hash": "s", "run_id": "r3", "ts": "2024-01-02T00:00:00Z"},
    ]
    history_file.write_text("\n".join(json.dumps(e, ensure_ascii=False) for e in entries), encoding="utf-8")
    overlay_entry = {"status": "ok", "scope_hash": "s", "run_id": "r4", "ts": "2024-01-04T00:00:00Z"}

    healed, used_entries = _consecutive_passes(
        case_id,
        overlay_entry,
        history_file,
        scope_hash="s",
        passes_required=2,
        fail_on="bad",
        require_assert=False,
        strict_scope_history=True,
    )
    assert healed is True
    assert used_entries[0]["run_id"] == "r4"


def test_anti_flake_respects_legacy_scope_when_not_strict(tmp_path: Path) -> None:
    artifacts_dir = tmp_path
    case_id = "case-legacy"
    history_dir = artifacts_dir / "runs" / "cases"
    history_dir.mkdir(parents=True)
    history_file = history_dir / f"{case_id}.jsonl"
    history_file.write_text(
        "\n".join(
            json.dumps(e, ensure_ascii=False)
            for e in [
                {"status": "ok", "scope_hash": None, "run_id": "r1", "ts": "2024-01-01T00:00:00Z"},
            ]
        ),
        encoding="utf-8",
    )
    overlay_entry = {"status": "ok", "scope_hash": "s", "run_id": "r2", "ts": "2024-01-02T00:00:00Z"}

    healed_strict, _ = _consecutive_passes(
        case_id,
        overlay_entry,
        history_file,
        scope_hash="s",
        passes_required=2,
        fail_on="bad",
        require_assert=False,
        strict_scope_history=True,
    )
    healed_migrating, _ = _consecutive_passes(
        case_id,
        overlay_entry,
        history_file,
        scope_hash="s",
        passes_required=2,
        fail_on="bad",
        require_assert=False,
        strict_scope_history=False,
    )
    assert healed_strict is False
    assert healed_migrating is True


def test_only_missed_uses_planned_pool_from_baseline_meta(tmp_path: Path) -> None:
    artifacts_dir = tmp_path
    run_dir = artifacts_dir / "runs" / "r1"
    run_dir.mkdir(parents=True)
    results_path = run_dir / "results.jsonl"
    results_path.write_text("", encoding="utf-8")
    meta = {"planned_case_ids": ["a", "b"], "selected_case_ids": ["a", "b"], "scope_hash": "s"}
    (run_dir / "run_meta.json").write_text(json.dumps(meta), encoding="utf-8")

    planned_pool = _planned_pool_from_meta(None, results_path, ["x", "y"])
    assert planned_pool == {"a", "b"}


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


def test_format_healed_explain_includes_key_lines() -> None:
    healed = {"a", "b"}
    healed_details = {
        "a": [
            {"run_id": "r2", "ts": "2024-01-02T00:00:00Z", "status": "ok"},
            {"run_id": "r1", "ts": "2024-01-01T00:00:00Z", "status": "ok"},
        ]
    }
    lines = _format_healed_explain(healed, healed_details, anti_flake_passes=2, limit=2)
    assert any("Healed because last 2 results are PASS for case a" in line for line in lines)
    assert any("run_id=r2" in line and "status=ok" in line for line in lines)
