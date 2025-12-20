from __future__ import annotations

import itertools
import json
import os
import time
from pathlib import Path

import pytest

from examples.demo_qa.batch import _fingerprint_dir, bad_statuses, is_failure, render_markdown, write_results
from examples.demo_qa.runner import RunResult, diff_runs


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
    compare = {
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
    }
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
