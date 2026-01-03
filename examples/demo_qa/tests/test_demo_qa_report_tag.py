from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from examples.demo_qa.commands.report import handle_report_tag
from examples.demo_qa.runner import RunResult
from examples.demo_qa.runs.io import write_results
from examples.demo_qa.runs.layout import _effective_paths


def _make_result(case_id: str, status: str) -> RunResult:
    return RunResult(
        id=case_id,
        question="q",
        status=status,
        checked=True,
        reason=None,
        details=None,
        artifacts_dir="/tmp",
        duration_ms=0,
        tags=["group"],
    )


def _write_effective(data_dir: Path) -> tuple[Path, Path]:
    eff_results_path, eff_meta_path = _effective_paths(data_dir / ".runs", "demo")
    results = [_make_result("ok", "ok"), _make_result("bad", "failed"), _make_result("err", "error")]
    write_results(eff_results_path, results)
    counts = {
        "total": 3,
        "ok": 1,
        "failed": 1,
        "error": 1,
        "mismatch": 0,
        "skipped": 0,
        "unchecked": 0,
        "summary_by_tag": {"group": {"total": 3, "ok": 1, "failed": 1, "error": 1, "mismatch": 0, "skipped": 0, "unchecked": 0, "pass_rate": 0.333}},
    }
    eff_meta_path.parent.mkdir(parents=True, exist_ok=True)
    eff_meta_path.write_text(
        json.dumps(
            {
                "tag": "demo",
                "planned_total": 4,
                "executed_total": 3,
                "missed_total": 1,
                "counts": counts,
                "fail_on": "bad",
                "require_assert": False,
                "effective_results_path": str(eff_results_path),
            }
        ),
        encoding="utf-8",
    )
    changes_path = eff_results_path.parent / "effective_changes.jsonl"
    changes_path.write_text(
        json.dumps({"timestamp": "t1", "run_id": "r1", "note": "n1", "regressed": [], "fixed": [], "changed_bad": [], "new_cases": ["c1"]})
        + "\n"
        + json.dumps({"timestamp": "t2", "run_id": "r2", "note": "n2", "regressed": [{"id": "x"}], "fixed": [{"id": "y"}], "changed_bad": [], "new_cases": []})
        + "\n",
        encoding="utf-8",
    )
    return eff_results_path, changes_path


def test_report_tag_short_output(tmp_path, capsys):
    _write_effective(tmp_path)
    args = SimpleNamespace(data=tmp_path, tag="demo", verbose=False, changes=1)

    exit_code = handle_report_tag(args)
    out = capsys.readouterr().out

    assert exit_code == 0
    assert "Coverage" in out
    assert "planned" in out
    assert "Quality" in out
    assert "Top bad groups (by tag):" in out
    assert "effective_results_path" in out
    assert "Last 1 effective change" in out


def test_report_tag_changes_limit(tmp_path, capsys):
    _write_effective(tmp_path)
    args = SimpleNamespace(data=tmp_path, tag="demo", verbose=False, changes=2)

    exit_code = handle_report_tag(args)
    out = capsys.readouterr().out

    assert exit_code == 0
    assert "r1" in out
    assert "r2" in out


def test_report_tag_plain_output(tmp_path, capsys):
    _write_effective(tmp_path)
    args = SimpleNamespace(data=tmp_path, tag="demo", verbose=False, changes=1, format="plain", color="never")

    exit_code = handle_report_tag(args)
    out = capsys.readouterr().out

    assert exit_code == 0
    assert "Coverage: planned=4 executed=3 missed=1 (75.0% executed)" in out
    assert "Quality: ok=1 mismatch=0 failed=1 error=1 unchecked=0 bad=2 pass_rate=33.3%" in out


def test_report_tag_color_never(tmp_path, capsys):
    _write_effective(tmp_path)
    args = SimpleNamespace(data=tmp_path, tag="demo", verbose=False, changes=1, color="never")

    exit_code = handle_report_tag(args)
    out = capsys.readouterr().out

    assert exit_code == 0
    assert "\x1b[" not in out
