from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

from ..runner import bad_statuses, load_results, summarize
from ..runs.effective import _load_effective_diff
from ..runs.layout import _effective_paths


def _resolve_run_dir_arg(run_arg: Path, artifacts_dir: Path) -> Optional[Path]:
    if run_arg.exists():
        return run_arg
    candidate = artifacts_dir / "runs" / run_arg
    if candidate.exists():
        return candidate
    return None


def handle_report_run(args) -> int:
    artifacts_dir = args.data / ".runs"
    run_dir = _resolve_run_dir_arg(args.run, artifacts_dir)
    if not run_dir:
        print("Run directory not found.", file=sys.stderr)
        return 2
    summary_path = run_dir / "summary.json"
    if not summary_path.exists():
        print(f"summary.json not found in {run_dir}", file=sys.stderr)
        return 2
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Failed to read summary: {exc}", file=sys.stderr)
        return 2
    print(f"Run: {run_dir}")
    for key in ["run_id", "tag", "note", "exit_code", "interrupted", "interrupted_at_case_id", "results_path"]:
        if key in summary:
            print(f"{key}: {summary.get(key)}")
    counts = summary.get("counts") or {}
    if counts:
        print("Counts:", counts)
    return 0


def _reason_text(res) -> str:
    if getattr(res, "reason", None):
        return res.reason
    if getattr(res, "error", None):
        return res.error
    expected = getattr(res, "expected_check", None)
    if expected and getattr(expected, "detail", None):
        return expected.detail
    return ""


def handle_report_tag(args) -> int:
    artifacts_dir = args.data / ".runs"
    eff_results_path, eff_meta_path = _effective_paths(artifacts_dir, args.tag)
    if not eff_results_path.exists() or not eff_meta_path.exists():
        print(f"No effective snapshot found for tag {args.tag!r}.", file=sys.stderr)
        return 2
    try:
        meta = json.loads(eff_meta_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Failed to read effective_meta.json: {exc}", file=sys.stderr)
        return 2
    try:
        results = load_results(eff_results_path)
    except Exception as exc:
        print(f"Failed to read effective results: {exc}", file=sys.stderr)
        return 2
    counts = meta.get("counts") or summarize(results.values())
    fail_on = meta.get("fail_on", "bad")
    require_assert = bool(meta.get("require_assert", False))
    print(f"Tag: {args.tag}")
    print(f"Planned: {meta.get('planned_total')} | Executed: {meta.get('executed_total')} | Missed: {meta.get('missed_total')}")
    print("Counts:", counts)
    bad = bad_statuses(str(fail_on), require_assert)
    failing = [res for res in results.values() if res.status in bad]
    failing = sorted(failing, key=lambda r: r.id)[:10]
    if failing:
        print("Failing cases (top 10):")
        for res in failing:
            print(f"- {res.id}: {res.status} ({_reason_text(res)}) [{res.artifacts_dir}]")
    diff_entry = _load_effective_diff(eff_results_path.parent)
    if diff_entry:
        print("Last effective change:")
        for key in ["timestamp", "run_id", "note"]:
            if key in diff_entry:
                print(f"  {key}: {diff_entry.get(key)}")
        for label in ["regressed", "fixed", "changed_bad", "new_cases"]:
            items = diff_entry.get(label) or []
            print(f"  {label}: {len(items)}")
    return 0


__all__ = ["handle_report_run", "handle_report_tag", "_resolve_run_dir_arg"]
