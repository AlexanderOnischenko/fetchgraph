from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

from ..runner import bad_statuses, load_results, summarize
from ..runs.effective import _load_effective_diff, _load_effective_diff_history
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
    planned = meta.get("planned_total")
    executed = meta.get("executed_total")
    missed = meta.get("missed_total")
    executed_pct = f"{(executed / planned * 100):.1f}%" if planned else "n/a"
    print(f"Coverage: planned={planned} executed={executed} missed={missed} ({executed_pct} executed)")
    ok = counts.get("ok", 0)
    mismatch = counts.get("mismatch", 0)
    failed = counts.get("failed", 0)
    error = counts.get("error", 0)
    unchecked = counts.get("unchecked", 0)
    skipped = counts.get("skipped", 0)
    non_skipped = (counts.get("total", 0) or 0) - (skipped or 0)
    pass_rate = (ok / non_skipped) if non_skipped else None
    pass_rate_display = f"{pass_rate*100:.1f}%" if pass_rate is not None else "n/a"
    bad = bad_statuses(str(fail_on), require_assert)
    bad_total = sum(counts.get(status, 0) or 0 for status in bad)
    print(f"Quality: ok={ok} mismatch={mismatch} failed={failed} error={error} unchecked={unchecked} bad={bad_total} pass_rate={pass_rate_display}")
    if args.verbose:
        print("Counts:", counts)
    summary_by_tag = meta.get("counts", {}).get("summary_by_tag") or counts.get("summary_by_tag") or {}
    if summary_by_tag:
        buckets = []
        for tag, bucket in summary_by_tag.items():
            bad_bucket = sum((bucket.get(status, 0) or 0) for status in bad)
            pr = bucket.get("pass_rate")
            buckets.append((tag, bad_bucket, pr))
        buckets.sort(key=lambda t: (-t[1], (t[2] if t[2] is not None else 1.1)))
        print("Top bad groups (by tag):")
        for tag, bad_bucket, pr in buckets[:5]:
            pr_disp = f"{pr*100:.1f}%" if isinstance(pr, (int, float)) else "n/a"
            print(f"- {tag}: bad={bad_bucket} pass_rate={pr_disp}")
        if args.verbose:
            print("Full summary_by_tag:")
            print(json.dumps(summary_by_tag, ensure_ascii=False, indent=2))
    bad = bad_statuses(str(fail_on), require_assert)
    failing = [res for res in results.values() if res.status in bad]
    failing = sorted(failing, key=lambda r: r.id)[:10]
    if failing:
        print("Failing cases (top 10):")
        for res in failing:
            print(f"- {res.id}: {res.status} ({_reason_text(res)}) [{res.artifacts_dir}]")
    tag_dir = eff_results_path.parent
    print("Paths:")
    print(f"- effective_results_path: {eff_results_path}")
    print(f"- effective_meta_path:    {eff_meta_path}")
    diff_history_path = tag_dir / "effective_changes.jsonl"
    print(f"- effective_diff_history_path: {diff_history_path}")

    history_limit = max(0, int(args.changes)) if hasattr(args, "changes") else 1
    if history_limit > 0:
        history_entries = _load_effective_diff_history(tag_dir, limit=history_limit)
        if history_entries:
            print(f"Last {len(history_entries)} effective change(s):")
            for entry in history_entries:
                for key in ["timestamp", "run_id", "note"]:
                    if key in entry:
                        print(f"  {key}: {entry.get(key)}")
                for label in ["regressed", "fixed", "changed_bad", "new_cases"]:
                    items = entry.get(label) or []
                    print(f"  {label}: {len(items)}")
                print("")
        else:
            print("No effective change history found.")
    return 0


__all__ = ["handle_report_run", "handle_report_tag", "_resolve_run_dir_arg"]
