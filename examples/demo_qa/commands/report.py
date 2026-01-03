from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

from ..runner import bad_statuses, load_results, summarize
from ..runs.effective import _load_effective_diff_history
from ..runs.layout import _effective_paths
from ..term import color, fmt_num, fmt_pct, render_table, should_use_color, truncate


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


def _color_for_metric(name: str) -> str | None:
    if name == "ok":
        return "green"
    if name in {"bad", "error", "failed"}:
        return "red"
    if name in {"mismatch", "unchecked"}:
        return "yellow"
    return None


def handle_report_tag(args) -> int:
    format_mode = getattr(args, "format", "table")
    color_mode = getattr(args, "color", "auto")
    use_color = should_use_color(color_mode, stream=sys.stdout)
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
    planned = meta.get("planned_total")
    executed = meta.get("executed_total")
    missed = meta.get("missed_total")
    executed_ratio = (executed / planned) if planned else None
    executed_pct = fmt_pct(executed_ratio)
    ok = counts.get("ok", 0)
    mismatch = counts.get("mismatch", 0)
    failed = counts.get("failed", 0)
    error = counts.get("error", 0)
    unchecked = counts.get("unchecked", 0)
    skipped = counts.get("skipped", 0)
    non_skipped = (counts.get("total", 0) or 0) - (skipped or 0)
    pass_rate = (ok / non_skipped) if non_skipped else None
    pass_rate_display = fmt_pct(pass_rate)
    bad = bad_statuses(str(fail_on), require_assert)
    bad_total = sum(counts.get(status, 0) or 0 for status in bad)
    summary_by_tag = meta.get("counts", {}).get("summary_by_tag") or counts.get("summary_by_tag") or {}
    bad = bad_statuses(str(fail_on), require_assert)
    failing = [res for res in results.values() if res.status in bad]
    failing = sorted(failing, key=lambda r: r.id)[:10]
    tag_dir = eff_results_path.parent
    diff_history_path = tag_dir / "effective_changes.jsonl"

    history_limit = max(0, int(args.changes)) if hasattr(args, "changes") else 1
    history_entries = _load_effective_diff_history(tag_dir, limit=history_limit) if history_limit > 0 else []

    if format_mode == "plain":
        print(f"Tag: {args.tag}")
        print(f"Coverage: planned={planned} executed={executed} missed={missed} ({executed_pct} executed)")
        ok_disp = color(fmt_num(ok), _color_for_metric("ok"), use_color=use_color)
        mismatch_disp = color(fmt_num(mismatch), _color_for_metric("mismatch"), use_color=use_color)
        failed_disp = color(fmt_num(failed), _color_for_metric("failed"), use_color=use_color)
        error_disp = color(fmt_num(error), _color_for_metric("error"), use_color=use_color)
        unchecked_disp = color(fmt_num(unchecked), _color_for_metric("unchecked"), use_color=use_color)
        bad_disp = color(fmt_num(bad_total), _color_for_metric("bad"), use_color=use_color)
        print(
            "Quality: "
            f"ok={ok_disp} mismatch={mismatch_disp} failed={failed_disp} error={error_disp} "
            f"unchecked={unchecked_disp} bad={bad_disp} pass_rate={pass_rate_display}"
        )
    elif format_mode == "table":
        print(f"Tag: {args.tag}")
        coverage_headers = ["planned", "executed", "missed", "executed%"]
        coverage_rows = [[fmt_num(planned), fmt_num(executed), fmt_num(missed), executed_pct]]
        print("Coverage:")
        print(
            render_table(
                coverage_headers,
                coverage_rows,
                align_right={0, 1, 2, 3},
                indent="  ",
            )
        )
        quality_headers = ["ok", "mismatch", "failed", "error", "unchecked", "bad", "pass_rate"]
        pass_rate_color = None
        if pass_rate is not None:
            if pass_rate >= 0.9:
                pass_rate_color = "green"
            elif pass_rate >= 0.6:
                pass_rate_color = "yellow"
            else:
                pass_rate_color = "red"
        quality_row = [
            color(fmt_num(ok), _color_for_metric("ok"), use_color=use_color),
            color(fmt_num(mismatch), _color_for_metric("mismatch"), use_color=use_color),
            color(fmt_num(failed), _color_for_metric("failed"), use_color=use_color),
            color(fmt_num(error), _color_for_metric("error"), use_color=use_color),
            color(fmt_num(unchecked), _color_for_metric("unchecked"), use_color=use_color),
            color(fmt_num(bad_total), _color_for_metric("bad"), use_color=use_color),
            color(pass_rate_display, pass_rate_color, use_color=use_color),
        ]
        print("Quality:")
        print(
            render_table(
                quality_headers,
                [quality_row],
                align_right={0, 1, 2, 3, 4, 5, 6},
                indent="  ",
            )
        )
    else:
        print("Unknown format; use plain or table.", file=sys.stderr)
        return 2

    if summary_by_tag:
        buckets = []
        for tag, bucket in summary_by_tag.items():
            bad_bucket = sum((bucket.get(status, 0) or 0) for status in bad)
            pr = bucket.get("pass_rate")
            buckets.append((tag, bad_bucket, pr))
        buckets.sort(key=lambda t: (-t[1], (t[2] if t[2] is not None else 1.1)))
        print("Top bad groups (by tag):")
        headers = ["tag", "bad", "pass_rate"]
        rows = []
        for tag, bad_bucket, pr in buckets[:5]:
            pr_disp = fmt_pct(pr if isinstance(pr, (int, float)) else None)
            rows.append([tag, fmt_num(bad_bucket), pr_disp])
        print(
            render_table(
                headers,
                rows,
                align_right={1, 2},
                indent="  ",
            )
        )
        if args.verbose:
            print("Full summary_by_tag:")
            print(json.dumps(summary_by_tag, ensure_ascii=False, indent=2))
    if failing:
        print("Failing cases (top 10):")
        for res in failing:
            status_text = f"{res.status:<9}"
            color_name = _color_for_metric(res.status) or ("red" if res.status in bad else None)
            status_disp = color(status_text, color_name, use_color=use_color)
            print(f"- {res.id:<20} {status_disp} {_reason_text(res)} [{res.artifacts_dir}]")
    print("Paths:")
    path_labels = ["effective_results_path", "effective_meta_path", "effective_diff_history_path"]
    label_width = max(len(name) for name in path_labels)
    print(f"- {'effective_results_path'.ljust(label_width)}: {eff_results_path}")
    print(f"- {'effective_meta_path'.ljust(label_width)}: {eff_meta_path}")
    print(f"- {'effective_diff_history_path'.ljust(label_width)}: {diff_history_path}")

    if history_limit > 0:
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
