from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

from ..runner import bad_statuses, load_results, summarize
from ..runs.effective import _load_effective_diff_history
from ..runs.layout import _effective_paths


ANSI = {
    "reset": "\x1b[0m",
    "red": "\x1b[31m",
    "green": "\x1b[32m",
    "yellow": "\x1b[33m",
    "gray": "\x1b[90m",
}


def _color(text: str, color: str | None, *, use_color: bool) -> str:
    if not use_color or not color:
        return text
    prefix = ANSI.get(color)
    if not prefix:
        return text
    return f"{prefix}{text}{ANSI['reset']}"


def _should_use_color(mode: str, *, stream) -> bool:
    if mode == "always":
        return True
    if mode == "never":
        return False
    return bool(getattr(stream, "isatty", lambda: False)())


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


def _format_pass_rate(value: float | None) -> str:
    return f"{value*100:.1f}%" if value is not None else "n/a"


def _format_table_rows(
    headers: list[str],
    rows: list[list[str]],
    *,
    align_right: set[str],
    use_color: bool,
    color_map: dict[str, str] | None = None,
    indent: str = "",
    label: str | None = None,
    label_width: int = 0,
) -> list[str]:
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _format_row(row: list[str], *, colorize: bool) -> str:
        cells: list[str] = []
        for idx, cell in enumerate(row):
            header = headers[idx]
            formatted = cell.rjust(widths[idx]) if header in align_right else cell.ljust(widths[idx])
            color = (color_map or {}).get(header) if colorize else None
            if color:
                formatted = _color(formatted, color, use_color=use_color)
            cells.append(formatted)
        return "  ".join(cells)

    lines: list[str] = []
    label_prefix = ""
    if label is not None:
        label_prefix = f"{label:<{label_width}}" if label_width else label
        lines.append(f"{indent}{label_prefix}  {_format_row(headers, colorize=False)}")
        label_prefix = f"{'':<{label_width}}" if label_width else ""
    for row in rows:
        lines.append(f"{indent}{label_prefix}  {_format_row(row, colorize=True)}")
    return lines


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
    use_color = _should_use_color(color_mode, stream=sys.stdout)
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
    executed_pct_value = (executed / planned * 100) if planned else None
    executed_pct = f"{executed_pct_value:.1f}%" if executed_pct_value is not None else "n/a"
    ok = counts.get("ok", 0)
    mismatch = counts.get("mismatch", 0)
    failed = counts.get("failed", 0)
    error = counts.get("error", 0)
    unchecked = counts.get("unchecked", 0)
    skipped = counts.get("skipped", 0)
    non_skipped = (counts.get("total", 0) or 0) - (skipped or 0)
    pass_rate = (ok / non_skipped) if non_skipped else None
    pass_rate_display = _format_pass_rate(pass_rate)
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
        ok_disp = _color(str(ok), _color_for_metric("ok") or "", use_color=use_color)
        mismatch_disp = _color(str(mismatch), _color_for_metric("mismatch") or "", use_color=use_color)
        failed_disp = _color(str(failed), _color_for_metric("failed") or "", use_color=use_color)
        error_disp = _color(str(error), _color_for_metric("error") or "", use_color=use_color)
        unchecked_disp = _color(str(unchecked), _color_for_metric("unchecked") or "", use_color=use_color)
        bad_disp = _color(str(bad_total), _color_for_metric("bad") or "", use_color=use_color)
        print(
            "Quality: "
            f"ok={ok_disp} mismatch={mismatch_disp} failed={failed_disp} error={error_disp} "
            f"unchecked={unchecked_disp} bad={bad_disp} pass_rate={pass_rate_display}"
        )
    elif format_mode == "table":
        label_width = max(len("Coverage"), len("Quality"))
        print(f"Tag: {args.tag}")
        coverage_headers = ["planned", "executed", "missed", "executed%"]
        coverage_rows = [[str(planned), str(executed), str(missed), executed_pct]]
        for line in _format_table_rows(
            coverage_headers,
            coverage_rows,
            align_right=set(coverage_headers),
            use_color=use_color,
            label="Coverage",
            label_width=label_width,
        ):
            print(line)
        quality_headers = ["ok", "mismatch", "failed", "error", "unchecked", "bad", "pass_rate"]
        quality_row = [
            str(ok),
            str(mismatch),
            str(failed),
            str(error),
            str(unchecked),
            str(bad_total),
            pass_rate_display,
        ]
        quality_colors = {name: _color_for_metric(name) for name in quality_headers}
        for line in _format_table_rows(
            quality_headers,
            [quality_row],
            align_right=set(quality_headers),
            use_color=use_color,
            color_map=quality_colors,
            label="Quality",
            label_width=label_width,
        ):
            print(line)
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
            pr_disp = _format_pass_rate(pr if isinstance(pr, (int, float)) else None)
            rows.append([tag, str(bad_bucket), pr_disp])
        for line in _format_table_rows(
            headers, rows, align_right={"bad", "pass_rate"}, use_color=use_color, indent="  ", label=""
        ):
            print(line)
        if args.verbose:
            print("Full summary_by_tag:")
            print(json.dumps(summary_by_tag, ensure_ascii=False, indent=2))
    if failing:
        print("Failing cases (top 10):")
        for res in failing:
            status_text = f"{res.status:<9}"
            color = _color_for_metric(res.status) or ("red" if res.status in bad else None)
            status_disp = _color(status_text, color or "", use_color=use_color)
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
