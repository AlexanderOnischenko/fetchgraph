from __future__ import annotations

import datetime
import hashlib
import json
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Iterable, Mapping, Optional, cast

from .llm.factory import build_llm
from .logging_config import configure_logging
from .provider_factory import build_provider
from .runner import (
    Case,
    DiffCaseChange,
    DiffReport,
    EventLogger,
    RunResult,
    RunTimings,
    bad_statuses,
    build_agent,
    diff_runs,
    format_status_line,
    is_failure,
    load_cases,
    load_results,
    run_one,
    save_status,
    summarize,
)
from .runs.case_history import _append_case_history, _iter_case_entries_newest_first
from .runs.coverage import _missed_case_ids
from .runs.effective import (
    _append_effective_diff,
    _build_effective_diff,
    _load_effective_results,
    _update_effective_snapshot,
)
from .runs.io import write_results
from .runs.layout import (
    _effective_paths,
    _load_latest_any_results,
    _load_latest_results,
    _load_latest_run,
    _load_run_meta,
    _run_dir_from_results_path,
    _update_latest_markers,
)
from .runs.scope import _scope_hash, _scope_payload
from .settings import load_settings
from .utils import dump_json


def write_summary(out_path: Path, summary: dict) -> Path:
    summary_path = out_path.with_name("summary.json")
    dump_json(summary_path, summary)
    return summary_path


def _coerce_number(value: object | None) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _coerce_int(value: object | None) -> int:
    number = _coerce_number(value)
    if number is None:
        return 0
    return int(number)


def _isoformat_utc(dt: datetime.datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _pass_rate(counts: Mapping[str, object]) -> Optional[float]:
    total = _coerce_int(counts.get("total"))
    skipped = _coerce_int(counts.get("skipped"))
    denom = total - skipped
    if denom <= 0:
        return None
    ok = _coerce_number(counts.get("ok"))
    return None if ok is None else ok / denom


def _hash_file(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha256(data).hexdigest()


def _split_csv(value: Optional[str]) -> set[str] | None:
    if not value:
        return None
    return {item.strip() for item in value.split(",") if item.strip()}


def _load_ids(path: Optional[Path]) -> set[str] | None:
    if path is None:
        return None
    ids = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                ids.add(line)
    return ids


def _consecutive_passes(
    case_id: str,
    overlay_entry: Mapping[str, object] | None,
    history_path: Path | None,
    *,
    tag: str | None = None,
    scope_hash: str = "",
    passes_required: int = 1,
    fail_on: str = "bad",
    require_assert: bool = False,
    strict_scope_history: bool = False,
    max_entries: int | None = None,
) -> tuple[bool, list[dict]]:
    bad = bad_statuses(fail_on, require_assert)
    if overlay_entry is None:
        return False, []
    if passes_required <= 1:
        return (overlay_entry.get("status") not in bad, [dict(overlay_entry)])
    if history_path is None:
        return False, [dict(overlay_entry)]
    entries: list[dict] = []
    passes_needed = max(passes_required, 1)
    iterator = _iter_case_entries_newest_first(
        history_path,
        case_id,
        tag,
        scope_hash or None,
        strict_scope=strict_scope_history,
        fail_on=fail_on,
        require_assert=require_assert,
        overlay_entry=dict(overlay_entry) if overlay_entry else None,
        max_entries=max_entries or (passes_needed + 5),
    )
    consecutive = 0
    for entry in iterator:
        entries.append(entry)
        status = str(entry.get("status", ""))
        if status in bad:
            return False, entries
        consecutive += 1
        if consecutive >= passes_needed:
            return True, entries
    return False, entries


def _only_failed_selection(
    baseline_results: Mapping[str, RunResult] | None,
    overlay_results: Mapping[str, RunResult] | None,
    *,
    fail_on: str = "bad",
    require_assert: bool = False,
    artifacts_dir: Path | None = None,
    tag: str | None = None,
    scope_hash: str = "",
    anti_flake_passes: int = 1,
    strict_scope_history: bool = False,
    overlay_run_meta: Mapping[str, object] | None = None,
    overlay_run_path: Path | None = None,
    explain_selection: bool = False,
    explain_limit: int = 20,
) -> tuple[set[str], dict[str, object]]:
    baseline = baseline_results or {}
    overlay = overlay_results or {}
    bad = bad_statuses(fail_on, require_assert)
    baseline_bad = {cid for cid, res in baseline.items() if res.status in bad}
    overlay_bad = {cid for cid, res in overlay.items() if res.status in bad}
    overlay_run_id = cast(Optional[str], overlay_run_meta.get("run_id") if isinstance(overlay_run_meta, Mapping) else None)
    overlay_scope_hash = cast(Optional[str], overlay_run_meta.get("scope_hash") if isinstance(overlay_run_meta, Mapping) else None)
    overlay_tag = cast(Optional[str], overlay_run_meta.get("tag") if isinstance(overlay_run_meta, Mapping) else None)
    overlay_ts: Optional[object] = None
    if isinstance(overlay_run_meta, Mapping):
        overlay_ts = (
            overlay_run_meta.get("ended_at")
            or overlay_run_meta.get("started_at")
            or overlay_run_meta.get("ts")
            or overlay_run_meta.get("timestamp")
        )
    if overlay_ts is None and overlay_run_path and overlay_run_path.exists():
        try:
            overlay_ts = overlay_run_path.stat().st_mtime
        except OSError:
            overlay_ts = None

    current_scope_hash = scope_hash or None
    overlay_scope_matches_current = True
    if strict_scope_history and current_scope_hash:
        overlay_scope_matches_current = overlay_scope_hash == current_scope_hash

    explain_lines: list[str] = []
    if explain_selection:
        explain_lines.append(
            f"current_scope_hash={current_scope_hash} overlay_scope_hash={overlay_scope_hash} "
            f"overlay_scope_matches_current={overlay_scope_matches_current}"
        )
        if overlay_tag is None and tag is not None:
            explain_lines.append(f"Overlay tag missing; using current tag={tag} for overlay entries")
        elif overlay_tag is not None and tag is not None and overlay_tag != tag:
            explain_lines.append(
                f"Overlay tag differs from current selection; using overlay tag={overlay_tag} (current tag={tag})"
            )
    overlay_entries: dict[str, dict] = {}
    for cid, res in overlay.items():
        entry = {
            "run_id": overlay_run_id or (str(overlay_run_path) if overlay_run_path else "overlay"),
            "ts": overlay_ts,
            "timestamp": overlay_ts,
            "status": res.status,
            "scope_hash": overlay_scope_hash,
            "tag": overlay_tag if overlay_tag is not None else tag,
            "run_dir": str(overlay_run_path) if overlay_run_path else None,
        }
        overlay_entries[cid] = {k: v for k, v in entry.items() if v is not None}

    overlay_good: set[str] = set()
    healed_details: dict[str, list[dict]] = {}
    scope_mismatch_warned = False
    for cid, res in overlay.items():
        overlay_entry_for_history = overlay_entries.get(cid) if overlay_scope_matches_current else None
        ok, history_entries = _consecutive_passes(
            cid,
            overlay_entry_for_history,
            artifacts_dir / "runs" / "cases" / f"{cid}.jsonl" if artifacts_dir else None,
            tag=tag,
            scope_hash=scope_hash,
            passes_required=anti_flake_passes,
            fail_on=fail_on,
            require_assert=require_assert,
            strict_scope_history=strict_scope_history,
        )
        if explain_selection and strict_scope_history and not overlay_scope_matches_current:
            if res.status not in bad:
                explain_lines.append(
                    f"Overlay PASS for case {cid} ignored due to strict scope mismatch "
                    f"(overlay_scope_hash={overlay_scope_hash}, current_scope_hash={current_scope_hash})"
                )
            elif not scope_mismatch_warned:
                explain_lines.append(
                    f"Overlay scope mismatch; overlay failures still counted (overlay_scope_hash={overlay_scope_hash}, "
                    f"current_scope_hash={current_scope_hash})"
                )
                scope_mismatch_warned = True
        if ok:
            overlay_good.add(cid)
            if explain_selection:
                healed_details[cid] = history_entries

    healed = baseline_bad & overlay_good
    selection = (baseline_bad - healed) | overlay_bad
    breakdown = {
        "baseline_failures": baseline_bad,
        "healed": healed,
        "new_failures": overlay_bad,
    }
    if explain_selection and healed_details:
        limit = max(1, explain_limit)
        breakdown["healed_details"] = {cid: healed_details[cid] for cid in list(sorted(healed_details))[:limit]}
    if explain_selection and explain_lines:
        breakdown["explain"] = explain_lines
    return selection, breakdown


def _format_healed_explain(
    healed: Iterable[str],
    healed_details: Mapping[str, list[dict]] | None,
    *,
    anti_flake_passes: int,
    limit: int,
) -> list[str]:
    details = healed_details or {}
    max_entries = max(1, limit)
    lines: list[str] = []
    healed_list = sorted(set(healed))
    for cid in healed_list[:max_entries]:
        entries = details.get(cid, [])
        lines.append(f"Healed because last {anti_flake_passes} results are PASS for case {cid}")
        for entry in entries[:anti_flake_passes]:
            rid = entry.get("run_id")
            ts = entry.get("ts") or entry.get("timestamp")
            status = entry.get("status")
            lines.append(f"  - run_id={rid} ts={ts} status={status}")
    if len(healed_list) > max_entries:
        lines.append(f"... {len(healed_list) - max_entries} more healed cases not shown (limit={max_entries})")
    return lines


def _only_missed_selection(
    selected_case_ids: Iterable[str],
    baseline_results: Mapping[str, RunResult] | None,
    overlay_results: Mapping[str, RunResult] | None,
    *,
    overlay_scope_hash: str | None = None,
    selection_scope_hash: str | None = None,
    overlay_tag: str | None = None,
    selection_tag: str | None = None,
    overlay_disabled_reason: str | None = None,
    overlay_ignored_reason: str | None = None,
) -> tuple[set[str], dict[str, object]]:
    selected = set(selected_case_ids)
    baseline_ids = set(baseline_results.keys()) if baseline_results else set()
    overlay_scope_matches_current: bool | None = None
    overlay_tag_matches_current: bool | None = None
    overlay_results_for_calc: Mapping[str, RunResult] | None = None
    ignored_reason = overlay_ignored_reason or overlay_disabled_reason
    if overlay_results is not None and overlay_disabled_reason is None:
        overlay_scope_matches_current = (
            overlay_scope_hash == selection_scope_hash if overlay_scope_hash is not None and selection_scope_hash is not None else None
        )
        overlay_tag_matches_current = (
            overlay_tag == selection_tag if overlay_tag is not None and selection_tag is not None else None
        )
        overlay_results_for_calc = overlay_results
        if overlay_scope_matches_current is False:
            overlay_results_for_calc = None
            ignored_reason = "scope_mismatch"
        elif overlay_tag_matches_current is False:
            overlay_results_for_calc = None
            ignored_reason = "tag_mismatch"
    overlay_executed = set(overlay_results_for_calc.keys()) if overlay_results_for_calc else set()
    missed_base = selected - baseline_ids
    missed_final = missed_base - overlay_executed
    breakdown: dict[str, object] = {
        "missed_base": missed_base,
        "overlay_executed": overlay_executed,
        "overlay_scope_hash": overlay_scope_hash,
        "overlay_scope_matches_current": overlay_scope_matches_current,
    }
    if overlay_tag is not None:
        breakdown["overlay_tag"] = overlay_tag
    if selection_tag is not None:
        breakdown["overlay_tag_matches_current"] = overlay_tag_matches_current
    if ignored_reason:
        breakdown["overlay_ignored_reason"] = ignored_reason
    return missed_final, breakdown


def _planned_pool_from_meta(
    effective_meta: Mapping[str, object] | None, baseline_results_path: Path | None, suite_case_ids: Iterable[str]
) -> set[str]:
    planned: set[str] | None = None
    if effective_meta:
        planned_from_eff = effective_meta.get("planned_case_ids")
        if isinstance(planned_from_eff, list):
            planned = {str(cid) for cid in planned_from_eff}
    if planned is None and baseline_results_path is not None:
        run_dir = _run_dir_from_results_path(baseline_results_path)
        meta = _load_run_meta(run_dir)
        if isinstance(meta, dict):
            planned_from_meta = meta.get("planned_case_ids") or meta.get("selected_case_ids")
            if isinstance(planned_from_meta, list):
                planned = {str(cid) for cid in planned_from_meta}
    return planned or set(suite_case_ids)


def _fingerprint_dir(data_dir: Path, *, verbose: bool = False) -> Mapping[str, object]:
    entries: list[dict] = []
    total_bytes = 0
    files_count = 0
    digest = hashlib.sha256()
    for path in sorted(data_dir.rglob("*")):
        if path.is_file():
            rel = path.relative_to(data_dir)
            if rel.parts and rel.parts[0] in {".runs", ".cache"}:
                continue
            stat = path.stat()
            record = {
                "path": str(rel),
                "size": stat.st_size,
                "mtime": stat.st_mtime,
            }
            digest.update(json.dumps(record, sort_keys=True).encode("utf-8"))
            files_count += 1
            total_bytes += stat.st_size
            if verbose:
                entries.append(record)
    fingerprint: dict[str, object] = {
        "hash": digest.hexdigest(),
        "files_count": files_count,
        "bytes_total": total_bytes,
    }
    if verbose:
        fingerprint["files"] = entries
    return fingerprint


def _git_sha() -> Optional[str]:
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
    except Exception:
        return None
    return result.stdout.strip() or None




def _find_case_artifact(run_path: Path, case_id: str) -> Optional[Path]:
    cases_dir = run_path / "cases"
    if not cases_dir.exists():
        return None
    matches = sorted(cases_dir.glob(f"{case_id}_*"))
    if matches:
        return matches[-1]
    return None


def _resolve_run_path(path: Path | None, artifacts_dir: Path) -> Optional[Path]:
    if path is not None:
        return path
    return _load_latest_run(artifacts_dir, kind="any")


def handle_chat(args) -> int:
    try:
        settings, _ = load_settings(config_path=args.config, data_dir=args.data)
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    log_dir = args.log_dir or args.data / ".runs" / "logs"
    log_file = configure_logging(
        level=args.log_level,
        log_dir=log_dir,
        to_stderr=args.log_stderr,
        jsonl=args.log_jsonl,
        run_id=None,
    )

    llm_settings = settings.llm
    llm_endpoint = llm_settings.base_url or "https://api.openai.com/v1"
    diagnostics = [
        f"LLM endpoint: {llm_endpoint}",
        f"Plan model: {llm_settings.plan_model} (temp={llm_settings.plan_temperature})",
        f"Synth model: {llm_settings.synth_model} (temp={llm_settings.synth_temperature})",
        f"Timeout: {llm_settings.timeout_s if llm_settings.timeout_s is not None else 'default'}, "
        f"Retries: {llm_settings.retries if llm_settings.retries is not None else 'default'}",
    ]
    if args.enable_semantic:
        diagnostics.append(f"Embeddings: CSV semantic backend in {args.data} (*.embeddings.json)")
    else:
        diagnostics.append("Embeddings: disabled (use --enable-semantic to build/search embeddings).")

    llm = build_llm(settings)

    from .chat_repl import start_repl

    start_repl(
        args.data,
        args.schema,
        llm,
        enable_semantic=args.enable_semantic,
        log_file=log_file,
        diagnostics=diagnostics,
    )
    return 0


def compare_runs(base_path: Path, new_path: Path, *, fail_on: str, require_assert: bool) -> DiffReport:
    base = load_results(base_path)
    new = load_results(new_path)
    return diff_runs(base.values(), new.values(), fail_on=fail_on, require_assert=require_assert)


def _id_sort_key(row: Mapping[str, object]) -> str:
    identifier = row.get("id")
    if isinstance(identifier, str):
        return identifier
    if identifier is None:
        return ""
    return str(identifier)


def render_markdown(compare: DiffReport, out_path: Optional[Path]) -> str:
    lines: list[str] = []
    base_counts = compare["base_counts"]
    new_counts = compare["new_counts"]
    fail_on = compare.get("fail_on", "bad")
    require_assert = compare.get("require_assert", False)

    def _bad_total(counts: Mapping[str, object], *, fallback: int) -> int:
        bad_set = bad_statuses(str(fail_on), bool(require_assert))
        total = 0
        for status in bad_set:
            total += _coerce_int(counts.get(status))
        return total or fallback

    base_bad = _bad_total(base_counts, fallback=compare.get("base_bad_total", 0))
    new_bad = _bad_total(new_counts, fallback=compare.get("new_bad_total", 0))
    lines.append("# Batch comparison report")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Base OK: {base_counts.get('ok',0)}, Bad: {base_bad}")
    lines.append(f"- New  OK: {new_counts.get('ok',0)}, Bad: {new_bad}")
    base_med = compare.get("base_median")
    new_med = compare.get("new_median")
    if base_med is not None and new_med is not None:
        lines.append(f"- Median total time: base {base_med:.2f}s → new {new_med:.2f}s (Δ {new_med - base_med:+.2f}s)")
    lines.append("")

    def table(title: str, rows: list[Mapping[str, object]]) -> None:
        lines.append(f"## {title}")
        if not rows:
            lines.append("None")
            lines.append("")
            return
        lines.append("| id | status | reason | artifacts |")
        lines.append("|---|---|---|---|")
        for row in sorted(rows, key=_id_sort_key):
            artifacts_val = row.get("artifacts", {})
            artifacts = artifacts_val if isinstance(artifacts_val, Mapping) else {}
            links = ", ".join(f"[{k}]({v})" for k, v in sorted(artifacts.items()))
            lines.append(
                f"| {row['id']} | {row['from']} → {row['to']} | {row.get('reason','')} | {links or ''} |"
            )
        lines.append("")

    table("New regressions", compare["new_fail"])  # type: ignore[arg-type]
    table("Fixed", compare["fixed"])  # type: ignore[arg-type]
    table("Still failing", compare["still_fail"])  # type: ignore[arg-type]

    content = "\n".join(lines)
    if out_path:
        out_path.write_text(content, encoding="utf-8")
    return content


ANSI = {
    "reset": "\x1b[0m",
    "red": "\x1b[31m",
    "green": "\x1b[32m",
    "yellow": "\x1b[33m",
    "gray": "\x1b[90m",
}


def _color(text: str, color: str, *, use_color: bool) -> str:
    if not use_color:
        return text
    prefix = ANSI.get(color)
    if not prefix:
        return text
    return f"{prefix}{text}{ANSI['reset']}"


def _format_delta(value: int | float | None, *, positive_good: bool, use_color: bool) -> str:
    if value is None:
        return "n/a"
    sign = "+" if value >= 0 else ""
    text = f"{sign}{value}"
    if value == 0:
        return _color(text, "gray", use_color=use_color)
    is_improvement = (value > 0 and positive_good) or (value < 0 and not positive_good)
    return _color(text, "green" if is_improvement else "red", use_color=use_color)


def _top_list(label: str, entries: list[DiffCaseChange], *, use_color: bool) -> list[str]:
    lines = [label]
    if not entries:
        lines.append("  none")
        return lines
    for entry in entries[:10]:
        status = f"{entry.get('from')} -> {entry.get('to')}"
        reason = entry.get("reason") or ""
        lines.append(f"  - {entry.get('id')}: {status} ({reason})")
    return lines


def render_table(compare: DiffReport, *, use_color: bool) -> str:
    base_counts = compare["base_counts"]
    new_counts = compare["new_counts"]
    counts_delta = compare.get("counts_delta", {})
    base_total = compare.get("base_total_cases", base_counts.get("total", 0))
    new_total = compare.get("new_total_cases", new_counts.get("total", 0))
    delta_bad = compare.get("new_bad_total", 0) - compare.get("base_bad_total", 0)
    base_med = compare.get("base_median")
    new_med = compare.get("new_median")
    med_delta = compare.get("median_delta")
    base_avg = compare.get("base_avg")
    new_avg = compare.get("new_avg")
    avg_delta = compare.get("avg_delta")
    lines: list[str] = []
    lines.append("Summary:")
    lines.append(
        f"  Base: ok={base_counts.get('ok',0)} mismatch={base_counts.get('mismatch',0)} error={base_counts.get('error',0)} failed={base_counts.get('failed',0)} unchecked={base_counts.get('unchecked',0)} total={base_total}"
    )
    lines.append(
        f"  New : ok={new_counts.get('ok',0)} mismatch={new_counts.get('mismatch',0)} error={new_counts.get('error',0)} failed={new_counts.get('failed',0)} unchecked={new_counts.get('unchecked',0)} total={new_total}"
    )
    lines.append(
        "  Δ    : "
        f"ok={_format_delta(counts_delta.get('ok'), positive_good=True, use_color=use_color)} "
        f"bad={_format_delta(delta_bad, positive_good=False, use_color=use_color)} "
        f"error={_format_delta(counts_delta.get('error'), positive_good=False, use_color=use_color)} "
        f"mismatch={_format_delta(counts_delta.get('mismatch'), positive_good=False, use_color=use_color)} "
        f"failed={_format_delta(counts_delta.get('failed'), positive_good=False, use_color=use_color)} "
        f"unchecked={_format_delta(counts_delta.get('unchecked'), positive_good=False, use_color=use_color)} "
        f"total={_format_delta(counts_delta.get('total'), positive_good=True, use_color=use_color)}"
    )
    if base_med is not None or new_med is not None:
        lines.append(
            f"  Median total time: base={base_med if base_med is not None else 'n/a'}s new={new_med if new_med is not None else 'n/a'}s Δ={_format_delta(med_delta, positive_good=False, use_color=use_color)}"
        )
    if base_avg is not None or new_avg is not None:
        lines.append(
            f"  Avg total time:    base={base_avg if base_avg is not None else 'n/a'}s new={new_avg if new_avg is not None else 'n/a'}s Δ={_format_delta(avg_delta, positive_good=False, use_color=use_color)}"
        )
    lines.append("")
    base_only_count = compare.get("base_only_count", 0)
    new_only_count = compare.get("new_only_count", 0)
    lines.append("Coverage:")
    lines.append(f"  base_total_cases={base_total} new_total_cases={new_total}")
    lines.append(
        f"  only_in_base={_color(str(base_only_count), 'yellow', use_color=use_color)} only_in_new={_color(str(new_only_count), 'yellow', use_color=use_color)}"
    )
    lines.append("")
    lines.extend(_top_list("Top regressions:", compare["new_fail"], use_color=use_color))
    lines.append("")
    lines.extend(_top_list("Top fixes:", compare["fixed"], use_color=use_color))
    return "\n".join(lines)


def render_json(compare: DiffReport) -> str:
    summary = {
        "base": compare.get("base_counts"),
        "new": compare.get("new_counts"),
        "deltas": {
            "counts": compare.get("counts_delta"),
            "bad": compare.get("new_bad_total", 0) - compare.get("base_bad_total", 0),
        },
        "coverage": {
            "base_total_cases": compare.get("base_total_cases"),
            "new_total_cases": compare.get("new_total_cases"),
            "only_in_base": compare.get("base_only_count"),
            "only_in_new": compare.get("new_only_count"),
        },
    }
    payload = {
        "summary": summary,
        "top_regressions": compare.get("new_fail", [])[:10],
        "top_fixes": compare.get("fixed", [])[:10],
        "fail_on": compare.get("fail_on"),
        "require_assert": compare.get("require_assert"),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def write_junit(compare: DiffReport, out_path: Path) -> None:
    import xml.etree.ElementTree as ET

    suite = ET.Element("testsuite", name="demo_qa_compare")
    bad: list[DiffCaseChange] = compare["new_fail"] + compare["still_fail"]
    fixed: list[DiffCaseChange] = compare["fixed"]
    all_ids_list: list[str] = list(compare.get("all_ids", []) or [])
    all_ids = sorted(all_ids_list)
    cases_total = len(all_ids)
    suite.set("tests", str(cases_total))
    suite.set("failures", str(len(bad)))
    suite.set("errors", "0")

    for row in sorted(bad, key=_id_sort_key):
        tc = ET.SubElement(suite, "testcase", name=row["id"])
        msg: str = row["reason"] or f"{row.get('from')} → {row.get('to')}"
        failure = ET.SubElement(tc, "failure", message=msg)
        artifacts = row.get("artifacts", {})
        if artifacts:
            failure.text = "\n".join(f"{k}: {v}" for k, v in sorted(artifacts.items()))

    for row in sorted(fixed, key=_id_sort_key):
        ET.SubElement(suite, "testcase", name=row["id"])

    bad_ids = {row["id"] for row in bad}
    fixed_ids = {row["id"] for row in fixed}
    ok_ids = [cid for cid in all_ids if cid not in bad_ids and cid not in fixed_ids]
    for cid in ok_ids:
        ET.SubElement(suite, "testcase", name=cid)

    out_path.write_text(ET.tostring(suite, encoding="unicode"), encoding="utf-8")


def _select_cases_for_rerun(
    cases: list[Case],
    *,
    include_tags: set[str] | None,
    exclude_tags: set[str] | None,
    include_ids: set[str] | None,
    exclude_ids: set[str] | None,
) -> list[Case]:
    filtered: list[Case] = []
    for case in cases:
        tags = set(case.tags)
        if include_tags and not tags.intersection(include_tags):
            continue
        if exclude_tags and tags.intersection(exclude_tags):
            continue
        if include_ids and case.id not in include_ids:
            continue
        if exclude_ids and case.id in exclude_ids:
            continue
        filtered.append(case)
    return filtered


def handle_batch(args) -> int:
    only_failed_effective = bool(getattr(args, "only_failed_effective", False))
    only_missed_effective = bool(getattr(args, "only_missed_effective", False))
    if only_failed_effective and args.only_failed:
        print("Use either --only-failed or --only-failed-effective (not both).", file=sys.stderr)
        return 2
    if only_missed_effective and args.only_missed:
        print("Use either --only-missed or --only-missed-effective (not both).", file=sys.stderr)
        return 2
    if only_failed_effective and args.only_failed_from:
        print("--only-failed-effective is not compatible with --only-failed-from.", file=sys.stderr)
        return 2
    if only_missed_effective and args.only_missed_from:
        print("--only-missed-effective is not compatible with --only-missed-from.", file=sys.stderr)
        return 2
    if (only_failed_effective or only_missed_effective) and not args.tag:
        print("--tag is required when using --only-failed-effective/--only-missed-effective.", file=sys.stderr)
        return 2
    if only_failed_effective:
        args.only_failed = True
    if only_missed_effective:
        args.only_missed = True

    started_at = datetime.datetime.now(datetime.timezone.utc)
    run_id = uuid.uuid4().hex[:8]
    interrupted = False
    interrupted_at_case_id: str | None = None
    data_dir = Path(args.data)
    schema_path = Path(args.schema)
    cases_path = Path(args.cases)
    cli_config_path = Path(args.config) if args.config else None

    try:
        settings, resolved_config_path = load_settings(config_path=cli_config_path, data_dir=data_dir)
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    try:
        cases = load_cases(args.cases)
        cases_hash = _hash_file(args.cases)
    except Exception as exc:
        print(f"Cases error: {exc}", file=sys.stderr)
        return 2

    baseline_for_compare: Optional[Mapping[str, RunResult]] = None
    failed_baseline_results: Optional[Mapping[str, RunResult]] = None
    missed_baseline_results: Optional[Mapping[str, RunResult]] = None
    missed_baseline_path: Path | None = None
    overlay_results: Optional[Mapping[str, RunResult]] = None
    overlay_results_path: Path | None = None
    overlay_run_path: Path | None = None

    artifacts_dir = Path(args.artifacts_dir) if args.artifacts_dir else data_dir / ".runs"

    include_tags = _split_csv(args.include_tags)
    exclude_tags = _split_csv(args.exclude_tags)
    include_ids = _load_ids(args.include_ids)
    exclude_ids = _load_ids(args.exclude_ids)
    scope = _scope_payload(
        cases_hash=cases_hash,
        include_tags=include_tags,
        exclude_tags=exclude_tags,
        include_ids=include_ids,
        exclude_ids=exclude_ids,
    )
    scope_id = _scope_hash(scope)

    baseline_filter_path_arg = cast(Optional[Path], args.only_failed_from)
    baseline_filter_path: Path | None = Path(baseline_filter_path_arg) if baseline_filter_path_arg else None
    only_failed_baseline_kind: str | None = None
    if baseline_filter_path_arg:
        only_failed_baseline_kind = "path"
        print(
            "Using explicit baseline from --only-failed-from; overlay (latest any run) will still be considered unless --no-overlay is set.",
            file=sys.stderr,
        )
    elif args.tag and args.only_failed:
        effective_results, effective_meta, eff_path = _load_effective_results(artifacts_dir, args.tag)
        if not effective_results:
            print(f"No effective results found for tag {args.tag!r}; run a tagged batch first.", file=sys.stderr)
            return 2
        if effective_meta and effective_meta.get("cases_hash") not in (None, cases_hash):
            print(
                f"Effective results cases_hash {effective_meta.get('cases_hash')} does not match current cases file.",
                file=sys.stderr,
            )
            return 2
        if effective_meta and effective_meta.get("scope_hash") not in (None, scope_id):
            print("Effective results scope does not match current selection; refusing to merge.", file=sys.stderr)
            return 2
        failed_baseline_results = effective_results
        baseline_filter_path = eff_path
        only_failed_baseline_kind = "effective"
    elif args.only_failed:
        baseline_filter_path = _load_latest_results(artifacts_dir, args.tag)
        if baseline_filter_path:
            only_failed_baseline_kind = "latest_complete"
    if baseline_filter_path is not None and failed_baseline_results is None:
        try:
            failed_baseline_results = load_results(baseline_filter_path)
        except Exception as exc:
            print(f"Failed to read baseline for --only-failed-from: {exc}", file=sys.stderr)
            return 2
    if args.only_failed and failed_baseline_results is None:
        print("No baseline found for --only-failed.", file=sys.stderr)
        return 2

    compare_to_arg = cast(Optional[Path], args.compare_to)
    compare_path: Path | None = Path(compare_to_arg) if compare_to_arg else None
    if compare_path is None and args.only_failed and baseline_filter_path:
        compare_path = baseline_filter_path
    if compare_path is not None:
        try:
            if baseline_filter_path is not None and compare_path.resolve() == baseline_filter_path.resolve():
                baseline_for_compare = failed_baseline_results
            else:
                baseline_for_compare = load_results(compare_path)
        except Exception as exc:
            print(f"Failed to read baseline for --compare-to: {exc}", file=sys.stderr)
            return 2

    overlay_run_path = None
    overlay_results_path = None
    overlay_disabled = args.no_overlay or only_failed_effective or only_missed_effective
    overlay_disabled_reason = "no_overlay" if args.no_overlay else None
    if overlay_disabled and overlay_disabled_reason is None:
        overlay_disabled_reason = "effective_only"
    overlay_run_meta: Optional[Mapping[str, object]] = None
    if not overlay_disabled:
        overlay_run_path = _load_latest_run(artifacts_dir, args.tag, kind="any")
        overlay_results_path = _load_latest_any_results(artifacts_dir, args.tag)
        if overlay_results_path:
            try:
                overlay_results = load_results(overlay_results_path)
            except Exception as exc:
                print(f"Failed to read overlay results from latest run: {exc}", file=sys.stderr)
                overlay_results_path = None
                overlay_results = None
        if overlay_run_path:
            overlay_run_meta = _load_run_meta(overlay_run_path)

    filtered_cases = _select_cases_for_rerun(
        cases,
        include_tags=include_tags,
        exclude_tags=exclude_tags,
        include_ids=include_ids,
        exclude_ids=exclude_ids,
    )
    suite_case_ids = [case.id for case in filtered_cases]
    cases = filtered_cases
    failed_selection_ids: set[str] | None = None

    if args.only_failed:
        selection_ids, breakdown = _only_failed_selection(
            failed_baseline_results,
            overlay_results if not args.no_overlay else None,
            fail_on=args.fail_on,
            require_assert=args.require_assert,
            artifacts_dir=artifacts_dir,
            tag=args.tag,
            scope_hash=scope_id,
            anti_flake_passes=max(1, int(args.anti_flake_passes)),
            strict_scope_history=args.strict_scope_history,
            overlay_run_meta=overlay_run_meta,
            overlay_run_path=overlay_run_path,
            explain_selection=args.explain_selection,
            explain_limit=args.explain_limit,
        )
        cases = [case for case in cases if case.id in selection_ids]
        failed_selection_ids = selection_ids
        healed = breakdown.get("healed", set())
        baseline_fails = breakdown.get("baseline_failures", set())
        new_failures = breakdown.get("new_failures", set())
        baseline_meta = _load_run_meta(_run_dir_from_results_path(baseline_filter_path))
        baseline_label = baseline_meta.get("run_id") if isinstance(baseline_meta, dict) else None
        baseline_status = baseline_meta.get("run_status") if isinstance(baseline_meta, dict) else None
        overlay_meta = overlay_run_meta if overlay_run_meta is not None else _load_run_meta(overlay_run_path)
        overlay_label = overlay_meta.get("run_id") if isinstance(overlay_meta, dict) else None
        overlay_status = overlay_meta.get("run_status") if isinstance(overlay_meta, dict) else None
        baseline_complete = baseline_meta.get("results_complete") if isinstance(baseline_meta, dict) else None
        overlay_complete = overlay_meta.get("results_complete") if isinstance(overlay_meta, dict) else None
        scope_display = scope_id or "n/a"
        print(
            f"Baseline: run_id={baseline_label or 'n/a'} status={baseline_status or 'n/a'} complete={baseline_complete} scope={scope_display}",
            file=sys.stderr,
        )
        overlay_line = (
            "Overlay: disabled (--no-overlay)"
            if overlay_disabled_reason == "no_overlay"
            else "Overlay: disabled (effective-only selection)"
            if overlay_disabled
            else f"Overlay: run_id={overlay_label or 'n/a'} status={overlay_status or 'n/a'} complete={overlay_complete} scope={scope_display}"
        )
        if overlay_results_path is None and not overlay_disabled and not args.no_overlay:
            overlay_line = "Overlay: none (no latest_any run)"
        print(overlay_line, file=sys.stderr)
        print(f"Baseline failures: {len(baseline_fails)}", file=sys.stderr)
        print(f"Healed by overlay: {len(healed)}", file=sys.stderr)
        print(f"New failures in overlay: {len(new_failures)}", file=sys.stderr)
        print(f"Final only-failed selection: {len(selection_ids)}", file=sys.stderr)
        if args.explain_selection and healed:
            healed_lines = _format_healed_explain(
                healed,
                breakdown.get("healed_details"),
                anti_flake_passes=args.anti_flake_passes,
                limit=args.explain_limit,
            )
            for line in healed_lines:
                print(line, file=sys.stderr)

    only_missed_baseline_kind: str | None = None
    missed_effective_meta: Mapping[str, object] | None = None
    if args.only_missed:
        only_missed_from_arg = cast(Optional[Path], args.only_missed_from)
        if only_missed_from_arg:
            missed_baseline_path = only_missed_from_arg
            only_missed_baseline_kind = "path"
        elif args.tag:
            effective_results, effective_meta, eff_path = _load_effective_results(artifacts_dir, args.tag)
            if not effective_results:
                print(f"No effective results found for tag {args.tag!r}; run a tagged batch first.", file=sys.stderr)
                return 2
            if effective_meta and effective_meta.get("cases_hash") not in (None, cases_hash):
                print(
                    f"Effective results cases_hash {effective_meta.get('cases_hash')} does not match current cases file.",
                    file=sys.stderr,
                )
                return 2
            if effective_meta and effective_meta.get("scope_hash") not in (None, scope_id):
                print("Effective results scope does not match current selection; refusing to merge.", file=sys.stderr)
                return 2
            missed_baseline_path = eff_path
            missed_baseline_results = effective_results
            only_missed_baseline_kind = "effective"
            missed_effective_meta = effective_meta
        else:
            missed_baseline_path = only_missed_from_arg or _load_latest_results(artifacts_dir, args.tag)
            if only_missed_from_arg:
                only_missed_baseline_kind = "path"
            elif missed_baseline_path is not None:
                only_missed_baseline_kind = "latest_complete"
        if missed_baseline_path is not None and missed_baseline_results is None:
            try:
                missed_baseline_results = load_results(missed_baseline_path)
            except Exception as exc:
                print(f"Failed to read baseline for --only-missed: {exc}", file=sys.stderr)
                return 2
        if args.only_missed and missed_baseline_results is None:
            print("No baseline found for --only-missed.", file=sys.stderr)
            return 2
        overlay_scope_hash = cast(Optional[str], overlay_run_meta.get("scope_hash") if isinstance(overlay_run_meta, Mapping) else None)
        overlay_tag = cast(Optional[str], overlay_run_meta.get("tag") if isinstance(overlay_run_meta, Mapping) else None)
        selected_case_ids = _planned_pool_from_meta(missed_effective_meta, missed_baseline_path, suite_case_ids)
        missed_ids, missed_breakdown = _only_missed_selection(
            selected_case_ids,
            missed_baseline_results,
            overlay_results if not args.no_overlay else None,
            overlay_scope_hash=overlay_scope_hash,
            selection_scope_hash=scope_id,
            overlay_tag=overlay_tag,
            selection_tag=args.tag,
            overlay_disabled_reason="no_overlay" if args.no_overlay else None,
        )
        target_ids = missed_ids
        if args.only_failed and failed_selection_ids is not None:
            target_ids = target_ids & failed_selection_ids
            print(
                f"Combining --only-failed and --only-missed via intersection: {len(target_ids)} cases remain (missed={len(missed_ids)}).",
                file=sys.stderr,
            )
        cases = [case for case in filtered_cases if case.id in target_ids]
        print(f"Baseline (missed) results: {missed_baseline_path}", file=sys.stderr)
        print(f"Overlay executed: {len(missed_breakdown.get('overlay_executed', set()))}", file=sys.stderr)
        print(f"Missed in baseline: {len(missed_breakdown.get('missed_base', set()))}", file=sys.stderr)
        print(f"Final only-missed selection: {len(target_ids)}", file=sys.stderr)
        if not cases:
            print("0 missed cases selected.", file=sys.stderr)

    selected_case_ids = [case.id for case in cases]

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_folder = artifacts_dir / "runs" / f"{timestamp}_{cases_path.stem}"
    results_path = Path(args.out) if args.out else (run_folder / "results.jsonl")
    artifacts_root = run_folder / "cases"
    results_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path = results_path.with_name("summary.json")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    history_path = Path(args.history) if args.history else (artifacts_dir / "history.jsonl")

    log_dir = Path(args.log_dir) if args.log_dir else data_dir / ".runs" / "logs"
    configure_logging(
        level=args.log_level,
        log_dir=log_dir,
        to_stderr=args.log_stderr,
        jsonl=args.log_jsonl,
        run_id=None,
    )

    provider, _ = build_provider(data_dir, schema_path, enable_semantic=args.enable_semantic)
    llm = build_llm(settings)
    runner = build_agent(llm, provider)
    events_path = None
    if args.events == "on":
        events_path = args.events_file or (run_folder / "events.jsonl")
    event_logger = EventLogger(events_path, run_id) if events_path else None
    if event_logger:
        event_logger.emit({"type": "run_started", "cases": len(cases), "run_dir": str(run_folder)})

    results: list[RunResult] = []
    failures = 0
    current_case_id: str | None = None
    try:
        for case in cases:
            current_case_id = case.id
            try:
                result = run_one(case, runner, artifacts_root, plan_only=args.plan_only, event_logger=event_logger)
            except KeyboardInterrupt:
                interrupted = True
                interrupted_at_case_id = current_case_id
                run_dir = artifacts_root / f"{case.id}_{uuid.uuid4().hex[:8]}"
                run_dir.mkdir(parents=True, exist_ok=True)
                stub = RunResult(
                    id=case.id,
                    question=case.question,
                    status="error",
                    checked=case.has_asserts,
                    reason="KeyboardInterrupt",
                    details={"error": "KeyboardInterrupt"},
                    artifacts_dir=str(run_dir),
                    duration_ms=0,
                    tags=list(case.tags),
                    answer=None,
                    error="KeyboardInterrupt",
                    plan_path=None,
                    timings=RunTimings(),
                    expected_check=None,
                )
                save_status(stub)
                results.append(stub)
                print("\nInterrupted during case execution; saved partial status.", file=sys.stderr)
                break
            results.append(result)
            if not args.quiet:
                print(format_status_line(result))
            if is_failure(result.status, args.fail_on, args.require_assert):
                failures += 1
                if args.fail_fast or (args.max_fails and failures >= args.max_fails):
                    break
    except KeyboardInterrupt:
        interrupted = True
        interrupted_at_case_id = current_case_id
        print("Interrupted; finalizing partial results...", file=sys.stderr)

    write_results(results_path, results)
    counts = summarize(results)

    diff_block: DiffReport | None = None
    baseline_path: Path | None = None
    if baseline_for_compare:
        baseline_path = compare_path or baseline_filter_path
        diff = diff_runs(
            baseline_for_compare.values(),
            results,
            fail_on=args.fail_on,
            require_assert=args.require_assert,
        )
        if baseline_path:
            diff["baseline_path"] = str(baseline_path)
        diff_block = diff

    policy_bad = bad_statuses(args.fail_on, args.require_assert)
    bad_count = sum(_coerce_int(counts.get(status)) for status in policy_bad)
    exit_code = 130 if interrupted else (1 if bad_count else 0)

    ended_at = datetime.datetime.now(datetime.timezone.utc)
    duration_ms = int((ended_at - started_at).total_seconds() * 1000)
    executed_results = {res.id: res for res in results}
    planned_total = len(selected_case_ids)
    executed_total = len(results)
    missed_total = len(_missed_case_ids(selected_case_ids, executed_results))
    suite_planned_total = len(suite_case_ids)
    suite_missed_total = len(_missed_case_ids(suite_case_ids, executed_results))
    results_complete = (planned_total == executed_total) and not interrupted
    if interrupted:
        run_status = "INTERRUPTED"
    elif not results_complete:
        run_status = "ERROR"
    elif bad_count:
        run_status = "FAILED"
    else:
        run_status = "SUCCESS"
    summary = {
        "run_id": run_id,
        "started_at": _isoformat_utc(started_at),
        "ended_at": _isoformat_utc(ended_at),
        "duration_ms": duration_ms,
        "counts": counts,
        "summary_by_tag": counts.get("summary_by_tag"),
        "exit_code": exit_code,
        "results_path": str(results_path),
        "require_assert": args.require_assert,
        "fail_on": args.fail_on,
        "planned_total": planned_total,
        "executed_total": executed_total,
        "missed_total": missed_total,
        "suite_planned_total": suite_planned_total,
        "suite_missed_total": suite_missed_total,
        "interrupted": interrupted,
        "interrupted_at_case_id": interrupted_at_case_id,
        "tag": args.tag,
        "note": args.note,
        "run_status": run_status,
        "results_complete": results_complete,
        "total_selected": planned_total,
        "total_executed": executed_total,
    }
    if diff_block:
        summary["diff"] = diff_block

    summary_path = write_summary(results_path, summary)
    summary_by_tag = summary.get("summary_by_tag")
    if summary_by_tag:
        summary_by_tag_path = summary_path.with_name("summary_by_tag.json")
        dump_json(summary_by_tag_path, summary_by_tag)

    if event_logger:
        event_logger.emit(
            {
                "type": "run_finished",
                "counts": counts,
                "exit_code": exit_code,
                "duration_ms": duration_ms,
                "run_dir": str(run_folder),
                "results_path": str(results_path),
                "interrupted": interrupted,
                "planned_total": planned_total,
                "executed_total": executed_total,
                "missed_total": missed_total,
                "run_status": run_status,
                "results_complete": results_complete,
            }
        )

    _update_latest_markers(run_folder, results_path, artifacts_dir, args.tag, results_complete=results_complete)
    effective_path = None
    effective_meta_path = None
    if args.tag:
        try:
            effective_path, effective_meta_path, prev_effective, new_effective = _update_effective_snapshot(
                artifacts_dir=artifacts_dir,
                tag=args.tag,
                cases_hash=cases_hash,
                cases_path=cases_path,
                suite_case_ids=suite_case_ids,
                executed_results=results,
                run_folder=run_folder,
                scope=scope,
                scope_hash=scope_id,
                fail_on=args.fail_on,
                require_assert=args.require_assert,
            )
            diff_entry = _build_effective_diff(
                prev_effective,
                new_effective,
                fail_on=args.fail_on,
                require_assert=args.require_assert,
                run_id=run_id,
                tag=args.tag,
                note=args.note,
                run_dir=run_folder,
                results_path=results_path,
                scope_hash=scope_id,
            )
            _append_effective_diff(effective_path.parent, diff_entry)
        except Exception as exc:
            print(f"Failed to update effective results for tag {args.tag!r}: {exc}", file=sys.stderr)

    config_hash = _hash_file(resolved_config_path) if resolved_config_path else None
    schema_hash = _hash_file(schema_path)
    data_fingerprint = _fingerprint_dir(data_dir, verbose=args.fingerprint_verbose)
    git_sha = _git_sha()
    llm_settings = settings.llm
    run_meta = {
        "run_id": run_id,
        "timestamp": _isoformat_utc(started_at),
        "tag": args.tag,
        "note": args.note,
        "inputs": {
            "cases_path": str(cases_path),
            "cases_hash": cases_hash,
            "config_path": str(resolved_config_path) if resolved_config_path else None,
            "config_hash": config_hash,
            "schema_path": str(schema_path),
            "schema_hash": schema_hash,
            "data_dir": str(data_dir),
        },
        "suite_case_ids": suite_case_ids,
        "selected_case_ids": selected_case_ids,
        "planned_total": planned_total,
        "executed_total": executed_total,
        "run_status": run_status,
        "results_complete": results_complete,
        "exit_code": exit_code,
        "total_selected": planned_total,
        "total_executed": executed_total,
        "selected_filters": {
            "include_tags": sorted(include_tags) if include_tags else None,
            "exclude_tags": sorted(exclude_tags) if exclude_tags else None,
            "include_ids_path": str(args.include_ids) if args.include_ids else None,
            "exclude_ids_path": str(args.exclude_ids) if args.exclude_ids else None,
            "only_failed": bool(args.only_failed or args.only_failed_from),
            "only_failed_from": str(baseline_filter_path) if baseline_filter_path else None,
            "only_failed_baseline_kind": only_failed_baseline_kind,
            "only_missed": args.only_missed,
            "only_missed_from": str(missed_baseline_path) if missed_baseline_path else None,
            "only_missed_baseline_kind": only_missed_baseline_kind,
            "overlay_results_path": str(overlay_results_path) if overlay_results_path else None,
            "baseline_tag": args.tag,
            "effective_path": str(effective_path) if effective_path else None,
            "scope_hash": scope_id,
            "scope": scope,
            "plan_only": args.plan_only,
            "fail_fast": args.fail_fast,
            "max_fails": args.max_fails,
            "no_overlay": args.no_overlay,
            "anti_flake_passes": args.anti_flake_passes,
            "strict_scope_history": args.strict_scope_history,
            "explain_selection": args.explain_selection,
            "explain_limit": args.explain_limit,
        },
        "interrupted": interrupted,
        "interrupted_at_case_id": interrupted_at_case_id,
        "data_fingerprint": data_fingerprint,
        "llm": {
            "plan_model": llm_settings.plan_model,
            "synth_model": llm_settings.synth_model,
            "plan_temperature": llm_settings.plan_temperature,
            "synth_temperature": llm_settings.synth_temperature,
            "base_url": llm_settings.base_url or "https://api.openai.com/v1",
        },
        "enable_semantic": args.enable_semantic,
        "git_sha": git_sha,
        "results_path": str(results_path),
        "summary_path": str(summary_path),
        "run_dir": str(run_folder),
    }
    dump_json(run_folder / "run_meta.json", run_meta)

    prate = _pass_rate(counts)
    history_entry = {
        "run_id": run_id,
        "timestamp": _isoformat_utc(started_at),
        "config_hash": config_hash,
        "schema_hash": schema_hash,
        "cases_hash": cases_hash,
        "tag": args.tag,
        "note": args.note,
        "ok": counts.get("ok", 0),
        "mismatch": counts.get("mismatch", 0),
        "error": counts.get("error", 0),
        "skipped": counts.get("skipped", 0),
        "pass_rate": prate,
        "avg_total_s": counts.get("avg_total_s"),
        "median_total_s": counts.get("median_total_s"),
        "run_dir": str(run_folder),
        "results_path": str(results_path),
        "failed": counts.get("failed", 0),
        "unchecked": counts.get("unchecked", 0),
        "plan_only": counts.get("plan_only", 0),
        "fail_on": args.fail_on,
        "require_assert": args.require_assert,
        "fail_count": bad_count,
        "planned_total": planned_total,
        "executed_total": executed_total,
        "total_selected": planned_total,
        "total_executed": executed_total,
        "missed_total": missed_total,
        "suite_planned_total": suite_planned_total,
        "suite_missed_total": suite_missed_total,
        "interrupted": interrupted,
        "interrupted_at_case_id": interrupted_at_case_id,
        "scope_hash": scope_id,
        "run_status": run_status,
        "results_complete": results_complete,
        "exit_code": exit_code,
    }
    for res in results:
        _append_case_history(
            artifacts_dir,
            res,
            run_id=run_id,
            tag=args.tag,
            note=args.note,
            fail_on=args.fail_on,
            require_assert=args.require_assert,
            scope_hash=scope_id,
            cases_hash=cases_hash,
            git_sha=git_sha,
            run_dir=run_folder,
            results_path=results_path,
            run_ts=_isoformat_utc(ended_at),
        )
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(history_entry, ensure_ascii=False, sort_keys=True) + "\n")

    unchecked = counts.get("unchecked", 0)
    plan_only = counts.get("plan_only", 0)
    summary_line = (
        f"Batch: planned {planned_total}, executed {executed_total}, missed {missed_total} | "
        f"Checked: {counts.get('checked_total', 0)} | Checked OK: {counts.get('checked_ok', 0)} | "
        f"Unchecked(no-assert): {unchecked} | Plan-only: {plan_only} | "
        f"FAIL(policy): {bad_count} | Skipped: {counts.get('skipped', 0)}"
    )

    if args.quiet:
        print(summary_line)
        if diff_block:
            print(
                f"Δ vs baseline: +{len(diff_block.get('fixed', []))} fixed, "
                f"-{len(diff_block.get('new_fail', []))} regressions, "
                f"{len(diff_block.get('still_fail', []))} still failing, "
                f"{len(diff_block.get('new_cases', []))} new cases"
            )
        return exit_code

    print(summary_line)
    if diff_block:
        print(
            f"Δ vs baseline: +{len(diff_block.get('fixed', []))} fixed, "
            f"-{len(diff_block.get('new_fail', []))} regressions, "
            f"{len(diff_block.get('still_fail', []))} still failing, "
            f"{len(diff_block.get('new_cases', []))} new cases"
        )

    failures_list: dict[str, RunResult] = {}
    for res in results:
        if is_failure(res.status, args.fail_on, args.require_assert):
            failures_list[res.id] = res
    if failures_list:
        print(f"Failures (top {args.show_failures}):")
        for res in list(failures_list.values())[: args.show_failures]:
            reason = res.reason or res.error or ""
            repro = (
                f"python -m examples.demo_qa.cli case run {res.id} --cases {args.cases} --data {args.data} "
                f"--schema {args.schema}" + (" --plan-only" if args.plan_only else "")
            )
            print(f"- {res.id}: {res.status} ({reason}) [{res.artifacts_dir}]")
            if args.show_artifacts:
                print(f"  artifacts: {res.artifacts_dir}")
            print(f"  repro: {repro}")

    print(f"Results written to: {results_path}")
    print(f"Summary written to: {summary_path}")

    return exit_code


def handle_case_run(args) -> int:
    try:
        settings, _ = load_settings(config_path=args.config, data_dir=args.data)
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    try:
        cases = {c.id: c for c in load_cases(args.cases)}
    except Exception as exc:
        print(f"Cases error: {exc}", file=sys.stderr)
        return 2
    if args.case_id not in cases:
        print(f"Case {args.case_id} not found in {args.cases}", file=sys.stderr)
        return 2

    artifacts_dir = args.artifacts_dir or (args.data / ".runs")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = uuid.uuid4().hex[:8]
    run_folder = artifacts_dir / "runs" / f"{timestamp}_{args.cases.stem}_{run_id}"
    artifacts_root = run_folder / "cases"
    results_path = run_folder / "results.jsonl"

    log_dir = artifacts_dir / "logs"
    configure_logging(level="INFO", log_dir=log_dir, to_stderr=True, jsonl=False, run_id=None)

    provider, _ = build_provider(args.data, args.schema, enable_semantic=args.enable_semantic)
    llm = build_llm(settings)
    runner = build_agent(llm, provider)

    result = run_one(cases[args.case_id], runner, artifacts_root, plan_only=args.plan_only)
    write_results(results_path, [result])
    counts = summarize([result])
    bad = bad_statuses("bad", False)
    bad_count = sum(_coerce_int(counts.get(status)) for status in bad)
    run_status = "FAILED" if bad_count else "SUCCESS"
    exit_code = 1 if bad_count else 0
    summary = {
        "run_id": run_id,
        "timestamp": timestamp + "Z",
        "counts": counts,
        "results_path": str(results_path),
        "fail_on": "bad",
        "require_assert": False,
        "run_status": run_status,
        "results_complete": True,
        "total_selected": 1,
        "total_executed": 1,
        "exit_code": exit_code,
        "run_dir": str(run_folder),
    }
    summary_path = write_summary(results_path, summary)
    _update_latest_markers(run_folder, results_path, artifacts_dir, None, results_complete=True)

    print(format_status_line(result))
    print(f"Artifacts: {result.artifacts_dir}")
    print(f"Summary: {summary_path}")
    return exit_code


def handle_case_open(args) -> int:
    artifacts_dir = args.artifacts_dir or (args.data / ".runs")
    run_path = _resolve_run_path(args.run, artifacts_dir)
    if not run_path:
        print(
            "No run found. Provide --run or ensure latest markers exist (latest_any/latest_complete); run a batch first.",
            file=sys.stderr,
        )
        return 2
    case_dir = _find_case_artifact(run_path, args.case_id)
    if not case_dir:
        print(f"Case {args.case_id} not found under {run_path}", file=sys.stderr)
        return 2
    print(f"Case {args.case_id} artifacts: {case_dir}")
    plan = case_dir / "plan.json"
    answer = case_dir / "answer.txt"
    status = case_dir / "status.json"
    for path in [plan, answer, status]:
        if path.exists():
            print(f"- {path}")
    return 0


def _load_history(history_path: Path) -> list[dict]:
    if not history_path.exists():
        return []
    entries: list[dict] = []
    with history_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def _print_stats(entries: list[dict]) -> None:
    if not entries:
        print("No history entries found.")
        return
    header = (
        f"{'run_id':<10} {'ok':>4} {'mis':>4} {'fail':>4} {'err':>4} {'skip':>5} "
        f"{'pass%':>7} {'median_s':>10} {'Δpass':>8} {'Δmedian':>9} {'policy':>8} {'reqA':>5}"
    )
    print(header)
    prev = None
    for entry in entries:
        pass_rate = _coerce_number(entry.get("pass_rate"))
        median = _coerce_number(entry.get("median_total_s"))
        delta_pass = None
        delta_median = None
        if prev:
            prev_pass_rate = _coerce_number(prev.get("pass_rate"))
            if pass_rate is not None and prev_pass_rate is not None:
                delta_pass = pass_rate - prev_pass_rate
            prev_median = _coerce_number(prev.get("median_total_s"))
            if median is not None and prev_median is not None:
                delta_median = median - prev_median
        pr_display = f"{pass_rate*100:.1f}%" if pass_rate is not None else "n/a"
        median_display = f"{median:.2f}" if median is not None else "n/a"
        dp = f"{delta_pass*100:+.1f}pp" if delta_pass is not None else "n/a"
        dm = f"{delta_median:+.2f}" if delta_median is not None else "n/a"
        print(
            f"{entry.get('run_id',''):<10} "
            f"{entry.get('ok',0):>4} {entry.get('mismatch',0):>4} {entry.get('failed',0):>4} "
            f"{entry.get('error',0):>4} {entry.get('skipped',0):>5} "
            f"{pr_display:>7} {median_display:>10} {dp:>8} {dm:>9} "
            f"{entry.get('fail_on',''):>8} {str(entry.get('require_assert', False)):>5}"
        )
        prev = entry


def handle_stats(args) -> int:
    history_path: Path | None = args.history
    if history_path is None:
        if not args.data:
            print("Provide --data or --history to locate history.jsonl", file=sys.stderr)
            return 2
        history_path = Path(args.data) / ".runs" / "history.jsonl"
    entries = _load_history(history_path)
    if args.group_by == "config_hash":
        grouped: dict[str, list[dict]] = {}
        for e in entries:
            key = e.get("config_hash") or "unknown"
            grouped.setdefault(key, []).append(e)
        for key, vals in grouped.items():
            print(f"\nconfig_hash={key}")
            _print_stats(vals[-args.last :])
    else:
        _print_stats(entries[-args.last :])
    return 0


def _resolve_effective_results_for_tag(
    tag: str, *, candidates: Iterable[Path], preferred: Path | None = None
) -> tuple[Optional[Path], Optional[Path], list[Path]]:
    attempted: list[Path] = []
    ordered: list[Path] = []
    if preferred is not None:
        ordered.append(preferred)
    for candidate in candidates:
        if preferred is not None and candidate == preferred:
            continue
        ordered.append(candidate)
    for artifacts_dir in ordered:
        results_path, meta_path = _effective_paths(artifacts_dir, tag)
        if results_path.exists() and meta_path.exists():
            return results_path, artifacts_dir, attempted
        attempted.append(artifacts_dir)
    return None, None, attempted


def _render_missing_effective_error(tag: str, attempted: list[Path]) -> str:
    message = f"No effective snapshot found for tag {tag!r}. Run a tagged batch to create it."
    if attempted:
        details = []
        for dir_path in attempted:
            res_path, meta_path = _effective_paths(dir_path, tag)
            details.append(f"{dir_path} (effective: {res_path}, {meta_path})")
        message = f"{message} Looked in: {', '.join(details)}."
    return message


def _should_use_color(mode: str, *, stream, force_plain: bool = False) -> bool:
    if force_plain:
        return False
    if mode == "always":
        return True
    if mode == "never":
        return False
    return bool(getattr(stream, "isatty", lambda: False)())


def handle_compare(args) -> int:
    if args.base and args.base_tag:
        print("Use either --base or --base-tag (not both).", file=sys.stderr)
        return 2
    if args.new and args.new_tag:
        print("Use either --new or --new-tag (not both).", file=sys.stderr)
        return 2

    base_path: Path | None = Path(args.base) if args.base else None
    new_path: Path | None = Path(args.new) if args.new else None

    if args.base_tag:
        if not args.data:
            print("--data is required when using --base-tag/--new-tag.", file=sys.stderr)
            return 2
        artifacts_dir = Path(args.data) / ".runs"
        base_path, artifacts_dir, attempted = _resolve_effective_results_for_tag(
            args.base_tag, candidates=[artifacts_dir]
        )
        if base_path is None:
            print(_render_missing_effective_error(args.base_tag, attempted), file=sys.stderr)
            return 2
    if args.new_tag:
        if not args.data:
            print("--data is required when using --base-tag/--new-tag.", file=sys.stderr)
            return 2
        artifacts_dir = Path(args.data) / ".runs"
        new_path, _, attempted = _resolve_effective_results_for_tag(
            args.new_tag, candidates=[artifacts_dir]
        )
        if new_path is None:
            print(_render_missing_effective_error(args.new_tag, attempted), file=sys.stderr)
            return 2

    if base_path is None and not args.base_tag:
        print("Provide either --base or --base-tag.", file=sys.stderr)
        return 2
    if new_path is None and not args.new_tag:
        print("Provide either --new or --new-tag.", file=sys.stderr)
        return 2

    if not base_path.exists() or not new_path.exists():
        print("Base or new results file not found.", file=sys.stderr)
        return 2
    comparison = compare_runs(base_path, new_path, fail_on=args.fail_on, require_assert=args.require_assert)
    out_path = Path(args.out) if args.out is not None else None
    format_mode = getattr(args, "format", "md")
    color_mode = getattr(args, "color", "auto")
    stdout_color = _should_use_color(color_mode, stream=sys.stdout, force_plain=bool(out_path))

    report = ""
    file_report = None
    if format_mode == "md":
        report = render_markdown(comparison, out_path)
    elif format_mode == "table":
        report = render_table(comparison, use_color=stdout_color)
        if out_path:
            file_report = render_table(comparison, use_color=False)
    elif format_mode == "json":
        report = render_json(comparison)
        file_report = report
    else:
        print("Unknown format; use md, table, or json.", file=sys.stderr)
        return 2

    if out_path and file_report is not None:
        out_path.write_text(file_report, encoding="utf-8")
    print(report)
    if args.junit:
        junit_path = Path(args.junit)
        write_junit(comparison, junit_path)
        print(f"JUnit written to {junit_path}")
    return 0


__all__ = [
    "handle_batch",
    "handle_case_open",
    "handle_case_run",
    "handle_chat",
    "handle_stats",
    "handle_compare",
    "bad_statuses",
    "is_failure",
    "write_results",
    "write_summary",
]
