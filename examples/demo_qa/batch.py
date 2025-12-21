from __future__ import annotations

import datetime
import hashlib
import json
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Iterable, Mapping, Optional

from .llm.factory import build_llm
from .logging_config import configure_logging
from .provider_factory import build_provider
from .runner import (
    Case,
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
from .settings import load_settings
from .utils import dump_json


def write_results(out_path: Path, results: Iterable[RunResult]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for res in results:
            f.write(json.dumps(res.to_json(), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")


def write_summary(out_path: Path, summary: dict) -> Path:
    summary_path = out_path.with_name("summary.json")
    dump_json(summary_path, summary)
    return summary_path


def _pass_rate(counts: Mapping[str, object]) -> Optional[float]:
    total = int(counts.get("total", 0) or 0)
    skipped = int(counts.get("skipped", 0) or 0)
    denom = total - skipped
    if denom <= 0:
        return None
    return (counts.get("ok", 0) or 0) / denom


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


def _sanitize_tag(tag: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in tag)
    return cleaned or "tag"


def _effective_paths(artifacts_dir: Path, tag: str) -> tuple[Path, Path]:
    base = artifacts_dir / "runs" / "tags" / _sanitize_tag(tag)
    return base / "effective_results.jsonl", base / "effective_meta.json"


def _latest_markers(artifacts_dir: Path, tag: str | None) -> tuple[Path, Path]:
    runs_dir = artifacts_dir / "runs"
    if tag:
        slug = _sanitize_tag(tag)
        return runs_dir / f"tag-latest-{slug}.txt", runs_dir / f"tag-latest-results-{slug}.txt"
    return runs_dir / "latest.txt", runs_dir / "latest_results.txt"


def _load_latest_run(artifacts_dir: Path, tag: str | None = None) -> Optional[Path]:
    latest_file, _ = _latest_markers(artifacts_dir, tag)
    if latest_file.exists():
        content = latest_file.read_text(encoding="utf-8").strip()
        if content:
            return Path(content)
    return None


def _load_latest_results(artifacts_dir: Path, tag: str | None = None) -> Optional[Path]:
    _, latest_file = _latest_markers(artifacts_dir, tag)
    if latest_file.exists():
        content = latest_file.read_text(encoding="utf-8").strip()
        if content:
            return Path(content)
    latest_run = _load_latest_run(artifacts_dir, tag)
    if latest_run:
        summary_path = latest_run / "summary.json"
        if summary_path.exists():
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                results_path = summary.get("results_path")
                if results_path:
                    return Path(results_path)
            except Exception:
                pass
    return None


def _load_run_meta(run_path: Path | None) -> Optional[dict]:
    if run_path is None:
        return None
    meta_path = run_path / "run_meta.json"
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _run_dir_from_results_path(results_path: Path | None) -> Optional[Path]:
    if results_path is None:
        return None
    run_dir = results_path.parent
    summary_path = run_dir / "summary.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            run_dir_from_summary = summary.get("run_dir")
            if run_dir_from_summary:
                return Path(run_dir_from_summary)
        except Exception:
            pass
    return run_dir


def _missed_case_ids(planned_case_ids: Iterable[str], executed_results: Mapping[str, RunResult] | None) -> set[str]:
    planned_set = set(planned_case_ids)
    if not executed_results:
        return planned_set
    try:
        executed_ids = set(executed_results.keys())
    except Exception:
        executed_ids = set()
    return planned_set - executed_ids


def _update_latest_markers(run_folder: Path, results_path: Path, artifacts_dir: Path, tag: str | None) -> None:
    marker_pairs = {_latest_markers(artifacts_dir, None)}
    if tag:
        marker_pairs.add(_latest_markers(artifacts_dir, tag))
    for latest_path, latest_results_path in marker_pairs:
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(str(run_folder), encoding="utf-8")
        latest_results_path.write_text(str(results_path), encoding="utf-8")


def _load_effective_results(artifacts_dir: Path, tag: str) -> tuple[dict[str, RunResult], Optional[dict], Path]:
    results_path, meta_path = _effective_paths(artifacts_dir, tag)
    meta: Optional[dict] = None
    results: dict[str, RunResult] = {}
    if results_path.exists():
        results = load_results(results_path)
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = None
    return results, meta, results_path


def _write_effective_results(results_path: Path, results: Mapping[str, RunResult]) -> None:
    results_path.parent.mkdir(parents=True, exist_ok=True)
    ordered = [results[cid] for cid in sorted(results)]
    write_results(results_path, ordered)


def _update_effective_snapshot(
    *,
    artifacts_dir: Path,
    tag: str,
    cases_hash: str,
    cases_path: Path,
    planned_case_ids: list[str],
    executed_results: list[RunResult],
    run_folder: Path,
    planned_case_ids_source: list[str] | None,
) -> tuple[Path, Path]:
    effective_results, effective_meta, effective_results_path = _load_effective_results(artifacts_dir, tag)
    if effective_meta and effective_meta.get("cases_hash") and effective_meta["cases_hash"] != cases_hash:
        raise ValueError(
            f"Existing effective results for tag {tag!r} use a different cases_hash; refusing to merge."
        )

    planned_pool: set[str]
    if effective_meta and isinstance(effective_meta.get("planned_case_ids"), list):
        planned_pool = {str(cid) for cid in effective_meta["planned_case_ids"]}
    elif planned_case_ids_source:
        planned_pool = set(planned_case_ids_source)
    else:
        planned_pool = set(planned_case_ids)

    for res in executed_results:
        effective_results[res.id] = res
    _write_effective_results(effective_results_path, effective_results)

    summary_counts = summarize(effective_results.values())
    executed_total = len(effective_results)
    missed_total = len(_missed_case_ids(planned_pool, effective_results))
    meta_path = effective_results_path.with_name("effective_meta.json")
    built_from = set(effective_meta.get("built_from_runs", [])) if effective_meta else set()
    built_from.add(str(run_folder))
    effective_meta_payload = {
        "tag": tag,
        "cases_hash": cases_hash,
        "cases_path": str(cases_path),
        "planned_case_ids": sorted(planned_pool),
        "planned_total": len(planned_pool),
        "executed_total": executed_total,
        "missed_total": missed_total,
        "counts": summary_counts,
        "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "built_from_runs": sorted(built_from),
        "effective_results_path": str(effective_results_path),
    }
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    dump_json(meta_path, effective_meta_payload)
    return effective_results_path, meta_path


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
    return _load_latest_run(artifacts_dir)


def handle_chat(args) -> int:
    try:
        settings = load_settings(config_path=args.config, data_dir=args.data)
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


def compare_runs(base_path: Path, new_path: Path, *, fail_on: str, require_assert: bool) -> dict[str, object]:
    base = load_results(base_path)
    new = load_results(new_path)
    return diff_runs(base.values(), new.values(), fail_on=fail_on, require_assert=require_assert)


def render_markdown(compare: dict[str, object], out_path: Optional[Path]) -> str:
    lines: list[str] = []
    base_counts = compare["base_counts"]  # type: ignore[index]
    new_counts = compare["new_counts"]  # type: ignore[index]
    fail_on = compare.get("fail_on", "bad")  # type: ignore[assignment]
    require_assert = bool(compare.get("require_assert", False))

    def _bad_total(counts: dict) -> int:
        bad_from_compare = compare.get("base_bad_total") if counts is base_counts else compare.get("new_bad_total")
        if isinstance(bad_from_compare, int):
            return bad_from_compare
        bad_set = bad_statuses(str(fail_on), require_assert)
        total = 0
        for status in bad_set:
            try:
                total += int(counts.get(status, 0) or 0)
            except Exception:
                continue
        return total

    base_bad = _bad_total(base_counts)  # type: ignore[arg-type]
    new_bad = _bad_total(new_counts)  # type: ignore[arg-type]
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

    def table(title: str, rows: list[dict]) -> None:
        lines.append(f"## {title}")
        if not rows:
            lines.append("None")
            lines.append("")
            return
        lines.append("| id | status | reason | artifacts |")
        lines.append("|---|---|---|---|")
        for row in sorted(rows, key=lambda r: r.get("id", "")):
            artifacts = row.get("artifacts", {})
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


def write_junit(compare: dict[str, object], out_path: Path) -> None:
    import xml.etree.ElementTree as ET

    suite = ET.Element("testsuite", name="demo_qa_compare")
    bad = compare["new_fail"] + compare["still_fail"]  # type: ignore[operator]
    fixed = compare["fixed"]  # type: ignore[assignment]
    all_ids_list = list(compare.get("all_ids", []) or [])  # type: ignore[arg-type]
    all_ids = sorted(all_ids_list)
    cases_total = len(all_ids)
    suite.set("tests", str(cases_total))
    suite.set("failures", str(len(bad)))
    suite.set("errors", "0")

    for row in sorted(bad, key=lambda r: r.get("id", "")):
        tc = ET.SubElement(suite, "testcase", name=row["id"])
        msg = row.get("reason", "") or f"{row.get('from')} → {row.get('to')}"
        failure = ET.SubElement(tc, "failure", message=msg)
        artifacts = row.get("artifacts", {})
        if artifacts:
            failure.text = "\n".join(f"{k}: {v}" for k, v in sorted(artifacts.items()))

    for row in sorted(fixed, key=lambda r: r.get("id", "")):
        ET.SubElement(suite, "testcase", name=row["id"])

    bad_ids = {row["id"] for row in bad}
    fixed_ids = {row["id"] for row in fixed}
    ok_ids = [cid for cid in all_ids if cid not in bad_ids and cid not in fixed_ids]
    for cid in ok_ids:
        ET.SubElement(suite, "testcase", name=cid)

    out_path.write_text(ET.tostring(suite, encoding="unicode"), encoding="utf-8")


def _select_cases_for_rerun(
    cases: list[Case],
    baseline_for_filter: Optional[Mapping[str, RunResult]],
    *,
    require_assert: bool,
    fail_on: str,
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
    if not baseline_for_filter:
        return filtered
    target_ids = {
        case_id for case_id, res in baseline_for_filter.items() if res.status in bad_statuses(fail_on, require_assert)
    }
    return [case for case in filtered if case.id in target_ids]


def handle_batch(args) -> int:
    started_at = datetime.datetime.utcnow()
    run_id = uuid.uuid4().hex[:8]
    interrupted = False
    interrupted_at_case_id: str | None = None
    cases_hash = _hash_file(args.cases)

    try:
        settings = load_settings(config_path=args.config, data_dir=args.data)
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    try:
        cases = load_cases(args.cases)
    except Exception as exc:
        print(f"Cases error: {exc}", file=sys.stderr)
        return 2

    baseline_for_filter: Optional[Mapping[str, RunResult]] = None
    baseline_for_compare: Optional[Mapping[str, RunResult]] = None

    artifacts_dir = args.artifacts_dir
    if artifacts_dir is None:
        artifacts_dir = args.data / ".runs"

    include_tags = _split_csv(args.include_tags)
    exclude_tags = _split_csv(args.exclude_tags)
    include_ids = _load_ids(args.include_ids)
    exclude_ids = _load_ids(args.exclude_ids)

    baseline_filter_path = args.only_failed_from
    only_failed_baseline_kind: str | None = None
    effective_results_path: Path | None = None
    if args.only_failed_from:
        only_failed_baseline_kind = "path"
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
        baseline_for_filter = effective_results
        baseline_filter_path = eff_path
        effective_results_path = eff_path
        only_failed_baseline_kind = "effective"
    elif args.only_failed:
        latest_results = _load_latest_results(artifacts_dir, args.tag)
        if latest_results:
            baseline_filter_path = latest_results
            only_failed_baseline_kind = "latest"
        else:
            latest_run = _load_latest_run(artifacts_dir, args.tag)
            if latest_run:
                candidate = latest_run / "results.jsonl"
                if candidate.exists():
                    baseline_filter_path = candidate
                    only_failed_baseline_kind = "latest"
    if baseline_filter_path and baseline_for_filter is None:
        try:
            baseline_for_filter = load_results(baseline_filter_path)
        except Exception as exc:
            print(f"Failed to read baseline for --only-failed-from: {exc}", file=sys.stderr)
            return 2
    if args.only_failed and baseline_for_filter is None:
        print("No baseline found for --only-failed.", file=sys.stderr)
        return 2

    compare_path = args.compare_to
    if compare_path is None and args.only_failed and baseline_filter_path:
        compare_path = baseline_filter_path
    if compare_path:
        try:
            if baseline_filter_path and compare_path.resolve() == baseline_filter_path.resolve():
                baseline_for_compare = baseline_for_filter
            else:
                baseline_for_compare = load_results(compare_path)
        except Exception as exc:
            print(f"Failed to read baseline for --compare-to: {exc}", file=sys.stderr)
            return 2

    cases = _select_cases_for_rerun(
        cases,
        baseline_for_filter,
        require_assert=args.require_assert,
        fail_on=args.fail_on,
        include_tags=include_tags,
        exclude_tags=exclude_tags,
        include_ids=include_ids,
        exclude_ids=exclude_ids,
    )

    baseline_planned_ids: set[str] | None = None
    missed_baseline_results: Optional[Mapping[str, RunResult]] = None
    missed_baseline_path: Path | None = None
    missed_baseline_run: Path | None = None
    only_missed_baseline_kind: str | None = None
    if args.only_missed:
        if args.only_missed_from:
            missed_baseline_path = args.only_missed_from
            only_missed_baseline_kind = "path"
            try:
                missed_baseline_results = load_results(missed_baseline_path)
            except Exception as exc:
                print(f"Failed to read baseline for --only-missed-from: {exc}", file=sys.stderr)
                return 2
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
            missed_baseline_path = eff_path
            missed_baseline_results = effective_results
            only_missed_baseline_kind = "effective"
            baseline_planned_ids = (
                {str(cid) for cid in effective_meta.get("planned_case_ids", [])}
                if isinstance(effective_meta, dict)
                else None
            )
            if not baseline_planned_ids:
                print(
                    "Effective results missing planned_case_ids; computing missed relative to current filtered cases.",
                    file=sys.stderr,
                )
                baseline_planned_ids = {case.id for case in cases}
        else:
            missed_baseline_path = args.only_missed_from or _load_latest_results(artifacts_dir, args.tag)
            if args.only_missed_from:
                only_missed_baseline_kind = "path"
            elif missed_baseline_path:
                only_missed_baseline_kind = "latest"
            missed_baseline_run = _run_dir_from_results_path(missed_baseline_path)
            if missed_baseline_run is None:
                missed_baseline_run = _load_latest_run(artifacts_dir, args.tag)
            if missed_baseline_path:
                try:
                    missed_baseline_results = load_results(missed_baseline_path)
                except Exception as exc:
                    print(f"Failed to read baseline for --only-missed: {exc}", file=sys.stderr)
                    return 2
            else:
                print("No baseline results found for --only-missed; running all filtered cases.", file=sys.stderr)
            baseline_meta = _load_run_meta(missed_baseline_run)
            if isinstance(baseline_meta, dict):
                planned_from_meta = baseline_meta.get("planned_case_ids")
                if isinstance(planned_from_meta, list):
                    baseline_planned_ids = {str(cid) for cid in planned_from_meta}
                else:
                    print(
                        "Baseline run meta missing planned_case_ids; computing missed relative to current filtered cases.",
                        file=sys.stderr,
                    )
                    baseline_planned_ids = {case.id for case in cases}
        if args.only_missed and missed_baseline_results is None:
            print("No baseline found for --only-missed.", file=sys.stderr)
            return 2

    planned_case_ids = [case.id for case in cases]
    if args.only_missed:
        planned_pool = baseline_planned_ids or set(planned_case_ids)
        missed_ids = _missed_case_ids(planned_pool, missed_baseline_results)
        cases = [case for case in cases if case.id in missed_ids]
        planned_case_ids = [case.id for case in cases]
        if not cases:
            print("0 missed cases selected.", file=sys.stderr)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_folder = artifacts_dir / "runs" / f"{timestamp}_{args.cases.stem}"
    results_path = args.out or (run_folder / "results.jsonl")
    artifacts_root = run_folder / "cases"
    results_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path = results_path.with_name("summary.json")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    history_path = args.history or (args.data / ".runs" / "history.jsonl")

    log_dir = args.log_dir or args.data / ".runs" / "logs"
    configure_logging(
        level=args.log_level,
        log_dir=log_dir,
        to_stderr=args.log_stderr,
        jsonl=args.log_jsonl,
        run_id=None,
    )

    provider, _ = build_provider(args.data, args.schema, enable_semantic=args.enable_semantic)
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
                print("Interrupted during case execution; saved partial status.", file=sys.stderr)
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

    diff_block: dict | None = None
    baseline_path: Path | None = None
    if baseline_for_compare:
        baseline_path = args.compare_to or baseline_filter_path
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
    bad_count = sum(int(counts.get(status, 0) or 0) for status in policy_bad)
    exit_code = 130 if interrupted else (1 if bad_count else 0)

    ended_at = datetime.datetime.utcnow()
    duration_ms = int((ended_at - started_at).total_seconds() * 1000)
    executed_results = {res.id: res for res in results}
    planned_total = len(planned_case_ids)
    executed_total = len(results)
    missed_total = len(_missed_case_ids(planned_case_ids, executed_results))
    summary = {
        "run_id": run_id,
        "started_at": started_at.isoformat() + "Z",
        "ended_at": ended_at.isoformat() + "Z",
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
        "interrupted": interrupted,
        "interrupted_at_case_id": interrupted_at_case_id,
        "tag": args.tag,
        "note": args.note,
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
            }
        )

    _update_latest_markers(run_folder, results_path, artifacts_dir, args.tag)
    effective_path = None
    effective_meta_path = None
    if args.tag:
        try:
            effective_path, effective_meta_path = _update_effective_snapshot(
                artifacts_dir=artifacts_dir,
                tag=args.tag,
                cases_hash=cases_hash,
                cases_path=args.cases,
                planned_case_ids=planned_case_ids,
                executed_results=results,
                run_folder=run_folder,
                planned_case_ids_source=planned_case_ids,
            )
        except Exception as exc:
            print(f"Failed to update effective results for tag {args.tag!r}: {exc}", file=sys.stderr)

    config_hash = _hash_file(args.config) if args.config else None
    schema_hash = _hash_file(args.schema)
    data_fingerprint = _fingerprint_dir(args.data, verbose=args.fingerprint_verbose)
    llm_settings = settings.llm
    run_meta = {
        "run_id": run_id,
        "timestamp": started_at.isoformat() + "Z",
        "tag": args.tag,
        "note": args.note,
        "inputs": {
            "cases_path": str(args.cases),
            "cases_hash": cases_hash,
            "config_path": str(args.config) if args.config else None,
            "config_hash": config_hash,
            "schema_path": str(args.schema),
            "schema_hash": schema_hash,
            "data_dir": str(args.data),
        },
        "planned_case_ids": planned_case_ids,
        "planned_total": planned_total,
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
            "baseline_tag": args.tag,
            "effective_path": str(effective_path) if effective_path else None,
            "plan_only": args.plan_only,
            "fail_fast": args.fail_fast,
            "max_fails": args.max_fails,
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
        "git_sha": _git_sha(),
        "results_path": str(results_path),
        "summary_path": str(summary_path),
        "run_dir": str(run_folder),
    }
    dump_json(run_folder / "run_meta.json", run_meta)

    prate = _pass_rate(counts)
    history_entry = {
        "run_id": run_id,
        "timestamp": started_at.isoformat() + "Z",
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
        "missed_total": missed_total,
        "interrupted": interrupted,
        "interrupted_at_case_id": interrupted_at_case_id,
    }
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
        settings = load_settings(config_path=args.config, data_dir=args.data)
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
    run_folder = artifacts_dir / "runs" / f"{timestamp}_{args.cases.stem}"
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
    summary = {
        "run_id": run_folder.name,
        "timestamp": timestamp + "Z",
        "counts": counts,
        "results_path": str(results_path),
        "fail_on": "bad",
        "require_assert": False,
    }
    summary_path = write_summary(results_path, summary)
    save_dir = run_folder.parent
    save_dir.mkdir(parents=True, exist_ok=True)
    (save_dir / "latest.txt").write_text(str(run_folder), encoding="utf-8")
    (save_dir / "latest_results.txt").write_text(str(results_path), encoding="utf-8")

    print(format_status_line(result))
    print(f"Artifacts: {result.artifacts_dir}")
    print(f"Summary: {summary_path}")
    return 0


def handle_case_open(args) -> int:
    artifacts_dir = args.artifacts_dir or (args.data / ".runs")
    run_path = _resolve_run_path(args.run, artifacts_dir)
    if not run_path:
        print("No run found. Provide --run or ensure runs/latest.txt exists.", file=sys.stderr)
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
        pass_rate = entry.get("pass_rate")
        median = entry.get("median_total_s")
        delta_pass = None
        delta_median = None
        if prev:
            if pass_rate is not None and prev.get("pass_rate") is not None:
                delta_pass = pass_rate - prev.get("pass_rate")
            if median is not None and prev.get("median_total_s") is not None:
                delta_median = median - prev.get("median_total_s")
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
    history_path: Optional[Path] = args.history
    if history_path is None:
        if not args.data:
            print("Provide --data or --history to locate history.jsonl", file=sys.stderr)
            return 2
        history_path = args.data / ".runs" / "history.jsonl"
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


def handle_compare(args) -> int:
    if not args.base.exists() or not args.new.exists():
        print("Base or new results file not found.", file=sys.stderr)
        return 2
    comparison = compare_runs(args.base, args.new, fail_on=args.fail_on, require_assert=args.require_assert)
    report = render_markdown(comparison, args.out)
    print(report)
    if args.junit:
        write_junit(comparison, args.junit)
        print(f"JUnit written to {args.junit}")
    return 0


__all__ = [
    "handle_batch",
    "handle_case_open",
    "handle_case_run",
    "handle_chat",
    "bad_statuses",
    "is_failure",
    "write_results",
    "write_summary",
    "_load_latest_run",
    "_find_case_artifact",
]
