from __future__ import annotations

import datetime
import hashlib
import json
import sys
import uuid
from pathlib import Path
from typing import Iterable, Mapping, Optional

from .llm.factory import build_llm
from .logging_config import configure_logging
from .provider_factory import build_provider
from .runner import (
    Case,
    RunResult,
    build_agent,
    compare_results,
    format_status_line,
    load_cases,
    load_results,
    run_one,
    summarize,
)
from .settings import load_settings


def write_results(out_path: Path, results: Iterable[RunResult]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for res in results:
            f.write(json.dumps(res.to_json(), ensure_ascii=False) + "\n")


def write_summary(out_path: Path, summary: dict) -> Path:
    summary_path = out_path.with_name("summary.json")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary_path


def is_failure(status: str, fail_on: str, require_assert: bool) -> bool:
    bad = {"error", "failed", "mismatch"}
    unchecked = {"unchecked", "plan_only"}
    if require_assert:
        bad |= unchecked
    if fail_on == "error":
        bad = {"error"}
    elif fail_on == "mismatch":
        bad = {"mismatch"}
    elif fail_on == "unchecked":
        bad |= unchecked
    elif fail_on == "bad":
        bad = {"error", "failed", "mismatch"}
        if require_assert:
            bad |= unchecked
    elif fail_on == "any":
        bad |= unchecked
    elif fail_on == "skipped":
        bad |= {"skipped"}
    return status in bad


def _hash_file(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha256(data).hexdigest()


def build_config_fingerprint(settings, cases_path: Path) -> Mapping[str, object]:
    llm_settings = settings.llm
    return {
        "base_url": llm_settings.base_url or "https://api.openai.com/v1",
        "plan_model": llm_settings.plan_model,
        "synth_model": llm_settings.synth_model,
        "cases_hash": _hash_file(cases_path),
    }


def _load_latest_run(artifacts_dir: Path) -> Optional[Path]:
    latest_file = artifacts_dir / "runs" / "latest.txt"
    if latest_file.exists():
        content = latest_file.read_text(encoding="utf-8").strip()
        if content:
            return Path(content)
    return None


def _load_latest_results(artifacts_dir: Path) -> Optional[Path]:
    latest_file = artifacts_dir / "runs" / "latest_results.txt"
    if latest_file.exists():
        content = latest_file.read_text(encoding="utf-8").strip()
        if content:
            return Path(content)
    return None


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


def _select_cases_for_rerun(
    cases: list[Case],
    baseline_for_filter: Optional[Mapping[str, RunResult]],
    *,
    require_assert: bool,
    fail_on: str,
) -> list[Case]:
    if not baseline_for_filter:
        return cases
    bad_statuses = {"mismatch", "failed", "error"}
    if require_assert or fail_on in {"unchecked", "any"}:
        bad_statuses |= {"unchecked", "plan_only"}
    target_ids = {case_id for case_id, res in baseline_for_filter.items() if res.status in bad_statuses}
    return [case for case in cases if case.id in target_ids]


def handle_batch(args) -> int:
    started_at = datetime.datetime.utcnow()
    run_id = uuid.uuid4().hex[:8]

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

    baseline_filter_path = args.only_failed_from
    if args.only_failed and not baseline_filter_path:
        latest_results = _load_latest_results(artifacts_dir)
        if latest_results:
            baseline_filter_path = latest_results
        else:
            latest_run = _load_latest_run(artifacts_dir)
            if latest_run:
                candidate = latest_run / "results.jsonl"
                if candidate.exists():
                    baseline_filter_path = candidate
    if baseline_filter_path:
        try:
            baseline_for_filter = load_results(baseline_filter_path)
        except Exception as exc:
            print(f"Failed to read baseline for --only-failed-from: {exc}", file=sys.stderr)
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
        cases, baseline_for_filter, require_assert=args.require_assert, fail_on=args.fail_on
    )

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_folder = artifacts_dir / "runs" / f"{timestamp}_{args.cases.stem}"
    results_path = args.out or (run_folder / "results.jsonl")
    artifacts_root = run_folder / "cases"
    results_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path = results_path.with_name("summary.json")
    artifacts_dir.mkdir(parents=True, exist_ok=True)

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

    results: list[RunResult] = []
    failures = 0
    for case in cases:
        result = run_one(case, runner, artifacts_root, plan_only=args.plan_only)
        results.append(result)
        if not args.quiet:
            print(format_status_line(result))
        if is_failure(result.status, args.fail_on, args.require_assert):
            failures += 1
            if args.fail_fast or (args.max_fails and failures >= args.max_fails):
                break

    write_results(results_path, results)
    counts = summarize(results)

    results_by_id = {r.id: r for r in results}
    diff_block: dict | None = None
    baseline_path: Path | None = None
    if baseline_for_compare:
        baseline_path = args.compare_to or baseline_filter_path
        diff = compare_results(baseline_for_compare, results_by_id, require_assert=args.require_assert)
        if baseline_path:
            diff["baseline_path"] = str(baseline_path)
        diff_block = diff

    failure_count = sum(1 for res in results if is_failure(res.status, args.fail_on, args.require_assert))
    exit_code = 1 if failure_count else 0

    ended_at = datetime.datetime.utcnow()
    duration_ms = int((ended_at - started_at).total_seconds() * 1000)
    summary = {
        "run_id": run_id,
        "started_at": started_at.isoformat() + "Z",
        "ended_at": ended_at.isoformat() + "Z",
        "duration_ms": duration_ms,
        "counts": counts,
        "exit_code": exit_code,
        "config_fingerprint": build_config_fingerprint(settings, args.cases),
        "results_path": str(results_path),
        "require_assert": args.require_assert,
        "fail_on": args.fail_on,
    }
    if diff_block:
        summary["diff"] = diff_block

    summary_path = write_summary(results_path, summary)

    latest_path = run_folder.parent / "latest.txt"
    latest_results_path = run_folder.parent / "latest_results.txt"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(str(run_folder), encoding="utf-8")
    latest_results_path.write_text(str(results_path), encoding="utf-8")

    bad_count = counts.get("mismatch", 0) + counts.get("failed", 0) + counts.get("error", 0)
    unchecked = counts.get("unchecked", 0)
    plan_only = counts.get("plan_only", 0)
    if args.require_assert or args.fail_on in {"unchecked", "any"}:
        bad_count += unchecked + plan_only
    summary_line = (
        f"Batch: {counts.get('total', 0)} cases | Checked: {counts.get('checked_total', 0)} | "
        f"Checked OK: {counts.get('checked_ok', 0)} | Unchecked(no-assert): {unchecked} | "
        f"Plan-only: {plan_only} | BAD: {bad_count} | Skipped: {counts.get('skipped', 0)}"
    )

    if args.quiet:
        print(summary_line)
        if diff_block:
            print(
                f"Δ vs baseline: +{len(diff_block.get('new_ok', []))} green, "
                f"-{len(diff_block.get('regressed', []))} regressions, "
                f"{len(diff_block.get('still_bad', []))} still failing, "
                f"{len(diff_block.get('new_unchecked', []))} new unchecked"
            )
        return exit_code

    print(summary_line)
    if diff_block:
        print(
            f"Δ vs baseline: +{len(diff_block.get('new_ok', []))} green, "
            f"-{len(diff_block.get('regressed', []))} regressions, "
            f"{len(diff_block.get('still_bad', []))} still failing, "
            f"{len(diff_block.get('new_unchecked', []))} new unchecked"
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
    save_path = run_folder.parent / "latest.txt"
    save_path.parent.mkdir(parents=True, exist_ok=True)
    save_path.write_text(str(run_folder), encoding="utf-8")

    print(format_status_line(result))
    print(f"Artifacts: {result.artifacts_dir}")
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


__all__ = [
    "handle_batch",
    "handle_case_open",
    "handle_case_run",
    "handle_chat",
    "is_failure",
    "write_results",
    "write_summary",
    "_load_latest_run",
    "_find_case_artifact",
    "build_config_fingerprint",
]
