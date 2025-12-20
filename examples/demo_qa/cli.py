from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import sys
import uuid
from pathlib import Path
from typing import Iterable, Mapping, Optional

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from .chat_repl import start_repl
from .data_gen import generate_and_save
from .llm.factory import build_llm
from .logging_config import configure_logging
from .provider_factory import build_provider
from .runner import (
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
    failure_statuses = {"error", "mismatch", "failed"}
    if fail_on == "error":
        failure_statuses = {"error"}
    elif fail_on == "mismatch":
        failure_statuses = {"error", "mismatch", "failed"}
    else:
        failure_statuses = {"error", "mismatch", "failed", "unchecked"}
    if require_assert and status == "unchecked":
        return True
    return status in failure_statuses


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

    start_repl(
        args.data,
        args.schema,
        llm,
        enable_semantic=args.enable_semantic,
        log_file=log_file,
        diagnostics=diagnostics,
    )
    return 0


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

    if args.only_failed_from:
        try:
            baseline_for_filter = load_results(args.only_failed_from)
        except Exception as exc:
            print(f"Failed to read baseline for --only-failed-from: {exc}", file=sys.stderr)
            return 2

    if args.compare_to:
        try:
            if args.only_failed_from and args.compare_to.resolve() == args.only_failed_from.resolve():
                baseline_for_compare = baseline_for_filter
            else:
                baseline_for_compare = load_results(args.compare_to)
        except Exception as exc:
            print(f"Failed to read baseline for --compare-to: {exc}", file=sys.stderr)
            return 2

    if baseline_for_filter:
        bad_statuses = {"mismatch", "failed", "error"}
        if args.require_assert:
            bad_statuses.add("unchecked")
        target_ids = {case_id for case_id, res in baseline_for_filter.items() if res.status in bad_statuses}
        cases = [case for case in cases if case.id in target_ids]

    artifacts_dir = args.artifacts_dir
    if artifacts_dir is None:
        artifacts_dir = args.data / ".runs"
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
        result = run_one(case, runner, artifacts_root)
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
        baseline_path = args.compare_to or args.only_failed_from
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
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(str(run_folder), encoding="utf-8")

    bad_count = counts.get("mismatch", 0) + counts.get("failed", 0) + counts.get("error", 0)
    unchecked = counts.get("unchecked", 0)
    if args.require_assert:
        bad_count += unchecked
    summary_line = (
        f"Batch: {counts.get('total', 0)} cases | Checked: {counts.get('checked_total', 0)} | "
        f"OK: {counts.get('ok', 0)} | BAD: {bad_count} | Unchecked: {unchecked} | Skipped: {counts.get('skipped', 0)}"
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
        if is_failure(res.status, args.fail_on, args.require_assert) or (
            args.require_assert and res.status == "unchecked"
        ):
            failures_list[res.id] = res
    if failures_list:
        print(f"Failures (top {args.show_failures}):")
        for res in list(failures_list.values())[: args.show_failures]:
            reason = res.reason or res.error or ""
            print(f"- {res.id}: {res.status} ({reason}) [{res.artifacts_dir}]")

    print(f"Results written to: {results_path}")
    print(f"Summary written to: {summary_path}")

    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(description="Demo QA harness for fetchgraph")
    sub = parser.add_subparsers(dest="command", required=True)

    gen_p = sub.add_parser("gen", help="Generate synthetic dataset")
    gen_p.add_argument("--out", type=Path, required=True)
    gen_p.add_argument("--rows", type=int, default=1000)
    gen_p.add_argument("--seed", type=int, default=None)
    gen_p.add_argument("--enable-semantic", action="store_true")

    chat_p = sub.add_parser("chat", help="Start chat REPL")
    chat_p.add_argument("--data", type=Path, required=True)
    chat_p.add_argument("--schema", type=Path, required=True)
    chat_p.add_argument("--config", type=Path, default=None, help="Path to demo_qa.toml")
    chat_p.add_argument("--enable-semantic", action="store_true")
    chat_p.add_argument("--log-level", default="INFO", help="Logging level (INFO, DEBUG, etc.)")
    chat_p.add_argument("--log-dir", type=Path, default=None, help="Directory for log files")
    chat_p.add_argument("--log-stderr", action="store_true", help="Also stream logs to stderr")
    chat_p.add_argument("--log-jsonl", action="store_true", help="Write logs as JSONL")

    batch_p = sub.add_parser("batch", help="Run a batch of questions from a JSONL file")
    batch_p.add_argument("--data", type=Path, required=True)
    batch_p.add_argument("--schema", type=Path, required=True)
    batch_p.add_argument("--config", type=Path, default=None, help="Path to demo_qa.toml")
    batch_p.add_argument("--cases", type=Path, required=True, help="Path to cases jsonl")
    batch_p.add_argument("--out", type=Path, required=False, default=None, help="Path to results jsonl")
    batch_p.add_argument("--artifacts-dir", type=Path, default=None, help="Where to store per-case artifacts")
    batch_p.add_argument("--enable-semantic", action="store_true")
    batch_p.add_argument("--log-level", default="INFO", help="Logging level (INFO, DEBUG, etc.)")
    batch_p.add_argument("--log-dir", type=Path, default=None, help="Directory for log files")
    batch_p.add_argument("--log-stderr", action="store_true", help="Also stream logs to stderr")
    batch_p.add_argument("--log-jsonl", action="store_true", help="Write logs as JSONL")
    batch_p.add_argument("--max-fails", type=int, default=None, help="Maximum allowed failures before stopping")
    batch_p.add_argument("--fail-fast", action="store_true", help="Stop on first failing case")
    batch_p.add_argument(
        "--fail-on",
        choices=["error", "mismatch", "any"],
        default="mismatch",
        help="Which statuses should cause a failing exit code",
    )
    batch_p.add_argument("--require-assert", action="store_true", help="Treat unchecked cases as failures")
    batch_p.add_argument("--compare-to", type=Path, default=None, help="Path to previous results.jsonl for diff")
    batch_p.add_argument(
        "--only-failed-from",
        type=Path,
        default=None,
        help="Run only cases that failed/mismatched/errored in a previous results.jsonl",
    )
    batch_p.add_argument("--quiet", action="store_true", help="Print only summary and exit code")
    batch_p.add_argument("--show-failures", type=int, default=10, help="How many failing cases to show")

    args = parser.parse_args()

    if args.command == "gen":
        generate_and_save(args.out, rows=args.rows, seed=args.seed, enable_semantic=args.enable_semantic)
        print(f"Generated data in {args.out}")
        raise SystemExit(0)

    if args.command == "chat":
        code = handle_chat(args)
    elif args.command == "batch":
        code = handle_batch(args)
    else:
        code = 0
    raise SystemExit(code)


if __name__ == "__main__":
    main()
