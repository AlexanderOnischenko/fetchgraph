from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from .chat_repl import start_repl
from .data_gen import generate_and_save
from .llm.factory import build_llm
from .logging_config import configure_logging
from .provider_factory import build_provider
from .runner import RunResult, build_agent, format_status_line, load_cases, run_one, summarize
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


def is_failure(status: str, fail_on: str) -> bool:
    if fail_on == "error":
        return status == "error"
    if fail_on == "mismatch":
        return status in {"error", "mismatch"}
    return status in {"error", "mismatch", "skipped"}


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

    artifacts_dir = args.artifacts_dir
    if artifacts_dir is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        artifacts_dir = args.data / ".runs" / f"batch_{timestamp}"
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
        result = run_one(case, runner, artifacts_dir)
        results.append(result)
        print(format_status_line(result))
        if is_failure(result.status, args.fail_on):
            failures += 1
            if args.fail_fast or (args.max_fails and failures >= args.max_fails):
                break

    write_results(args.out, results)
    summary = summarize(results)
    summary_path = write_summary(args.out, summary)
    print(f"Summary: {json.dumps(summary, ensure_ascii=False)}")
    print(f"Results written to: {args.out}")
    print(f"Summary written to: {summary_path}")

    failure_count = sum(1 for res in results if is_failure(res.status, args.fail_on))
    return 1 if failure_count else 0


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
    batch_p.add_argument("--out", type=Path, required=True, help="Path to results jsonl")
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
