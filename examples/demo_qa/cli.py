from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"


def ensure_repo_imports() -> None:
    """Ensure local src/ is on sys.path for demo entrypoints."""
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))


ensure_repo_imports()

from .batch import (
    handle_batch,
    handle_case_open,
    handle_case_run,
    handle_chat,
    handle_compare,
    handle_stats,
)  # noqa: E402
from .data_gen import generate_and_save  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
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
        choices=["error", "mismatch", "bad", "unchecked", "any", "skipped"],
        default="bad",
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
    batch_p.add_argument("--only-failed", action="store_true", help="Use latest run for --only-failed-from automatically")
    batch_p.add_argument("--plan-only", action="store_true", help="Run planner only (no fetch/synthesize)")
    batch_p.add_argument("--quiet", action="store_true", help="Print only summary and exit code")
    batch_p.add_argument("--show-failures", type=int, default=10, help="How many failing cases to show")
    batch_p.add_argument("--show-artifacts", action="store_true", help="Show artifact paths for failures")
    batch_p.add_argument("--history", type=Path, default=None, help="Path to history.jsonl (default: <data>/.runs/history.jsonl)")

    case_root = sub.add_parser("case", help="Single-case utilities")
    case_sub = case_root.add_subparsers(dest="case_command", required=True)

    case_run = case_sub.add_parser("run", help="Run a single case by id")
    case_run.add_argument("case_id")
    case_run.add_argument("--cases", type=Path, required=True, help="Path to cases jsonl")
    case_run.add_argument("--data", type=Path, required=True)
    case_run.add_argument("--schema", type=Path, required=True)
    case_run.add_argument("--config", type=Path, default=None)
    case_run.add_argument("--enable-semantic", action="store_true")
    case_run.add_argument("--artifacts-dir", type=Path, default=None)
    case_run.add_argument("--plan-only", action="store_true")

    case_open = case_sub.add_parser("open", help="Show artifacts for a case in a run folder")
    case_open.add_argument("case_id")
    case_open.add_argument("--data", type=Path, required=True)
    case_open.add_argument("--run", type=Path, default=None, help="Run folder (defaults to latest)")
    case_open.add_argument(
        "--artifacts-dir", type=Path, default=None, help="Base artifacts dir for latest lookup (default data/.runs)"
    )

    stats_p = sub.add_parser("stats", help="Show batch history stats")
    stats_p.add_argument("--data", type=Path, default=None, help="Data dir to resolve default history path")
    stats_p.add_argument("--history", type=Path, default=None, help="Path to history.jsonl (default: <data>/.runs/history.jsonl)")
    stats_p.add_argument("--last", type=int, default=10, help="How many recent runs to show")
    stats_p.add_argument("--group-by", choices=["config_hash"], default=None, help="Group stats by config hash")

    compare_p = sub.add_parser("compare", help="Compare two batch result files")
    compare_p.add_argument("--base", type=Path, required=True, help="Path to baseline results.jsonl")
    compare_p.add_argument("--new", type=Path, required=True, help="Path to new results.jsonl")
    compare_p.add_argument("--out", type=Path, default=None, help="Path to markdown report to write")
    compare_p.add_argument("--junit", type=Path, default=None, help="Path to junit xml output")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "gen":
        generate_and_save(args.out, rows=args.rows, seed=args.seed, enable_semantic=args.enable_semantic)
        print(f"Generated data in {args.out}")
        raise SystemExit(0)

    if args.command == "chat":
        code = handle_chat(args)
    elif args.command == "batch":
        code = handle_batch(args)
    elif args.command == "case":
        if args.case_command == "run":
            code = handle_case_run(args)
        elif args.case_command == "open":
            code = handle_case_open(args)
        else:
            code = 1
    elif args.command == "stats":
        code = handle_stats(args)
    elif args.command == "compare":
        code = handle_compare(args)
    else:
        code = 0
    raise SystemExit(code)


if __name__ == "__main__":
    main()
