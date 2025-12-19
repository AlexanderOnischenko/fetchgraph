from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from .chat_repl import start_repl
from .data_gen import generate_and_save
from .provider_factory import build_provider
from .cases.runner import run_cases
from .llm.factory import build_llm
from .settings import DemoQASettings, load_settings


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
    chat_p.add_argument("--llm-provider", choices=["mock", "openai"], default=None)
    chat_p.add_argument("--enable-semantic", action="store_true")

    cases_p = sub.add_parser("run-cases", help="Run regression cases with mock LLM")
    cases_p.add_argument("--data", type=Path, required=True)
    cases_p.add_argument("--schema", type=Path, required=True)

    args = parser.parse_args()

    if args.command == "gen":
        generate_and_save(args.out, rows=args.rows, seed=args.seed, enable_semantic=args.enable_semantic)
        print(f"Generated data in {args.out}")
        return

    if args.command == "chat":
        overrides = {}
        if args.llm_provider:
            overrides["llm"] = {"provider": args.llm_provider}

        try:
            settings = load_settings(config_path=args.config, data_dir=args.data, overrides=overrides)
        except Exception as exc:
            raise SystemExit(f"Configuration error: {exc}")

        llm = build_llm(settings)

        start_repl(args.data, args.schema, llm, enable_semantic=args.enable_semantic)
        return

    if args.command == "run-cases":
        results = run_cases(args.data, args.schema)
        failed = [k for k, ok in results.items() if not ok]
        print(json.dumps(results, indent=2, ensure_ascii=False))
        if failed:
            raise SystemExit(f"Failed cases: {failed}")
        return


if __name__ == "__main__":
    main()
