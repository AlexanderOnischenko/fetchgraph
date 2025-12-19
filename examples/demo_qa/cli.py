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
from .llm.factory import build_llm
from .llm.cache import apply_llm_cache
from .runner import ensure_artifacts_root, load_cases, setup_runner
from .logging_config import configure_logging
from .settings import load_settings


def _is_failure(status: str, fail_on: str) -> bool:
    if status == "error":
        return True
    if fail_on in {"mismatch", "any"} and status == "mismatch":
        return True
    if fail_on == "any" and status == "skipped":
        return True
    return False


def _build_summary(results, *, fail_on: str, artifacts_dir: str) -> dict:
    import statistics

    totals = {"ok": 0, "error": 0, "mismatch": 0, "skipped": 0}
    durations = []
    for res in results:
        totals[res.status] = totals.get(res.status, 0) + 1
        if res.timings.total_s is not None:
            durations.append(res.timings.total_s)
    avg = statistics.mean(durations) if durations else 0.0
    median = statistics.median(durations) if durations else 0.0
    failures = sum(1 for r in results if _is_failure(r.status, fail_on))
    return {
        "total": len(results),
        "ok": totals.get("ok", 0),
        "error": totals.get("error", 0),
        "mismatch": totals.get("mismatch", 0),
        "skipped": totals.get("skipped", 0),
        "avg_total_s": avg,
        "median_total_s": median,
        "fail_on": fail_on,
        "failures": failures,
        "artifacts_dir": artifacts_dir,
    }


def _format_summary(summary: dict) -> str:
    return (
        "Summary: "
        f"total={summary['total']}, "
        f"ok={summary.get('ok', 0)}, "
        f"mismatch={summary.get('mismatch', 0)}, "
        f"error={summary.get('error', 0)}, "
        f"skipped={summary.get('skipped', 0)}, "
        f"avg={summary.get('avg_total_s', 0):.2f}s, "
        f"median={summary.get('median_total_s', 0):.2f}s"
    )


def _cache_namespace(settings) -> str:
    llm = settings.llm
    payload = {
        "base_url": llm.base_url,
        "plan_model": llm.plan_model,
        "synth_model": llm.synth_model,
        "plan_temperature": llm.plan_temperature,
        "synth_temperature": llm.synth_temperature,
    }
    return json.dumps(payload, sort_keys=True)


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
    chat_p.add_argument("--llm-cache", choices=["off", "record", "replay"], default="off")
    chat_p.add_argument("--llm-cache-file", type=Path, default=None)

    batch_p = sub.add_parser("batch", help="Run batch QA cases")
    batch_p.add_argument("--data", type=Path, required=True)
    batch_p.add_argument("--schema", type=Path, required=True)
    batch_p.add_argument("--cases", type=Path, required=True, help="Path to cases JSONL")
    batch_p.add_argument("--out", type=Path, required=True, help="Path to results JSONL")
    batch_p.add_argument("--config", type=Path, default=None, help="Path to demo_qa.toml")
    batch_p.add_argument("--artifacts-dir", type=Path, default=None)
    batch_p.add_argument("--enable-semantic", action="store_true")
    batch_p.add_argument("--fail-fast", action="store_true", help="Stop on first failure")
    batch_p.add_argument("--max-fails", type=int, default=None, help="Stop after N failures")
    batch_p.add_argument("--fail-on", choices=["error", "mismatch", "any"], default="error")
    batch_p.add_argument("--llm-cache", choices=["off", "record", "replay"], default="off")
    batch_p.add_argument("--llm-cache-file", type=Path, default=None)
    chat_p.add_argument("--log-level", default="INFO", help="Logging level (INFO, DEBUG, etc.)")
    chat_p.add_argument("--log-dir", type=Path, default=None, help="Directory for log files")
    chat_p.add_argument("--log-stderr", action="store_true", help="Also stream logs to stderr")
    chat_p.add_argument("--log-jsonl", action="store_true", help="Write logs as JSONL")

    args = parser.parse_args()

    if args.command == "gen":
        generate_and_save(args.out, rows=args.rows, seed=args.seed, enable_semantic=args.enable_semantic)
        print(f"Generated data in {args.out}")
        return

    if args.command == "chat":
        try:
            settings = load_settings(config_path=args.config, data_dir=args.data)
        except Exception as exc:
            print(f"Configuration error: {exc}", file=sys.stderr)
            raise SystemExit(2) from exc

        log_dir = args.log_dir or args.data / ".runs" / "logs"
        log_file = configure_logging(
            level=args.log_level,
            log_dir=log_dir,
            to_stderr=args.log_stderr,
            jsonl=args.log_jsonl,
            run_id=None,
        )

        llm = build_llm(settings)
        cache_file = args.llm_cache_file or (args.data / ".runs" / "llm_cache.jsonl")
        llm = apply_llm_cache(
            llm,
            mode=args.llm_cache,
            path=cache_file if args.llm_cache != "off" else None,
            namespace=_cache_namespace(settings),
        )

        start_repl(
            args.data,
            args.schema,
            llm,
            enable_semantic=args.enable_semantic,
            log_file=log_file,
        )
        return

    if args.command == "batch":
        try:
            settings = load_settings(config_path=args.config, data_dir=args.data)
        except Exception as exc:
            print(f"Configuration error: {exc}", file=sys.stderr)
            raise SystemExit(2) from exc

        llm = build_llm(settings)
        cache_file = args.llm_cache_file or (args.data / ".runs" / "llm_cache.jsonl")
        llm = apply_llm_cache(
            llm,
            mode=args.llm_cache,
            path=cache_file if args.llm_cache != "off" else None,
            namespace=_cache_namespace(settings),
        )

        try:
            cases = load_cases(args.cases)
        except Exception as exc:
            print(f"Cases error: {exc}", file=sys.stderr)
            raise SystemExit(2) from exc

        artifacts_root = ensure_artifacts_root(args.artifacts_dir, data_dir=args.data)
        runner = setup_runner(args.data, args.schema, llm, enable_semantic=args.enable_semantic)
        results = []
        failures = 0
        if args.fail_fast and args.max_fails is None:
            args.max_fails = 1

        for case in cases:
            res = runner.run_one(case, artifacts_root=artifacts_root)
            results.append(res)

            duration = f"{res.timings.total_s:.2f}s" if res.timings.total_s is not None else "-"
            extra = ""
            if res.status == "mismatch" and res.expected_check:
                extra = res.expected_check.details or res.expected_check.mode
            elif res.status == "error":
                extra = res.error or ""
            elif res.status == "skipped":
                extra = "skipped"
            print(f"{res.status.upper():<7} {res.id} {duration} {extra}".strip())

            if _is_failure(res.status, args.fail_on):
                failures += 1
                if args.max_fails and failures >= args.max_fails:
                    break

        out_path: Path = args.out
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            for res in results:
                f.write(res.to_json() + "\n")

        summary_path = out_path.with_name("summary.json")
        summary = _build_summary(results, fail_on=args.fail_on, artifacts_dir=str(artifacts_root))
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

        print(_format_summary(summary))

        if summary["failures"] > 0:
            raise SystemExit(1)
        return


if __name__ == "__main__":
    main()
