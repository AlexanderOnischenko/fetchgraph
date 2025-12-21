from __future__ import annotations

from ..runs.case_history import _load_case_history


def handle_history_case(args) -> int:
    artifacts_dir = args.data / ".runs"
    path = artifacts_dir / "runs" / "cases" / f"{args.case_id}.jsonl"
    entries = _load_case_history(path)
    if args.tag:
        entries = [e for e in entries if e.get("tag") == args.tag]
    if not entries:
        print(f"No history found for case {args.case_id}.")
        return 0
    entries = list(reversed(entries))[: args.limit]
    header = (
        f"{'timestamp':<25} {'run_id':<12} {'tag':<15} {'status':<10} "
        f"{'reason':<30} {'note':<15} {'run_dir':<30}"
    )
    print(header)
    for e in entries:
        ts = str(e.get("timestamp", ""))[:25]
        print(
            f"{ts:<25} {str(e.get('run_id','')):<12} {str(e.get('tag','')):<15} "
            f"{str(e.get('status','')):<10} {str(e.get('reason','')):<30} {str(e.get('note','')):<15} "
            f"{str(e.get('run_dir','')):<30}"
        )
    return 0


__all__ = ["handle_history_case"]
