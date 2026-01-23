from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from fetchgraph.replay.export import export_replay_fixture, export_replay_fixtures


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DemoQA: export replay fixtures from .runs")
    p.add_argument("--case", required=True, help="Case id to extract from runs")
    p.add_argument("--tag", default=None, help="Optional run tag filter")
    p.add_argument("--run-id", default=None, help="Exact run_id to select")
    p.add_argument("--id", default="plan_normalize.spec_v1", help="Replay point id to extract")
    p.add_argument("--spec-idx", type=int, default=None, help="Filter replay_point by meta.spec_idx")
    p.add_argument("--provider", default=None, help="Filter replay_point by meta.provider")
    p.add_argument("--with-requires", action="store_true", help="Export replay bundle with dependencies")
    p.add_argument("--all", action="store_true", help="Export all matching replay points")

    p.add_argument("--data", type=Path, default=None, help="Data dir containing .runs (default: cwd)")
    p.add_argument("--runs-dir", type=Path, default=None, help="Explicit .runs/runs dir (overrides --data)")
    p.add_argument(
        "--out-dir",
        type=Path,
        default=Path("tests") / "fixtures" / "replay_points",
        help="Output directory for replay fixtures",
    )
    return p.parse_args(argv)


def _load_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _iter_run_folders(runs_root: Path) -> Iterable[Path]:
    if not runs_root.exists():
        return []
    for entry in runs_root.iterdir():
        if entry.is_dir():
            yield entry


def _load_run_meta(run_folder: Path) -> dict:
    return (_load_json(run_folder / "run_meta.json") or {}) or (_load_json(run_folder / "summary.json") or {})


def _parse_run_id_from_name(run_folder: Path) -> str | None:
    parts = run_folder.name.split("_")
    return parts[-1] if parts else None


def _case_dirs(run_folder: Path, case_id: str) -> list[Path]:
    cases_root = run_folder / "cases"
    if not cases_root.exists():
        return []
    return sorted(cases_root.glob(f"{case_id}_*"))


def _pick_latest(paths: Iterable[Path]) -> Optional[Path]:
    candidates = list(paths)
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _is_missed_case(case_dir: Path) -> bool:
    status = _load_json(case_dir / "status.json") or {}
    status_value = str(status.get("status") or "").lower()
    if status_value in {"missed"}:
        return True
    if status_value and status_value != "skipped":
        return False
    reason = str(status.get("reason") or "").lower()
    if "missed" in reason or "missing" in reason:
        return True
    return bool(status.get("missed"))


def _resolve_runs_root(args: argparse.Namespace) -> Path:
    if args.runs_dir:
        return args.runs_dir
    if args.data:
        return args.data / ".runs" / "runs"
    return Path(".runs") / "runs"


def _resolve_run_folder(args: argparse.Namespace, runs_root: Path) -> Path:
    if args.run_id:
        for run_folder in _iter_run_folders(runs_root):
            meta = _load_run_meta(run_folder)
            run_id = meta.get("run_id") or _parse_run_id_from_name(run_folder)
            if run_id == args.run_id:
                return run_folder
        raise SystemExit(f"run_id={args.run_id!r} not found in {runs_root}")

    tag = args.tag
    candidates = []
    for run_folder in _iter_run_folders(runs_root):
        meta = _load_run_meta(run_folder)
        if tag:
            if meta.get("tag") != tag:
                continue
        case_dirs = _case_dirs(run_folder, args.case)
        if not case_dirs:
            continue
        latest_case = _pick_latest(case_dirs)
        if latest_case and _is_missed_case(latest_case):
            continue
        candidates.append(run_folder)

    latest = _pick_latest(candidates)
    if latest is None:
        raise SystemExit(f"No runs found for case={args.case!r} (tag={tag!r}) in {runs_root}")
    return latest


def _find_case_run_dir(run_folder: Path, case_id: str) -> Path:
    latest = _pick_latest(_case_dirs(run_folder, case_id))
    if latest is None:
        raise SystemExit(f"No case run dir found for case={case_id!r} in {run_folder}")
    return latest


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    runs_root = _resolve_runs_root(args)
    run_folder = _resolve_run_folder(args, runs_root)
    case_run_dir = _find_case_run_dir(run_folder, args.case)
    events_path = case_run_dir / "events.jsonl"
    if not events_path.exists():
        raise SystemExit(f"events.jsonl not found at {events_path}")

    if args.all:
        export_replay_fixtures(
            events_path=events_path,
            run_dir=case_run_dir,
            out_dir=args.out_dir,
            replay_id=args.id,
            spec_idx=args.spec_idx,
            provider=args.provider,
            with_requires=args.with_requires,
            source_extra={
                "case_id": args.case,
                "tag": args.tag,
                "picked": "run_id" if args.run_id else "latest_non_missed",
            },
        )
    else:
        export_replay_fixture(
            events_path=events_path,
            run_dir=case_run_dir,
            out_dir=args.out_dir,
            replay_id=args.id,
            spec_idx=args.spec_idx,
            provider=args.provider,
            with_requires=args.with_requires,
            source_extra={
                "case_id": args.case,
                "tag": args.tag,
                "picked": "run_id" if args.run_id else "latest_non_missed",
            },
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
