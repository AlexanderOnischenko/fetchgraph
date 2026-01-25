from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class CaseResolution:
    run_dir: Path
    case_dir: Path
    events_path: Path
    tag: str | None


def resolve_case_events(
    *,
    case_id: str,
    data_dir: Path,
    tag: str | None = None,
    runs_subdir: str = ".runs/runs",
    pick_run: str = "latest_non_missed",
) -> CaseResolution:
    if not case_id:
        raise ValueError("case_id is required")
    if not data_dir:
        raise ValueError("data_dir is required")
    if pick_run != "latest_non_missed":
        raise ValueError(f"Unsupported pick_run mode: {pick_run}")

    runs_root = (data_dir / runs_subdir).resolve()
    if not runs_root.exists():
        raise FileNotFoundError(f"Runs directory does not exist: {runs_root}")

    run_dirs = list(_iter_run_dirs(runs_root))
    inspected_runs = 0
    missing_cases = 0
    missed_cases = 0
    tag_mismatches = 0

    for run_dir in run_dirs:
        inspected_runs += 1
        run_tag = _extract_run_tag(run_dir)
        if tag and run_tag != tag:
            tag_mismatches += 1
            continue

        case_dirs = _case_dirs(run_dir, case_id)
        if not case_dirs:
            missing_cases += 1
            continue

        for case_dir in case_dirs:
            if _case_is_missed(case_dir):
                missed_cases += 1
                continue
            events_path = case_dir / "events.jsonl"
            if not events_path.exists():
                raise FileNotFoundError(f"events.jsonl not found at {events_path}")
            return CaseResolution(
                run_dir=run_dir,
                case_dir=case_dir,
                events_path=events_path,
                tag=run_tag,
            )

    details = [
        "No suitable case run found.",
        f"runs_root: {runs_root}",
        f"case_id: {case_id}",
        f"inspected_runs: {inspected_runs}",
        f"missing_cases: {missing_cases}",
        f"missed_cases: {missed_cases}",
    ]
    if tag:
        details.append(f"tag: {tag}")
        details.append(f"tag_mismatches: {tag_mismatches}")
    raise LookupError("\n".join(details))


def _iter_run_dirs(runs_root: Path) -> Iterable[Path]:
    candidates = [p for p in runs_root.iterdir() if p.is_dir()]
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)


def _case_dirs(run_dir: Path, case_id: str) -> list[Path]:
    cases_root = run_dir / "cases"
    if not cases_root.exists():
        return []
    return sorted(cases_root.glob(f"{case_id}_*"), key=lambda p: p.stat().st_mtime, reverse=True)


def _extract_run_tag(run_dir: Path) -> str | None:
    for name in ("run_meta.json", "meta.json", "summary.json"):
        path = run_dir / name
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        tag = _extract_tag_value(payload)
        if tag:
            return tag
    return None


def _extract_tag_value(payload: dict) -> str | None:
    for key in ("tag", "TAG"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    tags_value = payload.get("tags")
    if isinstance(tags_value, list) and tags_value:
        for entry in tags_value:
            if isinstance(entry, str) and entry:
                return entry
    return None


def _case_is_missed(case_dir: Path) -> bool:
    for name in ("status.json", "result.json"):
        path = case_dir / name
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if _payload_is_missed(payload):
            return True
        if _payload_is_non_missed(payload):
            return False
    return False


def _payload_is_missed(payload: dict) -> bool:
    if payload.get("missed") is True:
        return True
    status = str(payload.get("status") or payload.get("result") or "").lower()
    if status in {"missed", "missing"}:
        return True
    reason = str(payload.get("reason") or "").lower()
    return "missed" in reason or "missing" in reason


def _payload_is_non_missed(payload: dict) -> bool:
    status = str(payload.get("status") or payload.get("result") or "").lower()
    return status in {"ok", "pass", "passed", "success"}
