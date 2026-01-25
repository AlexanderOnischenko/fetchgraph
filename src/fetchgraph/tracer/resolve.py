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


@dataclass(frozen=True)
class CaseRunCandidate:
    run_dir: Path
    case_dir: Path
    tag: str | None
    mtime: float


@dataclass(frozen=True)
class EventsResolution:
    events_path: Path | None
    searched: list[str]
    found: list[Path]


def resolve_case_events(
    *,
    case_id: str,
    data_dir: Path,
    tag: str | None = None,
    runs_subdir: str = ".runs/runs",
    pick_run: str = "latest_non_missed",
    select_index: int | None = None,
) -> CaseResolution:
    if not case_id:
        raise ValueError("case_id is required")
    if not data_dir:
        raise ValueError("data_dir is required")
    if pick_run != "latest_non_missed":
        raise ValueError(f"Unsupported pick_run mode: {pick_run}")

    candidates, stats = list_case_runs(
        case_id=case_id,
        data_dir=data_dir,
        tag=tag,
        runs_subdir=runs_subdir,
    )
    if not candidates:
        details = [
            "No suitable case run found.",
            f"runs_root: {stats.runs_root}",
            f"case_id: {case_id}",
            f"inspected_runs: {stats.inspected_runs}",
            f"missing_cases: {stats.missing_cases}",
            f"missed_cases: {stats.missed_cases}",
        ]
        if tag:
            details.append(f"tag: {tag}")
            details.append(f"tag_mismatches: {stats.tag_mismatches}")
        raise LookupError("\n".join(details))

    candidate = select_case_run(candidates, select_index=select_index)
    events = find_events_file(candidate.case_dir)
    return CaseResolution(
        run_dir=candidate.run_dir,
        case_dir=candidate.case_dir,
        events_path=events.events_path,
        tag=candidate.tag,
    )


def _iter_run_dirs(runs_root: Path) -> Iterable[Path]:
    candidates = [p for p in runs_root.iterdir() if p.is_dir()]
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)


def _case_dirs(run_dir: Path, case_id: str) -> list[Path]:
    cases_root = run_dir / "cases"
    if not cases_root.exists():
        return []
    return sorted(cases_root.glob(f"{case_id}_*"), key=lambda p: p.stat().st_mtime, reverse=True)


@dataclass(frozen=True)
class RunScanStats:
    runs_root: Path
    inspected_runs: int
    missing_cases: int
    missed_cases: int
    tag_mismatches: int


def list_case_runs(
    *,
    case_id: str,
    data_dir: Path,
    tag: str | None = None,
    runs_subdir: str = ".runs/runs",
) -> tuple[list[CaseRunCandidate], RunScanStats]:
    runs_root = (data_dir / runs_subdir).resolve()
    if not runs_root.exists():
        raise FileNotFoundError(f"Runs directory does not exist: {runs_root}")

    candidates: list[CaseRunCandidate] = []
    inspected_runs = 0
    missing_cases = 0
    missed_cases = 0
    tag_mismatches = 0

    for run_dir in _iter_run_dirs(runs_root):
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
            candidates.append(
                CaseRunCandidate(
                    run_dir=run_dir,
                    case_dir=case_dir,
                    tag=run_tag,
                    mtime=case_dir.stat().st_mtime,
                )
            )

    candidates.sort(key=lambda candidate: candidate.mtime, reverse=True)
    stats = RunScanStats(
        runs_root=runs_root,
        inspected_runs=inspected_runs,
        missing_cases=missing_cases,
        missed_cases=missed_cases,
        tag_mismatches=tag_mismatches,
    )
    return candidates, stats


def select_case_run(candidates: list[CaseRunCandidate], *, select_index: int | None = None) -> CaseRunCandidate:
    if not candidates:
        raise LookupError("No case run candidates available.")
    if select_index is None:
        return candidates[0]
    if select_index < 1 or select_index > len(candidates):
        raise ValueError(f"select_index must be between 1 and {len(candidates)}")
    return candidates[select_index - 1]


def format_case_runs(candidates: list[CaseRunCandidate], *, limit: int | None = 10) -> str:
    rows = []
    for idx, candidate in enumerate(candidates[:limit], start=1):
        rows.append(
            "  "
            f"{idx}. run_dir={candidate.run_dir} "
            f"case_dir={candidate.case_dir.name} "
            f"tag={candidate.tag!r} "
            f"mtime={candidate.mtime:.0f}"
        )
    if len(candidates) > (limit or 0):
        rows.append(f"  ... ({len(candidates) - (limit or 0)} more)")
    return "\n".join(rows)


_EVENTS_CANDIDATES = (
    "events.jsonl",
    "events.ndjson",
    "trace.jsonl",
    "trace.ndjson",
    "traces/events.jsonl",
    "traces/trace.jsonl",
)


def find_events_file(run_dir: Path) -> EventsResolution:
    searched = list(_EVENTS_CANDIDATES)
    found: list[Path] = []
    for rel in _EVENTS_CANDIDATES:
        candidate = run_dir / rel
        if candidate.exists():
            return EventsResolution(events_path=candidate, searched=searched, found=found)

    candidates = []
    for path in run_dir.rglob("*"):
        if path.is_dir():
            continue
        if path.suffix not in {".jsonl", ".ndjson"}:
            continue
        rel_parts = path.relative_to(run_dir).parts
        if "resources" in rel_parts:
            continue
        if len(rel_parts) > 3:
            continue
        candidates.append(path)
    if candidates:
        candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
        found = candidates
        return EventsResolution(events_path=candidates[0], searched=searched, found=found)

    return EventsResolution(events_path=None, searched=searched, found=found)


def format_events_search(run_dir: Path, resolution: EventsResolution) -> str:
    found_list = [str(path) for path in resolution.found]
    return "\n".join(
        [
            f"events file not found in {run_dir}.",
            f"Looked for: {', '.join(resolution.searched)}",
            f"Found jsonl/ndjson: {found_list}",
            "You can pass --events explicitly.",
        ]
    )


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
