from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from fetchgraph.replay.export import iter_events


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
    events_path: Path
    tag: str | None
    tag_source: str | None
    status: str | None
    is_missed: bool
    run_mtime: float
    case_mtime: float


@dataclass(frozen=True)
class CaseRunInfo:
    run_dir: Path
    case_dir: Path
    events: "EventsResolution"
    tag: str | None
    tag_source: str | None
    status: str | None
    is_missed: bool
    run_mtime: float
    case_mtime: float


@dataclass(frozen=True)
class EventsResolution:
    events_path: Path | None
    searched: list[str]
    found: list[Path]


@dataclass(frozen=True)
class RejectionReason:
    run_dir: Path
    case_dir: Path
    reason: str


def resolve_case_events(
    *,
    case_id: str,
    data_dir: Path,
    tag: str | None = None,
    runs_subdir: str = ".runs/runs",
    pick_run: str = "latest_non_missed",
    select_index: int | None = None,
    replay_id: str | None = None,
) -> CaseResolution:
    if not case_id:
        raise ValueError("case_id is required")
    if not data_dir:
        raise ValueError("data_dir is required")
    if pick_run not in {"latest_non_missed", "latest_with_replay"}:
        raise ValueError(f"Unsupported pick_run mode: {pick_run}")
    if pick_run == "latest_with_replay" and not replay_id:
        raise ValueError("replay_id is required for latest_with_replay selection")

    candidates, stats = list_case_runs(
        case_id=case_id,
        data_dir=data_dir,
        tag=tag,
        pick_run=pick_run,
        replay_id=replay_id,
        runs_subdir=runs_subdir,
    )
    if not candidates:
        details = _format_missing_case_runs(
            stats,
            case_id=case_id,
            tag=tag,
            rule=_resolve_rule(tag=tag, pick_run=pick_run),
        )
        raise LookupError(details)

    candidate = select_case_run(candidates, select_index=select_index)
    return CaseResolution(
        run_dir=candidate.run_dir,
        case_dir=candidate.case_dir,
        events_path=candidate.events_path,
        tag=candidate.tag,
    )


def resolve_run_dir_from_run_id(*, data_dir: Path, runs_subdir: str, run_id: str) -> Path:
    if not run_id:
        raise ValueError("run_id is required")
    if not data_dir:
        raise ValueError("data_dir is required")
    runs_root = (data_dir / runs_subdir).resolve()
    history_path = data_dir / ".runs" / "history.jsonl"
    history_candidates: list[Path] = []
    if history_path.exists():
        entries: list[dict] = []
        with history_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        for entry in reversed(entries):
            if str(entry.get("run_id")) != run_id:
                continue
            run_dir_value = entry.get("run_dir")
            run_folder_value = entry.get("run_folder")
            if not run_dir_value and not run_folder_value:
                continue
            candidate: Path | None = None
            if run_dir_value:
                candidate = Path(run_dir_value)
                if not candidate.is_absolute():
                    candidate = runs_root / candidate
            if candidate is None and run_folder_value:
                candidate = runs_root / str(run_folder_value)
            if candidate is None:
                continue
            if candidate.exists():
                return candidate
            history_candidates.append(candidate)

    if not runs_root.exists():
        raise FileNotFoundError(f"Runs directory does not exist: {runs_root}")
    matched: list[Path] = []
    for run_dir in _iter_run_dirs(runs_root):
        meta_path = run_dir / "run_meta.json"
        if not meta_path.exists():
            continue
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if str(payload.get("run_id")) != run_id:
            continue
        matched.append(run_dir)
    if matched:
        matched.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return matched[0]

    hint = ""
    if history_candidates:
        hint = f"History entries pointed to: {', '.join(str(p) for p in history_candidates)}"
    raise LookupError(
        "Run id could not be resolved.\n"
        f"run_id: {run_id}\n"
        f"history_path: {history_path}\n"
        f"runs_root: {runs_root}\n"
        f"{hint}".rstrip()
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
    inspected_cases: int
    missing_cases: int
    missing_events: int
    missed_cases: int
    tag_mismatches: int
    recent: list[CaseRunInfo]


def list_case_runs(
    *,
    case_id: str,
    data_dir: Path,
    tag: str | None = None,
    pick_run: str = "latest_non_missed",
    replay_id: str | None = None,
    runs_subdir: str = ".runs/runs",
) -> tuple[list[CaseRunCandidate], RunScanStats]:
    infos, stats = scan_case_runs(
        case_id=case_id,
        data_dir=data_dir,
        tag=tag,
        runs_subdir=runs_subdir,
    )
    candidates = _filter_case_run_infos(infos, tag=tag, pick_run=pick_run, replay_id=replay_id)
    return candidates, stats


def scan_case_runs(
    *,
    case_id: str,
    data_dir: Path,
    tag: str | None = None,
    runs_subdir: str = ".runs/runs",
) -> tuple[list[CaseRunInfo], RunScanStats]:
    runs_root = (data_dir / runs_subdir).resolve()
    if not runs_root.exists():
        raise FileNotFoundError(f"Runs directory does not exist: {runs_root}")

    infos: list[CaseRunInfo] = []
    inspected_runs = 0
    inspected_cases = 0
    missing_cases = 0
    missing_events = 0
    missed_cases = 0

    for run_dir in _iter_run_dirs(runs_root):
        inspected_runs += 1
        case_dirs = _case_dirs(run_dir, case_id)
        if not case_dirs:
            missing_cases += 1
            continue
        run_mtime = run_dir.stat().st_mtime
        for case_dir in case_dirs:
            inspected_cases += 1
            events = find_events_file(case_dir)
            if events.events_path is None:
                missing_events += 1
            case_mtime = case_dir.stat().st_mtime
            status_value, is_missed = _case_status(case_dir)
            if is_missed:
                missed_cases += 1
            tag_value, tag_source = _extract_case_tag(case_dir)
            infos.append(
                CaseRunInfo(
                    run_dir=run_dir,
                    case_dir=case_dir,
                    events=events,
                    tag=tag_value,
                    tag_source=tag_source,
                    status=status_value,
                    is_missed=is_missed,
                    run_mtime=run_mtime,
                    case_mtime=case_mtime,
                )
            )

    infos.sort(key=lambda info: (info.run_mtime, info.case_mtime), reverse=True)
    stats = RunScanStats(
        runs_root=runs_root,
        inspected_runs=inspected_runs,
        inspected_cases=inspected_cases,
        missing_cases=missing_cases,
        missing_events=missing_events,
        missed_cases=missed_cases,
        tag_mismatches=_count_tag_mismatches(infos, tag=tag),
        recent=infos[:10],
    )
    return infos, stats


def _filter_case_run_infos(
    infos: list[CaseRunInfo],
    *,
    tag: str | None = None,
    pick_run: str = "latest_non_missed",
    replay_id: str | None = None,
) -> list[CaseRunCandidate]:
    candidates: list[CaseRunCandidate] = []
    for info in infos:
        if info.events.events_path is None:
            continue
        if pick_run == "latest_non_missed" and info.is_missed:
            continue
        if pick_run == "latest_with_replay":
            if replay_id is None:
                raise ValueError("replay_id is required for latest_with_replay selection")
            if not _has_replay_case_id(info.events.events_path, replay_id):
                continue
        if tag and info.tag != tag:
            continue
        candidates.append(
            CaseRunCandidate(
                run_dir=info.run_dir,
                case_dir=info.case_dir,
                events_path=info.events.events_path,
                tag=info.tag,
                tag_source=info.tag_source,
                status=info.status,
                is_missed=info.is_missed,
                run_mtime=info.run_mtime,
                case_mtime=info.case_mtime,
            )
        )
    return candidates


def _count_tag_mismatches(infos: list[CaseRunInfo], tag: str | None) -> int:
    if not tag:
        return 0
    mismatches = 0
    for info in infos:
        if info.tag != tag:
            mismatches += 1
    return mismatches


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
            f"status={candidate.status!r} "
            f"missed={candidate.is_missed} "
            f"run_mtime={candidate.run_mtime:.0f} "
            f"case_mtime={candidate.case_mtime:.0f}"
        )
    if len(candidates) > (limit or 0):
        rows.append(f"  ... ({len(candidates) - (limit or 0)} more)")
    return "\n".join(rows)


def format_case_run_debug(infos: list[CaseRunInfo], *, limit: int = 10) -> str:
    rows = []
    for idx, info in enumerate(infos[:limit], start=1):
        rows.append(
            "  "
            f"{idx}. run_dir={info.run_dir} "
            f"case_dir={info.case_dir.name} "
            f"tag={info.tag!r} "
            f"tag_source={info.tag_source!r} "
            f"status={info.status!r} "
            f"missed={info.is_missed} "
            f"events={bool(info.events.events_path)} "
            f"run_mtime={info.run_mtime:.0f} "
            f"case_mtime={info.case_mtime:.0f}"
        )
    if len(infos) > limit:
        rows.append(f"  ... ({len(infos) - limit} more)")
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
    searched = [str(entry) for entry in _EVENTS_CANDIDATES]
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


def _extract_case_tag(case_dir: Path) -> tuple[str | None, str | None]:
    run_dir = case_dir.parent.parent
    for name in ("run_meta.json", "meta.json"):
        tag = _extract_tag_from_json(run_dir / name)
        if tag:
            return tag, name
    tag = _extract_tag_from_json(run_dir / "summary.json")
    if tag:
        return tag, "summary.json"
    for name in ("status.json", "result.json"):
        tag = _extract_tag_from_json(case_dir / name)
        if tag:
            return tag, name
    tag = _tag_from_run_dir_name(run_dir.name)
    if tag:
        return tag, "run_dir"
    events = find_events_file(case_dir)
    if events.events_path:
        tag = _extract_tag_from_events(events.events_path)
        if tag:
            return tag, "events"
    return None, None


def _extract_tag_from_json(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    tag = _extract_tag_value(payload)
    if tag:
        return tag
    for meta_key in ("run_meta", "meta"):
        nested = payload.get(meta_key)
        if isinstance(nested, dict):
            tag = _extract_tag_value(nested)
            if tag:
                return tag
    return None


def _extract_tag_from_events(path: Path, *, max_lines: int = 200) -> str | None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for idx, line in enumerate(handle, start=1):
                if idx > max_lines:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    tag = _extract_tag_value(payload)
                    if tag:
                        return tag
    except OSError:
        return None
    return None


def _tag_from_run_dir_name(name: str) -> str | None:
    if not name:
        return None
    lowered = name.lower()
    for prefix in ("tag=", "tag-", "bucket=", "bucket-"):
        if prefix in lowered:
            tail = lowered.split(prefix, 1)[1].strip()
            return tail or None
    return None


def _extract_tag_value(payload: dict) -> str | None:
    for key in ("tag", "TAG", "bucket", "batch_tag", "bucket_tag"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    tags_value = payload.get("tags")
    if isinstance(tags_value, list) and tags_value:
        for entry in tags_value:
            if isinstance(entry, str) and entry:
                return entry
    return None


def _case_status(case_dir: Path) -> tuple[str | None, bool]:
    for name in ("status.json", "result.json"):
        path = case_dir / name
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        status_value = payload.get("status") or payload.get("result")
        status_str = str(status_value) if status_value is not None else None
        is_missed = _payload_is_missed(payload)
        return status_str, is_missed
    return None, False


def _payload_is_missed(payload: dict) -> bool:
    if payload.get("missed") is True:
        return True
    status = str(payload.get("status") or payload.get("result") or "").lower()
    if status in {"missed", "missing"}:
        return True
    reason = str(payload.get("reason") or "").lower()
    return "missed" in reason or "missing" in reason


def _has_replay_case_id(events_path: Path, replay_id: str) -> bool:
    try:
        for _, event in iter_events(events_path, allow_bad_json=True):
            if event.get("type") == "replay_case" and event.get("id") == replay_id:
                return True
    except FileNotFoundError:
        return False
    return False


def collect_rejections(
    infos: list[CaseRunInfo],
    *,
    tag: str | None,
    pick_run: str,
    replay_id: str | None,
) -> list[RejectionReason]:
    rejections: list[RejectionReason] = []
    for info in infos:
        if tag and info.tag != tag:
            rejections.append(RejectionReason(info.run_dir, info.case_dir, "tag_mismatch"))
            continue
        if info.events.events_path is None:
            rejections.append(RejectionReason(info.run_dir, info.case_dir, "no_events"))
            continue
        if pick_run == "latest_non_missed" and info.is_missed:
            rejections.append(RejectionReason(info.run_dir, info.case_dir, "missed"))
            continue
        if pick_run == "latest_with_replay":
            if replay_id is None or not _has_replay_case_id(info.events.events_path, replay_id):
                rejections.append(RejectionReason(info.run_dir, info.case_dir, "no_replay_id"))
                continue
        rejections.append(RejectionReason(info.run_dir, info.case_dir, "filtered"))
    return rejections


def _resolve_rule(*, tag: str | None, pick_run: str = "latest_non_missed") -> str:
    base = "latest with events"
    if pick_run == "latest_with_replay":
        base = "latest with replay_case"
    if tag:
        return f"{base} filtered by TAG={tag!r}"
    return base


def _format_missing_case_runs(
    stats: RunScanStats,
    *,
    case_id: str,
    tag: str | None,
    rule: str,
) -> str:
    details = [
        "No suitable case run found.",
        f"selection_rule: {rule}",
        f"runs_root: {stats.runs_root}",
        f"case_id: {case_id}",
        f"inspected_runs: {stats.inspected_runs}",
        f"inspected_cases: {stats.inspected_cases}",
        f"missing_cases: {stats.missing_cases}",
        f"missing_events: {stats.missing_events}",
        f"missed_cases: {stats.missed_cases}",
    ]
    if tag:
        details.append(f"tag: {tag}")
        details.append(f"tag_mismatches: {stats.tag_mismatches}")
        if stats.recent:
            details.append("recent_cases:")
            for info in stats.recent[:5]:
                details.append(
                    "  "
                    f"case_dir={info.case_dir} "
                    f"tag={info.tag!r} "
                    f"events={bool(info.events.events_path)} "
                    f"status={info.status!r} "
                    f"missed={info.is_missed}"
                )
        details.append("Tip: verify TAG or pass RUN_ID/CASE_DIR/EVENTS.")
    else:
        details.append("Tip: pass TAG/RUN_ID/CASE_DIR/EVENTS for a narrower selection.")
    return "\n".join(details)
