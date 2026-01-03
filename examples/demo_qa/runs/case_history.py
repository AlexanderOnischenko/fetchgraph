from __future__ import annotations

import datetime
import json
import logging
from pathlib import Path
from typing import Iterable, Mapping, Optional

from ..runner import RunResult
from .layout import _load_run_meta


logger = logging.getLogger(__name__)


def _reason_text(res: RunResult) -> str:
    if res.reason:
        return res.reason
    if res.error:
        return res.error
    expected = getattr(res, "expected_check", None)
    if expected and getattr(expected, "detail", None):
        return expected.detail
    return ""


def _append_case_history(
    artifacts_dir: Path,
    result: RunResult,
    *,
    run_id: str,
    tag: str | None,
    note: str | None,
    fail_on: str,
    require_assert: bool,
    scope_hash: str,
    cases_hash: str,
    git_sha: str | None,
    run_dir: Path,
    results_path: Path,
    run_ts: str | None,
) -> None:
    history_dir = artifacts_dir / "runs" / "cases"
    history_dir.mkdir(parents=True, exist_ok=True)
    ts = run_ts or datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    payload = {
        "timestamp": ts,
        "ts": ts,
        "run_id": run_id,
        "tag": tag,
        "note": note,
        "status": result.status,
        "reason": _reason_text(result),
        "duration_ms": result.duration_ms,
        "artifacts_dir": result.artifacts_dir,
        "run_dir": str(run_dir),
        "results_path": str(results_path),
        "fail_on": fail_on,
        "require_assert": require_assert,
        "scope_hash": scope_hash,
        "cases_hash": cases_hash,
        "git_sha": git_sha,
    }
    target = history_dir / f"{result.id}.jsonl"
    with target.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _load_case_history(path: Path) -> list[dict]:
    if not path.exists():
        return []
    entries: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception:
                continue
    return entries


def _parse_ts(value: object | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            try:
                return float(value)
            except Exception:
                return None
    return None


def _entry_ts(entry: Mapping[str, object], *, run_dir: Path | None) -> tuple[float | None, str | None]:
    ts = _parse_ts(entry.get("ts")) or _parse_ts(entry.get("timestamp"))
    if ts is not None:
        return ts, None
    meta_ts: float | None = None
    if run_dir:
        meta = _load_run_meta(run_dir)
        if isinstance(meta, dict):
            meta_ts = _parse_ts(meta.get("ended_at") or meta.get("timestamp") or meta.get("started_at"))
        if meta_ts is None:
            try:
                meta_ts = run_dir.stat().st_mtime
            except OSError:
                meta_ts = None
    return meta_ts, "history order fallback used"


def _iter_case_entries_newest_first(
    history_path: Path,
    case_id: str,
    tag: str | None,
    scope_hash: str | None,
    *,
    strict_scope: bool,
    fail_on: str,
    require_assert: bool,
    overlay_entry: dict | None,
    max_entries: int,
) -> Iterable[dict]:
    entries = list(_load_case_history(history_path)) if history_path.exists() else []
    overlay_index = None
    if overlay_entry:
        overlay_index = len(entries)
        entries.append(dict(overlay_entry))

    accepted: dict[str, dict] = {}
    ts_map: dict[str, float | None] = {}
    is_overlay_map: dict[str, bool] = {}
    warnings_emitted = False
    for idx, entry in enumerate(entries):
        if tag is not None and entry.get("tag") != tag:
            continue
        entry_scope = entry.get("scope_hash")
        if scope_hash:
            if entry_scope != scope_hash and (strict_scope or entry_scope is not None):
                continue
        run_id = str(entry.get("run_id")) if entry.get("run_id") is not None else None
        if not run_id:
            continue
        run_dir = None
        if entry.get("run_dir"):
            run_dir = Path(str(entry["run_dir"]))
        ts_value, warn = _entry_ts(entry, run_dir=run_dir)
        if ts_value is None and warn:
            warnings_emitted = True
        is_overlay = overlay_entry is not None and idx == overlay_index
        current_ts = ts_map.get(run_id)
        current_is_overlay = is_overlay_map.get(run_id, False)
        candidate_ts = ts_value
        should_replace = False
        if run_id not in accepted:
            should_replace = True
        else:
            if candidate_ts is not None:
                if current_ts is None or candidate_ts > current_ts or (
                    candidate_ts == current_ts and is_overlay and not current_is_overlay
                ):
                    should_replace = True
            else:
                if current_ts is None and is_overlay and not current_is_overlay:
                    should_replace = True
        if should_replace:
            accepted[run_id] = entry
            ts_map[run_id] = candidate_ts
            is_overlay_map[run_id] = is_overlay
    if warnings_emitted:
        logger.warning("ts missing; history order fallback used for case %s", case_id)

    sorted_entries = sorted(accepted.items(), key=lambda kv: ts_map.get(kv[0], 0), reverse=True)
    for _, entry in sorted_entries[:max_entries]:
        yield entry


__all__ = ["_append_case_history", "_iter_case_entries_newest_first", "_load_case_history"]
