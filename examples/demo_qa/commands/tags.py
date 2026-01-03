from __future__ import annotations

import fnmatch
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from ..runner import bad_statuses


@dataclass
class TagInfo:
    name: str
    updated_at: str
    updated_sort: float
    planned: str
    executed: str
    missed: str
    bad: str
    pass_rate: str
    last_run_id: str
    note: str


def _parse_ts(value: str | None) -> Optional[float]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).timestamp()
    except Exception:
        return None


def _format_pct(value: float | None) -> str:
    return f"{value*100:.1f}%" if value is not None else "n/a"


def _match_pattern(name: str, pattern: str | None) -> bool:
    if not pattern:
        return True
    if pattern.startswith("re:"):
        try:
            return re.search(pattern[3:], name) is not None
        except re.error:
            return False
    return fnmatch.fnmatch(name, pattern)


def _load_meta(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_run_note(run_dir: Path) -> tuple[str, str]:
    if not run_dir.exists():
        return "", ""
    summary_path = run_dir / "summary.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            note = summary.get("note") or ""
            run_id = summary.get("run_id") or run_dir.name
            return run_id, note
        except Exception:
            pass
    run_meta = run_dir / "run_meta.json"
    if run_meta.exists():
        try:
            meta = json.loads(run_meta.read_text(encoding="utf-8"))
            note = meta.get("note") or ""
            run_id = meta.get("run_id") or run_dir.name
            return run_id, note
        except Exception:
            pass
    return run_dir.name, ""


def _collect_tag_info(tag_dir: Path) -> Optional[TagInfo]:
    meta_path = tag_dir / "effective_meta.json"
    results_path = tag_dir / "effective_results.jsonl"
    if not meta_path.exists() and not results_path.exists():
        return None
    meta = _load_meta(meta_path) or {}
    counts = meta.get("counts") or {}

    planned = meta.get("planned_total")
    executed = meta.get("executed_total")
    missed = meta.get("missed_total")
    planned_str = str(planned) if planned is not None else "n/a"
    executed_str = str(executed) if executed is not None else "n/a"
    missed_str = str(missed) if missed is not None else "n/a"

    fail_on = meta.get("fail_on", "bad")
    require_assert = bool(meta.get("require_assert", False))
    bad_status = bad_statuses(str(fail_on), require_assert)
    bad_total = sum(counts.get(status, 0) or 0 for status in bad_status)
    bad_str = str(bad_total) if counts else "n/a"

    ok = counts.get("ok", 0) or 0
    skipped = counts.get("skipped", 0) or 0
    total = counts.get("total", 0) or 0
    non_skipped = total - skipped
    pass_rate = (ok / non_skipped) if non_skipped else None
    pass_rate_str = _format_pct(pass_rate)

    updated_at = meta.get("updated_at") or ""
    updated_sort = _parse_ts(updated_at)
    if updated_sort is None:
        updated_sort = meta_path.stat().st_mtime if meta_path.exists() else results_path.stat().st_mtime
        updated_at = (
            datetime.fromtimestamp(updated_sort, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            if updated_sort
            else "n/a"
        )

    built_from = meta.get("built_from_runs") or []
    last_run_id = "n/a"
    note = meta.get("note") or ""
    if built_from:
        last_path = Path(sorted(str(p) for p in built_from)[-1])
        run_id, run_note = _load_run_note(last_path)
        last_run_id = run_id or "n/a"
        note = note or run_note

    return TagInfo(
        name=tag_dir.name,
        updated_at=updated_at or "n/a",
        updated_sort=updated_sort or 0,
        planned=planned_str,
        executed=executed_str,
        missed=missed_str,
        bad=bad_str,
        pass_rate=pass_rate_str,
        last_run_id=last_run_id,
        note=note or "",
    )


def _format_table(rows: Iterable[TagInfo]) -> list[str]:
    headers = ["tag", "updated_at", "plan/exe/miss", "bad", "pass_rate", "last_run_id", "note"]
    data = []
    for row in rows:
        data.append(
            [
                row.name,
                row.updated_at,
                f"{row.planned}/{row.executed}/{row.missed}",
                row.bad,
                row.pass_rate,
                row.last_run_id,
                row.note,
            ]
        )
    widths = [len(h) for h in headers]
    for row in data:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))
    lines = []
    header_line = "  ".join(h.ljust(widths[idx]) for idx, h in enumerate(headers))
    lines.append(header_line)
    for row in data:
        lines.append("  ".join(str(cell).ljust(widths[idx]) for idx, cell in enumerate(row)))
    return lines


def handle_tags_list(args) -> int:
    artifacts_dir = args.data / ".runs"
    tags_dir = artifacts_dir / "runs" / "tags"
    if not tags_dir.exists():
        print("No tags found.")
        return 0

    tag_infos: list[TagInfo] = []
    for child in sorted(tags_dir.iterdir()):
        if not child.is_dir():
            continue
        if not _match_pattern(child.name, args.pattern):
            continue
        info = _collect_tag_info(child)
        if info:
            tag_infos.append(info)

    if not tag_infos:
        print("No tags found.")
        return 0

    sort_by = getattr(args, "sort", "updated")
    if sort_by == "name":
        tag_infos.sort(key=lambda t: t.name)
    else:
        tag_infos.sort(key=lambda t: (-t.updated_sort, t.name))

    limit = getattr(args, "limit", None)
    if limit is not None:
        try:
            limit_int = int(limit)
            if limit_int > 0:
                tag_infos = tag_infos[:limit_int]
        except Exception:
            pass

    for line in _format_table(tag_infos):
        print(line)
    return 0


__all__ = ["handle_tags_list"]
