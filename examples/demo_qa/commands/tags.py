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
from ..term import color, fmt_num, fmt_pct, render_table, should_use_color, truncate


@dataclass
class TagInfo:
    name: str
    updated_at: str
    updated_sort: float
    planned: int | None
    executed: int | None
    missed: int | None
    bad: int | None
    pass_rate: float | None
    note: str
    effective_meta_path: Path


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

    fail_on = meta.get("fail_on", "bad")
    require_assert = bool(meta.get("require_assert", False))
    bad_status = bad_statuses(str(fail_on), require_assert)
    bad_total = sum(counts.get(status, 0) or 0 for status in bad_status) if counts else None

    ok = counts.get("ok", 0) or 0
    skipped = counts.get("skipped", 0) or 0
    total = counts.get("total", 0) or 0
    non_skipped = total - skipped
    pass_rate = (ok / non_skipped) if non_skipped else None

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
    note = meta.get("note") or ""
    if built_from:
        last_path = Path(sorted(str(p) for p in built_from)[-1])
        _, run_note = _load_run_note(last_path)
        note = note or run_note

    return TagInfo(
        name=tag_dir.name,
        updated_at=updated_at or "n/a",
        updated_sort=updated_sort or 0,
        planned=planned if isinstance(planned, int) else None,
        executed=executed if isinstance(executed, int) else None,
        missed=missed if isinstance(missed, int) else None,
        bad=bad_total if isinstance(bad_total, int) else bad_total,
        pass_rate=pass_rate,
        note=note or "",
        effective_meta_path=meta_path,
    )


def _pass_rate_color(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 0.9:
        return "green"
    if value >= 0.6:
        return "yellow"
    return "red"


def _format_table(rows: Iterable[TagInfo], *, use_color: bool) -> str:
    headers = ["tag", "updated_at", "executed/planned", "missed", "bad", "pass_rate", "note"]
    table_rows: list[list[str]] = []
    for row in rows:
        bad_display = fmt_num(row.bad)
        bad_color = "red" if (isinstance(row.bad, (int, float)) and row.bad > 0) else "green" if row.bad == 0 else None
        pass_rate_display = fmt_pct(row.pass_rate)
        pass_rate_color = _pass_rate_color(row.pass_rate)
        executed_display = f"{fmt_num(row.executed)}/{fmt_num(row.planned)}"
        note_display = truncate(row.note, 80)
        table_rows.append(
            [
                row.name,
                truncate(row.updated_at, 25),
                executed_display,
                fmt_num(row.missed),
                color(bad_display, bad_color, use_color=use_color),
                color(pass_rate_display, pass_rate_color, use_color=use_color),
                note_display,
            ]
        )
    return render_table(
        headers,
        table_rows,
        align_right={2, 3, 4, 5},
        col_max={1: 25, 6: 80},
    )


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

    format_mode = getattr(args, "format", "table")
    color_mode = getattr(args, "color", "auto")
    use_color = should_use_color(color_mode, stream=sys.stdout)

    if format_mode == "json":
        payload = [
            {
                "tag": info.name,
                "updated_at": info.updated_at,
                "planned": info.planned,
                "executed": info.executed,
                "missed": info.missed,
                "bad": info.bad,
                "pass_rate": info.pass_rate,
                "note": info.note,
                "effective_meta_path": str(info.effective_meta_path),
            }
            for info in tag_infos
        ]
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print(_format_table(tag_infos, use_color=use_color))
    return 0


__all__ = ["handle_tags_list"]
