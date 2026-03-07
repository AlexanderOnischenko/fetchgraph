from __future__ import annotations

import json
import re
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Optional

from .runner import RunResult, load_results
from .runs.layout import _effective_paths, _load_run_meta


@dataclass
class ResolvedCompareInput:
    input_value: str
    kind: str
    source_description: str
    results: dict[str, RunResult]
    run_dir: Path | None = None
    run_id: str | None = None
    tag: str | None = None


def _runs_root(data_dir: Path) -> Path:
    return data_dir / ".runs" / "runs"


def _iter_run_dirs(runs_root: Path) -> Iterable[Path]:
    if not runs_root.exists():
        return []
    return sorted((p for p in runs_root.iterdir() if p.is_dir()), key=lambda p: p.name)


def _load_results_from_status(status_path: Path, *, run_dir: Path, run_meta: Mapping[str, object]) -> RunResult:
    payload: dict[str, object] = {}
    try:
        data = json.loads(status_path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            payload = data
    except Exception:
        payload = {}
    case_dir = status_path.parent
    case_id = str(payload.get("id") or case_dir.name)
    return RunResult(
        id=case_id,
        question=str(payload.get("question") or ""),
        status=str(payload.get("status") or "error"),
        checked=bool(payload.get("checked", False)),
        reason=(str(payload.get("reason")) if payload.get("reason") is not None else None),
        details=payload.get("details") if isinstance(payload.get("details"), dict) else None,
        artifacts_dir=str(payload.get("artifacts_dir") or case_dir),
        duration_ms=int(payload.get("duration_ms", 0) or 0),
        tags=[str(v) for v in payload.get("tags", []) if isinstance(v, (str, int, float))]
        if isinstance(payload.get("tags"), list)
        else ([str(run_meta.get("tag"))] if run_meta.get("tag") else []),
        answer=str(payload.get("answer")) if payload.get("answer") is not None else None,
        error=str(payload.get("error")) if payload.get("error") is not None else None,
        plan_path=str(payload.get("plan_path")) if payload.get("plan_path") is not None else None,
    )


def load_results_for_run_dir(run_dir: Path) -> tuple[dict[str, RunResult], str, dict]:
    results_file = run_dir / "results.jsonl"
    run_meta = _load_run_meta(run_dir) or {}
    if results_file.exists():
        return load_results(results_file), "results.jsonl", run_meta
    results: dict[str, RunResult] = {}
    for status_path in sorted(run_dir.glob("cases/**/status.json")):
        row = _load_results_from_status(status_path, run_dir=run_dir, run_meta=run_meta)
        results[row.id] = row
    return results, "cases/**/status.json", run_meta




def _as_iso_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _run_dir_name_timestamp(run_dir: Path) -> datetime | None:
    match = re.match(r"^(\d{8})_(\d{6})", run_dir.name)
    if not match:
        return None
    try:
        return datetime.strptime(f"{match.group(1)}{match.group(2)}", "%Y%m%d%H%M%S")
    except ValueError:
        return None


def _run_sort_key(run_dir: Path, run_meta: Mapping[str, object]) -> tuple[int, float, float, str]:
    for key in ["finished_at", "ended_at", "timestamp", "started_at"]:
        ts = _as_iso_timestamp(run_meta.get(key))
        if ts is not None:
            return (3, ts.timestamp(), run_dir.stat().st_mtime, run_dir.name)
    name_ts = _run_dir_name_timestamp(run_dir)
    if name_ts is not None:
        return (2, name_ts.timestamp(), run_dir.stat().st_mtime, run_dir.name)
    return (1, run_dir.stat().st_mtime, run_dir.stat().st_mtime, run_dir.name)

def _latest_run_for_tag(data_dir: Path, tag: str | None) -> Optional[Path]:
    runs: list[tuple[tuple[int, float, float, str], Path]] = []
    for run_dir in _iter_run_dirs(_runs_root(data_dir)):
        meta = _load_run_meta(run_dir) or {}
        if tag is not None and str(meta.get("tag") or "") != tag:
            continue
        runs.append((_run_sort_key(run_dir, meta), run_dir))
    if not runs:
        return None
    runs.sort(key=lambda item: item[0])
    return runs[-1][1]


def _looks_like_run_id(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{4,64}", value))


def _resolve_run_id(data_dir: Path, run_id: str) -> Path:
    matched: list[Path] = []
    for run_dir in _iter_run_dirs(_runs_root(data_dir)):
        meta = _load_run_meta(run_dir) or {}
        if str(meta.get("run_id") or "") == run_id or run_dir.name.endswith(run_id):
            matched.append(run_dir)
    if not matched:
        raise ValueError(f"compare: run_id={run_id} not found under {_runs_root(data_dir)}")
    if len(matched) > 1:
        details = "\n".join(f"  {p}" for p in matched)
        raise ValueError(f"compare: run_id={run_id} resolved to multiple run directories:\n{details}")
    return matched[0]


def _resolve_effective_tag(data_dir: Path, tag: str) -> Path:
    results_path, meta_path = _effective_paths(data_dir / ".runs", tag)
    if not results_path.exists() or not meta_path.exists():
        raise ValueError(f"compare: tag={tag} has no effective snapshot")
    return results_path


def resolve_compare_input(ref: str, *, data_dir: Path | None) -> ResolvedCompareInput:
    ref_path = Path(ref)

    if ref_path.exists() and ref_path.is_file():
        results = load_results(ref_path)
        return ResolvedCompareInput(ref, "results_file", "results file", results, run_dir=ref_path.parent)

    if ref_path.exists() and ref_path.is_dir():
        results, source, run_meta = load_results_for_run_dir(ref_path)
        return ResolvedCompareInput(
            ref,
            "run_dir",
            source,
            results,
            run_dir=ref_path,
            run_id=str(run_meta.get("run_id")) if run_meta.get("run_id") else None,
            tag=str(run_meta.get("tag")) if run_meta.get("tag") else None,
        )

    if ref == "latest":
        if data_dir is None:
            raise ValueError("compare: latest requires --data")
        run_dir = _latest_run_for_tag(data_dir, None)
        if run_dir is None:
            raise ValueError(f"compare: no runs found under {_runs_root(data_dir)}")
        results, source, run_meta = load_results_for_run_dir(run_dir)
        return ResolvedCompareInput(ref, "latest", source, results, run_dir=run_dir, run_id=str(run_meta.get("run_id") or ""))

    if ref.startswith("latest:"):
        if data_dir is None:
            raise ValueError("compare: latest:<tag> requires --data")
        tag = ref.split(":", 1)[1]
        run_dir = _latest_run_for_tag(data_dir, tag)
        if run_dir is None:
            raise ValueError(f"compare: no runs found with run_meta.tag == {tag}")
        results, source, run_meta = load_results_for_run_dir(run_dir)
        return ResolvedCompareInput(ref, "latest_tag_run", source, results, run_dir=run_dir, run_id=str(run_meta.get("run_id") or ""), tag=tag)

    if ref.startswith("tag:"):
        if data_dir is None:
            raise ValueError("compare: tag:<tag> requires --data")
        tag = ref.split(":", 1)[1]
        results_path = _resolve_effective_tag(data_dir, tag)
        results = load_results(results_path)
        return ResolvedCompareInput(ref, "tag", "effective snapshot", results, run_dir=results_path.parent, tag=tag)

    if ":" in ref:
        raise ValueError(
            'compare: unsupported ref "{}"\nSupported forms:\n  /path/to/results.jsonl\n  /path/to/run_dir\n  <run_id>\n  latest\n  tag:<tag>\n  latest:<tag>'.format(ref)
        )

    if _looks_like_run_id(ref):
        if data_dir is None:
            raise ValueError("compare: run_id resolution requires --data")
        run_dir = _resolve_run_id(data_dir, ref)
        results, source, run_meta = load_results_for_run_dir(run_dir)
        return ResolvedCompareInput(ref, "run_id", source, results, run_dir=run_dir, run_id=str(run_meta.get("run_id") or ref), tag=str(run_meta.get("tag")) if run_meta.get("tag") else None)

    raise ValueError(
        'compare: unsupported ref "{}"\nSupported forms:\n  /path/to/results.jsonl\n  /path/to/run_dir\n  <run_id>\n  latest\n  tag:<tag>\n  latest:<tag>'.format(ref)
    )


def resolved_lines(label: str, resolved: ResolvedCompareInput) -> list[str]:
    lines = [
        f"Resolved {label}:",
        f"  input: {resolved.input_value}",
        f"  kind: {resolved.kind}",
    ]
    if resolved.tag:
        lines.append(f"  tag: {resolved.tag}")
    if resolved.run_id:
        lines.append(f"  run_id: {resolved.run_id}")
    if resolved.run_dir:
        lines.append(f"  run_dir: {resolved.run_dir}")
    lines.append(f"  source: {resolved.source_description}")
    return lines
