from __future__ import annotations

import json
from pathlib import Path
from typing import NamedTuple, Optional


class LatestMarkers(NamedTuple):
    complete: Path
    results: Path
    any_run: Path
    legacy_run: Path


def _sanitize_tag(tag: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in tag)
    return cleaned or "tag"


def _effective_paths(artifacts_dir: Path, tag: str) -> tuple[Path, Path]:
    base = artifacts_dir / "runs" / "tags" / _sanitize_tag(tag)
    return base / "effective_results.jsonl", base / "effective_meta.json"


def _latest_markers(artifacts_dir: Path, tag: str | None) -> LatestMarkers:
    runs_dir = artifacts_dir / "runs"
    if tag:
        slug = _sanitize_tag(tag)
        return LatestMarkers(
            runs_dir / f"tag-latest-complete-{slug}.txt",
            runs_dir / f"tag-latest-results-{slug}.txt",
            runs_dir / f"tag-latest-any-{slug}.txt",
            runs_dir / f"tag-latest-{slug}.txt",
        )
    return LatestMarkers(
        runs_dir / "latest_complete.txt",
        runs_dir / "latest_results.txt",
        runs_dir / "latest_any.txt",
        runs_dir / "latest.txt",
    )


def _read_marker(path: Path) -> Optional[Path]:
    if path.exists():
        content = path.read_text(encoding="utf-8").strip()
        if content:
            return Path(content)
    return None


def _load_latest_run(artifacts_dir: Path, tag: str | None = None, *, kind: str = "complete") -> Optional[Path]:
    markers = _latest_markers(artifacts_dir, tag)
    candidates: list[Path] = []
    if kind == "any":
        candidates.append(markers.any_run)
    candidates.append(markers.complete)
    candidates.append(markers.legacy_run)
    for marker in candidates:
        resolved = _read_marker(marker)
        if resolved:
            return resolved
    return None


def _resolve_results_path_for_run(run_path: Path | None) -> Optional[Path]:
    if run_path is None:
        return None
    summary_path = run_path / "summary.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            results_path = summary.get("results_path")
            if results_path:
                return Path(results_path)
        except Exception:
            pass
    candidate = run_path / "results.jsonl"
    if candidate.exists():
        return candidate
    return None


def _load_latest_results(artifacts_dir: Path, tag: str | None = None) -> Optional[Path]:
    markers = _latest_markers(artifacts_dir, tag)
    resolved = _read_marker(markers.results)
    if resolved:
        return resolved
    latest_run = _load_latest_run(artifacts_dir, tag, kind="complete")
    return _resolve_results_path_for_run(latest_run)


def _load_latest_any_results(artifacts_dir: Path, tag: str | None = None) -> Optional[Path]:
    latest_run = _load_latest_run(artifacts_dir, tag, kind="any")
    if latest_run is None:
        return None
    results = _resolve_results_path_for_run(latest_run)
    if results:
        return results
    markers = _latest_markers(artifacts_dir, tag)
    return _read_marker(markers.results)


def _load_run_meta(run_path: Path | None) -> Optional[dict]:
    if run_path is None:
        return None
    meta_path = run_path / "run_meta.json"
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _run_dir_from_results_path(results_path: Path | None) -> Optional[Path]:
    if results_path is None:
        return None
    run_dir = results_path.parent
    summary_path = run_dir / "summary.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            run_dir_from_summary = summary.get("run_dir")
            if run_dir_from_summary:
                return Path(run_dir_from_summary)
        except Exception:
            pass
    return run_dir


def _update_latest_markers(
    run_folder: Path, results_path: Path, artifacts_dir: Path, tag: str | None, *, results_complete: bool
) -> None:
    marker_sets = {_latest_markers(artifacts_dir, None)}
    if tag:
        marker_sets.add(_latest_markers(artifacts_dir, tag))
    for markers in marker_sets:
        markers.complete.parent.mkdir(parents=True, exist_ok=True)
        markers.any_run.write_text(str(run_folder), encoding="utf-8")
        markers.legacy_run.write_text(str(run_folder), encoding="utf-8")
        if results_complete:
            markers.complete.write_text(str(run_folder), encoding="utf-8")
            markers.results.write_text(str(results_path), encoding="utf-8")


__all__ = [
    "LatestMarkers",
    "_effective_paths",
    "_load_latest_any_results",
    "_latest_markers",
    "_load_latest_results",
    "_load_latest_run",
    "_load_run_meta",
    "_resolve_results_path_for_run",
    "_run_dir_from_results_path",
    "_sanitize_tag",
    "_update_latest_markers",
]
