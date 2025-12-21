from __future__ import annotations

import json
from pathlib import Path
from typing import Optional


def _sanitize_tag(tag: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in tag)
    return cleaned or "tag"


def _effective_paths(artifacts_dir: Path, tag: str) -> tuple[Path, Path]:
    base = artifacts_dir / "runs" / "tags" / _sanitize_tag(tag)
    return base / "effective_results.jsonl", base / "effective_meta.json"


def _latest_markers(artifacts_dir: Path, tag: str | None) -> tuple[Path, Path]:
    runs_dir = artifacts_dir / "runs"
    if tag:
        slug = _sanitize_tag(tag)
        return runs_dir / f"tag-latest-{slug}.txt", runs_dir / f"tag-latest-results-{slug}.txt"
    return runs_dir / "latest.txt", runs_dir / "latest_results.txt"


def _load_latest_run(artifacts_dir: Path, tag: str | None = None) -> Optional[Path]:
    latest_file, _ = _latest_markers(artifacts_dir, tag)
    if latest_file.exists():
        content = latest_file.read_text(encoding="utf-8").strip()
        if content:
            return Path(content)
    return None


def _load_latest_results(artifacts_dir: Path, tag: str | None = None) -> Optional[Path]:
    _, latest_file = _latest_markers(artifacts_dir, tag)
    if latest_file.exists():
        content = latest_file.read_text(encoding="utf-8").strip()
        if content:
            return Path(content)
    latest_run = _load_latest_run(artifacts_dir, tag)
    if latest_run:
        summary_path = latest_run / "summary.json"
        if summary_path.exists():
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                results_path = summary.get("results_path")
                if results_path:
                    return Path(results_path)
            except Exception:
                pass
    return None


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


def _update_latest_markers(run_folder: Path, results_path: Path, artifacts_dir: Path, tag: str | None) -> None:
    marker_pairs = {_latest_markers(artifacts_dir, None)}
    if tag:
        marker_pairs.add(_latest_markers(artifacts_dir, tag))
    for latest_path, latest_results_path in marker_pairs:
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(str(run_folder), encoding="utf-8")
        latest_results_path.write_text(str(results_path), encoding="utf-8")


__all__ = [
    "_effective_paths",
    "_latest_markers",
    "_load_latest_results",
    "_load_latest_run",
    "_load_run_meta",
    "_run_dir_from_results_path",
    "_sanitize_tag",
    "_update_latest_markers",
]
