from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from fetchgraph.tracer.resolve import EventsResolution, find_events_file, resolve_case_events


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")


def _set_mtime(path: Path, ts: float) -> None:
    os.utime(path, (ts, ts))


def _make_case_dir(
    run_dir: Path,
    case_id: str,
    suffix: str,
    *,
    status: str,
    tag: str | None = None,
    with_events: bool = True,
) -> Path:
    case_dir = run_dir / "cases" / f"{case_id}_{suffix}"
    case_dir.mkdir(parents=True, exist_ok=True)
    if with_events:
        _touch(case_dir / "events.jsonl")
    payload = {"status": status}
    _write_json(case_dir / "status.json", payload)
    if tag:
        _write_json(run_dir / "run_meta.json", {"tag": tag})
    return case_dir


def test_resolve_latest_with_events(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    run_old = runs_root / "run_old"
    run_old.mkdir()
    _make_case_dir(run_old, "case_1", "abc", status="ok")
    _set_mtime(run_old, 100)

    run_new = runs_root / "run_new"
    run_new.mkdir()
    _make_case_dir(run_new, "case_1", "def", status="ok", with_events=False)
    _set_mtime(run_new, 200)

    resolution = resolve_case_events(case_id="case_1", data_dir=data_dir)
    assert resolution.case_dir.parent.parent == run_old


def test_resolve_with_tag(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    run_a = runs_root / "run_a"
    run_a.mkdir()
    _make_case_dir(run_a, "case_2", "aaa", status="ok", tag="alpha")
    _set_mtime(run_a, 100)

    run_b = runs_root / "run_b"
    run_b.mkdir()
    _make_case_dir(run_b, "case_2", "bbb", status="ok", tag="beta")
    _set_mtime(run_b, 200)

    resolution = resolve_case_events(case_id="case_2", data_dir=data_dir, tag="alpha")
    assert resolution.run_dir == run_a


def test_resolve_not_found(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    run_dir = runs_root / "run_only"
    run_dir.mkdir()
    _make_case_dir(run_dir, "case_3", "ccc", status="missed", with_events=False)

    with pytest.raises(LookupError, match="No suitable case run found"):
        resolve_case_events(case_id="case_3", data_dir=data_dir)


def test_find_events_file_not_found(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    resolution = find_events_file(run_dir)
    assert isinstance(resolution, EventsResolution)
    assert resolution.events_path is None
