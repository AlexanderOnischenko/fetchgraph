from __future__ import annotations

import os
from pathlib import Path

from fetchgraph.tracer import cli


def _touch_case(run_dir: Path, case_id: str, suffix: str, *, mtime: float) -> Path:
    case_dir = run_dir / "cases" / f"{case_id}_{suffix}"
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / "events.jsonl").write_text("{}", encoding="utf-8")
    os.utime(case_dir, (mtime, mtime))
    return case_dir


def test_resolve_case_dir_from_run_dir_selects_latest(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    _touch_case(run_dir, "case_1", "old", mtime=1)
    newest = _touch_case(run_dir, "case_1", "new", mtime=2)

    resolved = cli._resolve_case_dir_from_run_dir(run_dir, "case_1")
    assert resolved == newest
