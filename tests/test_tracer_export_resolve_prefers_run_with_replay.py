from __future__ import annotations

import json
import os
from pathlib import Path

from fetchgraph.tracer.resolve import resolve_case_events


def _write_events(path: Path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")


def _make_case_dir(run_dir: Path, case_id: str, suffix: str, *, status: str) -> Path:
    case_dir = run_dir / "cases" / f"{case_id}_{suffix}"
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / "status.json").write_text(json.dumps({"status": status}), encoding="utf-8")
    return case_dir


def test_resolve_prefers_run_with_replay(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    old_run = runs_root / "run_old"
    old_run.mkdir()
    old_case = _make_case_dir(old_run, "agg_003", "old", status="ok")
    _write_events(old_case / "events.jsonl", [{"type": "replay_case", "id": "other_case"}])
    os.utime(old_run, (1, 1))

    new_run = runs_root / "run_new"
    new_run.mkdir()
    new_case = _make_case_dir(new_run, "agg_003", "new", status="error")
    _write_events(new_case / "events.jsonl", [{"type": "replay_case", "id": "plan_normalize.spec_v1"}])
    os.utime(new_run, (2, 2))

    resolution = resolve_case_events(
        case_id="agg_003",
        data_dir=data_dir,
        pick_run="latest_with_replay",
        replay_id="plan_normalize.spec_v1",
    )
    assert resolution.case_dir == new_case
