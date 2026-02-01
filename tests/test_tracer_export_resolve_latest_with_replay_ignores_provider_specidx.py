from __future__ import annotations

import json
import os
from pathlib import Path

from fetchgraph.tracer import cli


def _write_events(path: Path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")


def _make_case_dir(run_dir: Path, case_id: str, suffix: str) -> Path:
    case_dir = run_dir / "cases" / f"{case_id}_{suffix}"
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


def test_latest_with_replay_ignores_provider_specidx(tmp_path: Path, capsys: object) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    old_run = runs_root / "run_old"
    old_run.mkdir()
    old_case = _make_case_dir(old_run, "agg_003", "old")
    _write_events(
        old_case / "events.jsonl",
        [
            {
                "type": "replay_case",
                "id": "replay_1",
                "v": 2,
                "input": {"value": 1},
                "observed": {"out": "ok"},
                "meta": {"provider": "other", "spec_idx": 1},
            }
        ],
    )
    os.utime(old_run, (1, 1))

    new_run = runs_root / "run_new"
    new_run.mkdir()
    new_case = _make_case_dir(new_run, "agg_003", "new")
    _write_events(
        new_case / "events.jsonl",
        [
            {
                "type": "replay_case",
                "id": "replay_1",
                "v": 2,
                "input": {"value": 2},
                "observed": {"out": "ok"},
                "meta": {"provider": "sql", "spec_idx": 0},
            }
        ],
    )
    os.utime(new_run, (2, 2))

    out_dir = tmp_path / "out"
    exit_code = cli.main(
        [
            "export-case-bundle",
            "--id",
            "replay_1",
            "--provider",
            "other",
            "--out",
            str(out_dir),
            "--case",
            "agg_003",
            "--data",
            str(data_dir),
            "--pick-run",
            "latest_with_replay",
        ]
    )

    assert exit_code == 2
    captured = capsys.readouterr()
    assert "Found providers" in captured.err
    assert "sql" in captured.err
