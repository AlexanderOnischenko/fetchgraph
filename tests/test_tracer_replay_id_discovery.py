from __future__ import annotations

import json
import os
from pathlib import Path

from _pytest.capture import CaptureFixture

from fetchgraph.tracer import cli


def _write_events(path: Path, events: list[dict]) -> None:
    lines = [json.dumps(event) for event in events]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _set_mtime(path: Path, ts: float) -> None:
    os.utime(path, (ts, ts))


def _make_case_dir(
    run_dir: Path,
    case_id: str,
    suffix: str,
    *,
    status: str,
    events: list[dict] | None,
) -> Path:
    case_dir = run_dir / "cases" / f"{case_id}_{suffix}"
    case_dir.mkdir(parents=True, exist_ok=True)
    if events is not None:
        _write_events(case_dir / "events.jsonl", events)
    _write_json(case_dir / "status.json", {"status": status})
    return case_dir


def test_replay_id_discovery_skips_runs_without_replay_case(
    tmp_path: Path, capsys: CaptureFixture[str]
) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    run_new = runs_root / "20260201_173717_retail_cases"
    run_old = runs_root / "20260125_155226_retail_cases"
    run_new.mkdir()
    run_old.mkdir()

    _make_case_dir(
        run_new,
        "agg_003",
        "z",
        status="error",
        events=[{"type": "replay_case", "id": "replay_new", "v": 2, "input": {}}],
    )
    _make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "event", "id": "no_replay"}],
    )

    _set_mtime(run_old, 100)
    _set_mtime(run_new, 200)

    exit_code = cli.main(
        [
            "export-case-bundle",
            "--case",
            "agg_003",
            "--data",
            str(data_dir),
            "--list-replay-ids",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "replay_new" in captured.out
    assert str(run_new) in captured.err


def test_no_replay_case_triggers_fallback_scan_next_run(
    tmp_path: Path, capsys: CaptureFixture[str]
) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    run_new = runs_root / "20260201_173717_retail_cases"
    run_old = runs_root / "20260125_155226_retail_cases"
    run_new.mkdir()
    run_old.mkdir()

    _make_case_dir(
        run_new,
        "agg_003",
        "z",
        status="ok",
        events=[{"type": "event", "id": "no_replay"}],
    )
    _make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "replay_case", "id": "replay_old", "v": 2, "input": {}}],
    )

    _set_mtime(run_old, 100)
    _set_mtime(run_new, 200)

    exit_code = cli.main(
        [
            "export-case-bundle",
            "--case",
            "agg_003",
            "--data",
            str(data_dir),
            "--list-replay-ids",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "replay_old" in captured.out
    assert str(run_old) in captured.err
