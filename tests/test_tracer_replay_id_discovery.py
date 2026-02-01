from __future__ import annotations

from pathlib import Path

from pytest import CaptureFixture

from fetchgraph.tracer import cli
from tests.helpers.tracer_testkit import make_case_dir, set_mtime


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

    make_case_dir(
        run_new,
        "agg_003",
        "z",
        status="error",
        events=[{"type": "replay_case", "id": "replay_new", "v": 2, "input": {}}],
        mtime=200,
    )
    make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "event", "id": "no_replay"}],
        mtime=100,
    )

    set_mtime(run_old, 100)
    set_mtime(run_new, 200)

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
    combined = "\n".join([captured.out, captured.err])
    assert "replay_new" in combined
    assert str(run_new) in combined


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

    make_case_dir(
        run_new,
        "agg_003",
        "z",
        status="ok",
        events=[{"type": "event", "id": "no_replay"}],
        mtime=200,
    )
    make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "replay_case", "id": "replay_old", "v": 2, "input": {}}],
        mtime=100,
    )

    set_mtime(run_old, 100)
    set_mtime(run_new, 200)

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
    combined = "\n".join([captured.out, captured.err])
    assert "replay_old" in combined
    assert str(run_old) in combined
