from __future__ import annotations

from pathlib import Path

from pytest import CaptureFixture

from examples.demo_qa.runs.case_history import _load_case_history
from fetchgraph.tracer import cli
from fetchgraph.tracer.resolve import list_case_runs
from tests.helpers.tracer_testkit import (
    case_history_path,
    make_case_dir,
    set_mtime,
    write_history_entry,
)


def _history_run_dirs(path: Path) -> list[Path]:
    entries = _load_case_history(path)
    return [Path(entry["run_dir"]) for entry in reversed(entries)]


def _parse_listed_run_dirs(output: str) -> list[Path]:
    run_dirs: list[Path] = []
    for line in output.splitlines():
        if "run_dir=" not in line:
            continue
        chunk = line.split("run_dir=", 1)[1]
        run_value = chunk.split(" case_dir=", 1)[0].strip()
        run_dirs.append(Path(run_value))
    return run_dirs


def test_run_candidates_parity_history_vs_tracer(
    tmp_path: Path, capsys: CaptureFixture[str]
) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    run_old = runs_root / "20260125_155226_retail_cases"
    run_mid = runs_root / "20260126_101751_retail_cases"
    run_new = runs_root / "20260201_173717_retail_cases"
    run_old.mkdir()
    run_mid.mkdir()
    run_new.mkdir()

    make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "event", "id": "run_old"}],
    )
    make_case_dir(
        run_mid,
        "agg_003",
        "y",
        status="ok",
        events=[{"type": "replay_case", "id": "replay_mid", "v": 2, "input": {}}],
    )
    make_case_dir(
        run_new,
        "agg_003",
        "z",
        status="error",
        events=[{"type": "replay_case", "id": "replay_new", "v": 2, "input": {}}],
    )

    set_mtime(run_old, 100)
    set_mtime(run_mid, 200)
    set_mtime(run_new, 300)

    history_path = case_history_path(data_dir, "agg_003")
    write_history_entry(history_path, {"run_dir": str(run_old)})
    write_history_entry(history_path, {"run_dir": str(run_mid)})
    write_history_entry(history_path, {"run_dir": str(run_new)})

    history_dirs = _history_run_dirs(history_path)
    candidates, _ = list_case_runs(case_id="agg_003", data_dir=data_dir)
    candidate_dirs = [candidate.run_dir for candidate in candidates]

    exit_code = cli.main(
        [
            "export-case-bundle",
            "--case",
            "agg_003",
            "--data",
            str(data_dir),
            "--list-matches",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    tracer_dirs = _parse_listed_run_dirs(captured.out)

    assert candidate_dirs == history_dirs
    assert tracer_dirs == history_dirs


def test_latest_ordering_is_consistent_across_tools(
    tmp_path: Path, capsys: CaptureFixture[str]
) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    run_old = runs_root / "20260125_155226_retail_cases"
    run_mid = runs_root / "20260127_101751_retail_cases"
    run_new = runs_root / "20260201_173717_retail_cases"
    run_old.mkdir()
    run_mid.mkdir()
    run_new.mkdir()

    make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "event", "id": "run_old"}],
    )
    make_case_dir(
        run_mid,
        "agg_003",
        "y",
        status="ok",
        events=[{"type": "event", "id": "run_mid"}],
    )
    make_case_dir(
        run_new,
        "agg_003",
        "z",
        status="ok",
        events=[{"type": "event", "id": "run_new"}],
    )

    set_mtime(run_old, 300)
    set_mtime(run_mid, 100)
    set_mtime(run_new, 200)

    history_path = case_history_path(data_dir, "agg_003")
    write_history_entry(history_path, {"run_dir": str(run_old)})
    write_history_entry(history_path, {"run_dir": str(run_mid)})
    write_history_entry(history_path, {"run_dir": str(run_new)})

    history_dirs = _history_run_dirs(history_path)
    candidates, _ = list_case_runs(case_id="agg_003", data_dir=data_dir)
    candidate_dirs = [candidate.run_dir for candidate in candidates]

    exit_code = cli.main(
        [
            "export-case-bundle",
            "--case",
            "agg_003",
            "--data",
            str(data_dir),
            "--list-matches",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    tracer_dirs = _parse_listed_run_dirs(captured.out)

    assert candidate_dirs == history_dirs
    assert tracer_dirs == history_dirs


def test_tracer_ls_includes_run_selected_by_exporter(
    tmp_path: Path, capsys: CaptureFixture[str]
) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    run_old = runs_root / "20260125_155226_retail_cases"
    run_new = runs_root / "20260201_173717_retail_cases"
    run_old.mkdir()
    run_new.mkdir()

    make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "replay_case", "id": "replay_old", "v": 2, "input": {}}],
    )
    make_case_dir(
        run_new,
        "agg_003",
        "y",
        status="ok",
        events=[{"type": "replay_case", "id": "replay_new", "v": 2, "input": {}}],
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
            "--id",
            "replay_new",
            "--print-resolve",
            "--list-replay-ids",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    resolved_line = next(
        line for line in captured.out.splitlines() if line.startswith("Resolved run_dir:")
    )
    resolved_run_dir = Path(resolved_line.split(":", 1)[1].strip())

    exit_code = cli.main(
        [
            "export-case-bundle",
            "--case",
            "agg_003",
            "--data",
            str(data_dir),
            "--list-matches",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    tracer_dirs = _parse_listed_run_dirs(captured.out)

    assert resolved_run_dir in tracer_dirs
