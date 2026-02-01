from __future__ import annotations

import os
from pathlib import Path

from fetchgraph.tracer.resolve import list_case_run_listings, list_case_runs
from tests.helpers.tracer_testkit import case_history_path, make_case_dir, write_history_entry


def test_history_relative_run_dir_is_normalized(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    run_dir = runs_root / "20260127_101751_retail_cases"
    run_dir.mkdir(parents=True, exist_ok=True)
    make_case_dir(
        run_dir,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "event", "id": "relative_history"}],
    )

    relative_run_dir = os.path.relpath(run_dir, Path.cwd())
    history_path = case_history_path(data_dir, "agg_003")
    write_history_entry(history_path, {"run_dir": str(relative_run_dir)})

    candidates, _ = list_case_runs(case_id="agg_003", data_dir=data_dir)
    assert candidates[0].run_dir == run_dir


def test_nested_runs_root_is_scanned(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runs_root_cli = data_dir / ".runs" / "runs"
    nested_root = runs_root_cli / "runs"
    run_dir = nested_root / "20260127_101751_retail_cases"
    run_dir.mkdir(parents=True, exist_ok=True)
    make_case_dir(
        run_dir,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "event", "id": "nested_root"}],
    )

    candidates, stats = list_case_runs(case_id="agg_003", data_dir=data_dir)
    candidate_dirs = [candidate.run_dir for candidate in candidates]

    assert run_dir in candidate_dirs
    assert stats.runs_root_effective == nested_root


def test_history_missing_on_disk_is_listed(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    missing_run_dir = data_dir / ".runs" / "runs" / "20260127_101751_retail_cases"

    history_path = case_history_path(data_dir, "agg_003")
    write_history_entry(history_path, {"run_dir": str(missing_run_dir)})

    listings, _ = list_case_run_listings(case_id="agg_003", data_dir=data_dir)

    assert listings[0].run_dir == missing_run_dir
    assert listings[0].status == "missing_on_disk"
