from __future__ import annotations

from pathlib import Path

from fetchgraph.tracer.resolve import list_case_runs
from tests.helpers.tracer_testkit import make_case_dir, set_mtime


def _setup_runs(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)
    return data_dir


def test_run_dir_sorting_uses_name_over_mtime(tmp_path: Path) -> None:
    data_dir = _setup_runs(tmp_path)
    runs_root = data_dir / ".runs" / "runs"

    run_old = runs_root / "20260125_155226_retail_cases"
    run_new = runs_root / "20260201_173717_retail_cases"
    run_old.mkdir()
    run_new.mkdir()

    make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "event", "id": "run_old"}],
        mtime=200,
    )
    make_case_dir(
        run_new,
        "agg_003",
        "y",
        status="ok",
        events=[{"type": "event", "id": "run_new"}],
        mtime=100,
    )

    set_mtime(run_old, 300)
    set_mtime(run_new, 100)

    candidates, _ = list_case_runs(case_id="agg_003", data_dir=data_dir)
    candidate_dirs = [candidate.run_dir for candidate in candidates]

    assert candidate_dirs[0] == run_new


def test_run_dir_sorting_tiebreaks_by_mtime_then_name(tmp_path: Path) -> None:
    data_dir = _setup_runs(tmp_path)
    runs_root = data_dir / ".runs" / "runs"

    run_a = runs_root / "20260201_173717_alpha"
    run_b = runs_root / "20260201_173717_beta"
    run_a.mkdir()
    run_b.mkdir()

    make_case_dir(
        run_a,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "event", "id": "run_a"}],
        mtime=100,
    )
    make_case_dir(
        run_b,
        "agg_003",
        "y",
        status="ok",
        events=[{"type": "event", "id": "run_b"}],
        mtime=200,
    )

    set_mtime(run_a, 100)
    set_mtime(run_b, 200)

    candidates, _ = list_case_runs(case_id="agg_003", data_dir=data_dir)
    candidate_dirs = [candidate.run_dir for candidate in candidates]

    assert candidate_dirs[0] == run_b
