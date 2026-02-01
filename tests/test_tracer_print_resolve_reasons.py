from __future__ import annotations

import re
from pathlib import Path

from pytest import CaptureFixture

from fetchgraph.tracer import cli
from tests.helpers.tracer_testkit import make_case_dir, set_mtime


def test_print_resolve_lists_rejected_runs_with_reasons(
    tmp_path: Path, capsys: CaptureFixture[str]
) -> None:
    data_dir = tmp_path / "data"
    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    run_missing_events = runs_root / "20260125_155226_retail_cases"
    run_tag_mismatch = runs_root / "20260126_101751_retail_cases"
    run_selected = runs_root / "20260201_173717_retail_cases"
    run_missing_events.mkdir()
    run_tag_mismatch.mkdir()
    run_selected.mkdir()

    missing_events_dir = make_case_dir(
        run_missing_events,
        "agg_003",
        "x",
        status="ok",
        tag="alpha",
        events=None,
    )
    tag_mismatch_dir = make_case_dir(
        run_tag_mismatch,
        "agg_003",
        "y",
        status="ok",
        tag="beta",
        events=[{"type": "event", "id": "tag_mismatch"}],
    )
    make_case_dir(
        run_selected,
        "agg_003",
        "z",
        status="ok",
        tag="alpha",
        events=[{"type": "replay_case", "id": "replay_ok", "v": 2, "input": {}}],
    )

    set_mtime(run_missing_events, 100)
    set_mtime(run_tag_mismatch, 200)
    set_mtime(run_selected, 300)

    exit_code = cli.main(
        [
            "export-case-bundle",
            "--case",
            "agg_003",
            "--data",
            str(data_dir),
            "--tag",
            "alpha",
            "--print-resolve",
            "--list-replay-ids",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    output = "\n".join([captured.out, captured.err])

    assert "Rejected candidates:" in output
    assert re.search(rf"{re.escape(str(missing_events_dir))}.*\\bno_events\\b", output)
    assert re.search(rf"{re.escape(str(tag_mismatch_dir))}.*\\btag_mismatch\\b", output)
