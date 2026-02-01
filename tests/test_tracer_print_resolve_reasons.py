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
    tag: str | None = None,
    events: list[dict] | None,
) -> Path:
    case_dir = run_dir / "cases" / f"{case_id}_{suffix}"
    case_dir.mkdir(parents=True, exist_ok=True)
    if events is not None:
        _write_events(case_dir / "events.jsonl", events)
    payload = {"status": status}
    if tag:
        payload["tag"] = tag
    _write_json(case_dir / "status.json", payload)
    return case_dir


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

    _make_case_dir(
        run_missing_events,
        "agg_003",
        "x",
        status="ok",
        tag="alpha",
        events=None,
    )
    _make_case_dir(
        run_tag_mismatch,
        "agg_003",
        "y",
        status="ok",
        tag="beta",
        events=[{"type": "event", "id": "tag_mismatch"}],
    )
    _make_case_dir(
        run_selected,
        "agg_003",
        "z",
        status="ok",
        tag="alpha",
        events=[{"type": "replay_case", "id": "replay_ok", "v": 2, "input": {}}],
    )

    _set_mtime(run_missing_events, 100)
    _set_mtime(run_tag_mismatch, 200)
    _set_mtime(run_selected, 300)

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
    output = captured.out

    assert "Rejected candidates:" in output
    assert f"{run_missing_events / 'cases' / 'agg_003_x'} (no_events)" in output
    assert f"{run_tag_mismatch / 'cases' / 'agg_003_y'} (tag_mismatch)" in output
