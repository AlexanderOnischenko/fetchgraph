from __future__ import annotations

import json
from pathlib import Path

from fetchgraph.tracer import cli


def _write_jsonl(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _write_history_entry(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def _make_run_case(data_dir: Path, run_dir_name: str, case_id: str) -> tuple[Path, Path]:
    run_dir = data_dir / ".runs" / "runs" / run_dir_name
    case_dir = run_dir / "cases" / f"{case_id}_x"
    case_dir.mkdir(parents=True, exist_ok=True)
    events_path = case_dir / "events.jsonl"
    _write_jsonl(events_path, {"type": "replay_case", "id": "replay_1", "input": {}})
    return run_dir, case_dir


def test_export_case_bundle_resolves_run_id_from_history(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "out"
    run_dir, _ = _make_run_case(data_dir, "run_folder", "agg_003")
    history_path = data_dir / ".runs" / "history.jsonl"
    _write_history_entry(history_path, {"run_id": "abc123", "run_dir": str(run_dir)})

    exit_code = cli.main(
        [
            "export-case-bundle",
            "--id",
            "replay_1",
            "--out",
            str(out_dir),
            "--case",
            "agg_003",
            "--data",
            str(data_dir),
            "--run-id",
            "abc123",
        ]
    )

    assert exit_code == 0
    assert list(out_dir.glob("*.case.json"))


def test_export_case_bundle_resolves_run_id_from_run_meta(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "out"
    run_dir, _ = _make_run_case(data_dir, "run_folder_meta", "agg_003")
    (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "meta123"}), encoding="utf-8")

    exit_code = cli.main(
        [
            "export-case-bundle",
            "--id",
            "replay_1",
            "--out",
            str(out_dir),
            "--case",
            "agg_003",
            "--data",
            str(data_dir),
            "--run-id",
            "meta123",
        ]
    )

    assert exit_code == 0
    assert list(out_dir.glob("*.case.json"))
