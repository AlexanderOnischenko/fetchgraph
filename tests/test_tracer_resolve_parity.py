from __future__ import annotations

import json
import os
from pathlib import Path

from _pytest.capture import CaptureFixture

from examples.demo_qa.runs.case_history import _load_case_history
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
    tag: str | None = None,
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


def _write_history_entry(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


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

    _make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "event", "id": "run_old"}],
    )
    _make_case_dir(
        run_mid,
        "agg_003",
        "y",
        status="ok",
        events=[{"type": "replay_case", "id": "replay_mid", "v": 2, "input": {}}],
    )
    _make_case_dir(
        run_new,
        "agg_003",
        "z",
        status="error",
        events=[{"type": "replay_case", "id": "replay_new", "v": 2, "input": {}}],
    )

    _set_mtime(run_old, 100)
    _set_mtime(run_mid, 200)
    _set_mtime(run_new, 300)

    history_path = data_dir / ".runs" / "runs" / "cases" / "agg_003.jsonl"
    _write_history_entry(history_path, {"run_dir": str(run_old)})
    _write_history_entry(history_path, {"run_dir": str(run_mid)})
    _write_history_entry(history_path, {"run_dir": str(run_new)})

    history_dirs = _history_run_dirs(history_path)

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

    _make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "event", "id": "run_old"}],
    )
    _make_case_dir(
        run_mid,
        "agg_003",
        "y",
        status="ok",
        events=[{"type": "event", "id": "run_mid"}],
    )
    _make_case_dir(
        run_new,
        "agg_003",
        "z",
        status="ok",
        events=[{"type": "event", "id": "run_new"}],
    )

    _set_mtime(run_old, 300)
    _set_mtime(run_mid, 100)
    _set_mtime(run_new, 200)

    history_path = data_dir / ".runs" / "runs" / "cases" / "agg_003.jsonl"
    _write_history_entry(history_path, {"run_dir": str(run_old)})
    _write_history_entry(history_path, {"run_dir": str(run_mid)})
    _write_history_entry(history_path, {"run_dir": str(run_new)})

    history_dirs = _history_run_dirs(history_path)

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

    _make_case_dir(
        run_old,
        "agg_003",
        "x",
        status="ok",
        events=[{"type": "replay_case", "id": "replay_old", "v": 2, "input": {}}],
    )
    _make_case_dir(
        run_new,
        "agg_003",
        "y",
        status="ok",
        events=[{"type": "replay_case", "id": "replay_new", "v": 2, "input": {}}],
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
