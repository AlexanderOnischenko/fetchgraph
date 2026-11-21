from __future__ import annotations

import json
from pathlib import Path

from _pytest.capture import CaptureFixture

from fetchgraph.replay.export import input_hash8
from fetchgraph.tracer import cli


def _write_events(path: Path, events: list[dict]) -> None:
    lines = [json.dumps(event) for event in events]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_export_filters_by_input_hash(tmp_path: Path, capsys: CaptureFixture[str]) -> None:
    events_path = tmp_path / "events.jsonl"
    out_dir = tmp_path / "out"
    events = [
        {
            "type": "replay_case",
            "id": "replay_same",
            "v": 2,
            "input": {"value": 1},
            "observed": {"out": "ok"},
        },
        {
            "type": "replay_case",
            "id": "replay_same",
            "v": 2,
            "input": {"value": 2},
            "observed": {"out": "ok"},
        },
    ]
    _write_events(events_path, events)

    wanted_hash = input_hash8(events[0]["input"])
    exit_code = cli.main(
        [
            "export-case-bundle",
            "--events",
            str(events_path),
            "--out",
            str(out_dir),
            "--id",
            "replay_same",
            "--input-hash",
            wanted_hash,
        ]
    )

    assert exit_code == 0
    bundles = list(out_dir.glob("*.case.json"))
    assert len(bundles) == 1
    payload = json.loads(bundles[0].read_text(encoding="utf-8"))
    assert payload["root"]["input"] == events[0]["input"]

    capsys.readouterr()
    exit_code = cli.main(
        [
            "export-case-bundle",
            "--events",
            str(events_path),
            "--out",
            str(out_dir),
            "--id",
            "replay_same",
            "--input-hash",
            "ffffffff",
        ]
    )

    assert exit_code == 2
    captured = capsys.readouterr()
    assert "--list-replay-matches" in captured.err
