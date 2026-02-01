from __future__ import annotations

import json
from pathlib import Path

from fetchgraph.replay.export import input_hash8
from fetchgraph.tracer import cli


def _write_events(path: Path, events: list[dict]) -> None:
    lines = [json.dumps(event) for event in events]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_list_replay_matches_outputs_table(tmp_path: Path, capsys: object) -> None:
    events_path = tmp_path / "events.jsonl"
    events = [
        {
            "type": "replay_case",
            "id": "replay_a",
            "v": 2,
            "input": {"x": 1},
            "observed": {"out": "ok"},
            "meta": {"provider": "sql", "spec_idx": 0},
            "timestamp": "2024-01-01T00:00:00Z",
        },
        {"type": "other", "id": "noop"},
        {
            "type": "replay_case",
            "id": "replay_b",
            "v": 2,
            "input": {"y": 2},
            "observed_error": {"msg": "fail"},
            "meta": {"provider": "relational", "spec_idx": 2},
            "timestamp": "2024-01-02T00:00:00Z",
        },
    ]
    _write_events(events_path, events)

    exit_code = cli.main(
        [
            "export-case-bundle",
            "--events",
            str(events_path),
            "--list-replay-matches",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    lines = captured.out.strip().splitlines()
    assert lines[0].startswith("idx\tline\ttimestamp\treplay_id")

    rows = [line.split("\t") for line in lines[1:]]
    assert rows[0][0] == "1"
    assert rows[0][1] == "1"
    assert rows[0][3] == "replay_a"
    assert rows[0][6] == input_hash8(events[0]["input"])
    assert rows[0][7] == "ok"

    assert rows[1][0] == "2"
    assert rows[1][1] == "3"
    assert rows[1][3] == "replay_b"
    assert rows[1][6] == input_hash8(events[2]["input"])
    assert rows[1][7] == "error"
