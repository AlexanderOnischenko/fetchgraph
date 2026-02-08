from __future__ import annotations

import json
import logging
from pathlib import Path

from _pytest.logging import LogCaptureFixture

from fetchgraph.replay.export import export_replay_case_bundle


def _write_events(path: Path, events: list[dict]) -> None:
    lines = [json.dumps(event) for event in events]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_export_without_provider_specidx_for_single_match(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    out_dir = tmp_path / "out"
    events = [
        {
            "type": "replay_case",
            "id": "single",
            "v": 2,
            "input": {"value": 1},
            "observed": {"out": "ok"},
        }
    ]
    _write_events(events_path, events)

    out_path = export_replay_case_bundle(
        events_path=events_path,
        out_dir=out_dir,
        replay_id="single",
    )

    assert out_path.exists()


def test_export_warns_with_select_index_and_input_hash(
    tmp_path: Path, caplog: LogCaptureFixture
) -> None:
    events_path = tmp_path / "events.jsonl"
    out_dir = tmp_path / "out"
    events = [
        {
            "type": "replay_case",
            "id": "multi",
            "v": 2,
            "input": {"value": 1},
            "observed": {"out": "ok"},
            "timestamp": "2024-01-01T00:00:00Z",
        },
        {
            "type": "replay_case",
            "id": "multi",
            "v": 2,
            "input": {"value": 2},
            "observed": {"out": "ok"},
            "timestamp": "2024-01-02T00:00:00Z",
        },
    ]
    _write_events(events_path, events)

    caplog.set_level(logging.WARNING, logger="fetchgraph.replay.export")

    out_path = export_replay_case_bundle(
        events_path=events_path,
        out_dir=out_dir,
        replay_id="multi",
    )

    assert out_path.exists()
    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "--select-index" in messages
    assert "--input-hash" in messages
