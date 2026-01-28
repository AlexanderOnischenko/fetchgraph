from __future__ import annotations

import json
from pathlib import Path

import pytest

from fetchgraph.replay.export import iter_events


def test_iter_events_bad_json_excerpt(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(
        '{"type":"ok"}\n'
        '{"type":"bad"\n'
        '{"type":"ok2"}\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Invalid JSON on line 2: .* in .*events.jsonl"):
        list(iter_events(events_path, allow_bad_json=False))


def test_iter_events_allow_bad_json_warns(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(
        json.dumps({"type": "ok"}) + "\n"
        + "{bad json}\n"
        + json.dumps({"type": "ok2"}) + "\n",
        encoding="utf-8",
    )

    caplog.set_level("WARNING")
    events = list(iter_events(events_path, allow_bad_json=True))
    assert [event["type"] for _, event in events] == ["ok", "ok2"]
    assert any("Skipped 1 invalid JSON lines" in record.message for record in caplog.records)
