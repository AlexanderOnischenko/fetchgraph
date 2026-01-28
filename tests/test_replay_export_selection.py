from __future__ import annotations

import json
from pathlib import Path

import pytest

from fetchgraph.replay.export import (
    case_bundle_name,
    export_replay_case_bundle,
    find_replay_case_matches,
    iter_events,
)


def _write_events(path: Path, events: list[dict]) -> None:
    lines = [json.dumps(event) for event in events]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_iter_events_allow_bad_json(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    events_path.write_text('{"type":"ok"}\n{bad json}\n{"type":"ok2"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid JSON on line 2"):
        list(iter_events(events_path, allow_bad_json=False))

    events = list(iter_events(events_path, allow_bad_json=True))
    assert [event["type"] for _, event in events] == ["ok", "ok2"]


def test_export_replay_case_selection_controls(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    out_dir = tmp_path / "out"
    event_base = {
        "type": "replay_case",
        "v": 2,
        "id": "plan_normalize.spec_v1",
        "input": {"spec": {"provider": "sql"}},
        "observed": {"out_spec": {"provider": "sql"}},
    }
    _write_events(
        events_path,
        [
            {**event_base, "timestamp": "2024-01-01T00:00:00Z"},
            {**event_base, "timestamp": "2024-01-02T00:00:00Z"},
        ],
    )

    selections = find_replay_case_matches(events_path, replay_id="plan_normalize.spec_v1")
    assert len(selections) == 2

    with pytest.raises(LookupError, match="Multiple replay_case entries matched"):
        export_replay_case_bundle(
            events_path=events_path,
            out_dir=out_dir,
            replay_id="plan_normalize.spec_v1",
            require_unique=True,
        )

    out_path = export_replay_case_bundle(
        events_path=events_path,
        out_dir=out_dir,
        replay_id="plan_normalize.spec_v1",
        select_index=1,
    )
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["source"]["line"] == 1


def test_export_replay_case_requires_run_dir_for_resources(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    out_dir = tmp_path / "out"
    events = [
        {
            "type": "replay_resource",
            "id": "rid1",
            "data_ref": {"file": "artifact.txt"},
        },
        {
            "type": "replay_case",
            "v": 2,
            "id": "plan_normalize.spec_v1",
            "input": {"spec": {"provider": "sql"}},
            "observed": {"out_spec": {"provider": "sql"}},
            "requires": [{"kind": "resource", "id": "rid1"}],
        },
    ]
    _write_events(events_path, events)

    with pytest.raises(ValueError, match="run_dir is required to export file resources"):
        export_replay_case_bundle(
            events_path=events_path,
            out_dir=out_dir,
            replay_id="plan_normalize.spec_v1",
        )


def test_export_replay_case_overwrite_cleans_resources(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    out_dir = tmp_path / "out"
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "artifact.txt").write_text("data", encoding="utf-8")

    events = [
        {
            "type": "replay_resource",
            "id": "rid1",
            "data_ref": {"file": "artifact.txt"},
        },
        {
            "type": "replay_case",
            "v": 2,
            "id": "plan_normalize.spec_v1",
            "input": {"spec": {"provider": "sql"}},
            "observed": {"out_spec": {"provider": "sql"}},
            "requires": [{"kind": "resource", "id": "rid1"}],
        },
    ]
    _write_events(events_path, events)

    out_path = export_replay_case_bundle(
        events_path=events_path,
        out_dir=out_dir,
        replay_id="plan_normalize.spec_v1",
        run_dir=run_dir,
    )
    fixture_stem = case_bundle_name("plan_normalize.spec_v1", events[1]["input"]).replace(".case.json", "")
    extra_path = out_dir / "resources" / fixture_stem / "extra.txt"
    extra_path.parent.mkdir(parents=True, exist_ok=True)
    extra_path.write_text("extra", encoding="utf-8")

    export_replay_case_bundle(
        events_path=events_path,
        out_dir=out_dir,
        replay_id="plan_normalize.spec_v1",
        run_dir=run_dir,
        overwrite=True,
    )
    assert out_path.exists()
    assert not extra_path.exists()
