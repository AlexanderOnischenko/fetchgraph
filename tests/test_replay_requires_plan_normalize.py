from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from fetchgraph.replay.export import export_replay_case_bundle
from fetchgraph.replay.handlers.plan_normalize import replay_plan_normalize_spec_v1
from fetchgraph.replay.runtime import ReplayContext


def _write_events(path: Path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")


def test_export_bundle_resolves_planner_input_schema_ref(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    replay_case = {
        "type": "replay_case",
        "v": 2,
        "id": "plan_normalize.spec_v1",
        "meta": {"provider": "relational", "spec_idx": 0},
        "input": {
            "spec": {"provider": "relational", "mode": "full", "selectors": {"op": "query"}},
            "options": {},
            "normalizer_rules": {"relational": "relational_v1"},
        },
        "observed": {
            "out_spec": {"provider": "relational", "mode": "full", "selectors": {"op": "query"}},
        },
        "requires": [{"kind": "extra", "id": "planner_input_v1"}],
    }
    planner_input = {
        "type": "planner_input",
        "id": "planner_input_v1",
        "input": {
            "provider_catalog": {
                "relational": {
                    "name": "relational",
                    "selectors_schema": {"type": "object", "properties": {"op": {"type": "string"}}},
                }
            },
            "schema_ref": "schema_v1",
        },
    }
    schema_resource = {"type": "replay_resource", "id": "schema_v1", "data": {"schema": {"type": "object"}}}
    _write_events(events_path, [replay_case, planner_input, schema_resource])

    out_dir = tmp_path / "out"
    bundle_path = export_replay_case_bundle(
        events_path=events_path,
        out_dir=out_dir,
        replay_id="plan_normalize.spec_v1",
    )
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert "planner_input_v1" in bundle["extras"]
    assert "schema_v1" in bundle["resources"]


def test_export_requires_missing_planner_input_raises(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    replay_case = {
        "type": "replay_case",
        "v": 2,
        "id": "plan_normalize.spec_v1",
        "meta": {"provider": "relational", "spec_idx": 2},
        "input": {
            "spec": {"provider": "relational", "mode": "full", "selectors": {"op": "query"}},
            "options": {},
            "normalizer_rules": {"relational": "relational_v1"},
        },
        "observed": {
            "out_spec": {"provider": "relational", "mode": "full", "selectors": {"op": "query"}},
        },
        "requires": [{"kind": "extra", "id": "planner_input_v1"}],
    }
    _write_events(events_path, [replay_case])

    with pytest.raises(KeyError) as excinfo:
        export_replay_case_bundle(
            events_path=events_path,
            out_dir=tmp_path / "out",
            replay_id="plan_normalize.spec_v1",
        )
    message = str(excinfo.value)
    assert "planner_input_v1" in message
    assert "plan_normalize.spec_v1" in message


def test_replay_plan_normalize_uses_planner_input(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="fetchgraph.replay.handlers.plan_normalize")
    inp = {
        "spec": {"provider": "relational", "mode": "full", "selectors": {"op": "query"}},
        "options": {},
        "normalizer_rules": {"relational": "relational_v1"},
    }
    ctx = ReplayContext(
        extras={
            "planner_input_v1": {
                "input": {
                    "provider_catalog": {
                        "relational": {
                            "name": "relational",
                            "selectors_schema": {"type": "object", "properties": {"op": {"type": "string"}}},
                        }
                    }
                }
            }
        }
    )
    out = replay_plan_normalize_spec_v1(inp, ctx)
    assert out["out_spec"]["provider"] == "relational"
    assert "provider_info_source=planner_input" in caplog.text
