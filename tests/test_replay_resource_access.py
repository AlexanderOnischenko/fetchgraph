from __future__ import annotations

import json
from pathlib import Path

import fetchgraph.tracer.handlers  # noqa: F401
from fetchgraph.tracer.runtime import load_case_bundle, run_case


def test_replay_resource_read_uses_fixture_file() -> None:
    case_path = (
        Path(__file__).parent
        / "fixtures"
        / "replay_cases"
        / "fixed"
        / "resource_read.case.json"
    )
    expected_path = case_path.with_name("resource_read.expected.json")
    root, ctx = load_case_bundle(case_path)
    out = run_case(root, ctx)
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    assert out == expected
