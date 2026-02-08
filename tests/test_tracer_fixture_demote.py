from __future__ import annotations

import json
from pathlib import Path

import pytest

from fetchgraph.tracer.fixture_tools import fixture_demote


def _write_bundle(path: Path, *, case_id: str) -> None:
    payload = {
        "schema": "fetchgraph.tracer.case_bundle",
        "v": 1,
        "root": {"type": "replay_case", "v": 2, "id": "plan_normalize.spec_v1", "input": {}},
        "source": {"case_id": case_id},
        "resources": {},
        "extras": {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_fixture_demote_moves_files(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    fixed = root / "fixed"
    known_bad = root / "known_bad"
    case_path = fixed / "agg_003__a.case.json"
    expected_path = fixed / "agg_003__a.expected.json"
    resources_dir = fixed / "resources" / "agg_003__a"
    _write_bundle(case_path, case_id="agg_003")
    expected_path.write_text('{"ok": true}', encoding="utf-8")
    resources_dir.mkdir(parents=True, exist_ok=True)
    (resources_dir / "sample.txt").write_text("data", encoding="utf-8")

    fixture_demote(
        root=root,
        case_id="agg_003",
        dry_run=True,
    )
    assert case_path.exists()
    assert expected_path.exists()
    assert (resources_dir / "sample.txt").exists()

    fixture_demote(
        root=root,
        case_id="agg_003",
        dry_run=False,
    )
    assert not case_path.exists()
    assert not expected_path.exists()
    assert not resources_dir.exists()
    assert (known_bad / "agg_003__a.case.json").exists()
    assert (known_bad / "agg_003__a.expected.json").exists()
    assert (known_bad / "resources" / "agg_003__a" / "sample.txt").exists()
