from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from fetchgraph.tracer.fixture_tools import resolve_fixture_candidates, select_fixture_candidate


def _write_bundle(path: Path, *, case_id: str, timestamp: str) -> None:
    payload = {
        "schema": "fetchgraph.tracer.case_bundle",
        "v": 1,
        "root": {"type": "replay_case", "v": 2, "id": "plan_normalize.spec_v1", "input": {}},
        "source": {"case_id": case_id, "timestamp": timestamp},
        "resources": {},
        "extras": {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_resolve_fixture_by_case_id(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    known_bad = root / "known_bad"
    case_old = known_bad / "agg_003__a.case.json"
    case_new = known_bad / "agg_003__b.case.json"
    _write_bundle(case_old, case_id="agg_003", timestamp="2024-01-01T00:00:00Z")
    _write_bundle(case_new, case_id="agg_003", timestamp="2024-01-02T00:00:00Z")
    os.utime(case_old, (1, 1))
    os.utime(case_new, (2, 2))

    candidates = resolve_fixture_candidates(root=root, bucket="known_bad", case_id="agg_003", name=None)
    selected = select_fixture_candidate(candidates, select="latest")
    assert selected.path == case_new

    selected_first = select_fixture_candidate(candidates, select_index=1)
    assert selected_first.path == candidates[0].path

    with pytest.raises(FileExistsError):
        select_fixture_candidate(candidates, require_unique=True)
