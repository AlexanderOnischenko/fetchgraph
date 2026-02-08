from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from fetchgraph.tracer.fixture_tools import fixture_migrate, fixture_rm


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


def test_fixture_selector_for_rm_migrate(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = tmp_path / "fixtures"
    known_bad = root / "known_bad"
    case_old = known_bad / "agg_003__a.case.json"
    case_new = known_bad / "agg_003__b.case.json"
    _write_bundle(case_old, case_id="agg_003")
    _write_bundle(case_new, case_id="agg_003")
    os.utime(case_old, (1, 1))
    os.utime(case_new, (2, 2))

    removed = fixture_rm(
        root=root,
        bucket="known_bad",
        scope="cases",
        dry_run=True,
        case_id="agg_003",
    )
    assert removed == 1
    assert case_old.exists()
    assert case_new.exists()

    with pytest.raises(FileExistsError):
        fixture_rm(
            root=root,
            bucket="known_bad",
            scope="cases",
            dry_run=True,
            case_id="agg_003",
            require_unique=True,
        )

    bundles_updated, files_moved = fixture_migrate(
        root=root,
        bucket="known_bad",
        dry_run=True,
        case_id="agg_003",
        all_matches=True,
    )
    assert bundles_updated == 0
    assert files_moved == 0
    output = capsys.readouterr().out
    assert "bulk operation" in output
