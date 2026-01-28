from __future__ import annotations

import json
from pathlib import Path

import pytest

from fetchgraph.tracer.fixture_tools import fixture_fix, fixture_green, fixture_migrate


def _write_bundle(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")), encoding="utf-8")


def _bundle_payload(root: dict, *, resources: dict | None = None, extras: dict | None = None) -> dict:
    return {
        "schema": "fetchgraph.tracer.case_bundle",
        "v": 1,
        "root": root,
        "resources": resources or {},
        "extras": extras or {},
        "source": {"events_path": "events.jsonl"},
    }


def test_fixture_green_requires_observed(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    case_path = root / "known_bad" / "case.case.json"
    payload = _bundle_payload(
        {
            "type": "replay_case",
            "v": 2,
            "id": "plan_normalize.spec_v1",
            "input": {"spec": {"provider": "sql"}},
            "observed_error": {"type": "ValueError", "message": "boom"},
        }
    )
    _write_bundle(case_path, payload)

    with pytest.raises(ValueError, match="root.observed is missing"):
        fixture_green(case_path=case_path, out_root=root, expected_from="observed")


def test_fixture_green_moves_case_and_resources(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    case_path = root / "known_bad" / "case.case.json"
    resources_dir = root / "known_bad" / "resources" / "case"
    resources_dir.mkdir(parents=True, exist_ok=True)
    (resources_dir / "rid1.txt").write_text("data", encoding="utf-8")
    payload = _bundle_payload(
        {
            "type": "replay_case",
            "v": 2,
            "id": "plan_normalize.spec_v1",
            "input": {"spec": {"provider": "sql"}, "options": {}},
            "observed": {"out_spec": {"provider": "sql"}},
        }
    )
    _write_bundle(case_path, payload)

    fixture_green(case_path=case_path, out_root=root, expected_from="replay")

    fixed_case = root / "fixed" / "case.case.json"
    expected_path = root / "fixed" / "case.expected.json"
    fixed_resources = root / "fixed" / "resources" / "case"
    assert fixed_case.exists()
    assert expected_path.exists()
    assert fixed_resources.exists()
    assert not (root / "known_bad" / "case.case.json").exists()
    assert not resources_dir.exists()


def test_fixture_green_rolls_back_on_validation_failure(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    case_path = root / "known_bad" / "case.case.json"
    payload = _bundle_payload(
        {
            "type": "replay_case",
            "v": 2,
            "id": "plan_normalize.spec_v1",
            "input": {"spec": {"provider": "sql"}, "options": {}},
            "observed": {"out_spec": {"provider": "other"}},
        }
    )
    _write_bundle(case_path, payload)

    with pytest.raises(AssertionError, match="rollback completed"):
        fixture_green(case_path=case_path, out_root=root, expected_from="observed")

    assert case_path.exists()
    assert not (root / "fixed" / "case.case.json").exists()
    assert not (root / "fixed" / "case.expected.json").exists()


def test_fixture_fix_renames_and_updates_resource_paths(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    bucket = "fixed"
    case_path = root / bucket / "old.case.json"
    resources_dir = root / bucket / "resources" / "old" / "rid1"
    resources_dir.mkdir(parents=True, exist_ok=True)
    (resources_dir / "file.txt").write_text("data", encoding="utf-8")
    payload = _bundle_payload(
        {
            "type": "replay_case",
            "v": 2,
            "id": "plan_normalize.spec_v1",
            "input": {"spec": {"provider": "sql"}},
            "observed": {"out_spec": {"provider": "sql"}},
        },
        resources={
            "rid1": {"data_ref": {"file": "resources/old/rid1/file.txt"}},
        },
    )
    _write_bundle(case_path, payload)
    expected_path = root / bucket / "old.expected.json"
    expected_path.write_text("{}", encoding="utf-8")

    fixture_fix(root=root, name="old", new_name="new", bucket=bucket, dry_run=False)

    new_case = root / bucket / "new.case.json"
    new_expected = root / bucket / "new.expected.json"
    new_resources = root / bucket / "resources" / "new" / "rid1" / "file.txt"
    assert new_case.exists()
    assert new_expected.exists()
    assert new_resources.exists()
    data = json.loads(new_case.read_text(encoding="utf-8"))
    assert data["resources"]["rid1"]["data_ref"]["file"] == "resources/new/rid1/file.txt"


def test_fixture_migrate_moves_resources(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    bucket = "fixed"
    case_path = root / bucket / "case.case.json"
    legacy_dir = root / bucket / "legacy"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    legacy_path = legacy_dir / "file.txt"
    legacy_path.write_text("data", encoding="utf-8")
    payload = _bundle_payload(
        {
            "type": "replay_case",
            "v": 2,
            "id": "plan_normalize.spec_v1",
            "input": {"spec": {"provider": "sql"}},
            "observed": {"out_spec": {"provider": "sql"}},
        },
        resources={"rid1": {"data_ref": {"file": "legacy/file.txt"}}},
    )
    _write_bundle(case_path, payload)

    bundles_updated, files_moved = fixture_migrate(root=root, bucket=bucket, dry_run=False)
    assert bundles_updated == 1
    assert files_moved == 1
    data = json.loads(case_path.read_text(encoding="utf-8"))
    assert data["resources"]["rid1"]["data_ref"]["file"] == "resources/case/rid1/legacy/file.txt"
    assert (root / bucket / "resources" / "case" / "rid1" / "legacy" / "file.txt").exists()
