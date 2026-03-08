from __future__ import annotations

import json
from pathlib import Path

import pytest

from fetchgraph.tracer.fixture_tools import (
    fixture_fix,
    fixture_green,
    fixture_migrate,
    parse_fixture_ref,
    resolve_user_case_input,
)


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




def test_fixture_green_preserves_relative_paths_for_nested_cases(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    case_path = root / "known_bad" / "agg_003" / "nested" / "case.case.json"
    resources_dir = root / "known_bad" / "resources" / "agg_003" / "nested" / "case"
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

    fixed_case = root / "fixed" / "agg_003" / "nested" / "case.case.json"
    expected_path = root / "fixed" / "agg_003" / "nested" / "case.expected.json"
    fixed_resources = root / "fixed" / "resources" / "agg_003" / "nested" / "case"
    assert fixed_case.exists()
    assert expected_path.exists()
    assert fixed_resources.exists()
    assert not case_path.exists()
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


def test_fixture_migrate_leaves_canonical_paths(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    bucket = "fixed"
    case_path = root / bucket / "case.case.json"
    resources_dir = root / bucket / "resources" / "case" / "rid1"
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
        resources={"rid1": {"data_ref": {"file": "resources/case/rid1/file.txt"}}},
    )
    _write_bundle(case_path, payload)

    bundles_updated, files_moved = fixture_migrate(root=root, bucket=bucket, dry_run=False)
    assert bundles_updated == 0
    assert files_moved == 0
    data = json.loads(case_path.read_text(encoding="utf-8"))
    assert data["resources"]["rid1"]["data_ref"]["file"] == "resources/case/rid1/file.txt"
    assert (root / bucket / "resources" / "case" / "rid1" / "file.txt").exists()




def test_fixture_migrate_supports_nested_stem_paths(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    bucket = "fixed"
    case_path = root / bucket / "agg_003" / "nested" / "case.case.json"
    resource_file = root / bucket / "resources" / "agg_003" / "nested" / "case" / "rid1" / "file.txt"
    resource_file.parent.mkdir(parents=True, exist_ok=True)
    resource_file.write_text("data", encoding="utf-8")
    payload = _bundle_payload(
        {
            "type": "replay_case",
            "v": 2,
            "id": "plan_normalize.spec_v1",
            "input": {"spec": {"provider": "sql"}},
            "observed": {"out_spec": {"provider": "sql"}},
        },
        resources={"rid1": {"data_ref": {"file": "resources/agg_003/nested/case/rid1/file.txt"}}},
    )
    _write_bundle(case_path, payload)

    bundles_updated, files_moved = fixture_migrate(root=root, bucket=bucket, dry_run=False)

    assert bundles_updated == 0
    assert files_moved == 0


def test_fixture_migrate_normalizes_backslashes(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    bucket = "fixed"
    case_path = root / bucket / "case.case.json"
    resources_dir = root / bucket / "resources" / "case" / "rid1"
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
        resources={"rid1": {"data_ref": {"file": r"resources\\case\\rid1\\file.txt"}}},
    )
    _write_bundle(case_path, payload)

    bundles_updated, files_moved = fixture_migrate(root=root, bucket=bucket, dry_run=False)
    assert bundles_updated == 1
    assert files_moved == 0
    data = json.loads(case_path.read_text(encoding="utf-8"))
    assert data["resources"]["rid1"]["data_ref"]["file"] == "resources/case/rid1/file.txt"
    assert (root / bucket / "resources" / "case" / "rid1" / "file.txt").exists()




def test_fixture_migrate_normalizes_backslashes_for_nested_stem(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    bucket = "fixed"
    case_path = root / bucket / "agg_003" / "nested" / "case.case.json"
    resources_dir = root / bucket / "resources" / "agg_003" / "nested" / "case" / "rid1"
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
        resources={"rid1": {"data_ref": {"file": r"resources\agg_003\nested\case\rid1\file.txt"}}},
    )
    _write_bundle(case_path, payload)

    bundles_updated, files_moved = fixture_migrate(root=root, bucket=bucket, dry_run=False)

    assert bundles_updated == 1
    assert files_moved == 0
    data = json.loads(case_path.read_text(encoding="utf-8"))
    assert data["resources"]["rid1"]["data_ref"]["file"] == "resources/agg_003/nested/case/rid1/file.txt"
    assert (root / bucket / "resources" / "agg_003" / "nested" / "case" / "rid1" / "file.txt").exists()

def test_fixture_migrate_rejects_windows_absolute_paths(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    bucket = "fixed"
    case_path = root / bucket / "case.case.json"
    payload = _bundle_payload(
        {
            "type": "replay_case",
            "v": 2,
            "id": "plan_normalize.spec_v1",
            "input": {"spec": {"provider": "sql"}},
            "observed": {"out_spec": {"provider": "sql"}},
        },
        resources={"rid1": {"data_ref": {"file": r"C:\\tmp\\file.txt"}}},
    )
    _write_bundle(case_path, payload)

    with pytest.raises(ValueError, match="Invalid resource path"):
        fixture_migrate(root=root, bucket=bucket, dry_run=False)


def test_resolve_user_case_input_prefers_existing_repo_relative(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = tmp_path / "repo"
    root = repo / "tests" / "fixtures" / "replay_cases"
    case_path = root / "known_bad" / "a.case.json"
    _write_bundle(case_path, _bundle_payload({"type": "replay_case", "v": 2, "id": "x", "input": {}}))
    monkeypatch.chdir(repo)

    resolved = resolve_user_case_input(root=root, case_path=Path("tests/fixtures/replay_cases/known_bad/a.case.json"))

    assert resolved == case_path.resolve()


def test_resolve_user_case_input_supports_root_relative(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = tmp_path / "repo"
    root = repo / "tests" / "fixtures" / "replay_cases"
    case_path = root / "known_bad" / "nested" / "a.case.json"
    _write_bundle(case_path, _bundle_payload({"type": "replay_case", "v": 2, "id": "x", "input": {}}))
    monkeypatch.chdir(repo)

    resolved = resolve_user_case_input(root=root, case_path=Path("known_bad/nested/a.case.json"))

    assert resolved == case_path.resolve()


def test_resolve_user_case_input_detects_ambiguous_relative_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = tmp_path / "repo"
    root = repo / "fixtures_root"
    cwd_case = repo / "known_bad" / "a.case.json"
    root_case = root / "known_bad" / "a.case.json"
    _write_bundle(cwd_case, _bundle_payload({"type": "replay_case", "v": 2, "id": "x", "input": {}}))
    _write_bundle(root_case, _bundle_payload({"type": "replay_case", "v": 2, "id": "x", "input": {}}))
    monkeypatch.chdir(repo)

    with pytest.raises(ValueError, match="Ambiguous relative case_path"):
        resolve_user_case_input(root=root, case_path=Path("known_bad/a.case.json"))


def test_parse_fixture_ref_rejects_path_outside_bucket(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    outside = tmp_path / "other" / "a.case.json"
    _write_bundle(outside, _bundle_payload({"type": "replay_case", "v": 2, "id": "x", "input": {}}))

    with pytest.raises(ValueError, match="outside valid buckets"):
        parse_fixture_ref(root=root, case_path=outside)


def test_fixture_fix_supports_nested_stem_rename(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    bucket = "fixed"
    case_path = root / bucket / "agg_003" / "nested" / "old.case.json"
    resources_dir = root / bucket / "resources" / "agg_003" / "nested" / "old" / "rid1"
    resources_dir.mkdir(parents=True, exist_ok=True)
    (resources_dir / "file.txt").write_text("data", encoding="utf-8")
    payload = _bundle_payload(
        {"type": "replay_case", "v": 2, "id": "plan_normalize.spec_v1", "input": {"spec": {"provider": "sql"}}},
        resources={"rid1": {"data_ref": {"file": "resources/agg_003/nested/old/rid1/file.txt"}}},
    )
    _write_bundle(case_path, payload)
    expected_path = root / bucket / "agg_003" / "nested" / "old.expected.json"
    expected_path.parent.mkdir(parents=True, exist_ok=True)
    expected_path.write_text("{}", encoding="utf-8")

    fixture_fix(
        root=root,
        name="agg_003/nested/old",
        new_name="agg_003/fixed/new",
        bucket=bucket,
        dry_run=False,
    )

    new_case = root / bucket / "agg_003" / "fixed" / "new.case.json"
    new_expected = root / bucket / "agg_003" / "fixed" / "new.expected.json"
    new_resource = root / bucket / "resources" / "agg_003" / "fixed" / "new" / "rid1" / "file.txt"
    assert new_case.exists()
    assert new_expected.exists()
    assert new_resource.exists()
    data = json.loads(new_case.read_text(encoding="utf-8"))
    assert data["resources"]["rid1"]["data_ref"]["file"] == "resources/agg_003/fixed/new/rid1/file.txt"


def test_fixture_green_accepts_repo_relative_case_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = tmp_path / "repo"
    root = repo / "tests" / "fixtures" / "replay_cases"
    case_path = root / "known_bad" / "agg_003" / "a.case.json"
    resources_dir = root / "known_bad" / "resources" / "agg_003" / "a"
    resources_dir.mkdir(parents=True, exist_ok=True)
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
    monkeypatch.chdir(repo)

    fixture_green(
        case_path=Path("tests/fixtures/replay_cases/known_bad/agg_003/a.case.json"),
        out_root=root,
        expected_from="replay",
    )

    assert (root / "fixed" / "agg_003" / "a.case.json").exists()
    assert (root / "fixed" / "resources" / "agg_003" / "a").exists()
