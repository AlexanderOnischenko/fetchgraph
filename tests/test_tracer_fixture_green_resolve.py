from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from fetchgraph.tracer.fixture_layout import find_case_bundles
from fetchgraph.tracer.fixture_tools import fixture_ls, resolve_fixture_candidates, select_fixture_candidate


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


def test_resolve_fixture_candidates_nested_stem_and_uniqueness(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    known_bad = root / "known_bad"
    case_a = known_bad / "agg_003" / "x" / "a.case.json"
    case_b = known_bad / "agg_003" / "y" / "a.case.json"
    _write_bundle(case_a, case_id="agg_003", timestamp="2024-01-01T00:00:00Z")
    _write_bundle(case_b, case_id="agg_003", timestamp="2024-01-02T00:00:00Z")

    candidates = resolve_fixture_candidates(root=root, bucket="known_bad", case_id="agg_003", name=None)

    stems = {item.stem for item in candidates}
    assert stems == {"agg_003/x/a", "agg_003/y/a"}
    with pytest.raises(FileExistsError):
        select_fixture_candidate(candidates, require_unique=True)


def test_fixture_ls_and_find_case_bundles_support_nested_patterns(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    known_bad = root / "known_bad"
    _write_bundle(known_bad / "agg_003" / "nested" / "a.case.json", case_id="agg_003", timestamp="2024-01-01T00:00:00Z")
    _write_bundle(known_bad / "agg_003" / "other" / "b.case.json", case_id="agg_003", timestamp="2024-01-01T00:00:00Z")

    by_name = find_case_bundles(root=root, bucket="known_bad", name="agg_003/nested/a", pattern=None)
    by_pattern = find_case_bundles(root=root, bucket="known_bad", name=None, pattern="agg_003/**")
    listed = fixture_ls(root=root, bucket="known_bad", pattern="**/a")

    assert len(by_name) == 1
    assert by_name[0].as_posix().endswith("known_bad/agg_003/nested/a.case.json")
    assert {p.name for p in by_pattern} == {"a.case.json", "b.case.json"}
    assert [c.stem for c in listed] == ["agg_003/nested/a"]


def test_find_case_bundles_globstar_semantics(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    known_bad = root / "known_bad"
    _write_bundle(known_bad / "a.case.json", case_id="x", timestamp="2024-01-01T00:00:00Z")
    _write_bundle(known_bad / "x" / "a.case.json", case_id="x", timestamp="2024-01-01T00:00:00Z")
    _write_bundle(known_bad / "agg_003" / "a.case.json", case_id="agg_003", timestamp="2024-01-01T00:00:00Z")
    _write_bundle(known_bad / "agg_003" / "x" / "a.case.json", case_id="agg_003", timestamp="2024-01-01T00:00:00Z")

    p1 = find_case_bundles(root=root, bucket="known_bad", name=None, pattern="**/a")
    p2 = find_case_bundles(root=root, bucket="known_bad", name=None, pattern="agg_003/**")
    p3 = find_case_bundles(root=root, bucket="known_bad", name=None, pattern="agg_003/**/a")

    stems1 = {p.relative_to(known_bad).as_posix().removesuffix('.case.json') for p in p1}
    stems2 = {p.relative_to(known_bad).as_posix().removesuffix('.case.json') for p in p2}
    stems3 = {p.relative_to(known_bad).as_posix().removesuffix('.case.json') for p in p3}

    assert {"a", "x/a", "agg_003/a", "agg_003/x/a"}.issubset(stems1)
    assert stems2 == {"agg_003/a", "agg_003/x/a"}
    assert stems3 == {"agg_003/a", "agg_003/x/a"}
