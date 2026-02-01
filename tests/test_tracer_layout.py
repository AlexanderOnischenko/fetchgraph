from __future__ import annotations

import os
from pathlib import Path

import pytest

from fetchgraph.utils.path_layout import (
    case_dir_from_run_root,
    events_path_for_case,
    find_case_dirs,
    run_relative_posix_path,
    run_root_from_case_dir,
    validate_run_relative_posix,
)


def test_case_dir_helpers(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    case_dir = case_dir_from_run_root(run_root, "case_1", "abc123")
    assert case_dir == run_root / "cases" / "case_1_abc123"
    assert events_path_for_case(case_dir) == case_dir / "events.jsonl"


def test_run_root_from_case_dir(tmp_path: Path) -> None:
    case_dir = tmp_path / "run" / "cases" / "case_2_def"
    case_dir.mkdir(parents=True, exist_ok=True)
    assert run_root_from_case_dir(case_dir) == tmp_path / "run"


def test_run_root_from_case_dir_rejects_invalid(tmp_path: Path) -> None:
    bad_case_dir = tmp_path / "run" / "case_3_bad"
    bad_case_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(ValueError, match="case_dir must be under <run_root>/cases"):
        run_root_from_case_dir(bad_case_dir)


def test_run_relative_posix_path(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    case_dir = run_root / "cases" / "case_4_ghi"
    case_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = case_dir / "schema_snapshot.json"
    snapshot_path.write_text("{}", encoding="utf-8")
    assert run_relative_posix_path(run_root, snapshot_path) == "cases/case_4_ghi/schema_snapshot.json"


def test_find_case_dirs_includes_direct_case_dir(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    cases_dir = run_root / "cases"
    cases_dir.mkdir(parents=True, exist_ok=True)
    direct = cases_dir / "case_5"
    suffixed = cases_dir / "case_5_abc"
    direct.mkdir()
    suffixed.mkdir()
    direct_mtime = 1000.0
    suffixed_mtime = 2000.0
    direct.touch()
    suffixed.touch()
    os.utime(direct, (direct_mtime, direct_mtime))
    os.utime(suffixed, (suffixed_mtime, suffixed_mtime))

    candidates = find_case_dirs(run_root, "case_5")

    assert candidates == [suffixed, direct]


@pytest.mark.parametrize(
    "path_str",
    [
        "/absolute/path.json",
        "../escape.json",
        "//server/share/file.json",
        "C:/temp/file.json",
        "c:/temp/file.json",
        "C:temp/file.json",
        "cases/case_1:bad/schema.json",
        r"cases\\case_1\\schema.json",
    ],
)
def test_validate_run_relative_posix_rejects(path_str: str) -> None:
    with pytest.raises(ValueError):
        validate_run_relative_posix(path_str)
