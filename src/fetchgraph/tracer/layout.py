from __future__ import annotations

from pathlib import Path, PurePosixPath


def run_root_from_case_dir(case_dir: Path) -> Path:
    case_dir = case_dir.resolve()
    cases_dir = case_dir.parent
    if cases_dir.name != "cases":
        raise ValueError(f"case_dir must be under <run_root>/cases: {case_dir}")
    run_root = cases_dir.parent
    if run_root == cases_dir:
        raise ValueError(f"case_dir missing run root: {case_dir}")
    return run_root


def case_dir_from_run_root(run_root: Path, case_id: str, suffix: str) -> Path:
    if not isinstance(case_id, str) or not case_id:
        raise ValueError("case_id must be a non-empty string")
    if not isinstance(suffix, str) or not suffix:
        raise ValueError("suffix must be a non-empty string")
    if "/" in case_id or "\\" in case_id:
        raise ValueError(f"case_id must be a simple segment: {case_id!r}")
    if "/" in suffix or "\\" in suffix:
        raise ValueError(f"suffix must be a simple segment: {suffix!r}")
    return run_root / "cases" / f"{case_id}_{suffix}"


def events_path_for_case(case_dir: Path) -> Path:
    return case_dir / "events.jsonl"


def run_relative_posix_path(run_root: Path, path: Path) -> str:
    run_root = run_root.resolve()
    path = path.resolve()
    if not path.is_relative_to(run_root):
        raise ValueError(f"Path must be within run_root: {path} (run_root={run_root})")
    return path.relative_to(run_root).as_posix()


def validate_run_relative_posix(path_str: str) -> Path:
    if not isinstance(path_str, str) or not path_str:
        raise ValueError("path must be a non-empty string")
    if "\\" in path_str:
        raise ValueError(f"path must use POSIX separators: {path_str}")
    rel = PurePosixPath(path_str)
    if rel.is_absolute():
        raise ValueError(f"path must be relative: {path_str}")
    if ".." in rel.parts:
        raise ValueError(f"path must not traverse parents: {path_str}")
    return Path(*rel.parts)


__all__ = [
    "case_dir_from_run_root",
    "events_path_for_case",
    "run_relative_posix_path",
    "run_root_from_case_dir",
    "validate_run_relative_posix",
]
