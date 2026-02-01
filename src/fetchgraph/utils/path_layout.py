from __future__ import annotations

"""Canonical run/case/resources layout helpers.

Definitions:
- run_root: <data>/.runs/runs/<run_dir>/
- case_dir: <run_root>/cases/<case_id>_<suffix>/
- events.jsonl: <case_dir>/events.jsonl

Resource contract:
- replay_resource.data_ref.file is a POSIX-relative path from run_root
- paths must be relative (no absolute paths, no parent traversal)
- run_root/cases uses case-insensitive match for "cases" to be Windows-safe
- use these helpers to validate or derive paths in exporters/emitter code
"""

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4


@dataclass(frozen=True)
class LayoutConfig:
    runs_subdir: str = ".runs/runs"
    cases_dirname: str = "cases"
    events_filename: str = "events.jsonl"


@dataclass(frozen=True)
class RunLayout:
    data_dir: Path
    run_root: Path
    run_dir_name: str
    run_id: str | None


@dataclass(frozen=True)
class CaseLayout:
    run: RunLayout
    case_dir: Path
    case_id: str
    suffix: str
    events_path: Path


def runs_root(data_dir: Path, cfg: LayoutConfig | None = None) -> Path:
    cfg = cfg or LayoutConfig()
    return data_dir / cfg.runs_subdir


def make_run_root(*, data_dir: Path, run_dir_name: str, cfg: LayoutConfig | None = None) -> RunLayout:
    cfg = cfg or LayoutConfig()
    if not run_dir_name:
        raise ValueError("run_dir_name is required")
    run_root = runs_root(data_dir, cfg) / run_dir_name
    return RunLayout(data_dir=data_dir, run_root=run_root, run_dir_name=run_dir_name, run_id=None)


def make_case_dir(
    *,
    run: RunLayout,
    case_id: str,
    suffix: str | None,
    cfg: LayoutConfig | None = None,
) -> CaseLayout:
    cfg = cfg or LayoutConfig()
    if not isinstance(case_id, str) or not case_id:
        raise ValueError("case_id must be a non-empty string")
    if "/" in case_id or "\\" in case_id:
        raise ValueError(f"case_id must be a simple segment: {case_id!r}")
    if suffix is None:
        suffix = uuid4().hex[:6]
    if "/" in suffix or "\\" in suffix:
        raise ValueError(f"suffix must be a simple segment: {suffix!r}")
    case_dir = run.run_root / cfg.cases_dirname / f"{case_id}_{suffix}"
    events_path = canonical_events_path(case_dir, cfg)
    return CaseLayout(run=run, case_dir=case_dir, case_id=case_id, suffix=suffix, events_path=events_path)


def ensure_dirs(run: RunLayout, case: CaseLayout | None = None, cfg: LayoutConfig | None = None) -> None:
    cfg = cfg or LayoutConfig()
    run.run_root.mkdir(parents=True, exist_ok=True)
    if case is None:
        cases_dir = run.run_root / cfg.cases_dirname
        cases_dir.mkdir(parents=True, exist_ok=True)
        return
    case.case_dir.parent.mkdir(parents=True, exist_ok=True)
    case.case_dir.mkdir(parents=True, exist_ok=True)


def run_root_from_case_dir(case_dir: Path, cfg: LayoutConfig | None = None) -> Path:
    cfg = cfg or LayoutConfig()
    case_dir = case_dir.resolve(strict=False)
    cases_dir = case_dir.parent
    if cases_dir.name.lower() != cfg.cases_dirname.lower():
        raise ValueError(f"case_dir must be under <run_root>/{cfg.cases_dirname}: {case_dir}")
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
    return run_root / LayoutConfig().cases_dirname / f"{case_id}_{suffix}"


def events_path_for_case(case_dir: Path) -> Path:
    return canonical_events_path(case_dir, LayoutConfig())


def find_case_dirs(run_root: Path, case_id: str, cfg: LayoutConfig | None = None) -> list[Path]:
    cfg = cfg or LayoutConfig()
    cases_dir = _find_cases_dir(run_root, cfg)
    if not cases_dir.exists():
        return []
    return sorted(cases_dir.glob(f"{case_id}_*"), key=lambda p: p.stat().st_mtime, reverse=True)


def canonical_events_path(case_dir: Path, cfg: LayoutConfig | None = None) -> Path:
    cfg = cfg or LayoutConfig()
    return case_dir / cfg.events_filename


def run_relative_posix_path(run_root: Path, path: Path) -> str:
    run_root = run_root.resolve(strict=False)
    path = path.resolve(strict=False)
    if not path.is_relative_to(run_root):
        raise ValueError(f"Path must be within run_root: {path} (run_root={run_root})")
    return path.relative_to(run_root).as_posix()


def validate_run_relative_posix(path_str: str) -> Path:
    if not isinstance(path_str, str) or not path_str:
        raise ValueError("path must be a non-empty string")
    if "\\" in path_str:
        raise ValueError(f"path must use POSIX separators: {path_str}")
    if path_str.startswith("//"):
        raise ValueError(f"path must not be UNC-like: {path_str}")
    rel = PurePosixPath(path_str)
    if rel.is_absolute():
        raise ValueError(f"path must be relative: {path_str}")
    if ".." in rel.parts:
        raise ValueError(f"path must not traverse parents: {path_str}")
    if any(":" in part for part in rel.parts):
        raise ValueError(f"path must not contain ':': {path_str}")
    first = rel.parts[0] if rel.parts else ""
    if len(first) == 2 and first[1] == ":" and first[0].isalpha():
        raise ValueError(f"path must not be a windows drive path: {path_str}")
    return Path(*rel.parts)


def _find_cases_dir(run_root: Path, cfg: LayoutConfig) -> Path:
    direct = run_root / cfg.cases_dirname
    if direct.exists():
        return direct
    if run_root.exists():
        for entry in run_root.iterdir():
            if entry.is_dir() and entry.name.lower() == cfg.cases_dirname.lower():
                return entry
    return direct


__all__ = [
    "CaseLayout",
    "LayoutConfig",
    "RunLayout",
    "case_dir_from_run_root",
    "canonical_events_path",
    "ensure_dirs",
    "events_path_for_case",
    "find_case_dirs",
    "make_case_dir",
    "make_run_root",
    "run_relative_posix_path",
    "run_root_from_case_dir",
    "runs_root",
    "validate_run_relative_posix",
]
