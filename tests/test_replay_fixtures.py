from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

import fetchgraph.tracer.handlers  # noqa: F401
from fetchgraph.tracer.runtime import load_case_bundle, run_case
from fetchgraph.tracer.validators import validate_plan_normalize_spec_v1

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "replay_cases"
KNOWN_BAD_DIR = FIXTURES_ROOT / "known_bad"
FIXED_DIR = FIXTURES_ROOT / "fixed"


def _iter_case_paths(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob("*.case.json"))


def _expected_path(case_path: Path) -> Path:
    if not case_path.name.endswith(".case.json"):
        raise ValueError(f"Unexpected case filename: {case_path}")
    return case_path.with_name(case_path.name.replace(".case.json", ".expected.json"))


@pytest.mark.known_bad
@pytest.mark.parametrize("case_path", _iter_case_paths(KNOWN_BAD_DIR))
def test_known_bad_cases(case_path: Path) -> None:
    root, ctx = load_case_bundle(case_path)
    out = run_case(root, ctx)
    with pytest.raises((AssertionError, ValidationError)):
        validate_plan_normalize_spec_v1(out)


@pytest.mark.parametrize("case_path", _iter_case_paths(FIXED_DIR))
def test_replay_cases_expected(case_path: Path) -> None:
    expected_path = _expected_path(case_path)
    if not expected_path.exists():
        pytest.skip(f"Expected fixture missing: {expected_path}")
    root, ctx = load_case_bundle(case_path)
    out = run_case(root, ctx)
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    assert out == expected
