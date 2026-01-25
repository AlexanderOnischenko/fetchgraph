from __future__ import annotations

import json
import traceback
from pathlib import Path

import pytest
from pydantic import ValidationError

import fetchgraph.tracer.handlers  # noqa: F401
from fetchgraph.tracer.runtime import load_case_bundle, run_case
from fetchgraph.tracer.validators import validate_plan_normalize_spec_v1

REPLAY_CASES_ROOT = Path(__file__).parent / "fixtures" / "replay_cases"
KNOWN_BAD_DIR = REPLAY_CASES_ROOT / "known_bad"
FIXED_DIR = REPLAY_CASES_ROOT / "fixed"


def _iter_case_paths(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.rglob("*.case.json"))


def _case_id(case_path: Path, base: Path) -> str:
    try:
        return str(case_path.relative_to(base))
    except ValueError:
        return case_path.stem


def _iter_case_params(directory: Path, base: Path) -> list[pytest.ParamSpec]:
    return [pytest.param(path, id=_case_id(path, base)) for path in _iter_case_paths(directory)]


def _iter_all_case_paths() -> list[Path]:
    return _iter_case_paths(FIXED_DIR) + _iter_case_paths(KNOWN_BAD_DIR)


def test_replay_cases_present() -> None:
    if not _iter_all_case_paths():
        pytest.skip("No replay case bundles found under tests/fixtures/replay_cases")


def _expected_path(case_path: Path) -> Path:
    if not case_path.name.endswith(".case.json"):
        raise ValueError(f"Unexpected case filename: {case_path}")
    return case_path.with_name(case_path.name.replace(".case.json", ".expected.json"))


def format_json(obj: object, *, max_chars: int = 10_000, max_depth: int = 6) -> str:
    def _prune(value: object, depth: int) -> object:
        if depth <= 0:
            return "...(max depth reached)"
        if isinstance(value, dict):
            return {str(key): _prune(val, depth - 1) for key, val in value.items()}
        if isinstance(value, list):
            return [_prune(item, depth - 1) for item in value]
        if isinstance(value, tuple):
            return tuple(_prune(item, depth - 1) for item in value)
        return value

    text = json.dumps(
        _prune(obj, max_depth),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        default=str,
    )
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}...(truncated)"


def _format_case_debug(
    *,
    case_path: Path,
    case_id: str,
    root: object,
    out: object | None = None,
    exc: BaseException | None = None,
    tb: str | None = None,
) -> str:
    root_id = root.get("id") if isinstance(root, dict) else None
    root_meta = root.get("meta") if isinstance(root, dict) else None
    root_input = root.get("input") if isinstance(root, dict) else None
    lines = [
        "Replay fixture diagnostics:",
        f"case_id: {case_id}",
        f"case_path: {case_path}",
        f"root.id: {root_id!r}",
        f"root.meta: {format_json(root_meta)}",
        f"root.input: {format_json(root_input)}",
    ]
    if out is not None:
        lines.append(f"handler.out: {format_json(out)}")
    if exc is not None:
        lines.append(f"handler.exc: {type(exc).__name__}: {exc}")
    if tb:
        lines.append(f"handler.traceback:\n{tb}")
    return "\n".join(lines)


@pytest.mark.known_bad
@pytest.mark.parametrize("case_path", _iter_case_params(KNOWN_BAD_DIR, REPLAY_CASES_ROOT))
def test_known_bad_cases(case_path: Path) -> None:
    root, ctx = load_case_bundle(case_path)
    case_id = _case_id(case_path, REPLAY_CASES_ROOT)
    try:
        out = run_case(root, ctx)
    except Exception as exc:
        pytest.fail(
            _format_case_debug(
                case_path=case_path,
                case_id=case_id,
                root=root,
                exc=exc,
                tb=traceback.format_exc(),
            ),
            pytrace=False,
        )
    try:
        validate_plan_normalize_spec_v1(out)
    except (AssertionError, ValidationError):
        return
    except Exception as exc:
        pytest.fail(
            _format_case_debug(
                case_path=case_path,
                case_id=case_id,
                root=root,
                out=out,
                exc=exc,
                tb=traceback.format_exc(),
            ),
            pytrace=False,
        )
    pytest.fail(
        "DID NOT RAISE: expected validate_plan_normalize_spec_v1 to fail.\n"
        + _format_case_debug(
            case_path=case_path,
            case_id=case_id,
            root=root,
            out=out,
        ),
        pytrace=False,
    )


@pytest.mark.parametrize("case_path", _iter_case_params(FIXED_DIR, REPLAY_CASES_ROOT))
def test_replay_cases_expected(case_path: Path) -> None:
    expected_path = _expected_path(case_path)
    if not expected_path.exists():
        pytest.fail(
            "Expected file is required for fixed fixtures:\n"
            f"  missing: {expected_path}\n"
            "Hint: run `fetchgraph-tracer fixture-green --case ...` or create expected json manually."
        )
    root, ctx = load_case_bundle(case_path)
    out = run_case(root, ctx)
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    assert out == expected


def test_replay_case_resources_exist() -> None:
    case_paths = _iter_all_case_paths()
    if not case_paths:
        pytest.skip("No replay case bundles found.")
    missing: list[tuple[Path, Path]] = []
    for case_path in case_paths:
        raw = json.loads(case_path.read_text(encoding="utf-8"))
        resources = raw.get("resources") or {}
        if not isinstance(resources, dict):
            continue
        for resource in resources.values():
            if not isinstance(resource, dict):
                continue
            data_ref = resource.get("data_ref")
            if not isinstance(data_ref, dict):
                continue
            file_name = data_ref.get("file")
            if not isinstance(file_name, str) or not file_name:
                continue
            resolved = case_path.parent / file_name
            if not resolved.exists():
                missing.append((case_path, resolved))
    if missing:
        details = "\n".join(f"- {fixture}: {resource}" for fixture, resource in missing)
        pytest.fail(f"Missing replay resources:\n{details}")
