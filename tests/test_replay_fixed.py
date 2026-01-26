from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

import fetchgraph.tracer.handlers  # noqa: F401
import tests.helpers.handlers_resource_read  # noqa: F401
from fetchgraph.tracer.runtime import load_case_bundle, run_case
from tests.helpers.replay_dx import (
    format_json,
    ids_from_path,
    truncate,
    truncate_limits,
)

REPLAY_CASES_ROOT = Path(__file__).parent / "fixtures" / "replay_cases"
FIXED_DIR = REPLAY_CASES_ROOT / "fixed"


def _iter_fixed_paths() -> list[Path]:
    if not FIXED_DIR.exists():
        return []
    return sorted(FIXED_DIR.rglob("*.case.json"))


def _iter_case_params() -> list[object]:
    return [pytest.param(path, id=ids_from_path(path, REPLAY_CASES_ROOT)) for path in _iter_fixed_paths()]


def _expected_path(case_path: Path) -> Path:
    if not case_path.name.endswith(".case.json"):
        raise ValueError(f"Unexpected case filename: {case_path}")
    return case_path.with_name(case_path.name.replace(".case.json", ".expected.json"))


def _first_diff_path(left: object, right: object, *, prefix: str = "") -> str | None:
    if type(left) is not type(right):
        return prefix or "<root>"
    if isinstance(left, dict):
        left_keys = set(left.keys())
        right_keys = set(right.keys())
        for key in sorted(left_keys | right_keys):
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            if key not in left or key not in right:
                return next_prefix
            diff = _first_diff_path(left[key], right[key], prefix=next_prefix)
            if diff is not None:
                return diff
        return None
    if isinstance(left, list):
        if len(left) != len(right):
            return f"{prefix}.length" if prefix else "length"
        for idx, (l_item, r_item) in enumerate(zip(left, right), start=1):
            next_prefix = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
            diff = _first_diff_path(l_item, r_item, prefix=next_prefix)
            if diff is not None:
                return diff
        return None
    if left != right:
        return prefix or "<root>"
    return None


@pytest.mark.parametrize("case_path", _iter_case_params())
def test_replay_fixed_cases(case_path: Path) -> None:
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
    if out != expected:
        input_limit, meta_limit = truncate_limits()
        meta = root.get("meta") if isinstance(root, dict) else None
        note = root.get("note") if isinstance(root, dict) else None
        missing_keys = []
        extra_keys = []
        if isinstance(out, dict) and isinstance(expected, dict):
            out_keys = set(out.keys())
            expected_keys = set(expected.keys())
            missing_keys = sorted(expected_keys - out_keys)
            extra_keys = sorted(out_keys - expected_keys)
        out_spec_path = None
        if isinstance(out, dict) and isinstance(expected, dict):
            out_spec = out.get("out_spec")
            expected_spec = expected.get("out_spec")
            if out_spec != expected_spec:
                out_spec_path = _first_diff_path(out_spec, expected_spec, prefix="out_spec")
        message = "\n".join(
            [
                "Fixed fixture mismatch.",
                f"fixture: {case_path}",
                f"meta: {truncate(format_json(meta, max_chars=meta_limit), limit=meta_limit)}",
                f"note: {truncate(format_json(note, max_chars=meta_limit), limit=meta_limit)}",
                f"out: {truncate(format_json(out, max_chars=input_limit), limit=input_limit)}",
                f"expected: {truncate(format_json(expected, max_chars=input_limit), limit=input_limit)}",
                f"missing_keys: {missing_keys}",
                f"extra_keys: {extra_keys}",
                f"out_spec_path: {out_spec_path}",
            ]
        )
        pytest.fail(message, pytrace=False)
    assert out == expected


def test_replay_case_resources_exist() -> None:
    case_paths = _iter_fixed_paths()
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


def test_resource_read_missing_file(tmp_path: Path) -> None:
    bundle_path = FIXED_DIR / "resource_read.v1__sample.case.json"
    if not bundle_path.exists():
        pytest.skip("Resource read fixture not available.")
    target_bundle = tmp_path / bundle_path.name
    resources_dir = FIXED_DIR / "resources" / "resource_read.v1__sample"
    target_resources = tmp_path / "resources" / "resource_read.v1__sample"
    target_resources.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(bundle_path, target_bundle)
    shutil.copytree(resources_dir, target_resources)

    missing_path = target_resources / "sample" / "sample.txt"
    if missing_path.exists():
        missing_path.unlink()

    root, ctx = load_case_bundle(target_bundle)
    with pytest.raises(FileNotFoundError):
        run_case(root, ctx)
