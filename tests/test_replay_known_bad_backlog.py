from __future__ import annotations

import json
import traceback
from pathlib import Path
from typing import Callable

import pytest
from pydantic import ValidationError

import fetchgraph.tracer.handlers  # noqa: F401
from fetchgraph.tracer.runtime import load_case_bundle, run_case
from fetchgraph.tracer.validators import validate_plan_normalize_spec_v1
from tests.helpers.replay_dx import (
    build_rerun_hints,
    debug_enabled,
    format_json,
    format_rule_trace,
    ids_from_path,
    rule_trace_tail,
    truncate,
    truncate_limits,
)

REPLAY_CASES_ROOT = Path(__file__).parent / "fixtures" / "replay_cases"
KNOWN_BAD_DIR = REPLAY_CASES_ROOT / "known_bad"

VALIDATORS: dict[str, Callable[[dict], None]] = {
    "plan_normalize.spec_v1": validate_plan_normalize_spec_v1,
}


def _iter_known_bad_paths() -> list[Path]:
    if not KNOWN_BAD_DIR.exists():
        return []
    return sorted(KNOWN_BAD_DIR.rglob("*.case.json"))


def _iter_case_params() -> list[object]:
    return [pytest.param(path, id=ids_from_path(path, REPLAY_CASES_ROOT)) for path in _iter_known_bad_paths()]


def _format_common_block(
    *,
    bundle_path: Path,
    root: dict,
    input_limit: int,
    meta_limit: int,
) -> list[str]:
    note = root.get("note")
    requires = root.get("requires")
    meta = root.get("meta")
    lines = [
        f"fixture: {bundle_path}",
        "bucket: known_bad",
        f"id: {root.get('id')!r}",
        "rerun:",
    ]
    lines.extend([f"  - {hint}" for hint in build_rerun_hints(bundle_path)])
    lines.append(f"meta: {truncate(format_json(meta, max_chars=meta_limit), limit=meta_limit)}")
    if note:
        lines.append(f"note: {truncate(format_json(note, max_chars=meta_limit), limit=meta_limit)}")
    lines.append(f"requires: {truncate(format_json(requires, max_chars=meta_limit), limit=meta_limit)}")
    lines.append(f"input: {truncate(format_json(root.get('input'), max_chars=input_limit), limit=input_limit)}")
    return lines


def _validate_root_schema(root: dict) -> None:
    if root.get("schema") != "fetchgraph.tracer.case_bundle":
        raise ValueError(f"Unexpected schema: {root.get('schema')!r}")
    if root.get("v") != 1:
        raise ValueError(f"Unexpected bundle version: {root.get('v')!r}")
    root_case = root.get("root")
    if not isinstance(root_case, dict):
        raise ValueError("Missing root replay_case entry.")
    if root_case.get("type") != "replay_case":
        raise ValueError(f"Unexpected root type: {root_case.get('type')!r}")
    if root_case.get("v") != 2:
        raise ValueError(f"Unexpected replay_case version: {root_case.get('v')!r}")


@pytest.mark.known_bad
@pytest.mark.parametrize("bundle_path", _iter_case_params())
def test_known_bad_backlog(bundle_path: Path) -> None:
    root, ctx = load_case_bundle(bundle_path)
    if not isinstance(root, dict):
        pytest.fail(f"Unexpected bundle payload type: {type(root)}", pytrace=False)
    _validate_root_schema(root)
    replay_id = root.get("id")
    validator = VALIDATORS.get(replay_id)
    if validator is None:
        pytest.fail(
            f"No validator registered for replay id={replay_id!r}. Add it to VALIDATORS.",
            pytrace=False,
        )
    input_limit, meta_limit = truncate_limits()
    common_block = _format_common_block(
        bundle_path=bundle_path,
        root=root,
        input_limit=input_limit,
        meta_limit=meta_limit,
    )
    if debug_enabled():
        print("\n".join(["KNOWN_BAD debug summary:"] + common_block))

    try:
        out = run_case(root, ctx)
    except Exception as exc:
        message = "\n".join(
            [
                "KNOWN_BAD (backlog): handler raised exception",
                *common_block,
                f"exception: {exc!r}",
                f"traceback:\n{truncate(traceback.format_exc(), limit=meta_limit)}",
            ]
        )
        pytest.fail(message, pytrace=False)

    try:
        validator(out)
    except (AssertionError, ValidationError) as exc:
        diag = out.get("diag") if isinstance(out, dict) else None
        rule_trace = format_rule_trace(diag, tail=rule_trace_tail())
        message = "\n".join(
            [
                "KNOWN_BAD (backlog): output is invalid",
                *common_block,
                f"validator_error: {exc!r}",
                f"rule_trace (tail): {rule_trace or 'N/A'}",
                f"out: {truncate(format_json(out, max_chars=input_limit), limit=input_limit)}",
            ]
        )
        pytest.fail(message, pytrace=False)
    except Exception as exc:
        message = "\n".join(
            [
                "KNOWN_BAD (backlog): validator raised unexpected exception",
                *common_block,
                f"validator_error: {exc!r}",
                f"traceback:\n{truncate(traceback.format_exc(), limit=meta_limit)}",
            ]
        )
        pytest.fail(message, pytrace=False)

    message = "\n".join(
        [
            "KNOWN_BAD is now PASSING. Promote to fixed",
            *common_block,
            "Promote this fixture to fixed (freeze expected) and remove it from known_bad.",
            f"Command: fetchgraph-tracer fixture-green --case {bundle_path} --validate",
            "expected will be created from root.observed",
        ]
    )
    pytest.fail(message, pytrace=False)
