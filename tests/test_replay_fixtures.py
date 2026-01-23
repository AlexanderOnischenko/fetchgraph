from __future__ import annotations

import difflib
import json
import os
from pathlib import Path
from typing import Iterable

import pytest
from _pytest.mark.structures import ParameterSet
from pydantic import TypeAdapter

import fetchgraph.replay.handlers.plan_normalize  # noqa: F401
from fetchgraph.relational.models import RelationalRequest
from fetchgraph.replay.runtime import REPLAY_HANDLERS, ReplayContext

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "replay_points"
_BUCKETS = ("fixed", "known_bad")
_REL_ADAPTER = TypeAdapter(RelationalRequest)
DEBUG_REPLAY = os.getenv("DEBUG_REPLAY", "").lower() in ("1", "true", "yes", "on")


def _is_replay_payload(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("type") == "replay_point":
        return True
    if payload.get("type") == "replay_bundle":
        root = payload.get("root") or {}
        return isinstance(root, dict) and root.get("type") == "replay_point"
    return False


def _iter_fixture_paths() -> tuple[list[tuple[str, Path]], list[Path]]:
    if not FIXTURES_ROOT.exists():
        return [], []
    paths: list[tuple[str, Path]] = []
    ignored: list[Path] = []
    for path in FIXTURES_ROOT.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            ignored.append(path)
            continue
        if _is_replay_payload(payload):
            paths.append(("root", path))
        else:
            ignored.append(path)
    for bucket in _BUCKETS:
        bucket_dir = FIXTURES_ROOT / bucket
        if not bucket_dir.exists():
            continue
        for path in bucket_dir.rglob("*.json"):
            if "resources" in path.parts:
                ignored.append(path)
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                ignored.append(path)
                continue
            if _is_replay_payload(payload):
                paths.append((bucket, path))
            else:
                ignored.append(path)
    return sorted(paths), sorted(ignored)


def _format_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)


def _truncate(text: str, limit: int = 2000) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}\n... (truncated {len(text) - limit} chars)"


def _selectors_diff(expected: object, actual: object) -> str:
    expected_text = _format_json(expected).splitlines()
    actual_text = _format_json(actual).splitlines()
    diff = "\n".join(
        difflib.unified_diff(expected_text, actual_text, fromfile="expected", tofile="actual", lineterm="")
    )
    return _truncate(diff)


def _load_fixture(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_fixture(event: dict, *, base_dir: Path) -> tuple[dict, ReplayContext]:
    if event.get("type") == "replay_bundle":
        ctx = ReplayContext(
            resources=event.get("resources") or {},
            extras=event.get("extras") or {},
            base_dir=base_dir,
        )
        return event["root"], ctx
    return event, ReplayContext(base_dir=base_dir)


def _fixture_paths() -> list[ParameterSet]:
    paths, ignored = _iter_fixture_paths()
    if not paths:
        pytest.skip(
            "No replay fixtures found in tests/fixtures/replay_points/{fixed,known_bad}",
            allow_module_level=True,
        )
    if DEBUG_REPLAY and ignored:
        ignored_list = "\n".join(f"- {path}" for path in ignored)
        print(f"\n=== DEBUG ignored json files ===\n{ignored_list}")
    params: list[ParameterSet] = []
    for bucket, path in paths:
        marks = (pytest.mark.known_bad,) if bucket == "known_bad" else ()
        params.append(pytest.param((bucket, path), id=f"{bucket}/{path.name}", marks=marks))
    return params


def _rerun_hint(bucket: str, path: Path) -> str:
    return f"pytest -vv {__file__}::test_replay_fixture[{bucket}/{path.name}] -s"


@pytest.mark.parametrize("fixture_info", _fixture_paths())
def test_replay_fixture(fixture_info: tuple[str, Path]) -> None:
    bucket, path = fixture_info
    raw = _load_fixture(path)
    event, ctx = _parse_fixture(raw, base_dir=path.parent)
    assert event.get("type") == "replay_point"
    event_id = event.get("id")
    assert event_id in REPLAY_HANDLERS

    handler = REPLAY_HANDLERS[event_id]
    result = handler(event["input"], ctx)
    expected = event["expected"]

    actual_spec = result.get("out_spec")
    expected_spec = expected.get("out_spec")
    assert isinstance(expected_spec, dict)
    assert isinstance(actual_spec, dict)
    if DEBUG_REPLAY:
        print(f"\n=== DEBUG {path} ===")
        print("meta:", _format_json(event.get("meta")))
        print("note:", event.get("note"))
        print("input:", _truncate(_format_json(event.get("input")), 8000))
    if actual_spec != expected_spec:
        meta = _format_json(event.get("meta"))
        note = event.get("note")
        inp = _truncate(_format_json(event.get("input")), limit=8000)
        diff = _selectors_diff(expected_spec.get("selectors"), actual_spec.get("selectors"))
        pytest.fail(
            "\n".join(
                [
                    f"Replay mismatch for {path.name}",
                    f"rerun: {_rerun_hint(bucket, path)}",
                    f"meta: {meta}",
                    f"note: {note}",
                    "input:",
                    inp,
                    "selectors diff:",
                    diff,
                ]
            )
        )

    if event_id == "plan_normalize.spec_v1":
        provider = actual_spec.get("provider") or event["input"]["spec"]["provider"]
        rule_kind = (event["input"].get("normalizer_rules") or {}).get(provider)
        if rule_kind == "relational_v1":
            _REL_ADAPTER.validate_python(actual_spec["selectors"])


def test_replay_fixture_resources_exist() -> None:
    paths, _ = _iter_fixture_paths()
    resource_checks: list[tuple[Path, Path]] = []
    for _, path in paths:
        raw = _load_fixture(path)
        event, ctx = _parse_fixture(raw, base_dir=path.parent)
        if raw.get("type") != "replay_bundle":
            continue
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
            resolved = ctx.resolve_resource_path(file_name)
            resource_checks.append((path, resolved))

    if not resource_checks:
        pytest.skip("No replay fixtures with file resources found.")

    missing = [(fixture, resource) for fixture, resource in resource_checks if not resource.exists()]
    if missing:
        details = "\n".join(f"- {fixture}: {resource}" for fixture, resource in missing)
        pytest.fail(f"Missing replay resources:\n{details}")
