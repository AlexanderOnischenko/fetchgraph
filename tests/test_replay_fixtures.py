from __future__ import annotations

import difflib
import json
from pathlib import Path
from typing import Iterable

import pytest

import fetchgraph.replay.handlers.plan_normalize  # noqa: F401

from fetchgraph.replay.runtime import REPLAY_HANDLERS, ReplayContext

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "replay_points"
_BUCKETS = ("fixed", "known_bad")


def _iter_fixture_paths() -> Iterable[tuple[str, Path]]:
    if not FIXTURES_ROOT.exists():
        return []
    paths: list[tuple[str, Path]] = []
    for bucket in _BUCKETS:
        bucket_dir = FIXTURES_ROOT / bucket
        if not bucket_dir.exists():
            continue
        for path in bucket_dir.rglob("*.json"):
            paths.append((bucket, path))
    return sorted(paths)


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


def _parse_fixture(event: dict) -> tuple[dict, ReplayContext]:
    if event.get("type") == "replay_bundle":
        ctx = ReplayContext(
            resources=event.get("resources") or {},
            extras=event.get("extras") or {},
        )
        return event["root"], ctx
    return event, ReplayContext()


def _fixture_paths() -> list[pytest.ParameterSet]:
    paths = list(_iter_fixture_paths())
    if not paths:
        pytest.skip(
            "No replay fixtures found in tests/fixtures/replay_points/{fixed,known_bad}",
            allow_module_level=True,
        )
    params: list[pytest.ParameterSet] = []
    for bucket, path in paths:
        marks = (pytest.mark.known_bad,) if bucket == "known_bad" else ()
        params.append(pytest.param(path, id=f"{bucket}/{path.name}", marks=marks))
    return params


@pytest.mark.parametrize("path", _fixture_paths())
def test_replay_fixture(path: Path) -> None:
    raw = _load_fixture(path)
    event, ctx = _parse_fixture(raw)
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
    if actual_spec != expected_spec:
        meta = _format_json(event.get("meta"))
        note = event.get("note")
        diff = _selectors_diff(expected_spec.get("selectors"), actual_spec.get("selectors"))
        pytest.fail(
            "\n".join(
                [
                    f"Replay mismatch for {path.name}",
                    f"meta: {meta}",
                    f"note: {note}",
                    "selectors diff:",
                    diff,
                ]
            )
        )
