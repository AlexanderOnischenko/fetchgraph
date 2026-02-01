from __future__ import annotations

from pathlib import Path

import pytest

from fetchgraph.replay.runtime import ReplayContext, run_case


def test_run_case_missing_handler_message() -> None:
    root = {"id": "unknown.handler", "input": {}}
    ctx = ReplayContext()
    with pytest.raises(KeyError, match="Did you import fetchgraph.tracer.handlers\\?"):
        run_case(root, ctx)


def test_resolve_resource_path_run_root_relative(tmp_path: Path) -> None:
    base_dir = tmp_path / "fixtures"
    base_dir.mkdir()
    fixture_stem = "case_1__abcd1234"
    resource_id = "schema_v1"
    resource_path = "cases/case_1_abcd/schema_snapshot.json"
    stored_path = base_dir / "resources" / fixture_stem / resource_id / resource_path
    stored_path.parent.mkdir(parents=True, exist_ok=True)
    stored_path.write_text("schema", encoding="utf-8")

    ctx = ReplayContext(
        resources={resource_id: {"data_ref": {"file": resource_path}}},
        base_dir=base_dir,
        fixture_stem=fixture_stem,
    )

    resolved = ctx.resolve_resource_path(resource_path)
    assert resolved == stored_path
    assert resolved.read_text(encoding="utf-8") == "schema"
