from __future__ import annotations

from pathlib import Path

from fetchgraph.replay.runtime import ReplayContext


def test_resolve_resource_path(tmp_path) -> None:
    ctx = ReplayContext(base_dir=tmp_path)

    rel_path = Path("resources/sample.json")
    assert ctx.resolve_resource_path(rel_path) == tmp_path / rel_path

    abs_path = Path("/var/data/sample.json")
    assert ctx.resolve_resource_path(abs_path) == abs_path
