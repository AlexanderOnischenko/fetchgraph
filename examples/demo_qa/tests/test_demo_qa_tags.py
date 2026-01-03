from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace


def _install_fake_pydantic_settings() -> None:
    if "pydantic_settings" in sys.modules:
        return
    from pydantic import BaseModel

    fake_settings = types.ModuleType("pydantic_settings")
    fake_settings.BaseSettings = BaseModel
    fake_settings.SettingsConfigDict = dict
    fake_sources = types.ModuleType("pydantic_settings.sources")
    fake_sources.TomlConfigSettingsSource = object
    sys.modules["pydantic_settings"] = fake_settings
    sys.modules["pydantic_settings.sources"] = fake_sources


def _write_meta(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_tags_list_outputs_columns_and_filters(tmp_path: Path, capsys) -> None:
    _install_fake_pydantic_settings()
    tags_module = importlib.import_module("examples.demo_qa.commands.tags")
    handle_tags_list = tags_module.handle_tags_list

    artifacts = tmp_path / ".runs"
    tag_a = artifacts / "runs" / "tags" / "alpha"
    tag_b = artifacts / "runs" / "tags" / "beta"
    run_dir = artifacts / "runs" / "20240101_cases"
    run_dir.mkdir(parents=True)
    (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "r1", "note": "hello note"}), encoding="utf-8")

    _write_meta(
        tag_a / "effective_meta.json",
        {
            "tag": "alpha",
            "updated_at": "2024-01-02T00:00:00Z",
            "planned_total": 10,
            "executed_total": 8,
            "missed_total": 2,
            "counts": {"ok": 8, "total": 10, "skipped": 0},
            "built_from_runs": [str(run_dir)],
        },
    )
    _write_meta(
        tag_b / "effective_meta.json",
        {
            "tag": "beta",
            "updated_at": "2024-01-01T00:00:00Z",
            "planned_total": 5,
            "executed_total": 5,
            "missed_total": 0,
            "counts": {"ok": 5, "total": 5, "skipped": 0},
        },
    )

    args = SimpleNamespace(data=tmp_path, tags_command="list", pattern="a*", limit=None, sort="name")
    exit_code = handle_tags_list(args)
    out = capsys.readouterr().out

    assert exit_code == 0
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == 2  # header + one row due to pattern
    header = lines[0]
    assert "tag" in header and "updated_at" in header and "plan/exe/miss" in header
    row = lines[1]
    assert "alpha" in row
    assert "10/8/2" in row
    assert "80.0%" in row
    assert "r1" in row
    assert "hello note" in row
    assert "beta" not in out
