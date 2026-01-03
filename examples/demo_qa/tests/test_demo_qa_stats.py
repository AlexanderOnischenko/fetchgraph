from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace


def _write_history(tmp_path: Path, entries: list[dict]) -> Path:
    history_path = tmp_path / ".runs" / "history.jsonl"
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")
    return history_path


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


def test_stats_output_includes_metadata_and_truncates_notes(tmp_path: Path, capsys) -> None:
    _install_fake_pydantic_settings()
    handle_stats = importlib.import_module("examples.demo_qa.batch").handle_stats
    long_note = (
        "This is a very long note that should be truncated because it exceeds the maximum width "
        "of the column in the stats output."
    )
    entries = [
        {
            "run_id": "run123",
            "timestamp": "2024-01-01T00:00:00Z",
            "run_status": "FAILED",
            "tag": "demo-tag",
            "note": long_note,
            "fail_count": 5,
            "pass_rate": 0.5,
            "planned_total": 10,
            "executed_total": 8,
        }
    ]
    _write_history(tmp_path, entries)
    args = SimpleNamespace(history=None, data=tmp_path, last=10, group_by=None, color="never")

    exit_code = handle_stats(args)
    out = capsys.readouterr().out

    assert exit_code == 0
    lines = [line for line in out.splitlines() if line.strip()]
    assert lines, "Expected stats output"
    header = lines[0]
    assert "timestamp" in header
    assert "run_id" in header
    assert "status" in header
    assert "tag" in header
    assert "note" in header
    row = lines[1]
    assert "run123" in row
    assert "FAILED" in row
    assert "demo-tag" in row
    assert "80.0%" in row  # coverage
    assert "50.0%" in row  # pass rate
    assert "…" in row  # truncated note
    assert long_note not in row
    assert "\x1b[" not in out
