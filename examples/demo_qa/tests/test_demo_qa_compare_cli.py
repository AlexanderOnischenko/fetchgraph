from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest


def _install_pydantic_settings_stub() -> None:
    if "pydantic_settings" in sys.modules:
        return
    module = types.ModuleType("pydantic_settings")

    class _BaseSettings:
        def __init__(self, **kwargs: object) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

    def _settings_config_dict(**kwargs: object) -> dict:
        return dict(**kwargs)

    module.BaseSettings = _BaseSettings
    module.SettingsConfigDict = _settings_config_dict

    sources = types.ModuleType("pydantic_settings.sources")

    class _TomlConfigSettingsSource:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

    sources.TomlConfigSettingsSource = _TomlConfigSettingsSource

    sys.modules["pydantic_settings"] = module
    sys.modules["pydantic_settings.sources"] = sources


_install_pydantic_settings_stub()

from examples.demo_qa.batch import handle_compare
from examples.demo_qa.cli import build_parser
from examples.demo_qa.runner import RunResult
from examples.demo_qa.runs.io import write_results
from examples.demo_qa.runs.layout import _effective_paths


def _make_result(case_id: str, status: str) -> RunResult:
    return RunResult(
        id=case_id,
        question="q",
        status=status,
        checked=True,
        reason=None,
        details=None,
        artifacts_dir="/tmp",
        duration_ms=0,
        tags=[],
    )


def _write_effective_snapshot(data_dir: Path, tag: str, results: list[RunResult]) -> Path:
    artifacts_dir = data_dir / ".runs"
    results_path, meta_path = _effective_paths(artifacts_dir, tag)
    write_results(results_path, results)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps({"tag": tag}), encoding="utf-8")
    return results_path


def test_compare_resolves_effective_snapshots(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    data_dir = tmp_path / "data"
    _write_effective_snapshot(data_dir, "baseline", [_make_result("case-1", "ok")])
    _write_effective_snapshot(data_dir, "baseline_v2", [_make_result("case-1", "failed")])

    args = build_parser().parse_args(
        ["compare", "--data", str(data_dir), "--base-tag", "baseline", "--new-tag", "baseline_v2"]
    )
    exit_code = handle_compare(args)
    captured = capsys.readouterr().out

    assert exit_code == 0
    assert "case-1" in captured


def test_compare_reports_missing_effective_snapshot(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    data_dir = tmp_path / "data"
    args = build_parser().parse_args(["compare", "--data", str(data_dir), "--base-tag", "missing", "--new-tag", "next"])

    exit_code = handle_compare(args)
    captured = capsys.readouterr().err

    assert exit_code == 2
    assert "No effective snapshot found" in captured


def test_compare_requires_data_for_tag(capsys: pytest.CaptureFixture[str]) -> None:
    args = build_parser().parse_args(["compare", "--base-tag", "a", "--new-tag", "b"])

    exit_code = handle_compare(args)
    captured = capsys.readouterr().err

    assert exit_code == 2
    assert "--data is required" in captured


def test_compare_rejects_mixed_base_args(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    base = tmp_path / "base.jsonl"
    new = tmp_path / "new.jsonl"
    base.write_text("{}", encoding="utf-8")
    new.write_text("{}", encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(["compare", "--base", str(base), "--base-tag", "tag", "--new", str(new)])
    assert excinfo.value.code == 2
    captured = capsys.readouterr().err
    assert "argument --base-tag: not allowed with argument --base" in captured
