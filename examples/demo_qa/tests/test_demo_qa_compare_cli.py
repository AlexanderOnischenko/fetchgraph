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


def _write_results_file(path: Path, results: list[RunResult]) -> Path:
    write_results(path, results)
    return path


def _make_run(data_dir: Path, run_name: str, run_id: str, *, tag: str | None, with_results: bool, status: str) -> Path:
    run_dir = data_dir / ".runs" / "runs" / run_name
    case_dir = run_dir / "cases" / "case-1"
    case_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_meta.json").write_text(json.dumps({"run_id": run_id, "tag": tag}), encoding="utf-8")
    status_payload = {
        "id": "case-1",
        "question": "q",
        "status": status,
        "checked": True,
        "reason": None,
        "details": None,
        "artifacts_dir": str(case_dir),
        "duration_ms": 1,
        "tags": [tag] if tag else [],
    }
    (case_dir / "status.json").write_text(json.dumps(status_payload), encoding="utf-8")
    if with_results:
        write_results(run_dir / "results.jsonl", [_make_result("case-1", status)])
    return run_dir


def test_compare_resolves_effective_snapshots(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    data_dir = tmp_path / "data"
    _write_effective_snapshot(data_dir, "baseline", [_make_result("case-1", "ok")])
    _write_effective_snapshot(data_dir, "baseline_v2", [_make_result("case-1", "failed")])

    args = build_parser().parse_args(
        ["compare", "--data", str(data_dir), "--base", "tag:baseline", "--new", "tag:baseline_v2"]
    )
    exit_code = handle_compare(args)
    captured = capsys.readouterr().out

    assert exit_code == 0
    assert "Resolved BASE" in captured
    assert "kind: tag" in captured
    assert "case-1" in captured


def test_compare_reports_missing_effective_snapshot(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    data_dir = tmp_path / "data"
    args = build_parser().parse_args(["compare", "--data", str(data_dir), "--base", "tag:missing", "--new", "tag:next"])

    exit_code = handle_compare(args)
    captured = capsys.readouterr().err

    assert exit_code == 2
    assert "has no effective snapshot" in captured


def test_compare_requires_data_for_tag(capsys: pytest.CaptureFixture[str]) -> None:
    args = build_parser().parse_args(["compare", "--base", "tag:a", "--new", "tag:b"])

    exit_code = handle_compare(args)
    captured = capsys.readouterr().err

    assert exit_code == 2
    assert "requires --data" in captured


def test_compare_table_format_without_color(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    base = _write_results_file(tmp_path / "base.jsonl", [_make_result("case-1", "ok")])
    new = _write_results_file(tmp_path / "new.jsonl", [_make_result("case-1", "failed")])

    args = build_parser().parse_args(
        ["compare", "--base", str(base), "--new", str(new), "--format", "table", "--color", "never"]
    )
    exit_code = handle_compare(args)
    captured = capsys.readouterr().out

    assert exit_code == 0
    assert "Summary:" in captured
    assert "Top regressions" in captured
    assert "\x1b[" not in captured


def test_compare_json_format(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    base = _write_results_file(tmp_path / "base.jsonl", [_make_result("case-1", "failed")])
    new = _write_results_file(tmp_path / "new.jsonl", [_make_result("case-1", "ok")])

    args = build_parser().parse_args(["compare", "--base", str(base), "--new", str(new), "--format", "json"])
    exit_code = handle_compare(args)
    captured = capsys.readouterr().out

    assert exit_code == 0
    payload = json.loads(captured[captured.find("{"):])
    assert payload["summary"]["coverage"]["base_total_cases"] == 1
    assert payload["top_fixes"]


def test_compare_run_id_and_latest_tag_refs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    data_dir = tmp_path / "data"
    _make_run(data_dir, "20260306_195550_retail_cases_32d32108", "32d32108", tag="baseline", with_results=False, status="ok")
    _make_run(data_dir, "20260306_201512_retail_cases_44148564", "44148564", tag="qwen_pipeline", with_results=True, status="failed")

    args = build_parser().parse_args(
        ["compare", "--data", str(data_dir), "--base", "32d32108", "--new", "latest:qwen_pipeline"]
    )
    exit_code = handle_compare(args)
    captured = capsys.readouterr().out

    assert exit_code == 0
    assert "kind: run_id" in captured
    assert "kind: latest_tag_run" in captured
    assert "source: cases/**/status.json" in captured


def test_compare_latest_ref(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    data_dir = tmp_path / "data"
    _make_run(data_dir, "20260306_195550_retail_cases_32d32108", "32d32108", tag="baseline", with_results=True, status="ok")
    _make_run(data_dir, "20260306_201512_retail_cases_44148564", "44148564", tag="qwen_pipeline", with_results=True, status="failed")

    args = build_parser().parse_args(["compare", "--data", str(data_dir), "--base", "latest", "--new", "44148564"])
    exit_code = handle_compare(args)
    captured = capsys.readouterr().out

    assert exit_code == 0
    assert "kind: latest" in captured


def test_compare_reports_unsupported_ref(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    data_dir = tmp_path / "data"
    _make_run(data_dir, "20260306_195550_retail_cases_32d32108", "32d32108", tag="baseline", with_results=True, status="ok")

    args = build_parser().parse_args(["compare", "--data", str(data_dir), "--base", "foo:bar:baz", "--new", "32d32108"])
    exit_code = handle_compare(args)
    captured = capsys.readouterr().err

    assert exit_code == 2
    assert 'unsupported ref "foo:bar:baz"' in captured
