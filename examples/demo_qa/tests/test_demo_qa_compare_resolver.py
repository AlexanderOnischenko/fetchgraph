from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from examples.demo_qa.compare_resolver import load_results_for_run_dir, resolve_compare_input
from examples.demo_qa.runner import RunResult
from examples.demo_qa.runs.io import write_results
from examples.demo_qa.runs.layout import _effective_paths


def _result(case_id: str, status: str) -> RunResult:
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


def _mk_run(data_dir: Path, name: str, run_id: str, *, tag: str | None, with_results: bool, status: str) -> Path:
    run_dir = data_dir / ".runs" / "runs" / name
    case_dir = run_dir / "cases" / "case-1"
    case_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_meta.json").write_text(json.dumps({"run_id": run_id, "tag": tag}), encoding="utf-8")
    (case_dir / "status.json").write_text(
        json.dumps({"id": "case-1", "status": status, "artifacts_dir": str(case_dir), "checked": True}),
        encoding="utf-8",
    )
    if with_results:
        write_results(run_dir / "results.jsonl", [_result("case-1", status)])
    return run_dir


def test_resolve_explicit_file(tmp_path: Path) -> None:
    path = tmp_path / "base.results.jsonl"
    write_results(path, [_result("case-1", "ok")])
    resolved = resolve_compare_input(str(path), data_dir=None)
    assert resolved.kind == "results_file"


def test_resolve_explicit_run_dir(tmp_path: Path) -> None:
    run_dir = _mk_run(tmp_path, "run_1", "abc1", tag="t1", with_results=False, status="ok")
    resolved = resolve_compare_input(str(run_dir), data_dir=tmp_path)
    assert resolved.kind == "run_dir"
    assert resolved.source_description == "cases/**/status.json"


def test_resolve_run_id(tmp_path: Path) -> None:
    _mk_run(tmp_path, "run_1_abc1", "abc1", tag=None, with_results=True, status="ok")
    resolved = resolve_compare_input("abc1", data_dir=tmp_path)
    assert resolved.kind == "run_id"


def test_resolve_latest(tmp_path: Path) -> None:
    _mk_run(tmp_path, "2020_aaa", "aaa1", tag=None, with_results=True, status="ok")
    _mk_run(tmp_path, "2021_bbb", "bbb1", tag=None, with_results=True, status="failed")
    resolved = resolve_compare_input("latest", data_dir=tmp_path)
    assert resolved.kind == "latest"


def test_resolve_tag(tmp_path: Path) -> None:
    results_path, meta_path = _effective_paths(tmp_path / ".runs", "baseline")
    write_results(results_path, [_result("case-1", "ok")])
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text("{}", encoding="utf-8")
    resolved = resolve_compare_input("tag:baseline", data_dir=tmp_path)
    assert resolved.kind == "tag"


def test_resolve_latest_tag(tmp_path: Path) -> None:
    _mk_run(tmp_path, "2020_aaa", "aaa1", tag="baseline", with_results=True, status="ok")
    _mk_run(tmp_path, "2021_bbb", "bbb1", tag="baseline", with_results=True, status="failed")
    resolved = resolve_compare_input("latest:baseline", data_dir=tmp_path)
    assert resolved.kind == "latest_tag_run"


def test_missing_ref_errors(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported ref"):
        resolve_compare_input("???", data_dir=tmp_path)


def test_ambiguous_run_id_errors(tmp_path: Path) -> None:
    _mk_run(tmp_path, "run_1_abc1", "abc1", tag=None, with_results=True, status="ok")
    _mk_run(tmp_path, "run_2_abc1", "abc1", tag=None, with_results=True, status="ok")
    with pytest.raises(ValueError, match="resolved to multiple"):
        resolve_compare_input("abc1", data_dir=tmp_path)


def test_load_results_prefers_results_jsonl(tmp_path: Path) -> None:
    run_dir = _mk_run(tmp_path, "run_1", "abc1", tag=None, with_results=True, status="ok")
    results, source, _ = load_results_for_run_dir(run_dir)
    assert source == "results.jsonl"
    assert results["case-1"].status == "ok"


def test_load_results_falls_back_to_status_json(tmp_path: Path) -> None:
    run_dir = _mk_run(tmp_path, "run_1", "abc1", tag=None, with_results=False, status="failed")
    results, source, _ = load_results_for_run_dir(run_dir)
    assert source == "cases/**/status.json"
    assert results["case-1"].status == "failed"


def test_tag_requires_effective_meta(tmp_path: Path) -> None:
    results_path, _ = _effective_paths(tmp_path / ".runs", "baseline")
    write_results(results_path, [_result("case-1", "ok")])
    with pytest.raises(ValueError, match="has no effective snapshot"):
        resolve_compare_input("tag:baseline", data_dir=tmp_path)


def test_latest_prefers_run_name_timestamp_over_mtime(tmp_path: Path) -> None:
    older_name = _mk_run(tmp_path, "20260306_195550_cases_aaa1", "aaa1", tag="baseline", with_results=True, status="ok")
    newer_name = _mk_run(tmp_path, "20260306_201512_cases_bbb1", "bbb1", tag="baseline", with_results=True, status="failed")

    now = 1_900_000_000
    os.utime(older_name, (now + 10_000, now + 10_000))
    os.utime(newer_name, (now, now))

    resolved = resolve_compare_input("latest:baseline", data_dir=tmp_path)
    assert resolved.run_id == "bbb1"
