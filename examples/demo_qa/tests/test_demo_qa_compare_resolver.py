from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from examples.demo_qa.compare_resolver import is_comparable_run_dir, load_results_for_run_dir, resolve_compare_input
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


def test_existing_dir_but_not_run_is_invalid(tmp_path: Path) -> None:
    bad_dir = tmp_path / "just_dir"
    bad_dir.mkdir()
    ok, reason = is_comparable_run_dir(bad_dir)
    assert not ok
    assert "missing" in reason
    with pytest.raises(ValueError, match="not comparable"):
        resolve_compare_input(str(bad_dir), data_dir=tmp_path)


def test_run_dir_with_empty_results_is_invalid(tmp_path: Path) -> None:
    run_dir = tmp_path / ".runs" / "runs" / "run_empty"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "abcd"}), encoding="utf-8")
    (run_dir / "results.jsonl").write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="not comparable"):
        resolve_compare_input(str(run_dir), data_dir=tmp_path)


def test_run_dir_with_no_results_and_no_statuses_invalid(tmp_path: Path) -> None:
    run_dir = tmp_path / ".runs" / "runs" / "run_empty2"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "efgh"}), encoding="utf-8")
    with pytest.raises(ValueError, match="not comparable"):
        resolve_compare_input(str(run_dir), data_dir=tmp_path)


def test_run_id_incomplete_run_hard_error(tmp_path: Path) -> None:
    run_dir = tmp_path / ".runs" / "runs" / "20260306_201512_cases_abcd"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "abcd", "tag": "t"}), encoding="utf-8")
    with pytest.raises(ValueError, match="non-comparable"):
        resolve_compare_input("abcd", data_dir=tmp_path)


def test_latest_tag_skips_incomplete_and_selects_previous_complete(tmp_path: Path) -> None:
    _mk_run(tmp_path, "20260306_195550_cases_ok11", "ok11", tag="baseline", with_results=True, status="ok")
    incomplete = tmp_path / ".runs" / "runs" / "20260306_201512_cases_bad2"
    incomplete.mkdir(parents=True, exist_ok=True)
    (incomplete / "run_meta.json").write_text(json.dumps({"run_id": "bad2", "tag": "baseline"}), encoding="utf-8")

    resolved = resolve_compare_input("latest:baseline", data_dir=tmp_path)
    assert resolved.run_id == "ok11"
    assert resolved.candidate_count == 2
    assert resolved.skipped_incomplete_runs


def test_latest_prefers_newer_effective_ts_over_source_rank(tmp_path: Path) -> None:
    older_meta = _mk_run(
        tmp_path,
        "20260306_090000_cases_meta_old",
        "metaold",
        tag="baseline",
        with_results=True,
        status="ok",
    )
    (older_meta / "run_meta.json").write_text(
        json.dumps({"run_id": "metaold", "tag": "baseline", "timestamp": "2026-03-06T10:00:00Z"}),
        encoding="utf-8",
    )

    _mk_run(
        tmp_path,
        "20260306_110000_cases_name_new",
        "namenew",
        tag="baseline",
        with_results=True,
        status="failed",
    )

    resolved = resolve_compare_input("latest:baseline", data_dir=tmp_path)
    assert resolved.run_id == "namenew"


def test_run_id_partial_suffix_does_not_resolve(tmp_path: Path) -> None:
    _mk_run(
        tmp_path,
        "20260306_201512_retail_cases_abcd1234",
        "abcd1234",
        tag=None,
        with_results=True,
        status="ok",
    )
    with pytest.raises(ValueError, match="run_id=1234 not found"):
        resolve_compare_input("1234", data_dir=tmp_path)


def test_run_id_can_fallback_to_canonical_dir_suffix_when_meta_missing(tmp_path: Path) -> None:
    run_dir = tmp_path / ".runs" / "runs" / "20260306_201512_retail_cases_44148564"
    case_dir = run_dir / "cases" / "case-1"
    case_dir.mkdir(parents=True, exist_ok=True)
    # intentionally missing run_meta.json
    (case_dir / "status.json").write_text(
        json.dumps({"id": "case-1", "status": "ok", "artifacts_dir": str(case_dir), "checked": True}),
        encoding="utf-8",
    )

    resolved = resolve_compare_input("44148564", data_dir=tmp_path)
    assert resolved.kind == "run_id"
    assert resolved.run_dir == run_dir
