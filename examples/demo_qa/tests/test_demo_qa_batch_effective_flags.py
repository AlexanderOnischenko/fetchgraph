from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from examples.demo_qa import batch
from examples.demo_qa.batch import handle_batch
from examples.demo_qa.cli import build_parser
from examples.demo_qa.runner import Case, RunResult
from examples.demo_qa.runs.scope import _scope_hash, _scope_payload


def _base_args(tmp_path: Path) -> list[str]:
    data_dir = tmp_path / "data"
    schema = data_dir / "schema.json"
    cases = data_dir / "cases.jsonl"
    # Paths need not exist because validation should short-circuit before file access.
    return ["batch", "--data", str(data_dir), "--schema", str(schema), "--cases", str(cases), "--events", "off"]


def test_only_failed_effective_requires_tag(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    args = build_parser().parse_args(_base_args(tmp_path) + ["--only-failed-effective"])

    exit_code = handle_batch(args)
    captured = capsys.readouterr().err

    assert exit_code == 2
    assert "--tag is required" in captured


def test_only_missed_effective_requires_tag(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    args = build_parser().parse_args(_base_args(tmp_path) + ["--only-missed-effective"])

    exit_code = handle_batch(args)
    captured = capsys.readouterr().err

    assert exit_code == 2
    assert "--tag is required" in captured


def test_only_failed_effective_rejects_only_failed_from(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    args = build_parser().parse_args(
        _base_args(tmp_path)
        + [
            "--only-failed-effective",
            "--tag",
            "demo",
            "--only-failed-from",
            str(tmp_path / "prev.jsonl"),
        ]
    )

    exit_code = handle_batch(args)
    captured = capsys.readouterr().err

    assert exit_code == 2
    assert "not compatible with --only-failed-from" in captured


def _stub_settings():
    llm = SimpleNamespace(
        base_url=None,
        plan_model="p",
        synth_model="s",
        plan_temperature=0.0,
        synth_temperature=0.0,
        timeout_s=None,
        retries=None,
    )
    return SimpleNamespace(llm=llm)


def _stub_run_one(case: Case, runner, artifacts_root: Path, *args, **kwargs) -> RunResult:
    return RunResult(
        id=case.id,
        question=case.question or "",
        status="ok",
        checked=True,
        reason=None,
        details=None,
        artifacts_dir=str(artifacts_root),
        duration_ms=0,
        tags=[],
    )


def _write_cases(tmp_path: Path, cases: list[Case]) -> Path:
    path = tmp_path / "cases.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for case in cases:
            f.write(f'{{"id": "{case.id}", "question": "q"}}\n')
    return path


def test_only_failed_effective_uses_effective_baseline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    schema = data_dir / "schema.json"
    schema.parent.mkdir(parents=True, exist_ok=True)
    schema.write_text("{}", encoding="utf-8")
    cases = [
        Case(id="ok", question=""),
        Case(id="err", question=""),
        Case(id="mis", question=""),
    ]
    cases_path = _write_cases(tmp_path, cases)
    cases_hash = batch._hash_file(cases_path)
    scope_hash = _scope_hash(_scope_payload(cases_hash=cases_hash, include_tags=None, exclude_tags=None, include_ids=None, exclude_ids=None))

    effective_results = {
        "ok": RunResult(id="ok", question="", status="ok", checked=True, reason=None, details=None, artifacts_dir="", duration_ms=0, tags=[]),
        "err": RunResult(id="err", question="", status="error", checked=True, reason=None, details=None, artifacts_dir="", duration_ms=0, tags=[]),
        "mis": RunResult(id="mis", question="", status="mismatch", checked=True, reason=None, details=None, artifacts_dir="", duration_ms=0, tags=[]),
    }

    def _load_effective_results_stub(artifacts_dir: Path, tag: str):
        eff_path = artifacts_dir / "runs" / "tags" / tag / "effective_results.jsonl"
        return effective_results, {"cases_hash": cases_hash, "scope_hash": scope_hash, "planned_case_ids": list(effective_results)}, eff_path

    monkeypatch.setattr(batch, "load_settings", lambda config_path=None, data_dir=None: (_stub_settings(), None))
    monkeypatch.setattr(batch, "build_provider", lambda *a, **k: (SimpleNamespace(name="p"), None))
    monkeypatch.setattr(batch, "build_llm", lambda *a, **k: SimpleNamespace())
    monkeypatch.setattr(batch, "build_agent", lambda *a, **k: SimpleNamespace())
    monkeypatch.setattr(batch, "configure_logging", lambda **kwargs: None)
    monkeypatch.setattr(batch, "_load_effective_results", _load_effective_results_stub)
    monkeypatch.setattr(batch, "_load_run_meta", lambda *a, **k: {"run_id": "eff", "run_status": "SUCCESS", "results_complete": True})
    monkeypatch.setattr(batch, "_load_latest_run", lambda *a, **k: (_ for _ in ()).throw(AssertionError("overlay should not be used")))  # type: ignore[arg-type]
    monkeypatch.setattr(batch, "_load_latest_any_results", lambda *a, **k: (_ for _ in ()).throw(AssertionError("overlay should not be used")))  # type: ignore[arg-type]
    monkeypatch.setattr(batch, "run_one", _stub_run_one)

    args = build_parser().parse_args(
        [
            "batch",
            "--data",
            str(data_dir),
            "--schema",
            str(schema),
            "--cases",
            str(cases_path),
            "--events",
            "off",
            "--tag",
            "demo",
            "--only-failed-effective",
        ]
    )

    exit_code = handle_batch(args)
    assert exit_code == 0

    run_meta_path = next((data_dir / ".runs" / "runs").rglob("run_meta.json"))
    run_meta = json.loads(run_meta_path.read_text(encoding="utf-8"))
    assert sorted(run_meta["selected_case_ids"]) == ["err", "mis"]


def test_only_missed_effective_uses_effective_baseline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    schema = data_dir / "schema.json"
    schema.parent.mkdir(parents=True, exist_ok=True)
    schema.write_text("{}", encoding="utf-8")
    cases = [
        Case(id="present", question=""),
        Case(id="missing1", question=""),
        Case(id="missing2", question=""),
    ]
    cases_path = _write_cases(tmp_path, cases)
    cases_hash = batch._hash_file(cases_path)
    scope_hash = _scope_hash(_scope_payload(cases_hash=cases_hash, include_tags=None, exclude_tags=None, include_ids=None, exclude_ids=None))

    effective_results = {
        "present": RunResult(id="present", question="", status="ok", checked=True, reason=None, details=None, artifacts_dir="", duration_ms=0, tags=[]),
    }

    def _load_effective_results_stub(artifacts_dir: Path, tag: str):
        eff_path = artifacts_dir / "runs" / "tags" / tag / "effective_results.jsonl"
        meta = {
            "cases_hash": cases_hash,
            "scope_hash": scope_hash,
            "planned_case_ids": [case.id for case in cases],
        }
        return effective_results, meta, eff_path

    monkeypatch.setattr(batch, "load_settings", lambda config_path=None, data_dir=None: (_stub_settings(), None))
    monkeypatch.setattr(batch, "build_provider", lambda *a, **k: (SimpleNamespace(name="p"), None))
    monkeypatch.setattr(batch, "build_llm", lambda *a, **k: SimpleNamespace())
    monkeypatch.setattr(batch, "build_agent", lambda *a, **k: SimpleNamespace())
    monkeypatch.setattr(batch, "configure_logging", lambda **kwargs: None)
    monkeypatch.setattr(batch, "_load_effective_results", _load_effective_results_stub)
    monkeypatch.setattr(batch, "_load_run_meta", lambda *a, **k: {"run_id": "eff", "run_status": "SUCCESS", "results_complete": True})
    monkeypatch.setattr(batch, "_load_latest_run", lambda *a, **k: (_ for _ in ()).throw(AssertionError("overlay should not be used")))  # type: ignore[arg-type]
    monkeypatch.setattr(batch, "_load_latest_any_results", lambda *a, **k: (_ for _ in ()).throw(AssertionError("overlay should not be used")))  # type: ignore[arg-type]
    monkeypatch.setattr(batch, "run_one", _stub_run_one)

    args = build_parser().parse_args(
        [
            "batch",
            "--data",
            str(data_dir),
            "--schema",
            str(schema),
            "--cases",
            str(cases_path),
            "--events",
            "off",
            "--tag",
            "demo",
            "--only-missed-effective",
        ]
    )

    exit_code = handle_batch(args)
    assert exit_code == 0

    run_meta_path = next((data_dir / ".runs" / "runs").rglob("run_meta.json"))
    run_meta = json.loads(run_meta_path.read_text(encoding="utf-8"))
    assert sorted(run_meta["selected_case_ids"]) == ["missing1", "missing2"]
