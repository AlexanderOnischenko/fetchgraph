from __future__ import annotations

from pathlib import Path

import pytest

from examples.demo_qa.batch import handle_batch
from examples.demo_qa.cli import build_parser


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
