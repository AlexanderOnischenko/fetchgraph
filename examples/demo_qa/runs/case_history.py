from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Optional

from ..runner import RunResult


def _reason_text(res: RunResult) -> str:
    if res.reason:
        return res.reason
    if res.error:
        return res.error
    expected = getattr(res, "expected_check", None)
    if expected and getattr(expected, "detail", None):
        return expected.detail
    return ""


def _append_case_history(
    artifacts_dir: Path,
    result: RunResult,
    *,
    run_id: str,
    tag: str | None,
    note: str | None,
    fail_on: str,
    require_assert: bool,
    scope_hash: str,
    cases_hash: str,
    git_sha: str | None,
    run_dir: Path,
    results_path: Path,
) -> None:
    history_dir = artifacts_dir / "runs" / "cases"
    history_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "run_id": run_id,
        "tag": tag,
        "note": note,
        "status": result.status,
        "reason": _reason_text(result),
        "duration_ms": result.duration_ms,
        "artifacts_dir": result.artifacts_dir,
        "run_dir": str(run_dir),
        "results_path": str(results_path),
        "fail_on": fail_on,
        "require_assert": require_assert,
        "scope_hash": scope_hash,
        "cases_hash": cases_hash,
        "git_sha": git_sha,
    }
    target = history_dir / f"{result.id}.jsonl"
    with target.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _load_case_history(path: Path) -> list[dict]:
    if not path.exists():
        return []
    entries: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception:
                continue
    return entries


__all__ = ["_append_case_history", "_load_case_history"]
