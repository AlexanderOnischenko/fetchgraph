from __future__ import annotations

import json
import os
from pathlib import Path


def write_events(path: Path, events: list[dict]) -> None:
    lines = [json.dumps(event) for event in events]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def set_mtime(path: Path, ts: float) -> None:
    os.utime(path, (ts, ts))


def make_case_dir(
    run_dir: Path,
    case_id: str,
    suffix: str,
    *,
    status: str,
    events: list[dict] | None = None,
    tag: str | None = None,
) -> Path:
    case_dir = run_dir / "cases" / f"{case_id}_{suffix}"
    case_dir.mkdir(parents=True, exist_ok=True)
    if events is not None:
        write_events(case_dir / "events.jsonl", events)
    payload = {"status": status}
    if tag:
        payload["tag"] = tag
    write_json(case_dir / "status.json", payload)
    return case_dir


def write_history_entry(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def case_history_path(data_dir: Path, case_id: str) -> Path:
    artifacts_dir = data_dir / ".runs"
    return artifacts_dir / "runs" / "cases" / f"{case_id}.jsonl"
