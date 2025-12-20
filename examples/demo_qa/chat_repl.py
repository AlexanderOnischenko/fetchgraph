from __future__ import annotations

import datetime
import sys
import uuid
from pathlib import Path
from typing import Optional, Sequence

import readline
import json

from .provider_factory import build_provider
from .runner import RunArtifacts, build_agent, save_artifacts


def _maybe_add_history(entry: str) -> None:
    """Record the entry so it can be recalled with ↑ like a shell."""
    if not entry:  # pragma: no cover - simple guard
        return
    hist_len = readline.get_current_history_length()
    if hist_len == 0 or readline.get_history_item(hist_len) != entry:
        readline.add_history(entry)


def start_repl(
    data_dir: Path,
    schema_path: Path,
    llm,
    enable_semantic: bool = False,
    log_file: Optional[Path] = None,
    diagnostics: Sequence[str] | None = None,
) -> None:
    provider, _ = build_provider(data_dir, schema_path, enable_semantic=enable_semantic)
    runner = build_agent(llm, provider)

    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    plan_debug_mode = "off"
    last_artifacts: RunArtifacts | None = None

    print("Type your question (or /help). Use /exit or Ctrl+D to exit. Press ↑ to edit the last input.")
    if diagnostics:
        for line in diagnostics:
            print(line)
    print(f"Artifacts root: {runs_root}")
    if log_file:
        print(f"Log file: {log_file}")
    while True:
        try:
            line = input("demo-qa> ").strip()
        except EOFError:
            print()
            break
        if not line:
            continue
        _maybe_add_history(line)
        if line == "/exit":
            break
        if line == "/help":
            print("Commands: /schema, /plan on|off|once, /ctx, /run, /logs, /exit")
            continue
        if line.startswith("/plan"):
            _, _, arg = line.partition(" ")
            choice = arg.strip()
            if choice in {"on", "off", "once"}:
                plan_debug_mode = choice
                print(f"Plan debug set to {plan_debug_mode}")
            else:
                print("Usage: /plan on|off|once")
            continue
        if line == "/ctx":
            if last_artifacts and last_artifacts.context is not None:
                print(json.dumps(last_artifacts.context, indent=2, ensure_ascii=False))
            else:
                print("No context yet.")
            continue
        if line == "/schema":
            print(provider.describe())
            continue
        if line == "/run":
            if last_artifacts:
                print(f"run_id={last_artifacts.run_id} at {last_artifacts.run_dir}")
            else:
                print("No runs yet.")
            continue
        if line == "/logs":
            if log_file:
                print(f"Logs: {log_file}\nTail: tail -f {log_file}")
            else:
                print("Logging to stderr only (no file configured).")
            continue

        run_id = uuid.uuid4().hex[:8]
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = runs_root / f"{timestamp}_{run_id}"

        artifacts: RunArtifacts | None = None
        try:
            artifacts = runner.run_question(line, run_id, run_dir)
            last_artifacts = artifacts
            save_artifacts(artifacts)
            if plan_debug_mode in {"on", "once"} and artifacts.plan:
                print("--- PLAN ---")
                print(json.dumps(artifacts.plan, ensure_ascii=False, indent=2))
            print(artifacts.answer or "")
        except Exception as exc:  # pragma: no cover - REPL resilience
            error_artifacts = artifacts or RunArtifacts(run_id=run_id, run_dir=run_dir, question=line)
            error_artifacts.error = error_artifacts.error or str(exc)
            last_artifacts = error_artifacts
            save_artifacts(error_artifacts)
            print(f"Error during run {run_id}: {exc}", file=sys.stderr)
        finally:
            if plan_debug_mode == "once":
                plan_debug_mode = "off"


__all__ = ["start_repl"]
