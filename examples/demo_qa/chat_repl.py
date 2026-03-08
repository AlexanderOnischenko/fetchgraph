from __future__ import annotations

import json
import readline
import sys
import datetime
import uuid
import os
from pathlib import Path
from typing import Optional, Sequence

from fetchgraph.utils.path_layout import LayoutConfig, RunLayout, ensure_dirs, make_case_dir

from .provider_factory import build_provider
from .runner import (
    Case,
    EventLogger,
    RunArtifacts,
    build_agent,
    run_one,
    save_artifacts,
)


def _load_json(path: Path) -> object | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _maybe_add_history(entry: str) -> None:
    """Record the entry so it can be recalled with ↑ like a shell."""
    if not entry:  # pragma: no cover - simple guard
        return
    hist_len = readline.get_current_history_length()
    if hist_len == 0 or readline.get_history_item(hist_len) != entry:
        readline.add_history(entry)


class _Ansi:
    RESET = "\033[0m"
    BLUE = "\033[34m"
    YELLOW = "\033[33m"
    DIM = "\033[90m"


def _use_color() -> bool:
    if os.getenv("NO_COLOR"):
        return False
    return sys.stdout.isatty()


def _paint(text: str, color: str) -> str:
    if not _use_color():
        return text
    return f"{color}{text}{_Ansi.RESET}"


def _print_banner(title: str, color: str) -> None:
    print(_paint(f"--- {title} ---", color))


def start_repl(
    data_dir: Path,
    schema_path: Path,
    llm,
    enable_semantic: bool = False,
    log_file: Optional[Path] = None,
    diagnostics: Sequence[str] | None = None,
    llm_endpoint: str | None = None,
    verbose: bool = False,
) -> None:
    provider, _ = build_provider(data_dir, schema_path, enable_semantic=enable_semantic)
    runner = build_agent(llm, provider)

    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    plan_debug_mode = "off"
    last_artifacts: RunArtifacts | None = None

    if llm_endpoint:
        _print_banner("LLM", _Ansi.BLUE)
        print(_paint(f"Endpoint: {llm_endpoint}", _Ansi.BLUE))
        print(_paint("-------------", _Ansi.BLUE))

    if diagnostics:
        _print_banner("Diagnostics", _Ansi.BLUE)
        for line in diagnostics:
            print(_paint(line, _Ansi.BLUE))
        print(_paint(f"Artifacts root: {runs_root}", _Ansi.BLUE))
        if log_file:
            print(_paint(f"Log file: {log_file}", _Ansi.BLUE))
        print(_paint("-------------------", _Ansi.BLUE))

    print(_paint("Type your question (or /help). Use /exit or Ctrl+D to exit. Press ↑ to edit the last input.", _Ansi.YELLOW))
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
            print(_paint("Commands: /schema, /plan on|off|once, /ctx, /run, /logs, /exit", _Ansi.YELLOW))
            continue
        if line.startswith("/plan"):
            _, _, arg = line.partition(" ")
            choice = arg.strip()
            if choice in {"on", "off", "once"}:
                plan_debug_mode = choice
                print(_paint(f"Plan debug set to {plan_debug_mode}", _Ansi.YELLOW))
            else:
                print(_paint("Usage: /plan on|off|once", _Ansi.YELLOW))
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
        run_dir_name = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{run_id}"
        event_logger = EventLogger(path=None, run_id=run_id)

        artifacts: RunArtifacts | None = None
        try:
            case = Case(id=run_id, question=line, tags=[])
            result = run_one(
                case,
                runner,
                runs_root,
                plan_only=False,
                event_logger=event_logger,
                run_dir_name=run_dir_name,
                schema_path=None,
            )
            plan_obj = _load_json(Path(result.artifacts_dir) / "plan.json")
            ctx_obj = _load_json(Path(result.artifacts_dir) / "context.json") or {}
            artifacts = RunArtifacts(
                run_id=run_id,
                run_dir=Path(result.artifacts_dir),
                question=line,
                plan=plan_obj if isinstance(plan_obj, dict) else None,
                context=ctx_obj if isinstance(ctx_obj, dict) else None,
                answer=result.answer,
                raw_synth=None,
                error=result.error,
            )
            last_artifacts = artifacts
            if plan_debug_mode in {"on", "once"} and artifacts.plan:
                print("--- PLAN ---")
                print(json.dumps(artifacts.plan, ensure_ascii=False, indent=2))
            print(result.answer or "")
            if verbose:
                print(_paint(f"Events: {Path(result.artifacts_dir) / 'events.jsonl'}", _Ansi.DIM))
        except Exception as exc:  # pragma: no cover - REPL resilience
            run_root = runs_root / run_dir_name
            cfg = LayoutConfig()
            run_layout = RunLayout(data_dir=data_dir, run_root=run_root, run_dir_name=run_dir_name, run_id=run_id)
            case_layout = make_case_dir(run=run_layout, case_id=run_id, suffix=None, cfg=cfg)
            ensure_dirs(run_layout, case_layout, cfg=cfg)
            error_artifacts = artifacts or RunArtifacts(run_id=run_id, run_dir=case_layout.case_dir, question=line)
            error_artifacts.error = error_artifacts.error or str(exc)
            last_artifacts = error_artifacts
            save_artifacts(error_artifacts)
            print(f"Error during run {run_id}: {exc}", file=sys.stderr)
        finally:
            if plan_debug_mode == "once":
                plan_debug_mode = "off"


__all__ = ["start_repl"]
