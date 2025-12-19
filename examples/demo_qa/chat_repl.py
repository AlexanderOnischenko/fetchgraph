from __future__ import annotations

import importlib
import datetime
import json
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional

from fetchgraph.core import create_generic_agent
from fetchgraph.core.models import TaskProfile
from fetchgraph.relational.schema import SchemaConfig
from fetchgraph.utils import set_run_id

from .provider_factory import build_provider
from .settings import DemoQASettings, LLMSettings


@dataclass
class RunArtifacts:
    run_id: str
    run_dir: Path
    plan: str | None = None
    context: Dict[str, object] | None = None
    answer: str | None = None
    error: str | None = None


def build_agent(llm, provider) -> Callable[[str, str, Path], RunArtifacts]:
    def saver(feature_name: str, parsed: object) -> None:
        # Placeholder to satisfy BaseGraphAgent.saver; artifacts captured elsewhere.
        return None

    task_profile = TaskProfile(
        task_name="Demo QA",
        goal="Answer analytics questions over the demo dataset",
        output_format="Plain text answer",
        focus_hints=[
            "Prefer aggregates",
            "Use concise answers",
        ],
    )

    agent = create_generic_agent(
        llm_invoke=llm,
        providers={provider.name: provider},
        saver=saver,
        task_profile=task_profile,
    )

    def run_question(question: str, run_id: str, run_dir: Path) -> RunArtifacts:
        set_run_id(run_id)
        artifacts = RunArtifacts(run_id=run_id, run_dir=run_dir)
        plan = agent._plan(question)  # type: ignore[attr-defined]
        artifacts.plan = json.dumps(plan.model_dump(), ensure_ascii=False, indent=2)
        try:
            ctx = agent._fetch(question, plan)  # type: ignore[attr-defined]
            artifacts.context = {k: v.text for k, v in (ctx or {}).items()} if ctx else {}
        except Exception as exc:  # pragma: no cover - demo fallback
            artifacts.error = str(exc)
            artifacts.context = {"error": str(exc)}
            ctx = None
        draft = agent._synthesize(question, ctx, plan)  # type: ignore[attr-defined]
        parsed = agent.domain_parser(draft)
        artifacts.answer = str(parsed)
        return artifacts

    return run_question


def _save_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _save_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_artifacts(artifacts: RunArtifacts) -> None:
    artifacts.run_dir.mkdir(parents=True, exist_ok=True)
    if artifacts.plan is not None:
        _save_text(artifacts.run_dir / "plan.json", artifacts.plan)
    if artifacts.context is not None:
        _save_json(artifacts.run_dir / "context.json", artifacts.context)
    if artifacts.answer is not None:
        _save_text(artifacts.run_dir / "answer.txt", artifacts.answer)
    if artifacts.error is not None:
        _save_text(artifacts.run_dir / "error.txt", artifacts.error)


def _maybe_load_readline():
    spec = importlib.util.find_spec("readline")
    if spec is None:
        return None
    return importlib.import_module("readline")


def _format_llm_diag(llm_settings: LLMSettings) -> str:
    endpoint = llm_settings.base_url or "api.openai.com (default)"
    timeout = f", timeout={llm_settings.timeout_s}s" if llm_settings.timeout_s is not None else ""
    retries = f", retries={llm_settings.retries}" if llm_settings.retries is not None else ""
    return (
        f"LLM endpoint: {endpoint} | plan_model={llm_settings.plan_model} (T={llm_settings.plan_temperature}), "
        f"synth_model={llm_settings.synth_model} (T={llm_settings.synth_temperature}){timeout}{retries}"
    )


def _format_embedding_diag(data_dir: Path, schema: SchemaConfig, enable_semantic: bool) -> str:
    if not enable_semantic:
        return "Embeddings: disabled (semantic search is off)"

    sources: list[str] = []
    for ent in schema.entities:
        if not ent.semantic_text_fields or not ent.source:
            continue
        embed_path = (data_dir / ent.source).with_suffix(".embeddings.json")
        sources.append(f"{ent.name} → {embed_path}")

    if not sources:
        return "Embeddings: enabled but no semantic_text_fields found in schema"
    return "Embeddings: " + "; ".join(sources)


def start_repl(
    data_dir: Path,
    schema_path: Path,
    llm,
    settings: DemoQASettings,
    enable_semantic: bool = False,
    log_file: Optional[Path] = None,
) -> None:
    provider, schema = build_provider(data_dir, schema_path, enable_semantic=enable_semantic)
    runner, artifacts = build_agent(llm, provider)

    readline = _maybe_load_readline()
    history_enabled = readline is not None
    if history_enabled:
        readline.set_history_length(1000)

    runs_root = data_dir / ".runs" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)
    plan_debug_mode = "off"
    print(_format_llm_diag(settings.llm))
    print(_format_embedding_diag(data_dir, schema, enable_semantic))
    last_artifacts: RunArtifacts | None = None
    hint = "Type your question (or /help). Ctrl+D or /exit to quit."
    print(f"Artifacts root: {runs_root}")
    if log_file:
        print(f"Log file: {log_file}")
    if history_enabled:
        hint += " Use ↑/↓ to edit previous input."
    print(hint)
    while True:
        try:
            line = input("demo-qa> ").strip()
        except EOFError:
            print()
            break
        if not line:
            continue
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

        if history_enabled and not line.startswith("/"):
            readline.add_history(line)

        run_id = uuid.uuid4().hex[:8]
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = runs_root / f"{timestamp}_{run_id}"

        artifacts: RunArtifacts | None = None
        try:
            artifacts = runner(line, run_id, run_dir)
            last_artifacts = artifacts
            _save_artifacts(artifacts)
            if plan_debug_mode in {"on", "once"} and artifacts.plan:
                print("--- PLAN ---")
                print(artifacts.plan)
            print(artifacts.answer or "")
        except Exception as exc:  # pragma: no cover - REPL resilience
            error_artifacts = artifacts or RunArtifacts(run_id=run_id, run_dir=run_dir)
            error_artifacts.error = error_artifacts.error or str(exc)
            last_artifacts = error_artifacts
            _save_artifacts(error_artifacts)
            print(f"Error during run {run_id}: {exc}", file=sys.stderr)
        finally:
            if plan_debug_mode == "once":
                plan_debug_mode = "off"


__all__ = ["start_repl"]
