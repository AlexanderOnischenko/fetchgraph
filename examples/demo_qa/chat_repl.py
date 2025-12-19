from __future__ import annotations

import importlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from fetchgraph.core import create_generic_agent
from fetchgraph.core.models import TaskProfile
from fetchgraph.relational.schema import SchemaConfig

from .provider_factory import build_provider
from .settings import DemoQASettings, LLMSettings


@dataclass
class RunArtifacts:
    plan: str | None = None
    context: Dict[str, object] | None = None
    answer: str | None = None


def build_agent(llm, provider) -> tuple[object, RunArtifacts]:
    artifacts = RunArtifacts()

    def saver(feature_name: str, parsed: object) -> None:
        artifacts.answer = str(parsed)

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

    def run_question(question: str) -> str:
        plan = agent._plan(question)  # type: ignore[attr-defined]
        artifacts.plan = json.dumps(plan.model_dump(), ensure_ascii=False, indent=2)
        try:
            ctx = agent._fetch(question, plan)  # type: ignore[attr-defined]
            artifacts.context = {k: v.text for k, v in (ctx or {}).items()} if ctx else {}
        except Exception as exc:  # pragma: no cover - demo fallback
            artifacts.context = {"error": str(exc)}
            ctx = None
        draft = agent._synthesize(question, ctx, plan)  # type: ignore[attr-defined]
        parsed = agent.domain_parser(draft)
        saver(question, parsed)
        return str(parsed)

    return run_question, artifacts


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
) -> None:
    provider, schema = build_provider(data_dir, schema_path, enable_semantic=enable_semantic)
    runner, artifacts = build_agent(llm, provider)

    readline = _maybe_load_readline()
    history_enabled = readline is not None
    if history_enabled:
        readline.set_history_length(1000)

    plan_debug = False
    print(_format_llm_diag(settings.llm))
    print(_format_embedding_diag(data_dir, schema, enable_semantic))
    hint = "Type your question (or /help). Ctrl+D or /exit to quit."
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
            print("Commands: /schema, /plan on|off, /ctx, /exit")
            continue
        if line.startswith("/plan"):
            _, _, arg = line.partition(" ")
            plan_debug = arg.strip() == "on"
            print(f"Plan debug {'enabled' if plan_debug else 'disabled'}")
            continue
        if line == "/ctx":
            if artifacts.context:
                print(json.dumps(artifacts.context, indent=2, ensure_ascii=False))
            else:
                print("No context yet.")
            continue
        if line == "/schema":
            print(provider.describe())
            continue

        if history_enabled and not line.startswith("/"):
            readline.add_history(line)

        answer = runner(line)
        if plan_debug and artifacts.plan:
            print("--- PLAN ---")
            print(artifacts.plan)
        print(answer)


__all__ = ["start_repl"]
