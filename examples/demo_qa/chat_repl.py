from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from fetchgraph.core import create_generic_agent
from fetchgraph.core.models import TaskProfile

from .provider_factory import build_provider


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


def start_repl(data_dir: Path, schema_path: Path, llm, enable_semantic: bool = False) -> None:
    provider, _ = build_provider(data_dir, schema_path, enable_semantic=enable_semantic)
    runner, artifacts = build_agent(llm, provider)

    plan_debug = False
    print("Type your question (or /help). Ctrl+D to exit.")
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

        answer = runner(line)
        if plan_debug and artifacts.plan:
            print("--- PLAN ---")
            print(artifacts.plan)
        print(answer)


__all__ = ["start_repl"]
