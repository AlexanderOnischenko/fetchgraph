from __future__ import annotations

import json
import sys
from pathlib import Path

from .runner import AgentRunner, setup_runner


def start_repl(data_dir: Path, schema_path: Path, llm, enable_semantic: bool = False) -> None:
    runner: AgentRunner = setup_runner(data_dir, schema_path, llm, enable_semantic=enable_semantic)
    artifacts = runner.artifacts
    provider = next(iter(runner.agent.providers.values()))

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

        answer = runner.run_question(line)
        if plan_debug and artifacts.plan is not None:
            print("--- PLAN ---")
            print(json.dumps(artifacts.plan, indent=2, ensure_ascii=False))
        print(answer)


__all__ = ["start_repl"]
