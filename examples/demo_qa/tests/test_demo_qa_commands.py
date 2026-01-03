from __future__ import annotations

import subprocess
import sys


def test_commands_report_import_is_lightweight() -> None:
    script = """
import sys

import examples.demo_qa.commands.report  # noqa: F401

heavy = [name for name in sys.modules if name.startswith("examples.demo_qa.llm") or name.startswith("examples.demo_qa.provider")]
if heavy:
    raise SystemExit(f"Heads up: heavy deps imported: {heavy}")
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout
