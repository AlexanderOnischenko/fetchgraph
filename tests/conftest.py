from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

@dataclass(frozen=True)
class PromoteCandidate:
    bundle_path: str
    replay_id: str
    command: str
    # для verbose-режима можно хранить полный текст
    full_message: str | None = None

KNOWN_BAD_PROMOTE_KEY: pytest.StashKey[list[PromoteCandidate]] = pytest.StashKey()


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("known_bad")
    group.addoption(
        "--known-bad-promote-verbose",
        action="store_true",
        default=False,
        help="Print full promote blocks for KNOWN_BAD passing cases.",
    )


@pytest.hookimpl(trylast=True)
def pytest_terminal_summary(
    terminalreporter: pytest.TerminalReporter,
    exitstatus: int,
    config: pytest.Config,
) -> None:
    items = config.stash.get(KNOWN_BAD_PROMOTE_KEY, [])
    if not items:
        return

    verbose = bool(config.getoption("--known-bad-promote-verbose"))

    # дедуп по пути (на всякий)
    uniq: dict[str, PromoteCandidate] = {it.bundle_path: it for it in items}
    ordered = sorted(uniq.values(), key=lambda x: x.bundle_path)

    terminalreporter.section(
        f"KNOWN_BAD: candidates to promote to fixed ({len(ordered)})",
        sep="=",
    )

    if not verbose:
        terminalreporter.line("The following cases are green and can be potentially promoted (copy-paste commands).")
        terminalreporter.line("")
        # компактно
        for it in ordered:
            terminalreporter.line(f"  {it.command}")
        terminalreporter.line("")
        terminalreporter.line("Tip: rerun with --known-bad-promote-verbose to print full details.")
        return

    # verbose: печатаем полный блок
    for it in ordered:
        terminalreporter.line(it.full_message or f"{it.bundle_path}\n{it.command}")
        terminalreporter.line("")