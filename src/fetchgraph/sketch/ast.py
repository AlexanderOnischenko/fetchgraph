"""Lightweight AST placeholders for selector sketch inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class SketchNode:
    """Generic sketch node placeholder."""

    payload: Dict[str, Any]
