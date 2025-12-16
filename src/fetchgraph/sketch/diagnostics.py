from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class SketchDiagnostics:
    """Collect compile-time notes and warnings for sketch processing."""

    infos: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_info(self, message: str) -> None:
        self.infos.append(message)

    def add_warning(self, message: str) -> None:
        self.warnings.append(message)
