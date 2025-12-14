from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class Diagnostic:
    code: str
    message: str
    path: str
    severity: Severity


@dataclass
class Diagnostics:
    messages: List[Diagnostic] = field(default_factory=list)

    def add(self, code: str, message: str, path: str, severity: Severity) -> None:
        self.messages.append(Diagnostic(code=code, message=message, path=path, severity=severity))

    def extend(self, other: "Diagnostics") -> None:
        self.messages.extend(other.messages)

    def has_errors(self) -> bool:
        return any(msg.severity == Severity.ERROR for msg in self.messages)

    def warnings(self) -> List[Diagnostic]:
        return [msg for msg in self.messages if msg.severity == Severity.WARNING]

    def errors(self) -> List[Diagnostic]:
        return [msg for msg in self.messages if msg.severity == Severity.ERROR]
