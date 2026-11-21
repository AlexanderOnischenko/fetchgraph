"""Investigation module - orchestrates artifact investigations."""

from .orchestrator import (
    InvestigationOrchestrator,
    create_orchestrator,
)

__all__ = [
    "InvestigationOrchestrator",
    "create_orchestrator",
]
