from __future__ import annotations

"""Protocols for semantic search backends used by relational providers."""

from typing import Protocol, Sequence

from .relational_models import SemanticMatch


class SemanticBackend(Protocol):
    """Interface for semantic search backends.

    Real implementations may use vector stores like Faiss, Qdrant, or pgvector.
    """

    def search(
        self,
        entity: str,
        fields: Sequence[str],
        query: str,
        top_k: int = 100,
    ) -> list[SemanticMatch]:
        """Return semantic matches for the given entity and text query."""


__all__ = ["SemanticBackend"]
