"""Retrieval primitives and transport-independent evidence models."""

from .errors import RetrievalError
from .api import FetchGraph, retrieve, answer
from .models import (
    AnswerResult,
    EvidenceBundle,
    RetrievedEvidence,
    RetrievalDiagnostics,
    RetrievalSource,
    Provenance,
)

__all__ = [
    "RetrievalError",
    "FetchGraph",
    "retrieve",
    "answer",
    "EvidenceBundle",
    "AnswerResult",
    "RetrievedEvidence",
    "RetrievalDiagnostics",
    "RetrievalSource",
    "Provenance",
]
