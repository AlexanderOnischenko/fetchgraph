"""Relational providers and helpers."""

from . import models
from .types import SelectorsDict
from .models import *  # noqa: F401,F403
from .providers import (
    RelationalDataProvider,
    SqlRelationalDataProvider,
    CompositeRelationalProvider,
)
from .semantic import (
    SemanticBackend,
    CsvSemanticBackend,
    CsvSemanticSource,
    CsvEmbeddingBuilder,
    PgVectorSemanticBackend,
    PgVectorSemanticSource,
    VectorStoreLike,
)

try:  # optional dependency
    from .providers import PandasRelationalDataProvider
except Exception:  # pragma: no cover - optional dependency path
    PandasRelationalDataProvider = None  # type: ignore[assignment]

__all__ = [
    *models.__all__,
    "SelectorsDict",
    "RelationalDataProvider",
    "SqlRelationalDataProvider",
    "CompositeRelationalProvider",
    "SemanticBackend",
    "CsvSemanticBackend",
    "CsvSemanticSource",
    "CsvEmbeddingBuilder",
    "PgVectorSemanticBackend",
    "PgVectorSemanticSource",
    "VectorStoreLike",
]

if PandasRelationalDataProvider is not None:
    __all__.append("PandasRelationalDataProvider")
