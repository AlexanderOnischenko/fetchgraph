from __future__ import annotations

"""Compatibility layer exposing relational provider components.

Legacy import paths (e.g., ``fetchgraph.relational_provider``) now forward to
the reorganized modules under :mod:`fetchgraph.relational`.
"""

import importlib.util

from .relational import models as _relational_models
from .relational.models import *  # noqa: F401,F403
from .relational.providers import (
    CompositeRelationalProvider,
    RelationalDataProvider,
    SqlRelationalDataProvider,
)
from .relational.semantic import SemanticBackend

_maybe_pandas = importlib.util.find_spec("pandas")
if _maybe_pandas:
    from .relational.providers import PandasRelationalDataProvider
else:  # pragma: no cover - optional dependency path
    PandasRelationalDataProvider = None  # type: ignore[assignment]

__all__ = [
    *_relational_models.__all__,
    "RelationalDataProvider",
    "SqlRelationalDataProvider",
    "CompositeRelationalProvider",
    "SemanticBackend",
]

if _maybe_pandas:
    __all__.append("PandasRelationalDataProvider")

