from __future__ import annotations

"""Compatibility layer exposing relational provider components.

The original monolithic implementation has been decomposed into separate
modules for clarity:
- :mod:`fetchgraph.relational.models`
- :mod:`fetchgraph.relational.providers.base`
- :mod:`fetchgraph.relational.providers.pandas_provider` (optional dependency)
- :mod:`fetchgraph.relational.providers.sql_provider`
- :mod:`fetchgraph.relational.providers.composite_provider`
- :mod:`fetchgraph.relational.semantic.backend`
"""

import importlib.util

from .relational import models as _relational_models
from .relational.providers.base import RelationalDataProvider
from .relational.providers.composite_provider import CompositeRelationalProvider
from .relational.providers.sql_provider import SqlRelationalDataProvider
from .relational.semantic.backend import SemanticBackend
from .relational.models import *  # noqa: F401,F403

_maybe_pandas = importlib.util.find_spec("pandas")
if _maybe_pandas:
    from .relational.providers.pandas_provider import PandasRelationalDataProvider
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
