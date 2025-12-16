from __future__ import annotations

"""Compatibility layer exposing relational provider components.

The original monolithic implementation has been decomposed into separate
modules for clarity:
- :mod:`fetchgraph.relational_models`
- :mod:`fetchgraph.relational_base`
- :mod:`fetchgraph.relational_pandas` (optional dependency)
- :mod:`fetchgraph.relational_sql`
- :mod:`fetchgraph.relational_composite`
- :mod:`fetchgraph.semantic_backend`
"""

from . import models as _relational_models
from .base import RelationalDataProvider
from .providers.composite import CompositeRelationalProvider
from .models import *  # noqa: F401,F403
from .providers.sql import SqlRelationalDataProvider
from ..semantic_backend import SemanticBackend
from .providers.pandas import PandasRelationalDataProvider


__all__ = [
    *_relational_models.__all__,
    "RelationalDataProvider",
    "SqlRelationalDataProvider",
    "CompositeRelationalProvider",
    "SemanticBackend",
    "PandasRelationalDataProvider",
]
