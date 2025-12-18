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

import importlib.util

from . import relational_models as _relational_models
from .relational_base import RelationalDataProvider
from .relational_composite import CompositeRelationalProvider
from .relational_models import *  # noqa: F401,F403
from .relational_sql import SqlRelationalDataProvider
from .semantic_backend import SemanticBackend


from .relational_pandas import PandasRelationalDataProvider

__all__ = [
    *_relational_models.__all__,
    "RelationalDataProvider",
    "SqlRelationalDataProvider",
    "CompositeRelationalProvider",
    "SemanticBackend",
    "PandasRelationalDataProvider",
]
