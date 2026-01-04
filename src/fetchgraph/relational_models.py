"""Legacy compatibility layer for relational models.

The current implementations live under :mod:`fetchgraph.relational.models`.
Importing from this module continues to work for consumers pinned to the old
paths.
"""

from .relational import models as _models
from .relational.models import *  # noqa: F401,F403
from .relational.models import QueryResult

__all__ = [*_models.__all__, "QueryResult"]
