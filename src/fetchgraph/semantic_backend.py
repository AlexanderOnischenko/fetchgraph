"""Legacy compatibility shim for semantic backends.

The canonical implementations reside in :mod:`fetchgraph.relational.semantic`.
This module preserves the historical import path used prior to the relational
package refactor.
"""

from .relational import semantic as _semantic
from .relational.semantic import *  # noqa: F401,F403

__all__ = _semantic.__all__

