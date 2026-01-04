"""Legacy compatibility aliases for JSON selector types.

The canonical definitions live in :mod:`fetchgraph.relational.types`. This
module preserves the historical import path used prior to the relational
package refactor.
"""

from .relational.types import JSONDict, JSONPrimitive, JSONValue, SelectorsDict

__all__ = ["JSONPrimitive", "JSONValue", "JSONDict", "SelectorsDict"]

