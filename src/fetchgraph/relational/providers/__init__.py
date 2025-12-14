"""Relational data providers."""

from .base import RelationalDataProvider
from .composite_provider import CompositeRelationalProvider
from .sql_provider import SqlRelationalDataProvider

try:  # optional dependency
    from .pandas_provider import PandasRelationalDataProvider
except Exception:  # pragma: no cover - optional dependency path
    PandasRelationalDataProvider = None  # type: ignore[assignment]

__all__ = (
    "RelationalDataProvider",
    "CompositeRelationalProvider",
    "SqlRelationalDataProvider",
    "PandasRelationalDataProvider",
)
