"""Legacy compatibility shim for the pandas relational provider."""

from .relational.providers import PandasRelationalDataProvider

__all__ = ["PandasRelationalDataProvider"]

