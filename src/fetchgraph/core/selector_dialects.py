from __future__ import annotations

from typing import Any, Callable, Dict

from ..dsl import compile_relational_query, parse_and_normalize
from .protocols import ContextProvider


QUERY_SKETCH_DSL_ID = "fetchgraph.dsl.query_sketch@v0"


def _compile_query_sketch(payload: Any) -> Dict[str, Any]:
    if payload is None:
        raise ValueError("Selector dialect fetchgraph.dsl.query_sketch@v0 requires 'payload'")

    sketch, diagnostics = parse_and_normalize(payload)
    if diagnostics.has_errors():
        errors = diagnostics.errors()
        summary = "; ".join(f"{err.code}: {err.message}" for err in errors)
        raise ValueError(
            "Selector dialect fetchgraph.dsl.query_sketch@v0 parse errors: " + summary
        )

    compiled = compile_relational_query(sketch)
    return compiled.model_dump()


_COMPILERS: Dict[str, Callable[[Any], Dict[str, Any]]] = {
    QUERY_SKETCH_DSL_ID: _compile_query_sketch,
}


def compile_selectors(provider: ContextProvider, selectors: Dict[str, Any]) -> Dict[str, Any]:
    """Compile selector envelopes into provider-native selectors when needed."""

    selectors = selectors or {}
    if not isinstance(selectors, dict):
        return selectors

    # Native selectors: leave untouched
    if selectors.get("op") is not None or "$dsl" not in selectors:
        return selectors

    dialect_id = selectors.get("$dsl")
    compiler = _COMPILERS.get(dialect_id)
    if compiler is None:
        provider_name = getattr(provider, "name", provider.__class__.__name__)
        raise ValueError(
            f"Provider {provider_name!r} does not support selector dialect {dialect_id!r}"
        )

    payload = selectors.get("payload")
    return compiler(payload)


__all__ = ["compile_selectors", "QUERY_SKETCH_DSL_ID"]
