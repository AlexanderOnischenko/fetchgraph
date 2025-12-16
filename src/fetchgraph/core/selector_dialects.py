from __future__ import annotations

from typing import Any, Callable, Dict

from ..dsl import (
    SchemaRegistry,
    bind_query_sketch,
    compile_relational_query,
    normalized_from_bound,
    parse_and_normalize,
)
from ..dsl.resolution_policy import ResolutionPolicy
from .protocols import ContextProvider, SupportsDescribe
from .models import ProviderInfo


QUERY_SKETCH_DSL_ID = "fetchgraph.dsl.query_sketch@v0"


def _compile_query_sketch(provider: SupportsDescribe, payload: Any) -> Dict[str, Any]:
    if payload is None:
        raise ValueError("Selector dialect fetchgraph.dsl.query_sketch@v0 requires 'payload'")

    sketch, diagnostics = parse_and_normalize(payload)
    if diagnostics.has_errors():
        errors = diagnostics.errors()
        summary = "; ".join(
            f"{err.code}: {err.message}{f' (path={err.path})' if err.path else ''}"
            for err in errors
        )
        raise ValueError(
            "Selector dialect fetchgraph.dsl.query_sketch@v0 parse errors: " + summary
        )

    try:
        entities = provider.entities
        relations = provider.relations
    except AttributeError as exc:  # pragma: no cover - defensive
        provider_name = getattr(provider, "name", provider.__class__.__name__)
        raise ValueError(
            f"Provider {provider_name!r} must expose 'entities' and 'relations' for schema binding"
        ) from exc

    registry = SchemaRegistry(entities, relations)
    bound, bind_diags = bind_query_sketch(sketch, registry, ResolutionPolicy())
    if bind_diags.has_errors():
        errors = bind_diags.errors()
        summary = "; ".join(
            f"{err.code}: {err.message}{f' (path={err.path})' if err.path else ''}"
            for err in errors
        )
        raise ValueError(
            "Selector dialect fetchgraph.dsl.query_sketch@v0 errors: " + summary
        )

    sketch2 = normalized_from_bound(bound)
    compiled = compile_relational_query(sketch2)
    return compiled.model_dump()


_COMPILERS: Dict[str, Callable[[SupportsDescribe, Any], Dict[str, Any]]] = {
    QUERY_SKETCH_DSL_ID: _compile_query_sketch,
}


def compile_selectors(provider: ContextProvider, selectors: Dict[str, Any]) -> Dict[str, Any]:
    """Compile selector envelopes into provider-native selectors when needed."""

    selectors = selectors or {}
    if not isinstance(selectors, dict):
        return selectors

    has_dsl = "$dsl" in selectors
    has_op = "op" in selectors

    if has_dsl and has_op:
        raise ValueError("Selectors cannot contain both 'op' and '$dsl' — choose one")

    # Native selectors: leave untouched
    if not has_dsl:
        return selectors

    if not isinstance(provider, SupportsDescribe):
        provider_name = getattr(provider, "name", provider.__class__.__name__)
        raise ValueError(
            f"Provider {provider_name!r} does not declare selector dialects; cannot use $dsl"
        )

    info: ProviderInfo = provider.describe()
    supported = {d.id for d in info.selector_dialects}

    dialect_field = selectors.get("$dsl")
    dialect_id: str | None = None
    payload = selectors.get("payload")

    if isinstance(dialect_field, str):
        dialect_id = dialect_field
    elif isinstance(dialect_field, dict):
        payload = dialect_field.get("payload", payload)
        dialect_id = dialect_field.get("id")
    else:
        raise ValueError("Selector dialect must be a string id or an object with payload/id")

    if dialect_id is None:
        if len(supported) == 1:
            dialect_id = next(iter(supported))
        else:
            available = ", ".join(sorted(supported))
            raise ValueError(f"missing $dsl.id; available: {available}")

    if not isinstance(dialect_id, str):
        raise ValueError("Selector dialect id must be a string in '$dsl' field")

    if dialect_id not in supported:
        provider_name = getattr(provider, "name", provider.__class__.__name__)
        raise ValueError(
            f"Provider {provider_name!r} does not declare support for selector dialect {dialect_id!r}"
        )

    compiler = _COMPILERS.get(dialect_id)
    if compiler is None:
        provider_name = getattr(provider, "name", provider.__class__.__name__)
        raise ValueError(
            f"Provider {provider_name!r} does not support selector dialect {dialect_id!r}"
        )

    return compiler(provider, payload)


__all__ = ["compile_selectors", "QUERY_SKETCH_DSL_ID"]
