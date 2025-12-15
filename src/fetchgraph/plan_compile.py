"""Compile and validate selectors inside a parsed plan.

The planner may emit selector envelopes that rely on the $dsl mechanism. This
module compiles them into provider-native selectors immediately after parsing
to catch schema or validation errors early.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping

from .core.models import Plan
from .core.protocols import ContextProvider
from .core.selector_dialects import compile_selectors
from .relational.models import RelationalQuery, SchemaRequest, SemanticOnlyRequest


def _validate_compiled(compiled: Dict[str, Any]) -> None:
    """Validate compiled selectors using provider schemas when possible."""

    op = compiled.get("op") if isinstance(compiled, dict) else None

    if op == "query":
        RelationalQuery.model_validate(compiled)
    elif op == "schema":
        SchemaRequest.model_validate(compiled)
    elif op == "semantic_only":
        SemanticOnlyRequest.model_validate(compiled)


def compile_plan_selectors(
    plan: Plan, providers: Mapping[str, ContextProvider]
) -> Plan:
    """Compile all selector envelopes inside a parsed plan.

    Parameters
    ----------
    plan:
        Parsed plan produced by :class:`PlanParser`.
    providers:
        Mapping from provider keys to provider instances capable of selector
        compilation.
    """

    if not plan.context_plan:
        return plan

    compiled_specs = []
    for spec in plan.context_plan:
        provider = providers.get(spec.provider)
        if provider is None:
            raise ValueError(
                f"Provider {spec.provider!r} is missing; cannot compile selectors"
            )

        compiled = compile_selectors(provider, spec.selectors or {})
        _validate_compiled(compiled)

        compiled_specs.append(spec.model_copy(update={"selectors": compiled}))

    return plan.model_copy(update={"context_plan": compiled_specs})


__all__ = ["compile_plan_selectors"]
