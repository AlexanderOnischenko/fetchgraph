"""Compile and validate selectors inside a parsed plan."""

from __future__ import annotations

from typing import Mapping

from .core.models import Plan
from .core.protocols import ContextProvider
from .core.selectors import coerce_selectors_to_native


def compile_plan_selectors(
    plan: Plan, providers: Mapping[str, ContextProvider], *, planner_mode: bool = True
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

        compiled = coerce_selectors_to_native(
            provider, spec.selectors or {}, planner_mode=planner_mode
        )

        compiled_specs.append(spec.model_copy(update={"selectors": compiled}))

    return plan.model_copy(update={"context_plan": compiled_specs})


__all__ = ["compile_plan_selectors"]
