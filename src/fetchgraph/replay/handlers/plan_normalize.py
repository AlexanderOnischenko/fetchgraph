from __future__ import annotations

from typing import Dict

from pydantic import TypeAdapter

from ...core.models import ContextFetchSpec, ProviderInfo
from ...planning.normalize import (
    PlanNormalizer,
    PlanNormalizerOptions,
    SelectorNormalizationRule,
)
from ...relational.models import RelationalRequest
from ...relational.normalize import normalize_relational_selectors
from ..runtime import REPLAY_HANDLERS, ReplayContext


def replay_plan_normalize_spec_v1(inp: dict, ctx: ReplayContext) -> dict:
    spec_dict = dict(inp["spec"])
    options = PlanNormalizerOptions(**inp["options"])
    rules = inp.get("normalizer_rules") or inp.get("normalizer_registry") or {}
    provider = spec_dict["provider"]
    provider_catalog: Dict[str, ProviderInfo] = {}
    planner = ctx.extras.get("planner_input_v1") or {}
    planner_input = planner.get("input") if isinstance(planner, dict) else {}
    catalog_raw = {}
    if isinstance(planner_input, dict):
        catalog_raw = planner_input.get("provider_catalog") or {}
    if provider in catalog_raw and isinstance(catalog_raw[provider], dict):
        provider_catalog[provider] = ProviderInfo(**catalog_raw[provider])
    else:
        provider_catalog[provider] = ProviderInfo(name=provider, capabilities=[])

    rule_kind = rules.get(provider)
    normalizer_registry: Dict[str, SelectorNormalizationRule] = {}
    if rule_kind == "relational_v1":
        normalizer_registry[provider] = SelectorNormalizationRule(
            kind="relational_v1",
            validator=TypeAdapter(RelationalRequest),
            normalize_selectors=normalize_relational_selectors,
        )

    normalizer = PlanNormalizer(
        provider_catalog,
        normalizer_registry=normalizer_registry,
        options=options,
    )

    spec = ContextFetchSpec(**spec_dict)
    notes: list[str] = []
    out_specs = normalizer.normalize_specs([spec], notes=notes)
    out = out_specs[0]

    out_spec = {
        "provider": out.provider,
        "mode": out.mode,
        "selectors": out.selectors,
    }
    return {
        "out_spec": out_spec,
        "notes_last": notes[-1] if notes else None,
    }


REPLAY_HANDLERS["plan_normalize.spec_v1"] = replay_plan_normalize_spec_v1
