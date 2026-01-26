from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)


def replay_plan_normalize_spec_v1(inp: dict, ctx: ReplayContext) -> dict:
    spec_dict = dict(inp["spec"])
    options = PlanNormalizerOptions(**inp["options"])
    rules = inp.get("normalizer_rules") or inp.get("normalizer_registry") or {}
    provider = spec_dict["provider"]
    provider_catalog: Dict[str, ProviderInfo] = {}
    provider_info_source = "minimal_fallback"
    provider_snapshot = inp.get("provider_info_snapshot")
    if isinstance(provider_snapshot, dict):
        provider_catalog[provider] = ProviderInfo(**provider_snapshot)
        provider_info_source = "snapshot"
    else:
        planner = ctx.extras.get("planner_input_v1") or {}
        planner_input = planner.get("input") if isinstance(planner, dict) else {}
        catalog_raw = {}
        if isinstance(planner_input, dict):
            catalog_raw = planner_input.get("provider_catalog") or {}
        if provider in catalog_raw and isinstance(catalog_raw[provider], dict):
            provider_catalog[provider] = ProviderInfo(**catalog_raw[provider])
            provider_info_source = "planner_input"
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
    logger.info(
        "replay_plan_normalize_spec_v1: replay_id=%s provider=%s provider_info_source=%s",
        "plan_normalize.spec_v1",
        provider,
        provider_info_source,
    )
    if provider_info_source == "minimal_fallback":
        logger.warning(
            "replay_plan_normalize_spec_v1: provider_info_source=minimal_fallback "
            "replay_id=%s provider=%s",
            "plan_normalize.spec_v1",
            provider,
        )
    out_payload = {
        "out_spec": out_spec,
        "notes_last": notes[-1] if notes else None,
    }
    if provider_info_source == "minimal_fallback":
        out_payload["diag"] = {"provider_info_source": provider_info_source}
    return out_payload


REPLAY_HANDLERS["plan_normalize.spec_v1"] = replay_plan_normalize_spec_v1
