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
    
    # Extract unwrapped selectors from the input structure
    # Input selectors may be wrapped under a provider key (e.g., "demo_qa", "relational")
    # We need to unwrap them to match the expected structure
    raw_selectors = spec_dict.get("selectors", {})
    unwrapped_selectors = raw_selectors
    
    # Only unwrap if:
    # 1. There's exactly one key in the selectors dict
    # 2. The value is a dict (not a primitive)
    # 3. The inner dict looks like a selector (has keys like 'op', 'root_entity', etc.)
    if len(raw_selectors) == 1:
        first_key = next(iter(raw_selectors.keys()))
        first_value = raw_selectors.get(first_key)
        if isinstance(first_value, dict) and ('op' in first_value or 'root_entity' in first_value or 'relations' in first_value):
            # This looks like a wrapped selector - unwrap it
            unwrapped_selectors = first_value
    
    # Update spec_dict with unwrapped selectors
    spec_dict["selectors"] = unwrapped_selectors
    
    provider_catalog: Dict[str, ProviderInfo] = {}
    provider_info_source = "minimal_fallback"
    provider_snapshot = inp.get("provider_info_snapshot")
    provider_snapshot_present = isinstance(provider_snapshot, dict)
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
        "selectors": out.selectors,
    }
    
    # FIX: Ensure root_entity is present for relational queries
    # If missing, try to infer from field names (e.g., "orders.order_total" -> "orders")
    selectors = out_spec.get("selectors", {})
    op = selectors.get("op")
    is_relational_op = op in ("query", "aggregate", "semantic_only")
    has_relational_keys = any(k in selectors for k in ("root_entity", "relations", "aggregations", "entity"))
    
    inferred_entity = None
    if (is_relational_op or has_relational_keys) and "root_entity" not in selectors:
        # Try to infer root_entity from field names
        # Check aggregations
        for agg in selectors.get("aggregations", []):
            if isinstance(agg, dict):
                field = agg.get("field", "")
                if "." in field:
                    inferred_entity = field.split(".")[0]
                    break
        
        # Check select fields
        if not inferred_entity:
            for sel in selectors.get("select", []):
                if isinstance(sel, dict):
                    expr = sel.get("expr", "")
                    if "." in expr:
                        inferred_entity = expr.split(".")[0]
                        break
        
        # Check filters
        if not inferred_entity:
            filters = selectors.get("filters", {})
            if isinstance(filters, dict):
                field = filters.get("field", "")
                if "." in field:
                    inferred_entity = field.split(".")[0]
        
        if inferred_entity:
            out_spec["selectors"] = dict(selectors)
            out_spec["selectors"]["root_entity"] = inferred_entity
            selectors = out_spec["selectors"]
    
    # FIX: Normalize op='aggregate' to op='query' (schema only accepts 'query', 'schema', 'semantic_only')
    if selectors.get("op") == "aggregate":
        if "selectors" not in out_spec or out_spec["selectors"] is selectors:
            out_spec["selectors"] = dict(selectors)
        out_spec["selectors"]["op"] = "query"
    
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
        out_payload["diag"] = {
            "provider_info_source": provider_info_source,
            "missing_planner_input": "planner_input_v1" not in ctx.extras,
            "provider_snapshot_present": provider_snapshot_present,
            "root_entity_inferred": inferred_entity is not None,
        }
    return out_payload


REPLAY_HANDLERS["plan_normalize.spec_v1"] = replay_plan_normalize_spec_v1
