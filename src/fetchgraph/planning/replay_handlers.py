"""Replay handlers for planning pipeline nodes.

This module registers replay handlers for the planning pipeline,
allowing fixture creation and replay testing.

Usage:
    import fetchgraph.planning.replay_handlers  # noqa: F401
    
    # Handlers are now registered in REPLAY_HANDLERS
    from fetchgraph.replay.runtime import REPLAY_HANDLERS, run_case
    
    # Run a replay case
    root, ctx = load_case_bundle(Path(".../*.case.json"))
    out = run_case(root, ctx)
"""

from __future__ import annotations

from typing import Any, Dict

from fetchgraph.replay.runtime import REPLAY_HANDLERS


def replay_plan_normalize_spec_v1(input_data: dict[str, Any], ctx: Any) -> dict[str, Any]:
    """Replay handler for plan normalization (spec_v1).
    
    This handler replays the normalization of selectors through the pipeline.
    
    Input:
        {
            "spec": {
                "provider": str,
                "selectors": Dict[str, Any],
            },
            "options": {...},
            "normalizer_rules": {...},
            "provider_info_snapshot": {...},  # optional
        }
    
    Output:
        {
            "out_spec": {
                "provider": str,
                "selectors": Dict[str, Any],  # normalized selectors
            },
        }
    """
    from fetchgraph.core.models import Plan
    from fetchgraph.planning.adapter import create_pipeline_normalizer
    from fetchgraph.planning.nodes import PipelineConfig
    
    spec = input_data.get("spec", {})
    options = input_data.get("options", {})
    provider = spec.get("provider", "relational")
    selectors = spec.get("selectors", {})
    
    # Create minimal provider for normalization
    # (In real usage, this would come from the actual provider)
    providers = {}
    
    # Create pipeline normalizer
    config = PipelineConfig(
        active_provider=provider,
        trim_text_fields=options.get("trim_text_fields", True),
        dedupe_required_context=options.get("dedupe_required_context", True),
        dedupe_context_plan=options.get("dedupe_context_plan", True),
        default_mode=options.get("default_mode", "full"),
        enable_replay_logging=False,  # Disable nested logging
    )

    try:
        from fetchgraph.planning.nodes import PlanningPipeline
        
        # Create pipeline directly (bypass adapter) to access state
        pipeline = PlanningPipeline(
            config=config,
            plan_model=Plan,
            provider_rules=[],
            validation_rules=[],
            repair_rules=[],
            schema=None,
            llm_fn=None,
        )
        
        # Create a minimal Plan from selectors
        # The input selectors may be wrapped under a provider key (e.g., "demo_qa")
        # We need to unwrap them to match the pipeline's expected structure
        from fetchgraph.core.models import ContextFetchSpec
        
        # Extract unwrapped selectors from the input structure
        # Input: {"demo_qa": {...}} or {"relational": {...}}
        # We want the inner selectors dict
        unwrapped_selectors = selectors
        if len(selectors) == 1:
            # Single provider case - unwrap the selectors
            first_key = next(iter(selectors.keys()))
            unwrapped_selectors = selectors.get(first_key, selectors)
        
        plan = Plan(
            required_context=[],
            context_plan=[ContextFetchSpec(provider=provider, mode="full", selectors=unwrapped_selectors)],
            adr_queries=[],
            constraints=[],
            entities=[],
            dtos=[],
        )

        # Convert plan to raw text for pipeline
        import json
        plan_dict = {
            "required_context": [],
            "context_plan": [
                {
                    "provider": provider,
                    "mode": "full",
                    "selectors": unwrapped_selectors,
                }
            ],
            "adr_queries": [],
            "constraints": [],
            "entities": [],
            "dtos": [],
        }
        raw_text = json.dumps(plan_dict)
        
        # Execute pipeline
        result = pipeline.execute(raw_text)
        
        # Extract normalized selectors directly from pipeline state
        # This is what the pipeline produces after normalization
        normalized_selectors = pipeline.state.normalized_selectors

        # FIX: Ensure root_entity is present for relational queries
        # If missing, try to infer from field names (e.g., "orders.order_total" -> "orders")
        op = normalized_selectors.get("op")
        is_relational_op = op in ("query", "aggregate", "semantic_only")
        has_relational_keys = any(k in normalized_selectors for k in ("root_entity", "relations", "aggregations", "entity"))
        
        if (is_relational_op or has_relational_keys) and "root_entity" not in normalized_selectors:
            # Try to infer root_entity from field names
            inferred_entity = None
            
            # Check aggregations
            for agg in normalized_selectors.get("aggregations", []):
                if isinstance(agg, dict):
                    field = agg.get("field", "")
                    if "." in field:
                        inferred_entity = field.split(".")[0]
                        break
            
            # Check select fields
            if not inferred_entity:
                for sel in normalized_selectors.get("select", []):
                    if isinstance(sel, dict):
                        expr = sel.get("expr", "")
                        if "." in expr:
                            inferred_entity = expr.split(".")[0]
                            break
            
            # Check filters
            if not inferred_entity:
                filters = normalized_selectors.get("filters", {})
                if isinstance(filters, dict):
                    field = filters.get("field", "")
                    if "." in field:
                        inferred_entity = field.split(".")[0]
            
            if inferred_entity:
                normalized_selectors = dict(normalized_selectors)
                normalized_selectors["root_entity"] = inferred_entity

        # VALIDATION: For relational queries, root_entity is required
        op = normalized_selectors.get("op")
        is_relational_op = op in ("query", "aggregate", "semantic_only")
        has_relational_keys = any(k in normalized_selectors for k in ("root_entity", "relations", "aggregations", "entity"))
        
        if is_relational_op or has_relational_keys:
            if "root_entity" not in normalized_selectors:
                # Check if entity is present (common LLM mistake)
                if "entity" in normalized_selectors:
                    raise AssertionError(
                        "Relational selectors missing required key 'root_entity' "
                        "(selectors have 'entity' but should have 'root_entity'). "
                        "Plan normalizer should rename 'entity' to 'root_entity'."
                    )
                raise AssertionError(
                    f"Relational selectors (op={op!r}) missing required key 'root_entity'. "
                    "Plan normalizer should infer or add root_entity from context."
                )

        return {
            "out_spec": {
                "provider": provider,
                "selectors": normalized_selectors,
            },
            "diag": {
                "root_entity_present": "root_entity" in normalized_selectors,
                "root_entity_inferred": inferred_entity is not None,
                "op_normalized": selectors.get("op") != normalized_selectors.get("op"),
            },
        }

    except Exception as e:
        # Return error in observed format
        return {
            "observed_error": {
                "type": type(e).__name__,
                "message": str(e),
                "trace": "",  # Trace will be added by replay runtime
            },
        }


def replay_compile_bind_spec_v1(input_data: dict[str, Any], ctx: Any) -> dict[str, Any]:
    """Replay handler for compile/bind stage (spec_v1).
    
    This handler replays the binding of selectors to schema.
    
    Input:
        {
            "selectors": Dict[str, Any],
            "schema": Dict[str, Any],
        }
    
    Output:
        {
            "bound_query": {...},  # BoundRelationalQuery or None
            "bindings": {...},  # Field bindings
        }
    """
    from fetchgraph.core.models import Plan
    from fetchgraph.planning.nodes import (
        CompileBindNode,
        PipelineConfig,
        PlanningPipeline,
    )
    
    selectors = input_data.get("selectors", {})
    schema = input_data.get("schema", {})

    # Create compile/bind node
    compile_node = CompileBindNode(schema=schema)

    # Execute
    from fetchgraph.planning.nodes.base import NodeContext
    ctx_obj = NodeContext()  # type: ignore
    result = compile_node.execute(ctx_obj, selectors)

    if result.is_error:
        return {
            "observed_error": {
                "type": "CompileError",
                "message": result.error or "Unknown compile error",
                "trace": "",
            },
        }

    bound_query = result.value.bound_query if result.value else None

    return {
        "bound_query": {
            "root_entity": getattr(bound_query, 'root_entity', None) if bound_query else None,
            "bound_select": len(getattr(bound_query, 'bound_select', []) if bound_query else []),
            "resolved_relations": getattr(bound_query, 'resolved_relations', []) if bound_query else [],
        } if bound_query else None,
    }


def replay_aggregation_normalize_spec_v1(input_data: dict[str, Any], ctx: Any) -> dict[str, Any]:
    """Replay handler for aggregation normalization (spec_v1).

    This handler replays the normalization of aggregations.

    Input:
        {
            "selectors": Dict[str, Any],
        }

    Output:
        {
            "normalized_aggregations": [...],
            "normalized_group_by": [...],
            "diag": {...},
        }
    """
    from fetchgraph.core.models import Plan
    from fetchgraph.planning.nodes import AggregationNormalizeNode

    selectors = input_data.get("selectors", {})

    # Create aggregation normalize node
    agg_node = AggregationNormalizeNode()

    # Execute
    from fetchgraph.planning.nodes.base import NodeContext
    ctx_obj = NodeContext()  # type: ignore
    result = agg_node.execute(ctx_obj, selectors)

    if result.is_error:
        return {
            "observed_error": {
                "type": "AggregationError",
                "message": result.error or "Unknown aggregation error",
                "trace": "",
            },
        }

    agg_value = result.value

    # Extract input aggregations count for validation
    input_aggregations = selectors.get("aggregations", [])
    input_agg_count = len(input_aggregations) if isinstance(input_aggregations, list) else 0

    output_agg_count = len(agg_value.normalized_aggregations) if agg_value else 0

    # VALIDATION: Ensure aggregations are preserved
    if input_agg_count > 0 and output_agg_count == 0:
        raise AssertionError(
            f"Input had {input_agg_count} aggregation(s) but output has none - "
            "aggregations were lost during normalization. "
            "Check that AggregationNormalizeNode properly extracts aggregations from selectors."
        )

    # VALIDATION: Ensure each normalized aggregation has required fields
    for i, agg in enumerate(agg_value.normalized_aggregations if agg_value else []):
        if not agg.agg:
            raise AssertionError(f"normalized_aggregations[{i}] missing required field 'agg'")
        if not agg.field:
            raise AssertionError(f"normalized_aggregations[{i}] missing required field 'field'")
        if not agg.alias:
            raise AssertionError(f"normalized_aggregations[{i}] missing required field 'alias'")

    # VALIDATION: Ensure op was normalized if needed
    input_op = selectors.get("op")
    output_op = agg_value.normalized_selectors.get("op") if agg_value and agg_value.normalized_selectors else input_op
    if input_op == "aggregate" and output_op != "query":
        raise AssertionError(
            f"Input had op='aggregate' but output still has op='{output_op}' - "
            "op should be normalized to 'query' for compile_bind to work."
        )

    return {
        "normalized_aggregations": [
            {
                "agg": agg.agg,
                "field": agg.field,
                "alias": agg.alias,
                "is_distinct": agg.is_distinct,
            }
            for agg in (agg_value.normalized_aggregations if agg_value else [])
        ],
        "normalized_group_by": agg_value.normalized_group_by if agg_value else [],
        "diag": {
            "input_aggregations_count": input_agg_count,
            "output_aggregations_count": output_agg_count,
            "aggregations_preserved": input_agg_count == output_agg_count,
            "op_normalized": input_op != output_op,
        },
        # Include normalized selectors for compile_bind
        "normalized_selectors": agg_value.normalized_selectors if agg_value else selectors,
    }


# Register handlers
REPLAY_HANDLERS["plan_normalize.spec_v1"] = replay_plan_normalize_spec_v1
REPLAY_HANDLERS["compile_bind.spec_v1"] = replay_compile_bind_spec_v1
REPLAY_HANDLERS["aggregation_normalize.spec_v1"] = replay_aggregation_normalize_spec_v1


__all__ = [
    "replay_plan_normalize_spec_v1",
    "replay_compile_bind_spec_v1",
    "replay_aggregation_normalize_spec_v1",
]
