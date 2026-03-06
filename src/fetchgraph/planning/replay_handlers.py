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
        normalizer = create_pipeline_normalizer(
            providers=providers,
            plan_model=Plan,
            config=config,
        )

        # Create a minimal Plan from selectors
        plan = Plan(
            required_context=[],
            context_plan=[],
        )
        # Set selectors_by_provider dynamically (not in constructor)
        setattr(plan, 'selectors_by_provider', {provider: selectors})

        # Normalize
        normalized_plan = normalizer.normalize(plan)

        # Extract normalized selectors
        normalized_selectors = {}
        if hasattr(normalized_plan, 'selectors_by_provider'):
            normalized_selectors = getattr(normalized_plan, 'selectors_by_provider', {}).get(provider, selectors)
        elif hasattr(normalized_plan, 'context_plan'):
            for spec_item in getattr(normalized_plan, 'context_plan', []):
                if getattr(spec_item, 'provider', None) == provider:
                    normalized_selectors = getattr(spec_item, 'selectors', {})
                    break

        return {
            "out_spec": {
                "provider": provider,
                "selectors": normalized_selectors,
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
