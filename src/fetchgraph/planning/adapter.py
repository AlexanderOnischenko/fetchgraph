"""Integration of PlanningPipeline nodes into fetchgraph agent.

This module provides a bridge between the new node-based pipeline
and the existing BaseGraphAgent architecture.

Usage:
    from fetchgraph.planning.adapter import create_pipeline_normalizer

    pipeline_normalizer = create_pipeline_normalizer(
        providers=providers,
        plan_model=MyPlan,
        schema=schema,
        llm_fn=llm_invoke,
    )

    agent = BaseGraphAgent(
        ...,
        plan_normalizer=pipeline_normalizer,
    )
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from fetchgraph.core.models import Plan
from fetchgraph.core.protocols import ContextProvider, SupportsDescribe
from fetchgraph.replay.log import EventLoggerLike

from .nodes import (
    PipelineConfig,
    PipelineResult,
    PlanningPipeline,
    ProviderNormalizationRule,
    RepairRule,
    ValidationRule,
)

# Import replay handlers to register them (side-effect import)
# This enables tracer fixture creation for pipeline stages
try:
    from . import replay_handlers  # noqa: F401
    REPLAY_HANDLERS_REGISTERED = True
except ImportError:
    REPLAY_HANDLERS_REGISTERED = False

logger = logging.getLogger(__name__)


class PipelineNormalizerAdapter:
    """Adapter that wraps PlanningPipeline to implement PlanNormalizer interface.
    
    This allows using the new node-based pipeline with existing BaseGraphAgent
    without changing the agent's code.
    """
    
    def __init__(
        self,
        pipeline: PlanningPipeline,
        plan_model: type[BaseModel],
    ) -> None:
        self.pipeline = pipeline
        self.plan_model = plan_model
        self.event_logger: EventLoggerLike | None = None
        self.provider_catalog: dict[str, Any] = {}
    
    def normalize(self, plan: Plan, *, event_logger: EventLoggerLike | None = None) -> Plan:
        """Normalize a Plan using the node-based pipeline.

        This is the main entry point called by BaseGraphAgent.

        Args:
            plan: The Plan to normalize
            event_logger: Optional event logger for replay

        Returns:
            Normalized Plan (or original if pipeline fails)
        """
        self.event_logger = event_logger or self.event_logger

        # Pass event_logger to pipeline state
        self.pipeline.state.event_logger = self.event_logger

        # Convert Plan to raw text representation
        # (In real usage, this would come from LLM, but here we reconstruct from Plan)
        raw_plan_text = self._plan_to_raw_text(plan)

        # Build schema info for self-heal feedback
        schema_info = self.pipeline.config.schema

        # Execute pipeline with schema info for self-heal feedback
        result = self.pipeline.execute(
            raw_plan_text,
            original_query="",  # Will be set by caller if available
            schema_info=schema_info,
        )

        if result.is_success:
            # Convert pipeline output back to Plan
            normalized_plan = self._pipeline_output_to_plan(result)
            return normalized_plan
        else:
            # Pipeline failed, return original plan with notes
            logger.warning(f"Pipeline normalization failed: {result.error}")
            plan.normalization_notes = getattr(plan, 'normalization_notes', []) + [
                f"Pipeline normalization failed: {result.error}"
            ]
            return plan
    
    def normalize_specs(
        self,
        specs: list[Any],
        *,
        notes: list[str] | None = None,
        event_logger: EventLoggerLike | None = None,
    ) -> list[Any]:
        """Normalize context fetch specs (legacy interface).
        
        For now, this is a stub that passes through unchanged.
        The main normalization happens in normalize().
        """
        return specs
    
    def _plan_to_raw_text(self, plan: Plan) -> str:
        """Convert Plan object to raw text representation.
        
        This is a stub - in real usage, the raw text comes from LLM.
        Here we reconstruct a JSON-like representation.
        """
        import json
        
        # Extract key fields from Plan
        plan_dict = {
            "required_context": getattr(plan, 'required_context', []),
            "context_plan": [
                {
                    "provider": getattr(spec, 'provider', None),
                    "mode": getattr(spec, 'mode', None),
                    "selectors": getattr(spec, 'selectors', {}),
                }
                for spec in getattr(plan, 'context_plan', [])
            ],
            "adr_queries": getattr(plan, 'adr_queries', []),
            "constraints": getattr(plan, 'constraints', []),
            "entities": getattr(plan, 'entities', []),
            "dtos": getattr(plan, 'dtos', []),
        }
        
        return json.dumps(plan_dict, ensure_ascii=False, default=str)
    
    def _pipeline_output_to_plan(self, result: PipelineResult) -> Plan:
        """Convert PipelineResult back to Plan object.

        This reconstructs a Plan from the pipeline's normalized selectors.
        """
        from fetchgraph.core.models import ContextFetchSpec, Plan

        # Extract normalized selectors from PipelineResult
        # The pipeline now exposes them in result.normalized_selectors
        normalized_selectors = result.normalized_selectors if hasattr(result, 'normalized_selectors') else {}

        # Reconstruct Plan with normalized selectors in context_plan
        # The selectors should be unwrapped (no provider key)
        # IMPORTANT: Use the pipeline's active_provider, not a hardcoded value
        provider_name = self.pipeline.active_provider or "unknown"
        normalized_plan = Plan(
            required_context=[],
            context_plan=[ContextFetchSpec(provider=provider_name, mode="full", selectors=normalized_selectors)] if normalized_selectors else [],
            adr_queries=[],
            constraints=[],
            entities=[],
            dtos=[],
        )

        # Add normalization notes
        notes = result.notes if hasattr(result, 'notes') else []
        normalized_plan.normalization_notes = notes

        return normalized_plan


def create_pipeline_normalizer(
    providers: dict[str, ContextProvider],
    plan_model: type[BaseModel] | None = None,
    schema: dict[str, Any] | None = None,
    llm_fn: Callable[[str], str] | None = None,
    config: PipelineConfig | None = None,
    provider_rules: list[ProviderNormalizationRule] | None = None,
    validation_rules: list[ValidationRule] | None = None,
    repair_rules: list[RepairRule] | None = None,
    enable_replay_logging: bool = False,  # Disabled by default for production
    max_heal_attempts: int = 3,  # NEW: self-heal attempts
    max_refetch_attempts: int = 3,  # NEW: LLM refetch attempts
) -> PipelineNormalizerAdapter:
    """Create a PipelineNormalizerAdapter from providers.

    This is the main factory function for creating the pipeline normalizer.

    Args:
        providers: Dictionary of provider name → provider instance
        plan_model: Pydantic model for Plan (e.g., from PlanParser)
        schema: JSON schema for validation
        llm_fn: LLM function for refetch (optional)
        config: Pipeline configuration (optional, uses defaults)
        provider_rules: Provider-specific normalization rules
        validation_rules: Validation rules
        repair_rules: Self-heal repair rules
        max_heal_attempts: Maximum self-heal attempts (default: 3, 0 to disable)
        max_refetch_attempts: Maximum LLM refetch attempts (default: 3, 0 to disable)

    Returns:
        PipelineNormalizerAdapter ready to use with BaseGraphAgent
    """
    # Import Plan model if not provided
    if plan_model is None:
        from fetchgraph.core.models import Plan
        plan_model = Plan
    
    # Build provider rules from providers if not provided
    if provider_rules is None:
        provider_rules = _build_provider_rules_from_providers(providers)
    
    # Build validation rules if not provided
    if validation_rules is None:
        validation_rules = _build_validation_rules(providers)
    
    # Build repair rules if not provided
    if repair_rules is None:
        repair_rules = _build_repair_rules()

    # Create pipeline config with replay logging setting
    # IMPORTANT: active_provider must be set from providers dict to avoid mismatches
    if config is None:
        # Derive active_provider from the first provider in the dict
        # If providers is empty, allow it but pipeline will fail if actually used
        # This is acceptable for test scenarios that don't invoke normalization
        if providers:
            active_provider_name = next(iter(providers.keys()))
            
            # Build schema info from providers for self-heal feedback
            schema_info = {}
            provider_catalog = {}
            for name, prov in providers.items():
                if isinstance(prov, SupportsDescribe):
                    try:
                        info = prov.describe()
                        provider_catalog[name] = info
                        
                        # Extract entities from describe() result
                        # ProviderInfo doesn have entities directly, but some providers
                        # return additional info via describe() that we can use
                        if hasattr(info, 'selectors_schema') and info.selectors_schema:
                            # Extract entity info from selectors_schema if available
                            schema_info[name] = {"schema": info.selectors_schema}
                    except Exception:
                        pass
            
            config = PipelineConfig(
                active_provider=active_provider_name,
                enable_replay_logging=enable_replay_logging,
                schema=schema_info,
                provider_catalog=provider_catalog,
                max_heal_attempts=max_heal_attempts,
                max_refetch_attempts=max_refetch_attempts,
            )
        else:
            # Empty providers - create config without active_provider
            # Pipeline will raise on first use, which is fine for tests
            config = PipelineConfig(
                active_provider=None,
                enable_replay_logging=enable_replay_logging,
                max_heal_attempts=max_heal_attempts,
                max_refetch_attempts=max_refetch_attempts,
            )
    else:
        # Override enable_replay_logging if explicitly provided
        config.enable_replay_logging = enable_replay_logging
        # Validate active_provider if set
        if config.active_provider is not None and config.active_provider not in providers:
            raise ValueError(
                f"PipelineConfig.active_provider={config.active_provider!r} does not match "
                f"any of the registered providers: {list(providers.keys())}. "
                f"Either change active_provider to one of the provider names, or register "
                f"a provider with the name {config.active_provider!r}."
            )

    # Create pipeline
    pipeline = PlanningPipeline(
        config=config,
        plan_model=plan_model,
        provider_rules=provider_rules,
        validation_rules=validation_rules,
        repair_rules=repair_rules,
        schema=schema,
        llm_fn=llm_fn,
    )
    
    # Create adapter
    adapter = PipelineNormalizerAdapter(pipeline, plan_model)

    # Extract provider catalog for compatibility
    for name, provider in providers.items():
        try:
            if hasattr(provider, 'describe'):
                info = provider.describe()  # type: ignore
                adapter.provider_catalog[name] = {
                    'name': info.name if hasattr(info, 'name') else name,
                    'capabilities': info.capabilities if hasattr(info, 'capabilities') else [],
                }
        except Exception as e:
            logger.warning(f"Failed to describe provider {name}: {e}")
            adapter.provider_catalog[name] = {'name': name, 'capabilities': []}

    return adapter


def _build_provider_rules_from_providers(
    providers: dict[str, ContextProvider]
) -> list[ProviderNormalizationRule]:
    """Build provider normalization rules from provider instances."""
    rules = []

    for name, provider in providers.items():
        # Check if provider has a normalizer (e.g., relational providers)
        if hasattr(provider, 'normalize_selectors'):
            normalize_fn = getattr(provider, 'normalize_selectors')
            rules.append(ProviderNormalizationRule(
                provider=name,
                kind=f"{name}_v1",
                validator=None,  # type: ignore  # Would need TypeAdapter
                normalize_selectors=normalize_fn,
            ))

    return rules


def _build_validation_rules(
    providers: dict[str, ContextProvider]
) -> list[ValidationRule]:
    """Build validation rules from providers."""
    rules = []

    for name, provider in providers.items():
        # Check if provider has a validator
        if hasattr(provider, 'validate_selectors'):
            validate_fn = getattr(provider, 'validate_selectors')
            rules.append(ValidationRule(
                provider=name,
                validator=None,  # type: ignore  # Would need TypeAdapter
                kind=f"{name}_v1",
            ))

    return rules


def _build_repair_rules() -> list[RepairRule]:
    """Build default repair rules."""
    # Import built-in repair rules from self_heal_node
    from .nodes import (
        make_aggregation_normalize_repair_rule,
        make_ambiguous_field_repair_rule,
        make_missing_relation_repair_rule,
        make_missing_value_repair_rule,
        make_unknown_field_repair_rule,
    )
    
    return [
        make_missing_value_repair_rule(),
        make_ambiguous_field_repair_rule(),
        make_unknown_field_repair_rule(),
        make_missing_relation_repair_rule(),
        make_aggregation_normalize_repair_rule(),
    ]


__all__ = [
    "PipelineNormalizerAdapter",
    "create_pipeline_normalizer",
]
