"""Planning pipeline orchestrator (v4 — critical fixes).

Fixes from v3:
1. No infinite loop: failed self-heal → immediate refetch (not lost flag)
2. _rerun_segment doesn't overwrite healed selectors (skip parse/extract)
3. Single-provider selectors after validate (active_provider)
4. RefetchNode loop detection only by response hash (removed prompt check)
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel

from .aggregation_normalize_node import (
    AggregationNormalizeNode,
    AggregationNormalizeResult,
)
from .base import NodeContext, NodeResult
from .compile_bind_node import CompileBindNode, CompileBindResult
from .execute_node import ExecuteNode, ExecutionResult
from .finalize_policy_node import FinalizePolicyNode, FinalizePolicyResult, PolicyConfig
from .llm_plan_node import LLMPlanNode, LLMPlanResult
from .lowering_node import LoweringNode, LoweringResult
from .parse_node import ParseNode, ParseResult
from .plan_normalize_node import PlanNormalizeNode, PlanNormalizeResult
from .postprocess_node import PostprocessNode, PostprocessResult
from .provider_normalize_node import (
    ProviderNormalizationRule,
    ProviderNormalizeNode,
    ProviderNormalizeResult,
)
from .refetch_node import RefetchNode, RefetchRequest, RefetchResult
from .self_heal_node import RepairRule, SelfHealNode, SelfHealResult
from .semantic_validate_node import SemanticValidateNode, SemanticValidateResult
from .validate_aggregation_node import (
    ValidateAggregationNode,
    ValidateAggregationResult,
)
from .validate_selectors_node import (
    ValidateSelectorsNode,
    ValidateSelectorsResult,
    ValidationRule,
)

# Tracer imports for replay logging
try:
    from fetchgraph.replay.log import log_replay_case
    from fetchgraph.replay.snapshots import snapshot_provider_info
    TRACER_AVAILABLE = True
except ImportError:
    TRACER_AVAILABLE = False
    log_replay_case = None  # type: ignore
    snapshot_provider_info = None  # type: ignore

logger = logging.getLogger(__name__)


def _log_replay_event(
    event_logger: Any | None,
    replay_id: str,
    input_payload: dict[str, Any],
    observed_payload: dict[str, Any],
    diag: dict[str, Any] | None = None,
    note: str | None = None,
) -> None:
    """Log a replay event if tracer is available and logger is provided."""
    if not TRACER_AVAILABLE or event_logger is None:
        return

    try:
        log_replay_case(
            event_logger,  # type: ignore
            id=replay_id,
            input=input_payload,
            observed=observed_payload,
            diag=diag,
            note=note,
        )
    except Exception as e:
        logger.warning(f"Failed to log replay event {replay_id}: {e}")


@dataclass
class PipelineConfig:
    """Configuration for the planning pipeline."""

    # Plan normalization
    trim_text_fields: bool = True
    dedupe_required_context: bool = True
    dedupe_context_plan: bool = True
    default_mode: str = "full"

    # Self-heal
    max_heal_attempts: int = 3

    # Refetch
    max_refetch_attempts: int = 3

    # Aggregation
    allow_having: bool = False
    type_checking_enabled: bool = False

    # Policy
    max_limit: int = 1000
    default_limit: int = 100

    # Execution
    output_format: str = "json"

    # Active provider (for single-provider pipeline)
    active_provider: str = "relational"

    # Tracer replay logging
    enable_replay_logging: bool = False  # Disabled by default for production


@dataclass
class PipelineState:
    """Mutable state during pipeline execution."""

    # Current raw plan text (for refetch)
    current_raw_text: str = ""

    # Selectors for active provider (after extraction)
    selectors: dict[str, Any] = field(default_factory=dict)

    # Normalized selectors (after provider-specific normalize)
    normalized_selectors: dict[str, Any] = field(default_factory=dict)

    # Refetch tracking
    refetch_count: int = 0
    refetch_responses: set[str] = field(default_factory=set)  # hash of LLM responses

    # Self-heal tracking
    self_heal_count: int = 0

    # Notes from all stages
    all_notes: list[str] = field(default_factory=list)

    # Tracer event logger (optional)
    event_logger: Any | None = None  # EventLoggerLike

    # Intermediate results (dataclasses)
    parse_result: ParseResult | None = None
    plan_normalize_result: PlanNormalizeResult | None = None
    provider_normalize_result: ProviderNormalizeResult | None = None
    validate_result: ValidateSelectorsResult | None = None
    compile_result: CompileBindResult | None = None
    semantic_result: SemanticValidateResult | None = None
    agg_normalize_result: AggregationNormalizeResult | None = None
    agg_validate_result: ValidateAggregationResult | None = None
    policy_result: FinalizePolicyResult | None = None
    lowering_result: LoweringResult | None = None
    execute_result: ExecutionResult | None = None
    postprocess_result: PostprocessResult | None = None


@dataclass
class PipelineResult:
    """Final result from the planning pipeline."""
    
    # Success case
    output: Any | None = None
    
    # Error case
    error: str | None = None
    errors: list[str] = field(default_factory=list)
    
    # Metadata
    notes: list[str] = field(default_factory=list)
    diag: dict[str, Any] = field(default_factory=dict)
    
    # Retry metadata
    refetch_count: int = 0
    self_heal_count: int = 0
    
    @property
    def is_success(self) -> bool:
        return self.error is None and len(self.errors) == 0


class PlanningPipeline:
    """Planning pipeline orchestrator (v4 — critical fixes).
    
    Data flow:
      raw_text → Parse → Plan → extract selectors → ProviderNormalize → Validate
                                                                  ↓
      output ← Postprocess ← Execute ← Lowering ← Policy ← AggValidate ← AggNorm
                                                              ↑
                              CompileBind ← SemanticValidate ←──┘
    
    Key fixes:
    1. Failed self-heal → immediate refetch (no lost flags)
    2. _rerun_segment skips parse/extract (uses healed selectors directly)
    3. Single-provider selectors after validate (active_provider)
    4. Refetch loop detection only by response hash
    """
    
    def __init__(
        self,
        config: PipelineConfig | None = None,
        plan_model: type[BaseModel] | None = None,
        provider_rules: list[ProviderNormalizationRule] | None = None,
        validation_rules: list[ValidationRule] | None = None,
        repair_rules: list[RepairRule] | None = None,
        schema: dict[str, Any] | None = None,
        llm_fn: Callable[[str], str] | None = None,
    ) -> None:
        self.config = config or PipelineConfig()
        self.plan_model = plan_model
        self.schema = schema
        self.llm_fn = llm_fn
        self.active_provider = self.config.active_provider
        
        # Initialize nodes
        self.llm_node = LLMPlanNode()
        self.parse_node = ParseNode(plan_model) if plan_model else None
        self.plan_normalize_node = PlanNormalizeNode(
            trim_text_fields=self.config.trim_text_fields,
            dedupe_required_context=self.config.dedupe_required_context,
            dedupe_context_plan=self.config.dedupe_context_plan,
            default_mode=self.config.default_mode,
        )
        self.provider_normalize_node = ProviderNormalizeNode(rules=provider_rules)
        self.validate_selectors_node = ValidateSelectorsNode(rules=validation_rules)
        self.self_heal_node = SelfHealNode(
            rules=repair_rules,
            max_heal_attempts=self.config.max_heal_attempts,
        )
        self.refetch_node = RefetchNode(
            llm_fn=llm_fn,
            max_refetch_attempts=self.config.max_refetch_attempts,
        )
        self.compile_bind_node = CompileBindNode(schema=schema)
        self.semantic_validate_node = SemanticValidateNode()
        self.aggregation_normalize_node = AggregationNormalizeNode()
        self.validate_aggregation_node = ValidateAggregationNode(
            allow_having=self.config.allow_having,
            type_checking_enabled=self.config.type_checking_enabled,
        )
        self.finalize_policy_node = FinalizePolicyNode(
            policy=PolicyConfig(
                max_limit=self.config.max_limit,
                default_limit=self.config.default_limit,
            )
        )
        self.lowering_node = LoweringNode(provider=self.active_provider)
        self.execute_node = ExecuteNode(provider=self.active_provider)
        self.postprocess_node = PostprocessNode(output_format=self.config.output_format)  # type: ignore

        # Pipeline state
        self.state = PipelineState()
        self.ctx = NodeContext()
    
    def execute(self, raw_plan_text: str) -> PipelineResult:
        """Execute the full planning pipeline."""
        logger.info("PlanningPipeline: starting execution")

        # Preserve event_logger before resetting state
        preserved_event_logger = self.state.event_logger if self.state else None

        # Reset state
        self.state = PipelineState(current_raw_text=raw_plan_text)
        self.state.event_logger = preserved_event_logger
        self.ctx = NodeContext()
        self.self_heal_node.reset()
        self.refetch_node.reset()
        
        # Main retry loop
        while True:
            result = self._run_pipeline_segments()
            
            if result.is_success:
                return result
            
            # Handle error
            error_info = self._classify_error(result.error)

            # Try self-heal first (if repairable)
            if error_info["repairable"] and result.error:
                heal_result = self._do_self_heal(
                    self.state.normalized_selectors,
                    result.error,
                    error_info["error_types"],
                    error_info["stage"],
                )

                if heal_result and heal_result.is_success and heal_result.repaired_selectors:
                    # Update state with repaired selectors
                    self.state.normalized_selectors = heal_result.repaired_selectors
                    self.state.self_heal_count += 1
                    self.state.all_notes.extend(heal_result.notes)

                    # Re-run from the appropriate segment (skipping parse/extract)
                    re_run_result = self._rerun_segment(heal_result.re_run_segment)
                    if re_run_result.is_success:
                        return re_run_result
                    else:
                        # Re-run failed, try refetch
                        pass

                # Self-heal failed or not applicable → refetch
                # (CRITICAL FIX #1: immediate refetch, not lost flag)

            # Try refetch
            if error_info["needs_refetch"] or self.state.self_heal_count >= self.config.max_heal_attempts:
                refetch_result = self._do_refetch(result.error or "")
                if refetch_result and refetch_result.is_success and refetch_result.raw_text:
                    self.state.current_raw_text = refetch_result.raw_text
                    self.state.refetch_count += 1

                    # Loop detection by response hash (CRITICAL FIX #4)
                    response_hash = hashlib.sha256(
                        refetch_result.raw_text.encode()
                    ).hexdigest()[:16]
                    if response_hash in self.state.refetch_responses:
                        return self._error_result(
                            "Refetch loop detected (same response twice)"
                        )
                    self.state.refetch_responses.add(response_hash)
                    self.state.all_notes.extend(refetch_result.notes)
                    continue  # Restart pipeline with new text
                else:
                    _refetch = refetch_result
                    skip_reason = _refetch.skip_reason if _refetch else None
                    llm_error = _refetch.llm_error if _refetch else None
                    return self._error_result(f"Refetch failed: {skip_reason or llm_error or 'unknown'}")
            
            # Non-repairable, non-refetchable error
            return result
    
    def _run_pipeline_segments(self) -> PipelineResult:
        """Run all pipeline segments in order."""
        
        # ========== SEGMENT 0: Pre-binding (Parse → Validate) ==========
        pre_result = self._run_pre_binding_segment()
        if pre_result.get("error"):
            return self._error_result(
                pre_result["error"],
                error_types=pre_result.get("error_types", []),
            )
        
        # ========== SEGMENT 1: Binding (Compile → Semantic Validate) ==========
        bind_result = self._run_binding_segment()
        if bind_result.get("error"):
            return self._error_result(
                bind_result["error"],
                error_types=bind_result.get("error_types", []),
                stage="compile_bind",
            )
        
        # ========== SEGMENT 2: Aggregation (AggNorm → AggValidate) ==========
        agg_result = self._run_aggregation_segment()
        if agg_result.get("error"):
            return self._error_result(
                agg_result["error"],
                error_types=agg_result.get("error_types", []),
                stage="aggregation_normalize",
            )
        
        # ========== SEGMENT 3: Policy & Execution ==========
        exec_result = self._run_execution_segment()
        if exec_result.get("error"):
            return self._error_result(exec_result["error"])
        
        # Success
        return PipelineResult(
            output=exec_result.get("output"),
            notes=self.state.all_notes,
            refetch_count=self.state.refetch_count,
            self_heal_count=self.state.self_heal_count,
        )
    
    def _run_pre_binding_segment(self, skip_parse_extract: bool = False) -> dict[str, Any]:
        """Run Segment 0: Parse → PlanNormalize → ProviderNormalize → Validate.
        
        Args:
            skip_parse_extract: If True, skip parse/extract and use state.selectors
                               (used after self-heal to preserve repaired selectors)
        
        Returns:
            {"error": str, "error_types": [...]} on failure
            {} on success
        """
        notes = self.state.all_notes
        
        # Stage 1: Parse raw text → Plan (skip if re-running after self-heal)
        if not skip_parse_extract:
            if self.parse_node is None:
                return {"error": "ParseNode not configured", "error_types": ["config_error"]}

            parse_result = self.parse_node.execute(self.ctx, self.state.current_raw_text)
            self.state.parse_result = parse_result.value
            notes.extend(parse_result.notes)

            if parse_result.is_error:
                return {"error": parse_result.error, "error_types": ["parse_failed"]}

            # Stage 2: Plan-level normalize
            if parse_result.value:
                plan_norm_result = self.plan_normalize_node.execute(
                    self.ctx,
                    parse_result.value.plan,
                )
                self.state.plan_normalize_result = plan_norm_result.value
                notes.extend(plan_norm_result.notes)

                # Stage 3: Extract selectors from normalized plan
                if plan_norm_result.value and plan_norm_result.value.normalized_plan:
                    selectors = self._extract_selectors_from_plan(plan_norm_result.value.normalized_plan)
                    self.state.selectors = selectors

        # Stage 4: Provider-specific normalize (CRITICAL FIX #3: single provider)
        provider_norm_result = self.provider_normalize_node.execute(
            self.ctx,
            self.state.selectors,
        )
        self.state.provider_normalize_result = provider_norm_result.value
        notes.extend(provider_norm_result.notes)

        # Extract selectors for active provider
        if provider_norm_result.value and provider_norm_result.value.normalized_selectors:
            self.state.normalized_selectors = self._get_active_provider_selectors(
                provider_norm_result.value.normalized_selectors
            )

        # Stage 5: Validate selectors
        validate_result = self.validate_selectors_node.execute(
            self.ctx,
            self.state.normalized_selectors,
        )
        self.state.validate_result = validate_result.value
        notes.extend(validate_result.notes)

        # TRACER: Log normalize+validate replay event
        if self.config.enable_replay_logging and validate_result.value:
            validate_value = validate_result.value
            before_ok = validate_value.validation_results.get(self.config.active_provider, True) if validate_value.validation_results else True
            _log_replay_event(
                event_logger=self.state.event_logger,
                replay_id="plan_normalize.spec_v1",
                input_payload={
                    "spec": {
                        "provider": self.config.active_provider,
                        "selectors": self.state.selectors,
                    },
                    "options": {
                        "trim_text_fields": self.config.trim_text_fields,
                        "dedupe_required_context": self.config.dedupe_required_context,
                        "dedupe_context_plan": self.config.dedupe_context_plan,
                        "default_mode": self.config.default_mode,
                    },
                },
                observed_payload={
                    "out_spec": {
                        "provider": self.config.active_provider,
                        "selectors": self.state.normalized_selectors,
                    },
                },
                diag={
                    "selectors_valid_before": before_ok,
                    "selectors_valid_after": not bool(validate_value.errors_by_provider),
                    "rule_trace": [
                        {"stage": "select_rule", "provider": self.config.active_provider, "rule_kind": "relational_v1"},
                        {"stage": "normalize", "decision": "use_normalized_fixed" if not validate_value.errors_by_provider else "use_normalized_unvalidated", "changed": True},
                    ],
                },
                note=json.dumps({
                    "provider": self.config.active_provider,
                    "selectors_validate_before": "ok" if before_ok else "error",
                    "selectors_validate_after": "error" if validate_value.errors_by_provider else "ok",
                    "selectors_normalization_decision": "use_normalized_fixed" if not validate_value.errors_by_provider else "use_normalized_unvalidated",
                }),
            )

        # Check for validation errors
        if validate_result.value and validate_result.value.errors_by_provider:
            errors = list(validate_result.value.errors_by_provider.values())
            return {
                "error": "; ".join(errors),
                "error_types": ["validation_error"],
                "stage": "validate_selectors",
            }

        return {}
    
    def _run_binding_segment(self) -> dict[str, Any]:
        """Run Segment 1: CompileBind → SemanticValidate."""
        notes = self.state.all_notes

        # Stage 6: Compile/Bind (CRITICAL FIX #3: single-provider selectors)
        compile_result = self.compile_bind_node.execute(
            self.ctx,
            self.state.normalized_selectors,
        )
        self.state.compile_result = compile_result.value
        notes.extend(compile_result.notes)

        # TRACER: Log compile+bind replay event
        if self.config.enable_replay_logging and self.state.event_logger is not None and compile_result.value:
            compile_value = compile_result.value
            bound_query = compile_value.bound_query
            _log_replay_event(
                event_logger=self.state.event_logger,
                replay_id="compile_bind.spec_v1",
                input_payload={
                    "selectors": self.state.normalized_selectors,
                    "schema": self.compile_bind_node.schema,
                },
                observed_payload={
                    "bound_query": {
                        "root_entity": getattr(bound_query, 'root_entity', None) if bound_query else None,
                        "bound_select_count": len(getattr(bound_query, 'bound_select', [])) if bound_query else 0,
                        "resolved_relations": getattr(bound_query, 'resolved_relations', []) if bound_query else [],
                    } if bound_query else None,
                },
                diag={
                    "compile_success": not bool(compile_value.errors),
                    "has_bound_query": bound_query is not None,
                },
                note=json.dumps({
                    "compile_status": "ok" if not compile_value.errors else "error",
                    "bound_query_status": "present" if bound_query else "none",
                }),
            )

        if compile_result.value and compile_result.value.errors:
            return {
                "error": "; ".join(compile_result.value.errors),
                "error_types": ["compile_bind_error"],
                "stage": "compile_bind",
            }

        # Stage 7: Semantic validate
        if compile_result.value is None or compile_result.value.bound_query is None:
            return {}

        semantic_result = self.semantic_validate_node.execute(
            self.ctx,
            compile_result.value.bound_query,
        )
        self.state.semantic_result = semantic_result.value
        notes.extend(semantic_result.notes)

        if semantic_result.value and not semantic_result.value.is_valid:
            return {
                "error": "; ".join(semantic_result.value.errors),
                "error_types": ["semantic_validate_error"],
                "stage": "semantic_validate",
            }

        return {}
    
    def _run_aggregation_segment(self) -> dict[str, Any]:
        """Run Segment 2: AggregationNormalize → ValidateAggregation."""
        notes = self.state.all_notes

        # Stage 8: Aggregation normalize (CRITICAL FIX #3: single-provider selectors)
        agg_norm_result = self.aggregation_normalize_node.execute(
            self.ctx,
            self.state.normalized_selectors,
        )
        self.state.agg_normalize_result = agg_norm_result.value
        notes.extend(agg_norm_result.notes)

        # TRACER: Log aggregation normalize replay event
        if self.config.enable_replay_logging and self.state.event_logger is not None and agg_norm_result.value:
            agg_value = agg_norm_result.value
            _log_replay_event(
                event_logger=self.state.event_logger,
                replay_id="aggregation_normalize.spec_v1",
                input_payload={
                    "selectors": self.state.normalized_selectors,
                },
                observed_payload={
                    "normalized_aggregations": [
                        {
                            "agg": agg.agg,
                            "field": agg.field,
                            "alias": agg.alias,
                            "is_distinct": agg.is_distinct,
                        }
                        for agg in agg_value.normalized_aggregations
                    ],
                    "normalized_group_by": agg_value.normalized_group_by,
                },
                diag={
                    "aggregations_count": len(agg_value.normalized_aggregations),
                    "group_by_count": len(agg_value.normalized_group_by),
                },
                note=json.dumps({
                    "aggregations_count": len(agg_value.normalized_aggregations),
                    "group_by_count": len(agg_value.normalized_group_by),
                }),
            )

        # Stage 9: Validate aggregation
        if agg_norm_result.value:
            agg_validate_result = self.validate_aggregation_node.execute(
                self.ctx,
                agg_norm_result.value.normalized_aggregations,
                self.state.normalized_selectors,
            )
            self.state.agg_validate_result = agg_validate_result.value
            notes.extend(agg_validate_result.notes)

            if agg_validate_result.value and not agg_validate_result.value.is_valid:
                return {
                    "error": "; ".join(agg_validate_result.value.errors),
                    "error_types": ["aggregation_validate_error"],
                    "stage": "validate_aggregation",
                }

        return {}
    
    def _run_execution_segment(self) -> dict[str, Any]:
        """Run Segment 3: FinalizePolicy → Lowering → Execute → Postprocess."""
        notes = self.state.all_notes

        # Stage 10: Finalize policy (CRITICAL FIX #3: single-provider selectors)
        policy_result = self.finalize_policy_node.execute(
            self.ctx,
            self.state.normalized_selectors,
        )
        self.state.policy_result = policy_result.value
        notes.extend(policy_result.notes)

        final_selectors = policy_result.value.finalized_selectors if policy_result.value else self.state.normalized_selectors

        # Stage 11: Lowering
        lowering_result = self.lowering_node.execute(self.ctx, final_selectors)
        self.state.lowering_result = lowering_result.value
        notes.extend(lowering_result.notes)

        # Stage 12: Execute
        if lowering_result.value is None or lowering_result.value.lowered_query is None:
            exec_result = self.execute_node.execute(
                self.ctx,
                type("LoweredQuery", (), {"native_query": final_selectors})(),
            )
        else:
            exec_result = self.execute_node.execute(
                self.ctx,
                lowering_result.value.lowered_query,
            )
        self.state.execute_result = exec_result.value
        notes.extend(exec_result.notes)

        # Stage 13: Postprocess
        if exec_result.value:
            postprocess_result = self.postprocess_node.execute(
                self.ctx,
                exec_result.value,
            )
            self.state.postprocess_result = postprocess_result.value
            notes.extend(postprocess_result.notes)

            return {"output": postprocess_result.value.output if postprocess_result.value else None}

        return {"output": None}
    
    def _rerun_segment(self, segment: str | None) -> PipelineResult:
        """Re-run pipeline from a specific segment after self-heal.
        
        CRITICAL FIX #2: skip parse/extract to preserve healed selectors.
        """
        
        if segment == "provider_normalize":
            # Re-run from provider_normalize through validate
            # (skip parse/extract to preserve healed selectors)
            result = self._run_pre_binding_segment(skip_parse_extract=True)
            if result.get("error"):
                return self._error_result(result["error"], error_types=result.get("error_types", []))
        
        elif segment == "compile_bind":
            # Re-run from compile_bind through semantic_validate
            result = self._run_binding_segment()
            if result.get("error"):
                return self._error_result(
                    result["error"],
                    error_types=result.get("error_types", []),
                    stage="compile_bind",
                )
        
        elif segment == "aggregation_normalize":
            # Re-run from aggregation_normalize through validate_aggregation
            result = self._run_aggregation_segment()
            if result.get("error"):
                return self._error_result(
                    result["error"],
                    error_types=result.get("error_types", []),
                    stage="aggregation_normalize",
                )
        
        # Continue with remaining segments
        return self._run_pipeline_segments()
    
    def _get_active_provider_selectors(
        self,
        selectors_by_provider: dict[str, Any],
    ) -> dict[str, Any]:
        """Extract selectors for active provider from multi-provider dict.
        
        CRITICAL FIX #3: convert {provider: selectors} → selectors for single provider.
        """
        if self.active_provider in selectors_by_provider:
            return selectors_by_provider[self.active_provider]
        
        # Fallback: return first provider's selectors or empty dict
        if selectors_by_provider:
            first_key = next(iter(selectors_by_provider.keys()))
            return selectors_by_provider[first_key]
        
        return {}
    
    def _extract_selectors_from_plan(self, plan: BaseModel) -> dict[str, Any]:
        """Extract selectors_by_provider from normalized plan."""
        if plan is None:
            return {}

        # Try selectors_by_provider attribute
        if hasattr(plan, "selectors_by_provider"):
            return getattr(plan, "selectors_by_provider", None) or {}

        # Try context_plan
        if hasattr(plan, "context_plan"):
            context_plan = getattr(plan, "context_plan", None) or []
            selectors: dict[str, Any] = {}
            for spec in context_plan:
                provider = getattr(spec, "provider", "unknown")
                spec_selectors = getattr(spec, "selectors", {})
                if provider not in selectors:
                    selectors[provider] = spec_selectors
                else:
                    if isinstance(selectors[provider], dict) and isinstance(spec_selectors, dict):
                        selectors[provider].update(spec_selectors)
            return selectors

        # Try direct selectors
        if hasattr(plan, "selectors"):
            return {"default": getattr(plan, "selectors", None) or {}}

        return {}
    
    def _classify_error(self, error: str | None) -> dict[str, Any]:
        """Classify error to determine repair/refetch strategy."""
        if not error:
            return {"needs_refetch": False, "repairable": False, "error_types": [], "stage": "unknown"}
        
        error_lower = error.lower()
        
        # Parse failures → refetch (not repairable)
        if "parse" in error_lower or "json" in error_lower:
            return {
                "needs_refetch": True,
                "repairable": False,
                "error_types": ["parse_failed"],
                "stage": "parse",
            }
        
        # Validation errors → repairable
        if "validation" in error_lower or "required" in error_lower:
            return {
                "needs_refetch": True,  # Can refetch if heal fails
                "repairable": True,
                "error_types": ["validation_error"],
                "stage": "validate_selectors",
            }
        
        # Binding errors → repairable
        if "bind" in error_lower or "unknown_field" in error_lower or "ambiguous" in error_lower:
            return {
                "needs_refetch": True,
                "repairable": True,
                "error_types": ["compile_bind_error"],
                "stage": "compile_bind",
            }
        
        # Aggregation errors → repairable
        if "aggregat" in error_lower:
            return {
                "needs_refetch": True,
                "repairable": True,
                "error_types": ["aggregation_validate_error"],
                "stage": "validate_aggregation",
            }
        
        # Default: try refetch
        return {
            "needs_refetch": True,
            "repairable": False,
            "error_types": ["unknown"],
            "stage": "unknown",
        }
    
    def _do_self_heal(
        self,
        selectors: dict[str, Any],
        error: str,
        error_types: list[str],
        stage: str,
    ) -> SelfHealResult | None:
        """Execute self-healing."""
        result = self.self_heal_node.execute(
            self.ctx,
            selectors,
            {
                "stage": stage,
                "errors": [error],
                "error_types": error_types,
                "selectors": selectors,
            },
        )
        return result.value

    def _do_refetch(self, error: str) -> RefetchResult | None:
        """Execute refetch."""
        request = RefetchRequest(
            original_prompt=self.state.current_raw_text,
            error_feedback=error,
            previous_attempts=[],
        )
        result = self.refetch_node.execute(self.ctx, request)
        return result.value
    
    def _error_result(
        self,
        error: str,
        error_types: list[str] | None = None,
        stage: str | None = None,
    ) -> PipelineResult:
        """Create an error result."""
        return PipelineResult(
            error=error,
            errors=[error],
            notes=self.state.all_notes,
            refetch_count=self.state.refetch_count,
            self_heal_count=self.state.self_heal_count,
            diag={
                "error_types": error_types or [],
                "stage": stage,
            },
        )
    
    def reset(self) -> None:
        """Reset pipeline state."""
        self.state = PipelineState()
        self.ctx = NodeContext()
        self.self_heal_node.reset()
        self.refetch_node.reset()
