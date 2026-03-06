"""Planning pipeline nodes.

Each node represents a distinct stage in the plan normalization and compilation pipeline.
Nodes are intentionally granular to allow:
- Clear separation of concerns
- Independent testing
- Flexible composition
- Observability at each stage

Pipeline stages (v4 — critical fixes):

0) PLAN stage: LLM → Plan (общий для всех провайдеров)
    1. LLM.plan() → raw текст
    2. Parse (строго): raw → Plan model (JsonParser.extract для markdown)
    3. Plan-level normalize (общая нормализация плана)
    4. Provider-spec normalize (shape-нормализация selectors по провайдеру)
    5. Validate selectors (структурная валидация)
    6. Self-heal (детерминированный) → retry loop
    [if self-heal fails → immediate Refetch]

1) RELATIONAL compile stage: selectors → bound query (bindings)
    7. Compile / Bind (семантика на схеме)
    8. Semantic validate (после биндинга)
    [if errors → Self-heal → re-run segment]

2) RELATIONAL aggregation closure: bound query → final query
    9. Aggregation normalize (семантическая, после binding)
    10. Validate aggregation semantics
    [if errors → Self-heal → re-run segment]

3) POLICY & EXECUTION stage: final query → results
    11. Finalize policy (limit clamp, defaults)
    12. Lowering (to SQL/pandas/ops)
    13. Execute
    14. Postprocess

Critical fixes in v4:
1. No infinite loop: failed self-heal → immediate refetch (not lost flag)
2. _rerun_segment skips parse/extract (preserves healed selectors)
3. Single-provider selectors after validate (active_provider)
4. Refetch loop detection only by response hash (not prompt)
"""

from .base import NodeContext, NodeResult
from .llm_plan_node import LLMPlanNode, LLMPlanResult
from .parse_node import ParseNode, ParseResult
from .plan_normalize_node import PlanNormalizeNode, PlanNormalizeResult
from .provider_normalize_node import ProviderNormalizeNode, ProviderNormalizeResult, ProviderNormalizationRule
from .validate_selectors_node import ValidateSelectorsNode, ValidateSelectorsResult, ValidationRule
from .self_heal_node import SelfHealNode, SelfHealResult, RepairRule
from .self_heal_node import (
    make_missing_value_repair_rule,
    make_ambiguous_field_repair_rule,
    make_unknown_field_repair_rule,
    make_missing_relation_repair_rule,
    make_aggregation_normalize_repair_rule,
)
from .refetch_node import RefetchNode, RefetchRequest, RefetchResult
from .compile_bind_node import CompileBindNode, CompileBindResult, BoundRelationalQuery, BoundField
from .semantic_validate_node import SemanticValidateNode, SemanticValidateResult
from .aggregation_normalize_node import (
    AggregationNormalizeNode,
    AggregationNormalizeResult,
    NormalizedAggregation,
)
from .validate_aggregation_node import ValidateAggregationNode, ValidateAggregationResult
from .finalize_policy_node import FinalizePolicyNode, FinalizePolicyResult, PolicyConfig
from .lowering_node import LoweringNode, LoweringResult, LoweredQuery
from .execute_node import ExecuteNode, ExecutionResult
from .postprocess_node import PostprocessNode, PostprocessResult
from .pipeline import PlanningPipeline, PipelineConfig, PipelineResult

__all__ = [
    # Base types
    "NodeContext",
    "NodeResult",
    
    # Stage 0: LLM
    "LLMPlanNode",
    "LLMPlanResult",
    
    # Stage 1: Parse
    "ParseNode",
    "ParseResult",
    
    # Stage 2: Plan-level normalize
    "PlanNormalizeNode",
    "PlanNormalizeResult",
    
    # Stage 3: Provider-specific normalize
    "ProviderNormalizeNode",
    "ProviderNormalizeResult",
    "ProviderNormalizationRule",
    
    # Stage 4: Validate selectors
    "ValidateSelectorsNode",
    "ValidateSelectorsResult",
    "ValidationRule",
    
    # Stage 5: Self-heal (universal)
    "SelfHealNode",
    "SelfHealResult",
    "RepairRule",
    
    # Built-in repair rules
    "make_missing_value_repair_rule",
    "make_ambiguous_field_repair_rule",
    "make_unknown_field_repair_rule",
    "make_missing_relation_repair_rule",
    "make_aggregation_normalize_repair_rule",

    # Stage 5b: Refetch (LLM retry)
    "RefetchNode",
    "RefetchRequest",
    "RefetchResult",
    
    # Stage 6: Compile/Bind
    "CompileBindNode",
    "CompileBindResult",
    "BoundRelationalQuery",
    "BoundField",
    
    # Stage 7: Semantic validate
    "SemanticValidateNode",
    "SemanticValidateResult",
    
    # Stage 8: Aggregation normalize
    "AggregationNormalizeNode",
    "AggregationNormalizeResult",
    "NormalizedAggregation",
    
    # Stage 9: Validate aggregation
    "ValidateAggregationNode",
    "ValidateAggregationResult",
    
    # Stage 10: Finalize policy
    "FinalizePolicyNode",
    "FinalizePolicyResult",
    "PolicyConfig",
    
    # Stage 11: Lowering
    "LoweringNode",
    "LoweringResult",
    "LoweredQuery",
    
    # Stage 12: Execute
    "ExecuteNode",
    "ExecutionResult",
    
    # Stage 13: Postprocess
    "PostprocessNode",
    "PostprocessResult",
    
    # Pipeline orchestrator
    "PlanningPipeline",
    "PipelineConfig",
    "PipelineResult",
]
