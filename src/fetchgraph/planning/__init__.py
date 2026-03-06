"""Planning pipeline with node-based architecture.

This module provides a 14-stage planning pipeline with self-heal and refetch:

0) PLAN stage: LLM → Plan
    1. LLMPlanNode - LLM → raw text
    2. ParseNode - raw text → Plan model
    3. PlanNormalizeNode - plan-level normalization
    4. ProviderNormalizeNode - provider-specific shape normalization
    5. ValidateSelectorsNode - structural validation
    6. SelfHealNode - universal deterministic repair
    6b. RefetchNode - LLM retry when self-heal fails

1) RELATIONAL compile stage: selectors → bound query
    7. CompileBindNode - semantic binding to schema
    8. SemanticValidateNode - post-binding validation

2) RELATIONAL aggregation closure: bound query → final query
    9. AggregationNormalizeNode - aggregation normalization
    10. ValidateAggregationNode - aggregation semantics validation

3) POLICY & EXECUTION stage: final query → results
    11. FinalizePolicyNode - limit clamp, defaults
    12. LoweringNode - to SQL/pandas/ops
    13. ExecuteNode - run on provider
    14. PostprocessNode - format results

Quick start:
    from fetchgraph.planning.adapter import create_pipeline_normalizer
    
    normalizer = create_pipeline_normalizer(
        providers=providers,
        plan_model=Plan,
        schema=schema,
        llm_fn=llm_invoke,
    )
    
    agent = BaseGraphAgent(
        ...,
        plan_normalizer=normalizer,
    )
"""

from .adapter import (
    PipelineNormalizerAdapter,
    create_pipeline_normalizer,
)
from .nodes import (
    # Pipeline orchestrator
    PlanningPipeline,
    PipelineConfig,
    PipelineResult,
    
    # Base types
    NodeContext,
    NodeResult,
    
    # Stage 0: LLM
    LLMPlanNode,
    LLMPlanResult,
    
    # Stage 1: Parse
    ParseNode,
    ParseResult,
    
    # Stage 2: Plan-level normalize
    PlanNormalizeNode,
    PlanNormalizeResult,
    
    # Stage 3: Provider-specific normalize
    ProviderNormalizeNode,
    ProviderNormalizeResult,
    ProviderNormalizationRule,
    
    # Stage 4: Validate selectors
    ValidateSelectorsNode,
    ValidateSelectorsResult,
    ValidationRule,
    
    # Stage 5: Self-heal (universal)
    SelfHealNode,
    SelfHealResult,
    RepairRule,
    
    # Stage 5b: Refetch (LLM retry)
    RefetchNode,
    RefetchRequest,
    RefetchResult,
    
    # Stage 6: Compile/Bind
    CompileBindNode,
    CompileBindResult,
    BoundRelationalQuery,
    BoundField,
    
    # Stage 7: Semantic validate
    SemanticValidateNode,
    SemanticValidateResult,
    
    # Stage 8: Aggregation normalize
    AggregationNormalizeNode,
    AggregationNormalizeResult,
    NormalizedAggregation,
    
    # Stage 9: Validate aggregation
    ValidateAggregationNode,
    ValidateAggregationResult,
    
    # Stage 10: Finalize policy
    FinalizePolicyNode,
    FinalizePolicyResult,
    PolicyConfig,
    
    # Stage 11: Lowering
    LoweringNode,
    LoweringResult,
    LoweredQuery,
    
    # Stage 12: Execute
    ExecuteNode,
    ExecutionResult,
    
    # Stage 13: Postprocess
    PostprocessNode,
    PostprocessResult,
    
    # Built-in repair rules
    make_missing_value_repair_rule,
    make_ambiguous_field_repair_rule,
    make_unknown_field_repair_rule,
    make_missing_relation_repair_rule,
    make_aggregation_normalize_repair_rule,
)

__all__ = [
    # Adapter
    "PipelineNormalizerAdapter",
    "create_pipeline_normalizer",
    
    # Pipeline orchestrator
    "PlanningPipeline",
    "PipelineConfig",
    "PipelineResult",
    
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
    
    # Stage 5: Self-heal
    "SelfHealNode",
    "SelfHealResult",
    "RepairRule",
    
    # Stage 5b: Refetch
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
    
    # Built-in repair rules
    "make_missing_value_repair_rule",
    "make_ambiguous_field_repair_rule",
    "make_unknown_field_repair_rule",
    "make_missing_relation_repair_rule",
    "make_aggregation_normalize_repair_rule",
]
