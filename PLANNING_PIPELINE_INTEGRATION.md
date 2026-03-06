# Integration Guide: Planning Pipeline Nodes

This document explains how to integrate the new node-based planning pipeline into your existing fetchgraph workflow.

## Quick Start

### Автоматически через create_generic_agent

Начиная с версии v4, новый pipeline используется **по умолчанию** при создании агента:

```python
from fetchgraph.core import create_generic_agent
from fetchgraph.core.models import TaskProfile

agent = create_generic_agent(
    llm_invoke=llm,
    providers=providers,
    saver=saver,
    task_profile=TaskProfile(task_name="my_task"),
    # use_pipeline_normalizer=True по умолчанию
)

result = agent.run("my_feature_name")
```

Для отката к legacy PlanNormalizer:

```python
agent = create_generic_agent(
    llm_invoke=llm,
    providers=providers,
    saver=saver,
    task_profile=TaskProfile(task_name="my_task"),
    use_pipeline_normalizer=False,  # ← legacy PlanNormalizer
)
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│ BaseGraphAgent.run(feature_name)                            │
│                                                             │
│   _plan(feature_name)                                       │
│     └─► llm_plan() → raw_text                               │
│     └─► plan_parser(raw_text) → Plan                        │
│     └─► plan_normalizer.normalize(Plan) ← NEW PIPELINE      │
│           │                                                 │
│           ├─► ParseNode (raw → Plan)                        │
│           ├─► PlanNormalizeNode                             │
│           ├─► ProviderNormalizeNode                         │
│           ├─► ValidateSelectorsNode                         │
│           ├─► SelfHealNode (retry loop)                     │
│           ├─► RefetchNode (LLM retry)                       │
│           ├─► CompileBindNode                               │
│           ├─► SemanticValidateNode                          │
│           ├─► AggregationNormalizeNode                      │
│           ├─► ValidateAggregationNode                       │
│           ├─► FinalizePolicyNode                            │
│           ├─► LoweringNode                                  │
│           ├─► ExecuteNode                                   │
│           └─► PostprocessNode                               │
│                                                             │
│   _fetch(feature_name, plan)                                │
│   _synthesize(feature_name, ctx, plan)                      │
│   _verify_and_refine(...)                                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Pipeline Stages

### Stage 0: PLAN (LLM → Plan)

1. **LLMPlanNode**: LLM → raw text
2. **ParseNode**: raw text → Plan model (with JSON extraction from markdown)
3. **PlanNormalizeNode**: Plan-level normalization (dedupe, trim, etc.)
4. **ProviderNormalizeNode**: Provider-specific selector normalization
5. **ValidateSelectorsNode**: Structural validation against schema
6. **SelfHealNode**: Deterministic repair of common errors
7. **RefetchNode**: LLM retry if self-heal fails

### Stage 1: RELATIONAL Compile (selectors → bound query)

7. **CompileBindNode**: Semantic binding to schema
8. **SemanticValidateNode**: Post-binding validation

### Stage 2: RELATIONAL Aggregation Closure

9. **AggregationNormalizeNode**: Aggregation normalization (count_distinct, group_by closure)
10. **ValidateAggregationNode**: Aggregation semantics validation

### Stage 3: POLICY & EXECUTION

11. **FinalizePolicyNode**: Limit clamp, defaults
12. **LoweringNode**: Convert to SQL/pandas/ops
13. **ExecuteNode**: Run on provider
14. **PostprocessNode**: Format results

## Configuration

### PipelineConfig

```python
@dataclass
class PipelineConfig:
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
```

### Custom Rules

You can provide custom rules for provider normalization, validation, and self-heal:

```python
from fetchgraph.planning.normalize.nodes import (
    ProviderNormalizationRule,
    ValidationRule,
    RepairRule,
)

provider_rules = [
    ProviderNormalizationRule(
        provider="relational",
        kind="relational_v1",
        validator=TypeAdapter(RelationalRequest),
        normalize_selectors=normalize_relational_selectors,
    ),
]

validation_rules = [
    ValidationRule(
        provider="relational",
        validator=TypeAdapter(RelationalRequest),
        kind="relational_v1",
    ),
]

repair_rules = [
    RepairRule(
        rule_id="add_missing_value",
        error_patterns=["missing_value", "field_required"],
        stages=["validate_selectors", "*"],
        repair_fn=my_repair_function,
        description="Add missing required fields",
        is_safe=True,
    ),
]

normalizer = create_pipeline_normalizer(
    providers=providers,
    provider_rules=provider_rules,
    validation_rules=validation_rules,
    repair_rules=repair_rules,
)
```

## Migration from Legacy PlanNormalizer

### Автоматическая миграция (по умолчанию)

Начиная с v4, `create_generic_agent()` автоматически использует новый pipeline:

```python
# Старый код (legacy)
agent = create_generic_agent(
    llm_invoke=llm,
    providers=providers,
    saver=saver,
    task_profile=task_profile,
)

# Новый код (v4+) — то же самое, но с pipeline
agent = create_generic_agent(
    llm_invoke=llm,
    providers=providers,
    saver=saver,
    task_profile=task_profile,
    # use_pipeline_normalizer=True по умолчанию
)
```

### Ручная настройка pipeline

Для кастомизации используйте `create_pipeline_normalizer`:

```python
from fetchgraph.planning.normalize import create_pipeline_normalizer
from fetchgraph.planning.normalize.nodes import PipelineConfig

normalizer = create_pipeline_normalizer(
    providers=providers,
    plan_model=Plan,
    schema=schema,
    llm_fn=llm_invoke,
    config=PipelineConfig(
        active_provider="relational",
        max_heal_attempts=3,
        max_refetch_attempts=3,
        max_limit=5000,
    ),
)

agent = BaseGraphAgent(
    ...,
    plan_normalizer=normalizer,
)
```

### Откат к legacy

```python
agent = create_generic_agent(
    ...,
    use_pipeline_normalizer=False,  # ← legacy PlanNormalizer
)
```

## Critical Fixes in v4

The new pipeline includes these critical fixes over previous versions:

1. **No infinite loop**: Failed self-heal → immediate refetch (not lost flag)
2. **_rerun_segment preserves healed selectors**: Skips parse/extract after self-heal
3. **Single-provider selectors after validate**: Uses `active_provider` config
4. **Refetch loop detection by response hash**: Not by prompt (prevents false positives)

## Troubleshooting

### Pipeline fails with "parse_failed"

Check that your LLM output contains valid JSON. The pipeline uses `JsonParser.extract()` which handles markdown code blocks, but the JSON must still be valid.

### Pipeline fails with "validation_error"

Check that your selectors match the provider's JSON schema. The pipeline will attempt self-heal for common errors (missing values, etc.).

### Pipeline enters refetch loop

The pipeline detects loops by hashing LLM responses. If the same response is returned twice, it will fail with "Refetch loop detected". This usually means the LLM is not responding to error feedback.

### Pipeline is slow

The pipeline may be running multiple self-heal or refetch iterations. Check the `result.notes` for details. You can reduce `max_heal_attempts` and `max_refetch_attempts` in `PipelineConfig`.

## Testing

To test the pipeline in isolation:

```python
from fetchgraph.planning.normalize.nodes import PlanningPipeline, PipelineConfig

pipeline = PlanningPipeline(
    config=PipelineConfig(),
    plan_model=Plan,
    providers=providers,
    schema=schema,
    llm_fn=llm_invoke,
)

result = pipeline.execute(raw_plan_text)

if result.is_success:
    print("Pipeline succeeded!")
    print(f"Output: {result.output}")
else:
    print(f"Pipeline failed: {result.error}")
    print(f"Notes: {result.notes}")
```

## See Also

- `src/fetchgraph/planning/normalize/nodes/` - All pipeline nodes
- `src/fetchgraph/planning/normalize/pipeline_adapter.py` - Adapter for BaseGraphAgent
- `tests/test_replay_fixed.py` - Example tests using the pipeline
