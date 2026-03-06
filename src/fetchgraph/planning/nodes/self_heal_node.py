"""Universal Self-Heal Node (pre- and post-binding).

This node attempts deterministic repair of errors from ANY stage:
- Pre-binding: JSON/schema errors, selector format issues, select * expand
- Post-binding: unknown_field, ambiguous_field, wrong entity, missing relations

The pipeline decides which segment to re-run after repair.
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Literal, Optional

from .base import NodeContext, NodeResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RepairRule:
    """A deterministic repair rule."""
    
    # Unique identifier for this rule
    rule_id: str
    
    # Which error types this rule can fix
    error_patterns: List[str]  # e.g. ["unknown_field", "ambiguous_field", "missing_value"]
    
    # Which stages this rule applies to
    stages: List[str]  # e.g. ["validate_selectors", "compile_bind", "semantic_validate"]
    
    # The repair function: (selectors, error_context) → repaired_selectors
    repair_fn: Callable[[Any, Dict[str, Any]], Any]
    
    # Human-readable description
    description: str = ""
    
    # Is this rule safe to apply automatically?
    is_safe: bool = True


@dataclass(frozen=True)
class SelfHealResult:
    """Result from self-healing."""
    
    # Repaired selectors (may be same as input if no repair applied)
    repaired_selectors: Optional[Dict[str, Any]]
    
    # Which rules were applied
    rules_applied: List[str]
    
    # What errors were fixed
    errors_fixed: List[str]
    
    # What couldn't be fixed
    unfixable_errors: List[str]
    
    # Which pipeline segment should be re-run
    re_run_segment: Optional[str] = None  # e.g. "provider_normalize", "compile_bind"
    
    # Notes
    notes: List[str] = field(default_factory=list)
    
    @property
    def is_success(self) -> bool:
        return len(self.unfixable_errors) == 0 and self.repaired_selectors is not None
    
    @property
    def needs_refetch(self) -> bool:
        """Signal that self-heal failed and we need LLM refetch."""
        return len(self.unfixable_errors) > 0


class SelfHealNode:
    """Universal Self-Heal Node (pre- and post-binding).
    
    Responsibilities:
    - Accept errors from ANY stage (validate, bind, semantic, agg)
    - Apply deterministic repair rules
    - Signal which pipeline segment to re-run
    - Signal when refetch is needed (unfixable errors)
    
    Repair categories:
    
    Pre-binding (shape/schema):
    - missing_required_field → add with default/null
    - invalid_type → coerce or remove
    - select_star → expand by schema
    - filter_syntax → normalize to canonical AST
    
    Post-binding (semantic):
    - unknown_field → suggest similar fields or remove
    - ambiguous_field → qualify with entity prefix
    - wrong_entity → map to correct entity if obvious
    - missing_relation → add if unique path exists
    - aggregation_mismatch → normalize agg function
    """
    
    def __init__(
        self,
        rules: Optional[List[RepairRule]] = None,
        max_heal_attempts: int = 3,
    ) -> None:
        self.rules = {r.rule_id: r for r in (rules or [])}
        self.max_heal_attempts = max_heal_attempts
        self._attempt_count = 0
    
    def add_rule(self, rule: RepairRule) -> None:
        """Add a repair rule."""
        self.rules[rule.rule_id] = rule
    
    def execute(
        self,
        ctx: NodeContext,
        selectors: Dict[str, Any],
        error_payload: Dict[str, Any],
    ) -> NodeResult[SelfHealResult]:
        """Execute self-healing.
        
        Args:
            ctx: Pipeline context
            selectors: Current selectors (may be invalid)
            error_payload: Error context from failing stage
                {
                    "stage": "validate_selectors" | "compile_bind" | ...,
                    "errors": [...],
                    "error_types": ["unknown_field", ...],
                    "selectors": {...},
                }
        
        Returns:
            NodeResult with repair result and re-run signal
        """
        logger.debug("SelfHealNode: executing (universal)")
        
        self._attempt_count += 1
        if self._attempt_count > self.max_heal_attempts:
            return NodeResult(
                value=SelfHealResult(
                    repaired_selectors=None,
                    rules_applied=[],
                    errors_fixed=[],
                    unfixable_errors=[f"Max heal attempts ({self.max_heal_attempts}) exceeded"],
                    notes=["SelfHealNode: max attempts exceeded"],
                ),
                notes=["SelfHealNode: max attempts exceeded"],
            )
        
        # Extract error types from payload
        error_types = error_payload.get("error_types", [])
        stage = error_payload.get("stage", "unknown")
        
        # Find applicable rules
        applicable_rules = [
            rule for rule in self.rules.values()
            if any(pattern in error_types for pattern in rule.error_patterns)
            and (stage in rule.stages or "*" in rule.stages)
        ]
        
        if not applicable_rules:
            return NodeResult(
                value=SelfHealResult(
                    repaired_selectors=None,
                    rules_applied=[],
                    errors_fixed=[],
                    unfixable_errors=error_types,
                    notes=[f"SelfHealNode: no rules for errors {error_types} at stage {stage}"],
                ),
                notes=["SelfHealNode: no applicable rules"],
            )
        
        # Apply rules
        repaired = copy.deepcopy(selectors)
        rules_applied: List[str] = []
        errors_fixed: List[str] = []
        unfixable: List[str] = []
        
        for rule in applicable_rules:
            try:
                repaired = rule.repair_fn(repaired, error_payload)
                rules_applied.append(rule.rule_id)
                errors_fixed.extend([
                    pattern for pattern in rule.error_patterns
                    if pattern in error_types
                ])
            except Exception as e:
                logger.warning(f"SelfHealNode: rule {rule.rule_id} failed: {e}")
                unfixable.extend(rule.error_patterns)
        
        # Determine which segment to re-run
        re_run_segment = self._determine_rerun_segment(stage, rules_applied)
        
        notes = [
            f"SelfHealNode: applied rules {rules_applied}",
            f"SelfHealNode: re-run segment: {re_run_segment}",
        ]
        
        result = SelfHealResult(
            repaired_selectors=repaired,
            rules_applied=rules_applied,
            errors_fixed=list(set(errors_fixed)),
            unfixable_errors=list(set(unfixable)),
            re_run_segment=re_run_segment,
            notes=notes,
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
    
    def _determine_rerun_segment(
        self,
        original_stage: str,
        rules_applied: List[str],
    ) -> Optional[str]:
        """Determine which pipeline segment should be re-run after repair.
        
        Returns:
            Segment name or None if no re-run needed
        """
        # Pre-binding repairs → re-run from provider_normalize
        if original_stage in ["validate_selectors", "provider_normalize"]:
            return "provider_normalize"
        
        # Binding repairs → re-run from compile_bind
        if original_stage in ["compile_bind", "semantic_validate"]:
            return "compile_bind"
        
        # Aggregation repairs → re-run from aggregation_normalize
        if original_stage in ["aggregation_normalize", "validate_aggregation"]:
            return "aggregation_normalize"
        
        # Default: re-run from provider_normalize
        return "provider_normalize"
    
    def reset(self) -> None:
        """Reset attempt counter (call between pipeline runs)."""
        self._attempt_count = 0


# ============================================================================
# Built-in repair rules (examples)
# ============================================================================

def make_missing_value_repair_rule() -> RepairRule:
    """Repair rule: add missing required fields with null/default values."""
    
    def repair_fn(selectors: Any, error_ctx: Dict[str, Any]) -> Any:
        if not isinstance(selectors, dict):
            return selectors
        
        repaired = dict(selectors)
        
        # Fix is_not_null / is_null filters missing value
        filters = repaired.get("filters")
        if isinstance(filters, dict):
            if filters.get("op") in ["is_not_null", "is_null"]:
                if "value" not in filters:
                    repaired["filters"] = {**filters, "value": None}
        
        return repaired
    
    return RepairRule(
        rule_id="add_missing_value",
        error_patterns=["missing_value", "field_required"],
        stages=["validate_selectors", "*"],
        repair_fn=repair_fn,
        description="Add missing required fields (e.g., value: null for is_not_null)",
        is_safe=True,
    )


def make_ambiguous_field_repair_rule() -> RepairRule:
    """Repair rule: qualify ambiguous bare fields with entity prefix."""
    
    def repair_fn(selectors: Any, error_ctx: Dict[str, Any]) -> Any:
        # Stub: would need schema context to implement properly
        return selectors
    
    return RepairRule(
        rule_id="qualify_ambiguous_field",
        error_patterns=["ambiguous_field"],
        stages=["compile_bind", "semantic_validate"],
        repair_fn=repair_fn,
        description="Qualify ambiguous bare fields with entity prefix",
        is_safe=True,
    )


def make_unknown_field_repair_rule() -> RepairRule:
    """Repair rule: suggest/remove unknown fields."""
    
    def repair_fn(selectors: Any, error_ctx: Dict[str, Any]) -> Any:
        # Stub: would need schema context to implement properly
        return selectors
    
    return RepairRule(
        rule_id="remove_unknown_field",
        error_patterns=["unknown_field", "field_not_found"],
        stages=["compile_bind", "semantic_validate"],
        repair_fn=repair_fn,
        description="Remove or suggest replacement for unknown fields",
        is_safe=False,  # May change semantics
    )


def make_missing_relation_repair_rule() -> RepairRule:
    """Repair rule: add missing relation if unique path exists."""
    
    def repair_fn(selectors: Any, error_ctx: Dict[str, Any]) -> Any:
        # Stub: would need schema context to implement properly
        return selectors
    
    return RepairRule(
        rule_id="add_missing_relation",
        error_patterns=["missing_relation", "relation_not_found"],
        stages=["compile_bind", "semantic_validate"],
        repair_fn=repair_fn,
        description="Add missing relation if unique path exists",
        is_safe=False,
    )


def make_aggregation_normalize_repair_rule() -> RepairRule:
    """Repair rule: normalize aggregation function names."""
    
    def repair_fn(selectors: Any, error_ctx: Dict[str, Any]) -> Any:
        if not isinstance(selectors, dict):
            return selectors
        
        repaired = dict(selectors)
        aggregations = repaired.get("aggregations", [])
        
        if isinstance(aggregations, list):
            normalized = []
            for agg in aggregations:
                if isinstance(agg, dict):
                    agg_fn = agg.get("agg", "")
                    # Normalize common variants
                    if agg_fn.upper() in ["COUNT", "COUNT(*)"]:
                        agg["agg"] = "count"
                    elif agg_fn.upper() == "SUM":
                        agg["agg"] = "sum"
                    elif agg_fn.upper() == "AVG":
                        agg["agg"] = "avg"
                    elif agg_fn.upper() == "MIN":
                        agg["agg"] = "min"
                    elif agg_fn.upper() == "MAX":
                        agg["agg"] = "max"
                    elif "DISTINCT" in agg_fn.upper():
                        agg["agg"] = "count_distinct"
                    normalized.append(agg)
            repaired["aggregations"] = normalized
        
        return repaired
    
    return RepairRule(
        rule_id="normalize_aggregation",
        error_patterns=["invalid_aggregation", "unknown_aggregation"],
        stages=["aggregation_normalize", "validate_aggregation"],
        repair_fn=repair_fn,
        description="Normalize aggregation function names to canonical form",
        is_safe=True,
    )
