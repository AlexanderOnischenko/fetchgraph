"""Stage 8: Aggregation normalize (semantic, post-binding).

This node normalizes aggregations after binding:
- Normalize aggregation names (count distinct, COUNT(DISTINCT x), etc.)
- group_by closure: all non-agg select fields must be in group_by
- min/max "as filter" → rewrite to aggregation + projection
- Alias uniqueness and stability

Currently a stub - passes through unchanged.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from .base import NodeContext, NodeResult
from .compile_bind_node import BoundRelationalQuery

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NormalizedAggregation:
    """A normalized aggregation specification."""
    
    # Aggregation function name (canonical)
    agg: str  # count, sum, avg, min, max, count_distinct, etc.
    
    # Field to aggregate
    field: str
    
    # Output alias
    alias: str
    
    # Distinct flag
    is_distinct: bool = False


@dataclass(frozen=True)
class AggregationNormalizeResult:
    """Result from aggregation normalization."""

    # Normalized aggregations
    normalized_aggregations: List[NormalizedAggregation]

    # Normalized group_by fields
    normalized_group_by: List[str]

    # Notes about transformations
    notes: List[str]

    # Changes made
    changes: Dict[str, Any] = field(default_factory=dict)
    
    # Normalized selectors (with op normalized, etc.)
    normalized_selectors: Dict[str, Any] = field(default_factory=dict)


class AggregationNormalizeNode:
    """Node 8: Aggregation normalize (semantic, post-binding).
    
    Responsibilities:
    - Normalize aggregation names to canonical form
    - group_by closure: ensure all non-agg select fields in group_by
    - min/max "as filter" → rewrite to aggregation + projection
    - Alias uniqueness and stability
    
    Currently a stub that passes through unchanged.
    """
    
    def __init__(
        self,
        canonical_agg_names: Optional[Dict[str, str]] = None,
    ) -> None:
        # Map various names to canonical form
        self.canonical_agg_names = canonical_agg_names or {
            # Count variants
            "count": "count",
            "COUNT": "count",
            "количество": "count",
            # Sum variants
            "sum": "sum",
            "SUM": "sum",
            "сумма": "sum",
            # Avg variants
            "avg": "avg",
            "average": "avg",
            "AVG": "avg",
            "среднее": "avg",
            # Min/Max variants
            "min": "min",
            "MIN": "min",
            "минимум": "min",
            "max": "max",
            "MAX": "max",
            "максимум": "max",
            # Count distinct variants
            "count_distinct": "count_distinct",
            "COUNT DISTINCT": "count_distinct",
            "COUNT(DISTINCT": "count_distinct",
            "количество разных": "count_distinct",
            "unique": "count_distinct",
        }
    
    def execute(
        self,
        ctx: NodeContext,
        selectors: Dict[str, Any],
    ) -> NodeResult[AggregationNormalizeResult]:
        """Execute aggregation normalization.

        Args:
            ctx: Pipeline context
            selectors: Provider-normalized selectors

        Returns:
            NodeResult with normalized aggregations and selectors
        """
        logger.debug("AggregationNormalizeNode: executing")

        notes = []
        normalized_aggregations = []
        normalized_group_by = []
        
        # Make a copy to avoid mutating input
        normalized_selectors = dict(selectors)

        # Normalize op: "aggregate" → "query" (LLM sometimes produces "aggregate" but schema only has "query")
        if normalized_selectors.get("op") == "aggregate":
            normalized_selectors["op"] = "query"
            notes.append("AggregationNormalizeNode: normalized op='aggregate' to op='query'")
        
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
                normalized_selectors["root_entity"] = inferred_entity
                notes.append(f"AggregationNormalizeNode: inferred root_entity='{inferred_entity}' from field names")

        # Extract aggregations from selectors
        aggregations = normalized_selectors.get("aggregations", [])
        if isinstance(aggregations, list):
            for agg in aggregations:
                if isinstance(agg, dict):
                    agg_func = agg.get("agg", "")
                    field_name = agg.get("field", "")
                    alias = agg.get("alias", None)
                    is_distinct = agg.get("is_distinct", False)

                    # Normalize aggregation function name
                    canonical_agg = self.normalize_agg_name(agg_func)
                    if is_distinct and canonical_agg == "count":
                        canonical_agg = "count_distinct"

                    # Generate alias if not provided
                    if not alias:
                        alias = f"{canonical_agg}_{field_name.replace('.', '_')}"

                    normalized_aggregations.append(
                        NormalizedAggregation(
                            agg=canonical_agg,
                            field=field_name,
                            alias=alias,
                            is_distinct=is_distinct,
                        )
                    )
                    notes.append(f"Normalized aggregation: {agg_func}({field_name}) -> {canonical_agg}({field_name}) as {alias}")

        # Extract group_by from selectors
        group_by = normalized_selectors.get("group_by", [])
        if isinstance(group_by, list):
            normalized_group_by = list(group_by)

        # Compute group_by closure (ensure all non-agg select fields are in group_by)
        select_fields = normalized_selectors.get("select", [])
        if isinstance(select_fields, list) and select_fields:
            # TODO: implement full group_by closure logic
            pass

        if not normalized_aggregations:
            notes.append("AggregationNormalizeNode: no aggregations found in selectors")
        else:
            notes.append(f"AggregationNormalizeNode: normalized {len(normalized_aggregations)} aggregation(s)")

        result = AggregationNormalizeResult(
            normalized_aggregations=normalized_aggregations,
            normalized_group_by=normalized_group_by,
            notes=notes,
            changes={
                "input_agg_count": len(aggregations) if isinstance(aggregations, list) else 0,
                "output_agg_count": len(normalized_aggregations),
                "op_normalized": selectors.get("op") != normalized_selectors.get("op"),
                "root_entity_inferred": "root_entity" not in selectors and "root_entity" in normalized_selectors,
            },
            normalized_selectors=normalized_selectors,
        )

        return NodeResult(
            value=result,
            notes=notes,
        )
    
    def normalize_agg_name(self, agg_name: str) -> str:
        """Normalize aggregation name to canonical form."""
        # Clean up common patterns
        cleaned = agg_name.strip()
        cleaned = re.sub(r'\s*\(\s*', '(', cleaned)
        cleaned = re.sub(r'\s*\)', ')', cleaned)
        
        # Check for distinct pattern
        distinct_match = re.match(r'COUNT\s*\(\s*DISTINCT', cleaned, re.IGNORECASE)
        if distinct_match:
            return "count_distinct"
        
        # Look up in canonical map
        for pattern, canonical in self.canonical_agg_names.items():
            if pattern.upper() in cleaned.upper():
                return canonical
        
        # Return lowercase by default
        return cleaned.lower()
    
    def extract_agg_from_expr(self, expr: str) -> Optional[NormalizedAggregation]:
        """Extract aggregation from expression string.
        
        Examples:
        - "COUNT(*)" → NormalizedAggregation(agg="count", field="*", alias="count")
        - "SUM(line_total)" → NormalizedAggregation(agg="sum", field="line_total", alias="sum_line_total")
        - "COUNT(DISTINCT customer_id)" → NormalizedAggregation(agg="count_distinct", field="customer_id", ...)
        """
        # Pattern: AGG(field) or AGG(DISTINCT field)
        agg_pattern = r'(\w+)\s*\(\s*(?:DISTINCT\s+)?(\*|\w+(?:\.\w+)?)\s*\)'
        match = re.match(agg_pattern, expr, re.IGNORECASE)
        
        if not match:
            return None
        
        agg_name = match.group(1)
        field_name = match.group(2)
        
        # Check for distinct
        is_distinct = 'DISTINCT' in expr.upper()
        
        canonical_agg = self.normalize_agg_name(agg_name)
        if is_distinct and canonical_agg == "count":
            canonical_agg = "count_distinct"
        
        # Generate alias
        alias = f"{canonical_agg}_{field_name.replace('.', '_')}"
        
        return NormalizedAggregation(
            agg=canonical_agg,
            field=field_name,
            alias=alias,
            is_distinct=is_distinct,
        )
    
    def compute_group_by_closure(
        self,
        select_fields: List[str],
        aggregation_fields: List[str],
        existing_group_by: List[str],
    ) -> List[str]:
        """Compute required group_by fields.
        
        All non-aggregation select fields must be in group_by.
        """
        # TODO: implement
        return existing_group_by
