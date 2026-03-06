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
            NodeResult with normalized aggregations
        """
        logger.debug("AggregationNormalizeNode: executing (stub)")
        
        # Stub: pass through unchanged
        notes = [
            "AggregationNormalizeNode: aggregation normalization (stub - no changes)",
        ]
        
        result = AggregationNormalizeResult(
            normalized_aggregations=[],
            normalized_group_by=[],
            notes=notes,
            changes={},
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
