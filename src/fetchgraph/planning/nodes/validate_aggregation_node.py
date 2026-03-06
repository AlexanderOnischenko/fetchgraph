"""Stage 9: Validate aggregation semantics.

This node validates aggregation semantics:
- No HAVING in filters (if supported) → either forbid or normalize
- No aggregating non-existent/incompatible types (if types available)

Currently a stub - passes through unchanged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .base import NodeContext, NodeResult
from .aggregation_normalize_node import NormalizedAggregation

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidateAggregationResult:
    """Result from aggregation validation."""
    
    # Whether validation passed
    is_valid: bool
    
    # Aggregation errors
    errors: List[str]
    
    # Warnings (non-blocking)
    warnings: List[str]
    
    # Notes
    notes: List[str]


class ValidateAggregationNode:
    """Node 9: Validate aggregation semantics.
    
    Responsibilities:
    - Validate no HAVING in filters (if supported)
    - Validate no aggregating non-existent/incompatible types
    - Validate aggregation/field type compatibility
    
    Currently a stub that passes through unchanged.
    """
    
    def __init__(
        self,
        allow_having: bool = False,
        type_checking_enabled: bool = False,
    ) -> None:
        self.allow_having = allow_having
        self.type_checking_enabled = type_checking_enabled
    
    def execute(
        self,
        ctx: NodeContext,
        aggregations: List[NormalizedAggregation],
        selectors: Dict[str, Any],
    ) -> NodeResult[ValidateAggregationResult]:
        """Execute aggregation validation.
        
        Args:
            ctx: Pipeline context
            aggregations: Normalized aggregations
            selectors: Selectors dict
        
        Returns:
            NodeResult with validation result
        """
        logger.debug("ValidateAggregationNode: executing (stub)")
        
        # Stub: always pass
        notes = [
            "ValidateAggregationNode: aggregation validation (stub - all pass)",
        ]
        
        result = ValidateAggregationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            notes=notes,
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
    
    def _check_having_in_filters(
        self,
        selectors: Dict[str, Any],
    ) -> List[str]:
        """Check for HAVING clause in filters.
        
        HAVING should not be in filters - it's a post-aggregation filter.
        Returns list of errors.
        """
        errors = []
        
        filters = selectors.get("filters")
        if not filters:
            return errors
        
        # TODO: detect aggregation functions in filters
        # If found and not allow_having, add error
        
        return errors
    
    def _check_field_type_compatibility(
        self,
        aggregations: List[NormalizedAggregation],
        schema: Dict[str, Any],
    ) -> List[str]:
        """Check that aggregation functions are compatible with field types.
        
        Examples:
        - SUM(text_field) → error
        - AVG(text_field) → error
        - COUNT(anything) → OK
        
        Returns list of errors.
        """
        errors = []
        
        if not self.type_checking_enabled:
            return errors
        
        # TODO: implement type checking
        
        return errors
