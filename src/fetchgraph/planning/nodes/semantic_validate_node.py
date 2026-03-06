"""Stage 7: Semantic validate (post-binding validation).

This node validates semantics after binding:
- "Field not in that entity" is now impossible (already bound)
- Ambiguity/unknown → binding error → may go to refetch with candidates

Currently a stub - passes through unchanged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .base import NodeContext, NodeResult
from .compile_bind_node import BoundRelationalQuery

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SemanticValidateResult:
    """Result from semantic validation."""
    
    # Whether validation passed
    is_valid: bool
    
    # Semantic errors
    errors: List[str]
    
    # Warnings (non-blocking)
    warnings: List[str]
    
    # Notes
    notes: List[str]


class SemanticValidateNode:
    """Node 7: Semantic validate (post-binding validation).
    
    Responsibilities:
    - Validate semantics after binding
    - "Field not in that entity" is now impossible
    - Detect ambiguity/unknown → binding error
    
    Currently a stub that passes through unchanged.
    """
    
    def __init__(self) -> None:
        pass
    
    def execute(
        self,
        ctx: NodeContext,
        bound_query: Optional[BoundRelationalQuery],
    ) -> NodeResult[SemanticValidateResult]:
        """Execute semantic validation.
        
        Args:
            ctx: Pipeline context
            bound_query: Bound relational query (may be None in stub mode)
        
        Returns:
            NodeResult with validation result
        """
        logger.debug("SemanticValidateNode: executing (stub)")
        
        # Handle stub mode (no binding performed)
        if bound_query is None:
            notes = [
                "SemanticValidateNode: skipping (no bound_query - stub mode)",
            ]
            
            result = SemanticValidateResult(
                is_valid=True,
                errors=[],
                warnings=[],
                notes=notes,
            )
            
            return NodeResult(
                value=result,
                notes=notes,
            )
        
        # Stub: always pass for actual bound queries
        notes = [
            "SemanticValidateNode: semantic validation (stub - all pass)",
        ]
        
        result = SemanticValidateResult(
            is_valid=True,
            errors=[],
            warnings=[],
            notes=notes,
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
    
    def _validate_entity_fields(
        self,
        bound_query: BoundRelationalQuery,
    ) -> List[str]:
        """Validate that all bound fields belong to their entities.
        
        After binding, this should always be true.
        Returns list of errors (should be empty).
        """
        # TODO: implement
        return []
    
    def _validate_relation_paths(
        self,
        bound_query: BoundRelationalQuery,
    ) -> List[str]:
        """Validate that relation paths are valid.
        
        Returns list of errors.
        """
        # TODO: implement
        return []
