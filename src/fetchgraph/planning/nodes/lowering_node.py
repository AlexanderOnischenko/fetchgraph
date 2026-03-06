"""Lowering Node (convert normalized query to provider-specific ops).

This node converts the normalized, policy-finalized query into
provider-specific operations:
- Relational → SQL
- Relational → pandas ops
- Other providers → their native format

This is the boundary between "what" (normalized query) and "how" (execution).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from .base import NodeContext, NodeResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoweredQuery:
    """A lowered query ready for execution."""
    
    # Provider-specific representation
    native_query: Any
    
    # Provider name
    provider: str
    
    # Lowering method used
    method: Literal["sql", "pandas", "api", "custom"]
    
    # Metadata for execution
    execution_hints: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LoweringResult:
    """Result from lowering."""
    
    # Lowered query
    lowered_query: LoweredQuery | None
    
    # Lowering errors
    errors: list[str]
    
    # Notes
    notes: list[str] = field(default_factory=list)


class LoweringNode:
    """Lowering Node (convert normalized query to provider-specific ops).
    
    Responsibilities:
    - Convert normalized query to provider-specific format
    - Relational → SQL
    - Relational → pandas ops
    - Other providers → their native format
    
    This runs AFTER policy finalization, BEFORE execution.
    """
    
    def __init__(
        self,
        provider: str | None = None,
        lowering_fn: Callable | None = None,
    ) -> None:
        self.provider = provider or "unknown"
        self.lowering_fn = lowering_fn
    
    def execute(
        self,
        ctx: NodeContext,
        selectors: dict[str, Any],
    ) -> NodeResult[LoweringResult]:
        """Execute lowering.
        
        Args:
            ctx: Pipeline context
            selectors: Finalized selectors
        
        Returns:
            NodeResult with lowered query
        """
        logger.debug(f"LoweringNode: executing for provider {self.provider}")
        
        # Stub: no actual lowering implemented
        # In real implementation, this would call provider-specific lowering
        
        notes = [
            f"LoweringNode: lowering for {self.provider} (stub - no conversion)",
        ]
        
        result = LoweringResult(
            lowered_query=None,  # Stub: no lowering
            errors=[],
            notes=notes,
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
    
    def lower_to_sql(
        self,
        selectors: dict[str, Any],
        schema: dict[str, Any],
    ) -> tuple[str | None, list[str]]:
        """Lower relational selectors to SQL.
        
        Returns:
            (sql_query, errors)
        """
        # TODO: implement SQL generation
        return None, ["LoweringNode: SQL lowering not implemented"]
    
    def lower_to_pandas(
        self,
        selectors: dict[str, Any],
        frames: dict[str, Any],
    ) -> tuple[Any | None, list[str]]:
        """Lower relational selectors to pandas operations.
        
        Returns:
            (pandas_ops, errors)
        """
        # TODO: implement pandas lowering
        return None, ["LoweringNode: pandas lowering not implemented"]
