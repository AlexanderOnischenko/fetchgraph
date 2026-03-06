"""Postprocess Node (format results for output).

This node post-processes raw execution results:
- Format data (JSON, CSV, etc.)
- Apply projections/aliases
- Aggregate/transform if needed
- Prepare for saver/verifier

This is the final step before returning to caller.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from .base import NodeContext, NodeResult
from .execute_node import ExecutionResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PostprocessResult:
    """Final post-processed result."""
    
    # Formatted output
    output: Any
    
    # Output format
    format: Literal["json", "csv", "dict", "custom"]
    
    # Metadata
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    
    # Notes
    notes: List[str] = field(default_factory=list)


class PostprocessNode:
    """Postprocess Node (format results for output).
    
    Responsibilities:
    - Format data (JSON, CSV, etc.)
    - Apply projections/aliases
    - Aggregate/transform if needed
    - Prepare for saver/verifier
    
    This is the final step before returning to caller.
    """
    
    def __init__(
        self,
        output_format: Literal["json", "csv", "dict"] = "json",
        apply_aliases: bool = True,
        trim_nulls: bool = False,
    ) -> None:
        self.output_format = output_format
        self.apply_aliases = apply_aliases
        self.trim_nulls = trim_nulls
    
    def execute(
        self,
        ctx: NodeContext,
        execution_result: ExecutionResult,
    ) -> NodeResult[PostprocessResult]:
        """Execute post-processing.
        
        Args:
            ctx: Pipeline context
            execution_result: Raw execution result
        
        Returns:
            NodeResult with post-processed result
        """
        logger.debug("PostprocessNode: executing")
        
        # Stub: no actual post-processing
        notes = [
            "PostprocessNode: post-processing (stub - no transformation)",
        ]
        
        result = PostprocessResult(
            output=execution_result.raw_data,  # Pass through unchanged
            format=self.output_format,
            row_count=execution_result.rows_returned,
            column_count=None,
            notes=notes,
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
