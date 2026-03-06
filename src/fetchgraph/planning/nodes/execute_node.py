"""Execute Node (run lowered query on provider).

This node executes the lowered query on the target provider:
- SQL → execute on database
- Pandas → apply operations
- API → make request

Returns raw results for post-processing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union

from .base import NodeContext, NodeResult
from .lowering_node import LoweredQuery

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExecutionResult:
    """Raw result from query execution."""
    
    # Raw data from provider
    raw_data: Any
    
    # Provider metadata
    provider: str
    
    # Execution metadata
    rows_returned: Optional[int] = None
    execution_time_ms: Optional[float] = None
    
    # Errors
    errors: List[str] = field(default_factory=list)
    
    # Notes
    notes: List[str] = field(default_factory=list)


class ExecuteNode:
    """Execute Node (run lowered query on provider).
    
    Responsibilities:
    - Execute lowered query on target provider
    - SQL → execute on database
    - Pandas → apply operations
    - API → make request
    
    Returns raw results for post-processing.
    """
    
    def __init__(
        self,
        provider: str | None = None,
        execute_fn: Optional[Callable[[Any], Any]] = None,
    ) -> None:
        self.provider = provider or "unknown"
        self.execute_fn = execute_fn
    
    def execute(
        self,
        ctx: NodeContext,
        query: Union[LoweredQuery, Dict[str, Any], Any],
    ) -> NodeResult[ExecutionResult]:
        """Execute query.
        
        Args:
            ctx: Pipeline context
            query: LoweredQuery or selectors dict (stub mode)
        
        Returns:
            NodeResult with execution result
        """
        logger.debug(f"ExecuteNode: executing on provider {self.provider}")
        
        # Stub: no actual execution
        notes = [
            f"ExecuteNode: execution on {self.provider} (stub - no execution)",
        ]
        
        result = ExecutionResult(
            raw_data=None,  # Stub: no execution
            provider=self.provider,
            rows_returned=0,
            execution_time_ms=0,
            errors=[],
            notes=notes,
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
