"""Stage 0: LLM → raw plan text.

This node represents the LLM invocation that produces a raw plan.
Currently a stub - just passes through the raw text.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from .base import NodeContext, NodeResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LLMPlanResult:
    """Result from LLM plan generation."""
    
    raw_text: str
    model: Optional[str] = None
    tokens_used: Optional[int] = None


class LLMPlanNode:
    """Node 0: LLM → raw plan text.
    
    Responsibilities:
    - Invoke LLM with plan prompt
    - Return raw text response
    
    Currently a stub that expects raw_text as input.
    """
    
    def __init__(
        self,
        model: Optional[str] = None,
        max_tokens: Optional[int] = None,
    ) -> None:
        self.model = model
        self.max_tokens = max_tokens
    
    def execute(self, ctx: NodeContext, raw_text: str) -> NodeResult[LLMPlanResult]:
        """Execute LLM plan generation.
        
        Args:
            ctx: Pipeline context
            raw_text: Raw plan text from LLM (stub - would normally call LLM)
        
        Returns:
            NodeResult with raw plan text
        """
        logger.debug("LLMPlanNode: executing (stub)")
        
        result = LLMPlanResult(
            raw_text=raw_text,
            model=self.model,
            tokens_used=self.max_tokens,
        )
        
        return NodeResult(
            value=result,
            notes=["LLMPlanNode: raw plan received (stub)"],
        )
