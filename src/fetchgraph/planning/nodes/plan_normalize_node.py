"""Stage 2: Plan-level normalization (provider-agnostic).

This node normalizes the Plan structure:
- required_context / context_plan (case, aliases)
- Deduplication
- Trim text fields
- Fill context_plan from required_context if needed

Currently a stub - passes through unchanged.
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from .base import NodeContext, NodeResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PlanNormalizeResult:
    """Result from plan-level normalization."""
    
    # Normalized plan
    normalized_plan: BaseModel
    
    # Notes about what was changed
    notes: List[str]
    
    # Changes made
    changes: Dict[str, Any]


class PlanNormalizeNode:
    """Node 2: Plan-level normalization (provider-agnostic).
    
    Responsibilities:
    - Normalize required_context (case, aliases, dedupe)
    - Normalize context_plan (dedupe, trim)
    - Fill context_plan from required_context if needed
    - Trim text fields
    
    Currently a stub that passes through unchanged.
    """
    
    def __init__(
        self,
        trim_text_fields: bool = True,
        dedupe_required_context: bool = True,
        dedupe_context_plan: bool = True,
        default_mode: str = "full",
    ) -> None:
        self.trim_text_fields = trim_text_fields
        self.dedupe_required_context = dedupe_required_context
        self.dedupe_context_plan = dedupe_context_plan
        self.default_mode = default_mode
    
    def execute(
        self,
        ctx: NodeContext,
        plan: BaseModel,
    ) -> NodeResult[PlanNormalizeResult]:
        """Execute plan-level normalization.
        
        Args:
            ctx: Pipeline context
            plan: Parsed Plan model
        
        Returns:
            NodeResult with normalized plan
        """
        logger.debug("PlanNormalizeNode: executing (stub)")
        
        # Stub: pass through unchanged
        notes = [
            "PlanNormalizeNode: plan-level normalization (stub - no changes)",
        ]
        
        result = PlanNormalizeResult(
            normalized_plan=plan,
            notes=notes,
            changes={},
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
    
    def _normalize_required_context(
        self,
        required_context: List[str],
        notes: List[str],
    ) -> List[str]:
        """Normalize required_context list.
        
        - Resolve provider aliases
        - Deduplicate
        """
        # TODO: implement
        return required_context
    
    def _normalize_context_plan(
        self,
        context_plan: List[Dict[str, Any]],
        notes: List[str],
    ) -> List[Dict[str, Any]]:
        """Normalize context_plan list.
        
        - Deduplicate specs
        - Trim text fields
        - Fill from required_context if needed
        """
        # TODO: implement
        return context_plan
