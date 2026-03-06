"""Finalize Policy Node (limit clamp, task profile policies).

This node applies final policies AFTER normalization:
- Clamp limits to max allowed
- Apply task profile defaults
- Enforce security/quotas

This is separate from normalization to keep policy logic isolated.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .base import NodeContext, NodeResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PolicyConfig:
    """Policy configuration for finalization."""
    
    # Max limits
    max_limit: int = 1000
    max_offset: int = 10000
    max_aggregations: int = 20
    max_group_by: int = 10
    max_relations: int = 10
    
    # Defaults
    default_limit: int = 100
    default_offset: int = 0
    
    # Flags
    allow_unlimited: bool = False
    require_explicit_limit: bool = False


@dataclass(frozen=True)
class FinalizePolicyResult:
    """Result from policy finalization."""
    
    # Finalized selectors
    finalized_selectors: Dict[str, Any]
    
    # Policies applied
    policies_applied: List[str]
    
    # Warnings (policy violations that were auto-fixed)
    warnings: List[str]
    
    # Notes
    notes: List[str] = field(default_factory=list)


class FinalizePolicyNode:
    """Finalize Policy Node (limit clamp, task profile policies).
    
    Responsibilities:
    - Clamp limits to max allowed
    - Apply task profile defaults
    - Enforce security/quotas
    
    This runs AFTER normalization, BEFORE lowering.
    """
    
    def __init__(
        self,
        policy: Optional[PolicyConfig] = None,
    ) -> None:
        self.policy = policy or PolicyConfig()
    
    def execute(
        self,
        ctx: NodeContext,
        selectors: Dict[str, Any],
    ) -> NodeResult[FinalizePolicyResult]:
        """Execute policy finalization.
        
        Args:
            ctx: Pipeline context
            selectors: Normalized selectors
        
        Returns:
            NodeResult with finalized selectors
        """
        logger.debug("FinalizePolicyNode: executing")
        
        finalized = dict(selectors)
        policies_applied: List[str] = []
        warnings: List[str] = []
        
        # Clamp limit
        limit = finalized.get("limit")
        if limit is None:
            finalized["limit"] = self.policy.default_limit
            policies_applied.append("default_limit")
        elif not isinstance(limit, int) or limit < 0:
            finalized["limit"] = self.policy.default_limit
            policies_applied.append("invalid_limit_defaulted")
            warnings.append(f"Invalid limit {limit} replaced with default {self.policy.default_limit}")
        elif limit > self.policy.max_limit and not self.policy.allow_unlimited:
            finalized["limit"] = self.policy.max_limit
            policies_applied.append("limit_clamped")
            warnings.append(f"Limit {limit} clamped to max {self.policy.max_limit}")
        
        # Clamp offset
        offset = finalized.get("offset", self.policy.default_offset)
        if not isinstance(offset, int) or offset < 0:
            finalized["offset"] = self.policy.default_offset
            policies_applied.append("invalid_offset_defaulted")
        elif offset > self.policy.max_offset:
            finalized["offset"] = self.policy.max_offset
            policies_applied.append("offset_clamped")
            warnings.append(f"Offset {offset} clamped to max {self.policy.max_offset}")
        
        # Clamp aggregations count
        aggregations = finalized.get("aggregations", [])
        if isinstance(aggregations, list) and len(aggregations) > self.policy.max_aggregations:
            finalized["aggregations"] = aggregations[:self.policy.max_aggregations]
            policies_applied.append("aggregations_truncated")
            warnings.append(f"Aggregations truncated to max {self.policy.max_aggregations}")
        
        # Clamp group_by count
        group_by = finalized.get("group_by", [])
        if isinstance(group_by, list) and len(group_by) > self.policy.max_group_by:
            finalized["group_by"] = group_by[:self.policy.max_group_by]
            policies_applied.append("group_by_truncated")
            warnings.append(f"Group by truncated to max {self.policy.max_group_by}")
        
        # Clamp relations count
        relations = finalized.get("relations", [])
        if isinstance(relations, list) and len(relations) > self.policy.max_relations:
            finalized["relations"] = relations[:self.policy.max_relations]
            policies_applied.append("relations_truncated")
            warnings.append(f"Relations truncated to max {self.policy.max_relations}")
        
        notes = [
            f"FinalizePolicyNode: applied policies {policies_applied}",
        ]
        if warnings:
            notes.extend([f"FinalizePolicyNode: warning - {w}" for w in warnings])
        
        result = FinalizePolicyResult(
            finalized_selectors=finalized,
            policies_applied=policies_applied,
            warnings=warnings,
            notes=notes,
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
