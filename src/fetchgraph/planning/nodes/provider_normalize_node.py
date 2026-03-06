"""Stage 3: Provider-specific selector normalization (shape normalization).

This node normalizes selectors to provider-specific canonical form:
- For relational: filters to canonical AST, aggregations/group_by to list form, etc.

Currently a stub - passes through unchanged.
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from pydantic import TypeAdapter

from .base import NodeContext, NodeResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProviderNormalizeResult:
    """Result from provider-specific normalization."""
    
    # Normalized selectors per provider
    normalized_selectors: Dict[str, Any]
    
    # Notes about transformations
    notes: List[str]
    
    # Which providers were normalized
    providers_normalized: List[str]


@dataclass(frozen=True)
class ProviderNormalizationRule:
    """Rule for normalizing a provider's selectors."""
    
    provider: str
    kind: str
    validator: TypeAdapter[Any]
    normalize_selectors: Callable[[Any], Any]


class ProviderNormalizeNode:
    """Node 3: Provider-specific selector normalization (shape normalization).
    
    Responsibilities:
    - Apply provider-specific normalization rules
    - For relational: filters to canonical AST, aggregations/group_by to list form
    - For other providers: their specific shape requirements
    
    Currently a stub that passes through unchanged.
    """
    
    def __init__(
        self,
        rules: Optional[List[ProviderNormalizationRule]] = None,
    ) -> None:
        self.rules = {r.provider: r for r in (rules or [])}
    
    def add_rule(self, rule: ProviderNormalizationRule) -> None:
        """Add a normalization rule for a provider."""
        self.rules[rule.provider] = rule
    
    def execute(
        self,
        ctx: NodeContext,
        selectors_by_provider: Dict[str, Any],
    ) -> NodeResult[ProviderNormalizeResult]:
        """Execute provider-specific normalization.
        
        Args:
            ctx: Pipeline context
            selectors_by_provider: Selectors dict per provider
        
        Returns:
            NodeResult with normalized selectors
        """
        logger.debug("ProviderNormalizeNode: executing (stub)")
        
        # Stub: pass through unchanged
        notes = [
            "ProviderNormalizeNode: provider-specific normalization (stub - no changes)",
        ]
        
        result = ProviderNormalizeResult(
            normalized_selectors=selectors_by_provider,
            notes=notes,
            providers_normalized=[],
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
    
    def _normalize_provider_selectors(
        self,
        provider: str,
        selectors: Any,
        notes: List[str],
    ) -> Any:
        """Normalize selectors for a specific provider.
        
        Apply the provider's normalization rule if registered.
        """
        rule = self.rules.get(provider)
        if rule is None:
            notes.append(f"ProviderNormalizeNode: no rule for provider {provider}")
            return selectors
        
        try:
            normalized = rule.normalize_selectors(copy.deepcopy(selectors))
            notes.append(f"ProviderNormalizeNode: normalized {provider} selectors")
            return normalized
        except Exception as e:
            notes.append(f"ProviderNormalizeNode: normalization failed for {provider}: {e}")
            return selectors
