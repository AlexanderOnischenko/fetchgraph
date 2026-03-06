"""Stage 4: Validate selectors (structural validation).

This node validates selectors against provider schema:
- TypeAdapter(RelationalRequest) for relational
- JSON Schema validation for others

Currently a stub - always passes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic import TypeAdapter, ValidationError

from .base import NodeContext, NodeResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidateSelectorsResult:
    """Result from selector validation."""
    
    # Validation results per provider
    validation_results: Dict[str, bool]
    
    # Errors per provider
    errors_by_provider: Dict[str, str]
    
    # Notes
    notes: List[str]


@dataclass(frozen=True)
class ValidationRule:
    """Validation rule for a provider."""
    
    provider: str
    validator: TypeAdapter[Any]
    kind: Optional[str] = None


class ValidateSelectorsNode:
    """Node 4: Validate selectors (structural validation).
    
    Responsibilities:
    - Validate selectors against provider schema
    - TypeAdapter(RelationalRequest) for relational
    - JSON Schema validation for others
    
    Currently a stub that always passes.
    """
    
    def __init__(
        self,
        rules: Optional[List[ValidationRule]] = None,
    ) -> None:
        self.rules = {r.provider: r for r in (rules or [])}
    
    def add_rule(self, rule: ValidationRule) -> None:
        """Add a validation rule for a provider."""
        self.rules[rule.provider] = rule
    
    def execute(
        self,
        ctx: NodeContext,
        selectors_by_provider: Dict[str, Any],
    ) -> NodeResult[ValidateSelectorsResult]:
        """Execute selector validation.
        
        Args:
            ctx: Pipeline context
            selectors_by_provider: Selectors dict per provider
        
        Returns:
            NodeResult with validation results
        """
        logger.debug("ValidateSelectorsNode: executing (stub)")
        
        # Stub: always pass
        validation_results = {
            provider: True
            for provider in selectors_by_provider.keys()
        }
        
        notes = [
            "ValidateSelectorsNode: validation (stub - all pass)",
        ]
        
        result = ValidateSelectorsResult(
            validation_results=validation_results,
            errors_by_provider={},
            notes=notes,
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
    
    def validate_provider_selectors(
        self,
        provider: str,
        selectors: Any,
    ) -> tuple[bool, Optional[str]]:
        """Validate selectors for a specific provider.
        
        Returns:
            (is_valid, error_message)
        """
        rule = self.rules.get(provider)
        if rule is None:
            return True, None
        
        try:
            rule.validator.validate_python(selectors)
            return True, None
        except ValidationError as e:
            return False, str(e)
        except Exception as e:
            return False, f"Unexpected error: {e}"
