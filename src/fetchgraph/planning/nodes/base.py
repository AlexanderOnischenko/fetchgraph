"""Base types for planning pipeline nodes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Optional, TypeVar

from pydantic import BaseModel


@dataclass(frozen=True)
class NodeContext:
    """Context passed through the pipeline."""
    
    # Input from LLM
    raw_plan: Optional[str] = None
    
    # Parsed plan
    parsed_plan: Optional[BaseModel] = None
    
    # Normalized plan
    normalized_plan: Optional[BaseModel] = None
    
    # Provider-specific normalized selectors
    provider_normalized: Optional[Dict[str, Any]] = None
    
    # Validated selectors
    validated_selectors: Optional[Dict[str, Any]] = None
    
    # Bound query (after compile/bind)
    bound_query: Optional[Any] = None
    
    # Final query with aggregation closure
    final_query: Optional[Any] = None
    
    # Error handling
    errors: List[str] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3
    
    # Metadata
    notes: List[str] = field(default_factory=list)
    diag: Dict[str, Any] = field(default_factory=dict)


T = TypeVar('T')


@dataclass(frozen=True)
class NodeResult(Generic[T]):
    """Result from a pipeline node."""
    
    # Success case
    value: Optional[T] = None
    
    # Error case
    error: Optional[str] = None
    errors: List[str] = field(default_factory=list)
    
    # Metadata for observability
    notes: List[str] = field(default_factory=list)
    diag: Dict[str, Any] = field(default_factory=dict)
    
    # Control flow
    should_retry: bool = False
    retry_reason: Optional[str] = None
    
    @property
    def is_success(self) -> bool:
        return self.error is None and len(self.errors) == 0
    
    @property
    def is_error(self) -> bool:
        return not self.is_success
