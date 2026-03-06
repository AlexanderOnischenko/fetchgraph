"""Stage 6: Compile/Bind (semantic binding to schema).

This node binds selectors to the relational schema:
- Check root_entity exists
- Normalize/resolve relations (names, join-path, uniqueness)
- Bind fields:
  - select.expr, filters.field, group_by.field, aggregations.field
  - Qualify bare fields (add entity prefix) or fail as ambiguous
  - (Optional) select * → expand by schema

Result: BoundRelationalQuery (internal canonical form)

Currently a stub - passes through unchanged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from .base import NodeContext, NodeResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BoundField:
    """A field bound to a specific entity/column."""
    
    entity: str
    column: str
    original_expr: str
    qualified_expr: str


@dataclass(frozen=True)
class BoundRelationalQuery:
    """A relational query with all fields bound to schema."""
    
    # Original selectors
    selectors: Dict[str, Any]
    
    # Bound fields
    bound_select: List[BoundField]
    bound_filters: List[BoundField]
    bound_group_by: List[BoundField]
    bound_aggregations: List[BoundField]
    
    # Resolved relations
    resolved_relations: List[str]
    
    # Root entity
    root_entity: str


@dataclass(frozen=True)
class CompileBindResult:
    """Result from compile/bind."""
    
    # Bound query
    bound_query: Optional[BoundRelationalQuery]
    
    # Binding errors
    errors: List[str]
    
    # Notes
    notes: List[str]


class CompileBindNode:
    """Node 6: Compile/Bind (semantic binding to schema).
    
    Responsibilities:
    - Check root_entity exists in schema
    - Normalize/resolve relations (names, join-path, uniqueness)
    - Bind fields to schema columns
    - Qualify bare fields or fail as ambiguous
    - (Optional) Expand select *
    
    Currently a stub that passes through unchanged.
    """
    
    def __init__(
        self,
        schema: Optional[Dict[str, Any]] = None,
        entities: Optional[Set[str]] = None,
        relations: Optional[List[str]] = None,
    ) -> None:
        self.schema = schema or {}
        self.entities = entities or set()
        self.relations = relations or []
    
    def execute(
        self,
        ctx: NodeContext,
        selectors: Dict[str, Any],
    ) -> NodeResult[CompileBindResult]:
        """Execute compile/bind.
        
        Args:
            ctx: Pipeline context
            selectors: Provider-normalized selectors
        
        Returns:
            NodeResult with bound query
        """
        logger.debug("CompileBindNode: executing (stub)")
        
        # Stub: pass through unchanged
        notes = [
            "CompileBindNode: compile/bind (stub - no binding performed)",
        ]
        
        result = CompileBindResult(
            bound_query=None,  # Stub: no binding
            errors=[],
            notes=notes,
        )
        
        return NodeResult(
            value=result,
            notes=notes,
        )
    
    def _check_root_entity(self, root_entity: str) -> Optional[str]:
        """Check that root_entity exists in schema.
        
        Returns error message if not found, None if OK.
        """
        # TODO: implement
        return None
    
    def _resolve_relations(
        self,
        relations: List[str],
        root_entity: str,
    ) -> tuple[List[str], List[str]]:
        """Resolve relation names to join paths.
        
        Returns:
            (resolved_relations, errors)
        """
        # TODO: implement
        return relations, []
    
    def _bind_fields(
        self,
        fields: List[str],
        root_entity: str,
        context: str,  # "select", "filter", etc.
    ) -> tuple[List[BoundField], List[str]]:
        """Bind fields to schema columns.
        
        Returns:
            (bound_fields, errors)
        """
        # TODO: implement
        return [], []
