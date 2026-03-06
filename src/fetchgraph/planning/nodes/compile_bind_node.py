"""Stage 6: Compile/Bind (semantic binding to schema).

This node binds selectors to the relational schema:
- Check root_entity exists
- Normalize/resolve relations (names, join-path, uniqueness)
- Bind fields:
  - select.expr, filters.field, group_by.field, aggregations.field
  - Qualify bare fields (add entity prefix) or fail as ambiguous
  - (Optional) select * → expand by schema
- Parse SQL expressions in select and extract aggregates
- Handle function calls like DATE_TRUNC, COUNT, SUM, etc.

Result: BoundRelationalQuery (internal canonical form) with transformed selectors
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

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
class ParsedAggregation:
    """An aggregation extracted from a SQL expression."""

    agg_func: str
    field: str
    alias: str
    is_distinct: bool = False


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
    
    # Transformed selectors (with aggregates extracted from select)
    transformed_selectors: Dict[str, Any] = field(default_factory=dict)


class CompileBindNode:
    """Node 6: Compile/Bind (semantic binding to schema).

    Responsibilities:
    - Check root_entity exists in schema
    - Normalize/resolve relations (names, join-path, uniqueness)
    - Bind fields to schema columns
    - Qualify bare fields or fail as ambiguous
    - Parse SQL expressions in select and extract aggregates
    - Handle function calls like DATE_TRUNC, COUNT, SUM, etc.
    - (Optional) Expand select *
    """

    # SQL aggregate function pattern: COUNT(*), SUM(x), MAX(a.b), etc.
    AGG_PATTERN = re.compile(
        r'\b(COUNT|SUM|AVG|MIN|MAX|COUNT_DISTINCT)\s*\(\s*(DISTINCT\s+)?(\*|[a-zA-Z_][a-zA-Z0-9_.]*)\s*\)',
        re.IGNORECASE
    )
    
    # Function call pattern: DATE_TRUNC('month', field), etc.
    FUNC_PATTERN = re.compile(
        r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(\s*([^)]*)\s*\)',
        re.IGNORECASE
    )

    def __init__(
        self,
        schema: Optional[Dict[str, Any]] = None,
        entities: Optional[Set[str]] = None,
        relations: Optional[List[str]] = None,
    ) -> None:
        self.schema = schema or {}
        self.entities = entities or set()
        self.relations = relations or []
        
        # Known SQL functions that are NOT aggregations
        self.non_agg_functions = {
            'DATE_TRUNC', 'DATE_PART', 'EXTRACT', 'CAST', 'CONVERT',
            'COALESCE', 'NULLIF', 'CASE', 'LOWER', 'UPPER', 'TRIM',
            'SUBSTRING', 'CONCAT', 'LENGTH', 'ROUND', 'FLOOR', 'CEIL'
        }

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
            NodeResult with bound query and transformed selectors
        """
        logger.debug("CompileBindNode: executing")
        notes = []
        errors = []
        
        root_entity = selectors.get("root_entity")
        if not root_entity:
            errors.append("Missing required 'root_entity' in selectors")
            return NodeResult(
                value=CompileBindResult(bound_query=None, errors=errors, notes=notes),
                notes=notes,
            )
        
        # Step 1: Parse SELECT expressions and extract aggregates
        transformed_selectors = dict(selectors)
        select_list = selectors.get("select", [])
        
        if select_list:
            parsed_aggs, new_select, agg_notes = self._parse_select_expressions(
                select_list, root_entity
            )
            notes.extend(agg_notes)
            
            # Merge extracted aggregates with existing ones
            existing_aggs = selectors.get("aggregations", [])
            merged_aggs = list(existing_aggs)
            
            for parsed_agg in parsed_aggs:
                # Check if this aggregation already exists
                already_exists = any(
                    a.get("alias") == parsed_agg.alias or
                    (a.get("field") == parsed_agg.field and a.get("agg") == parsed_agg.agg_func)
                    for a in merged_aggs
                )
                if not already_exists:
                    merged_aggs.append({
                        "agg": parsed_agg.agg_func.lower(),
                        "field": parsed_agg.field,
                        "alias": parsed_agg.alias,
                        "is_distinct": parsed_agg.is_distinct,
                    })
                    notes.append(f"CompileBindNode: extracted aggregation {parsed_agg.agg_func}({parsed_agg.field}) from SELECT")
            
            if merged_aggs:
                transformed_selectors["aggregations"] = merged_aggs
            
            if new_select:
                transformed_selectors["select"] = new_select
        
        # Step 2: Validate root_entity
        root_error = self._check_root_entity(root_entity)
        if root_error:
            errors.append(root_error)
        
        # Step 3: Resolve relations
        relations = selectors.get("relations", [])
        resolved_relations, rel_errors = self._resolve_relations(relations, root_entity)
        errors.extend(rel_errors)
        if resolved_relations != relations:
            notes.append(f"CompileBindNode: resolved relations {relations} -> {resolved_relations}")
        
        # Step 4: Move filters on aggregation aliases to HAVING clause
        transformed_selectors = self._move_agg_filters_to_having(
            transformed_selectors, notes
        )
        
        # Step 5: Bind fields (validation only - don't transform)
        self._validate_fields(transformed_selectors, root_entity, notes, errors)
        
        # Build bound query (for tracing/debugging)
        bound_query = BoundRelationalQuery(
            selectors=selectors,
            bound_select=[],
            bound_filters=[],
            bound_group_by=[],
            bound_aggregations=[],
            resolved_relations=resolved_relations,
            root_entity=root_entity,
        )
        
        if errors:
            notes.append(f"CompileBindNode: completed with {len(errors)} error(s)")
        else:
            notes.append("CompileBindNode: compile/bind successful")
        
        result = CompileBindResult(
            bound_query=bound_query,
            errors=errors,
            notes=notes,
            transformed_selectors=transformed_selectors,
        )

        return NodeResult(
            value=result,
            notes=notes,
        )
    
    def _parse_select_expressions(
        self,
        select_list: List[Dict[str, Any]],
        root_entity: str,
    ) -> Tuple[List[ParsedAggregation], List[Dict[str, Any]], List[str]]:
        """Parse SELECT expressions and extract aggregate functions.
        
        Args:
            select_list: List of select expressions from LLM plan
            root_entity: Root entity for field resolution
            
        Returns:
            (parsed_aggregations, new_select_list, notes)
        """
        parsed_aggs = []
        new_select = []
        notes = []
        
        for expr_dict in select_list:
            if not isinstance(expr_dict, dict):
                new_select.append(expr_dict)
                continue
                
            expr = expr_dict.get("expr", "")
            alias = expr_dict.get("alias")
            
            if not expr:
                new_select.append(expr_dict)
                continue
            
            # Check if expression contains aggregate functions
            agg_matches = list(self.AGG_PATTERN.finditer(expr))
            
            if agg_matches:
                # This is an aggregate expression
                for match in agg_matches:
                    agg_func = match.group(1).upper()
                    is_distinct = bool(match.group(2))
                    field_expr = match.group(3)
                    
                    # Handle COUNT(*) - use special marker
                    if field_expr == "*":
                        field_expr = "*"  # Keep as "*" - pandas provider will handle it
                    elif "." in field_expr:
                        # Qualified field like "orders.order_id" - extract just the column
                        parts = field_expr.split(".", 1)
                        if len(parts) == 2 and parts[0] == root_entity:
                            field_expr = parts[1]  # Use just the column name
                    
                    # Generate alias if not provided
                    if not alias:
                        if field_expr == "*":
                            alias = f"{agg_func.lower()}_all"
                        else:
                            alias = f"{agg_func.lower()}_{field_expr.replace('.', '_')}"
                    
                    parsed_aggs.append(ParsedAggregation(
                        agg_func=agg_func,
                        field=field_expr,
                        alias=alias,
                        is_distinct=is_distinct,
                    ))
                    notes.append(f"Extracted {agg_func}({field_expr}) as {alias}")
                
                # Keep the original expression in select (for reference)
                new_select.append(expr_dict)
            else:
                # Check for non-aggregate function calls
                func_matches = list(self.FUNC_PATTERN.finditer(expr))
                has_unsupported_func = False
                
                for match in func_matches:
                    func_name = match.group(1).upper()
                    if func_name not in self.non_agg_functions:
                        # Unknown function - might need special handling
                        notes.append(f"CompileBindNode: found function {func_name} in expression (may need special handling)")
                
                # Keep non-aggregate expressions as-is
                new_select.append(expr_dict)
        
        return parsed_aggs, new_select, notes
    
    def _validate_fields(
        self,
        selectors: Dict[str, Any],
        root_entity: str,
        notes: List[str],
        errors: List[str],
    ) -> None:
        """Validate field references in selectors."""
        # Validate aggregations
        for agg in selectors.get("aggregations", []):
            if isinstance(agg, dict):
                field_expr = agg.get("field", "")
                agg_func = agg.get("agg", "")
                
                # Check for unsupported patterns
                if field_expr == "*" and agg_func.upper() == "COUNT":
                    # COUNT(*) is valid but needs special handling
                    notes.append(f"CompileBindNode: COUNT(*) will be handled at execution time")
                elif "." in field_expr:
                    # Qualified field - check entity prefix
                    field_entity = field_expr.split(".")[0]
                    if field_entity != root_entity:
                        notes.append(f"CompileBindNode: aggregation field {field_expr} references non-root entity {field_entity}")
        
        # Validate filters
        filters = selectors.get("filters", {})
        if isinstance(filters, dict):
            filter_field = filters.get("field", "")
            filter_op = filters.get("op", "")
            
            # Check for unsupported operators
            supported_ops = ("=", "!=", "<", ">", "<=", ">=", "LIKE", "IN", "NOT_IN", "BETWEEN")
            if filter_op and filter_op.upper() not in supported_ops:
                if filter_op.upper() == "BETWEEN":
                    # BETWEEN is now supported - validate the value format
                    filter_value = filters.get("value")
                    if not isinstance(filter_value, (list, tuple)) or len(filter_value) != 2:
                        errors.append(f"BETWEEN operator requires a [low, high] list value, got: {filter_value}")
                    else:
                        notes.append(f"CompileBindNode: BETWEEN filter on {filter_field}")
                else:
                    errors.append(f"Unsupported comparison operator: {filter_op}")
        
        # Validate group_by
        for gb in selectors.get("group_by", []):
            if isinstance(gb, dict):
                field_expr = gb.get("field", "")
                if "(" in field_expr and ")" in field_expr:
                    # Function call in group_by (e.g., DATE_TRUNC)
                    notes.append(f"CompileBindNode: function in group_by: {field_expr} (will be handled at execution)")
    
    def _move_agg_filters_to_having(
        self,
        selectors: Dict[str, Any],
        notes: List[str],
    ) -> Dict[str, Any]:
        """Move filters on aggregation aliases to HAVING clause.
        
        Args:
            selectors: Selectors dict
            notes: Notes list to append to
            
        Returns:
            Transformed selectors with having clause
        """
        transformed = dict(selectors)
        
        # Get aggregation aliases
        agg_aliases = set()
        for agg in selectors.get("aggregations", []):
            if isinstance(agg, dict):
                alias = agg.get("alias")
                if alias:
                    agg_aliases.add(alias)
                # Also add the default alias format
                field = agg.get("field", "")
                agg_func = agg.get("agg", "")
                if field and agg_func:
                    agg_aliases.add(f"{agg_func}_{field.replace('.', '_')}")
        
        # Check if filters reference aggregation aliases
        filters = selectors.get("filters")
        if filters:
            moved_filters = self._extract_agg_filters(filters, agg_aliases)
            
            if moved_filters:
                # Remove moved filters from WHERE clause
                remaining_filters = self._remove_agg_filters(filters, agg_aliases)
                
                if remaining_filters:
                    transformed["filters"] = remaining_filters
                else:
                    transformed.pop("filters", None)
                
                # Add to HAVING clause
                existing_having = selectors.get("having")
                if existing_having:
                    # Combine with existing having
                    transformed["having"] = {
                        "type": "logical",
                        "op": "and",
                        "clauses": [existing_having, moved_filters] if isinstance(moved_filters, dict) else [existing_having] + moved_filters,
                    }
                else:
                    transformed["having"] = moved_filters
                
                notes.append(f"CompileBindNode: moved {len(moved_filters.get('clauses', [moved_filters]))} filter(s) on aggregation aliases to HAVING clause")
        
        return transformed
    
    def _extract_agg_filters(
        self,
        filters: Any,
        agg_aliases: Set[str],
    ) -> Optional[Any]:
        """Extract filters that reference aggregation aliases.
        
        Returns filters that should be moved to HAVING clause.
        """
        if isinstance(filters, dict):
            if filters.get("type") == "comparison":
                field = filters.get("field", "")
                if field in agg_aliases:
                    return filters
                return None
            elif filters.get("type") == "logical":
                moved_clauses = []
                for clause in filters.get("clauses", []):
                    moved = self._extract_agg_filters(clause, agg_aliases)
                    if moved:
                        moved_clauses.append(moved)
                
                if len(moved_clauses) == 0:
                    return None
                elif len(moved_clauses) == 1:
                    return moved_clauses[0]
                else:
                    return {
                        "type": "logical",
                        "op": filters.get("op", "and"),
                        "clauses": moved_clauses,
                    }
        return None
    
    def _remove_agg_filters(
        self,
        filters: Any,
        agg_aliases: Set[str],
    ) -> Optional[Any]:
        """Remove filters that reference aggregation aliases.
        
        Returns remaining filters for WHERE clause.
        """
        if isinstance(filters, dict):
            if filters.get("type") == "comparison":
                field = filters.get("field", "")
                if field in agg_aliases:
                    return None
                return filters
            elif filters.get("type") == "logical":
                remaining_clauses = []
                for clause in filters.get("clauses", []):
                    remaining = self._remove_agg_filters(clause, agg_aliases)
                    if remaining:
                        remaining_clauses.append(remaining)
                
                if len(remaining_clauses) == 0:
                    return None
                elif len(remaining_clauses) == 1:
                    return remaining_clauses[0]
                else:
                    return {
                        "type": "logical",
                        "op": filters.get("op", "and"),
                        "clauses": remaining_clauses,
                    }
        return None

    def _check_root_entity(self, root_entity: str) -> Optional[str]:
        """Check that root_entity exists in schema.

        Returns error message if not found, None if OK.
        """
        # For now, just check it's present and non-empty
        if not root_entity:
            return "Missing required 'root_entity' in selectors"
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
        # For now, pass through unchanged
        # In full implementation, would validate against schema
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
        # For now, pass through unchanged
        return [], []
