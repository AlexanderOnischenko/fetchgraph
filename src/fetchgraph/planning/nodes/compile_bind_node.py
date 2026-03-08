"""Stage 6: Compile/Bind (semantic binding to schema).

This node binds selectors to the relational schema:
- Check root_entity exists
- Normalize/resolve relations (names, join-path, uniqueness)
- Bind fields:
  - select.expr, filters.field, group_by.field, aggregations.field
  - Qualify bare fields (add entity prefix) or fail as ambiguous
  - (Optional) select * → expand by schema
- Deterministic auto-repair of obvious field/column errors (FR-1 to FR-8)

Result: BoundRelationalQuery (internal canonical form) with transformed selectors
"""

from __future__ import annotations

import copy
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

    # Auto-repairs applied (FR-7: Observability)
    auto_repairs: List[Dict[str, Any]] = field(default_factory=list)


class CompileBindNode:
    """Node 6: Compile/Bind (semantic binding to schema).

    Responsibilities:
    - Check root_entity exists in schema
    - Normalize/resolve relations (names, join-path, uniqueness)
    - Bind fields to schema columns
    - Qualify bare fields or fail as ambiguous
    - Parse SQL expressions in select and extract aggregates
    - Handle function calls like DATE_TRUNC, COUNT, SUM, etc.
    - Deterministic auto-repair of obvious field/column errors (FR-1 to FR-8)
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

        # Schema indices (FR-1)
        self._entity_to_columns: Dict[str, Set[str]] = {}
        self._adjacency_by_entity: Dict[str, Set[str]] = {}
        self._relation_by_entity_pair: Dict[Tuple[str, str], str] = {}

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
        auto_repairs = []

        root_entity = selectors.get("root_entity")
        if not root_entity:
            errors.append("Missing required 'root_entity' in selectors")
            return NodeResult(
                value=CompileBindResult(bound_query=None, errors=errors, notes=notes, auto_repairs=auto_repairs),
                notes=notes,
            )

        # Step 1: Parse SELECT expressions and extract aggregates
        # Use deepcopy to avoid mutating the original selectors (no-mutation guarantee)
        transformed_selectors = copy.deepcopy(selectors)
        select_list = transformed_selectors.get("select", [])

        if select_list:
            parsed_aggs, new_select, agg_notes = self._parse_select_expressions(
                select_list, root_entity
            )
            notes.extend(agg_notes)

            # Merge extracted aggregates with existing ones
            existing_aggs = transformed_selectors.get("aggregations", [])
            merged_aggs = copy.deepcopy(existing_aggs)

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

        # Step 2: Build schema index (FR-1)
        # Only build indices if schema has entities (not JSON Schema)
        if self.schema and self.schema.get("entities"):
            self._build_schema_index()

        # Step 3: Auto-qualify fields through relations (FR-4)
        # Only auto-qualify if we have schema indices
        if self._entity_to_columns:
            self._auto_qualify_fields(transformed_selectors, root_entity, notes, errors, auto_repairs)
        else:
            # No schema available - skip auto-qualification, just pass through
            # This is expected for replay handlers that receive JSON Schema instead of entities schema
            pass

        # Step 5: Validate root_entity
        root_error = self._check_root_entity(root_entity)
        if root_error:
            errors.append(root_error)

        # Step 6: Resolve relations
        relations = transformed_selectors.get("relations", [])
        resolved_relations, rel_errors = self._resolve_relations(relations, root_entity)
        errors.extend(rel_errors)
        if resolved_relations != relations:
            notes.append(f"CompileBindNode: resolved relations {relations} -> {resolved_relations}")

        # Step 7: Move filters on aggregation aliases to HAVING clause
        transformed_selectors = self._move_agg_filters_to_having(
            transformed_selectors, notes
        )

        # Step 7.5: Canonicalize structured field refs (entity + field pairs)
        self._canonicalize_structured_field_refs(transformed_selectors, notes, errors)

        # Step 8: Validate fields (now includes unresolved/ambiguous errors)
        self._validate_fields(transformed_selectors, notes, errors)

        # Build bound query (for tracing/debugging)
        # Use transformed_selectors (after auto-repair) not original selectors
        bound_query = BoundRelationalQuery(
            selectors=transformed_selectors,
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
            auto_repairs=auto_repairs,
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
        notes: List[str],
        errors: List[str],
    ) -> None:
        """Validate field references in selectors.

        Note: Auto-qualification happens in _auto_qualify_fields before this.
        This method just validates and logs.
        """
        # Log aggregations
        for agg in selectors.get("aggregations", []):
            if isinstance(agg, dict):
                field_expr = agg.get("field", "")
                agg_func = agg.get("agg", "")

                # Check for unsupported patterns
                if field_expr == "*" and agg_func.upper() == "COUNT":
                    # COUNT(*) is valid but needs special handling
                    notes.append(f"CompileBindNode: COUNT(*) will be handled at execution time")

        # Log group_by
        for gb in selectors.get("group_by", []):
            if isinstance(gb, dict):
                field_expr = gb.get("field", "")
                if "(" in field_expr and ")" in field_expr:
                    # Function call in group_by (e.g., DATE_TRUNC)
                    notes.append(f"CompileBindNode: function in group_by: {field_expr} (will be handled at execution)")

    def _auto_qualify_fields(
        self,
        selectors: Dict[str, Any],
        root_entity: str,
        notes: List[str],
        errors: List[str],
        auto_repairs: List[dict],
    ) -> None:
        """Auto-qualify fields through relations.
        
        Rules:
        1. If field is qualified (entity.column) and entity != root_entity:
           - Check if relation exists from root_entity to entity
           - If yes — add relation and keep field
           - If no — error
        2. If field is bare (column):
           - Check if exists in root_entity
           - If yes — keep
           - If no — check in connected entities via relations
           - If found in one — qualify with that entity
           - If found in multiple — ambiguous error
           - If not found — error
        """
        if not self._entity_to_columns:
            return  # No schema index available
        
        relations_to_add = set()
        
        # Process aggregations
        for agg in selectors.get("aggregations", []):
            if isinstance(agg, dict) and "field" in agg:
                agg_func = agg.get("agg", "")
                field_expr = agg["field"]
                
                # G-4a: Special case COUNT(*) - do not resolve * as schema column
                if agg_func == "count" and field_expr == "*":
                    notes.append(f"CompileBindNode: COUNT(*) special case preserved (field='*')")
                    auto_repairs.append({
                        "type": "count_star_special_case_preserved",
                        "agg": agg_func,
                        "field": field_expr,
                        "alias": agg.get("alias", ""),
                    })
                    continue  # Skip field resolution for COUNT(*)
                
                # G-4a: Reject other AGG(*) forms as contract errors
                if field_expr == "*" and agg_func != "count":
                    errors.append(f"Invalid aggregation: {agg_func}(*) is not supported. Only COUNT(*) is allowed.")
                    continue
                
                new_field, field_errors = self._resolve_field_via_relations(
                    field_expr, root_entity, relations_to_add, notes, auto_repairs
                )
                if new_field:
                    agg["field"] = new_field
                # Only add errors if field was NOT re-qualified
                if not new_field:
                    errors.extend(field_errors)

        # Process select expressions
        for sel in selectors.get("select", []):
            if isinstance(sel, dict) and "expr" in sel:
                expr = sel["expr"]
                # Only process simple field expressions (not functions/aggregations)
                if "(" not in expr:
                    new_field, field_errors = self._resolve_field_via_relations(
                        expr, root_entity, relations_to_add, notes, auto_repairs
                    )
                    if new_field:
                        sel["expr"] = new_field
                    # Only add errors if field was NOT re-qualified
                    if not new_field:
                        errors.extend(field_errors)

        # Process filters
        self._qualify_filter_fields(
            selectors.get("filters", {}), root_entity, relations_to_add, notes, errors, auto_repairs
        )

        # Process group_by
        for gb in selectors.get("group_by", []):
            if isinstance(gb, dict) and "field" in gb:
                field_expr = gb["field"]
                new_field, field_errors = self._resolve_field_via_relations(
                    field_expr, root_entity, relations_to_add, notes, auto_repairs
                )
                if new_field:
                    gb["field"] = new_field
                # Only add errors if field was NOT re-qualified
                if not new_field:
                    errors.extend(field_errors)

        # Process having (recursive, like filters)
        self._qualify_filter_fields(
            selectors.get("having", {}), root_entity, relations_to_add, notes, errors, auto_repairs
        )

        # Add discovered relations
        if relations_to_add:
            existing_relations = set(selectors.get("relations", []))
            selectors["relations"] = list(existing_relations | relations_to_add)
            for rel in relations_to_add:
                notes.append(f"CompileBindNode: auto-added relation {rel}")

    def _canonicalize_structured_field_refs(
        self,
        selectors: Dict[str, Any],
        notes: List[str],
        errors: List[str],
    ) -> None:
        """Canonicalize structured field references for provider compatibility.

        For structures with separate entity field (group_by, filters, having):
        - If entity is set and field is "entity.column" form, canonicalize to bare column
        - If entity is set and field is "other_entity.column", return error

        This ensures selectors are in canonical form expected by providers.
        """
        # Canonicalize group_by
        for i, gb in enumerate(selectors.get("group_by", [])):
            if isinstance(gb, dict):
                entity = gb.get("entity")
                field = gb.get("field", "")
                
                if entity and "." in field:
                    field_entity, column = field.split(".", 1)
                    
                    if field_entity == entity:
                        # Same entity prefix - canonicalize to bare field
                        gb["field"] = column
                        notes.append(f"CompileBindNode: canonicalized group_by[{i}] field {field} -> {column} for entity {entity}")
                    else:
                        # Different entity - this is an error
                        errors.append(
                            f"Inconsistent field reference in group_by[{i}]: "
                            f"entity='{entity}', field='{field}'"
                        )
        
        # Canonicalize filters
        def canonicalize_filter(clause: Any, path: str) -> None:
            if not isinstance(clause, dict):
                return
            
            if clause.get("type") == "comparison":
                entity = clause.get("entity")
                field = clause.get("field", "")
                
                if entity and "." in field:
                    field_entity, column = field.split(".", 1)
                    
                    if field_entity == entity:
                        # Same entity prefix - canonicalize to bare field
                        clause["field"] = column
                        notes.append(f"CompileBindNode: canonicalized {path} field {field} -> {column} for entity {entity}")
                    else:
                        # Different entity - this is an error
                        errors.append(
                            f"Inconsistent field reference in {path}: "
                            f"entity='{entity}', field='{field}'"
                        )
            elif clause.get("type") == "logical":
                for i, sub_clause in enumerate(clause.get("clauses", [])):
                    canonicalize_filter(sub_clause, f"{path}.clauses[{i}]")
        
        if selectors.get("filters"):
            canonicalize_filter(selectors["filters"], "filters")
        
        # Canonicalize having
        if selectors.get("having"):
            canonicalize_filter(selectors["having"], "having")

    def _resolve_field_via_relations(
        self,
        field_expr: str,
        root_entity: str,
        relations_to_add: Set[str],
        notes: List[str],
        auto_repairs: List[dict],
    ) -> tuple[Optional[str], List[str]]:
        """Resolve a field expression through relations.

        Returns (qualified_field, errors) where qualified_field is None if unchanged.
        """
        errors = []

        # Handle special case: COUNT(*) - no qualification needed
        if field_expr == "*":
            return None, errors

        if "." in field_expr:
            # Already qualified: entity.column
            field_entity, column = field_expr.split(".", 1)
            
            # Field is qualified with specific entity
            # First check if column exists in the specified entity
            if column in self._entity_to_columns.get(field_entity, set()):
                # Column exists - check relation if cross-entity
                if field_entity != root_entity:
                    relation = self._relation_by_entity_pair.get((root_entity, field_entity))
                    if relation:
                        relations_to_add.add(relation)
                        return None, errors  # Keep as-is, relation added
                    else:
                        # Check if relation exists in schema (will be added by another field)
                        has_relation = (
                            (root_entity, field_entity) in self._relation_by_entity_pair or
                            (field_entity, root_entity) in self._relation_by_entity_pair
                        )
                        if has_relation:
                            return None, errors  # Relation exists in schema
                        # CRITICAL: Cross-entity field without relation - this is an error
                        errors.append(
                            f"Cross-entity field '{field_expr}' requires relation "
                            f"between '{root_entity}' and '{field_entity}', but none found"
                        )
                        return None, errors
                return None, errors  # Same entity, column exists, no change needed
            
            # Column doesn't exist in specified entity - try same-entity prefix strip
            # Rule 5.3: Same-entity prefix strip (customer_segment -> segment for customers)
            # This applies to ANY qualified entity, not just root_entity
            normalized = self._normalize_candidate_name(column, field_entity)
            if normalized and normalized != column and normalized in self._entity_to_columns.get(field_entity, set()):
                # Found valid column after prefix strip
                notes.append(f"CompileBindNode: same-entity prefix strip {field_expr} -> {field_entity}.{normalized}")
                auto_repairs.append({
                    "kind": "same_entity_prefix_strip",
                    "from": field_expr,
                    "to": f"{field_entity}.{normalized}",
                })
                return f"{field_entity}.{normalized}", errors
            
            # Column still not found - search connected entities
            connected_entities = self._adjacency_by_entity.get(field_entity, set())
            found_in = []
            for entity in connected_entities:
                if column in self._entity_to_columns.get(entity, set()):
                    found_in.append(entity)
            if len(found_in) == 1:
                relation = self._relation_by_entity_pair.get((root_entity, found_in[0]))
                if relation:
                    relations_to_add.add(relation)
                notes.append(f"CompileBindNode: auto-qualified {field_expr} -> {found_in[0]}.{column}")
                return f"{found_in[0]}.{column}", errors
            elif len(found_in) > 1:
                errors.append(f"Ambiguous field reference '{field_expr}': column '{column}' not in {field_entity}, found in {[f'{e}.{column}' for e in found_in]}")
                return None, errors
            else:
                errors.append(f"Column not found for entity '{field_entity}': {column}")
                return None, errors
        else:
            # Bare field: column
            column = field_expr
            
            # Check if exists in root_entity
            if column in self._entity_to_columns.get(root_entity, set()):
                return None, errors  # Exists in root, no change needed
            
            # Check in connected entities
            connected_entities = self._adjacency_by_entity.get(root_entity, set())
            found_in = []
            
            for entity in connected_entities:
                if column in self._entity_to_columns.get(entity, set()):
                    found_in.append(entity)
            
            if len(found_in) == 1:
                # Found in exactly one connected entity — qualify
                relation = self._relation_by_entity_pair.get((root_entity, found_in[0]))
                if relation:
                    relations_to_add.add(relation)
                notes.append(f"CompileBindNode: auto-qualified {column} -> {found_in[0]}.{column}")
                # Record auto-repair
                auto_repairs.append({
                    "kind": "bare_field_qualified",
                    "from": column,
                    "to": f"{found_in[0]}.{column}",
                    "relation_added": relation if relation else None,
                })
                return f"{found_in[0]}.{column}", errors
            elif len(found_in) > 1:
                # Ambiguous
                errors.append(
                    f"Ambiguous field reference '{column}': "
                    f"candidates={[f'{e}.{column}' for e in found_in]}"
                )
                return None, errors
            else:
                # Not found anywhere
                errors.append(f"Column not found for entity '{root_entity}': {column}")
                return None, errors

    def _normalize_candidate_name(self, name: str, entity: str) -> Optional[str]:
        """Normalize a candidate column name within an entity (FR-5.3).

        Safe normalizations:
        - lowercase
        - trim
        - remove entity prefix (singular/plural): customer_segment -> segment for customers
        """
        name = name.lower().strip()

        # Try removing entity prefix
        entity_singular = entity.rstrip("s").lower()  # customers -> customer
        entity_plural = entity.lower()  # customers

        prefixes_to_try = [
            f"{entity_singular}_",
            f"{entity_plural}_",
            f"{entity_singular}.",
            f"{entity_plural}.",
        ]

        for prefix in prefixes_to_try:
            if name.startswith(prefix):
                return name[len(prefix):]

        return name

    def _qualify_filter_fields(
        self,
        filters: Any,
        root_entity: str,
        relations_to_add: Set[str],
        notes: List[str],
        errors: List[str],
        auto_repairs: List[dict],
    ) -> None:
        """Recursively qualify filter field references."""
        if not isinstance(filters, dict):
            return

        if filters.get("type") == "comparison":
            field_expr = filters.get("field", "")
            if field_expr:
                new_field, field_errors = self._resolve_field_via_relations(
                    field_expr, root_entity, relations_to_add, notes, auto_repairs
                )
                if new_field:
                    filters["field"] = new_field
                # Only add errors if field was NOT re-qualified
                if not new_field:
                    errors.extend(field_errors)
        elif filters.get("type") == "logical":
            for clause in filters.get("clauses", []):
                self._qualify_filter_fields(clause, root_entity, relations_to_add, notes, errors, auto_repairs)
    
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
        
        # If schema index is available, validate root_entity exists in schema
        if self._entity_to_columns and root_entity not in self._entity_to_columns:
            available_entities = list(self._entity_to_columns.keys())
            return f"root_entity '{root_entity}' not found in schema. Available entities: {available_entities}"
        
        return None

    # ============================================================================
    # FR-1: Schema Index Building
    # ============================================================================

    def _build_schema_index(self) -> None:
        """Build schema indices for fast field resolution (FR-1)."""
        self._entity_to_columns = {}
        self._adjacency_by_entity = {}
        self._relation_by_entity_pair = {}

        entities = self.schema.get("entities", [])
        relations = self.schema.get("relations", [])

        # Build entity_to_columns
        for entity in entities:
            entity_name = entity.get("name", "")
            columns = entity.get("columns", [])

            self._entity_to_columns[entity_name] = {col.get("name", "") for col in columns}

        # Build adjacency and relation indices from relations
        for rel in relations:
            from_entity = rel.get("from_entity", "")
            to_entity = rel.get("to_entity", "")
            rel_name = rel.get("name", "")
            
            if from_entity and to_entity:
                self._adjacency_by_entity.setdefault(from_entity, set()).add(to_entity)
                self._adjacency_by_entity.setdefault(to_entity, set()).add(from_entity)
                self._relation_by_entity_pair[(from_entity, to_entity)] = rel_name
                self._relation_by_entity_pair[(to_entity, from_entity)] = rel_name

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
