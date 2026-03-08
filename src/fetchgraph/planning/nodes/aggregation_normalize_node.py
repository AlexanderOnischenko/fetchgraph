"""Aggregation normalize (semantic, pre-binding).

This node normalizes aggregations BEFORE schema binding:
- Normalize aggregation names to canonical form (count, count_distinct, sum, avg, min, max)
- Extract aggregations from select[].expr to aggregations[]
- Remove raw aggregate expressions from select after extraction
- group_by closure: all non-agg select fields must be in group_by
- Move aggregate predicates from filters to having
- Alias uniqueness and stability
- COUNT(*) special case support
- COUNT(DISTINCT field) canonicalization
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
class NormalizedAggregation:
    """A normalized aggregation specification."""
    
    # Aggregation function name (canonical)
    agg: str  # count, sum, avg, min, max, count_distinct, etc.
    
    # Field to aggregate
    field: str
    
    # Output alias
    alias: str
    
    # Distinct flag
    is_distinct: bool = False


@dataclass(frozen=True)
class AggregationNormalizeResult:
    """Result from aggregation normalization."""

    # Normalized aggregations
    normalized_aggregations: List[NormalizedAggregation]

    # Normalized group_by fields
    normalized_group_by: List[str]

    # Notes about transformations
    notes: List[str]

    # Changes made
    changes: Dict[str, Any] = field(default_factory=dict)

    # Normalized selectors (with op normalized, etc.)
    normalized_selectors: Dict[str, Any] = field(default_factory=dict)


class AggregationNormalizeNode:
    """Aggregation normalize node (semantic, pre-binding).

    Responsibilities:
    - Normalize aggregation names to canonical form
    - Extract aggregations from select[].expr to aggregations[]
    - Remove raw aggregate expressions from select after extraction
    - group_by closure: ensure all non-agg select fields in group_by
    - Move aggregate predicates from filters to having
    - Alias uniqueness and stability
    - COUNT(*) special case
    - COUNT(DISTINCT field) canonicalization
    """
    
    def __init__(
        self,
        canonical_agg_names: Optional[Dict[str, str]] = None,
    ) -> None:
        # Map various names to canonical form
        self.canonical_agg_names = canonical_agg_names or {
            # Count variants
            "count": "count",
            "COUNT": "count",
            "количество": "count",
            # Sum variants
            "sum": "sum",
            "SUM": "sum",
            "сумма": "sum",
            # Avg variants
            "avg": "avg",
            "average": "avg",
            "AVG": "avg",
            "среднее": "avg",
            # Min/Max variants
            "min": "min",
            "MIN": "min",
            "минимум": "min",
            "max": "max",
            "MAX": "max",
            "максимум": "max",
            # Count distinct variants
            "count_distinct": "count_distinct",
            "COUNT DISTINCT": "count_distinct",
            "COUNT(DISTINCT": "count_distinct",
            "количество разных": "count_distinct",
            "unique": "count_distinct",
        }
    
    def execute(
        self,
        ctx: NodeContext,
        selectors: Dict[str, Any],
    ) -> NodeResult[AggregationNormalizeResult]:
        """Execute aggregation normalization.

        Args:
            ctx: Pipeline context
            selectors: Provider-normalized selectors

        Returns:
            NodeResult with normalized aggregations and selectors
        """
        logger.debug("AggregationNormalizeNode: executing")

        notes = []
        
        # Deep copy to avoid mutating input (G-AN-16)
        normalized_selectors = copy.deepcopy(selectors)

        # Track changes
        changes = {
            "op_normalized": False,
            "root_entity_inferred": False,
            "aggregations_extracted_from_select": 0,
            "aggregations_merged": 0,
            "group_by_fields_added": 0,
            "filters_moved_to_having": 0,
            "alias_collisions_resolved": 0,
        }

        # Step 1: Normalize op (G-AN-01)
        if normalized_selectors.get("op") == "aggregate":
            normalized_selectors["op"] = "query"
            notes.append("AggregationNormalizeNode: normalized op='aggregate' to op='query'")
            changes["op_normalized"] = True

        # Step 2: Infer root_entity if missing (G-AN-02)
        if "root_entity" not in normalized_selectors:
            inferred_entity = self._infer_root_entity(normalized_selectors)
            if inferred_entity:
                normalized_selectors["root_entity"] = inferred_entity
                notes.append(f"AggregationNormalizeNode: inferred root_entity='{inferred_entity}' from field names")
                changes["root_entity_inferred"] = True

        # Step 3: Extract aggregations from selectors["aggregations"]
        normalized_aggregations: List[NormalizedAggregation] = []
        existing_agg_keys: Set[Tuple[str, str]] = set()  # (agg, field) pairs
        
        aggregations_list = normalized_selectors.get("aggregations", [])
        if isinstance(aggregations_list, list):
            for agg in aggregations_list:
                if isinstance(agg, dict):
                    norm_agg = self._normalize_existing_aggregation(agg)
                    if norm_agg:
                        normalized_aggregations.append(norm_agg)
                        existing_agg_keys.add((norm_agg.agg, norm_agg.field))
                        notes.append(f"Normalized existing aggregation: {norm_agg.agg}({norm_agg.field}) as {norm_agg.alias}")

        # Step 4: Extract aggregations from select[].expr (G-AN-04)
        select_fields = normalized_selectors.get("select", [])
        cleaned_select = []
        
        if isinstance(select_fields, list):
            for sel in select_fields:
                if isinstance(sel, dict):
                    expr = sel.get("expr", "")
                    alias = sel.get("alias")
                    
                    # Try to extract aggregation from expr
                    extracted_agg = self._extract_agg_from_expr(expr)
                    
                    if extracted_agg:
                        # Use existing alias if provided, otherwise use generated
                        if alias:
                            extracted_agg = NormalizedAggregation(
                                agg=extracted_agg.agg,
                                field=extracted_agg.field,
                                alias=alias,
                                is_distinct=extracted_agg.is_distinct,
                            )
                        
                        # Check for duplicate
                        agg_key = (extracted_agg.agg, extracted_agg.field)
                        if agg_key in existing_agg_keys:
                            # Already have this aggregation - skip duplicate
                            changes["aggregations_merged"] += 1
                            notes.append(f"Merged duplicate aggregation: {extracted_agg.agg}({extracted_agg.field})")
                        else:
                            normalized_aggregations.append(extracted_agg)
                            existing_agg_keys.add(agg_key)
                            changes["aggregations_extracted_from_select"] += 1
                            notes.append(f"Extracted aggregation from select: {extracted_agg.agg}({extracted_agg.field}) as {extracted_agg.alias}")
                        # Don't add to cleaned_select - raw agg expression removed (G-AN-10)
                    else:
                        # Not an aggregation - keep in select
                        cleaned_select.append(sel)
                else:
                    cleaned_select.append(sel)

        # Update select with cleaned version (no raw aggregate expressions)
        if cleaned_select != select_fields:
            normalized_selectors["select"] = cleaned_select

        # Step 5: Ensure unique aliases (G-AN-08)
        normalized_aggregations = self._ensure_unique_aliases(normalized_aggregations, changes)

        # Step 6: Compute group_by closure (G-AN-09)
        normalized_group_by = self._compute_group_by_closure(
            select_fields=cleaned_select,
            aggregations=normalized_aggregations,
            existing_group_by=normalized_selectors.get("group_by", []),
        )
        if isinstance(normalized_group_by, list):
            # Count how many fields were added
            existing_gb_count = len(normalized_selectors.get("group_by", []))
            new_gb_count = len(normalized_group_by)
            if new_gb_count > existing_gb_count:
                changes["group_by_fields_added"] = new_gb_count - existing_gb_count
                notes.append(f"AggregationNormalizeNode: added {changes['group_by_fields_added']} field(s) to group_by for closure")
        
        # Update group_by in normalized_selectors
        if normalized_group_by:
            normalized_selectors["group_by"] = normalized_group_by

        # Step 7: Move aggregate predicates from filters to having (G-AN-11, G-AN-12)
        filters_moved_count = self._move_aggregate_filters_to_having(
            normalized_selectors,
            normalized_aggregations,
        )
        if filters_moved_count > 0:
            changes["filters_moved_to_having"] = filters_moved_count
            notes.append(f"AggregationNormalizeNode: moved {filters_moved_count} aggregate predicate(s) from filters to having")

        # Update aggregations in normalized_selectors
        normalized_selectors["aggregations"] = [
            {"agg": agg.agg, "field": agg.field, "alias": agg.alias}
            for agg in normalized_aggregations
        ]

        # Finalize changes dict
        changes["input_agg_count"] = len(aggregations_list) if isinstance(aggregations_list, list) else 0
        changes["output_agg_count"] = len(normalized_aggregations)

        if not normalized_aggregations:
            notes.append("AggregationNormalizeNode: no aggregations found in selectors")
        else:
            notes.append(f"AggregationNormalizeNode: normalized {len(normalized_aggregations)} aggregation(s)")

        result = AggregationNormalizeResult(
            normalized_aggregations=normalized_aggregations,
            normalized_group_by=normalized_group_by if isinstance(normalized_group_by, list) else [],
            notes=notes,
            changes=changes,
            normalized_selectors=normalized_selectors,
        )

        return NodeResult(
            value=result,
            notes=notes,
        )

    def normalize_agg_name(self, agg_name: str) -> str:
        """Normalize aggregation name to canonical form."""
        # Clean up common patterns
        cleaned = agg_name.strip()
        cleaned = re.sub(r'\s*\(\s*', '(', cleaned)
        cleaned = re.sub(r'\s*\)', ')', cleaned)

        # Check for exact match first (case-insensitive)
        cleaned_lower = cleaned.lower()
        exact_matches = {
            "count": "count",
            "sum": "sum",
            "avg": "avg",
            "average": "avg",
            "min": "min",
            "max": "max",
            "count_distinct": "count_distinct",
            "median": "median",
        }
        if cleaned_lower in exact_matches:
            return exact_matches[cleaned_lower]

        # Check for distinct pattern - "COUNT DISTINCT" or "COUNT(DISTINCT"
        if re.match(r'COUNT\s*\(\s*DISTINCT', cleaned, re.IGNORECASE):
            return "count_distinct"
        if re.match(r'COUNT\s+DISTINCT', cleaned, re.IGNORECASE):
            return "count_distinct"

        # Look up in canonical map (check longer patterns first)
        for pattern, canonical in sorted(self.canonical_agg_names.items(), key=lambda x: -len(x[0])):
            if pattern.upper() in cleaned.upper():
                return canonical

        # Return lowercase by default
        return cleaned_lower

    def _extract_agg_from_expr(self, expr: str) -> Optional[NormalizedAggregation]:
        """Extract aggregation from expression string (G-AN-04).

        Examples:
        - "COUNT(*)" → NormalizedAggregation(agg="count", field="*", alias="count_all")
        - "SUM(line_total)" → NormalizedAggregation(agg="sum", field="line_total", alias="sum_line_total")
        - "COUNT(DISTINCT customer_id)" → NormalizedAggregation(agg="count_distinct", field="customer_id", ...)
        
        Only COUNT(*) is supported for field="*". Other AGG(*) forms are rejected.
        """
        if not isinstance(expr, str):
            return None
        
        # Pattern: AGG(field) or AGG(DISTINCT field)
        # Support qualified fields: entity.column
        agg_pattern = r'(\w+)\s*\(\s*(?:DISTINCT\s+)?(\*|\w+(?:\.\w+)?)\s*\)'
        match = re.match(agg_pattern, expr, re.IGNORECASE)

        if not match:
            return None

        agg_name = match.group(1)
        field_name = match.group(2)

        # Check for distinct
        is_distinct = 'DISTINCT' in expr.upper()

        # Handle AGG(*) special case - only COUNT(*) is allowed (G-AN-06)
        if field_name == "*":
            if agg_name.upper() == "COUNT" and not is_distinct:
                return NormalizedAggregation(
                    agg="count",
                    field="*",
                    alias="count_all",
                    is_distinct=False,
                )
            # Other AGG(*) forms are not supported
            return None

        canonical_agg = self.normalize_agg_name(agg_name)
        if is_distinct and canonical_agg == "count":
            canonical_agg = "count_distinct"

        # Generate alias
        alias = f"{canonical_agg}_{field_name.replace('.', '_')}"

        return NormalizedAggregation(
            agg=canonical_agg,
            field=field_name,
            alias=alias,
            is_distinct=False,  # We use agg="count_distinct" instead
        )
    
    def _infer_root_entity(self, selectors: Dict[str, Any]) -> Optional[str]:
        """Infer root_entity from field names (G-AN-02)."""
        # Check aggregations first
        for agg in selectors.get("aggregations", []):
            if isinstance(agg, dict):
                field = agg.get("field", "")
                if "." in field:
                    return field.split(".")[0]

        # Check select fields
        for sel in selectors.get("select", []):
            if isinstance(sel, dict):
                expr = sel.get("expr", "")
                if "." in expr:
                    return expr.split(".")[0]

        # Check filters
        filters = selectors.get("filters", {})
        if isinstance(filters, dict):
            field = filters.get("field", "")
            if "." in field:
                return field.split(".")[0]

        return None

    def _normalize_existing_aggregation(self, agg: Dict[str, Any]) -> Optional[NormalizedAggregation]:
        """Normalize an existing aggregation from aggregations[] list."""
        agg_func = agg.get("agg", "")
        field_name = agg.get("field", "")
        alias = agg.get("alias")
        is_distinct = agg.get("is_distinct", False)

        # Handle COUNT(*) special case (G-AN-06) - only count(*) is allowed
        if field_name == "*":
            if agg_func.upper() == "COUNT" and not is_distinct:
                if not alias:
                    alias = "count_all"
                return NormalizedAggregation(
                    agg="count",
                    field="*",
                    alias=alias,
                    is_distinct=False,
                )
            # Other AGG(*) forms are not supported - return None to skip
            return None

        # Normalize aggregation function name
        canonical_agg = self.normalize_agg_name(agg_func)
        
        # Handle count_distinct (G-AN-13)
        if is_distinct and canonical_agg == "count":
            canonical_agg = "count_distinct"

        # Generate alias if not provided
        if not alias:
            alias = f"{canonical_agg}_{field_name.replace('.', '_')}"

        return NormalizedAggregation(
            agg=canonical_agg,
            field=field_name,
            alias=alias,
            is_distinct=False,  # We use agg="count_distinct" instead
        )

    def _ensure_unique_aliases(
        self,
        aggregations: List[NormalizedAggregation],
        changes: Dict[str, Any],
    ) -> List[NormalizedAggregation]:
        """Ensure all aliases are unique (G-AN-08)."""
        seen_aliases: Set[str] = set()
        result: List[NormalizedAggregation] = []
        
        for agg in aggregations:
            alias = agg.alias
            if alias in seen_aliases:
                # Generate unique alias with suffix
                base_alias = alias
                suffix = 2
                while f"{base_alias}_{suffix}" in seen_aliases:
                    suffix += 1
                alias = f"{base_alias}_{suffix}"
                changes["alias_collisions_resolved"] += 1
            
            seen_aliases.add(alias)
            result.append(NormalizedAggregation(
                agg=agg.agg,
                field=agg.field,
                alias=alias,
                is_distinct=agg.is_distinct,
            ))
        
        return result

    def _compute_group_by_closure(
        self,
        select_fields: List[Any],
        aggregations: List[NormalizedAggregation],
        existing_group_by: List[Any],
    ) -> List[Any]:
        """Compute group_by closure (G-AN-09).
        
        All non-aggregate select fields must be in group_by.
        """
        if not aggregations:
            # No aggregations - no closure needed
            return list(existing_group_by) if existing_group_by else []
        
        # Build set of existing group_by fields
        gb_set: Set[str] = set()
        for gb in existing_group_by:
            if isinstance(gb, str):
                gb_set.add(gb)
            elif isinstance(gb, dict):
                entity = gb.get("entity", "")
                field = gb.get("field", "")
                if entity:
                    gb_set.add(f"{entity}.{field}")
                else:
                    gb_set.add(field)
        
        result_group_by = list(existing_group_by) if existing_group_by else []
        
        # Find non-aggregate select fields
        for sel in select_fields:
            if isinstance(sel, dict):
                expr = sel.get("expr", "")
                
                # Check if this is an aggregate expression
                if self._is_aggregate_expr(expr):
                    continue
                
                # This is a non-aggregate field - ensure it's in group_by
                if expr and expr not in gb_set:
                    # Add to group_by in the same format as existing entries
                    if existing_group_by and isinstance(existing_group_by[0], dict):
                        # Use dict format
                        if "." in expr:
                            entity, field = expr.split(".", 1)
                            result_group_by.append({"entity": entity, "field": field})
                        else:
                            result_group_by.append({"entity": "", "field": expr})
                    else:
                        # Use string format
                        result_group_by.append(expr)
                    gb_set.add(expr)
        
        return result_group_by

    def _is_aggregate_expr(self, expr: str) -> bool:
        """Check if expression is an aggregate function."""
        if not isinstance(expr, str):
            return False
        
        # Check for aggregate function patterns
        canonical_agg_names = {"count", "sum", "avg", "min", "max", "count_distinct"}
        expr_upper = expr.upper()
        
        for agg_name in canonical_agg_names:
            if f"{agg_name.upper()}(" in expr_upper:
                return True
        
        return False

    def _move_aggregate_filters_to_having(
        self,
        normalized_selectors: Dict[str, Any],
        aggregations: List[NormalizedAggregation],
    ) -> int:
        """Move aggregate predicates from filters to having (G-AN-11, G-AN-12)."""
        # Build set of aggregate aliases
        agg_aliases = {agg.alias for agg in aggregations}
        
        filters = normalized_selectors.get("filters", {})
        if not filters:
            return 0
        
        moved_count = 0
        
        # Check if filters reference aggregate aliases
        aggregate_clauses = self._extract_aggregate_clauses(filters, agg_aliases)
        
        if aggregate_clauses:
            # Remove aggregate clauses from filters
            remaining_filters = self._remove_aggregate_clauses(filters, agg_aliases)
            
            # Add to having
            existing_having = normalized_selectors.get("having", {})
            if existing_having:
                # Merge with existing having
                if existing_having.get("type") == "logical" and existing_having.get("op") == "and":
                    # Add to existing logical AND
                    existing_clauses = existing_having.get("clauses", [])
                    existing_clauses.extend(aggregate_clauses)
                else:
                    # Wrap both in AND
                    normalized_selectors["having"] = {
                        "type": "logical",
                        "op": "and",
                        "clauses": [existing_having] + aggregate_clauses,
                    }
            else:
                # Create new having
                if len(aggregate_clauses) == 1:
                    normalized_selectors["having"] = aggregate_clauses[0]
                else:
                    normalized_selectors["having"] = {
                        "type": "logical",
                        "op": "and",
                        "clauses": aggregate_clauses,
                    }
            
            # Update filters
            if remaining_filters:
                normalized_selectors["filters"] = remaining_filters
            else:
                normalized_selectors.pop("filters", None)
            
            moved_count = len(aggregate_clauses)
        
        return moved_count

    def _extract_aggregate_clauses(
        self,
        clause: Any,
        agg_aliases: Set[str],
    ) -> List[Dict[str, Any]]:
        """Extract filter clauses that reference aggregate aliases."""
        result = []
        
        if not isinstance(clause, dict):
            return result
        
        if clause.get("type") == "comparison":
            field = clause.get("field", "")
            if field in agg_aliases:
                result.append(clause)
        elif clause.get("type") == "logical":
            for sub_clause in clause.get("clauses", []):
                result.extend(self._extract_aggregate_clauses(sub_clause, agg_aliases))
        
        return result

    def _remove_aggregate_clauses(
        self,
        clause: Any,
        agg_aliases: Set[str],
    ) -> Optional[Dict[str, Any]]:
        """Remove filter clauses that reference aggregate aliases."""
        if not isinstance(clause, dict):
            return clause
        
        if clause.get("type") == "comparison":
            field = clause.get("field", "")
            if field in agg_aliases:
                return None  # Remove this clause
            return clause
        elif clause.get("type") == "logical":
            new_clauses = []
            for sub_clause in clause.get("clauses", []):
                filtered = self._remove_aggregate_clauses(sub_clause, agg_aliases)
                if filtered is not None:
                    new_clauses.append(filtered)
            
            if not new_clauses:
                return None
            
            return {
                **clause,
                "clauses": new_clauses,
            }
        
        return clause
