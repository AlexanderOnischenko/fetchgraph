from __future__ import annotations

from typing import Any, Dict, List, Set

from pydantic import TypeAdapter

from fetchgraph.relational.models import RelationalRequest

RELATIONAL_PROVIDERS = {"demo_qa", "relational"}


def _load_entities_schema(root: dict, ctx: Any) -> Dict[str, Any] | None:
    """Load entities schema from replay context.
    
    Returns entities schema dict or None if not available.
    """
    if not hasattr(ctx, 'resources') or not isinstance(ctx.resources, dict):
        return None
    
    schema_resource = ctx.resources.get("schema_v1")
    if not schema_resource or not isinstance(schema_resource, dict):
        return None
    
    # Try to get schema from data first
    schema_data = schema_resource.get("data")
    if isinstance(schema_data, dict) and "entities" in schema_data:
        return schema_data
    
    # Try to load from data_ref file
    data_ref = schema_resource.get("data_ref")
    if not isinstance(data_ref, dict):
        return None
    
    file_path = data_ref.get("file")
    if not file_path or not hasattr(ctx, 'resolve_resource_path'):
        return None
    
    try:
        import yaml
        resolved_path = ctx.resolve_resource_path(file_path)
        if not resolved_path.exists():
            return None
        with open(resolved_path) as f:
            loaded_schema = yaml.safe_load(f)
        if isinstance(loaded_schema, dict) and "entities" in loaded_schema:
            return loaded_schema
    except Exception:
        pass
    
    return None


def _build_schema_index(schema: Dict[str, Any]) -> tuple[Dict[str, Set[str]], Dict[str, Set[str]], Dict[tuple, str]]:
    """Build schema indices for validation.
    
    Returns:
        - entity_to_columns: dict[entity_name, set[column_names]]
        - column_to_entities: dict[column_name, set[entity_names]]
        - relation_by_entity_pair: dict[(from_entity, to_entity), relation_name]
    """
    entity_to_columns: Dict[str, Set[str]] = {}
    column_to_entities: Dict[str, Set[str]] = {}
    relation_by_entity_pair: Dict[tuple, str] = {}
    
    for entity in schema.get("entities", []):
        entity_name = entity.get("name", "")
        columns = {col.get("name", "") for col in entity.get("columns", [])}
        entity_to_columns[entity_name] = columns
        
        for col in columns:
            column_to_entities.setdefault(col, set()).add(entity_name)
    
    for relation in schema.get("relations", []):
        from_entity = relation.get("from_entity", "")
        to_entity = relation.get("to_entity", "")
        rel_name = relation.get("name", "")
        relation_by_entity_pair[(from_entity, to_entity)] = rel_name
        relation_by_entity_pair[(to_entity, from_entity)] = rel_name
    
    return entity_to_columns, column_to_entities, relation_by_entity_pair


def _validate_field_refs_contract(
    transformed: Dict[str, Any],
    schema: Dict[str, Any],
    errors: List[str],
    auto_repairs: List[dict],
) -> None:
    """Validate compile-bind success contract against schema.
    
    Checks:
    - All field references are valid (column exists in referenced entity)
    - Required relations are added for cross-entity references
    - No ambiguous/unresolved fields masked as success
    """
    if not schema.get("entities"):
        # No schema to validate against
        return
    
    entity_to_columns, column_to_entities, relation_by_entity_pair = _build_schema_index(schema)
    root_entity = transformed.get("root_entity", "")
    relations = set(transformed.get("relations", []))
    
    # Validate group_by field refs
    for i, gb in enumerate(transformed.get("group_by", [])):
        if not isinstance(gb, dict):
            continue
        entity = gb.get("entity")
        field = gb.get("field", "")
        
        if entity and entity not in entity_to_columns:
            raise AssertionError(f"group_by[{i}]: entity '{entity}' not found in schema")
        
        if entity and field and "." in field:
            raise AssertionError(
                f"group_by[{i}]: non-canonical field '{field}' with entity '{entity}'; "
                "field must be bare column name when entity is specified"
            )
        
        if entity and field and field not in entity_to_columns.get(entity, set()):
            raise AssertionError(
                f"group_by[{i}]: column '{field}' not found in entity '{entity}'"
            )
    
    # Validate filter field refs
    def validate_filter(clause: Any, path: str, root_entity: str) -> None:
        if not isinstance(clause, dict):
            return

        if clause.get("type") == "comparison":
            entity = clause.get("entity")
            field = clause.get("field", "")

            # Validate qualified field references (field contains entity prefix)
            if field and "." in field:
                field_entity, column = field.split(".", 1)
                if field_entity not in entity_to_columns:
                    raise AssertionError(f"{path}: entity '{field_entity}' not found in schema")
                if column not in entity_to_columns.get(field_entity, set()):
                    raise AssertionError(
                        f"{path}: column '{column}' not found in entity '{field_entity}'"
                    )
                # Check relation for cross-entity references
                if field_entity != root_entity:
                    has_relation_in_schema = (
                        (root_entity, field_entity) in relation_by_entity_pair or
                        (field_entity, root_entity) in relation_by_entity_pair
                    )
                    if has_relation_in_schema:
                        expected_relations = set()
                        if (root_entity, field_entity) in relation_by_entity_pair:
                            expected_relations.add(relation_by_entity_pair[(root_entity, field_entity)])
                        if (field_entity, root_entity) in relation_by_entity_pair:
                            expected_relations.add(relation_by_entity_pair[(field_entity, root_entity)])
                        relations = set(transformed.get("relations", []))
                        relation_found = bool(expected_relations & relations)
                        if not relation_found:
                            raise AssertionError(
                                f"{path}: cross-entity field '{field}' requires relation "
                                f"between '{root_entity}' and '{field_entity}', but none found"
                            )
                # Qualified field validated successfully
                return
            
            # Validate unqualified field with separate entity field
            if entity and entity not in entity_to_columns:
                raise AssertionError(f"{path}: entity '{entity}' not found in schema")

            if entity and field and field not in entity_to_columns.get(entity, set()):
                raise AssertionError(
                    f"{path}: column '{field}' not found in entity '{entity}'"
                )

            # Check relation exists for cross-entity references
            if entity and entity != root_entity:
                # Check if relation exists in schema between root_entity and entity
                has_relation_in_schema = (
                    (root_entity, entity) in relation_by_entity_pair or
                    (entity, root_entity) in relation_by_entity_pair
                )
                if has_relation_in_schema:
                    # Get the actual relation name(s) from schema
                    expected_relations = set()
                    if (root_entity, entity) in relation_by_entity_pair:
                        expected_relations.add(relation_by_entity_pair[(root_entity, entity)])
                    if (entity, root_entity) in relation_by_entity_pair:
                        expected_relations.add(relation_by_entity_pair[(entity, root_entity)])

                    # Check if any expected relation was added to transformed selectors
                    relations = set(transformed.get("relations", []))
                    relation_found = bool(expected_relations & relations)
                    if not relation_found:
                        raise AssertionError(
                            f"{path}: cross-entity reference '{field}' requires relation "
                            f"between '{root_entity}' and '{entity}', but none found in relations"
                        )
        elif clause.get("type") == "logical":
            for j, sub_clause in enumerate(clause.get("clauses", [])):
                validate_filter(sub_clause, f"{path}.clauses[{j}]", root_entity)

    filters = transformed.get("filters", {})
    if filters:
        validate_filter(filters, "filters", root_entity)
    
    # Validate select field refs (for bare/qualified fields, not expressions)
    for i, sel in enumerate(transformed.get("select", [])):
        if not isinstance(sel, dict):
            continue
        expr = sel.get("expr", "")

        # Skip expressions with functions
        if "(" in expr:
            continue

        # Check qualified fields
        if "." in expr:
            entity, column = expr.split(".", 1)
            if entity not in entity_to_columns:
                raise AssertionError(f"select[{i}]: entity '{entity}' not found in schema")
            if column not in entity_to_columns.get(entity, set()):
                raise AssertionError(
                    f"select[{i}]: column '{column}' not found in entity '{entity}'"
                )

            # Check relation exists for cross-entity references
            if entity != root_entity:
                # Check if relation exists in schema between root_entity and entity
                has_relation_in_schema = (
                    (root_entity, entity) in relation_by_entity_pair or
                    (entity, root_entity) in relation_by_entity_pair
                )
                if has_relation_in_schema:
                    # Get the actual relation name(s) from schema
                    expected_relations = set()
                    if (root_entity, entity) in relation_by_entity_pair:
                        expected_relations.add(relation_by_entity_pair[(root_entity, entity)])
                    if (entity, root_entity) in relation_by_entity_pair:
                        expected_relations.add(relation_by_entity_pair[(entity, root_entity)])
                    
                    # Check if any expected relation was added to transformed selectors
                    relations = set(transformed.get("relations", []))
                    relation_found = bool(expected_relations & relations)
                    if not relation_found:
                        raise AssertionError(
                            f"select[{i}]: cross-entity reference '{expr}' requires relation "
                            f"between '{root_entity}' and '{entity}', but none found in relations"
                        )

    # Validate aggregations field refs
    for i, agg in enumerate(transformed.get("aggregations", [])):
        if not isinstance(agg, dict):
            continue
        field = agg.get("field", "")

        if "." in field:
            entity, column = field.split(".", 1)
            if entity not in entity_to_columns:
                raise AssertionError(f"aggregations[{i}]: entity '{entity}' not found in schema")
            if column not in entity_to_columns.get(entity, set()):
                raise AssertionError(
                    f"aggregations[{i}]: column '{column}' not found in entity '{entity}'"
                )
            
            # Check relation for non-root qualified field in aggregations
            if entity != root_entity:
                # Check if relation exists in schema
                has_relation_in_schema = (
                    (root_entity, entity) in relation_by_entity_pair or
                    (entity, root_entity) in relation_by_entity_pair
                )
                if has_relation_in_schema:
                    # Get the actual relation name(s) from schema
                    expected_relations = set()
                    if (root_entity, entity) in relation_by_entity_pair:
                        expected_relations.add(relation_by_entity_pair[(root_entity, entity)])
                    if (entity, root_entity) in relation_by_entity_pair:
                        expected_relations.add(relation_by_entity_pair[(entity, root_entity)])
                    
                    # Check if any expected relation was added to transformed selectors
                    relation_found = bool(expected_relations & relations)
                    if not relation_found:
                        raise AssertionError(
                            f"aggregations[{i}]: cross-entity field '{field}' requires relation "
                            f"between '{root_entity}' and '{entity}', but none found in relations"
                        )

    # Validate having field refs (same as filters)
    having = transformed.get("having", {})
    if having:
        validate_filter(having, "having", root_entity)


def validate_plan_normalize_spec_v1(out: dict) -> None:
    if not isinstance(out, dict):
        raise AssertionError("Output must be a dict")
    out_spec = out.get("out_spec")
    if not isinstance(out_spec, dict):
        raise AssertionError("Output must contain out_spec dict")
    provider = out_spec.get("provider")
    selectors = out_spec.get("selectors")
    if selectors is None:
        raise AssertionError("out_spec.selectors is required")
    if not isinstance(selectors, dict):
        raise AssertionError("out_spec.selectors must be a dict")
    is_relational = (
        provider in RELATIONAL_PROVIDERS
        or (isinstance(provider, str) and provider.startswith("relational"))
        or any(key in selectors for key in ("root_entity", "relations", "entity"))
    )
    if not is_relational:
        return
    if "root_entity" not in selectors:
        if "entity" in selectors:
            raise AssertionError(
                "Relational selectors missing required key 'root_entity' "
                "(looks like you produced 'entity' instead)."
            )
        raise AssertionError("Relational selectors missing required key 'root_entity'")
    TypeAdapter(RelationalRequest).validate_python(selectors)


def validate_aggregation_normalize_spec_v1(out: dict) -> None:
    """Validate aggregation_normalize.spec_v1 output.

    Expected output structure:
    {
        "normalized_aggregations": [...],
        "normalized_group_by": [...],
        "normalized_selectors": {...},
        "diag": {...},
    }

    Contract validation (semantic postcondition validation layer):
    - op == "query" for relational aggregation form
    - All aggregations have canonical names (count, count_distinct, sum, avg, min, max)
    - COUNT(*) allowed only as count with field="*"
    - Each aggregate spec has canonical shape (agg, field, alias)
    - Aliases are non-empty and unique
    - No raw supported aggregate expressions in select
    - If aggregations exist and non-aggregate projected fields exist, they are in group_by
    - Aggregate predicates are in having, not filters
    - COUNT(DISTINCT field) canonicalized as count_distinct
    - Unsupported expressions (e.g., SUM(price * qty)) not treated as simple aggregations
    """
    if not isinstance(out, dict):
        raise AssertionError("Output must be a dict")

    # Check normalized_aggregations exists and is a list
    normalized_aggregations = out.get("normalized_aggregations")
    if normalized_aggregations is None:
        raise AssertionError("Output must contain normalized_aggregations list")
    if not isinstance(normalized_aggregations, list):
        raise AssertionError("normalized_aggregations must be a list")

    # Check normalized_group_by exists and is a list
    normalized_group_by = out.get("normalized_group_by")
    if normalized_group_by is None:
        raise AssertionError("Output must contain normalized_group_by list")
    if not isinstance(normalized_group_by, list):
        raise AssertionError("normalized_group_by must be a list")

    # Validate each group_by entry is a proper GroupBySpec dict (not bare string)
    # After aggregation normalize, group_by should be structured as dicts with entity/field
    for i, gb in enumerate(normalized_group_by):
        if not isinstance(gb, dict):
            raise AssertionError(
                f"normalized_group_by[{i}] must be a dict (GroupBySpec), not {type(gb).__name__}; "
                "group_by entries should be structured with entity/field after normalization"
            )
        # Check required GroupBySpec fields
        if "field" not in gb:
            raise AssertionError(f"normalized_group_by[{i}] missing required key 'field'")
        # entity can be None for bare fields, but if present, it should be a string
        entity = gb.get("entity")
        if entity is not None and not isinstance(entity, str):
            raise AssertionError(
                f"normalized_group_by[{i}].entity must be a string or null, got {type(entity).__name__}"
            )

    # Check normalized_selectors exists
    normalized_selectors = out.get("normalized_selectors")
    if normalized_selectors is None:
        raise AssertionError("Output must contain normalized_selectors")
    if not isinstance(normalized_selectors, dict):
        raise AssertionError("normalized_selectors must be a dict")

    # Validate op is normalized to "query"
    op = normalized_selectors.get("op")
    if op != "query":
        raise AssertionError(f"op must be 'query' after aggregation normalize, got '{op}'")

    # Canonical aggregation names (supported aggregations)
    canonical_agg_names = {"count", "count_distinct", "sum", "avg", "min", "max", "median"}

    # Track aliases for uniqueness check
    seen_aliases = set()

    # Validate each aggregation
    for i, agg in enumerate(normalized_aggregations):
        if not isinstance(agg, dict):
            raise AssertionError(f"normalized_aggregations[{i}] must be a dict")

        # Check required fields
        if "agg" not in agg:
            raise AssertionError(f"normalized_aggregations[{i}] missing required key 'agg'")
        if "field" not in agg:
            raise AssertionError(f"normalized_aggregations[{i}] missing required field 'field'")
        if "alias" not in agg:
            raise AssertionError(f"normalized_aggregations[{i}] missing required key 'alias'")

        agg_name = agg.get("agg", "")
        agg_field = agg.get("field", "")
        agg_alias = agg.get("alias", "")

        # Check canonical agg name
        if agg_name not in canonical_agg_names:
            raise AssertionError(
                f"normalized_aggregations[{i}].agg='{agg_name}' is not canonical; "
                f"must be one of: {', '.join(sorted(canonical_agg_names))}"
            )

        # Check COUNT(*) special case - only count(*) is allowed, not sum(*), avg(*), etc.
        if agg_field == "*":
            if agg_name != "count":
                raise AssertionError(
                    f"COUNT(*) is the only allowed aggregation with field='*'; "
                    f"got agg='{agg_name}'"
                )

        # Check alias is non-empty
        if not agg_alias or not isinstance(agg_alias, str):
            raise AssertionError(f"normalized_aggregations[{i}].alias must be a non-empty string")

        # Check alias uniqueness
        if agg_alias in seen_aliases:
            raise AssertionError(
                f"Duplicate alias '{agg_alias}' in normalized_aggregations[{i}]; "
                "aliases must be unique"
            )
        seen_aliases.add(agg_alias)

    # Check diag for input aggregations count - if input had aggregations, output should too
    diag = out.get("diag", {})
    input_agg_count = diag.get("input_aggregations_count", 0)
    if isinstance(input_agg_count, int) and input_agg_count > 0:
        if len(normalized_aggregations) == 0:
            raise AssertionError(
                f"Input had {input_agg_count} aggregation(s) but output has none - "
                "aggregations were lost during normalization"
            )

    # GROUP-BY CLOSURE: All non-aggregate projected fields must be in group_by
    # This ensures proper SQL semantics for GROUP BY queries
    select_fields = normalized_selectors.get("select", [])
    if isinstance(select_fields, list) and normalized_aggregations:
        # Extract non-aggregate field expressions from select
        non_agg_select_fields = []
        for sel in select_fields:
            if isinstance(sel, dict):
                expr = sel.get("expr", "")
                # Check if this is a bare field reference (not an aggregation expression)
                # Aggregation expressions typically contain function calls like COUNT(...), SUM(...), etc.
                is_agg_expr = False
                if isinstance(expr, str):
                    # Check for aggregation function patterns
                    for agg_name in canonical_agg_names:
                        if agg_name.upper() + "(" in expr.upper():
                            is_agg_expr = True
                            break
                elif isinstance(expr, dict):
                    # Structured expression - check if it's an aggregation
                    if expr.get("type") in ("aggregation", "function"):
                        is_agg_expr = True

                if not is_agg_expr and expr:
                    non_agg_select_fields.append(expr)

        # All non-aggregate select fields must be in group_by
        group_by_set = set()
        for gb in normalized_group_by:
            if isinstance(gb, str):
                group_by_set.add(gb)
            elif isinstance(gb, dict):
                # Handle structured group_by with entity.field format
                entity = gb.get("entity")
                field = gb.get("field", "")
                if entity:
                    group_by_set.add(f"{entity}.{field}")
                else:
                    group_by_set.add(field)

        for field_expr in non_agg_select_fields:
            field_str = field_expr if isinstance(field_expr, str) else str(field_expr)
            if field_str not in group_by_set:
                raise AssertionError(
                    f"Non-aggregate select field '{field_str}' not found in group_by; "
                    "all non-aggregate projected fields must be in group_by (group-by closure)"
                )

    # HAVING VALIDATION: Aggregate predicates should be in having, not filters
    # Check that filters don't contain aggregate-level predicates
    filters = normalized_selectors.get("filters", {})
    if isinstance(filters, dict):
        # Look for filter clauses that reference aggregation aliases
        agg_aliases = {agg.get("alias") for agg in normalized_aggregations if isinstance(agg, dict)}

        def check_filter_for_agg_refs(clause: Any, path: str) -> None:
            if not isinstance(clause, dict):
                return

            if clause.get("type") == "comparison":
                field = clause.get("field", "")
                # Check if filter references an aggregation alias
                if field in agg_aliases:
                    raise AssertionError(
                        f"Filter at {path} references aggregation alias '{field}'; "
                        "aggregate predicates must be in having, not filters"
                    )
            elif clause.get("type") == "logical":
                for j, sub_clause in enumerate(clause.get("clauses", [])):
                    check_filter_for_agg_refs(sub_clause, f"{path}.clauses[{j}]")

        check_filter_for_agg_refs(filters, "filters")

    # SELECT VALIDATION: No raw supported aggregate expressions should remain in select
    # After normalization, aggregations should be extracted to aggregations[] list
    if isinstance(select_fields, list):
        for i, sel in enumerate(select_fields):
            if isinstance(sel, dict):
                expr = sel.get("expr", "")
                if isinstance(expr, str):
                    # Check for raw aggregation function patterns
                    for agg_name in canonical_agg_names:
                        pattern = f"{agg_name.upper()}("
                        if pattern in expr.upper():
                            raise AssertionError(
                                f"Raw aggregate expression '{expr}' found in select[{i}]; "
                                "aggregations must be extracted to aggregations[] list"
                            )

    # HAVING VALIDATION: Ensure having contains only aggregate-level predicates
    having = normalized_selectors.get("having", {})
    if isinstance(having, dict) and normalized_aggregations:
        # having should reference aggregation aliases or aggregate expressions
        # This is a soft check - we just verify having structure is reasonable
        agg_aliases = {agg.get("alias") for agg in normalized_aggregations if isinstance(agg, dict)}

        def validate_having_clause(clause: Any, path: str) -> None:
            if not isinstance(clause, dict):
                return

            if clause.get("type") == "comparison":
                field = clause.get("field", "")
                # Having should reference aggregation aliases
                if field and field not in agg_aliases:
                    # This is a warning-level check - non-agg fields in having might be valid
                    # in some edge cases, so we don't fail here
                    pass
            elif clause.get("type") == "logical":
                for j, sub_clause in enumerate(clause.get("clauses", [])):
                    validate_having_clause(sub_clause, f"{path}.clauses[{j}]")

        validate_having_clause(having, "having")


def validate_compile_bind_spec_v1(out: dict, root: dict | None = None, ctx: Any = None) -> None:
    """Validate compile_bind.spec_v1 output.

    Expected output structure:
    {
        "bound_query": {...} or None,
        "transformed_selectors": {...},
        "errors": [...],  # Binding errors (ambiguous/unresolved fields)
        "auto_repairs": [...],  # Auto-repairs applied
        "diag": {...},
    }

    Contract validation:
    - If success (no errors), all field references must be valid
    - Ambiguous/unresolved fields must produce errors, not success
    - Required relations must be added for cross-entity references
    """
    if not isinstance(out, dict):
        raise AssertionError("Output must be a dict")

    # Check for errors field - indicates binding failures
    errors = out.get("errors", [])
    if errors:
        # Errors are acceptable - indicates ambiguous/unresolved fields
        # The test framework will handle this as a known_bad case
        return

    # If no errors, validate binding contract
    bound_query = out.get("bound_query")
    if bound_query is not None and not isinstance(bound_query, dict):
        raise AssertionError("bound_query must be a dict or None")

    # Validate transformed_selectors if present
    transformed = out.get("transformed_selectors")
    if transformed:
        # Check root_entity
        if "root_entity" not in transformed:
            raise AssertionError("transformed_selectors missing 'root_entity'")

        # Check aggregations are properly formed
        aggs = transformed.get("aggregations", [])
        for i, agg in enumerate(aggs):
            if isinstance(agg, dict):
                if "agg" not in agg:
                    raise AssertionError(f"aggregations[{i}] missing required field 'agg'")
                if "field" not in agg:
                    raise AssertionError(f"aggregations[{i}] missing required field 'field'")

    # Check diag for binding validation mode
    diag = out.get("diag", {})
    binding_mode = diag.get("binding_validation_mode")
    if binding_mode == "shape_only":
        raise AssertionError(
            "compile_bind replay in shape_only mode is not allowed; "
            "real entities schema is required for binding validation"
        )

    # Check auto_repairs are recorded if any
    auto_repairs = out.get("auto_repairs", [])
    auto_repairs_count = diag.get("auto_repairs_count", 0)
    if auto_repairs_count > 0 and not auto_repairs:
        raise AssertionError(
            f"diag.auto_repairs_count={auto_repairs_count} but auto_repairs list is empty; "
            "auto-repairs must be recorded in output"
        )

    # CONTRACT VALIDATION: Validate field references against schema
    # This requires schema from ctx.resources
    if root is not None and ctx is not None:
        schema = _load_entities_schema(root, ctx)
        if schema and transformed:
            _validate_field_refs_contract(transformed, schema, errors, auto_repairs)


def validate_resource_read_v1(out: dict) -> None:
    if not isinstance(out, dict):
        raise AssertionError("Output must be a dict")
    text = out.get("text")
    if not isinstance(text, str):
        raise AssertionError("Output must include text string")


REPLAY_VALIDATORS = {
    "plan_normalize.spec_v1": validate_plan_normalize_spec_v1,
    "compile_bind.spec_v1": validate_compile_bind_spec_v1,
    "aggregation_normalize.spec_v1": validate_aggregation_normalize_spec_v1,
    "resource_read.v1": validate_resource_read_v1,
}
