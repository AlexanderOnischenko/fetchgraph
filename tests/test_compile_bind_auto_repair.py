"""Unit tests for CompileBindNode deterministic auto-repair (FR-1 to FR-8)."""

import pytest

from fetchgraph.planning.nodes.compile_bind_node import CompileBindNode


# Demo schema for testing
DEMO_SCHEMA = {
    "entities": [
        {
            "name": "customers",
            "columns": [
                {"name": "customer_id", "pk": True},
                {"name": "name"},
                {"name": "city"},
                {"name": "segment"},
                {"name": "signup_date"},
            ],
        },
        {
            "name": "orders",
            "columns": [
                {"name": "order_id", "pk": True},
                {"name": "customer_id"},
                {"name": "order_date"},
                {"name": "status"},
                {"name": "channel"},
                {"name": "order_total"},
            ],
        },
        {
            "name": "order_items",
            "columns": [
                {"name": "order_item_id", "pk": True},
                {"name": "order_id"},
                {"name": "product_id"},
                {"name": "quantity"},
                {"name": "unit_price"},
                {"name": "line_total"},
            ],
        },
        {
            "name": "products",
            "columns": [
                {"name": "product_id", "pk": True},
                {"name": "name"},
                {"name": "category"},
                {"name": "price"},
                {"name": "in_stock"},
            ],
        },
    ],
    "relations": [
        {
            "name": "orders_to_customers",
            "from_entity": "orders",
            "to_entity": "customers",
            "join": {
                "from_entity": "orders",
                "from_column": "customer_id",
                "to_entity": "customers",
                "to_column": "customer_id",
                "join_type": "left",
            },
        },
        {
            "name": "items_to_orders",
            "from_entity": "order_items",
            "to_entity": "orders",
            "join": {
                "from_entity": "order_items",
                "from_column": "order_id",
                "to_entity": "orders",
                "to_column": "order_id",
                "join_type": "left",
            },
        },
        {
            "name": "items_to_products",
            "from_entity": "order_items",
            "to_entity": "products",
            "join": {
                "from_entity": "order_items",
                "from_column": "product_id",
                "to_entity": "products",
                "to_column": "product_id",
                "join_type": "left",
            },
        },
    ],
}


def _make_node():
    """Create CompileBindNode with demo schema."""
    return CompileBindNode(schema=DEMO_SCHEMA)


class TestSchemaIndexBuilding:
    """Test FR-1: Schema index building."""

    def test_entity_to_columns_built(self):
        node = _make_node()
        node._build_schema_index()
        
        assert "customers" in node._entity_to_columns
        assert "segment" in node._entity_to_columns["customers"]
        assert "order_total" in node._entity_to_columns["orders"]

    def test_adjacency_built(self):
        node = _make_node()
        node._build_schema_index()
        
        # orders is connected to customers
        assert "orders" in node._adjacency_by_entity
        assert "customers" in node._adjacency_by_entity["orders"]
        
        # order_items is connected to orders and products
        assert "order_items" in node._adjacency_by_entity
        assert "orders" in node._adjacency_by_entity["order_items"]
        assert "products" in node._adjacency_by_entity["order_items"]

    def test_relation_by_entity_pair_built(self):
        node = _make_node()
        node._build_schema_index()
        
        assert ("orders", "customers") in node._relation_by_entity_pair
        assert node._relation_by_entity_pair[("orders", "customers")] == "orders_to_customers"


class TestAutoQualifyFields:
    """Test auto-qualification of fields through relations."""

    def test_prefix_strip_repair(self):
        node = _make_node()
        node._build_schema_index()

        # Test qualified field: customers.segment -> segment (if segment exists in customers)
        selectors = {
            "root_entity": "customers",
            "select": [{"expr": "customers.segment"}],
        }

        notes = []
        errors = []
        auto_repairs = []
        node._auto_qualify_fields(selectors, "customers", notes, errors, auto_repairs)

        # Field should remain as-is (already qualified correctly)
        assert selectors["select"][0]["expr"] == "customers.segment"
        assert len(errors) == 0  # No errors for valid qualified field

    def test_1hop_relocation_repair(self):
        node = _make_node()
        node._build_schema_index()

        # Test bare field relocation: city (bare) -> customers.city
        selectors = {
            "root_entity": "orders",
            "select": [{"expr": "city"}],  # Bare field, not qualified
        }

        notes = []
        errors = []
        auto_repairs = []
        node._auto_qualify_fields(selectors, "orders", notes, errors, auto_repairs)

        # Bare field 'city' should be qualified to 'customers.city'
        assert selectors["select"][0]["expr"] == "customers.city"
        assert len(errors) == 0  # No errors for successful repair
        assert any("auto-qualified" in note for note in notes)
        assert "orders_to_customers" in selectors["relations"]

    def test_aggregation_field_repair(self):
        node = _make_node()
        node._build_schema_index()

        selectors = {
            "root_entity": "order_items",
            "aggregations": [{"field": "order_items.order_total", "agg": "sum"}],
        }
        scope = {"order_items", "orders"}

        notes = []
        errors = []
        node._auto_qualify_fields(selectors, "order_items", notes, errors, [])

        assert selectors["aggregations"][0]["field"] == "orders.order_total"
        assert len(errors) == 0  # No errors for successful repair
        assert "items_to_orders" in selectors["relations"]

    def test_filter_field_repair(self):
        node = _make_node()
        node._build_schema_index()

        selectors = {
            "root_entity": "orders",
            "filters": {"type": "comparison", "field": "orders.city", "op": "=", "value": "NYC"},
        }
        scope = {"orders", "customers"}

        notes = []
        errors = []
        node._auto_qualify_fields(selectors, "orders", notes, errors, [])

        assert selectors["filters"]["field"] == "customers.city"
        assert len(errors) == 0  # No errors for successful repair

    def test_same_entity_prefix_strip_actual(self):
        """Test actual same-entity prefix strip: customers.customer_segment -> customers.segment."""
        node = _make_node()
        node._build_schema_index()

        # customers.customer_segment doesn't exist, but customers.segment does
        # This should trigger prefix strip
        selectors = {
            "root_entity": "customers",
            "select": [{"expr": "customers.customer_segment"}],
        }

        notes = []
        errors = []
        auto_repairs = []
        node._auto_qualify_fields(selectors, "customers", notes, errors, auto_repairs)

        # Field should be repaired via prefix strip
        assert selectors["select"][0]["expr"] == "customers.segment"
        assert len(errors) == 0
        assert len(auto_repairs) == 1
        assert auto_repairs[0]["kind"] == "same_entity_prefix_strip"
        assert auto_repairs[0]["from"] == "customers.customer_segment"
        assert auto_repairs[0]["to"] == "customers.segment"

    def test_same_entity_prefix_strip_cross_entity(self):
        """Test same-entity prefix strip works for ANY qualified entity, not just root_entity.
        
        Regression test: customers.customer_segment -> customers.segment should work
        even when root_entity="orders" (not "customers").
        """
        node = _make_node()
        node._build_schema_index()

        # root_entity is "orders", but field is "customers.customer_segment"
        selectors = {
            "root_entity": "orders",
            "select": [{"expr": "customers.customer_segment"}],
        }

        notes = []
        errors = []
        auto_repairs = []
        node._auto_qualify_fields(selectors, "orders", notes, errors, auto_repairs)

        # Field should be repaired via same-entity prefix strip
        assert selectors["select"][0]["expr"] == "customers.segment"
        assert len(errors) == 0
        assert len(auto_repairs) == 1
        assert auto_repairs[0]["kind"] == "same_entity_prefix_strip"


class TestCompileBindResult:
    """Test CompileBindNode execute() with auto-qualification."""

    def test_same_entity_prefix_strip(self):
        """Test same-entity prefix strip: customers.customer_segment -> customers.segment."""
        node = _make_node()
        node._build_schema_index()

        selectors = {
            "root_entity": "customers",
            "select": [{"expr": "customers.customer_segment"}],
        }

        notes = []
        errors = []
        auto_repairs = []
        node._auto_qualify_fields(selectors, "customers", notes, errors, auto_repairs)

        # Field should be repaired via prefix strip
        assert selectors["select"][0]["expr"] == "customers.segment"
        assert len(errors) == 0
        assert len(auto_repairs) == 1
        assert auto_repairs[0]["kind"] == "same_entity_prefix_strip"

    def test_auto_qualify_in_execute(self):
        node = _make_node()

        selectors = {
            "root_entity": "customers",
            "select": [{"expr": "customers.customer_segment"}],
            "filters": {"type": "comparison", "field": "customer_id", "op": "=", "value": 123},
        }

        from fetchgraph.planning.nodes.base import NodeContext
        ctx = NodeContext()
        result = node.execute(ctx, selectors)

        assert result.value is not None
        # Check that customer_segment was auto-qualified to segment
        select_expr = result.value.transformed_selectors["select"][0].get("expr", "")
        assert "segment" in select_expr

    def test_ambiguous_field_generates_error(self):
        """Test that ambiguous field references generate errors."""
        node = _make_node()
        node._build_schema_index()

        # In this schema, there are no truly ambiguous fields
        # Test that non-existent field generates error instead
        selectors = {
            "root_entity": "customers",
            "select": [{"expr": "nonexistent_column"}],
        }

        notes = []
        errors = []
        node._auto_qualify_fields(selectors, "customers", notes, errors, [])

        # Should have an error for non-existent column
        assert len(errors) > 0
        assert any("Column not found" in err for err in errors)

    def test_unresolved_field_generates_error(self):
        """Test that unresolved field references generate errors."""
        node = _make_node()
        node._build_schema_index()

        # 'nonexistent_column' doesn't exist anywhere
        selectors = {
            "root_entity": "customers",
            "select": [{"expr": "customers.nonexistent_column"}],
        }

        notes = []
        errors = []
        node._auto_qualify_fields(selectors, "customers", notes, errors, [])

        # Should have an error for unresolved reference
        assert len(errors) > 0
        assert any("Column not found" in err for err in errors)
        # Error message should reference the entity from the field expression
        assert any("customers" in err and "nonexistent_column" in err for err in errors)


class TestCompileBindIntegration:
    """Integration tests for CompileBindNode with pipeline fallback."""

    def test_deterministic_case_no_retry(self):
        """Test that deterministic auto-repair cases complete without errors."""
        node = _make_node()

        # order_items.order_total -> orders.order_total (auto-qualified via relation)
        selectors = {
            "root_entity": "order_items",
            "aggregations": [{"field": "order_items.order_total", "agg": "sum"}],
        }

        from fetchgraph.planning.nodes.base import NodeContext
        ctx = NodeContext()
        result = node.execute(ctx, selectors)

        # Should succeed with auto-repair
        assert result.value is not None
        assert len(result.value.errors) == 0
        assert result.value.transformed_selectors["aggregations"][0]["field"] == "orders.order_total"

    def test_unresolved_case_generates_compile_error(self):
        """Test that unresolved cases generate compile errors for self-heal/refetch."""
        node = _make_node()
        
        # 'nonexistent_column' doesn't exist
        selectors = {
            "root_entity": "customers",
            "select": [{"expr": "customers.nonexistent_column"}],
        }
        
        from fetchgraph.planning.nodes.base import NodeContext
        ctx = NodeContext()
        result = node.execute(ctx, selectors)
        
        # Should have compile error for unresolved reference
        assert result.value is not None
        assert len(result.value.errors) > 0
        assert any("Column not found" in err for err in result.value.errors)

    def test_having_processed(self):
        """Test that having clause is processed through auto-bind."""
        node = _make_node()
        node._build_schema_index()

        selectors = {
            "root_entity": "orders",
            "aggregations": [{"field": "orders.order_total", "agg": "sum"}],
            "having": {"type": "comparison", "field": "orders.order_total", "op": ">", "value": 1000},
        }

        from fetchgraph.planning.nodes.base import NodeContext
        ctx = NodeContext()
        result = node.execute(ctx, selectors)

        # Having should be processed without errors
        assert result.value is not None
        assert len(result.value.errors) == 0
        # Having field should remain qualified (having doesn't have separate entity field)
        assert result.value.transformed_selectors["having"]["field"] == "orders.order_total"

    def test_cross_entity_field_without_relation_errors(self):
        """Test that cross-entity field without relation produces error (safe-first contract)."""
        node = _make_node()
        node._build_schema_index()

        # products is NOT connected to orders in DEMO_SCHEMA
        # So products.name should error
        selectors = {
            "root_entity": "orders",
            "select": [{"expr": "products.name"}],  # products is not connected to orders
        }

        notes = []
        errors = []
        auto_repairs = []
        node._auto_qualify_fields(selectors, "orders", notes, errors, auto_repairs)

        # Should error because products is not connected to orders
        assert len(errors) > 0
        assert any("Cross-entity field" in err and "requires relation" in err for err in errors)

    def test_cross_entity_field_with_relation_succeeds(self):
        """Test that cross-entity field with relation succeeds."""
        node = _make_node()
        node._build_schema_index()

        # customers IS connected to orders via orders_to_customers
        selectors = {
            "root_entity": "orders",
            "select": [{"expr": "customers.name"}],  # customers is connected to orders
        }

        notes = []
        errors = []
        auto_repairs = []
        node._auto_qualify_fields(selectors, "orders", notes, errors, auto_repairs)

        # Should succeed because orders_to_customers relation exists
        assert len(errors) == 0
        assert selectors["select"][0]["expr"] == "customers.name"

    def test_no_mutation_of_original_selectors(self):
        """Test that execute() does not mutate the original selectors (no-mutation guarantee)."""
        import copy
        node = _make_node()
        node._build_schema_index()

        # Create selectors with nested structures
        original_selectors = {
            "root_entity": "order_items",
            "relations": ["items_to_orders"],
            "aggregations": [{"field": "order_items.order_total", "agg": "sum"}],
            "filters": {"type": "comparison", "field": "orders.status", "op": "=", "value": "pending"},
            "group_by": [{"entity": "orders", "field": "orders.status"}],
            "select": [{"expr": "orders.order_id"}],
        }

        # Make a deep copy to compare later
        original_copy = copy.deepcopy(original_selectors)

        from fetchgraph.planning.nodes.base import NodeContext
        ctx = NodeContext()
        result = node.execute(ctx, original_selectors)

        # Original selectors should NOT be mutated
        assert original_selectors == original_copy, "Original selectors were mutated!"

        # Transformed selectors should have the auto-repairs
        assert result.value is not None
        assert result.value.transformed_selectors["aggregations"][0]["field"] == "orders.order_total"
        # group_by should be canonicalized (bare field when entity is specified)
        assert result.value.transformed_selectors["group_by"][0]["field"] == "status"


class TestAggregationValidator:
    """Test aggregation_normalize validator."""

    def test_valid_aggregation_output(self):
        """Test that valid aggregation output passes validation."""
        from fetchgraph.tracer.validators import validate_aggregation_normalize_spec_v1

        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "order_id", "alias": "count_order_id"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1, "output_aggregations_count": 1},
        }

        # Should not raise
        validate_aggregation_normalize_spec_v1(out)

    def test_canonical_agg_names(self):
        """Test that all canonical aggregation names are accepted."""
        from fetchgraph.tracer.validators import validate_aggregation_normalize_spec_v1

        for agg_name in ["count", "count_distinct", "sum", "avg", "min", "max", "median"]:
            out = {
                "normalized_aggregations": [
                    {"agg": agg_name, "field": "field", "alias": f"{agg_name}_field"}
                ],
                "normalized_group_by": [],
                "normalized_selectors": {"op": "query"},
                "diag": {"input_aggregations_count": 1, "output_aggregations_count": 1},
            }
            # Should not raise for canonical names
            validate_aggregation_normalize_spec_v1(out)

    def test_non_canonical_agg_name_fails(self):
        """Test that non-canonical aggregation names fail validation."""
        from fetchgraph.tracer.validators import validate_aggregation_normalize_spec_v1

        out = {
            "normalized_aggregations": [
                {"agg": "COUNT_DISTINCT", "field": "field", "alias": "count_distinct_field"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1, "output_aggregations_count": 1},
        }

        # Should raise for non-canonical name
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "not canonical" in str(exc_info.value)

    def test_count_star_allowed(self):
        """Test that COUNT(*) is allowed as count with field='*'."""
        from fetchgraph.tracer.validators import validate_aggregation_normalize_spec_v1

        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "*", "alias": "count_all"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1, "output_aggregations_count": 1},
        }

        # Should not raise for COUNT(*)
        validate_aggregation_normalize_spec_v1(out)

    def test_sum_star_not_allowed(self):
        """Test that SUM(*) is not allowed."""
        from fetchgraph.tracer.validators import validate_aggregation_normalize_spec_v1

        out = {
            "normalized_aggregations": [
                {"agg": "sum", "field": "*", "alias": "sum_all"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1, "output_aggregations_count": 1},
        }

        # Should raise for SUM(*)
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "COUNT(*) is the only allowed" in str(exc_info.value)

    def test_alias_uniqueness(self):
        """Test that duplicate aliases fail validation."""
        from fetchgraph.tracer.validators import validate_aggregation_normalize_spec_v1

        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "field1", "alias": "count_all"},
                {"agg": "sum", "field": "field2", "alias": "count_all"},  # Duplicate alias
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 2, "output_aggregations_count": 2},
        }

        # Should raise for duplicate alias
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "Duplicate alias" in str(exc_info.value)

    def test_op_must_be_query(self):
        """Test that op must be 'query' after aggregation normalize."""
        from fetchgraph.tracer.validators import validate_aggregation_normalize_spec_v1

        out = {
            "normalized_aggregations": [],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "aggregate"},  # Should be 'query'
            "diag": {"input_aggregations_count": 0, "output_aggregations_count": 0},
        }

        # Should raise for op != 'query'
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "op must be 'query'" in str(exc_info.value)

    def test_aggregations_preserved(self):
        """Test that aggregations are preserved (input count == output count)."""
        from fetchgraph.tracer.validators import validate_aggregation_normalize_spec_v1

        out = {
            "normalized_aggregations": [],  # Empty output
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1, "output_aggregations_count": 0},  # Lost aggregations
        }

        # Should raise when aggregations are lost
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "aggregations were lost" in str(exc_info.value)


class TestReplayValidation:
    """Test replay validation with schema resources."""

    def test_replay_red_without_real_schema(self):
        """Test that replay fails validation without real entities schema."""
        from fetchgraph.tracer.validators import validate_compile_bind_spec_v1

        # Create output without schema resource
        out = {
            "bound_query": {"root_entity": "orders"},
            "transformed_selectors": {
                "root_entity": "orders",
                "aggregations": [{"field": "order_total", "agg": "sum"}],
            },
            "errors": [],
            "auto_repairs": [],
            "diag": {"binding_validation_mode": "shape_only"},
        }

        # Should fail because shape_only mode is not allowed
        with pytest.raises(AssertionError) as exc_info:
            validate_compile_bind_spec_v1(out, {}, type('Ctx', (), {'resources': {}})())

        assert "shape_only mode is not allowed" in str(exc_info.value)

    def test_schema_resource_contract_mismatch(self):
        """Test that replay fails when field references don't match schema."""
        from fetchgraph.tracer.validators import validate_compile_bind_spec_v1

        # Create output with field that doesn't exist in schema
        out = {
            "bound_query": {"root_entity": "orders"},
            "transformed_selectors": {
                "root_entity": "orders",
                "aggregations": [{"field": "orders.nonexistent_column", "agg": "sum"}],
            },
            "errors": [],
            "auto_repairs": [],
            "diag": {"binding_validation_mode": "full"},
        }

        # Create schema resource
        schema_resource = {
            "data": {
                "entities": [
                    {"name": "orders", "columns": [{"name": "order_id"}, {"name": "order_total"}]},
                ],
                "relations": [],
            }
        }

        # Should fail because nonexistent_column doesn't exist
        with pytest.raises(AssertionError) as exc_info:
            validate_compile_bind_spec_v1(
                out,
                {},
                type('Ctx', (), {'resources': {'schema_v1': schema_resource}})()
            )

        assert "nonexistent_column" in str(exc_info.value)

    def test_cross_entity_without_relation_errors_in_replay(self):
        """Test that cross-entity field without relation errors in replay."""
        from fetchgraph.tracer.validators import validate_compile_bind_spec_v1

        # Create output with cross-entity field but no relation added
        out = {
            "bound_query": {"root_entity": "orders"},
            "transformed_selectors": {
                "root_entity": "orders",
                "relations": [],  # No relations added!
                "select": [{"expr": "customers.name"}],  # customers.name is cross-entity
            },
            "errors": [],
            "auto_repairs": [],
            "diag": {"binding_validation_mode": "full"},
        }

        # Create schema where customers IS connected to orders (relation exists in schema)
        # But the transformed selectors don't include the relation
        schema_resource = {
            "data": {
                "entities": [
                    {"name": "orders", "columns": [{"name": "order_id"}]},
                    {"name": "customers", "columns": [{"name": "name"}]},
                ],
                "relations": [
                    {"name": "orders_to_customers", "from_entity": "orders", "to_entity": "customers"},
                ],
            }
        }

        # Should fail because cross-entity field without relation in transformed selectors
        with pytest.raises(AssertionError) as exc_info:
            validate_compile_bind_spec_v1(
                out,
                {},
                type('Ctx', (), {'resources': {'schema_v1': schema_resource}})()
            )

        assert "requires relation" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
