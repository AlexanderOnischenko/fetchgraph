"""Tests for G-4a: Special case COUNT(*) in compile_bind.

According to aggregation_normaization.md and schema_binding.md:
- COUNT(*) is a valid canonical special case
- Other AGG(*) forms are contract errors
"""

from __future__ import annotations

import pytest

from fetchgraph.planning.nodes.compile_bind_node import CompileBindNode


@pytest.fixture
def node_with_schema():
    """Create CompileBindNode with minimal schema for testing."""
    schema = {
        "entities": [
            {
                "name": "orders",
                "columns": [
                    {"name": "order_id"},
                    {"name": "order_total"},
                    {"name": "order_date"},
                    {"name": "status"},
                ],
            },
            {
                "name": "customers",
                "columns": [
                    {"name": "customer_id"},
                    {"name": "city"},
                    {"name": "name"},
                ],
            },
        ],
        "relations": [
            {
                "name": "orders_to_customers",
                "from_entity": "orders",
                "to_entity": "customers",
                "from_column": "customer_id",
                "to_column": "customer_id",
            },
        ],
    }
    return CompileBindNode(schema=schema)


class TestG4aCountStarSpecialCase:
    """Tests for G-4a: Special case COUNT(*)."""

    def test_accept_count_star_special_case(self, node_with_schema):
        """COUNT(*) should be accepted as special case."""
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [{"agg": "count", "field": "*", "alias": "count_all"}],
        }
        
        result = node_with_schema.execute(None, selectors)
        
        # Should succeed without errors
        assert not result.value.errors
        # COUNT(*) should be preserved
        assert len(result.value.transformed_selectors["aggregations"]) == 1
        agg = result.value.transformed_selectors["aggregations"][0]
        assert agg["agg"] == "count"
        assert agg["field"] == "*"
        # Should have observability note
        assert any("COUNT(*)" in note for note in result.notes)
        # Should have auto_repair entry
        assert any(
            repair.get("type") == "count_star_special_case_preserved"
            for repair in result.value.auto_repairs
        )

    def test_do_not_resolve_star_as_schema_column_for_count(self, node_with_schema):
        """COUNT(*) should not attempt to resolve * as schema column."""
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [{"agg": "count", "field": "*", "alias": "cnt"}],
        }
        
        result = node_with_schema.execute(None, selectors)
        
        # Should not produce "column * not found" errors
        assert not any("* not found" in error for error in result.value.errors)
        assert not any("column *" in error for error in result.value.errors)
        # Field should remain as "*"
        agg = result.value.transformed_selectors["aggregations"][0]
        assert agg["field"] == "*"

    def test_error_on_sum_star(self, node_with_schema):
        """SUM(*) should produce contract error."""
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [{"agg": "sum", "field": "*", "alias": "sum_all"}],
        }
        
        result = node_with_schema.execute(None, selectors)
        
        # Should produce error
        assert any("SUM(*)" in error or "sum(*)" in error or "not supported" in error 
                   for error in result.value.errors)

    def test_error_on_avg_star(self, node_with_schema):
        """AVG(*) should produce contract error."""
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [{"agg": "avg", "field": "*", "alias": "avg_all"}],
        }
        
        result = node_with_schema.execute(None, selectors)
        
        # Should produce error
        assert any("AVG(*)" in error or "avg(*)" in error or "not supported" in error 
                   for error in result.value.errors)

    def test_error_on_min_star(self, node_with_schema):
        """MIN(*) should produce contract error."""
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [{"agg": "min", "field": "*", "alias": "min_all"}],
        }
        
        result = node_with_schema.execute(None, selectors)
        
        # Should produce error
        assert any("MIN(*)" in error or "min(*)" in error or "not supported" in error 
                   for error in result.value.errors)

    def test_error_on_max_star(self, node_with_schema):
        """MAX(*) should produce contract error."""
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [{"agg": "max", "field": "*", "alias": "max_all"}],
        }
        
        result = node_with_schema.execute(None, selectors)
        
        # Should produce error
        assert any("MAX(*)" in error or "max(*)" in error or "not supported" in error 
                   for error in result.value.errors)

    def test_error_on_count_distinct_star(self, node_with_schema):
        """COUNT(DISTINCT *) should produce contract error."""
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [
                {"agg": "count_distinct", "field": "*", "alias": "unique_all"}
            ],
        }
        
        result = node_with_schema.execute(None, selectors)
        
        # Should produce error
        assert any(
            "count_distinct(*)" in error.lower() or "not supported" in error 
            for error in result.value.errors
        )

    def test_count_star_with_regular_fields(self, node_with_schema):
        """COUNT(*) should work alongside regular field aggregations."""
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [
                {"agg": "count", "field": "*", "alias": "count_all"},
                {"agg": "sum", "field": "orders.order_total", "alias": "sum_total"},
            ],
        }
        
        result = node_with_schema.execute(None, selectors)
        
        # Should succeed
        assert not result.value.errors
        # Both aggregations should be preserved
        aggs = result.value.transformed_selectors["aggregations"]
        assert len(aggs) == 2
        # COUNT(*) should remain as-is
        count_agg = next(a for a in aggs if a["agg"] == "count")
        assert count_agg["field"] == "*"
        # SUM field may be canonicalized (entity prefix stripped or kept)
        sum_agg = next(a for a in aggs if a["agg"] == "sum")
        assert "order_total" in sum_agg["field"]

    def test_count_star_preserved_in_bound_query(self, node_with_schema):
        """COUNT(*) should be preserved in bound query output."""
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [{"agg": "count", "field": "*", "alias": "count_all"}],
            "select": [],
        }
        
        result = node_with_schema.execute(None, selectors)
        
        # Should succeed without errors
        assert not result.value.errors
        # Bound query should exist
        assert result.value.bound_query is not None
        # COUNT(*) should be in transformed selectors
        aggs = result.value.transformed_selectors["aggregations"]
        assert len(aggs) == 1
        assert aggs[0]["field"] == "*"
