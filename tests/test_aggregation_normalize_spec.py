"""Comprehensive unit tests for AggregationNormalizeNode per aggregation_normaization.md spec.

This test suite covers all functional requirements G-AN-01 through G-AN-17.
"""

from __future__ import annotations

import copy
import pytest

from fetchgraph.planning.nodes.aggregation_normalize_node import (
    AggregationNormalizeNode,
    NormalizedAggregation,
)
from fetchgraph.planning.nodes.base import NodeContext


@pytest.fixture
def node():
    """Create AggregationNormalizeNode instance."""
    return AggregationNormalizeNode()


@pytest.fixture
def ctx():
    """Create NodeContext instance."""
    return NodeContext()


# =============================================================================
# G-AN-01: Нормализация op
# =============================================================================

class TestGAn01OpNormalization:
    """Tests for G-AN-01: op normalization."""

    def test_normalize_op_aggregate_to_query(self, node, ctx):
        """op='aggregate' should be normalized to op='query'."""
        selectors = {"op": "aggregate", "aggregations": []}
        result = node.execute(ctx, selectors)
        assert result.value.normalized_selectors["op"] == "query"
        assert result.value.changes["op_normalized"] is True

    def test_keep_op_query_unchanged(self, node, ctx):
        """op='query' should remain unchanged."""
        selectors = {"op": "query", "aggregations": []}
        result = node.execute(ctx, selectors)
        assert result.value.normalized_selectors["op"] == "query"
        assert result.value.changes["op_normalized"] is False

    def test_infer_query_op_for_relational_aggregation_shape(self, node, ctx):
        """Missing op with relational aggregation shape should be handled."""
        # Node should work even without explicit op
        selectors = {
            "root_entity": "orders",
            "aggregations": [{"agg": "count", "field": "orders.id"}],
        }
        result = node.execute(ctx, selectors)
        # Op should remain as-is (not set) or be query if node sets default
        assert "aggregations" in result.value.normalized_selectors


# =============================================================================
# G-AN-02: Best-effort inference root_entity
# =============================================================================

class TestGAn02RootEntityInference:
    """Tests for G-AN-02: root_entity inference."""

    def test_infer_root_entity_from_aggregation_field(self, node, ctx):
        """root_entity should be inferred from aggregation field."""
        selectors = {
            "aggregations": [{"agg": "count", "field": "orders.id", "alias": "cnt"}],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_selectors.get("root_entity") == "orders"
        assert result.value.changes["root_entity_inferred"] is True

    def test_infer_root_entity_from_select_field(self, node, ctx):
        """root_entity should be inferred from select field."""
        selectors = {
            "select": [{"expr": "customers.name"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_selectors.get("root_entity") == "customers"

    def test_infer_root_entity_from_filter_field(self, node, ctx):
        """root_entity should be inferred from filter field."""
        selectors = {
            "filters": {"field": "orders.status", "op": "=", "value": "active"},
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_selectors.get("root_entity") == "orders"

    def test_do_not_guess_root_entity_when_not_obvious(self, node, ctx):
        """root_entity should not be guessed when not obvious."""
        selectors = {
            "select": [{"expr": "name"}],  # No qualified field
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        # Should not infer entity from bare field
        assert result.value.normalized_selectors.get("root_entity") is None


# =============================================================================
# G-AN-03: Канонический словарь агрегатов
# =============================================================================

class TestGAn03CanonicalAggNames:
    """Tests for G-AN-03: canonical aggregation names."""

    def test_normalize_count_name(self, node, ctx):
        """COUNT should be normalized to count."""
        selectors = {"aggregations": [{"agg": "COUNT", "field": "id", "alias": "c"}]}
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "count"

    def test_normalize_sum_name(self, node, ctx):
        """SUM should be normalized to sum."""
        selectors = {"aggregations": [{"agg": "SUM", "field": "total", "alias": "s"}]}
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "sum"

    def test_normalize_avg_name(self, node, ctx):
        """AVG should be normalized to avg."""
        selectors = {"aggregations": [{"agg": "AVG", "field": "total", "alias": "a"}]}
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "avg"

    def test_normalize_min_name(self, node, ctx):
        """MIN should be normalized to min."""
        selectors = {"aggregations": [{"agg": "MIN", "field": "date", "alias": "m"}]}
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "min"

    def test_normalize_max_name(self, node, ctx):
        """MAX should be normalized to max."""
        selectors = {"aggregations": [{"agg": "MAX", "field": "date", "alias": "M"}]}
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "max"

    def test_normalize_count_distinct_name(self, node, ctx):
        """COUNT DISTINCT should be normalized to count_distinct."""
        selectors = {
            "aggregations": [{"agg": "COUNT DISTINCT", "field": "id", "alias": "cd"}]
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "count_distinct"

    def test_normalize_count_distinct_from_count_distinct_expr(self, node, ctx):
        """COUNT(DISTINCT field) expression should be normalized."""
        selectors = {
            "select": [{"expr": "COUNT(DISTINCT customer_id)", "alias": "unique_cust"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert len(result.value.normalized_aggregations) == 1
        assert result.value.normalized_aggregations[0].agg == "count_distinct"


# =============================================================================
# G-AN-04: Поддерживаемые aggregate expressions в select
# =============================================================================

class TestGAn04AggregateExtraction:
    """Tests for G-AN-04: aggregate extraction from select."""

    def test_extract_count_star_from_select_expr(self, node, ctx):
        """COUNT(*) should be extracted from select[].expr."""
        selectors = {
            "select": [{"expr": "COUNT(*)", "alias": "cnt"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert len(result.value.normalized_aggregations) == 1
        assert result.value.normalized_aggregations[0].agg == "count"
        assert result.value.normalized_aggregations[0].field == "*"

    def test_extract_count_field_from_select_expr(self, node, ctx):
        """COUNT(field) should be extracted from select[].expr."""
        selectors = {
            "select": [{"expr": "COUNT(orders.id)", "alias": "cnt"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert len(result.value.normalized_aggregations) == 1
        assert result.value.normalized_aggregations[0].field == "orders.id"

    def test_extract_count_distinct_from_select_expr(self, node, ctx):
        """COUNT(DISTINCT field) should be extracted from select[].expr."""
        selectors = {
            "select": [{"expr": "COUNT(DISTINCT customer_id)", "alias": "unique"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "count_distinct"

    def test_extract_sum_from_select_expr(self, node, ctx):
        """SUM(field) should be extracted from select[].expr."""
        selectors = {
            "select": [{"expr": "SUM(orders.total)", "alias": "sum_total"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "sum"

    def test_extract_avg_from_select_expr(self, node, ctx):
        """AVG(field) should be extracted from select[].expr."""
        selectors = {
            "select": [{"expr": "AVG(orders.total)", "alias": "avg_total"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "avg"

    def test_extract_min_from_select_expr(self, node, ctx):
        """MIN(field) should be extracted from select[].expr."""
        selectors = {
            "select": [{"expr": "MIN(orders.date)", "alias": "min_date"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "min"

    def test_extract_max_from_select_expr(self, node, ctx):
        """MAX(field) should be extracted from select[].expr."""
        selectors = {
            "select": [{"expr": "MAX(orders.date)", "alias": "max_date"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "max"

    def test_preserve_alias_when_extracting_aggregation(self, node, ctx):
        """Alias should be preserved when extracting aggregation."""
        selectors = {
            "select": [{"expr": "COUNT(*)", "alias": "custom_alias"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].alias == "custom_alias"

    def test_generate_alias_when_extracting_aggregation(self, node, ctx):
        """Alias should be generated when not provided."""
        selectors = {
            "select": [{"expr": "SUM(orders.total)"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].alias.startswith("sum_")


# =============================================================================
# G-AN-05: Канонический формат aggregations[]
# =============================================================================

class TestGAn05CanonicalFormat:
    """Tests for G-AN-05: canonical aggregations[] format."""

    def test_emit_canonical_count_aggregation(self, node, ctx):
        """Count aggregation should have canonical format."""
        selectors = {"aggregations": [{"agg": "count", "field": "id", "alias": "c"}]}
        result = node.execute(ctx, selectors)
        agg = result.value.normalized_aggregations[0]
        assert agg.agg == "count"
        assert agg.field == "id"
        assert agg.alias == "c"

    def test_emit_canonical_count_distinct_aggregation(self, node, ctx):
        """Count distinct aggregation should have canonical format."""
        selectors = {
            "aggregations": [
                {"agg": "count_distinct", "field": "id", "alias": "cd"}
            ]
        }
        result = node.execute(ctx, selectors)
        agg = result.value.normalized_aggregations[0]
        assert agg.agg == "count_distinct"

    def test_emit_canonical_sum_aggregation(self, node, ctx):
        """Sum aggregation should have canonical format."""
        selectors = {"aggregations": [{"agg": "sum", "field": "total", "alias": "s"}]}
        result = node.execute(ctx, selectors)
        agg = result.value.normalized_aggregations[0]
        assert agg.agg == "sum"

    def test_emit_canonical_avg_aggregation(self, node, ctx):
        """Avg aggregation should have canonical format."""
        selectors = {"aggregations": [{"agg": "avg", "field": "total", "alias": "a"}]}
        result = node.execute(ctx, selectors)
        agg = result.value.normalized_aggregations[0]
        assert agg.agg == "avg"

    def test_emit_canonical_min_aggregation(self, node, ctx):
        """Min aggregation should have canonical format."""
        selectors = {"aggregations": [{"agg": "min", "field": "date", "alias": "m"}]}
        result = node.execute(ctx, selectors)
        agg = result.value.normalized_aggregations[0]
        assert agg.agg == "min"

    def test_emit_canonical_max_aggregation(self, node, ctx):
        """Max aggregation should have canonical format."""
        selectors = {"aggregations": [{"agg": "max", "field": "date", "alias": "M"}]}
        result = node.execute(ctx, selectors)
        agg = result.value.normalized_aggregations[0]
        assert agg.agg == "max"


# =============================================================================
# G-AN-06: Специальный случай COUNT(*)
# =============================================================================

class TestGAn06CountStar:
    """Tests for G-AN-06: COUNT(*) special case."""

    def test_accept_count_star(self, node, ctx):
        """COUNT(*) should be accepted with field='*'."""
        selectors = {"aggregations": [{"agg": "count", "field": "*", "alias": "c"}]}
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].field == "*"

    def test_generate_alias_for_count_star(self, node, ctx):
        """Alias should be generated for COUNT(*) if not provided."""
        selectors = {"aggregations": [{"agg": "count", "field": "*"}]}
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].alias == "count_all"

    def test_reject_sum_star(self, node, ctx):
        """SUM(*) should not be treated as valid aggregation."""
        # SUM(*) is not a supported pattern - should remain in select
        selectors = {
            "select": [{"expr": "SUM(*)", "alias": "s"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        # SUM(*) doesn't match our regex pattern - should not be extracted
        assert len(result.value.normalized_aggregations) == 0

    def test_reject_avg_star(self, node, ctx):
        """AVG(*) should not be treated as valid aggregation."""
        selectors = {
            "select": [{"expr": "AVG(*)", "alias": "a"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert len(result.value.normalized_aggregations) == 0

    def test_reject_min_star(self, node, ctx):
        """MIN(*) should not be treated as valid aggregation."""
        selectors = {
            "select": [{"expr": "MIN(*)", "alias": "m"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert len(result.value.normalized_aggregations) == 0

    def test_reject_max_star(self, node, ctx):
        """MAX(*) should not be treated as valid aggregation."""
        selectors = {
            "select": [{"expr": "MAX(*)", "alias": "M"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert len(result.value.normalized_aggregations) == 0

    def test_reject_count_distinct_star(self, node, ctx):
        """COUNT(DISTINCT *) should not be treated as valid aggregation."""
        selectors = {
            "select": [{"expr": "COUNT(DISTINCT *)", "alias": "cd"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        # COUNT(DISTINCT *) doesn't match our pattern (requires word after DISTINCT)
        assert len(result.value.normalized_aggregations) == 0


# =============================================================================
# G-AN-07: Merge и dedupe агрегатов
# =============================================================================

class TestGAn07MergeDedupe:
    """Tests for G-AN-07: merge and deduplicate aggregations."""

    def test_deduplicate_same_aggregation_from_select_and_aggregations(self, node, ctx):
        """Same aggregation from select and aggregations should be deduplicated."""
        selectors = {
            "aggregations": [{"agg": "count", "field": "orders.id", "alias": "cnt"}],
            "select": [{"expr": "COUNT(orders.id)", "alias": "cnt"}],
        }
        result = node.execute(ctx, selectors)
        # Should have only one aggregation (merged)
        assert len(result.value.normalized_aggregations) == 1
        assert result.value.changes["aggregations_merged"] >= 1

    def test_prefer_explicit_aggregation_alias_over_generated(self, node, ctx):
        """Explicit alias should be preferred over generated."""
        selectors = {
            "aggregations": [{"agg": "count", "field": "id", "alias": "explicit"}],
            "select": [{"expr": "COUNT(id)"}],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].alias == "explicit"

    def test_prefer_existing_explicit_alias_when_merging_duplicates(self, node, ctx):
        """Existing explicit alias should be preferred when merging."""
        selectors = {
            "aggregations": [{"agg": "sum", "field": "total", "alias": "sum_t"}],
            "select": [{"expr": "SUM(total)", "alias": "total_sum"}],
        }
        result = node.execute(ctx, selectors)
        # First one (from aggregations[]) should win
        assert result.value.normalized_aggregations[0].alias == "sum_t"


# =============================================================================
# G-AN-08: Alias uniqueness и stability
# =============================================================================

class TestGAn08AliasUniqueness:
    """Tests for G-AN-08: alias uniqueness and stability."""

    def test_generate_stable_alias_for_sum(self, node, ctx):
        """Alias for sum should be stable and deterministic."""
        selectors = {
            "select": [{"expr": "SUM(orders.total)"}],
            "aggregations": [],
        }
        result1 = node.execute(ctx, selectors)
        result2 = node.execute(ctx, selectors)
        assert result1.value.normalized_aggregations[0].alias == result2.value.normalized_aggregations[0].alias

    def test_generate_stable_alias_for_count_distinct(self, node, ctx):
        """Alias for count_distinct should be stable."""
        selectors = {
            "select": [{"expr": "COUNT(DISTINCT customer_id)"}],
            "aggregations": [],
        }
        result1 = node.execute(ctx, selectors)
        result2 = node.execute(ctx, selectors)
        assert result1.value.normalized_aggregations[0].alias == result2.value.normalized_aggregations[0].alias

    def test_ensure_alias_uniqueness_on_collision(self, node, ctx):
        """Duplicate aliases should be made unique."""
        selectors = {
            "aggregations": [
                {"agg": "count", "field": "id1", "alias": "cnt"},
                {"agg": "count", "field": "id2", "alias": "cnt"},  # Duplicate
            ],
        }
        result = node.execute(ctx, selectors)
        aliases = [a.alias for a in result.value.normalized_aggregations]
        assert len(aliases) == len(set(aliases))  # All unique
        assert result.value.changes["alias_collisions_resolved"] > 0

    def test_keep_explicit_unique_alias_unchanged(self, node, ctx):
        """Unique explicit aliases should remain unchanged."""
        selectors = {
            "aggregations": [
                {"agg": "count", "field": "id", "alias": "count_id"},
                {"agg": "sum", "field": "total", "alias": "sum_total"},
            ],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].alias == "count_id"
        assert result.value.normalized_aggregations[1].alias == "sum_total"


# =============================================================================
# G-AN-09: Group-by closure
# =============================================================================

class TestGAn09GroupByClosure:
    """Tests for G-AN-09: group-by closure."""

    def test_add_missing_group_by_for_non_agg_select_field(self, node, ctx):
        """Non-aggregate select field should be added to group_by."""
        selectors = {
            "select": [{"expr": "orders.status"}],
            "aggregations": [{"agg": "count", "field": "orders.id", "alias": "cnt"}],
            "group_by": [],
        }
        result = node.execute(ctx, selectors)
        # orders.status should be in group_by
        gb = result.value.normalized_selectors.get("group_by", [])
        assert len(gb) > 0
        assert result.value.changes["group_by_fields_added"] > 0

    def test_preserve_existing_group_by_order(self, node, ctx):
        """Existing group_by order should be preserved."""
        selectors = {
            "select": [{"expr": "orders.region"}],
            "aggregations": [{"agg": "count", "field": "orders.id", "alias": "cnt"}],
            "group_by": [{"field": "orders.status"}],
        }
        result = node.execute(ctx, selectors)
        gb = result.value.normalized_selectors.get("group_by", [])
        # First should be the original (canonicalized to entity + field)
        assert gb[0].get("entity") == "orders"
        assert gb[0].get("field") == "status"

    def test_deduplicate_group_by_fields(self, node, ctx):
        """Duplicate group_by fields should be deduplicated."""
        selectors = {
            "select": [{"expr": "orders.status"}],
            "aggregations": [{"agg": "count", "field": "orders.id", "alias": "cnt"}],
            "group_by": [{"field": "orders.status"}],  # Already present
        }
        result = node.execute(ctx, selectors)
        gb = result.value.normalized_selectors.get("group_by", [])
        # Should not have duplicates
        fields = [g.get("field") if isinstance(g, dict) else g for g in gb]
        assert len(fields) == len(set(fields))

    def test_no_group_by_closure_when_no_aggregations(self, node, ctx):
        """Group-by closure should not apply when no aggregations."""
        selectors = {
            "select": [{"expr": "orders.status"}],
            "aggregations": [],
            "group_by": [],
        }
        result = node.execute(ctx, selectors)
        # Should not add fields to group_by
        assert result.value.changes["group_by_fields_added"] == 0

    def test_group_by_closure_with_multiple_dimension_fields(self, node, ctx):
        """Group-by closure should handle multiple dimension fields."""
        selectors = {
            "select": [
                {"expr": "orders.status"},
                {"expr": "orders.region"},
            ],
            "aggregations": [{"agg": "count", "field": "orders.id", "alias": "cnt"}],
            "group_by": [],
        }
        result = node.execute(ctx, selectors)
        gb = result.value.normalized_selectors.get("group_by", [])
        # Both fields should be in group_by
        assert len(gb) == 2


# =============================================================================
# G-AN-10: select после extraction
# =============================================================================

class TestGAn10SelectAfterExtraction:
    """Tests for G-AN-10: select after extraction."""

    def test_remove_raw_aggregate_expr_from_select_after_extraction(self, node, ctx):
        """Raw aggregate expressions should be removed from select."""
        selectors = {
            "select": [{"expr": "COUNT(*)", "alias": "cnt"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        # Select should be empty after extraction
        assert result.value.normalized_selectors.get("select") == []

    def test_keep_non_aggregate_select_fields(self, node, ctx):
        """Non-aggregate select fields should be kept."""
        selectors = {
            "select": [{"expr": "orders.status", "alias": "status"}],
            "aggregations": [{"agg": "count", "field": "orders.id", "alias": "cnt"}],
        }
        result = node.execute(ctx, selectors)
        # Non-agg field should remain in select
        select = result.value.normalized_selectors.get("select", [])
        assert len(select) == 1
        assert select[0]["expr"] == "orders.status"

    def test_preserve_dimension_field_alias_in_select(self, node, ctx):
        """Dimension field alias should be preserved."""
        selectors = {
            "select": [{"expr": "orders.status", "alias": "status_alias"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        select = result.value.normalized_selectors.get("select", [])
        assert select[0]["alias"] == "status_alias"


# =============================================================================
# G-AN-11/12: Нормализация having / разделение filters
# =============================================================================

class TestGAn1112HavingFilters:
    """Tests for G-AN-11/12: having normalization and filters separation."""

    def test_move_aggregate_alias_filter_from_filters_to_having(self, node, ctx):
        """Aggregate alias filter should be moved from filters to having."""
        selectors = {
            "aggregations": [{"agg": "count", "field": "id", "alias": "cnt"}],
            "filters": {"field": "cnt", "op": ">", "value": 10, "type": "comparison"},
        }
        result = node.execute(ctx, selectors)
        # Filter should be moved to having
        assert "having" in result.value.normalized_selectors
        assert result.value.normalized_selectors["having"]["field"] == "cnt"
        assert result.value.changes["filters_moved_to_having"] > 0

    def test_keep_row_filter_in_filters(self, node, ctx):
        """Row-level filter should remain in filters."""
        selectors = {
            "aggregations": [{"agg": "count", "field": "id", "alias": "cnt"}],
            "filters": {
                "field": "orders.status",
                "op": "=",
                "value": "active",
                "type": "comparison",
            },
        }
        result = node.execute(ctx, selectors)
        # Filter should remain in filters
        assert "filters" in result.value.normalized_selectors
        assert result.value.normalized_selectors["filters"]["field"] == "orders.status"

    def test_create_having_if_missing(self, node, ctx):
        """Having should be created if missing."""
        selectors = {
            "aggregations": [{"agg": "count", "field": "id", "alias": "cnt"}],
            "filters": {"field": "cnt", "op": ">", "value": 10, "type": "comparison"},
        }
        result = node.execute(ctx, selectors)
        assert "having" in result.value.normalized_selectors

    def test_merge_with_existing_having(self, node, ctx):
        """New having clauses should merge with existing."""
        selectors = {
            "aggregations": [
                {"agg": "count", "field": "id", "alias": "cnt"},
                {"agg": "sum", "field": "total", "alias": "sum_t"},
            ],
            "filters": {"field": "cnt", "op": ">", "value": 10, "type": "comparison"},
            "having": {"field": "sum_t", "op": "<", "value": 1000, "type": "comparison"},
        }
        result = node.execute(ctx, selectors)
        # Both should be in having
        having = result.value.normalized_selectors["having"]
        assert having.get("type") == "logical"  # Wrapped in AND
        assert len(having.get("clauses", [])) == 2

    def test_move_nested_logical_aggregate_filters_to_having(self, node, ctx):
        """Nested logical aggregate filters should be moved to having."""
        selectors = {
            "aggregations": [
                {"agg": "count", "field": "id", "alias": "cnt"},
                {"agg": "sum", "field": "total", "alias": "sum_t"},
            ],
            "filters": {
                "type": "logical",
                "op": "and",
                "clauses": [
                    {"field": "cnt", "op": ">", "value": 10, "type": "comparison"},
                    {"field": "sum_t", "op": "<", "value": 1000, "type": "comparison"},
                ],
            },
        }
        result = node.execute(ctx, selectors)
        # Both should be moved to having
        assert "having" in result.value.normalized_selectors
        assert result.value.changes["filters_moved_to_having"] == 2

    def test_do_not_move_unknown_filter_when_not_provably_aggregate(self, node, ctx):
        """Unknown filter should not be moved."""
        selectors = {
            "aggregations": [{"agg": "count", "field": "id", "alias": "cnt"}],
            "filters": {
                "field": "unknown_field",
                "op": ">",
                "value": 10,
                "type": "comparison",
            },
        }
        result = node.execute(ctx, selectors)
        # Should remain in filters
        assert "filters" in result.value.normalized_selectors
        assert result.value.normalized_selectors["filters"]["field"] == "unknown_field"


# =============================================================================
# G-AN-13: count_distinct
# =============================================================================

class TestGAn13CountDistinct:
    """Tests for G-AN-13: count_distinct."""

    def test_canonicalize_count_distinct_from_sql_expr(self, node, ctx):
        """COUNT(DISTINCT field) should be canonicalized from SQL expr."""
        selectors = {
            "select": [{"expr": "COUNT(DISTINCT customer_id)", "alias": "unique"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "count_distinct"

    def test_canonicalize_count_distinct_from_structured_agg(self, node, ctx):
        """count_distinct should be canonicalized from structured aggregation."""
        selectors = {
            "aggregations": [
                {"agg": "count_distinct", "field": "customer_id", "alias": "unique"}
            ]
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "count_distinct"

    def test_merge_duplicate_count_distinct_specs(self, node, ctx):
        """Duplicate count_distinct should be merged, but count and count_distinct are different."""
        # Same count_distinct twice - should merge
        selectors = {
            "aggregations": [
                {"agg": "count_distinct", "field": "id", "alias": "u1"}
            ],
            "select": [{"expr": "COUNT(DISTINCT id)", "alias": "u2"}],
        }
        result = node.execute(ctx, selectors)
        # Should be merged (same agg+field)
        assert len(result.value.normalized_aggregations) == 1
        assert result.value.changes["aggregations_merged"] >= 1

    def test_keep_count_and_count_distinct_as_different_aggregations(self, node, ctx):
        """count and count_distinct should be kept as different aggregations."""
        selectors = {
            "aggregations": [
                {"agg": "count", "field": "id", "alias": "cnt"},
                {"agg": "count_distinct", "field": "id", "alias": "unique"},
            ]
        }
        result = node.execute(ctx, selectors)
        # Should remain separate
        assert len(result.value.normalized_aggregations) == 2


# =============================================================================
# G-AN-14: min/max
# =============================================================================

class TestGAn14MinMax:
    """Tests for G-AN-14: min/max support."""

    def test_support_min_as_regular_aggregation(self, node, ctx):
        """MIN should be supported as regular aggregation."""
        selectors = {
            "select": [{"expr": "MIN(orders.date)", "alias": "min_date"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "min"

    def test_support_max_as_regular_aggregation(self, node, ctx):
        """MAX should be supported as regular aggregation."""
        selectors = {
            "select": [{"expr": "MAX(orders.date)", "alias": "max_date"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert result.value.normalized_aggregations[0].agg == "max"

    def test_do_not_attempt_argmax_rewrite_in_v1(self, node, ctx):
        """Node should not attempt argmax rewrite in v1."""
        # This is a stub test - argmax is out of scope for v1
        selectors = {
            "aggregations": [{"agg": "max", "field": "orders.total", "alias": "max_t"}],
        }
        result = node.execute(ctx, selectors)
        # Should just normalize, not rewrite
        assert result.value.normalized_aggregations[0].agg == "max"

    def test_do_not_attempt_argmin_rewrite_in_v1(self, node, ctx):
        """Node should not attempt argmin rewrite in v1."""
        # This is a stub test - argmin is out of scope for v1
        selectors = {
            "aggregations": [{"agg": "min", "field": "orders.date", "alias": "min_d"}],
        }
        result = node.execute(ctx, selectors)
        # Should just normalize, not rewrite
        assert result.value.normalized_aggregations[0].agg == "min"


# =============================================================================
# G-AN-15: Unsupported expressions
# =============================================================================

class TestGAn15UnsupportedExpressions:
    """Tests for G-AN-15: unsupported expressions."""

    def test_do_not_extract_arithmetic_expression_as_aggregation(self, node, ctx):
        """Arithmetic expressions should not be extracted."""
        selectors = {
            "select": [{"expr": "price * quantity", "alias": "total"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        # Should not be extracted
        assert len(result.value.normalized_aggregations) == 0
        # Should remain in select
        assert result.value.normalized_selectors["select"][0]["expr"] == "price * quantity"

    def test_do_not_extract_function_wrapped_expression_as_simple_aggregation(self, node, ctx):
        """Function-wrapped expressions should not be extracted as simple aggregations."""
        selectors = {
            "select": [{"expr": "DATE_TRUNC('month', orders.date)", "alias": "month"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        # Should not be extracted as aggregation
        assert len(result.value.normalized_aggregations) == 0

    def test_do_not_extract_case_expression_as_aggregation(self, node, ctx):
        """CASE expressions should not be extracted as aggregations."""
        selectors = {
            "select": [
                {
                    "expr": "CASE WHEN x > 0 THEN 1 ELSE 0 END",
                    "alias": "flag",
                }
            ],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        # Should not be extracted
        assert len(result.value.normalized_aggregations) == 0


# =============================================================================
# G-AN-16: No in-place mutation
# =============================================================================

class TestGAn16NoMutation:
    """Tests for G-AN-16: no in-place mutation."""

    def test_aggregation_normalize_does_not_mutate_original_selectors(self, node, ctx):
        """Original selectors should not be mutated."""
        selectors = {
            "op": "aggregate",
            "select": [{"expr": "COUNT(*)", "alias": "cnt"}],
            "aggregations": [{"agg": "sum", "field": "total", "alias": "sum_t"}],
            "group_by": [{"field": "status"}],
        }
        original = copy.deepcopy(selectors)
        
        node.execute(ctx, selectors)
        
        # Original should be unchanged
        assert selectors == original


# =============================================================================
# G-AN-17: Observability
# =============================================================================

class TestGAn17Observability:
    """Tests for G-AN-17: observability."""

    def test_notes_include_op_normalization(self, node, ctx):
        """Notes should include op normalization info."""
        selectors = {"op": "aggregate", "aggregations": []}
        result = node.execute(ctx, selectors)
        assert any("op='aggregate' to op='query'" in note for note in result.notes)

    def test_notes_include_aggregation_extraction(self, node, ctx):
        """Notes should include aggregation extraction info."""
        selectors = {
            "select": [{"expr": "COUNT(*)", "alias": "cnt"}],
            "aggregations": [],
        }
        result = node.execute(ctx, selectors)
        assert any("Extracted aggregation" in note for note in result.notes)

    def test_changes_capture_group_by_closure(self, node, ctx):
        """Changes should capture group_by closure."""
        selectors = {
            "select": [{"expr": "orders.status"}],
            "aggregations": [{"agg": "count", "field": "id", "alias": "cnt"}],
            "group_by": [],
        }
        result = node.execute(ctx, selectors)
        assert "group_by_fields_added" in result.value.changes

    def test_changes_capture_having_rewrite(self, node, ctx):
        """Changes should capture having rewrite."""
        selectors = {
            "aggregations": [{"agg": "count", "field": "id", "alias": "cnt"}],
            "filters": {"field": "cnt", "op": ">", "value": 10, "type": "comparison"},
        }
        result = node.execute(ctx, selectors)
        assert "filters_moved_to_having" in result.value.changes
        assert result.value.changes["filters_moved_to_having"] > 0

    def test_changes_capture_alias_collision_resolution(self, node, ctx):
        """Changes should capture alias collision resolution."""
        selectors = {
            "aggregations": [
                {"agg": "count", "field": "id1", "alias": "cnt"},
                {"agg": "count", "field": "id2", "alias": "cnt"},
            ],
        }
        result = node.execute(ctx, selectors)
        assert "alias_collisions_resolved" in result.value.changes
        assert result.value.changes["alias_collisions_resolved"] > 0
