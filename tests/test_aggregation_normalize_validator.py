"""Unit tests for aggregation_normalize.spec_v1 validator.

Tests the semantic postcondition validation layer for aggregation normalization.
"""

from __future__ import annotations

import pytest

from fetchgraph.tracer.validators import validate_aggregation_normalize_spec_v1


class TestCanonicalAggNames:
    """Test canonical aggregation names validation."""

    def test_valid_count(self):
        """Valid count aggregation should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)  # Should not raise

    def test_valid_sum(self):
        """Valid sum aggregation should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "sum", "field": "orders.total", "alias": "sum_total"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_valid_avg(self):
        """Valid avg aggregation should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "avg", "field": "orders.total", "alias": "avg_total"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_valid_min_max(self):
        """Valid min/max aggregations should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "min", "field": "orders.total", "alias": "min_total"},
                {"agg": "max", "field": "orders.total", "alias": "max_total"},
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 2},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_valid_count_distinct(self):
        """Valid count_distinct aggregation should pass."""
        out = {
            "normalized_aggregations": [
                {
                    "agg": "count_distinct",
                    "field": "orders.customer_id",
                    "alias": "count_distinct_customers",
                }
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_invalid_agg_name(self):
        """Non-canonical aggregation name should fail."""
        out = {
            "normalized_aggregations": [
                {"agg": "COUNT", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "not canonical" in str(exc_info.value)

    def test_invalid_custom_agg_function(self):
        """Custom aggregation function should fail."""
        out = {
            "normalized_aggregations": [
                {"agg": "stddev", "field": "orders.total", "alias": "stddev_total"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "not canonical" in str(exc_info.value)


class TestCountStar:
    """Test COUNT(*) special case validation."""

    def test_valid_count_star(self):
        """COUNT(*) as count with field='*' should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "*", "alias": "count_all"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_invalid_sum_star(self):
        """SUM(*) should fail - only COUNT(*) is allowed."""
        out = {
            "normalized_aggregations": [
                {"agg": "sum", "field": "*", "alias": "sum_all"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "COUNT(*) is the only allowed aggregation with field='*'" in str(
            exc_info.value
        )

    def test_invalid_avg_star(self):
        """AVG(*) should fail - only COUNT(*) is allowed."""
        out = {
            "normalized_aggregations": [
                {"agg": "avg", "field": "*", "alias": "avg_all"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "COUNT(*) is the only allowed aggregation with field='*'" in str(
            exc_info.value
        )


class TestAliasValidation:
    """Test alias validation."""

    def test_valid_alias(self):
        """Valid non-empty alias should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_empty_alias_fails(self):
        """Empty alias should fail."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": ""}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "alias must be a non-empty string" in str(exc_info.value)

    def test_missing_alias_fails(self):
        """Missing alias should fail."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "missing required key 'alias'" in str(exc_info.value)

    def test_duplicate_alias_fails(self):
        """Duplicate aliases should fail."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "cnt"},
                {"agg": "sum", "field": "orders.total", "alias": "cnt"},
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 2},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "Duplicate alias" in str(exc_info.value)
        assert "aliases must be unique" in str(exc_info.value)


class TestGroupByClosure:
    """Test group-by closure validation."""

    def test_non_agg_field_in_group_by_passes(self):
        """Non-aggregate select field in group_by should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [
                {"entity": "orders", "field": "status"}
            ],
            "normalized_selectors": {
                "op": "query",
                "select": [
                    {"expr": "orders.status"},
                ],
            },
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_non_agg_field_not_in_group_by_fails(self):
        """Non-aggregate select field not in group_by should fail."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {
                "op": "query",
                "select": [
                    {"expr": "orders.status"},
                    {"expr": "COUNT(orders.id)"},
                ],
            },
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "not found in group_by" in str(exc_info.value)
        assert "group-by closure" in str(exc_info.value)

    def test_multiple_non_agg_fields_all_in_group_by(self):
        """Multiple non-aggregate fields all in group_by should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "sum", "field": "orders.total", "alias": "sum_total"}
            ],
            "normalized_group_by": [
                {"entity": "orders", "field": "status"},
                {"entity": "orders", "field": "region"},
            ],
            "normalized_selectors": {
                "op": "query",
                "select": [
                    {"expr": "orders.status"},
                    {"expr": "orders.region"},
                ],
            },
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)


class TestHavingVsFilters:
    """Test aggregate predicates in having vs filters."""

    def test_agg_alias_in_filters_fails(self):
        """Filter referencing aggregation alias should fail."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {
                "op": "query",
                "filters": {
                    "type": "comparison",
                    "field": "count_orders",
                    "op": ">",
                    "value": 10,
                },
            },
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "references aggregation alias" in str(exc_info.value)
        assert "must be in having, not filters" in str(exc_info.value)

    def test_agg_alias_in_having_passes(self):
        """Aggregate predicate in having should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {
                "op": "query",
                "having": {
                    "type": "comparison",
                    "field": "count_orders",
                    "op": ">",
                    "value": 10,
                },
            },
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_regular_field_in_filters_passes(self):
        """Regular field in filters should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {
                "op": "query",
                "filters": {
                    "type": "comparison",
                    "field": "orders.status",
                    "op": "=",
                    "value": "completed",
                },
            },
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)


class TestRawAggregateExpressions:
    """Test no raw aggregate expressions in select."""

    def test_raw_count_in_select_fails(self):
        """Raw COUNT() in select should fail."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {
                "op": "query",
                "select": [
                    {"expr": "COUNT(orders.id)"},
                ],
            },
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "Raw aggregate expression" in str(exc_info.value)
        assert "must be extracted to aggregations[] list" in str(exc_info.value)

    def test_raw_sum_in_select_fails(self):
        """Raw SUM() in select should fail."""
        out = {
            "normalized_aggregations": [
                {"agg": "sum", "field": "orders.total", "alias": "sum_total"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {
                "op": "query",
                "select": [
                    {"expr": "SUM(orders.total)"},
                ],
            },
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "Raw aggregate expression" in str(exc_info.value)

    def test_extracted_aggregation_passes(self):
        """Extracted aggregation (no raw expr in select) should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {
                "op": "query",
                "select": [],
            },
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)


class TestAggregationsPreservation:
    """Test that aggregations are preserved during normalization."""

    def test_aggregations_lost_fails(self):
        """Input aggregations lost during normalization should fail."""
        out = {
            "normalized_aggregations": [],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 3},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "aggregations were lost during normalization" in str(exc_info.value)

    def test_aggregations_preserved_passes(self):
        """Aggregations preserved should pass."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)


class TestOpNormalization:
    """Test op normalization validation."""

    def test_op_query_passes(self):
        """op='query' should pass."""
        out = {
            "normalized_aggregations": [],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 0},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_op_aggregate_fails(self):
        """op='aggregate' should fail - must be normalized to 'query'."""
        out = {
            "normalized_aggregations": [],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "aggregate"},
            "diag": {"input_aggregations_count": 0},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "op must be 'query'" in str(exc_info.value)


class TestRequiredFields:
    """Test required fields validation."""

    def test_missing_agg_field_fails(self):
        """Missing 'agg' field in aggregation should fail."""
        out = {
            "normalized_aggregations": [
                {"field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "missing required key 'agg'" in str(exc_info.value)

    def test_missing_field_field_fails(self):
        """Missing 'field' in aggregation should fail."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "missing required field 'field'" in str(exc_info.value)

    def test_missing_normalized_aggregations_fails(self):
        """Missing normalized_aggregations should fail."""
        out = {
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "must contain normalized_aggregations list" in str(exc_info.value)

    def test_missing_normalized_group_by_fails(self):
        """Missing normalized_group_by should fail."""
        out = {
            "normalized_aggregations": [],
            "normalized_selectors": {"op": "query"},
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "must contain normalized_group_by list" in str(exc_info.value)

    def test_missing_normalized_selectors_fails(self):
        """Missing normalized_selectors should fail."""
        out = {
            "normalized_aggregations": [],
            "normalized_group_by": [],
        }
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(out)
        assert "must contain normalized_selectors" in str(exc_info.value)


class TestEdgeCases:
    """Test edge cases."""

    def test_no_aggregations_is_valid(self):
        """Query with no aggregations should be valid."""
        out = {
            "normalized_aggregations": [],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 0},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_multiple_aggregations_all_validated(self):
        """Multiple aggregations should all be validated."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"},
                {"agg": "sum", "field": "orders.total", "alias": "sum_total"},
                {"agg": "avg", "field": "orders.total", "alias": "avg_total"},
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 3},
        }
        validate_aggregation_normalize_spec_v1(out)

    def test_median_aggregation_valid(self):
        """Median aggregation should be valid."""
        out = {
            "normalized_aggregations": [
                {"agg": "median", "field": "orders.total", "alias": "median_total"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        validate_aggregation_normalize_spec_v1(out)
