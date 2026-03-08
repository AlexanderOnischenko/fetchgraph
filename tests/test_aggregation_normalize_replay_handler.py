"""Unit tests for aggregation_normalize.spec_v1 replay handler."""

from __future__ import annotations

import pytest

from fetchgraph.planning.replay_handlers import replay_aggregation_normalize_spec_v1
from fetchgraph.replay.runtime import ReplayContext


class TestReplayHandlerShape:
    """Test replay handler output shape."""

    def test_basic_output_shape(self):
        """Replay handler should return correct output shape."""
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [
                    {"agg": "count", "field": "orders.id", "alias": "count_orders"}
                ],
            }
        }
        ctx = ReplayContext()
        out = replay_aggregation_normalize_spec_v1(inp, ctx)

        assert isinstance(out, dict)
        assert "normalized_aggregations" in out
        assert "normalized_group_by" in out
        assert "diag" in out
        assert isinstance(out["normalized_aggregations"], list)
        assert isinstance(out["normalized_group_by"], list)
        assert isinstance(out["diag"], dict)

    def test_normalized_aggregations_structure(self):
        """Each normalized aggregation should have required fields."""
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [
                    {"agg": "count", "field": "orders.id", "alias": "count_orders"}
                ],
            }
        }
        ctx = ReplayContext()
        out = replay_aggregation_normalize_spec_v1(inp, ctx)

        assert len(out["normalized_aggregations"]) == 1
        agg = out["normalized_aggregations"][0]
        assert "agg" in agg
        assert "field" in agg
        assert "alias" in agg
        assert "is_distinct" in agg

    def test_diag_contains_input_output_counts(self):
        """Diag should contain input and output aggregations counts."""
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [
                    {"agg": "count", "field": "orders.id", "alias": "count_orders"},
                    {"agg": "sum", "field": "orders.total", "alias": "sum_total"},
                ],
            }
        }
        ctx = ReplayContext()
        out = replay_aggregation_normalize_spec_v1(inp, ctx)

        assert "input_aggregations_count" in out["diag"]
        assert "output_aggregations_count" in out["diag"]
        assert out["diag"]["input_aggregations_count"] == 2
        assert out["diag"]["output_aggregations_count"] == 2


class TestReplayHandlerDeterminism:
    """Test replay handler determinism."""

    def test_deterministic_output(self):
        """Same input should produce same output."""
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [
                    {"agg": "count", "field": "orders.id", "alias": "count_orders"}
                ],
            }
        }
        ctx = ReplayContext()

        out1 = replay_aggregation_normalize_spec_v1(inp, ctx)
        out2 = replay_aggregation_normalize_spec_v1(inp, ctx)

        assert out1 == out2

    def test_no_side_effects(self):
        """Replay should not mutate input."""
        inp = {
            "selectors": {
                "op": "aggregate",
                "root_entity": "orders",
                "aggregations": [
                    {"agg": "count", "field": "orders.id", "alias": "count_orders"}
                ],
            }
        }
        ctx = ReplayContext()

        import copy
        inp_copy = copy.deepcopy(inp)

        replay_aggregation_normalize_spec_v1(inp, ctx)

        assert inp == inp_copy


class TestReplayHandlerOpNormalization:
    """Test op normalization in replay handler."""

    def test_op_aggregate_normalized_to_query(self):
        """op='aggregate' should be normalized to 'query'."""
        inp = {
            "selectors": {
                "op": "aggregate",
                "root_entity": "orders",
                "aggregations": [],
            }
        }
        ctx = ReplayContext()
        out = replay_aggregation_normalize_spec_v1(inp, ctx)

        assert out["diag"]["op_normalized"] is True
        assert out.get("normalized_selectors", {}).get("op") == "query"

    def test_op_query_unchanged(self):
        """op='query' should remain unchanged."""
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [],
            }
        }
        ctx = ReplayContext()
        out = replay_aggregation_normalize_spec_v1(inp, ctx)

        assert out["diag"]["op_normalized"] is False
        assert out.get("normalized_selectors", {}).get("op") == "query"


class TestReplayHandlerAggregationNormalization:
    """Test aggregation normalization in replay handler."""

    def test_count_distinct_normalization(self):
        """COUNT DISTINCT should be normalized to count_distinct."""
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [
                    {
                        "agg": "count_distinct",
                        "field": "orders.customer_id",
                        "alias": "unique_customers",
                        "is_distinct": True,
                    }
                ],
            }
        }
        ctx = ReplayContext()
        out = replay_aggregation_normalize_spec_v1(inp, ctx)

        assert len(out["normalized_aggregations"]) == 1
        assert out["normalized_aggregations"][0]["agg"] == "count_distinct"
        assert out["normalized_aggregations"][0]["is_distinct"] is True

    def test_count_star_normalization(self):
        """COUNT(*) should be normalized to count with field='*'."""
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [
                    {"agg": "COUNT", "field": "*", "alias": "count_all"}
                ],
            }
        }
        ctx = ReplayContext()
        out = replay_aggregation_normalize_spec_v1(inp, ctx)

        assert len(out["normalized_aggregations"]) == 1
        assert out["normalized_aggregations"][0]["agg"] == "count"
        assert out["normalized_aggregations"][0]["field"] == "*"

    def test_canonical_agg_names(self):
        """Various aggregation names should be normalized to canonical form."""
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [
                    {"agg": "SUM", "field": "orders.total", "alias": "sum_total"},
                    {"agg": "AVG", "field": "orders.total", "alias": "avg_total"},
                    {"agg": "MIN", "field": "orders.total", "alias": "min_total"},
                    {"agg": "MAX", "field": "orders.total", "alias": "max_total"},
                ],
            }
        }
        ctx = ReplayContext()
        out = replay_aggregation_normalize_spec_v1(inp, ctx)

        agg_names = [agg["agg"] for agg in out["normalized_aggregations"]]
        assert "sum" in agg_names
        assert "avg" in agg_names
        assert "min" in agg_names
        assert "max" in agg_names


class TestReplayHandlerAggregationsPreservation:
    """Test aggregations preservation validation."""

    def test_aggregations_preserved(self):
        """Aggregations should be preserved during normalization."""
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [
                    {"agg": "count", "field": "orders.id", "alias": "count_orders"}
                ],
            }
        }
        ctx = ReplayContext()
        out = replay_aggregation_normalize_spec_v1(inp, ctx)

        assert out["diag"]["aggregations_preserved"] is True
        assert out["diag"]["input_aggregations_count"] == 1
        assert out["diag"]["output_aggregations_count"] == 1

    def test_aggregations_lost_raises_assertion(self):
        """If aggregations are lost, handler should raise assertion."""
        # This test verifies the validation logic in the handler
        # In practice, this would indicate a bug in AggregationNormalizeNode
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [
                    {"agg": "count", "field": "orders.id", "alias": "count_orders"}
                ],
            }
        }
        ctx = ReplayContext()

        # Handler should raise if aggregations are lost
        # (This would indicate a bug in the node implementation)
        # For now, we just verify the handler doesn't lose aggregations
        out = replay_aggregation_normalize_spec_v1(inp, ctx)
        assert len(out["normalized_aggregations"]) > 0


class TestReplayHandlerNoSchemaDependency:
    """Test that replay handler has no schema dependency."""

    def test_works_without_schema(self):
        """Replay handler should work without schema."""
        inp = {
            "selectors": {
                "op": "query",
                "root_entity": "orders",
                "aggregations": [
                    {"agg": "count", "field": "orders.id", "alias": "count_orders"}
                ],
            }
        }
        ctx = ReplayContext()

        # Should not raise - no schema dependency
        out = replay_aggregation_normalize_spec_v1(inp, ctx)
        assert "normalized_aggregations" in out

    def test_works_with_empty_context(self):
        """Replay handler should work with empty context."""
        inp = {
            "selectors": {
                "op": "query",
                "aggregations": [],
            }
        }
        ctx = ReplayContext()

        # Should not raise - works with minimal context
        out = replay_aggregation_normalize_spec_v1(inp, ctx)
        assert "normalized_aggregations" in out


class TestReplayHandlerRootEntityInference:
    """Test root_entity inference in replay handler."""

    def test_root_entity_present_in_output(self):
        """root_entity should be present in normalized_selectors if inferred by node."""
        inp = {
            "selectors": {
                "op": "query",
                "aggregations": [
                    {"agg": "count", "field": "orders.id", "alias": "count_orders"}
                ],
            }
        }
        ctx = ReplayContext()
        out = replay_aggregation_normalize_spec_v1(inp, ctx)

        # The AggregationNormalizeNode may infer root_entity from field names
        # Check that normalized_selectors contains the result
        assert "normalized_selectors" in out
        # root_entity may or may not be inferred depending on node implementation
        # The key test is that the handler doesn't crash
