"""Integration tests for aggregation_normalize export/replay workflow.

Tests the full workflow:
1. Export replay case from events
2. Run replay handler
3. Validate with aggregation contract validator
4. Verify known_bad fixtures fail validation
5. Verify fixed fixtures pass validation
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

import fetchgraph.planning.replay_handlers  # noqa: F401
import fetchgraph.tracer.handlers  # noqa: F401
from fetchgraph.replay.runtime import load_case_bundle, run_case
from fetchgraph.tracer.validators import REPLAY_VALIDATORS, validate_aggregation_normalize_spec_v1


def create_test_case_bundle(
    selectors: dict,
    normalized_aggregations: list,
    normalized_group_by: list,
    normalized_selectors: dict,
    diag: dict,
) -> dict:
    """Create a test case bundle payload."""
    return {
        "schema": "fetchgraph.tracer.case_bundle",
        "v": 1,
        "root": {
            "type": "replay_case",
            "v": 2,
            "id": "aggregation_normalize.spec_v1",
            "input": {"selectors": selectors},
            "observed": {
                "normalized_aggregations": normalized_aggregations,
                "normalized_group_by": normalized_group_by,
                "normalized_selectors": normalized_selectors,
                "diag": diag,
            },
            "meta": {
                "provider": "relational",
                "case_id": "test_case",
            },
        },
        "resources": {},
        "extras": {},
    }


def write_case_bundle(tmp_path: Path, payload: dict) -> Path:
    """Write a case bundle to a temp directory."""
    case_path = tmp_path / "test_agg.case.json"
    case_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return case_path


class TestIntegrationExportReplay:
    """Integration tests for export/replay workflow."""

    def test_replay_handler_produces_valid_output(self, tmp_path: Path):
        """Replay handler should produce output that passes validator."""
        # Create a valid case bundle
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
        }
        payload = create_test_case_bundle(
            selectors=selectors,
            normalized_aggregations=[
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            normalized_group_by=[],
            normalized_selectors={"op": "query", "root_entity": "orders"},
            diag={"input_aggregations_count": 1, "output_aggregations_count": 1},
        )

        case_path = write_case_bundle(tmp_path, payload)
        root, ctx = load_case_bundle(case_path)
        out = run_case(root, ctx)

        # Validator should pass
        validate_aggregation_normalize_spec_v1(out)

    def test_known_bad_fixture_fails_validator(self, tmp_path: Path):
        """Known bad fixture (contract violation) should fail validator.
        
        This test directly creates invalid output to prove that the validator
        catches contract violations, not relying on the handler which now fixes issues.
        """
        # Create output with group-by closure violation (bypassing handler fix)
        # This simulates what a buggy handler would produce
        buggy_out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],  # Missing 'orders.status' - violates closure!
            "normalized_selectors": {
                "op": "query",
                "root_entity": "orders",
                "select": [
                    {"expr": "orders.status"},  # Non-agg field not in group_by
                ],
            },
            "diag": {"input_aggregations_count": 1, "output_aggregations_count": 1},
        }

        # Validator should FAIL - this proves the contract validation works
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(buggy_out)

        assert "group-by closure" in str(exc_info.value)

    def test_fixed_fixture_passes_validator(self, tmp_path: Path):
        """Fixed fixture (contract compliant) should pass validator."""
        # Create a case bundle that satisfies the contract:
        # No non-aggregate select fields (so group-by closure is trivially satisfied)
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
        }
        payload = create_test_case_bundle(
            selectors=selectors,
            normalized_aggregations=[
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            normalized_group_by=[],  # No non-agg fields, so empty is fine
            normalized_selectors={
                "op": "query",
                "root_entity": "orders",
            },
            diag={"input_aggregations_count": 1, "output_aggregations_count": 1},
        )

        case_path = write_case_bundle(tmp_path, payload)
        root, ctx = load_case_bundle(case_path)
        out = run_case(root, ctx)

        # Validator should PASS
        validate_aggregation_normalize_spec_v1(out)

    def test_aggregate_predicate_in_filters_fails(self, tmp_path: Path):
        """Aggregate predicate in filters (not having) should fail validator.
        
        This test directly creates invalid output to prove validator catches
        aggregate predicates in wrong place.
        """
        # Create output with aggregate predicate in filters (bypassing handler fix)
        buggy_out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {
                "op": "query",
                "root_entity": "orders",
                "filters": {
                    "type": "comparison",
                    "field": "count_orders",  # References aggregation alias!
                    "op": ">",
                    "value": 10,
                },
            },
            "diag": {"input_aggregations_count": 1, "output_aggregations_count": 1},
        }

        # Validator should FAIL - aggregate predicate in filters
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(buggy_out)

        assert "must be in having, not filters" in str(exc_info.value)

    def test_op_not_normalized_fails(self, tmp_path: Path):
        """op='aggregate' (not normalized to 'query') should fail validator.
        
        Note: The replay handler normalizes op before returning output,
        so this test directly validates the validator's behavior with
        non-normalized input (simulating a buggy handler implementation).
        """
        # Direct validator test - simulating buggy handler that didn't normalize op
        buggy_out = {
            "normalized_aggregations": [],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "aggregate", "root_entity": "orders"},  # Not normalized!
            "diag": {"input_aggregations_count": 0, "output_aggregations_count": 0},
        }

        # Validator should FAIL - op not normalized
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(buggy_out)
        
        assert "op must be 'query'" in str(exc_info.value)

    def test_count_star_valid(self, tmp_path: Path):
        """COUNT(*) should be valid."""
        selectors = {
            "op": "query",
            "root_entity": "orders",
            "aggregations": [
                {"agg": "count", "field": "*", "alias": "count_all"}
            ],
        }
        payload = create_test_case_bundle(
            selectors=selectors,
            normalized_aggregations=[
                {"agg": "count", "field": "*", "alias": "count_all"}
            ],
            normalized_group_by=[],
            normalized_selectors={"op": "query", "root_entity": "orders"},
            diag={"input_aggregations_count": 1, "output_aggregations_count": 1},
        )

        case_path = write_case_bundle(tmp_path, payload)
        root, ctx = load_case_bundle(case_path)
        out = run_case(root, ctx)

        # Validator should PASS
        validate_aggregation_normalize_spec_v1(out)

    def test_sum_star_invalid(self, tmp_path: Path):
        """SUM(*) should be invalid (only COUNT(*) is allowed).
        
        Note: AGG(*) forms are now rejected by the node, so this test
        verifies that the validator catches such invalid output.
        """
        # Create invalid output directly (node would reject SUM(*))
        buggy_out = {
            "normalized_aggregations": [
                {"agg": "sum", "field": "*", "alias": "sum_all"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query", "root_entity": "orders"},
            "diag": {"input_aggregations_count": 1, "output_aggregations_count": 1},
        }

        # Validator should FAIL
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(buggy_out)

        assert "COUNT(*) is the only allowed aggregation with field='*'" in str(exc_info.value)

    def test_duplicate_aliases_invalid(self, tmp_path: Path):
        """Duplicate aliases should be invalid.
        
        This test directly creates invalid output to prove validator catches
        duplicate aliases (which the handler now automatically fixes).
        """
        # Create output with duplicate aliases (bypassing handler fix)
        buggy_out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "cnt"},
                {"agg": "sum", "field": "orders.total", "alias": "cnt"},  # Duplicate!
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query", "root_entity": "orders"},
            "diag": {"input_aggregations_count": 2, "output_aggregations_count": 2},
        }

        # Validator should FAIL
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(buggy_out)

        assert "Duplicate alias" in str(exc_info.value)
        assert "aliases must be unique" in str(exc_info.value)


class TestIntegrationValidatorRegistration:
    """Test that validator is properly registered."""

    def test_validator_registered(self):
        """Validator should be registered in REPLAY_VALIDATORS."""
        assert "aggregation_normalize.spec_v1" in REPLAY_VALIDATORS
        assert REPLAY_VALIDATORS["aggregation_normalize.spec_v1"] == validate_aggregation_normalize_spec_v1

    def test_validator_callable_with_replay_output(self):
        """Validator should be callable with replay output."""
        out = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],
            "normalized_selectors": {"op": "query"},
            "diag": {"input_aggregations_count": 1},
        }
        # Should not raise
        validate_aggregation_normalize_spec_v1(out)


class TestIntegrationExpectedVsObserved:
    """Test that buggy observed output doesn't automatically become expected."""

    def test_buggy_observed_doesnt_become_golden_standard(self, tmp_path: Path):
        """Prove that buggy observed output is not accepted as golden standard.
        
        This test demonstrates the key requirement: we can't just compare
        replay output with observed output - we need semantic contract validation.
        """
        # Create a buggy observed output (violates group-by closure)
        buggy_observed = {
            "normalized_aggregations": [
                {"agg": "count", "field": "orders.id", "alias": "count_orders"}
            ],
            "normalized_group_by": [],  # Missing 'orders.status'
            "normalized_selectors": {
                "op": "query",
                "select": [{"expr": "orders.status"}],
            },
            "diag": {"input_aggregations_count": 1},
        }

        # If we only compared with observed, this would pass
        # But with contract validation, it fails
        with pytest.raises(AssertionError) as exc_info:
            validate_aggregation_normalize_spec_v1(buggy_observed)
        
        assert "group-by closure" in str(exc_info.value)
        # This proves: buggy observed ≠ valid expected
