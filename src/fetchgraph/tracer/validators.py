from __future__ import annotations

from pydantic import TypeAdapter

from fetchgraph.relational.models import RelationalRequest

RELATIONAL_PROVIDERS = {"demo_qa", "relational"}


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
        "normalized_group_by": [...]
    }
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
    
    # Validate each aggregation has required fields
    for i, agg in enumerate(normalized_aggregations):
        if not isinstance(agg, dict):
            raise AssertionError(f"normalized_aggregations[{i}] must be a dict")
        if "agg" not in agg:
            raise AssertionError(f"normalized_aggregations[{i}] missing required key 'agg'")
        if "field" not in agg:
            raise AssertionError(f"normalized_aggregations[{i}] missing required key 'field'")
    
    # Check diag for input aggregations count - if input had aggregations, output should too
    diag = out.get("diag", {})
    input_agg_count = diag.get("input_aggregations_count", 0)
    if isinstance(input_agg_count, int) and input_agg_count > 0:
        if len(normalized_aggregations) == 0:
            raise AssertionError(
                f"Input had {input_agg_count} aggregation(s) but output has none - "
                "aggregations were lost during normalization"
            )


def validate_compile_bind_spec_v1(out: dict) -> None:
    """Validate compile_bind.spec_v1 output.
    
    Expected output structure:
    {
        "bound_query": {...} or None
    }
    """
    if not isinstance(out, dict):
        raise AssertionError("Output must be a dict")
    
    # bound_query can be None or a dict
    bound_query = out.get("bound_query")
    if bound_query is not None and not isinstance(bound_query, dict):
        raise AssertionError("bound_query must be a dict or None")


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
