from __future__ import annotations

from pydantic import TypeAdapter

from fetchgraph.relational.models import RelationalRequest


def validate_plan_normalize_spec_v1(out: dict) -> None:
    if not isinstance(out, dict):
        raise AssertionError("Output must be a dict")
    out_spec = out.get("out_spec")
    if not isinstance(out_spec, dict):
        raise AssertionError("Output must contain out_spec dict")
    selectors = out_spec.get("selectors")
    if selectors is None:
        raise AssertionError("out_spec.selectors is required")
    TypeAdapter(RelationalRequest).validate_python(selectors)
