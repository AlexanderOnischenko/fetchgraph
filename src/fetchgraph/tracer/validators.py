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
    if provider not in RELATIONAL_PROVIDERS:
        return
    if "root_entity" not in selectors:
        if "entity" in selectors:
            raise AssertionError(
                "Relational selectors missing required key 'root_entity' "
                "(looks like you produced 'entity' instead)."
            )
        raise AssertionError("Relational selectors missing required key 'root_entity'")
    TypeAdapter(RelationalRequest).validate_python(selectors)
