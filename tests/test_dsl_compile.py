import pytest

from fetchgraph.dsl import compile_relational_query, parse_and_normalize
from fetchgraph.relational.models import ComparisonFilter, LogicalFilter


def test_not_operator_inverts_equals_after_mapping():
    sketch, _ = parse_and_normalize({"from": "streams", "where": {"not": ["status", "active"]}})
    compiled = compile_relational_query(sketch)

    assert compiled.filters is not None
    assert compiled.filters.model_dump() == ComparisonFilter(
        entity=None, field="status", op="!=", value="active"
    ).model_dump()


def test_not_operator_inverts_between_into_or_bounds():
    sketch, _ = parse_and_normalize(
        {"from": "streams", "where": {"not": ["date", ["2020-01-01", "2020-12-31"]]}}
    )
    compiled = compile_relational_query(sketch)

    assert compiled.filters is not None
    assert compiled.filters.model_dump() == LogicalFilter(
        op="or",
        clauses=[
            ComparisonFilter(entity=None, field="date", op="<", value="2020-01-01"),
            ComparisonFilter(entity=None, field="date", op=">", value="2020-12-31"),
        ],
    ).model_dump()


def test_wildcard_select_results_in_empty_select_list():
    sketch, _ = parse_and_normalize({"from": "streams", "get": ["*"]})
    compiled = compile_relational_query(sketch)

    assert compiled.select == []


def test_relations_inferred_from_dotted_get():
    sketch, _ = parse_and_normalize({"from": "streams", "get": ["participant_details.name"]})
    compiled = compile_relational_query(sketch)

    assert "participant_details" in compiled.relations


def test_multi_hop_dotted_path_raises():
    sketch, _ = parse_and_normalize({"from": "streams", "get": ["a.b.c"]})
    with pytest.raises(ValueError):
        compile_relational_query(sketch)
