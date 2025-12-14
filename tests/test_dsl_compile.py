import pytest

from fetchgraph.dsl import compile_relational_query, parse_and_normalize
from fetchgraph.relational.models import ComparisonFilter, LogicalFilter


def test_is_operator_maps_to_equals():
    sketch, _ = parse_and_normalize("{from: streams, where: [[participant, 'АС ЕСП']]}")
    compiled = compile_relational_query(sketch)

    assert compiled.filters is not None
    assert compiled.filters.model_dump() == ComparisonFilter(
        entity=None, field="participant", op="=", value="АС ЕСП"
    ).model_dump()


def test_between_operator_expands_to_range():
    sketch, _ = parse_and_normalize("{from: streams, where: [[date, [\"2020-01-01\", \"2020-02-01\"]]]}")
    compiled = compile_relational_query(sketch)

    assert compiled.filters is not None
    assert compiled.filters.model_dump() == LogicalFilter(
        op="and",
        clauses=[
            ComparisonFilter(entity=None, field="date", op=">=", value="2020-01-01"),
            ComparisonFilter(entity=None, field="date", op="<=", value="2020-02-01"),
        ],
    ).model_dump()


def test_not_operator_inverts_simple_clause():
    sketch, _ = parse_and_normalize({"from": "streams", "where": {"not": ["is_test", True]}})
    compiled = compile_relational_query(sketch)

    assert compiled.filters is not None
    assert compiled.filters.model_dump() == ComparisonFilter(
        entity=None, field="is_test", op="!=", value=True
    ).model_dump()


def test_relations_inferred_from_dotted_get():
    sketch, _ = parse_and_normalize({"from": "streams", "get": ["participant_details.name"]})
    compiled = compile_relational_query(sketch)

    assert "participant_details" in compiled.relations


def test_multi_hop_dotted_path_raises():
    sketch, _ = parse_and_normalize({"from": "streams", "get": ["a.b.c"]})
    with pytest.raises(ValueError):
        compile_relational_query(sketch)
