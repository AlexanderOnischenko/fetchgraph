from fetchgraph.dsl import compile_relational_query, parse_and_normalize
from fetchgraph.relational.models import ComparisonFilter


def test_compile_string_edge_ops():
    sketch, _ = parse_and_normalize({"from": "people", "where": [["name", "starts", "Al"]]})
    compiled = compile_relational_query(sketch)

    assert compiled.filters is not None
    assert compiled.filters.model_dump() == ComparisonFilter(
        entity=None, field="name", op="starts", value="Al"
    ).model_dump()

    sketch_ends, _ = parse_and_normalize({"from": "people", "where": [["name", "ends", "ce"]]})
    compiled_ends = compile_relational_query(sketch_ends)

    assert compiled_ends.filters is not None
    assert compiled_ends.filters.model_dump() == ComparisonFilter(
        entity=None, field="name", op="ends", value="ce"
    ).model_dump()


def test_compile_not_contains_maps_to_not_ilike():
    sketch, _ = parse_and_normalize({"from": "people", "where": {"not": ["name", "contains", "x"]}})
    compiled = compile_relational_query(sketch)

    assert compiled.filters is not None
    assert compiled.filters.model_dump() == ComparisonFilter(
        entity=None, field="name", op="not_ilike", value="x"
    ).model_dump()


def test_compile_similar_and_related_fallback_to_ilike():
    sketch_similar, _ = parse_and_normalize({"from": "people", "where": [["name", "similar", "foo"]]})
    compiled_similar = compile_relational_query(sketch_similar)

    assert compiled_similar.filters is not None
    assert compiled_similar.filters.model_dump() == ComparisonFilter(
        entity=None, field="name", op="ilike", value="foo"
    ).model_dump()

    sketch_related, _ = parse_and_normalize({"from": "people", "where": [["name", "related", "foo"]]})
    compiled_related = compile_relational_query(sketch_related)

    assert compiled_related.filters is not None
    assert compiled_related.filters.model_dump() == ComparisonFilter(
        entity=None, field="name", op="ilike", value="foo"
    ).model_dump()
