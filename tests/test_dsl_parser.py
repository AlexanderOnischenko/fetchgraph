import pytest

from fetchgraph.dsl import Diagnostics, parse_query_sketch


def test_parse_unquoted_keys_trailing_commas():
    src = "{ from: streams, where: [], }"
    parsed, diags = parse_query_sketch(src)
    assert parsed.data["from"] == "streams"
    assert parsed.data["where"] == []
    assert isinstance(diags, Diagnostics)
    assert not diags.has_errors()


def test_parse_bare_identifiers_in_arrays():
    src = "{ from: streams, where: [[status, active]] }"
    parsed, diags = parse_query_sketch(src)
    assert parsed.data["where"] == [["status", "active"]]
    assert not diags.has_errors()


def test_parse_unquoted_ops_in_arrays():
    src = "{ from: streams, where: [[amount, gte, 1000], [amount, >=, 5]] }"
    parsed, diags = parse_query_sketch(src)
    assert parsed.data["where"] == [["amount", "gte", 1000], ["amount", ">=", 5]]
    assert not diags.has_errors()


def test_parse_dotted_path_unquoted():
    src = "{ from: streams, where: [[participant_details.name, \"АС ЕСП\"]] }"
    parsed, diags = parse_query_sketch(src)
    assert parsed.data["where"] == [["participant_details.name", "АС ЕСП"]]
    assert not diags.has_errors()


def test_parse_trailing_commas_and_single_quotes():
    src = "{ from: 'streams', where: [[status, 'active',],], }"
    parsed, diags = parse_query_sketch(src)
    assert parsed.data["from"] == "streams"
    assert parsed.data["where"] == [["status", "active"]]
    assert not diags.has_errors()


def test_parse_without_braces_is_recovered():
    src = "from: streams, where: []"
    parsed, diags = parse_query_sketch(src)
    assert parsed.data["from"] == "streams"
    assert parsed.data["where"] == []
    assert not diags.has_errors()


def test_parse_failure_returns_diagnostics_error():
    src = "{ from: streams, where: [ [ ] }"  # mismatched braces
    parsed, diags = parse_query_sketch(src)
    assert diags.has_errors()
    assert parsed.data == {}
    assert any(msg.code == "DSL_PARSE_ERROR" for msg in diags.messages)
