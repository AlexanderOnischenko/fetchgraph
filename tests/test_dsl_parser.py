import pytest

from fetchgraph.dsl import Diagnostics, parse_query_sketch


def test_parse_unquoted_keys_trailing_commas():
    src = "{ from: streams, where: [], }"
    parsed, diags = parse_query_sketch(src)
    assert parsed.data["from"] == "streams"
    assert parsed.data["where"] == []
    assert isinstance(diags, Diagnostics)
    assert not diags.has_errors()


def test_parse_wrap_missing_braces():
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
