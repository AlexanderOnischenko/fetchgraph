import pytest

pytest.importorskip("pandas", reason="pandas dependency required for package import")

from fetchgraph.relational_base import RelationalDataProvider


def test_normalize_string_basic():
    assert RelationalDataProvider._normalize_string("  Marketing  ") == "marketing"


def test_normalize_string_internal_whitespace():
    assert RelationalDataProvider._normalize_string("Foo   Bar\nBaz") == "foo bar baz"


def test_normalize_string_non_string_values():
    assert RelationalDataProvider._normalize_string(123) == "123"
    assert RelationalDataProvider._normalize_string(None) == "none"
