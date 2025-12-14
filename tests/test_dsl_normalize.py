from typing import cast

from fetchgraph.dsl import Clause, normalize_query_sketch, parse_and_normalize


def test_normalize_key_aliases_defaults():
    src = {"root": "streams", "filter": [["status", "active"]]}
    normalized, diags = normalize_query_sketch(src)
    assert normalized.from_ == "streams"
    assert normalized.get == ["*"]
    assert normalized.with_ == []
    assert normalized.take == 200
    first_clause = normalized.where.all[0]
    assert isinstance(first_clause, Clause)
    assert first_clause.op == "is"
    assert not diags.has_errors()


def test_normalize_where_list_to_all():
    src = {"from": "streams", "where": [["amount", ">", 1000]]}
    normalized, diags = normalize_query_sketch(src)
    assert len(normalized.where.all) == 1
    assert normalized.where.any == []
    assert normalized.where.not_ is None
    first_clause = normalized.where.all[0]
    assert isinstance(first_clause, Clause)
    assert first_clause.op == ">"
    assert not diags.has_errors()


def test_normalize_where_object_all_any_not():
    src = {
        "from": "streams",
        "where": {
            "all": [["amount", ">=", 1000]],
            "any": [["region", "EU"], ["region", "UK"]],
            "not": ["is_test", True],
        },
    }
    normalized, diags = normalize_query_sketch(src)
    assert normalized.where.not_ is not None
    assert isinstance(normalized.where.not_, Clause)
    assert normalized.where.not_.op == "="
    assert normalized.where.not_.path == "is_test"
    assert not diags.has_errors()


def test_auto_op_string_is():
    src = {"from": "streams", "where": [["status", "active"]]}
    normalized, _ = normalize_query_sketch(src)
    clause = normalized.where.all[0]
    assert isinstance(clause, Clause)
    assert clause.op == "is"


def test_auto_op_number_eq():
    src = {"from": "streams", "where": [["count", 10]]}
    normalized, _ = normalize_query_sketch(src)
    clause = normalized.where.all[0]
    assert isinstance(clause, Clause)
    assert clause.op == "="


def test_auto_op_list_in():
    src = {"from": "streams", "where": [["id", [1, 2, 3]]]}  # not dates
    normalized, _ = normalize_query_sketch(src)
    clause = normalized.where.all[0]
    assert isinstance(clause, Clause)
    assert clause.op == "in"


def test_auto_op_between_dates():
    src = {"from": "streams", "where": [["date", ["2020-01-01", "2020-02-01"]]]}
    normalized, _ = normalize_query_sketch(src)
    clause = normalized.where.all[0]
    assert isinstance(clause, Clause)
    assert clause.op == "between"


def test_op_aliases_and_autocorrect():
    src = {"from": "streams", "where": [["amount", "gte", 1000], ["name", "conains", "foo"]]}
    normalized, diags = normalize_query_sketch(src)
    first_clause = normalized.where.all[0]
    second_clause = normalized.where.all[1]
    assert isinstance(first_clause, Clause)
    assert isinstance(second_clause, Clause)
    assert first_clause.op == ">="
    assert second_clause.op == "contains"
    assert any(msg.code == "DSL_OP_AUTOCORRECT" for msg in diags.messages)


def test_defaults_from_spec_applied():
    src = {"from": "streams", "where": []}
    normalized, diags = normalize_query_sketch(src)
    assert normalized.get == ["*"]
    assert normalized.with_ == []
    assert normalized.take == 200
    assert not diags.has_errors()


def test_normalize_operator_aliases_gte_like():
    src = {"from": "streams", "where": [["amount", "gte", 1], ["name", "like", "foo"]]}
    normalized, diags = normalize_query_sketch(src)
    first_clause = normalized.where.all[0]
    second_clause = normalized.where.all[1]
    assert isinstance(first_clause, Clause)
    assert isinstance(second_clause, Clause)
    assert first_clause.op == ">="
    assert second_clause.op == "is"
    assert not diags.has_errors()


def test_operator_autocorrect_is_deterministic():
    src = {"from": "streams", "where": [["name", "contanis", "foo"]]}
    normalized_first, diags_first = normalize_query_sketch(src)

    normalized_second, diags_second = normalize_query_sketch(src)

    first_clause = normalized_first.where.all[0]
    second_clause = normalized_second.where.all[0]
    assert isinstance(first_clause, Clause)
    assert isinstance(second_clause, Clause)
    assert first_clause.op == "contains"
    assert second_clause.op == "contains"
    assert any(msg.code == "DSL_OP_AUTOCORRECT" for msg in diags_first.messages)
    assert any(msg.code == "DSL_OP_AUTOCORRECT" for msg in diags_second.messages)


def test_parse_and_normalize_with_defaults_and_dirty_input():
    src = "{ from: streams, where: [[status, active]], }"
    normalized, diags = parse_and_normalize(src)

    assert normalized.from_ == "streams"
    ops = [cast(Clause, cl).op for cl in normalized.where.all]
    assert ops == ["is"]
    assert normalized.get == ["*"]
    assert normalized.with_ == []
    assert normalized.take == 200
    assert not diags.has_errors()


def test_invalid_take_is_reported_and_defaults_used():
    src = {"from": "streams", "where": [], "take": "many"}
    normalized, diags = normalize_query_sketch(src)

    assert normalized.take == 200
    assert any(msg.code == "DSL_INVALID_TAKE" for msg in diags.messages)


def test_where_object_with_unknown_keys_emits_diagnostics():
    src = {"from": "streams", "where": {"path": "status", "op": "="}}
    normalized, diags = normalize_query_sketch(src)

    assert normalized.where.all == []
    assert normalized.where.any == []
    assert normalized.where.not_ is None
    codes = {msg.code for msg in diags.messages}
    assert "DSL_UNKNOWN_KEY" in codes
    assert "DSL_EMPTY_WHERE_OBJECT" in codes


def test_auto_operator_string_number_array_between_dates():
    src = {
        "from": "streams",
        "where": [
            ["status", "active"],
            ["count", 5],
            ["date", ["2020-01-01", "2020-02-01"]],
        ],
    }
    normalized, diags = normalize_query_sketch(src)
    ops = [cast(Clause, cl).op for cl in normalized.where.all]
    assert ops == ["is", "=", "between"]
    assert not diags.has_errors()
