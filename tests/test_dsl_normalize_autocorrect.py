from fetchgraph.dsl import normalize_query_sketch


def test_operator_autocorrect_deterministic():
    src = {"from": "streams", "where": [["name", "contanis", "foo"]]}
    normalized_first, diags_first = normalize_query_sketch(src)

    src_again = {"from": "streams", "where": [["name", "contanis", "foo"]]}
    normalized_second, diags_second = normalize_query_sketch(src_again)

    assert normalized_first.where.all[0].op == "contains"
    assert normalized_second.where.all[0].op == "contains"
    assert any(msg.code == "DSL_OP_AUTOCORRECT" for msg in diags_first.messages)
    assert any(msg.code == "DSL_OP_AUTOCORRECT" for msg in diags_second.messages)
