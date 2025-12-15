from fetchgraph.dsl import (
    BoundClause,
    BoundWhereExpr,
    Clause,
    NormalizedQuerySketch,
    WhereExpr,
    bound_from_normalized,
    compile_relational_selectors,
    normalized_from_bound,
)


def test_bound_roundtrip_simple_where_and_get():
    sketch = NormalizedQuerySketch(
        from_="fbs",
        where=WhereExpr(all=[Clause(path="system_name", op="contains", value="ЕСП")]),
        get=["bc_guid", "fbs_as.system_name"],
        with_=[],
        take=100,
    )

    bound = bound_from_normalized(sketch)
    restored = normalized_from_bound(bound)

    assert bound.get[0].raw == "bc_guid"
    assert bound.get[1].qualifier == "fbs_as"
    assert bound.get[1].field == "system_name"
    assert bound.where.all[0].field.raw == "system_name"

    assert restored == sketch

    selectors_original = compile_relational_selectors(sketch)
    selectors_roundtrip = compile_relational_selectors(restored)

    assert selectors_roundtrip == selectors_original


def test_bound_roundtrip_nested_groups():
    sketch = NormalizedQuerySketch(
        from_="root",
        where=WhereExpr(
            all=[
                Clause(path="a", op="is", value=1),
                WhereExpr(any=[Clause(path="b.c", op="is", value="x")]),
            ],
            not_=Clause(path="x.y.z", op="contains", value="q"),
        ),
        get=["field"],
        with_=[],
        take=0,
    )

    bound = bound_from_normalized(sketch)
    restored = normalized_from_bound(bound)

    assert isinstance(bound.where.all[1], BoundWhereExpr)
    assert restored == sketch
    assert isinstance(bound.where.all[1], BoundWhereExpr)
    assert isinstance(bound.where.all[1].any[0], BoundClause)
    assert bound.where.all[1].any[0].field.raw == "b.c"
    assert isinstance(bound.where.not_, BoundClause)
    assert bound.where.not_.field.raw == "x.y.z"
