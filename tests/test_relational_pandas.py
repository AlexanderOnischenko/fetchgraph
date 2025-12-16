from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from fetchgraph.relational.models import (
    AggregationSpec,
    ColumnDescriptor,
    ComparisonFilter,
    EntityDescriptor,
    GroupBySpec,
    LogicalFilter,
    RelationalQuery,
    RelationDescriptor,
    RelationJoin,
    SelectExpr,
    SemanticClause,
    SemanticMatch,
)
from fetchgraph.relational.providers.pandas import PandasRelationalDataProvider


class FakeSemanticBackend:
    def __init__(self, matches: list[SemanticMatch]):
        self.matches = matches

    def search(self, entity: str, fields, query: str, top_k: int = 100):
        return self.matches[:top_k]


def _make_provider(semantic_backend=None) -> PandasRelationalDataProvider:
    customers = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "notes": ["pharma buyer", "retail"],
        }
    )
    orders = pd.DataFrame(
        {
            "id": [101, 102, 103],
            "customer_id": [1, 2, 1],
            "total": [120, 80, 200],
            "status": ["shipped", "pending", "pending"],
        }
    )
    entities = [
        EntityDescriptor(
            name="customer",
            columns=[
                ColumnDescriptor(name="id", role="primary_key"),
                ColumnDescriptor(name="name"),
                ColumnDescriptor(name="notes"),
            ],
        ),
        EntityDescriptor(
            name="order",
            columns=[
                ColumnDescriptor(name="id", role="primary_key"),
                ColumnDescriptor(name="customer_id", role="foreign_key"),
                ColumnDescriptor(name="total", type="int"),
                ColumnDescriptor(name="status"),
            ],
        ),
    ]
    relations = [
        RelationDescriptor(
            name="order_customer",
            from_entity="order",
            to_entity="customer",
            join=RelationJoin(
                from_entity="order", from_column="customer_id", to_entity="customer", to_column="id"
            ),
        )
    ]
    return PandasRelationalDataProvider(
        name="orders_rel",
        entities=entities,
        relations=relations,
        frames={"customer": customers, "order": orders},
        semantic_backend=semantic_backend,
    )


def _make_text_provider() -> PandasRelationalDataProvider:
    teams = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["Marketing", "marketing ", "Маркетинг", "sales"],
        }
    )

    entities = [
        EntityDescriptor(
            name="team",
            columns=[
                ColumnDescriptor(name="id", role="primary_key"),
                ColumnDescriptor(name="name"),
            ],
        )
    ]

    return PandasRelationalDataProvider(
        name="teams_rel",
        entities=entities,
        relations=[],
        frames={"team": teams},
    )


def test_select_with_join_returns_related_data():
    provider = _make_provider()
    req = RelationalQuery(root_entity="order", relations=["order_customer"], limit=5)
    res = provider.fetch("demo", selectors=req.model_dump())

    assert len(res.rows) == 3
    assert res.rows[0].related["customer"]["name"] == "Alice"


def test_filters_with_logical_clause():
    provider = _make_provider()
    req = RelationalQuery(
        root_entity="order",
        relations=["order_customer"],
        filters=LogicalFilter(
            op="or",
            clauses=[
                ComparisonFilter(entity="order", field="total", op=">", value=150),
                LogicalFilter(
                    op="and",
                    clauses=[
                        ComparisonFilter(entity="order", field="status", op="=", value="pending"),
                        ComparisonFilter(entity="customer", field="name", op="like", value="Ali"),
                    ],
                ),
            ],
        ),
    )
    res = provider.fetch("demo", selectors=req.model_dump())

    ids = [row.data["id"] for row in res.rows]
    assert ids == [103]


def test_apply_filters_handles_dataframe_column():
    provider = _make_provider()
    original_resolve = provider._resolve_column

    def resolve_as_list(df, root_entity, field, entity=None):
        # Simulate a column resolution that returns a list-like, causing df[col] to be a DataFrame.
        return [original_resolve(df, root_entity, field, entity)]

    provider._resolve_column = resolve_as_list  # type: ignore[method-assign]

    clause = ComparisonFilter(entity="order", field="status", op="=", value="pending")
    filtered = provider._apply_filters(provider._get_frame("order"), "order", clause)

    assert filtered["status"].tolist() == ["pending", "pending"]


def test_group_by_with_aggregations():
    provider = _make_provider()
    req = RelationalQuery(
        root_entity="order",
        relations=["order_customer"],
        group_by=[GroupBySpec(entity="customer", field="name")],
        aggregations=[AggregationSpec(field="total", agg="sum", alias="total_spend")],
    )
    res = provider.fetch("demo", selectors=req.model_dump())

    totals = {row.data["customer__name"]: row.data["total_spend"] for row in res.rows}
    assert totals == {"Alice": 320, "Bob": 80}


def test_semantic_filter_respects_threshold():
    backend = FakeSemanticBackend(
        [
            SemanticMatch(entity="customer", id=1, score=0.9),
            SemanticMatch(entity="customer", id=2, score=0.3),
        ]
    )
    provider = _make_provider(semantic_backend=backend)
    req = RelationalQuery(
        root_entity="order",
        relations=["order_customer"],
        semantic_clauses=[
            SemanticClause(entity="customer", fields=["notes"], query="pharma", mode="filter", threshold=0.8),
        ],
    )
    res = provider.fetch("demo", selectors=req.model_dump())

    assert [row.data["customer_id"] for row in res.rows] == [1, 1]


def test_semantic_boost_sorts_by_score_and_threshold():
    backend = FakeSemanticBackend(
        [
            SemanticMatch(entity="customer", id=2, score=0.9),
            SemanticMatch(entity="customer", id=1, score=0.4),
        ]
    )
    provider = _make_provider(semantic_backend=backend)
    req = RelationalQuery(
        root_entity="order",
        relations=["order_customer"],
        semantic_clauses=[
            SemanticClause(entity="customer", fields=["notes"], query="buyers", mode="boost", threshold=0.5),
        ],
        select=[SelectExpr(expr="id")],
    )
    res = provider.fetch("demo", selectors=req.model_dump())

    assert [row.data["id"] for row in res.rows] == [102, 101, 103]


def test_semantic_filter_sorts_by_score_before_limit():
    backend = FakeSemanticBackend(
        [
            SemanticMatch(entity="customer", id=2, score=0.9),
            SemanticMatch(entity="customer", id=1, score=0.4),
        ]
    )
    provider = _make_provider(semantic_backend=backend)
    req = RelationalQuery(
        root_entity="order",
        relations=["order_customer"],
        semantic_clauses=[
            SemanticClause(entity="customer", fields=["notes"], query="buyers", mode="filter"),
        ],
        select=[SelectExpr(expr="id")],
        limit=2,
    )

    res = provider.fetch("demo", selectors=req.model_dump())

    assert [row.data["id"] for row in res.rows] == [102, 101]


def test_soft_string_filter_is_case_insensitive_and_trimmed():
    provider = _make_text_provider()
    req = RelationalQuery(
        root_entity="team",
        case_sensitivity=False,
        filters=ComparisonFilter(field="name", op="=", value="MARKETING"),
    )

    res = provider.fetch("demo", selectors=req.model_dump())

    assert [row.data["name"] for row in res.rows] == ["Marketing", "marketing "]


def test_soft_string_filter_supports_in_and_fuzzy_match():
    provider = _make_text_provider()
    req = RelationalQuery(
        root_entity="team",
        case_sensitivity=False,
        filters=ComparisonFilter(field="name", op="=", value="markeing"),
    )

    res = provider.fetch("demo", selectors=req.model_dump())

    assert [row.data["name"] for row in res.rows] == ["Marketing", "marketing "]

    req_in = RelationalQuery(
        root_entity="team",
        case_sensitivity=False,
        filters=ComparisonFilter(field="name", op="in", value=[" marketing", "sales"]),
    )

    res_in = provider.fetch("demo", selectors=req_in.model_dump())

    assert [row.data["name"] for row in res_in.rows] == ["Marketing", "marketing ", "sales"]


def test_case_sensitive_string_filter_remains_strict():
    provider = _make_text_provider()
    req = RelationalQuery(
        root_entity="team",
        case_sensitivity=True,
        filters=ComparisonFilter(field="name", op="=", value="MARKETING"),
    )

    res = provider.fetch("demo", selectors=req.model_dump())

    assert [row.data["name"] for row in res.rows] == []
