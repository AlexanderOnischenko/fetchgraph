from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from fetchgraph.relational_models import (
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
from fetchgraph.relational_pandas import PandasRelationalDataProvider


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
