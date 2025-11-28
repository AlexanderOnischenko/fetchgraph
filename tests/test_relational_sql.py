from __future__ import annotations

import sqlite3

import pytest

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
from fetchgraph.relational_sql import SqlRelationalDataProvider


class FakeSemanticBackend:
    def __init__(self, matches: list[SemanticMatch]):
        self.matches = matches

    def search(self, entity: str, fields, query: str, top_k: int = 100):
        return self.matches[:top_k]


def _make_provider(semantic_backend=None) -> SqlRelationalDataProvider:
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE "customer" (
            "id" INTEGER PRIMARY KEY,
            "name" TEXT,
            "notes" TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE "order" (
            "id" INTEGER PRIMARY KEY,
            "customer_id" INTEGER,
            "total" INTEGER,
            "status" TEXT
        )
        """
    )
    cur.executemany(
        'INSERT INTO "customer" (id, name, notes) VALUES (?, ?, ?)',
        [
            (1, "Alice", "pharma buyer"),
            (2, "Bob", "retail"),
        ],
    )
    cur.executemany(
        'INSERT INTO "order" (id, customer_id, total, status) VALUES (?, ?, ?, ?)',
        [
            (101, 1, 120, "shipped"),
            (102, 2, 80, "pending"),
            (103, 1, 200, "pending"),
        ],
    )
    conn.commit()

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

    return SqlRelationalDataProvider(
        name="orders_rel_sql",
        entities=entities,
        relations=relations,
        connection=conn,
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


def test_semantic_boost_applies_with_grouping_and_aggregations():
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
        group_by=[GroupBySpec(entity="customer", field="id")],
        aggregations=[AggregationSpec(field="total", agg="sum", alias="total_spend")],
        semantic_clauses=[
            SemanticClause(entity="customer", fields=["notes"], query="buyers", mode="boost"),
        ],
    )

    res = provider.fetch("demo", selectors=req.model_dump())

    totals = [(row.data["customer__id"], row.data["total_spend"]) for row in res.rows]
    assert totals == [(2, 80), (1, 320)]


def test_semantic_boost_with_filters_keeps_parameter_order():
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
        filters=ComparisonFilter(entity="order", field="status", op="=", value="pending"),
        semantic_clauses=[
            SemanticClause(entity="customer", fields=["notes"], query="buyers", mode="boost"),
        ],
        select=[SelectExpr(expr="id")],
    )

    res = provider.fetch("demo", selectors=req.model_dump())

    assert [row.data["id"] for row in res.rows] == [102, 103]

