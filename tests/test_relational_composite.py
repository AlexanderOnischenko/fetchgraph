from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from fetchgraph.relational import (
    ComparisonFilter,
    CompositeRelationalProvider,
    ColumnDescriptor,
    EntityDescriptor,
    PandasRelationalDataProvider,
    QueryResult,
    RelationDescriptor,
    RelationJoin,
    RelationalDataProvider,
    RelationalQuery,
    SelectExpr,
)


def _orders_provider():
    entities = [
        EntityDescriptor(
            name="order",
            columns=[
                ColumnDescriptor(name="id", role="primary_key"),
                ColumnDescriptor(name="customer_id", role="foreign_key"),
            ],
        ),
        EntityDescriptor(
            name="customer",
            columns=[ColumnDescriptor(name="id", role="primary_key"), ColumnDescriptor(name="name")],
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
    frames = {
        "order": pd.DataFrame({"id": [1], "customer_id": [10]}),
        "customer": pd.DataFrame({"id": [10], "name": ["Acme"]}),
    }
    return PandasRelationalDataProvider("orders_rel", entities, relations, frames)


def _products_provider():
    entities = [
        EntityDescriptor(
            name="product",
            columns=[
                ColumnDescriptor(name="id", role="primary_key"),
                ColumnDescriptor(name="sku"),
            ],
        )
    ]
    relations: list[RelationDescriptor] = []
    frames = {"product": pd.DataFrame({"id": [5], "sku": ["ABC-1"]})}
    return PandasRelationalDataProvider("products_rel", entities, relations, frames)


class DummyRelationalProvider(RelationalDataProvider):
    def __init__(self, name: str, entities: list[EntityDescriptor], relations: list[RelationDescriptor]):
        super().__init__(name, entities, relations)
        self._entity_index = {e.name: e for e in entities}

    def _handle_schema(self):
        raise NotImplementedError

    def _handle_semantic_only(self, req):
        raise NotImplementedError

    def _handle_query(self, req: RelationalQuery):
        return QueryResult(meta={"provider": self.name})


def test_composite_routes_and_enriches_meta():
    composite = CompositeRelationalProvider(
        "composite", {"orders": _orders_provider(), "products": _products_provider()}
    )
    query = RelationalQuery(root_entity="product", select=[SelectExpr(expr="sku")])

    result = composite.fetch("demo", selectors=query.model_dump())

    assert result.meta["provider"] == "products_rel"
    assert result.meta["child_provider"] == "products"
    assert [row.data["sku"] for row in result.rows] == ["ABC-1"]


def test_composite_describe_mentions_join_limitations():
    composite = CompositeRelationalProvider(
        "composite", {"orders": _orders_provider(), "products": _products_provider()}
    )

    info = composite.describe()

    assert "cross-provider joins" in info.description
    assert "single_provider_routing" in info.capabilities


def test_composite_handles_relation_alias_in_filters():
    orders_entities = [
        EntityDescriptor(
            name="order",
            columns=[
                ColumnDescriptor(name="id", role="primary_key"),
                ColumnDescriptor(name="customer_id", role="foreign_key"),
            ],
        ),
        EntityDescriptor(
            name="customer",
            columns=[ColumnDescriptor(name="id", role="primary_key")],
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
    composite = CompositeRelationalProvider(
        "composite",
        {
            "orders": DummyRelationalProvider("orders_rel", orders_entities, relations),
            "other": DummyRelationalProvider(
                "other_rel",
                [EntityDescriptor(name="product", columns=[ColumnDescriptor(name="id", role="primary_key")])],
                [],
            ),
        },
    )

    query = RelationalQuery(
        root_entity="order",
        relations=["order_customer"],
        filters=ComparisonFilter(entity="order_customer", field="id", op="=", value=10),
    )

    result = composite.fetch("demo", selectors=query.model_dump())

    assert result.meta["provider"] == "orders_rel"
