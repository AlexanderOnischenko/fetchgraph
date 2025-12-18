from __future__ import annotations

from typing import Literal, cast

import pytest

from typing import Literal, cast

pd = pytest.importorskip("pandas")

from fetchgraph.relational.providers.composite import CompositeRelationalProvider
from fetchgraph.relational.models import (
    AggregationSpec,
    ColumnDescriptor,
    ComparisonFilter,
    EntityDescriptor,
    GroupBySpec,
    RelationalQuery,
    RelationDescriptor,
    RelationJoin,
    SelectExpr,
)
from fetchgraph.relational.providers.pandas import PandasRelationalDataProvider


def _build_block_system_composite(
    *,
    join_type: Literal["inner", "left", "right", "outer"] = "inner",
    cardinality: Literal["1_to_1", "1_to_many", "many_to_1", "many_to_many"] = "1_to_many",
    block_df=None,
    system_df=None,
    **kwargs,
) -> CompositeRelationalProvider:
    block_df = block_df if block_df is not None else pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "group": ["g1", "g2", "g1"],
        }
    )
    system_df = system_df if system_df is not None else pd.DataFrame(
        {
            "id": [10, 11, 12],
            "block_id": [1, 1, 2],
            "code": ["S1", "S2", "S3"],
        }
    )
    block_entity = EntityDescriptor(
        name="block",
        columns=[
            ColumnDescriptor(name="id", role="primary_key"),
            ColumnDescriptor(name="name"),
            ColumnDescriptor(name="group"),
        ],
    )
    system_entity = EntityDescriptor(
        name="system",
        columns=[
            ColumnDescriptor(name="id", role="primary_key"),
            ColumnDescriptor(name="block_id", role="foreign_key"),
            ColumnDescriptor(name="code"),
        ],
    )
    relation = RelationDescriptor(
        name="block_system",
        from_entity="block",
        to_entity="system",
        cardinality=cardinality,
        join=RelationJoin(
            from_entity="block",
            from_column="id",
            to_entity="system",
            to_column="block_id",
            join_type=join_type,
        ),
    )
    block_provider = PandasRelationalDataProvider(
        "blocks_rel", [block_entity], [relation], {"block": block_df}
    )
    system_provider = PandasRelationalDataProvider(
        "systems_rel", [system_entity], [], {"system": system_df}
    )
    return CompositeRelationalProvider(
        "composite",
        {"blocks": block_provider, "systems": system_provider},
        **kwargs,
    )


def _build_employee_department_composite(**kwargs) -> CompositeRelationalProvider:
    employee_df = pd.DataFrame(
        {"id": [1, 2, 3], "department_id": [10, 10, 11], "name": ["A", "B", "C"]}
    )
    department_df = pd.DataFrame({"id": [10, 11], "title": ["Eng", "HR"]})

    employee = EntityDescriptor(
        name="employee",
        columns=[
            ColumnDescriptor(name="id", role="primary_key"),
            ColumnDescriptor(name="department_id", role="foreign_key"),
            ColumnDescriptor(name="name"),
        ],
    )
    department = EntityDescriptor(
        name="department",
        columns=[ColumnDescriptor(name="id", role="primary_key"), ColumnDescriptor(name="title")],
    )
    relation = RelationDescriptor(
        name="employee_department",
        from_entity="employee",
        to_entity="department",
        cardinality="many_to_1",
        join=RelationJoin(
            from_entity="employee",
            from_column="department_id",
            to_entity="department",
            to_column="id",
            join_type="inner",
        ),
    )
    employee_provider = PandasRelationalDataProvider(
        "employee_rel", [employee], [relation], {"employee": employee_df}
    )
    dept_provider = PandasRelationalDataProvider(
        "department_rel", [department], [], {"department": department_df}
    )
    return CompositeRelationalProvider(
        "composite", {"employees": employee_provider, "departments": dept_provider}, **kwargs
    )


def _build_one_to_one_composite(**kwargs) -> CompositeRelationalProvider:
    left_df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    right_df = pd.DataFrame({"id": [1, 2], "label": ["X", "Y"]})

    left = EntityDescriptor(
        name="left",
        columns=[ColumnDescriptor(name="id", role="primary_key"), ColumnDescriptor(name="name")],
    )
    right = EntityDescriptor(
        name="right",
        columns=[ColumnDescriptor(name="id", role="primary_key"), ColumnDescriptor(name="label")],
    )
    relation = RelationDescriptor(
        name="left_right",
        from_entity="left",
        to_entity="right",
        cardinality="1_to_1",
        join=RelationJoin(
            from_entity="left", from_column="id", to_entity="right", to_column="id", join_type="inner"
        ),
    )
    left_provider = PandasRelationalDataProvider("left_rel", [left], [relation], {"left": left_df})
    right_provider = PandasRelationalDataProvider("right_rel", [right], [], {"right": right_df})
    return CompositeRelationalProvider(
        "composite", {"left": left_provider, "right": right_provider}, **kwargs
    )


def test_cross_inner_join_1_to_many():
    composite = _build_block_system_composite(join_type="inner")
    query = RelationalQuery(root_entity="block", relations=["block_system"])

    result = composite.fetch("demo", selectors=query.model_dump())

    pairs = [(row.data["id"], row.related["system"]["code"]) for row in result.rows]
    assert pairs == [(1, "S1"), (1, "S2"), (2, "S3")]


def test_cross_left_join_1_to_many_with_unmatched():
    composite = _build_block_system_composite(join_type="left")
    query = RelationalQuery(root_entity="block", relations=["block_system"])

    result = composite.fetch("demo", selectors=query.model_dump())

    pairs = [
        (row.data["id"], row.related.get("system", {}).get("code"))
        for row in sorted(result.rows, key=lambda r: r.data["id"])
    ]
    assert pairs == [(1, "S1"), (1, "S2"), (2, "S3"), (3, None)]


def test_cross_join_many_to_1():
    composite = _build_employee_department_composite()
    query = RelationalQuery(root_entity="employee", relations=["employee_department"])

    result = composite.fetch("demo", selectors=query.model_dump())

    related_titles = [row.related["department"]["title"] for row in result.rows]
    assert related_titles == ["Eng", "Eng", "HR"]


def test_cross_join_1_to_1():
    composite = _build_one_to_one_composite()
    query = RelationalQuery(root_entity="left", relations=["left_right"])

    result = composite.fetch("demo", selectors=query.model_dump())

    labels = [row.related["right"]["label"] for row in result.rows]
    assert labels == ["X", "Y"]


def test_cross_join_1_to_1_cardinality_violation():
    right_df = pd.DataFrame({"id": [1, 1], "label": ["X", "Y"]})
    composite = _build_one_to_one_composite()
    right_provider = cast(PandasRelationalDataProvider, composite.children["right"])
    right_provider.frames["right"] = right_df
    query = RelationalQuery(root_entity="left", relations=["left_right"])

    with pytest.raises(ValueError, match="Cardinality 1_to_1 violated"):
        composite.fetch("demo", selectors=query.model_dump())


def test_cross_join_many_to_1_cardinality_violation():
    composite = _build_employee_department_composite()
    departments_provider = cast(PandasRelationalDataProvider, composite.children["departments"])
    departments_provider.frames["department"] = pd.DataFrame(
        {"id": [10, 10, 11], "title": ["Eng", "Ops", "HR"]}
    )
    query = RelationalQuery(root_entity="employee", relations=["employee_department"])

    with pytest.raises(ValueError, match="Cardinality many_to_1 violated"):
        composite.fetch("demo", selectors=query.model_dump())


def test_cross_join_many_to_many_allows_multiple_matches():
    composite = _build_block_system_composite(cardinality="many_to_many")
    query = RelationalQuery(root_entity="block", relations=["block_system"])

    result = composite.fetch("demo", selectors=query.model_dump())

    assert len(result.rows) == 3


def test_cross_join_respects_max_join_rows_per_batch():
    composite = _build_block_system_composite(max_join_rows_per_batch=2)
    query = RelationalQuery(root_entity="block", relations=["block_system"])

    result = composite.fetch("demo", selectors=query.model_dump())

    assert len(result.rows) == 3


def test_cross_join_handles_right_batch_overflow_for_1_to_many():
    system_df = pd.DataFrame(
        {"id": [10, 11, 12], "block_id": [1, 1, 1], "code": ["S1", "S2", "S3"]}
    )
    composite = _build_block_system_composite(
        cardinality="1_to_many",
        system_df=system_df,
        max_right_rows_per_batch=2,
    )
    query = RelationalQuery(root_entity="block", relations=["block_system"])

    result = composite.fetch("demo", selectors=query.model_dump())

    # All three system rows should be joined with block 1 even though they exceed
    # the per-batch right-row limit, thanks to overflow-safe paging.
    assert len(result.rows) == 3


def test_cross_join_raises_on_right_batch_overflow_for_1_to_1_or_many_to_1():
    composite = _build_one_to_one_composite()
    right_provider = cast(PandasRelationalDataProvider, composite.children["right"])
    right_provider.frames["right"] = pd.DataFrame(
        {"id": [1, 1, 2], "label": ["X", "Y", "Z"]}
    )
    query = RelationalQuery(root_entity="left", relations=["left_right"])

    with pytest.raises(ValueError, match="Cardinality 1_to_1 violated"):
        composite.fetch("demo", selectors=query.model_dump())


def test_cross_provider_groupby_root_count_remote():
    composite = _build_block_system_composite(join_type="left")
    query = RelationalQuery(
        root_entity="block",
        relations=["block_system"],
        group_by=[GroupBySpec(field="name")],
        aggregations=[AggregationSpec(field="system.id", agg="count", alias="system_count")],
    )

    result = composite.fetch("demo", selectors=query.model_dump())

    grouped = {row.data["name"]: row.data["system_count"] for row in result.rows}
    assert grouped == {"A": 2, "B": 1, "C": 0}


def test_cross_provider_global_aggregation_count_remote():
    composite = _build_block_system_composite(join_type="left")
    query = RelationalQuery(
        root_entity="block",
        relations=["block_system"],
        aggregations=[AggregationSpec(field="system.id", agg="count", alias="total_systems")],
    )

    result = composite.fetch("demo", selectors=query.model_dump())

    assert result.rows == []
    assert result.aggregations["total_systems"].value == 3


def test_cross_provider_groupby_default_count():
    composite = _build_block_system_composite(join_type="left")
    query = RelationalQuery(
        root_entity="block",
        relations=["block_system"],
        group_by=[GroupBySpec(field="group")],
    )

    result = composite.fetch("demo", selectors=query.model_dump())

    grouped = {row.data["group"]: row.data["count"] for row in result.rows}
    assert grouped == {"g1": 3, "g2": 1}


def test_cross_provider_groupby_remote_field():
    composite = _build_block_system_composite(join_type="left")
    query = RelationalQuery(
        root_entity="block",
        relations=["block_system"],
        group_by=[GroupBySpec(field="system.code")],
    )

    result = composite.fetch("demo", selectors=query.model_dump())

    grouped = {row.data["system.code"]: row.data["count"] for row in result.rows}
    assert grouped == {"S1": 1, "S2": 1, "S3": 1, None: 1}


def test_cross_provider_filter_on_remote_entity_raises():
    composite = _build_block_system_composite(join_type="inner")
    query = RelationalQuery(
        root_entity="block",
        relations=["block_system"],
        filters=ComparisonFilter(entity="system", field="code", op="=", value="S1"),
    )

    with pytest.raises(NotImplementedError, match="Filters on non-root providers are not supported"):
        composite.fetch("demo", selectors=query.model_dump())


def test_cross_provider_groupby_on_explicit_non_root_entity_raises():
    composite = _build_block_system_composite(join_type="inner")
    query = RelationalQuery(
        root_entity="block",
        relations=["block_system"],
        group_by=[GroupBySpec(entity="system", field="code")],
    )

    with pytest.raises(NotImplementedError, match="group_by on non-root entities is not supported"):
        composite.fetch("demo", selectors=query.model_dump())


def test_single_provider_routing_simple_query():
    block_df = pd.DataFrame({"id": [1], "name": ["A"]})
    system_df = pd.DataFrame({"id": [1], "block_id": [1], "code": ["S1"]})
    block = EntityDescriptor(
        name="block",
        columns=[ColumnDescriptor(name="id", role="primary_key"), ColumnDescriptor(name="name")],
    )
    system = EntityDescriptor(
        name="system",
        columns=[
            ColumnDescriptor(name="id", role="primary_key"),
            ColumnDescriptor(name="block_id", role="foreign_key"),
            ColumnDescriptor(name="code"),
        ],
    )
    relation = RelationDescriptor(
        name="block_system",
        from_entity="block",
        to_entity="system",
        join=RelationJoin(
            from_entity="block",
            from_column="id",
            to_entity="system",
            to_column="block_id",
            join_type="inner",
        ),
    )
    child = PandasRelationalDataProvider(
        "single_rel", [block, system], [relation], {"block": block_df, "system": system_df}
    )
    composite = CompositeRelationalProvider("composite", {"all": child})

    query = RelationalQuery(
        root_entity="block", relations=["block_system"], select=[SelectExpr(expr="system.code")]
    )

    expected = child.fetch("demo", selectors=query.model_dump())
    result = composite.fetch("demo", selectors=query.model_dump())

    assert [(row.data, row.related) for row in result.rows] == [
        (row.data, row.related) for row in expected.rows
    ]
