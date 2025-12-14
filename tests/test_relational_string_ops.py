import sqlite3

import pandas as pd

from fetchgraph.relational.models import (
    ColumnDescriptor,
    ComparisonFilter,
    EntityDescriptor,
    RelationalQuery,
)
from fetchgraph.relational.providers import PandasRelationalDataProvider, SqlRelationalDataProvider


def _make_people_provider() -> PandasRelationalDataProvider:
    people = pd.DataFrame({"id": [1, 2, 3, 4], "name": ["Alice", "Alfred", "Grace", "bob"]})
    entities = [
        EntityDescriptor(
            name="person",
            columns=[
                ColumnDescriptor(name="id", role="primary_key"),
                ColumnDescriptor(name="name"),
            ],
        )
    ]
    return PandasRelationalDataProvider(
        name="people_rel",
        entities=entities,
        relations=[],
        frames={"person": people},
    )


def _make_people_sql_provider() -> SqlRelationalDataProvider:
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE person (id INTEGER PRIMARY KEY, name TEXT)")
    cur.executemany(
        "INSERT INTO person (id, name) VALUES (?, ?)",
        [(1, "Alice"), (2, "Alfred"), (3, "Grace"), (4, "bob")],
    )
    conn.commit()

    entities = [
        EntityDescriptor(
            name="person",
            columns=[
                ColumnDescriptor(name="id", role="primary_key"),
                ColumnDescriptor(name="name"),
            ],
        )
    ]

    return SqlRelationalDataProvider(
        name="people_rel_sql",
        entities=entities,
        relations=[],
        connection=conn,
    )


def test_string_ops_with_pandas_provider():
    provider = _make_people_provider()
    req = RelationalQuery(
        root_entity="person",
        filters=ComparisonFilter(field="name", op="starts", value="Al"),
        limit=None,
    )

    res = provider.fetch("demo", selectors=req.model_dump())

    assert [row.data["name"] for row in res.rows] == ["Alice", "Alfred"]

    req_not = RelationalQuery(
        root_entity="person",
        filters=ComparisonFilter(field="name", op="not_ends", value="e"),
        limit=None,
    )

    res_not = provider.fetch("demo", selectors=req_not.model_dump())

    assert [row.data["name"] for row in res_not.rows] == ["Alfred", "bob"]


def test_string_ops_with_sql_provider():
    provider = _make_people_sql_provider()
    req = RelationalQuery(
        root_entity="person",
        filters=ComparisonFilter(field="name", op="ends", value="ice"),
        limit=None,
    )

    res = provider.fetch("demo", selectors=req.model_dump())

    assert [row.data["name"] for row in res.rows] == ["Alice"]

    req_not = RelationalQuery(
        root_entity="person",
        filters=ComparisonFilter(field="name", op="not_ilike", value="al"),
        limit=None,
    )

    res_not = provider.fetch("demo", selectors=req_not.model_dump())

    assert [row.data["name"] for row in res_not.rows] == ["Grace", "bob"]
