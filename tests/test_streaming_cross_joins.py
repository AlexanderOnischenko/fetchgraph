from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, Optional, Any

import pandas as pd
import pytest

from fetchgraph import (
    PandasRelationalDataProvider,
    CompositeRelationalProvider,
    RelationalDataProvider,
    EntityDescriptor,
    ColumnDescriptor,
    RelationDescriptor,
    RelationJoin,
    RelationalQuery,
    SelectExpr,
)

# -------------------- Spy wrapper --------------------

def _len_rows(obj: Any) -> Optional[int]:
    if obj is None:
        return None
    rows = getattr(obj, "rows", None)
    if rows is not None:
        return len(rows)
    if isinstance(obj, pd.DataFrame):
        return len(obj)
    return None


class SpyProvider(RelationalDataProvider):
    def __init__(self, inner: RelationalDataProvider):
        super().__init__(name=inner.name, entities=inner.entities, relations=inner.relations)
        self.inner = inner
        self.calls: list[dict[str, Any]] = []
        # composite использует _entity_index для построения индексов
        self._entity_index = getattr(inner, "_entity_index", {})
        self.entities = inner.entities
        self.relations = inner.relations
        self.name = inner.name

    def fetch(self, feature_name: str, selectors=None, **kwargs):
        res = self.inner.fetch(feature_name, selectors=selectors, **kwargs)
        self.calls.append(
            {
                "selectors": selectors or {},
                "n_rows": _len_rows(res),
            }
        )
        return res

    def __getattr__(self, item):
        return getattr(self.inner, item)


# -------------------- Schema/helpers --------------------

def _entities_and_relation(
    *,
    cardinality: Literal["1_to_1", "1_to_many", "many_to_1", "many_to_many"],
    join_type: Literal["inner", "left", "right", "outer"] = "inner",
):
    left = EntityDescriptor(
        name="left",
        columns=[
            ColumnDescriptor(name="id", role="primary_key"),
            ColumnDescriptor(name="k"),
            ColumnDescriptor(name="lval"),
        ],
    )
    right = EntityDescriptor(
        name="right",
        columns=[
            ColumnDescriptor(name="id", role="primary_key"),
            ColumnDescriptor(name="k"),
            ColumnDescriptor(name="rval"),
        ],
    )
    rel = RelationDescriptor(
        name="left_right",
        from_entity="left",
        to_entity="right",
        join=RelationJoin(
            from_entity="left",
            from_column="k",
            to_entity="right",
            to_column="k",
            join_type=join_type,
        ),
        cardinality=cardinality,
    )
    return left, right, rel


def make_composite(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    *,
    cardinality: Literal["1_to_1", "1_to_many", "many_to_1", "many_to_many"],
    max_join_rows_per_batch: int = 1000,
    max_right_rows_per_batch: int = 5000,
):
    left_ent, right_ent, rel = _entities_and_relation(cardinality=cardinality)

    left_provider = PandasRelationalDataProvider(
        name="left_p",
        entities=[left_ent],
        relations=[rel],
        frames={"left": left_df},
    )
    right_provider = PandasRelationalDataProvider(
        name="right_p",
        entities=[right_ent],
        relations=[rel],
        frames={"right": right_df},
    )

    spy_left = SpyProvider(left_provider)
    spy_right = SpyProvider(right_provider)

    comp = CompositeRelationalProvider(
        name="comp",
        children={"left": spy_left, "right": spy_right},
        max_join_rows_per_batch=max_join_rows_per_batch,
        max_right_rows_per_batch=max_right_rows_per_batch,
    )
    return comp, spy_left, spy_right


# -------------------- Data builders --------------------

def build_case_small_intersection(n_left=12000, n_right=12000, overlap=25):
    left_df = pd.DataFrame(
        {"id": range(n_left), "k": range(n_left), "lval": ["L"] * n_left}
    )

    overlap_keys = list(range(n_left - overlap, n_left))
    right_disjoint = list(range(10_000_000, 10_000_000 + (n_right - overlap)))
    right_keys = right_disjoint + overlap_keys
    right_df = pd.DataFrame(
        {"id": range(n_right), "k": right_keys, "rval": ["R"] * n_right}
    )
    return left_df, right_df, set(overlap_keys)


def build_case_sum_overflow(
    n_left=6000,
    hot_keys=1000,
    matches_per_hot_key=6,
    extra_right=2000,
):
    # left: ключи 0..n_left-1
    left_df = pd.DataFrame(
        {"id": range(n_left), "k": range(n_left), "lval": ["L"] * n_left}
    )

    rows = []
    rid = 0
    # right: на hot_keys ключах много матчей => sum overflow > 5000
    for k in range(hot_keys):
        for j in range(matches_per_hot_key):
            rows.append({"id": rid, "k": k, "rval": f"R{k}_{j}"})
            rid += 1

    for k in range(20_000_000, 20_000_000 + extra_right):
        rows.append({"id": rid, "k": k, "rval": "X"})
        rid += 1

    right_df = pd.DataFrame(rows)
    expected_join_rows = hot_keys * matches_per_hot_key
    return left_df, right_df, expected_join_rows


def build_case_single_key_overflow(
    n_left=6000,
    hot_key=0,
    right_rows_for_hot=6000,
    extra_right=2000,
):
    left_df = pd.DataFrame(
        {"id": range(n_left), "k": range(n_left), "lval": ["L"] * n_left}
    )

    rows = []
    rid = 0
    for j in range(right_rows_for_hot):
        rows.append({"id": rid, "k": hot_key, "rval": f"R{hot_key}_{j}"})
        rid += 1

    for k in range(30_000_000, 30_000_000 + extra_right):
        rows.append({"id": rid, "k": k, "rval": "X"})
        rid += 1

    right_df = pd.DataFrame(rows)
    expected_join_rows = right_rows_for_hot  # матчится только hot_key
    return left_df, right_df, expected_join_rows


def build_case_m2m_small_overlap_small_output(
    n_left=12000,
    n_right=12000,
    overlap_keys=(777, 778, 779, 780, 781),
    left_dups=5,
    right_dups=7,
):
    # left: много disjoint + немного overlap с дубликатами
    left_rows = []
    lid = 0
    for k in overlap_keys:
        for _ in range(left_dups):
            left_rows.append({"id": lid, "k": k, "lval": f"L{k}"})
            lid += 1

    # добиваем до n_left disjoint ключами
    disjoint_needed = n_left - len(left_rows)
    for k in range(1_000_000, 1_000_000 + disjoint_needed):
        left_rows.append({"id": lid, "k": k, "lval": "LD"})
        lid += 1
    left_df = pd.DataFrame(left_rows)

    # right: аналогично
    right_rows = []
    rid = 0
    for k in overlap_keys:
        for _ in range(right_dups):
            right_rows.append({"id": rid, "k": k, "rval": f"R{k}"})
            rid += 1

    disjoint_needed = n_right - len(right_rows)
    for k in range(2_000_000, 2_000_000 + disjoint_needed):
        right_rows.append({"id": rid, "k": k, "rval": "RD"})
        rid += 1
    right_df = pd.DataFrame(right_rows)

    expected_join_rows = len(overlap_keys) * left_dups * right_dups
    return left_df, right_df, set(overlap_keys), expected_join_rows


def build_case_m2m_limit_short_circuit(
    n_left=6000,
    n_right=6000,
    hot_key=1,
    left_hot_rows=5000,
    right_hot_rows=1000,  # держим умеренно, чтобы batch-join не раздувался
):
    left_rows = []
    for i in range(left_hot_rows):
        left_rows.append({"id": i, "k": hot_key, "lval": "LHOT"})
    # disjoint хвост
    for i in range(left_hot_rows, n_left):
        left_rows.append({"id": i, "k": 10_000_000 + i, "lval": "LD"})
    left_df = pd.DataFrame(left_rows)

    right_rows = []
    for i in range(right_hot_rows):
        right_rows.append({"id": i, "k": hot_key, "rval": "RHOT"})
    for i in range(right_hot_rows, n_right):
        right_rows.append({"id": i, "k": 20_000_000 + i, "rval": "RD"})
    right_df = pd.DataFrame(right_rows)

    return left_df, right_df, hot_key


# -------------------- Parametrized regressions --------------------

@dataclass(frozen=True)
class JoinCase:
    case_id: str
    cardinality: Literal["1_to_1", "1_to_many", "many_to_1", "many_to_many"]
    builder: Callable[[], Any]
    limit: Optional[int]
    max_join_rows_per_batch: int
    max_right_rows_per_batch: int
    check: Callable[[Any, SpyProvider, SpyProvider, Any], None]


def _mk_query(limit: Optional[int]):
    return RelationalQuery(
        op="query",
        root_entity="left",
        relations=["left_right"],
        select=[SelectExpr(expr="left.k"), SelectExpr(expr="right.k"), SelectExpr(expr="right.id")],
        limit=limit,
        offset=0,
    )

def _check_small_intersection(res: Any, _: SpyProvider, sr: SpyProvider, ctx: Any) -> None:
    overlap = ctx[2]
    assert_len_eq(len(res.rows), len(overlap))
    assert_set_eq({r.data["k"] for r in res.rows}, overlap)
    assert_any(sr.calls, lambda c: True)


def _check_overflow_sum(res: Any, _: SpyProvider, sr: SpyProvider, ctx: Any) -> None:
    expected = ctx[2]
    assert_len_eq(len(res.rows), expected)
    assert_any(sr.calls, lambda c: bool((c["selectors"] or {}).get("group_by")))


def _check_overflow_single_key(res: Any, _: SpyProvider, sr: SpyProvider, ctx: Any) -> None:
    expected = ctx[2]
    assert_len_eq(len(res.rows), expected)
    assert_any(sr.calls, lambda c: (c["selectors"] or {}).get("offset") == 5000)


def _check_m2m_small_overlap(res: Any, _: SpyProvider, __: SpyProvider, ctx: Any) -> None:
    overlap, expected = ctx[2], ctx[3]
    assert_len_eq(len(res.rows), expected)
    assert_set_subset({r.data["k"] for r in res.rows}, overlap)
    assert_set_subset({r.related.get("right", {}).get("k") for r in res.rows}, overlap)


def _check_m2m_limit_short_circuit(res: Any, sl: SpyProvider, sr: SpyProvider, ctx: Any) -> None:
    hot_key = ctx[2]
    assert_len_eq(len(res.rows), 50)
    assert_all(res.rows, lambda r: r.data["k"] == hot_key)
    assert_le(len(sl.calls), 1)
    assert_le(len(sr.calls), 2)


CASES = [
    JoinCase(
        case_id="small_intersection_12k_25",
        cardinality="1_to_1",
        builder=lambda: build_case_small_intersection(),
        limit=None,
        max_join_rows_per_batch=1000,
        max_right_rows_per_batch=5000,
        check=_check_small_intersection,
    ),
    JoinCase(
        case_id="overflow_sum_hot1000x6",
        cardinality="1_to_many",
        builder=lambda: build_case_sum_overflow(),
        limit=None,
        max_join_rows_per_batch=1000,
        max_right_rows_per_batch=5000,
        check=_check_overflow_sum,
    ),
    JoinCase(
        case_id="overflow_single_key_6000",
        cardinality="1_to_many",
        builder=lambda: build_case_single_key_overflow(),
        limit=None,
        max_join_rows_per_batch=1000,
        max_right_rows_per_batch=5000,
        check=_check_overflow_single_key,
    ),
    JoinCase(
        case_id="m2m_small_overlap_small_output",
        cardinality="many_to_many",
        builder=lambda: build_case_m2m_small_overlap_small_output(),
        limit=None,
        max_join_rows_per_batch=1000,
        max_right_rows_per_batch=5000,
        check=_check_m2m_small_overlap,
    ),
    pytest.param(
        JoinCase(
            case_id="m2m_limit_short_circuit",
            cardinality="many_to_many",
            builder=lambda: build_case_m2m_limit_short_circuit(),
            limit=50,
            max_join_rows_per_batch=5,   # важно: небольшой batch, чтобы fanout не раздувал память
            max_right_rows_per_batch=5000,
            check=_check_m2m_limit_short_circuit,
        ),
        marks=pytest.mark.slow,
    ),
]


# -------------------- tiny assert helpers (чтобы лямбды читались) --------------------

def assert_len_eq(a, b):
    assert a == b, f"len mismatch: {a} != {b}"

def assert_set_eq(a, b):
    assert a == b, f"set mismatch:\nA={sorted(a)[:20]}...\nB={sorted(b)[:20]}..."

def assert_set_subset(a, b):
    assert a.issubset(b), f"subset mismatch: {a - b}"

def assert_any(items, pred):
    assert any(pred(x) for x in items), "expected condition to be True for at least one item"

def assert_all(items, pred):
    assert all(pred(x) for x in items), "expected condition to be True for all items"

def assert_le(a, b):
    assert a <= b, f"expected {a} <= {b}"


# -------------------- The parametrized test --------------------

@pytest.mark.parametrize("case", CASES, ids=lambda c: c.case_id if hasattr(c, "case_id") else "case")
def test_cross_provider_streaming_join_regressions(case: JoinCase):
    ctx = case.builder()
    # распаковываем контекст builder-а
    if case.case_id in {"small_intersection_12k_25"}:
        left_df, right_df, _ = ctx
    elif case.case_id in {"overflow_sum_hot1000x6", "overflow_single_key_6000"}:
        left_df, right_df, _ = ctx
    elif case.case_id in {"m2m_small_overlap_small_output"}:
        left_df, right_df, _, _ = ctx
    elif case.case_id in {"m2m_limit_short_circuit"}:
        left_df, right_df, _ = ctx
    else:
        # fallback: первые два значения должны быть dataframes
        left_df, right_df = ctx[0], ctx[1]

    comp, spy_left, spy_right = make_composite(
        left_df,
        right_df,
        cardinality=case.cardinality,
        max_join_rows_per_batch=case.max_join_rows_per_batch,
        max_right_rows_per_batch=case.max_right_rows_per_batch,
    )

    req = _mk_query(case.limit)
    res = comp.fetch("t", selectors=req.model_dump())

    # базовая проверка: точно сработал composite path
    meta = getattr(res, "meta", {}) or {}
    assert meta.get("composite") is True, f"Expected composite execution, meta={meta}"

    # кейс-специфичные проверки
    case.check(res, spy_left, spy_right, ctx)
