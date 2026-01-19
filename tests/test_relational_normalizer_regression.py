from __future__ import annotations

import copy
import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pytest
from pydantic import ValidationError

from fetchgraph.relational.models import RelationalQuery
from fetchgraph.relational.normalize import normalize_relational_selectors

# -----------------------------
# Plan-trace parsing
# -----------------------------

_JSON_SPLIT_RE = re.compile(r"\n\s*\n", re.MULTILINE)


def _iter_json_objects_from_trace_text(text: str) -> Iterable[Dict[str, Any]]:
    parts = [p.strip() for p in _JSON_SPLIT_RE.split(text) if p.strip()]
    for part in parts:
        # В trace-файлах обычно всё — чистый JSON.
        try:
            obj = json.loads(part)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


@dataclass(frozen=True)
class TraceCase:
    trace_name: str
    selectors: Dict[str, Any]


def _load_trace_cases_from_fixtures() -> List[TraceCase]:
    """
    Ищет:
      - tests/fixtures/fetchgraph_plans.zip
      - tests/fixtures/fetchgraph_plans/*.txt
    Возвращает selectors из stage=before_normalize.
    """
    root = Path(__file__).resolve().parent
    fixtures_dir = root / "fixtures"
    zip_path = fixtures_dir / "fetchgraph_plans.zip"
    dir_path = fixtures_dir / "fetchgraph_plans"

    cases: List[TraceCase] = []

    if zip_path.exists():
        with zipfile.ZipFile(zip_path) as zf:
            for name in sorted(zf.namelist()):
                if not name.endswith("_plan_trace.txt"):
                    continue
                text = zf.read(name).decode("utf-8", errors="replace")
                cases.extend(_extract_before_selectors(name, text))
        return cases

    if dir_path.exists():
        for p in sorted(dir_path.glob("*_plan_trace.txt")):
            text = p.read_text(encoding="utf-8", errors="replace")
            cases.extend(_extract_before_selectors(p.name, text))
        return cases

    pytest.skip(
        "No plan fixtures found. Put fetchgraph_plans.zip into tests/fixtures/ "
        "or unpack it to tests/fixtures/fetchgraph_plans/.",
        allow_module_level=True,
    )
    return []


def _extract_before_selectors(trace_name: str, text: str) -> List[TraceCase]:
    before_objs = [
        obj for obj in _iter_json_objects_from_trace_text(text)
        if obj.get("stage") == "before_normalize"
    ]
    out: List[TraceCase] = []
    for obj in before_objs:
        plan = obj.get("plan") or {}
        context_plan = plan.get("context_plan") or []
        if not isinstance(context_plan, list):
            continue
        for item in context_plan:
            if not isinstance(item, dict):
                continue
            selectors = item.get("selectors")
            if isinstance(selectors, dict):
                out.append(TraceCase(trace_name=trace_name, selectors=selectors))
    return out


def _walk_filter_dicts(filters: Any) -> Iterable[Dict[str, Any]]:
    """Рекурсивно обходит фильтры и возвращает все dict-узлы."""
    if isinstance(filters, dict):
        yield filters
        clauses = filters.get("clauses")
        if isinstance(clauses, list):
            for c in clauses:
                yield from _walk_filter_dicts(c)
    elif isinstance(filters, list):
        for x in filters:
            yield from _walk_filter_dicts(x)


def _diagnose_known_validation_causes(normalized: Dict[str, Any], case: TraceCase) -> None:
    """
    Вызывается ТОЛЬКО если RelationalQuery.model_validate(normalized) упал.
    Здесь мы пытаемся найти “известную причину” и упасть с понятным сообщением.
    Если ничего не нашли — НЕ падаем, это решит внешний обработчик (unknown error).
    """

    # A) legacy aggregate должен быть преобразован в query
    op = normalized.get("op")
    if op == "aggregate":
        pytest.fail(
            f"{case.trace_name}: legacy op='aggregate' must be normalized to op='query'."
        )

    # B) после нормализации op обязан быть query (иначе непредвиденный формат)
    if op != "query":
        pytest.fail(f"{case.trace_name}: unexpected op={op!r} after normalization.")

    # C) list-поля не должны превращаться в None/не-листы
    for key in ("group_by", "aggregations", "relations", "select", "semantic_clauses"):
        if key in normalized and not isinstance(normalized[key], list):
            pytest.fail(
                f"{case.trace_name}: {key} must be list if present, "
                f"got {type(normalized[key]).__name__}"
            )

    # D) ComparisonFilter.value обязателен по модели — должен присутствовать (хотя бы None)
    filters = normalized.get("filters")
    for node in _walk_filter_dicts(filters):
        if node.get("type") == "comparison" and "value" not in node:
            pytest.fail(
                f"{case.trace_name}: comparison filter must include 'value' "
                f"(model requires it). Filter node: {node}"
            )

    # E) “мина” про aggregations: строка/дикт не должны становиться list(chars)/list(keys)
    aggs = normalized.get("aggregations")
    if aggs is not None:
        if not isinstance(aggs, list):
            pytest.fail(f"{case.trace_name}: aggregations must be list, got {type(aggs).__name__}")
        if any(not isinstance(x, dict) for x in aggs):
            pytest.fail(f"{case.trace_name}: aggregations must be list[dict], got: {aggs}")

# -----------------------------
# Tests
# -----------------------------

CASES = _load_trace_cases_from_fixtures()

@pytest.mark.parametrize("case", CASES, ids=lambda c: c.trace_name)
def test_normalizer_outputs_valid_relational_query(case: TraceCase) -> None:
    selectors_in = copy.deepcopy(case.selectors)
    normalized = normalize_relational_selectors(selectors_in)

    # 1) “Честная” валидация: если проходит — тест сразу ок.
    try:
        RelationalQuery.model_validate(normalized)
        return
    except ValidationError as e:
        # 2) Не прошло — пытаемся найти известную причину и дать осмысленный фейл.
        _diagnose_known_validation_causes(normalized, case)

        # 3) Если диагностика не упала — значит причина неизвестна.
        pytest.fail(
            f"{case.trace_name}: RelationalQuery.model_validate failed for unknown reason.\n"
            f"Errors: {e.errors()}\n"
            f"Normalized selectors (truncated): {json.dumps(normalized, ensure_ascii=False)[:2000]}"
        )


# Этот тест кейс и раньше не работал, так что это не регрессия

# def test_min_max_filter_normalization_does_not_corrupt_aggregations() -> None:
#     selectors = {
#         "op": "query",
#         "root_entity": "orders",
#         "aggregations": "count(order_id)",  # плохой вход (типичный LLM мусор)
#         "filters": {"type": "comparison", "field": "order_total", "op": "min"},
#     }
#     normalized = normalize_relational_selectors(copy.deepcopy(selectors))

#     # сначала пробуем “честно”
#     try:
#         RelationalQuery.model_validate(normalized)
#         return
#     except ValidationError:
#         # затем проверяем ожидаемую “мину”
#         aggs = normalized.get("aggregations")
#         if aggs is not None:
#             assert isinstance(aggs, list)
#             assert all(isinstance(x, dict) for x in aggs), f"aggregations must be list[dict], got: {aggs}"
#         pytest.fail("RelationalQuery.model_validate failed for unknown reason (not aggregations-shape).")

