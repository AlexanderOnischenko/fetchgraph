from __future__ import annotations

import copy
import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import pytest
from pydantic import TypeAdapter, ValidationError

from fetchgraph.core.models import ContextFetchSpec, ProviderInfo
from fetchgraph.planning.normalize import (
    PlanNormalizer,
    PlanNormalizerOptions,
)
from fetchgraph.planning.normalize.plan_normalizer import SelectorNormalizationRule
from fetchgraph.relational.models import RelationalRequest
from fetchgraph.relational.normalize import normalize_relational_selectors

# -----------------------------
# Plan-trace parsing
# -----------------------------

_JSON_SPLIT_RE = re.compile(r"\n\s*\n", re.MULTILINE)


def _iter_json_objects_from_trace_text(text: str) -> Iterable[Dict[str, Any]]:
    parts = [p.strip() for p in _JSON_SPLIT_RE.split(text) if p.strip()]
    for part in parts:
        try:
            obj = json.loads(part)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


def _find_fixtures_dir() -> Optional[Path]:
    """
    Ищем папку `fixtures` вверх по дереву от текущего test-файла.
    Это устойчиво к вложенности tests/...
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        cand = parent / "fixtures"
        if cand.exists():
            return cand
    return None


@dataclass(frozen=True)
class TraceCase:
    trace_name: str
    spec_idx: int
    provider: str
    mode: str
    selectors: Dict[str, Any]
    bucket: str

    @property
    def case_id(self) -> str:
        # Важно: не использовать '::' — VSCode/pytest UI строят дерево по этому разделителю.
        return f"{self.bucket} | {self.trace_name} | spec[{self.spec_idx}] | {self.provider}"


def _load_trace_cases_from_fixtures() -> List[TraceCase]:
    """
    Ищет по бакетам:
      - fixtures/regressions_fixed/fetchgraph_plans.zip
      - fixtures/regressions_fixed/fetchgraph_plans/*.txt
      - fixtures/regressions_known_bad/fetchgraph_plans.zip
      - fixtures/regressions_known_bad/fetchgraph_plans/*.txt

    Достаёт спецификации из stage=before_normalize (plan.context_plan[*]).
    """
    fixtures_dir = _find_fixtures_dir()
    if fixtures_dir is None:
        pytest.skip("No fixtures dir found (expected .../fixtures).", allow_module_level=True)
        return []

    cases: List[TraceCase] = []
    buckets = ["regressions_fixed", "regressions_known_bad"]
    for bucket in buckets:
        zip_path = fixtures_dir / bucket / "fetchgraph_plans.zip"
        dir_path = fixtures_dir / bucket / "fetchgraph_plans"

        if zip_path.exists():
            with zipfile.ZipFile(zip_path) as zf:
                for name in sorted(zf.namelist()):
                    if not name.endswith("_plan_trace.txt"):
                        continue
                    text = zf.read(name).decode("utf-8", errors="replace")
                    cases.extend(_extract_before_specs(trace_name=name, text=text, bucket=bucket))
            continue

        if dir_path.exists():
            for p in sorted(dir_path.glob("*_plan_trace.txt")):
                text = p.read_text(encoding="utf-8", errors="replace")
                cases.extend(_extract_before_specs(trace_name=p.name, text=text, bucket=bucket))

    legacy_zip = fixtures_dir / "fetchgraph_plans.zip"
    legacy_dir = fixtures_dir / "fetchgraph_plans"
    if legacy_zip.exists():
        with zipfile.ZipFile(legacy_zip) as zf:
            for name in sorted(zf.namelist()):
                if not name.endswith("_plan_trace.txt"):
                    continue
                text = zf.read(name).decode("utf-8", errors="replace")
                cases.extend(_extract_before_specs(trace_name=name, text=text, bucket="regressions_fixed"))
    if legacy_dir.exists():
        for p in sorted(legacy_dir.glob("*_plan_trace.txt")):
            text = p.read_text(encoding="utf-8", errors="replace")
            cases.extend(_extract_before_specs(trace_name=p.name, text=text, bucket="regressions_fixed"))

    if not cases:
        pytest.skip(
            "No plan fixtures found. Put fetchgraph_plans.zip into fixtures/regressions_fixed "
            "or fixtures/regressions_known_bad, or unpack it to "
            "fixtures/regressions_fixed/fetchgraph_plans or fixtures/regressions_known_bad/fetchgraph_plans.",
            allow_module_level=True,
        )
    return cases


def _extract_before_specs(trace_name: str, text: str, *, bucket: str) -> List[TraceCase]:
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

        for idx, item in enumerate(context_plan):
            if not isinstance(item, dict):
                continue
            provider = item.get("provider")
            mode = item.get("mode") or "full"
            selectors = item.get("selectors")
            if not isinstance(provider, str) or not isinstance(selectors, dict):
                continue
            out.append(
                TraceCase(
                    trace_name=trace_name,
                    spec_idx=idx,
                    provider=provider,
                    mode=str(mode),
                    selectors=selectors,
                    bucket=bucket,
                )
            )
    return out


# -----------------------------
# Normalizer builder (for tests)
# -----------------------------


def _build_plan_normalizer(providers: Set[str]) -> PlanNormalizer:
    """
    Строим PlanNormalizer, который умеет нормализовать selectors для заданных providers
    по тому же контракту, что и в проде: validate -> normalize -> validate.
    """
    provider_catalog: Dict[str, ProviderInfo] = {
        name: ProviderInfo(name=name, capabilities=[]) for name in sorted(providers)
    }

    relational_rule = SelectorNormalizationRule(
        validator=TypeAdapter(RelationalRequest),
        normalize_selectors=normalize_relational_selectors,
    )

    normalizer_registry: Dict[str, SelectorNormalizationRule] = {
        name: relational_rule for name in sorted(providers)
    }

    # В тесте schema-фильтрацию лучше выключить: это отдельная ответственность
    # (и она может зависеть от selectors_schema в ProviderInfo, которого тут нет).
    opts = PlanNormalizerOptions(filter_selectors_by_schema=False)

    return PlanNormalizer(
        provider_catalog,
        normalizer_registry=normalizer_registry,
        options=opts,
    )


def _validate(adapter: TypeAdapter[Any], selectors: Any) -> bool:
    try:
        adapter.validate_python(selectors)
    except ValidationError:
        return False
    return True


def _parse_note(note: str) -> Dict[str, Any]:
    # notes в PlanNormalizer — это json строка
    try:
        obj = json.loads(note)
    except Exception:
        return {"raw": note}
    return obj if isinstance(obj, dict) else {"raw": note}


# -----------------------------
# Tests (contract-driven)
# -----------------------------

CASES = _load_trace_cases_from_fixtures()

# 1) Контрактный тест запускаем ТОЛЬКО на regressions_fixed
CASES_FIXED = [c for c in CASES if c.bucket == "regressions_fixed"]

# 2) Для общего набора — автоматически проставляем marks по bucket
CASES_ALL = [
    pytest.param(
        c,
        id=c.case_id,
        marks=(pytest.mark.known_bad,) if c.bucket == "regressions_known_bad" else (),
    )
    for c in CASES
]

NORMALIZER = _build_plan_normalizer({c.provider for c in CASES}) if CASES else None


@pytest.mark.parametrize("case", CASES_FIXED, ids=lambda c: c.case_id)
def test_plan_normalizer_contract_never_regresses_valid_inputs(case: TraceCase) -> None:
    """
    Контракт PlanNormalizer.normalize_specs():
      - если selectors валидны ДО -> после normalize_specs
        они должны остаться валидными и НЕ измениться.
    """
    assert NORMALIZER is not None

    rule = NORMALIZER.normalizer_registry.get(case.provider)
    assert rule is not None, f"No normalizer rule registered for provider={case.provider!r}"

    orig_selectors = copy.deepcopy(case.selectors)
    spec = ContextFetchSpec(provider=case.provider, mode=case.mode, selectors=copy.deepcopy(case.selectors))

    before_ok = _validate(rule.validator, spec.selectors)

    notes: List[str] = []
    out_specs = NORMALIZER.normalize_specs([spec], notes=notes)
    assert len(out_specs) == 1
    out = out_specs[0]

    after_ok = _validate(rule.validator, out.selectors)

    assert not (before_ok and not after_ok), (
        f"{case.case_id}: regression: selectors were valid before normalization "
        f"but invalid after.\n"
        f"Note: {_parse_note(notes[-1]) if notes else 'no_notes'}\n"
        f"Selectors(before): {json.dumps(spec.selectors, ensure_ascii=False)[:2000]}\n"
        f"Selectors(after):  {json.dumps(out.selectors, ensure_ascii=False)[:2000]}"
    )

    if before_ok:
        assert out.selectors == spec.selectors, (
            f"{case.case_id}: valid selectors must not be changed by normalize_specs.\n"
            f"Note: {_parse_note(notes[-1]) if notes else 'no_notes'}\n"
            f"Selectors(before): {json.dumps(spec.selectors, ensure_ascii=False)[:2000]}\n"
            f"Selectors(after):  {json.dumps(out.selectors, ensure_ascii=False)[:2000]}"
        )

    assert spec.selectors == orig_selectors, (
        f"{case.case_id}: normalize_specs must not mutate input selectors in-place."
    )


@pytest.mark.parametrize("case", CASES_ALL)
def test_regression_fixtures_invalid_inputs_are_fixed_by_plan_normalizer(case: TraceCase) -> None:
    """
    Если ДО selectors невалидны -> ПОСЛЕ normalize_specs они должны стать валидными.

    Важно: known_bad здесь будут (пока) красными — и это ок.
    В CI они исключаются через -m "not known_bad".
    """
    assert NORMALIZER is not None

    rule = NORMALIZER.normalizer_registry.get(case.provider)
    assert rule is not None, f"No normalizer rule registered for provider={case.provider!r}"

    spec = ContextFetchSpec(provider=case.provider, mode=case.mode, selectors=copy.deepcopy(case.selectors))

    before_ok = _validate(rule.validator, spec.selectors)
    if before_ok:
        return

    notes: List[str] = []
    out_specs = NORMALIZER.normalize_specs([spec], notes=notes)
    out = out_specs[0]

    after_ok = _validate(rule.validator, out.selectors)
    if after_ok:
        return

    err_before: Optional[list] = None
    err_after: Optional[list] = None

    try:
        rule.validator.validate_python(spec.selectors)
    except ValidationError as e:
        err_before = e.errors()

    try:
        rule.validator.validate_python(out.selectors)
    except ValidationError as e:
        err_after = e.errors()

    pytest.fail(
        f"{case.case_id}: expected PlanNormalizer to fix invalid selectors (error -> ok), "
        f"but still invalid after normalization.\n"
        f"Note: {_parse_note(notes[-1]) if notes else 'no_notes'}\n"
        f"Errors(before): {err_before}\n"
        f"Errors(after):  {err_after}\n"
        f"Selectors(before): {json.dumps(spec.selectors, ensure_ascii=False)[:2000]}\n"
        f"Selectors(after):  {json.dumps(out.selectors, ensure_ascii=False)[:2000]}"
    )

