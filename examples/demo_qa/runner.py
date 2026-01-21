from __future__ import annotations

import datetime
import json
import re
import statistics
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, NotRequired, TypedDict

from fetchgraph.core import create_generic_agent
from fetchgraph.core.models import TaskProfile
from fetchgraph.utils import set_run_id


@dataclass
class RunTimings:
    plan_s: float | None = None
    fetch_s: float | None = None
    synth_s: float | None = None
    total_s: float | None = None


@dataclass
class ExpectedCheck:
    mode: str
    expected: str
    passed: bool
    detail: str | None = None


@dataclass
class RunArtifacts:
    run_id: str
    run_dir: Path
    question: str
    plan: Dict[str, object] | None = None
    context: Dict[str, object] | None = None
    answer: str | None = None
    raw_synth: str | None = None
    error: str | None = None
    timings: RunTimings = field(default_factory=RunTimings)
    plan_only: bool = False


@dataclass
class RunResult:
    id: str
    question: str
    status: str
    checked: bool
    reason: str | None
    details: Dict[str, object] | None
    artifacts_dir: str
    duration_ms: int
    tags: list[str]
    answer: str | None = None
    error: str | None = None
    plan_path: str | None = None
    timings: RunTimings | None = None
    expected_check: ExpectedCheck | None = None

    def to_json(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "id": self.id,
            "question": self.question,
            "status": self.status,
            "checked": self.checked,
            "reason": self.reason,
            "details": self.details,
            "artifacts_dir": self.artifacts_dir,
            "duration_ms": self.duration_ms,
            "tags": self.tags,
            "answer": self.answer,
            "error": self.error,
            "plan_path": self.plan_path,
            "timings": self.timings.__dict__ if self.timings else None,
        }
        if self.expected_check:
            payload["expected_check"] = self.expected_check.__dict__
        return payload


@dataclass
class Case:
    id: str
    question: str
    expected: str | None = None
    expected_regex: str | None = None
    expected_contains: str | None = None
    tags: List[str] = field(default_factory=list)
    skip: bool = False

    @property
    def has_asserts(self) -> bool:
        return any([self.expected, self.expected_regex, self.expected_contains])


class AgentRunner:
    def __init__(self, llm, provider) -> None:
        def saver(feature_name: str, parsed: object) -> None:
            # Placeholder to satisfy BaseGraphAgent.saver; artifacts captured elsewhere.
            return None

        task_profile = TaskProfile(
            task_name="Demo QA",
            goal="Answer analytics questions over the demo dataset",
            output_format="Plain text answer",
            focus_hints=[
                "Prefer aggregates",
                "Use concise answers",
            ],
        )

        self.agent = create_generic_agent(
            llm_invoke=llm,
            providers={provider.name: provider},
            saver=saver,
            task_profile=task_profile,
        )

    def run_question(
        self,
        case: Case,
        run_id: str,
        run_dir: Path,
        *,
        plan_only: bool = False,
        event_logger: EventLogger | None = None,
    ) -> RunArtifacts:
        set_run_id(run_id)
        artifacts = RunArtifacts(run_id=run_id, run_dir=run_dir, question=case.question, plan_only=plan_only)

        started = time.perf_counter()
        try:
            if event_logger:
                event_logger.emit({"type": "plan_started", "case_id": case.id})
            plan_started = time.perf_counter()
            plan = self.agent._plan(case.question)  # type: ignore[attr-defined]
            artifacts.timings.plan_s = time.perf_counter() - plan_started
            artifacts.plan = plan.model_dump()

            if event_logger:
                event_logger.emit({"type": "plan_built", "case_id": case.id, "plan_path": str(run_dir / "plan.json")})
            if not plan_only:
                if event_logger:
                    event_logger.emit({"type": "fetch_started", "case_id": case.id})
                fetch_started = time.perf_counter()
                ctx = self.agent._fetch(case.question, plan)  # type: ignore[attr-defined]
                artifacts.timings.fetch_s = time.perf_counter() - fetch_started
                artifacts.context = {k: v.text for k, v in (ctx or {}).items()} if ctx else {}

                if event_logger:
                    event_logger.emit({"type": "fetch_finished", "case_id": case.id})
                if event_logger:
                    event_logger.emit({"type": "synth_started", "case_id": case.id})
                synth_started = time.perf_counter()
                draft = self.agent._synthesize(case.question, ctx, plan)  # type: ignore[attr-defined]
                artifacts.timings.synth_s = time.perf_counter() - synth_started
                artifacts.raw_synth = str(draft)
                parsed = self.agent.domain_parser(draft)
                artifacts.answer = str(parsed)
                if event_logger:
                    event_logger.emit({"type": "synth_finished", "case_id": case.id})
        except Exception as exc:  # pragma: no cover - demo fallback
            artifacts.error = str(exc)
        finally:
            artifacts.timings.total_s = time.perf_counter() - started

        return artifacts


def build_agent(llm, provider) -> AgentRunner:
    return AgentRunner(llm, provider)


def _save_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _save_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_artifacts(artifacts: RunArtifacts) -> None:
    artifacts.run_dir.mkdir(parents=True, exist_ok=True)
    if artifacts.plan is not None:
        _save_json(artifacts.run_dir / "plan.json", artifacts.plan)
    if artifacts.context is not None:
        _save_json(artifacts.run_dir / "context.json", artifacts.context)
    if artifacts.answer is not None:
        _save_text(artifacts.run_dir / "answer.txt", artifacts.answer)
    if artifacts.raw_synth is not None:
        _save_text(artifacts.run_dir / "raw_synth.txt", artifacts.raw_synth)
    if artifacts.error is not None:
        _save_text(artifacts.run_dir / "error.txt", artifacts.error)


def save_status(result: RunResult) -> None:
    status_path = Path(result.artifacts_dir) / "status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    _save_json(status_path, result.to_json())


def _stringify(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _normalize_text(value: str) -> str:
    return value.strip().casefold()


def _normalize_strings(values: Iterable[object]) -> list[str]:
    return [_normalize_text(str(value)) for value in values]


def _match_expected(case: Case, answer: str | None) -> ExpectedCheck | None:
    if not case.has_asserts:
        return None
    expected_value = _stringify(case.expected) or _stringify(case.expected_regex) or _stringify(case.expected_contains) or ""
    if answer is None:
        return ExpectedCheck(mode="none", expected=expected_value, passed=False, detail="no answer")
    if case.expected is not None:
        expected_str = _stringify(case.expected) or ""
        if isinstance(case.expected, (list, tuple, set)):
            expected_items = _normalize_strings(case.expected)
            answer_items = _normalize_strings(answer) if isinstance(answer, (list, tuple, set)) else []
            if isinstance(case.expected, set) or isinstance(answer, set):
                passed = set(expected_items) == set(answer_items)
            else:
                passed = expected_items == answer_items
        else:
            passed = _normalize_text(answer) == _normalize_text(expected_str)
        detail = None if passed else f"expected={expected_str!r}, got={answer!r}"
        return ExpectedCheck(mode="exact", expected=expected_str, passed=passed, detail=detail)
    if case.expected_regex is not None:
        expected_regex = _stringify(case.expected_regex) or ""
        pattern = re.compile(expected_regex)
        passed = bool(pattern.search(answer))
        detail = None if passed else f"regex {expected_regex!r} not found"
        return ExpectedCheck(mode="regex", expected=expected_regex, passed=passed, detail=detail)
    if case.expected_contains is not None:
        expected_contains = _stringify(case.expected_contains) or ""
        passed = _normalize_text(expected_contains) in _normalize_text(answer)
        detail = None if passed else f"expected to contain {expected_contains!r}"
        return ExpectedCheck(mode="contains", expected=expected_contains, passed=passed, detail=detail)
    return None


def _build_result(
    case: Case, artifacts: RunArtifacts, run_dir: Path, expected_check: ExpectedCheck | None
) -> RunResult:
    status = "unchecked"
    reason: str | None = None
    details: Dict[str, object] | None = None

    if artifacts.error:
        status = "error"
        reason = artifacts.error
        details = {"error": artifacts.error}
    elif expected_check:
        status = "ok" if expected_check.passed else "mismatch"
        reason = expected_check.detail
        details = {"expected_check": expected_check.__dict__}
    else:
        status = "plan_only" if artifacts.plan_only else "unchecked"
        reason = "plan-only" if artifacts.plan_only else "no expectations provided"
        details = {"note": reason}

    plan_path = str(run_dir / "plan.json") if artifacts.plan is not None else None
    duration_ms = int((artifacts.timings.total_s or 0.0) * 1000)
    return RunResult(
        id=case.id,
        question=case.question,
        status=status,
        checked=case.has_asserts,
        reason=reason,
        details=details,
        artifacts_dir=str(run_dir),
        duration_ms=duration_ms,
        tags=list(case.tags),
        answer=artifacts.answer,
        error=artifacts.error,
        plan_path=plan_path,
        timings=artifacts.timings,
        expected_check=expected_check,
    )


def run_one(
    case: Case,
    runner: AgentRunner,
    artifacts_root: Path,
    *,
    plan_only: bool = False,
    event_logger: EventLogger | None = None,
    run_dir: Path | None = None,
) -> RunResult:
    if run_dir is None:
        run_id = uuid.uuid4().hex[:8]
        run_dir = artifacts_root / f"{case.id}_{run_id}"
    else:
        run_id = run_dir.name.split("_")[-1]

    case_logger = event_logger.for_case(case.id, run_dir / "events.jsonl") if event_logger else None
    if case_logger:
        case_logger.emit({"type": "case_started", "case_id": case.id, "run_dir": str(run_dir)})
    if case.skip:
        run_dir.mkdir(parents=True, exist_ok=True)
        _save_text(run_dir / "skipped.txt", "Skipped by request")
        result = RunResult(
            id=case.id,
            question=case.question,
            status="skipped",
            checked=False,
            reason="skipped",
            details=None,
            artifacts_dir=str(run_dir),
            duration_ms=0,
            tags=list(case.tags),
            answer=None,
            error=None,
            plan_path=None,
            timings=RunTimings(),
            expected_check=None,
        )
        save_status(result)
        if case_logger:
            case_logger.emit({"type": "case_finished", "case_id": case.id, "status": "skipped"})
        return result

    artifacts = runner.run_question(case, run_id, run_dir, plan_only=plan_only, event_logger=case_logger)
    save_artifacts(artifacts)

    expected_check = None if plan_only else _match_expected(case, artifacts.answer)
    result = _build_result(case, artifacts, run_dir, expected_check)
    save_status(result)
    if case_logger:
        if result.status == "error":
            case_logger.emit(
                {
                    "type": "case_failed",
                    "case_id": case.id,
                    "status": result.status,
                    "reason": result.reason,
                    "artifacts_dir": result.artifacts_dir,
                }
            )
        case_logger.emit(
            {
                "type": "case_finished",
                "case_id": case.id,
                "status": result.status,
                "duration_ms": result.duration_ms,
                "artifacts_dir": result.artifacts_dir,
            }
        )
    return result


def summarize(results: Iterable[RunResult]) -> Dict[str, object]:
    totals = {"ok": 0, "mismatch": 0, "failed": 0, "error": 0, "skipped": 0, "unchecked": 0, "plan_only": 0}
    total_times: List[float] = []
    checked_total = 0
    checked_ok = 0
    unchecked_no_assert = 0
    plan_only = 0
    per_tag: Dict[str, Dict[str, object]] = {}
    for res in results:
        totals[res.status] = totals.get(res.status, 0) + 1
        if res.duration_ms is not None:
            total_times.append(res.duration_ms / 1000)
        if res.checked and res.status in {"ok", "mismatch", "failed", "error"}:
            checked_total += 1
        if res.status == "ok" and res.checked:
            checked_ok += 1
        if res.status == "unchecked":
            unchecked_no_assert += 1
        if res.status == "plan_only":
            plan_only += 1
        for tag in res.tags:
            bucket = per_tag.setdefault(
                tag, {"ok": 0, "mismatch": 0, "failed": 0, "error": 0, "skipped": 0, "unchecked": 0, "plan_only": 0}
            )
            bucket[res.status] = bucket.get(res.status, 0) + 1
            bucket["total"] = bucket.get("total", 0) + 1

    summary: Dict[str, object] = {
        "total": sum(totals.values()),
        "checked_total": checked_total,
        "checked_ok": checked_ok,
        "unchecked_no_assert": unchecked_no_assert,
        "plan_only": plan_only,
        "summary_by_tag": {tag: per_tag[tag] for tag in sorted(per_tag)},
        **totals,
    }
    if total_times:
        summary["avg_total_s"] = statistics.fmean(total_times)
        summary["median_total_s"] = statistics.median(total_times)
    else:
        summary["avg_total_s"] = None
        summary["median_total_s"] = None

    for tag, bucket in per_tag.items():
        total = bucket.get("total", 0)
        checked_total_tag = (bucket.get("ok", 0) or 0) + (bucket.get("mismatch", 0) or 0) + (
            bucket.get("failed", 0) or 0
        ) + (bucket.get("error", 0) or 0)
        bucket["checked_total"] = checked_total_tag
        non_skipped = total - (bucket.get("skipped", 0) or 0)
        if non_skipped > 0:
            bucket["pass_rate"] = (bucket.get("ok", 0) or 0) / non_skipped
        else:
            bucket["pass_rate"] = None
    return summary


def load_cases(path: Path) -> List[Case]:
    if not path.exists():
        raise FileNotFoundError(f"Cases file not found: {path}")
    cases: List[Case] = []
    seen_ids: set[str] = set()
    text = path.read_text(encoding="utf-8")
    stripped = text.lstrip()

    def add_case(payload: Mapping[str, object], location: str) -> None:
        if not isinstance(payload, Mapping):
            raise ValueError(f"Case on {location} must be an object")
        if "id" not in payload or "question" not in payload:
            raise ValueError(f"Case on {location} missing required fields 'id' and 'question'")
        case_id = str(payload["id"])
        if case_id in seen_ids:
            raise ValueError(f"Duplicate case id {case_id!r} on {location}")
        seen_ids.add(case_id)
        expected = payload.get("expected")
        expected_regex = payload.get("expected_regex")
        expected_contains = payload.get("expected_contains")
        for field_name, val in [
            ("expected", expected),
            ("expected_regex", expected_regex),
            ("expected_contains", expected_contains),
        ]:
            if val is not None and str(val).strip() == "":
                raise ValueError(f"{field_name} must not be empty on {location}")
        if expected_regex is not None:
            try:
                re.compile(expected_regex)
            except re.error as exc:
                raise ValueError(f"Invalid expected_regex on {location}: {exc}") from exc
        case = Case(
            id=case_id,
            question=str(payload["question"]),
            expected=expected,
            expected_regex=expected_regex,
            expected_contains=expected_contains,
            tags=list(payload.get("tags", []) or []),
            skip=bool(payload.get("skip", False)),
        )
        cases.append(case)

    if stripped.startswith("["):
        try:
            payloads = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON array: {exc}") from exc
        if not isinstance(payloads, list):
            raise ValueError("Cases JSON must be an array of objects")
        for index, payload in enumerate(payloads, start=1):
            add_case(payload, f"array index {index}")
        return cases

    for lineno, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON on line {lineno}: {exc}") from exc
        add_case(payload, f"line {lineno}")
    return cases


def _build_timings(payload: Mapping[str, object] | None) -> RunTimings | None:
    if not payload:
        return None
    return RunTimings(
        plan_s=payload.get("plan_s"),  # type: ignore[arg-type]
        fetch_s=payload.get("fetch_s"),  # type: ignore[arg-type]
        synth_s=payload.get("synth_s"),  # type: ignore[arg-type]
        total_s=payload.get("total_s"),  # type: ignore[arg-type]
    )


def _build_expected_check(payload: Mapping[str, object] | None) -> ExpectedCheck | None:
    if not payload:
        return None
    return ExpectedCheck(
        mode=str(payload.get("mode", "")),
        expected=str(payload.get("expected", "")),
        passed=bool(payload.get("passed", False)),
        detail=payload.get("detail"),  # type: ignore[arg-type]
    )


def _duration_from_payload(payload: Mapping[str, object]) -> int:
    if "duration_ms" in payload and payload["duration_ms"] is not None:
        try:
            return int(payload["duration_ms"])  # type: ignore[arg-type]
        except Exception:
            pass
    timings = payload.get("timings")
    if isinstance(timings, Mapping) and timings.get("total_s") is not None:
        try:
            return int(float(timings["total_s"]) * 1000)  # type: ignore[arg-type]
        except Exception:
            return 0
    return 0


def _run_result_from_payload(payload: Mapping[str, object]) -> RunResult:
    expected_check = _build_expected_check(payload.get("expected_check") if isinstance(payload, Mapping) else None)
    timings = _build_timings(payload.get("timings") if isinstance(payload, Mapping) else None)
    checked = bool(payload.get("checked", False))
    if expected_check and not checked:
        checked = True
    status = str(payload.get("status", "error"))
    duration_ms = _duration_from_payload(payload)
    reason = payload.get("reason")  # type: ignore[arg-type]
    details = payload.get("details") if isinstance(payload.get("details"), dict) else None
    artifacts_dir = str(payload.get("artifacts_dir", ""))
    if not artifacts_dir:
        raise ValueError("artifacts_dir missing in result payload")
    return RunResult(
        id=str(payload.get("id", "")),
        question=str(payload.get("question", "")),
        status=status,
        checked=checked,
        reason=reason,
        details=details,
        artifacts_dir=artifacts_dir,
        duration_ms=duration_ms,
        tags=list(payload.get("tags", []) or []),
        answer=payload.get("answer"),  # type: ignore[arg-type]
        error=payload.get("error"),  # type: ignore[arg-type]
        plan_path=payload.get("plan_path"),  # type: ignore[arg-type]
        timings=timings,
        expected_check=expected_check,
    )


def load_results(path: Path) -> Dict[str, RunResult]:
    results: Dict[str, RunResult] = {}
    if not path.exists():
        raise FileNotFoundError(f"Results file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid result JSON on line {lineno}: {exc}") from exc
            result = _run_result_from_payload(payload)
            if result.id in results:
                raise ValueError(f"Duplicate result id {result.id!r} on line {lineno}")
            results[result.id] = result
    return results


def bad_statuses(fail_on: str, require_assert: bool) -> set[str]:
    unchecked = {"unchecked", "plan_only"}
    bad = {"error", "failed", "mismatch"}
    if fail_on == "error":
        bad = {"error"}
    elif fail_on in {"unchecked", "any"}:
        bad |= unchecked
    elif fail_on == "skipped":
        bad |= {"skipped"}

    if require_assert:
        bad |= unchecked

    return bad


def is_failure(status: str, fail_on: str, require_assert: bool) -> bool:
    return status in bad_statuses(fail_on, require_assert)


def _artifact_links(res: RunResult) -> dict[str, str]:
    links: dict[str, str] = {}
    base = Path(res.artifacts_dir)
    for name in ["plan.json", "answer.txt", "raw_synth.txt", "status.json"]:
        path = base / name
        if path.exists():
            links[name] = str(path)
    return links


def _reason(res: RunResult) -> str:
    if res.reason:
        return res.reason
    if res.error:
        return res.error
    if res.expected_check and res.expected_check.detail:
        return res.expected_check.detail
    return ""


def _median_duration(results: Mapping[str, RunResult]) -> float | None:
    durations = [res.duration_ms for res in results.values() if res.duration_ms is not None]
    if not durations:
        return None
    durations.sort()
    mid = len(durations) // 2
    if len(durations) % 2 == 1:
        return durations[mid] / 1000
    return (durations[mid - 1] + durations[mid]) / 2000


def _coerce_int(value: object | None) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _count_bad_from_summary(counts: Mapping[str, object], fail_on: str, require_assert: bool) -> int:
    bad = bad_statuses(fail_on, require_assert)
    total = 0
    for status in bad:
        total += _coerce_int(counts.get(status, 0))
    return total


def diff_runs(
    base_results: Iterable[RunResult],
    new_results: Iterable[RunResult],
    *,
    fail_on: str,
    require_assert: bool,
) -> DiffReport:
    base_by_id = {res.id: res for res in base_results}
    new_by_id = {res.id: res for res in new_results}
    all_ids = sorted(set(base_by_id.keys()) | set(new_by_id.keys()))

    bad = bad_statuses(fail_on, require_assert)

    def _is_bad(res: RunResult | None) -> bool:
        return bool(res and res.status in bad)

    def _entry(case_id: str, base_res: RunResult | None, new_res: RunResult | None) -> DiffCaseChange:
        artifacts: dict[str, str]
        if new_res is None:
            artifacts = {}
        else:
            artifacts = _artifact_links(new_res)
        return {
            "id": case_id,
            "from": base_res.status if base_res else None,
            "to": new_res.status if new_res else "missing",
            "reason": _reason(new_res) if new_res else "missing in new results",
            "artifacts": artifacts,
        }

    new_fail: list[DiffCaseChange] = []
    fixed: list[DiffCaseChange] = []
    still_fail: list[DiffCaseChange] = []
    changed_status: list[DiffStatusChange] = []
    new_cases: list[str] = []

    for case_id in all_ids:
        new_res = new_by_id.get(case_id)
        base_res = base_by_id.get(case_id)
        base_bad = _is_bad(base_res)
        new_bad = True if new_res is None else _is_bad(new_res)

        if base_res is None:
            new_cases.append(case_id)
            if new_bad:
                new_fail.append(_entry(case_id, base_res, new_res))
        elif new_res is None:
            changed_status.append({"id": case_id, "from": base_res.status, "to": "missing"})
        else:
            if base_res.status != new_res.status:
                changed_status.append({"id": case_id, "from": base_res.status, "to": new_res.status})

        if base_res is None:
            continue
        if new_res is None:
            entry = _entry(case_id, base_res, new_res)
            if base_bad:
                still_fail.append(entry)
            else:
                new_fail.append(entry)
            continue

        if not base_bad and new_bad:
            new_fail.append(_entry(case_id, base_res, new_res))
        elif base_bad and not new_bad:
            fixed.append(_entry(case_id, base_res, new_res))
        elif base_bad and new_bad:
            still_fail.append(_entry(case_id, base_res, new_res))

    new_fail = sorted(new_fail, key=lambda r: r.get("id", ""))
    fixed = sorted(fixed, key=lambda r: r.get("id", ""))
    still_fail = sorted(still_fail, key=lambda r: r.get("id", ""))
    changed_status = sorted(changed_status, key=lambda r: r.get("id", ""))
    new_cases = sorted(new_cases)

    base_counts = summarize(base_by_id.values())
    new_counts = summarize(new_by_id.values())
    base_total_cases = len(base_by_id)
    new_total_cases = len(new_by_id)
    overlap_ids = set(base_by_id) & set(new_by_id)
    base_only_count = base_total_cases - len(overlap_ids)
    new_only_count = new_total_cases - len(overlap_ids)
    base_med = _median_duration(base_by_id)
    new_med = _median_duration(new_by_id)
    base_avg = base_counts.get("avg_total_s")
    new_avg = new_counts.get("avg_total_s")

    def _count_delta(key: str) -> int | float | None:
        base_val = base_counts.get(key)
        new_val = new_counts.get(key)
        if isinstance(base_val, (int, float)) and isinstance(new_val, (int, float)):
            return new_val - base_val
        return None

    delta_keys = (
        "total",
        "ok",
        "mismatch",
        "failed",
        "error",
        "skipped",
        "unchecked",
        "plan_only",
    )
    count_deltas = {k: _count_delta(k) for k in delta_keys}

    return {
        "all_ids": all_ids,
        "new_fail": new_fail,
        "fixed": fixed,
        "still_fail": still_fail,
        "changed_status": changed_status,
        "new_cases": new_cases,
        "base_total_cases": base_total_cases,
        "new_total_cases": new_total_cases,
        "base_only_count": base_only_count,
        "new_only_count": new_only_count,
        "base_counts": base_counts,
        "new_counts": new_counts,
        "counts_delta": count_deltas,
        "base_median": base_med,
        "new_median": new_med,
        "base_avg": base_avg,
        "new_avg": new_avg,
        "median_delta": (new_med - base_med) if (new_med is not None and base_med is not None) else None,
        "avg_delta": (new_avg - base_avg) if (isinstance(new_avg, (int, float)) and isinstance(base_avg, (int, float))) else None,
        "base_bad_total": _count_bad_from_summary(base_counts, fail_on, require_assert),
        "new_bad_total": _count_bad_from_summary(new_counts, fail_on, require_assert),
        "fail_on": fail_on,
        "require_assert": require_assert,
    }


def format_status_line(result: RunResult) -> str:
    timing = f"{result.duration_ms / 1000:.2f}s"
    if result.status == "ok":
        return f"OK {result.id} {timing}"
    if result.status == "skipped":
        return f"SKIP {result.id}"
    if result.status in {"unchecked", "plan_only"}:
        return f"{result.status.upper()} {result.id} {timing}"
    reason = result.reason or ""
    return f"FAIL {result.id} {result.status} ({reason or 'unknown'}) {timing}"


class EventLogger:
    def __init__(self, path: Path | None, run_id: str):
        self.path = path
        self.run_id = run_id
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: Dict[str, object]) -> None:
        if not self.path:
            return
        now = datetime.datetime.now(datetime.timezone.utc)
        payload = {"timestamp": now.isoformat().replace("+00:00", "Z"), "run_id": self.run_id, **event}
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def for_case(self, case_id: str, path: Path | None = None) -> "EventLogger":
        if path is None:
            return self
        return EventLogger(path, self.run_id)


DiffCaseChange = TypedDict(
    "DiffCaseChange",
    {
        "id": str,
        "from": str | None,
        "to": str | None,
        "reason": str,
        "artifacts": Mapping[str, str],
    },
)


DiffStatusChange = TypedDict(
    "DiffStatusChange",
    {
        "id": str,
        "from": str | None,
        "to": str | None,
    },
)


class DiffReport(TypedDict):
    all_ids: list[str]
    new_fail: list[DiffCaseChange]
    fixed: list[DiffCaseChange]
    still_fail: list[DiffCaseChange]
    changed_status: list[DiffStatusChange]
    new_cases: list[str]
    base_total_cases: int
    new_total_cases: int
    base_only_count: int
    new_only_count: int
    base_counts: Dict[str, object]
    new_counts: Dict[str, object]
    counts_delta: Dict[str, int | float | None]
    base_median: float | None
    new_median: float | None
    base_avg: float | None
    new_avg: float | None
    median_delta: float | None
    avg_delta: float | None
    base_bad_total: int
    new_bad_total: int
    fail_on: str
    require_assert: bool
    baseline_path: NotRequired[str]


__all__ = [
    "AgentRunner",
    "Case",
    "ExpectedCheck",
    "RunArtifacts",
    "RunResult",
    "EventLogger",
    "build_agent",
    "bad_statuses",
    "diff_runs",
    "format_status_line",
    "is_failure",
    "load_results",
    "load_cases",
    "run_one",
    "save_artifacts",
    "save_status",
    "summarize",
    "_match_expected",
]
