from __future__ import annotations

import json
import re
import statistics
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional

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

    def run_question(self, question: str, run_id: str, run_dir: Path, *, plan_only: bool = False) -> RunArtifacts:
        set_run_id(run_id)
        artifacts = RunArtifacts(run_id=run_id, run_dir=run_dir, question=question, plan_only=plan_only)

        started = time.perf_counter()
        try:
            plan_started = time.perf_counter()
            plan = self.agent._plan(question)  # type: ignore[attr-defined]
            artifacts.timings.plan_s = time.perf_counter() - plan_started
            artifacts.plan = plan.model_dump()

            if not plan_only:
                fetch_started = time.perf_counter()
                ctx = self.agent._fetch(question, plan)  # type: ignore[attr-defined]
                artifacts.timings.fetch_s = time.perf_counter() - fetch_started
                artifacts.context = {k: v.text for k, v in (ctx or {}).items()} if ctx else {}

                synth_started = time.perf_counter()
                draft = self.agent._synthesize(question, ctx, plan)  # type: ignore[attr-defined]
                artifacts.timings.synth_s = time.perf_counter() - synth_started
                artifacts.raw_synth = str(draft)
                parsed = self.agent.domain_parser(draft)
                artifacts.answer = str(parsed)
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


def _match_expected(case: Case, answer: str | None) -> ExpectedCheck | None:
    if not case.has_asserts:
        return None
    expected_value = case.expected or case.expected_regex or case.expected_contains or ""
    if answer is None:
        return ExpectedCheck(mode="none", expected=expected_value, passed=False, detail="no answer")
    if case.expected is not None:
        passed = answer.strip() == case.expected.strip()
        detail = None if passed else f"expected={case.expected!r}, got={answer!r}"
        return ExpectedCheck(mode="exact", expected=case.expected, passed=passed, detail=detail)
    if case.expected_regex is not None:
        pattern = re.compile(case.expected_regex)
        passed = bool(pattern.search(answer))
        detail = None if passed else f"regex {case.expected_regex!r} not found"
        return ExpectedCheck(mode="regex", expected=case.expected_regex, passed=passed, detail=detail)
    if case.expected_contains is not None:
        passed = case.expected_contains in answer
        detail = None if passed else f"expected to contain {case.expected_contains!r}"
        return ExpectedCheck(mode="contains", expected=case.expected_contains, passed=passed, detail=detail)
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


def run_one(case: Case, runner: AgentRunner, artifacts_root: Path, *, plan_only: bool = False) -> RunResult:
    run_id = uuid.uuid4().hex[:8]
    run_dir = artifacts_root / f"{case.id}_{run_id}"
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
            answer=None,
            error=None,
            plan_path=None,
            timings=RunTimings(),
            expected_check=None,
        )
        save_status(result)
        return result

    artifacts = runner.run_question(case.question, run_id, run_dir, plan_only=plan_only)
    save_artifacts(artifacts)

    expected_check = None if plan_only else _match_expected(case, artifacts.answer)
    result = _build_result(case, artifacts, run_dir, expected_check)
    save_status(result)
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
        "summary_by_tag": per_tag,
        **totals,
    }
    if total_times:
        summary["avg_total_s"] = statistics.fmean(total_times)
        summary["median_total_s"] = statistics.median(total_times)
    else:
        summary["avg_total_s"] = None
        summary["median_total_s"] = None

    for tag, bucket in per_tag.items():
        times: List[float] = []
        # no per-tag timing collected; reuse overall average for simplicity
        if times:
            bucket["avg_total_s"] = statistics.fmean(times)
            bucket["median_total_s"] = statistics.median(times)
        else:
            bucket["avg_total_s"] = None
            bucket["median_total_s"] = None
        total = bucket.get("total", 0)
        checked_total_tag = (bucket.get("ok", 0) or 0) + (bucket.get("mismatch", 0) or 0) + (
            bucket.get("failed", 0) or 0
        )
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
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {lineno}: {exc}") from exc
            if "id" not in payload or "question" not in payload:
                raise ValueError(f"Case on line {lineno} missing required fields 'id' and 'question'")
            case_id = str(payload["id"])
            if case_id in seen_ids:
                raise ValueError(f"Duplicate case id {case_id!r} on line {lineno}")
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
                    raise ValueError(f"{field_name} must not be empty on line {lineno}")
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
            results[result.id] = result
    return results


def _bucket(status: str, checked: bool, require_assert: bool) -> str:
    if status == "ok":
        return "OK" if checked else "UNCHECKED"
    if status in {"mismatch", "failed", "error"}:
        return "BAD"
    if status in {"unchecked", "plan_only"}:
        return "BAD" if require_assert else "UNCHECKED"
    return "NEUTRAL"


def compare_results(
    baseline: Mapping[str, RunResult],
    current: Mapping[str, RunResult],
    *,
    require_assert: bool,
) -> Dict[str, object]:
    new_ok: List[str] = []
    regressed: List[str] = []
    still_ok: List[str] = []
    still_bad: List[str] = []
    new_unchecked: List[str] = []
    status_changes: Dict[str, Dict[str, str]] = {}
    new_cases: List[str] = []

    for case_id, res in current.items():
        base_res = baseline.get(case_id)
        new_bucket = _bucket(res.status, res.checked, require_assert)
        if base_res is None:
            new_cases.append(case_id)
            if new_bucket == "OK":
                new_ok.append(case_id)
            elif new_bucket == "BAD":
                still_bad.append(case_id)
            status_changes[case_id] = {"from": "new", "to": res.status}
            continue

        base_bucket = _bucket(base_res.status, base_res.checked, require_assert)
        if base_res.checked and res.status == "unchecked":
            new_unchecked.append(case_id)
        if base_bucket == "OK" and new_bucket in {"BAD", "UNCHECKED"}:
            regressed.append(case_id)
        elif base_bucket in {"BAD", "UNCHECKED"} and new_bucket == "OK":
            new_ok.append(case_id)
        elif base_bucket == "OK" and new_bucket == "OK":
            still_ok.append(case_id)
        elif base_bucket in {"BAD", "UNCHECKED"} and new_bucket in {"BAD", "UNCHECKED"}:
            still_bad.append(case_id)

        if base_res.status != res.status:
            status_changes[case_id] = {"from": base_res.status, "to": res.status}

    return {
        "new_ok": new_ok,
        "regressed": regressed,
        "still_ok": still_ok,
        "still_bad": still_bad,
        "new_unchecked": new_unchecked,
        "status_changes": status_changes,
        "new_cases": new_cases,
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


__all__ = [
    "AgentRunner",
    "Case",
    "ExpectedCheck",
    "RunArtifacts",
    "RunResult",
    "build_agent",
    "compare_results",
    "format_status_line",
    "load_results",
    "load_cases",
    "run_one",
    "save_artifacts",
    "save_status",
    "summarize",
    "_match_expected",
]
