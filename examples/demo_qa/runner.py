from __future__ import annotations

import json
import re
import statistics
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List

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


@dataclass
class RunResult:
    id: str
    question: str
    status: str
    answer: str | None
    error: str | None
    plan_path: str | None
    artifacts_dir: str
    timings: RunTimings
    expected_check: ExpectedCheck | None = None

    def to_json(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "id": self.id,
            "question": self.question,
            "status": self.status,
            "answer": self.answer,
            "error": self.error,
            "plan_path": self.plan_path,
            "artifacts_dir": self.artifacts_dir,
            "timings": self.timings.__dict__,
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

    def run_question(self, question: str, run_id: str, run_dir: Path) -> RunArtifacts:
        set_run_id(run_id)
        artifacts = RunArtifacts(run_id=run_id, run_dir=run_dir, question=question)

        started = time.perf_counter()
        try:
            plan_started = time.perf_counter()
            plan = self.agent._plan(question)  # type: ignore[attr-defined]
            artifacts.timings.plan_s = time.perf_counter() - plan_started
            artifacts.plan = plan.model_dump()

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


def _match_expected(case: Case, answer: str | None) -> ExpectedCheck | None:
    if answer is None:
        return ExpectedCheck(mode="none", expected="", passed=False, detail="no answer")
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


def run_one(case: Case, runner: AgentRunner, artifacts_root: Path) -> RunResult:
    run_id = uuid.uuid4().hex[:8]
    run_dir = artifacts_root / f"{case.id}_{run_id}"
    if case.skip:
        run_dir.mkdir(parents=True, exist_ok=True)
        _save_text(run_dir / "skipped.txt", "Skipped by request")
        return RunResult(
            id=case.id,
            question=case.question,
            status="skipped",
            answer=None,
            error=None,
            plan_path=None,
            artifacts_dir=str(run_dir),
            timings=RunTimings(),
            expected_check=None,
        )
    artifacts = runner.run_question(case.question, run_id, run_dir)
    save_artifacts(artifacts)

    expected_check = _match_expected(case, artifacts.answer)
    status = "ok"
    if artifacts.error:
        status = "error"
    elif expected_check and not expected_check.passed:
        status = "mismatch"

    plan_path = str(run_dir / "plan.json") if artifacts.plan is not None else None
    result = RunResult(
        id=case.id,
        question=case.question,
        status=status,
        answer=artifacts.answer,
        error=artifacts.error,
        plan_path=plan_path,
        artifacts_dir=str(run_dir),
        timings=artifacts.timings,
        expected_check=expected_check,
    )
    return result


def summarize(results: Iterable[RunResult]) -> Dict[str, object]:
    totals = {"ok": 0, "error": 0, "mismatch": 0, "skipped": 0}
    total_times: List[float] = []
    for res in results:
        totals[res.status] = totals.get(res.status, 0) + 1
        if res.timings.total_s is not None:
            total_times.append(res.timings.total_s)

    summary: Dict[str, object] = {
        "total": sum(totals.values()),
        **totals,
    }
    if total_times:
        summary["avg_total_s"] = statistics.fmean(total_times)
        summary["median_total_s"] = statistics.median(total_times)
    else:
        summary["avg_total_s"] = None
        summary["median_total_s"] = None
    return summary


def load_cases(path: Path) -> List[Case]:
    if not path.exists():
        raise FileNotFoundError(f"Cases file not found: {path}")
    cases: List[Case] = []
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
            case = Case(
                id=str(payload["id"]),
                question=str(payload["question"]),
                expected=payload.get("expected"),
                expected_regex=payload.get("expected_regex"),
                expected_contains=payload.get("expected_contains"),
                tags=list(payload.get("tags", []) or []),
                skip=bool(payload.get("skip", False)),
            )
            cases.append(case)
    return cases


def format_status_line(result: RunResult) -> str:
    timing = f"{result.timings.total_s:.2f}s" if result.timings.total_s is not None else "n/a"
    if result.status == "ok":
        return f"OK {result.id} {timing}"
    if result.status == "skipped":
        return f"SKIP {result.id}"
    reason = result.error or (result.expected_check.detail if result.expected_check else "")
    return f"FAIL {result.id} {result.status} ({reason or 'unknown'}) {timing}"


__all__ = [
    "AgentRunner",
    "Case",
    "ExpectedCheck",
    "RunArtifacts",
    "RunResult",
    "build_agent",
    "format_status_line",
    "load_cases",
    "run_one",
    "save_artifacts",
    "summarize",
]
