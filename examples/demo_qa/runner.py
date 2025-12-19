from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
import traceback
from typing import Any, Dict, List, Optional

from fetchgraph.core import BaseGraphAgent, TaskProfile, create_generic_agent
from fetchgraph.core.models import Plan

from .provider_factory import build_provider


@dataclass
class Timings:
    plan_s: float | None = None
    fetch_s: float | None = None
    synth_s: float | None = None
    total_s: float | None = None


@dataclass
class ExpectedCheck:
    mode: str
    expected: str
    observed: str | None
    passed: bool
    details: str | None = None


@dataclass
class RunArtifacts:
    plan: Dict[str, Any] | None = None
    context: Dict[str, str] | None = None
    raw_synth: str | None = None
    answer: str | None = None


@dataclass
class RunResult:
    id: str
    question: str
    answer: str | None
    status: str
    timings: Timings = field(default_factory=Timings)
    expected_check: ExpectedCheck | None = None
    error: str | None = None
    plan_path: str | None = None
    context_path: str | None = None
    answer_path: str | None = None
    raw_synth_path: str | None = None
    tags: List[str] = field(default_factory=list)

    def to_json(self) -> str:
        payload = asdict(self)
        payload["timings"] = asdict(self.timings)
        if self.expected_check is not None:
            payload["expected_check"] = asdict(self.expected_check)
        return json.dumps(payload, ensure_ascii=False)


@dataclass
class CaseInput:
    id: str
    question: str
    expected: str | None = None
    expected_regex: str | None = None
    expected_contains: str | None = None
    tags: List[str] = field(default_factory=list)
    skip: bool = False

    @classmethod
    def from_obj(cls, obj: Dict[str, Any]) -> "CaseInput":
        if not isinstance(obj, dict):
            raise ValueError("Case must be a JSON object.")
        if "id" not in obj or "question" not in obj:
            raise ValueError("Case must contain 'id' and 'question'.")
        tags = obj.get("tags") or []
        if tags is None:
            tags = []
        if not isinstance(tags, list):
            raise ValueError("Case 'tags' must be a list if provided.")
        expected = obj.get("expected")
        expected_regex = obj.get("expected_regex")
        expected_contains = obj.get("expected_contains")
        return cls(
            id=str(obj["id"]),
            question=str(obj["question"]),
            expected=None if expected is None else str(expected),
            expected_regex=None if expected_regex is None else str(expected_regex),
            expected_contains=None if expected_contains is None else str(expected_contains),
            tags=[str(t) for t in tags],
            skip=bool(obj.get("skip", False)),
        )


def load_cases(path: Path) -> List[CaseInput]:
    if not path.exists():
        raise FileNotFoundError(f"Cases file not found: {path}")
    out: List[CaseInput] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {lineno}: {exc}") from exc
            try:
                case = CaseInput.from_obj(obj)
            except Exception as exc:
                raise ValueError(f"Invalid case at line {lineno}: {exc}") from exc
            out.append(case)
    return out


def build_agent(llm, provider) -> "AgentRunner":
    artifacts = RunArtifacts()

    def saver(feature_name: str, parsed: object) -> None:
        artifacts.answer = str(parsed)

    task_profile = TaskProfile(
        task_name="Demo QA",
        goal="Answer analytics questions over the demo dataset",
        output_format="Plain text answer",
        focus_hints=[
            "Prefer aggregates",
            "Use concise answers",
        ],
    )

    agent = create_generic_agent(
        llm_invoke=llm,
        providers={provider.name: provider},
        saver=saver,
        task_profile=task_profile,
    )
    return AgentRunner(agent=agent, artifacts=artifacts)


class AgentRunner:
    def __init__(self, agent: BaseGraphAgent, artifacts: RunArtifacts | None = None, run_id: str | None = None):
        self.agent = agent
        self.artifacts = artifacts or RunArtifacts()
        self.run_id = run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self._counter = 0

    def run_question(self, question: str, *, artifacts_root: Path | None = None) -> str:
        self._counter += 1
        adhoc_case = CaseInput(id=f"adhoc_{self._counter:04d}", question=question)
        result = self.run_one(adhoc_case, artifacts_root=artifacts_root)
        if result.status != "ok":
            detail = result.error or result.status
            raise RuntimeError(f"Run failed for question: {detail}")
        if result.answer is None:
            raise RuntimeError("Run returned no answer.")
        return result.answer

    def run_one(self, case: CaseInput, *, artifacts_root: Path | None = None) -> RunResult:
        self.artifacts.plan = None
        self.artifacts.context = None
        self.artifacts.raw_synth = None
        self.artifacts.answer = None

        timings = Timings()
        error_msg: str | None = None

        artifact_dir: Optional[Path] = None
        if artifacts_root is not None:
            artifact_dir = artifacts_root / case.id
            artifact_dir.mkdir(parents=True, exist_ok=True)

        start_total = time.perf_counter()

        if case.skip:
            timings.total_s = 0.0
            result = RunResult(
                id=case.id,
                question=case.question,
                answer=None,
                status="skipped",
                timings=timings,
                expected_check=None,
                error=None,
                plan_path=str(artifact_dir / "plan.json") if artifact_dir else None,
                context_path=str(artifact_dir / "context.json") if artifact_dir else None,
                answer_path=str(artifact_dir / "answer.txt") if artifact_dir else None,
                raw_synth_path=str(artifact_dir / "raw_synth.txt") if artifact_dir else None,
                tags=case.tags,
            )
            if artifact_dir:
                (artifact_dir / "plan.json").write_text("{}", encoding="utf-8")
                (artifact_dir / "context.json").write_text("{}", encoding="utf-8")
                (artifact_dir / "answer.txt").write_text("", encoding="utf-8")
                (artifact_dir / "raw_synth.txt").write_text("", encoding="utf-8")
            return result

        plan: Plan | None = None
        ctx_text: Dict[str, str] | None = None
        raw_synth: str | None = None
        answer: str | None = None
        expected_check: ExpectedCheck | None = None
        status = "ok"

        try:
            t0 = time.perf_counter()
            plan = self.agent._plan(case.question)  # type: ignore[attr-defined]
            timings.plan_s = time.perf_counter() - t0
            self.artifacts.plan = plan.model_dump()

            t0 = time.perf_counter()
            ctx_items = self.agent._fetch(case.question, plan)  # type: ignore[attr-defined]
            if getattr(self.agent, "llm_refetch", None):
                ctx_items, plan = self.agent._assess_refetch_loop(case.question, ctx_items, plan)  # type: ignore[attr-defined]
            ctx_items = self.agent._ensure_required_baseline(case.question, ctx_items)  # type: ignore[attr-defined]
            timings.fetch_s = time.perf_counter() - t0
            ctx_text = {k: v.text for k, v in (ctx_items or {}).items()}
            self.artifacts.context = ctx_text

            t0 = time.perf_counter()
            draft = self.agent._synthesize(case.question, ctx_items, plan)  # type: ignore[attr-defined]
            draft, _ = self.agent._verify_and_refine(case.question, ctx_items, plan, draft)  # type: ignore[attr-defined]
            raw_synth = draft.text
            self.artifacts.raw_synth = raw_synth
            answer = self.agent.domain_parser(draft)
            self.artifacts.answer = str(answer)
            timings.synth_s = time.perf_counter() - t0
        except Exception as exc:
            status = "error"
            error_msg = f"{exc.__class__.__name__}: {exc}"
            stack = traceback.format_exc()
            if artifact_dir:
                (artifact_dir / "error.txt").write_text(stack, encoding="utf-8")
            else:
                error_msg = stack
        finally:
            timings.total_s = time.perf_counter() - start_total

        if artifact_dir:
            if self.artifacts.plan is not None:
                (artifact_dir / "plan.json").write_text(
                    json.dumps(self.artifacts.plan, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            else:
                (artifact_dir / "plan.json").write_text("{}", encoding="utf-8")
            if self.artifacts.context is not None:
                (artifact_dir / "context.json").write_text(
                    json.dumps(self.artifacts.context, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            else:
                (artifact_dir / "context.json").write_text("{}", encoding="utf-8")
            if self.artifacts.answer is not None:
                (artifact_dir / "answer.txt").write_text(self.artifacts.answer, encoding="utf-8")
            if self.artifacts.raw_synth is not None:
                (artifact_dir / "raw_synth.txt").write_text(self.artifacts.raw_synth, encoding="utf-8")

        if status != "error" and answer is not None:
            expected_check = _evaluate_expectation(case, str(answer))
            if expected_check and not expected_check.passed:
                status = "mismatch"

        result = RunResult(
            id=case.id,
            question=case.question,
            answer=str(answer) if answer is not None else None,
            status=status,
            timings=timings,
            expected_check=expected_check,
            error=error_msg,
            plan_path=str(artifact_dir / "plan.json") if artifact_dir else None,
            context_path=str(artifact_dir / "context.json") if artifact_dir else None,
            answer_path=str(artifact_dir / "answer.txt") if artifact_dir else None,
            raw_synth_path=str(artifact_dir / "raw_synth.txt") if artifact_dir else None,
            tags=case.tags,
        )
        return result


def _evaluate_expectation(case: CaseInput, answer: str) -> ExpectedCheck | None:
    if case.expected is None and case.expected_regex is None and case.expected_contains is None:
        return None
    if case.expected_regex is not None:
        import re

        matched = re.search(case.expected_regex, answer) is not None
        details = None if matched else f"Regex {case.expected_regex!r} not found in answer."
        return ExpectedCheck(
            mode="expected_regex",
            expected=case.expected_regex,
            observed=answer,
            passed=matched,
            details=details,
        )
    if case.expected_contains is not None:
        matched = case.expected_contains in answer
        details = None if matched else f"Expected substring {case.expected_contains!r} not found."
        return ExpectedCheck(
            mode="expected_contains",
            expected=case.expected_contains,
            observed=answer,
            passed=matched,
            details=details,
        )
    expected = case.expected or ""
    matched = answer.strip() == expected.strip()
    details = None if matched else "Answer did not match expected exactly."
    return ExpectedCheck(
        mode="expected",
        expected=expected,
        observed=answer,
        passed=matched,
        details=details,
    )


def ensure_artifacts_root(path: Path | None, *, data_dir: Path) -> Path:
    if path is None:
        return data_dir / ".runs" / f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    return path


def setup_runner(data_dir: Path, schema_path: Path, llm, *, enable_semantic: bool = False) -> AgentRunner:
    provider, _ = build_provider(data_dir, schema_path, enable_semantic=enable_semantic)
    return build_agent(llm, provider)


__all__ = [
    "AgentRunner",
    "CaseInput",
    "ExpectedCheck",
    "RunArtifacts",
    "RunResult",
    "Timings",
    "build_agent",
    "ensure_artifacts_root",
    "load_cases",
    "setup_runner",
]
