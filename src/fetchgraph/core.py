from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Callable, runtime_checkable, overload

from pydantic import BaseModel, Field
import json

from .utils import load_pkg_text, render_prompt

# -----------------------------------------------------------------------------
# Basic normalized LLM output
# -----------------------------------------------------------------------------
class RawLLMOutput(BaseModel):
    text: str

def normalize_llm_output(raw: Any) -> RawLLMOutput:
    if isinstance(raw, RawLLMOutput):
        return raw
    if isinstance(raw, str):
        return RawLLMOutput(text=raw)
    # dict-like?
    if isinstance(raw, dict):
        for k in ("text", "output", "content"):
            if k in raw and isinstance(raw[k], str):
                return RawLLMOutput(text=raw[k])
        return RawLLMOutput(text=json.dumps(raw, ensure_ascii=False))
    # fallback
    return RawLLMOutput(text=str(raw))

# -----------------------------------------------------------------------------
# Task profile & provider descriptors
# -----------------------------------------------------------------------------
class TaskProfile(BaseModel):
    task_name: str = "Generic Task"
    goal: str = "Synthesize output based on fetched context"
    output_format: str = "{}"
    acceptance_criteria: List[str] = Field(default_factory=list)
    constraints: List[str] = Field(default_factory=list)
    focus_hints: List[str] = Field(default_factory=list)
    lite_context_keys: List[str] = Field(default_factory=list)

class ProviderInfo(BaseModel):
    name: str
    description: str = ""
    selectors_schema: Dict[str, Any] = Field(default_factory=dict)
    capabilities: List[str] = Field(default_factory=list)  # e.g. ["slice","filter","search"]
    examples: List[str] = Field(default_factory=list)
    typical_cost: Optional[str] = None  # "cheap|moderate|expensive"

class LLMInvoke(Protocol):
    # 1) позиционный sender
    @overload
    def __call__(self, prompt: str, /, sender: str) -> str: ...
    # 2) keyword-only sender
    @overload
    def __call__(self, prompt: str, *, sender: str) -> str: ...
    # реальная сигнатура (наиболее общая)
    def __call__(self, *args: Any, **kwargs: Any) -> str: ...

class ContextItem(BaseModel):
    key: str
    raw: Any
    text: str
    tokens: int

# For maximum reuse, provider type is a free string:
ProviderType = str

class ContextFetchSpec(BaseModel):
    provider: ProviderType
    mode: str = "full"  # "full" | "slice"
    selectors: Dict[str, Any] = Field(default_factory=dict)
    max_tokens: Optional[int] = None

@dataclass(frozen=True)
class BaselineSpec:
    spec: ContextFetchSpec
    required: bool = True

class Plan(BaseModel):
    required_context: List[ProviderType] = Field(default_factory=list)
    context_plan: List[ContextFetchSpec] = Field(default_factory=list)
    adr_queries: Optional[List[str]] = None
    constraints: Optional[List[str]] = None
    # optional sketch fields kept for compatibility/inspection
    entities: List[Dict[str, Any]] = Field(default_factory=list)
    dtos: List[Dict[str, Any]] = Field(default_factory=list)

class RefetchDecision(BaseModel):
    add_specs: List[ContextFetchSpec] = Field(default_factory=list)
    stop: bool = True
    notes: Optional[str] = None

# -----------------------------------------------------------------------------
# Protocols
# -----------------------------------------------------------------------------
class Verifier(Protocol):
    name: str
    def check(self, output_text: RawLLMOutput) -> List[str]: ...

class Saver(Protocol):
    def save(self, feature_name: str, parsed: Any) -> None: ...

class ContextProvider(Protocol):
    name: str
    def fetch(self, feature_name: str, selectors: Optional[Dict[str, Any]] = None, **kwargs) -> Any: ...
    def serialize(self, obj: Any) -> str: ...

@runtime_checkable
class SupportsFilter(Protocol):
    def filter(self, obj: Any, selectors: Optional[Dict[str, Any]] = None) -> Any: ...

@runtime_checkable
class SupportsDescribe(Protocol):
    def describe(self) -> ProviderInfo: ...

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _apply_provider_filter(provider: ContextProvider, obj: Any, selectors: Optional[Dict[str, Any]]):
    if isinstance(provider, SupportsFilter):
        return provider.filter(obj, selectors)
    return obj

def provider_catalog_text(providers: Dict[str, ContextProvider]) -> str:
    lines: List[str] = []
    for key, prov in providers.items():
        info: Optional[ProviderInfo] = None
        if isinstance(prov, SupportsDescribe):
            try:
                info = prov.describe()
            except Exception:
                info = None
        if info is None:
            caps = []
            if isinstance(prov, SupportsFilter):
                caps = ["filter", "slice"]
            info = ProviderInfo(name=getattr(prov, "name", key), capabilities=caps)

        lines.append(f"- name: {info.name}")
        if info.description:
            lines.append(f"  description: {info.description}")
        if info.capabilities:
            lines.append(f"  capabilities: {', '.join(info.capabilities)}")
        if info.typical_cost:
            lines.append(f"  typical_cost: {info.typical_cost}")
        if info.selectors_schema:
            schema = json.dumps(info.selectors_schema, ensure_ascii=False, indent=2)
            lines.append("  selectors_schema:")
            lines += [f"    {ln}" for ln in schema.splitlines()]
        if info.examples:
            lines.append("  examples:")
            lines += [f"    - {ex}" for ex in info.examples]
    return "\n".join(lines) if lines else "(no providers)"

# -----------------------------------------------------------------------------
# Packer
# -----------------------------------------------------------------------------
class ContextPacker:
    def __init__(self, max_tokens: int, summarizer_llm: Callable[[str], str]):
        self.max_tokens = max_tokens
        self.summarizer = summarizer_llm

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        return max(1, len(text) // 4)

    def pack(self, items: List[ContextItem]) -> List[ContextItem]:
        items = sorted(items, key=lambda x: x.tokens)
        out: List[ContextItem] = []
        budget = 0
        for it in items:
            if budget + it.tokens <= self.max_tokens:
                out.append(it); budget += it.tokens
            else:
                summary = self.summarizer(f"Суммаризуй кратко и по делу:\n\n{it.text}")
                t = self._estimate_tokens(summary)
                if budget + t <= self.max_tokens:
                    out.append(ContextItem(key=it.key, raw=it.raw, text=summary, tokens=t))
                    budget += t
        return out

# -----------------------------------------------------------------------------
# Generic plan/synth factories (built on pkg prompts)
# -----------------------------------------------------------------------------
def make_llm_plan_generic(
    llm_invoke: LLMInvoke,
    task_profile: TaskProfile,
    providers: Dict[str, ContextProvider],
) -> Callable[[str, Dict[str, str]], str]:
    tpl = load_pkg_text("prompts/plan_generic.md")
    catalog = provider_catalog_text(providers)

    def llm_plan(feature_name: str, lite_ctx: Dict[str, str]) -> str:
        prompt = render_prompt(
            tpl,
            task_name=task_profile.task_name,
            goal=task_profile.goal,
            output_format=task_profile.output_format,
            acceptance_criteria="\n".join(f"- {x}" for x in task_profile.acceptance_criteria) or "(не задано)",
            constraints="\n".join(f"- {x}" for x in task_profile.constraints) or "(не задано)",
            focus_hints="\n".join(f"- {x}" for x in task_profile.focus_hints) or "(не задано)",
            provider_catalog=catalog,
            lite_context_json=json.dumps(lite_ctx or {}, ensure_ascii=False)[:8000],
        )
        return llm_invoke(prompt, sender="generic_plan")
    return llm_plan

def make_llm_synth_generic(
    llm_invoke: LLMInvoke,
    task_profile: TaskProfile,
) -> Callable[[str, Dict[str, str], Plan], str]:
    tpl = load_pkg_text("prompts/synth_generic.md")

    def _bundle_ctx(ctx_text: Dict[str, str]) -> str:
        parts = []
        for k, v in (ctx_text or {}).items():
            if v and v.strip():
                parts.append(f"<<<{k.upper()}>>>\n{v}\n<</{k.upper()}>>>")
        return "\n".join(parts) if parts else "(контекст недоступен)"

    def llm_synth(feature_name: str, ctx_text: Dict[str, str], plan: Plan) -> str:
        prompt = render_prompt(
            tpl,
            task_name=task_profile.task_name,
            goal=task_profile.goal,
            output_format=task_profile.output_format,
            acceptance_criteria="\n".join(f"- {x}" for x in task_profile.acceptance_criteria) or "(не задано)",
            constraints="\n".join(f"- {x}" for x in task_profile.constraints) or "(не задано)",
            focus_hints="\n".join(f"- {x}" for x in task_profile.focus_hints) or "(не задано)",
            plan_json=plan.model_dump_json(),
            context_bundle=_bundle_ctx(ctx_text),
        )
        return llm_invoke(prompt, sender="generic_synth")
    return llm_synth

# -----------------------------------------------------------------------------
# BaseGraphAgent (sequential engine; no external graph dep)
# -----------------------------------------------------------------------------
class BaseGraphAgent:
    def __init__(
        self,
        llm_plan: Optional[Callable[[str, Dict[str, str]], str]],
        llm_synth: Callable[[str, Dict[str, str], Plan], str],
        domain_parser: Callable[[RawLLMOutput], Any],
        saver: Saver | Callable[[str, Any], None],
        providers: Dict[str, ContextProvider],
        verifiers: List[Verifier],
        packer: ContextPacker,
        plan_parser: Optional[Callable[[RawLLMOutput], Plan]] = None,
        baseline: Optional[List[BaselineSpec]] = None,
        max_retries: int = 2,
        task_profile: Optional[TaskProfile] = None,
        llm_refetch: Optional[Callable[[str, Dict[str, str], Plan], str]] = None,
        max_refetch_iters: int = 1,
    ):
        self.llm_plan = llm_plan
        self.llm_synth = llm_synth
        self.domain_parser = domain_parser
        self.saver = saver
        self.providers = providers
        self.verifiers = verifiers
        self.packer = packer
        self.plan_parser = plan_parser
        self.baseline = baseline or []
        self.max_retries = max_retries
        self.task_profile = task_profile or TaskProfile()
        self.llm_refetch = llm_refetch
        self.max_refetch_iters = max_refetch_iters

    # ---- public API ----
    def run(self, feature_name: str) -> Any:
        # PLAN
        plan = self._plan(feature_name)

        # FETCH (+ pack)
        ctx = self._fetch(feature_name, plan)

        # ASSESS/REFETCH loop
        if self.llm_refetch:
            ctx, plan = self._assess_refetch_loop(feature_name, ctx, plan)

        # Ensure baseline items before synth
        ctx = self._ensure_required_baseline(feature_name, ctx)

        # SYNTH + VERIFY + REFINE
        draft = self._synthesize(feature_name, ctx, plan)
        draft, ok = self._verify_and_refine(feature_name, ctx, plan, draft)

        # PARSE & SAVE
        parsed = self.domain_parser(draft)
        if callable(self.saver):
            self.saver(feature_name, parsed)  # type: ignore[misc]
        else:
            self.saver.save(feature_name, parsed)  # type: ignore[attr-defined]
        return parsed

    # ---- steps ----
    def _plan(self, feature_name: str) -> Plan:
        lite_ctx = self._lite_context(feature_name)
        if self.llm_plan is None:
            raise RuntimeError("llm_plan is not provided. Use make_llm_plan_generic(...) or pass custom.")
        plan_text = self.llm_plan(feature_name, lite_ctx)
        plan_raw = normalize_llm_output(plan_text)
        if self.plan_parser is not None:
            plan = self.plan_parser(plan_raw)
        else:
            plan = Plan.model_validate_json(plan_raw.text)
        return plan

    def _merge_baseline_with_plan(self, plan: Plan) -> List[ContextFetchSpec]:
        by_provider: Dict[str, ContextFetchSpec] = {}
        for b in self.baseline:
            by_provider.setdefault(b.spec.provider, b.spec)
        for s in plan.context_plan or []:
            by_provider[s.provider] = s
        if not plan.context_plan:
            for p in plan.required_context or []:
                by_provider.setdefault(p, ContextFetchSpec(provider=p, mode="full"))
        return list(by_provider.values())

    def _fetch(self, feature_name: str, plan: Plan) -> Dict[str, ContextItem]:
        specs = self._merge_baseline_with_plan(plan)
        gathered: List[ContextItem] = []

        for spec in specs:
            prov = self.providers.get(spec.provider)
            if not prov:
                continue
            obj = prov.fetch(feature_name, selectors=spec.selectors)
            if spec.mode == "slice":
                obj = _apply_provider_filter(prov, obj, spec.selectors)
            text = prov.serialize(obj)
            tokens = max(1, len(text) // 4)
            if spec.max_tokens and tokens > spec.max_tokens:
                approx_chars = spec.max_tokens * 4
                text = text[:approx_chars]
                tokens = max(1, len(text) // 4)
            gathered.append(ContextItem(key=spec.provider, raw=obj, text=text, tokens=tokens))

        packed = self.packer.pack(gathered)
        return {it.key: it for it in packed}

    def _assess_refetch_loop(self, feature_name: str, ctx: Dict[str, ContextItem], plan: Plan):
        iters = 0
        while self.llm_refetch and iters < self.max_refetch_iters:
            ctx_text = {k: v.text for k, v in (ctx or {}).items()}
            decision_text = self.llm_refetch(feature_name, ctx_text, plan)
            decision_raw = normalize_llm_output(decision_text)
            try:
                decision = RefetchDecision.model_validate_json(decision_raw.text)
            except Exception:
                break
            if decision.stop or not decision.add_specs:
                break
            # merge new specs into plan
            merged = list(plan.context_plan or [])
            seen = {(s.provider, json.dumps(s.selectors, sort_keys=True), s.mode) for s in merged}
            for ns in decision.add_specs:
                key = (ns.provider, json.dumps(ns.selectors or {}, sort_keys=True), ns.mode)
                if key not in seen:
                    merged.append(ns); seen.add(key)
            plan = plan.model_copy(update={"context_plan": merged})
            # fetch again
            ctx = self._fetch(feature_name, plan)
            iters += 1
        return ctx, plan

    def _ensure_required_baseline(self, feature_name: str, ctx: Dict[str, ContextItem]) -> Dict[str, ContextItem]:
        out = dict(ctx)
        for b in self.baseline:
            if not b.required:
                continue
            key = b.spec.provider
            if key in out:
                continue
            prov = self.providers.get(key)
            if not prov:
                continue
            obj = prov.fetch(feature_name, selectors=b.spec.selectors)
            if b.spec.mode == "slice":
                obj = _apply_provider_filter(prov, obj, b.spec.selectors)
            text = prov.serialize(obj)
            tokens = max(1, len(text) // 4)
            out[key] = ContextItem(key=key, raw=obj, text=text, tokens=tokens)
        return out

    def _synthesize(self, feature_name: str, ctx: Dict[str, ContextItem], plan: Plan) -> RawLLMOutput:
        ctx_text = {k: v.text for k, v in (ctx or {}).items()}
        out_text = self.llm_synth(feature_name, ctx_text, plan)
        return normalize_llm_output(out_text)

    def _verify_and_refine(
        self, feature_name: str, ctx: Dict[str, ContextItem], plan: Plan, draft: RawLLMOutput
    ) -> tuple[RawLLMOutput, bool]:
        retries = 0
        while True:
            errors: List[str] = []
            for v in self.verifiers:
                try:
                    errors += v.check(draft)
                except Exception as e:
                    errors.append(f"[{getattr(v, 'name', 'verifier')}] error: {e}")
            if not errors:
                return draft, True
            if retries >= self.max_retries:
                return draft, False
            # refine
            ctx_text = {k: v.text for k, v in (ctx or {}).items()}
            ctx_text["issues"] = "\n".join(errors)
            refined = self.llm_synth(feature_name, ctx_text, plan)
            draft = normalize_llm_output(refined)
            retries += 1

    # ---- lite context (optional) ----
    def _lite_context(self, feature_name: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key in self.task_profile.lite_context_keys or []:
            prov = self.providers.get(key)
            if not prov:
                continue
            obj = prov.fetch(feature_name)
            out[key] = prov.serialize(obj)
        return out
