from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional, Callable

from .models import (
    BaselineSpec,
    ContextFetchSpec,
    ContextItem,
    Plan,
    ProviderInfo,
    ProviderType,
    RawLLMOutput,
    RefetchDecision,
    TaskProfile,
)
from .parsing.plan_parser import PlanParser
from .protocols import (
    ContextProvider,
    LLMInvoke,
    Saver,
    SupportsDescribe,
    SupportsFilter,
    Verifier,
)
from .utils import load_pkg_text, render_prompt

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Basic normalized LLM output
# -----------------------------------------------------------------------------
def normalize_llm_output(raw: Any) -> RawLLMOutput:
    if isinstance(raw, RawLLMOutput):
        return raw
    if isinstance(raw, str):
        return RawLLMOutput(text=raw)
    if isinstance(raw, dict):
        for k in ("text", "output", "content"):
            if k in raw and isinstance(raw[k], str):
                return RawLLMOutput(text=raw[k])
        return RawLLMOutput(text=json.dumps(raw, ensure_ascii=False))
    return RawLLMOutput(text=str(raw))


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
            except Exception as e:
                logger.warning(
                    "Provider %r.describe() failed: %s", key, e, exc_info=True
                )
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

    catalog_text = "\n".join(lines) if lines else "(no providers)"
    logger.debug(
        "Built provider catalog for %d providers (chars=%d)",
        len(providers),
        len(catalog_text),
    )
    return catalog_text


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
        if not items:
            logger.info("ContextPacker: no items to pack (max_tokens=%d)", self.max_tokens)

        items = sorted(items, key=lambda x: x.tokens)
        out: List[ContextItem] = []
        budget = 0
        total_tokens_before = sum(i.tokens for i in items)

        for it in items:
            if budget + it.tokens <= self.max_tokens:
                out.append(it)
                budget += it.tokens
            else:
                logger.info(
                    "ContextPacker: summarizing item key=%r tokens≈%d "
                    "to fit remaining budget (used≈%d / max≈%d)",
                    it.key,
                    it.tokens,
                    budget,
                    self.max_tokens,
                )
                t0 = time.perf_counter()
                summary = self.summarizer(
                    f"Суммаризуй кратко и по делу:\n\n{it.text}"
                )
                elapsed = time.perf_counter() - t0
                t = self._estimate_tokens(summary)
                logger.info(
                    "ContextPacker: item key=%r summarized tokens≈%d -> ≈%d "
                    "(elapsed=%.3fs)",
                    it.key,
                    it.tokens,
                    t,
                    elapsed,
                )
                if budget + t <= self.max_tokens:
                    out.append(
                        ContextItem(
                            key=it.key, raw=it.raw, text=summary, tokens=t
                        )
                    )
                    budget += t
                else:
                    logger.info(
                        "ContextPacker: even summarized item key=%r (tokens≈%d) "
                        "does not fit into remaining budget (used≈%d / max≈%d); skipping",
                        it.key,
                        t,
                        budget,
                        self.max_tokens,
                    )

        total_tokens_after = sum(i.tokens for i in out)
        logger.info(
            "ContextPacker: packed %d -> %d items, tokens≈%d -> ≈%d "
            "(max_tokens=%d)",
            len(items),
            len(out),
            total_tokens_before,
            total_tokens_after,
            self.max_tokens,
        )
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
        user_query = feature_name

        prompt = render_prompt(
            tpl,
            task_name=task_profile.task_name,
            goal=task_profile.goal,
            user_query=user_query,
            output_format=task_profile.output_format,
            acceptance_criteria="\n".join(f"- {x}" for x in task_profile.acceptance_criteria) or "(не задано)",
            constraints="\n".join(f"- {x}" for x in task_profile.constraints) or "(не задано)",
            focus_hints="\n".join(f"- {x}" for x in task_profile.focus_hints) or "(не задано)",
            provider_catalog=catalog,
            lite_context_json=json.dumps(lite_ctx or {}, ensure_ascii=False)[:8000],
        )
        logger.debug(
            "Rendered generic plan prompt for feature_name=%r (chars=%d)",
            feature_name,
            len(prompt),
        )
        t0 = time.perf_counter()
        result = llm_invoke(prompt, sender="generic_plan")
        elapsed = time.perf_counter() - t0
        logger.info(
            "LLM plan call finished for feature_name=%r (elapsed=%.3fs)",
            feature_name,
            elapsed,
        )
        return result

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
        user_query = feature_name

        prompt = render_prompt(
            tpl,
            task_name=task_profile.task_name,
            goal=task_profile.goal,
            user_query=user_query,
            output_format=task_profile.output_format,
            acceptance_criteria="\n".join(f"- {x}" for x in task_profile.acceptance_criteria) or "(не задано)",
            constraints="\n".join(f"- {x}" for x in task_profile.constraints) or "(не задано)",
            focus_hints="\n".join(f"- {x}" for x in task_profile.focus_hints) or "(не задано)",
            plan_json=plan.model_dump_json(),
            context_bundle=_bundle_ctx(ctx_text),
        )
        logger.debug(
            "Rendered generic synth prompt for feature_name=%r "
            "(ctx_keys=%d, chars=%d)",
            feature_name,
            len(ctx_text or {}),
            len(prompt),
        )
        t0 = time.perf_counter()
        result = llm_invoke(prompt, sender="generic_synth")
        elapsed = time.perf_counter() - t0
        logger.info(
            "LLM synth call finished for feature_name=%r (elapsed=%.3fs)",
            feature_name,
            elapsed,
        )
        return result

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
        if plan_parser is None:
            self.plan_parser = PlanParser().parse
        else:
            self.plan_parser = plan_parser
        self.baseline = baseline or []
        self.max_retries = max_retries
        self.task_profile = task_profile or TaskProfile()
        self.llm_refetch = llm_refetch
        self.max_refetch_iters = max_refetch_iters

        logger.info(
            "BaseGraphAgent initialized "
            "(task_name=%r, providers=%d, verifiers=%d, "
            "baseline_specs=%d, max_retries=%d, max_refetch_iters=%d)",
            self.task_profile.task_name,
            len(self.providers),
            len(self.verifiers),
            len(self.baseline),
            self.max_retries,
            self.max_refetch_iters,
        )

    # ---- public API ----
    def run(self, feature_name: str) -> Any:
        start = time.perf_counter()
        logger.info(
            "Run started for feature_name=%r task=%r",
            feature_name,
            self.task_profile.task_name,
        )

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
        saver_name: str
        if callable(self.saver):
            saver_name = getattr(self.saver, "__name__", self.saver.__class__.__name__)
            logger.info(
                "Saving parsed result for feature_name=%r via callable saver=%s",
                feature_name,
                saver_name,
            )
            self.saver(feature_name, parsed)  # type: ignore[misc]
        else:
            saver_name = self.saver.__class__.__name__
            logger.info(
                "Saving parsed result for feature_name=%r via saver object=%s",
                feature_name,
                saver_name,
            )
            self.saver.save(feature_name, parsed)  # type: ignore[attr-defined]

        elapsed = time.perf_counter() - start
        total_tokens = sum(it.tokens for it in (ctx or {}).values())
        logger.info(
            "Run finished for feature_name=%r in %.3fs "
            "(verification_ok=%s, providers_used=%d, total_tokens≈%d)",
            feature_name,
            elapsed,
            ok,
            len(ctx or {}),
            total_tokens,
        )
        return parsed

    # ---- steps ----
    def _plan(self, feature_name: str) -> Plan:
        t0 = time.perf_counter()
        lite_ctx = self._lite_context(feature_name)
        if self.llm_plan is None:
            raise RuntimeError(
                "llm_plan is not provided. Use make_llm_plan_generic(...) or pass custom."
            )
        logger.info(
            "Planning started for feature_name=%r (lite_ctx_keys=%d)",
            feature_name,
            len(lite_ctx or {}),
        )
        plan_text = self.llm_plan(feature_name, lite_ctx)
        plan_raw = normalize_llm_output(plan_text)
        if self.plan_parser is not None:
            plan = self.plan_parser(plan_raw)
        else:
            plan = Plan.model_validate_json(plan_raw.text)
        elapsed = time.perf_counter() - t0
        logger.info(
            "Planning finished for feature_name=%r in %.3fs "
            "(required_context=%s, context_plan_nodes=%d)",
            feature_name,
            elapsed,
            plan.required_context,
            len(plan.context_plan or []),
        )
        logger.debug(
            "Raw plan text for feature_name=%r (chars=%d)",
            feature_name,
            len(plan_raw.text),
        )
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
        specs = list(by_provider.values())
        logger.debug(
            "Merged baseline with plan: context_plan_nodes=%d, baseline_specs=%d, "
            "result_specs=%d",
            len(plan.context_plan or []),
            len(self.baseline),
            len(specs),
        )
        return specs

    def _fetch(self, feature_name: str, plan: Plan) -> Dict[str, ContextItem]:
        t0 = time.perf_counter()
        specs = self._merge_baseline_with_plan(plan)
        logger.info(
            "Fetching context for feature_name=%r using %d specs",
            feature_name,
            len(specs),
        )

        gathered: List[ContextItem] = []

        for spec in specs:
            prov = self.providers.get(spec.provider)
            if not prov:
                logger.warning(
                    "No provider registered for key %r (skipping, mode=%s)",
                    spec.provider,
                    spec.mode,
                )
                continue
            logger.info(
                "Fetching from provider=%r (mode=%s, selectors=%s, max_tokens=%s)",
                spec.provider,
                spec.mode,
                spec.selectors,
                getattr(spec, "max_tokens", None),
            )
            obj = prov.fetch(feature_name, selectors=spec.selectors)
            if spec.mode == "slice":
                obj = _apply_provider_filter(prov, obj, spec.selectors)
            text = prov.serialize(obj)
            tokens = max(1, len(text) // 4)
            if spec.max_tokens and tokens > spec.max_tokens:
                logger.info(
                    "Truncating provider=%r context: tokens≈%d > max_tokens=%d",
                    spec.provider,
                    tokens,
                    spec.max_tokens,
                )
                approx_chars = spec.max_tokens * 4
                text = text[:approx_chars]
                tokens = max(1, len(text) // 4)
            gathered.append(
                ContextItem(
                    key=spec.provider, raw=obj, text=text, tokens=tokens
                )
            )
            logger.debug(
                "Provider=%r returned tokens≈%d (text_chars=%d)",
                spec.provider,
                tokens,
                len(text),
            )

        total_tokens_before = sum(i.tokens for i in gathered)
        logger.info(
            "Context fetch (before pack) for feature_name=%r: providers=%d, "
            "total_tokens≈%d",
            feature_name,
            len(gathered),
            total_tokens_before,
        )

        packed = self.packer.pack(gathered)
        total_tokens_after = sum(i.tokens for i in packed)
        elapsed = time.perf_counter() - t0
        logger.info(
            "Context fetch finished for feature_name=%r in %.3fs "
            "(packed_providers=%d, packed_tokens≈%d)",
            feature_name,
            elapsed,
            len(packed),
            total_tokens_after,
        )
        return {it.key: it for it in packed}

    def _assess_refetch_loop(
        self,
        feature_name: str,
        ctx: Dict[str, ContextItem],
        plan: Plan,
    ):
        logger.info(
            "Starting refetch loop for feature_name=%r (max_refetch_iters=%d)",
            feature_name,
            self.max_refetch_iters,
        )
        iters = 0
        while self.llm_refetch and iters < self.max_refetch_iters:
            ctx_text = {k: v.text for k, v in (ctx or {}).items()}
            decision_text = self.llm_refetch(feature_name, ctx_text, plan)
            decision_raw = normalize_llm_output(decision_text)
            try:
                decision = RefetchDecision.model_validate_json(decision_raw.text)
            except Exception as e:
                logger.warning(
                    "Failed to parse refetch decision for feature_name=%r: %s",
                    feature_name,
                    e,
                    exc_info=True,
                )
                break

            logger.info(
                "Refetch iteration %d for feature_name=%r: stop=%s, add_specs=%d",
                iters + 1,
                feature_name,
                decision.stop,
                len(decision.add_specs or []),
            )

            if decision.stop or not decision.add_specs:
                logger.info(
                    "Refetch loop finished for feature_name=%r: stop=%s, add_specs=%d",
                    feature_name,
                    decision.stop,
                    len(decision.add_specs or []),
                )
                break

            # merge new specs into plan
            merged = list(plan.context_plan or [])
            seen = {
                (s.provider, json.dumps(s.selectors, sort_keys=True), s.mode)
                for s in merged
            }
            added = 0
            for ns in decision.add_specs:
                key = (
                    ns.provider,
                    json.dumps(ns.selectors or {}, sort_keys=True),
                    ns.mode,
                )
                if key not in seen:
                    merged.append(ns)
                    seen.add(key)
                    added += 1
            logger.info(
                "Refetch iteration %d for feature_name=%r: merged %d new specs "
                "(total_specs=%d)",
                iters + 1,
                feature_name,
                added,
                len(merged),
            )
            plan = plan.model_copy(update={"context_plan": merged})
            # fetch again
            ctx = self._fetch(feature_name, plan)
            iters += 1

        if iters >= self.max_refetch_iters:
            logger.info(
                "Refetch loop reached max_refetch_iters=%d for feature_name=%r",
                self.max_refetch_iters,
                feature_name,
            )
        return ctx, plan

    def _ensure_required_baseline(
        self, feature_name: str, ctx: Dict[str, ContextItem]
    ) -> Dict[str, ContextItem]:
        out = dict(ctx)
        added = 0
        for b in self.baseline:
            if not b.required:
                continue
            key = b.spec.provider
            if key in out:
                continue
            prov = self.providers.get(key)
            if not prov:
                logger.warning(
                    "Required baseline provider=%r is missing; skipping", key
                )
                continue
            logger.info(
                "Fetching required baseline provider=%r for feature_name=%r",
                key,
                feature_name,
            )
            obj = prov.fetch(feature_name, selectors=b.spec.selectors)
            if b.spec.mode == "slice":
                obj = _apply_provider_filter(prov, obj, b.spec.selectors)
            text = prov.serialize(obj)
            tokens = max(1, len(text) // 4)
            out[key] = ContextItem(key=key, raw=obj, text=text, tokens=tokens)
            added += 1

        if added:
            logger.info(
                "Added %d required baseline items for feature_name=%r "
                "(total_providers=%d)",
                added,
                feature_name,
                len(out),
            )
        else:
            logger.info(
                "No additional required baseline items needed for feature_name=%r",
                feature_name,
            )
        return out

    def _synthesize(
        self, feature_name: str, ctx: Dict[str, ContextItem], plan: Plan
    ) -> RawLLMOutput:
        ctx_text = {k: v.text for k, v in (ctx or {}).items()}
        tokens = sum(v.tokens for v in (ctx or {}).values())
        logger.info(
            "Synthesis started for feature_name=%r "
            "(ctx_providers=%d, ctx_tokens≈%d)",
            feature_name,
            len(ctx_text),
            tokens,
        )
        t0 = time.perf_counter()
        out_text = self.llm_synth(feature_name, ctx_text, plan)
        raw = normalize_llm_output(out_text)
        elapsed = time.perf_counter() - t0
        logger.info(
            "Synthesis finished for feature_name=%r in %.3fs (output_chars=%d)",
            feature_name,
            elapsed,
            len(raw.text),
        )
        return raw

    def _verify_and_refine(
        self,
        feature_name: str,
        ctx: Dict[str, ContextItem],
        plan: Plan,
        draft: RawLLMOutput,
    ) -> tuple[RawLLMOutput, bool]:
        retries = 0
        if not self.verifiers:
            logger.info(
                "No verifiers configured for feature_name=%r; skipping verification",
                feature_name,
            )

        while True:
            errors: List[str] = []
            for v in self.verifiers:
                try:
                    v_name = getattr(v, "name", v.__class__.__name__)
                    v_errors = v.check(draft)
                    logger.debug(
                        "Verifier %s returned %d error(s) for feature_name=%r",
                        v_name,
                        len(v_errors),
                        feature_name,
                    )
                    errors += v_errors
                except Exception as e:
                    msg = f"[{getattr(v, 'name', 'verifier')}] error: {e}"
                    logger.warning(
                        "Verifier %r raised exception for feature_name=%r: %s",
                        v,
                        feature_name,
                        e,
                        exc_info=True,
                    )
                    errors.append(msg)

            if not errors:
                logger.info(
                    "Verification passed for feature_name=%r (retries=%d)",
                    feature_name,
                    retries,
                )
                return draft, True

            logger.info(
                "Verification FAILED for feature_name=%r: %d error(s)",
                feature_name,
                len(errors),
            )
            if retries >= self.max_retries:
                logger.warning(
                    "Reached max_retries=%d for feature_name=%r; returning last draft",
                    self.max_retries,
                    feature_name,
                )
                return draft, False

            # refine
            ctx_text = {k: v.text for k, v in (ctx or {}).items()}
            ctx_text["issues"] = "\n".join(errors)
            logger.info(
                "Refining draft for feature_name=%r (retry=%d/%d)",
                feature_name,
                retries + 1,
                self.max_retries,
            )
            refined = self.llm_synth(feature_name, ctx_text, plan)
            draft = normalize_llm_output(refined)
            retries += 1

    # ---- lite context (optional) ----
    def _lite_context(self, feature_name: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        keys = self.task_profile.lite_context_keys or []
        if keys:
            logger.info(
                "Collecting lite context for feature_name=%r (keys=%s)",
                feature_name,
                keys,
            )
        for key in keys:
            prov = self.providers.get(key)
            if not prov:
                logger.warning(
                    "Lite context provider=%r is missing; skipping", key
                )
                continue
            obj = prov.fetch(feature_name)
            out[key] = prov.serialize(obj)
            logger.debug(
                "Lite context provider=%r fetched for feature_name=%r (chars=%d)",
                key,
                feature_name,
                len(out[key]),
            )
        return out
