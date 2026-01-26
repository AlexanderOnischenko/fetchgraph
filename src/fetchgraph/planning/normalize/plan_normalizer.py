from __future__ import annotations

import copy
import json
import logging
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from pydantic import Field, TypeAdapter, ValidationError

from ...core.models import ContextFetchSpec, Plan, ProviderInfo
from ...core.protocols import ContextProvider, SupportsDescribe, SupportsFilter
from ...relational.models import RelationalRequest
from ...relational.normalize import normalize_relational_selectors
from ...relational.providers.base import RelationalDataProvider
from ...replay.log import EventLoggerLike, log_replay_case
from ...replay.snapshots import snapshot_provider_info

logger = logging.getLogger(__name__)


class NormalizedPlan(Plan):
    normalization_notes: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class PlanNormalizerOptions:
    allow_unknown_providers: bool = False
    coerce_provider_case: bool = True
    dedupe_required_context: bool = True
    dedupe_context_plan: bool = True
    trim_text_fields: bool = True
    filter_selectors_by_schema: bool = True
    default_mode: str = "full"


@dataclass(frozen=True)
class SelectorNormalizationRule:
    validator: TypeAdapter[Any]
    normalize_selectors: Callable[[Any], Any]
    kind: str | None = None


class PlanNormalizer:
    def __init__(
        self,
        provider_catalog: Dict[str, ProviderInfo],
        schema_registry: Optional[Dict[str, Dict[str, Any]]] = None,
        normalizer_registry: Optional[Dict[str, SelectorNormalizationRule]] = None,
        options: Optional[PlanNormalizerOptions] = None,
        event_logger: EventLoggerLike | None = None,
    ) -> None:
        self.provider_catalog = dict(provider_catalog)
        self.schema_registry = schema_registry or {}
        self.normalizer_registry = normalizer_registry or {}
        self.options = options or PlanNormalizerOptions()
        self._provider_aliases = self._build_provider_aliases(self.provider_catalog)
        self.event_logger = event_logger

    @classmethod
    def from_providers(
        cls,
        providers: Dict[str, ContextProvider],
        *,
        options: Optional[PlanNormalizerOptions] = None,
    ) -> "PlanNormalizer":
        catalog: Dict[str, ProviderInfo] = {}
        schema_registry: Dict[str, Dict[str, Any]] = {}
        normalizer_registry: Dict[str, SelectorNormalizationRule] = {}
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
            catalog[key] = info
            if info.selectors_schema:
                schema_registry[key] = info.selectors_schema
            if isinstance(prov, RelationalDataProvider):
                normalizer_registry[key] = SelectorNormalizationRule(
                    kind="relational_v1",
                    validator=TypeAdapter(RelationalRequest),
                    normalize_selectors=normalize_relational_selectors,
                )
        return cls(
            catalog,
            schema_registry=schema_registry,
            normalizer_registry=normalizer_registry,
            options=options,
        )

    def normalize(self, plan: Plan, *, event_logger: EventLoggerLike | None = None) -> NormalizedPlan:
        notes: List[str] = []
        required_context = self._normalize_required_context(plan.required_context, notes)
        context_plan = self._normalize_context_plan(plan.context_plan, notes)

        # IMPORTANT:
        # Do NOT synthesize ContextFetchSpec from required_context here.
        # Baseline/plan merge owns "ensure required providers exist" logic,
        # and must do it in a baseline-safe way (never overriding baseline selectors/mode).

        context_plan = self._normalize_specs(context_plan, notes, event_logger=event_logger)

        adr_queries = self._normalize_text_list(plan.adr_queries, notes, "adr_queries")
        constraints = self._normalize_text_list(
            plan.constraints, notes, "constraints"
        )

        normalized = NormalizedPlan(
            required_context=required_context,
            context_plan=context_plan,
            adr_queries=adr_queries,
            constraints=constraints,
            entities=list(plan.entities or []),
            dtos=list(plan.dtos or []),
            normalization_notes=notes,
        )
        return normalized

    def normalize_specs(
        self,
        specs: Iterable[ContextFetchSpec],
        *,
        notes: Optional[List[str]] = None,
        event_logger: EventLoggerLike | None = None,
    ) -> List[ContextFetchSpec]:
        local_notes: List[str] = []
        normalized = self._normalize_specs(specs, local_notes, event_logger=event_logger)
        if notes is not None:
            notes.extend(local_notes)
        if local_notes:
            logger.debug(
                "PlanNormalizer selectors normalization notes: %s",
                "; ".join(local_notes),
            )
        return normalized

    def _normalize_specs(
        self,
        specs: Iterable[ContextFetchSpec],
        notes: List[str],
        *,
        event_logger: EventLoggerLike | None = None,
    ) -> List[ContextFetchSpec]:
        normalized: List[ContextFetchSpec] = []
        replay_logger = event_logger or self.event_logger
        for spec_idx, spec in enumerate(specs):
            rule = self.normalizer_registry.get(spec.provider)
            if rule is None:
                normalized.append(spec)
                continue
            selectors_before = copy.deepcopy(spec.selectors)
            before_ok = self._validate_selectors(rule.validator, selectors_before)
            decision = "keep_original_valid" if before_ok else "keep_original_still_invalid"
            use = selectors_before
            after_ok = before_ok
            rule_trace = [
                {
                    "stage": "select_rule",
                    "provider": spec.provider,
                    "rule_kind": rule.kind,
                }
            ]
            if not before_ok:
                candidate = rule.normalize_selectors(copy.deepcopy(selectors_before))
                after_ok = self._validate_selectors(rule.validator, candidate)
                if after_ok:
                    decision = "use_normalized_fixed"
                    use = candidate
                elif candidate != selectors_before:
                    decision = "use_normalized_unvalidated"
                    use = candidate
                rule_trace.append(
                    {
                        "stage": "normalize",
                        "decision": decision,
                        "changed": candidate != selectors_before,
                        "validators": {
                            "before_ok": before_ok,
                            "after_ok": after_ok,
                        },
                    }
                )
            else:
                rule_trace.append(
                    {
                        "stage": "validate",
                        "decision": decision,
                        "validators": {
                            "before_ok": before_ok,
                            "after_ok": after_ok,
                        },
                    }
                )
            note = self._format_selectors_note(
                spec.provider,
                before_ok,
                after_ok,
                decision,
                selectors_before=selectors_before,
                selectors_after=use,
            )
            notes.append(note)
            rule_kind = rule.kind
            if replay_logger and rule_kind:
                provider_info_snapshot = None
                provider_info = self.provider_catalog.get(spec.provider)
                if isinstance(provider_info, ProviderInfo):
                    provider_info_snapshot = snapshot_provider_info(provider_info)
                elif spec.provider in self.schema_registry:
                    provider_info_snapshot = snapshot_provider_info(
                        ProviderInfo(
                            name=spec.provider,
                            capabilities=[],
                            selectors_schema=self.schema_registry[spec.provider],
                        )
                    )
                requires = []
                if not self._provider_info_snapshot_sufficient(provider_info_snapshot):
                    requires.append({"kind": "extra", "id": "planner_input_v1"})
                input_payload = {
                    "spec": {
                        "provider": spec.provider,
                        "mode": spec.mode,
                        "selectors": selectors_before,
                    },
                    "options": asdict(self.options),
                    "normalizer_rules": {spec.provider: rule_kind},
                }
                if provider_info_snapshot:
                    input_payload["provider_info_snapshot"] = provider_info_snapshot
                observed_payload = {
                    "out_spec": {
                        "provider": spec.provider,
                        "mode": spec.mode,
                        "selectors": use,
                    },
                }
                log_replay_case(
                    replay_logger,
                    id="plan_normalize.spec_v1",
                    meta={
                        "provider": spec.provider,
                        "mode": spec.mode,
                        "spec_idx": spec_idx,
                    },
                    input=input_payload,
                    observed=observed_payload,
                    diag={
                        "selectors_valid_before": before_ok,
                        "selectors_valid_after": after_ok,
                        "rule_trace": rule_trace,
                    },
                    requires=requires or None,
                    note=note,
                )
            if use == selectors_before:
                normalized.append(spec)
                continue
            data = spec.model_dump()
            data["selectors"] = use
            normalized.append(ContextFetchSpec(**data))
        return normalized

    @staticmethod
    def _validate_selectors(adapter: TypeAdapter[Any], selectors: Any) -> bool:
        try:
            adapter.validate_python(selectors)
        except ValidationError:
            return False
        return True

    @staticmethod
    def _format_selectors_note(
        provider: str,
        before_ok: bool,
        after_ok: bool,
        decision: str,
        *,
        selectors_before: Any,
        selectors_after: Any,
    ) -> str:
        payload = {
            "provider": provider,
            "selectors_validate_before": "ok" if before_ok else "error",
            "selectors_validate_after": "ok" if after_ok else "error",
            "selectors_normalization_decision": decision,
        }
        if decision != "keep_original_valid":
            payload["selectors_before"] = selectors_before
            payload["selectors_after"] = selectors_after
        return json.dumps(payload, ensure_ascii=False, default=str)

    @staticmethod
    def _provider_info_snapshot_sufficient(snapshot: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(snapshot, dict):
            return False
        selectors_schema = snapshot.get("selectors_schema")
        if isinstance(selectors_schema, dict) and selectors_schema:
            return True
        capabilities = snapshot.get("capabilities")
        if isinstance(capabilities, list) and capabilities:
            return True
        return False

    def _normalize_required_context(
        self, values: Iterable[str], notes: List[str]
    ) -> List[str]:
        normalized: List[str] = []
        seen: set[str] = set()
        for raw in values or []:
            name = self._resolve_provider(raw)
            if name is None:
                if self.options.allow_unknown_providers:
                    name = str(raw)
                else:
                    notes.append(f"required_context_unknown:{raw}")
                    continue
            if self.options.dedupe_required_context:
                if name in seen:
                    notes.append(f"required_context_duplicate:{name}")
                    continue
                seen.add(name)
            normalized.append(name)
        return normalized

    def _normalize_context_plan(
        self, specs: Iterable[ContextFetchSpec], notes: List[str]
    ) -> List[ContextFetchSpec]:
        normalized: List[ContextFetchSpec] = []
        seen: set[Tuple[str, str, str]] = set()
        for spec in specs or []:
            provider = self._resolve_provider(spec.provider)
            if provider is None:
                if self.options.allow_unknown_providers:
                    provider = spec.provider
                else:
                    notes.append(f"context_plan_unknown:{spec.provider}")
                    continue
            mode = str(spec.mode or self.options.default_mode)
            if mode not in {"full", "slice"}:
                notes.append(f"context_plan_mode_defaulted:{provider}:{mode}")
                mode = self.options.default_mode
            selectors = spec.selectors or {}
            if not isinstance(selectors, dict):
                notes.append(f"context_plan_selectors_invalid:{provider}")
                selectors = {}
            selectors = self._filter_selectors(provider, selectors, notes)
            key = (
                provider,
                mode,
                json.dumps(selectors, sort_keys=True, ensure_ascii=False),
            )
            if self.options.dedupe_context_plan and key in seen:
                notes.append(f"context_plan_duplicate:{provider}:{mode}")
                continue
            seen.add(key)
            normalized.append(
                ContextFetchSpec(
                    provider=provider,
                    mode=mode,
                    selectors=selectors,
                    max_tokens=spec.max_tokens,
                )
            )
        return normalized

    def _normalize_text_list(
        self,
        values: Optional[Iterable[Any]],
        notes: List[str],
        label: str,
    ) -> List[str]:
        if values is None:
            return []
        normalized: List[str] = []
        for raw in values:
            if not isinstance(raw, str):
                notes.append(f"{label}_non_string")
                continue
            item = raw.strip() if self.options.trim_text_fields else raw
            if not item:
                notes.append(f"{label}_empty")
                continue
            normalized.append(item)
        return normalized

    def _filter_selectors(
        self, provider: str, selectors: Dict[str, Any], notes: List[str]
    ) -> Dict[str, Any]:
        if not self.options.filter_selectors_by_schema:
            return selectors
        schema = self.schema_registry.get(provider)
        if not schema:
            return selectors
        properties = schema.get("properties")
        if not isinstance(properties, dict):
            return selectors
        allowed = set(properties.keys())
        filtered = {key: value for key, value in selectors.items() if key in allowed}
        if len(filtered) != len(selectors):
            notes.append(f"context_plan_selectors_filtered:{provider}")
        return filtered

    def _resolve_provider(self, name: Any) -> Optional[str]:
        if name is None:
            return None
        name_str = str(name)
        if name_str in self.provider_catalog:
            return name_str
        if not self.options.coerce_provider_case:
            return None
        key = self._provider_aliases.get(name_str.lower())
        return key

    @staticmethod
    def _build_provider_aliases(
        catalog: Dict[str, ProviderInfo]
    ) -> Dict[str, str]:
        aliases: Dict[str, str] = {}
        for key, info in catalog.items():
            aliases[key.lower()] = key
            aliases[info.name.lower()] = key
        return aliases
