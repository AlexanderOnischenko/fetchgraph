from __future__ import annotations

from typing import Any, Dict, List

from ..models import Plan, ContextFetchSpec, RawLLMOutput
from .json_parser import JsonParser
from .exceptions import OutputParserException


class PlanParser(JsonParser[Plan]):
    """Парсер LLM-вывода шага PLAN."""

    def __init__(self) -> None:
        super().__init__(key=None)

    def _clean(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned: List[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            change = str(item.get("change", "")).lower().strip()
            if change not in {"new", "update"}:
                change = "new"
            item["change"] = change
            cleaned.append(item)
        return cleaned

    def to_model(self, data: Any) -> Plan:
        if not isinstance(data, dict):
            raise OutputParserException(
                f"План должен быть JSON-объектом, получено: {type(data).__name__}"
            )

        rc = data.get("required_context") or []
        if not isinstance(rc, list):
            raise OutputParserException("'required_context' должен быть списком")

        aq = data.get("adr_queries") or None
        if aq is not None and not isinstance(aq, list):
            raise OutputParserException("'adr_queries' должен быть списком строк или null")

        cs = data.get("constraints") or None
        if cs is not None and not isinstance(cs, list):
            raise OutputParserException("'constraints' должен быть списком строк или null")

        entities_raw = data.get("entities") or []
        dtos_raw = data.get("dtos") or []
        if not isinstance(entities_raw, list):
            raise OutputParserException("'entities' должен быть списком словарей")
        if not isinstance(dtos_raw, list):
            raise OutputParserException("'dtos' должен быть списком словарей")

        entities = self._clean(entities_raw)
        for entity in entities:
            entity["fields"] = self._clean(entity.get("fields") or [])
            entity["relations"] = self._clean(entity.get("relations") or [])

        dtos = self._clean(dtos_raw)

        cp_raw = data.get("context_plan") or []
        cp: List[ContextFetchSpec] = []
        if isinstance(cp_raw, list):
            for item in cp_raw:
                if not isinstance(item, dict):
                    continue
                try:
                    cp.append(ContextFetchSpec.model_validate(item))
                except Exception:
                    continue

        normalized = {
            "required_context": rc,
            "adr_queries": aq,
            "constraints": cs,
            "entities": entities,
            "dtos": dtos,
            "context_plan": cp,
        }

        try:
            return Plan.model_validate(normalized)
        except Exception as exc:
            raise OutputParserException(
                f"План не соответствует схеме Plan: {exc}"
            ) from exc

    def parse(self, raw: RawLLMOutput) -> Plan:
        return super().parse(raw)
