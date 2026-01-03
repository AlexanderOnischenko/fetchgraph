from __future__ import annotations

"""Base relational provider abstraction."""

import json
import re
from typing import Any, List, Optional

from ...core.models import ProviderInfo
from ...core.protocols import ContextProvider, SupportsDescribe
from ..models import (
    EntityDescriptor,
    QueryResult,
    RelationalQuery,
    RelationDescriptor,
    SchemaRequest,
    SchemaResult,
    SemanticOnlyRequest,
    SemanticOnlyResult,
)
from ..types import SelectorsDict


class RelationalDataProvider(ContextProvider, SupportsDescribe):
    """Base relational data provider operating on structured selectors.

    Subclasses must avoid invoking LLMs internally and should handle only
    structured JSON selectors defined by :class:`RelationalRequest`.
    """

    name: str = "relational"
    entities: List[EntityDescriptor]
    relations: List[RelationDescriptor]

    def __init__(self, name: str, entities: List[EntityDescriptor], relations: List[RelationDescriptor]):
        self.name = name
        self.entities = entities
        self.relations = relations

    @staticmethod
    def _normalize_string(value: Any) -> str:
        """
        Normalize string for soft comparison:
        - cast to string
        - strip leading/trailing whitespace
        - lower-case
        - collapse internal whitespace sequences to a single space
        """
        s = str(value)
        s = s.strip()
        s = s.lower()
        s = re.sub(r"\s+", " ", s)
        return s

    # --- ContextProvider API ---
    def fetch(self, feature_name: str, selectors: Optional[SelectorsDict] = None, **kwargs) -> Any:
        """Fetch relational data according to structured JSON selectors.

        Parameters
        ----------
        selectors:
            JSON-serializable selector payload (:class:`SelectorsDict`) constructed by the
            planner/LLM. The payload **must** include a string ``"op"`` indicating the
            operation type (e.g. ``"schema"``, ``"semantic_only"``, ``"query"``); accepted
            shapes for each operation are described by the JSON Schema emitted from
            :meth:`describe` via ``ProviderInfo.selectors_schema``. The provider raises a
            ``ValueError`` if ``"op"`` is missing.
        **kwargs:
            Runtime hints or options that are not part of the planner contract and may be
            non-JSON-serializable; passed through without interpretation.
        """
        selectors = selectors or {}
        op = selectors.get("op")
        if op is None:
            raise ValueError("Relational selectors must include 'op' field.")

        if op == "schema":
            return self._handle_schema()
        if op == "semantic_only":
            req = SemanticOnlyRequest.model_validate(selectors)
            return self._handle_semantic_only(req)
        if op == "query":
            req = RelationalQuery.model_validate(selectors)
            return self._handle_query(req)
        raise ValueError(f"Unsupported op: {op}")

    def serialize(self, obj: Any) -> str:
        """Return the LLM-facing textual form of provider outputs.

        The BaseGraphAgent stores both this text (for prompt inclusion) and the
        original ``obj`` inside :class:`fetchgraph.core.ContextItem.raw` so that
        agent tools can reuse structured results without re-fetching.
        """
        if isinstance(obj, SchemaResult):
            entities = ", ".join(e.name for e in obj.entities)
            relations = ", ".join(r.name for r in obj.relations)
            return f"Schema: entities=({entities}); relations=({relations})"
        if isinstance(obj, SemanticOnlyResult):
            parts = [f"{m.entity}:{m.id} ({m.score:.2f})" for m in obj.matches[:10]]
            return "Semantic matches: " + "; ".join(parts)
        if isinstance(obj, QueryResult):
            lines: List[str] = []
            for row in obj.rows[:10]:
                parts = [f"{k}={v}" for k, v in row.data.items()]
                for rk, rv in row.related.items():
                    parts.append(f"{rk}=" + ",".join(f"{k}:{v}" for k, v in rv.items()))
                lines.append(" | ".join(parts))
            if obj.aggregations:
                agg_parts = [f"{k}={v.value}" for k, v in obj.aggregations.items()]
                lines.append("Aggregations: " + ", ".join(agg_parts))
            if len(obj.rows) > 10:
                lines.append(f"... trimmed {len(obj.rows) - 10} rows ...")
            return "\n".join(lines) or "(empty result)"
        return str(obj)

  # --- SupportsDescribe API ---
    def describe(self) -> ProviderInfo:
        """
        Описать возможности провайдера и схему селекторов для планировщика.

        - selectors_schema: JSON Schema для допустимых selectors.
        - examples: примеры селекторов с реальными именами сущностей/связей.
        - description: краткое текстовое описание домена (entities/relations + hints).
        """

        # --- 1) Базовые схемы запросов ---
        schema_req = SchemaRequest.model_json_schema()
        semantic_req = SemanticOnlyRequest.model_json_schema()
        query_schema = RelationalQuery.model_json_schema()

        entity_names = [e.name for e in self.entities]
        relation_names = [r.name for r in self.relations]

        # Патчим enum для root_entity и relations в RelationalQuery
        q_props = query_schema.get("properties", {})
        if "root_entity" in q_props:
            q_props["root_entity"]["enum"] = entity_names
        if "relations" in q_props and isinstance(q_props["relations"].get("items"), dict):
            q_props["relations"]["items"]["enum"] = relation_names

        # Патчим enum для entity в SemanticOnlyRequest
        s_props = semantic_req.get("properties", {})
        if "entity" in s_props:
            s_props["entity"]["enum"] = entity_names

        selectors_schema = {
            "oneOf": [
                schema_req,
                semantic_req,
                query_schema,
            ]
        }

        # --- 2) Текстовое описание домена (entities/relations) ---
        schema_config = getattr(self, "_schema_config", None)

        # базовая шапка
        if schema_config and schema_config.description:
            header = schema_config.description
        else:
            header = "Реляционный провайдер данных."

        # сущности
        entity_lines: List[str] = []
        for e in self.entities:
            cols = e.columns or []
            pk_cols = [c.name for c in cols if getattr(c, "role", None) == "primary_key"]
            sem_cols = [c.name for c in cols if getattr(c, "semantic", False)]
            parts = [e.label or e.name]
            if pk_cols:
                parts.append(f"PK: {', '.join(pk_cols)}")
            if sem_cols:
                parts.append(f"semantic: {', '.join(sem_cols)}")
            # planning_hint из SchemaConfig, если есть
            hint = ""
            if schema_config:
                for ec in schema_config.entities:
                    if ec.name == e.name and ec.planning_hint:
                        hint = ec.planning_hint
                        break
            if hint:
                parts.append(hint)
            entity_lines.append(f"- {e.name}: " + "; ".join(parts))

        # связи
        relation_lines: List[str] = []
        for r in self.relations:
            rel_desc = f"- {r.name}: {r.from_entity}.{r.join.from_column} -> {r.to_entity}.{r.join.to_column} ({r.cardinality})"
            text_parts = [rel_desc]
            if r.semantic_hint:
                text_parts.append(r.semantic_hint)
            if schema_config:
                for rc in schema_config.relations:
                    if rc.name == r.name and rc.planning_hint:
                        text_parts.append(rc.planning_hint)
                        break
            relation_lines.append(" — ".join(text_parts))

        # общие подсказки
        provider_hints: List[str] = []
        if schema_config:
            provider_hints = schema_config.planning_hints or []

        description_parts: List[str] = [header]
        if entity_lines:
            description_parts.append("Сущности:")
            description_parts.extend(entity_lines)
        if relation_lines:
            description_parts.append("Связи:")
            description_parts.extend(relation_lines)
        if provider_hints:
            description_parts.append("Подсказки для планировщика:")
            description_parts.extend(f"- {h}" for h in provider_hints)

        description = "\n".join(description_parts)

        # --- 3) Авто-примеры селекторов ---
        examples: List[str] = []

        # schema
        examples.append(json.dumps({"op": "schema"}, ensure_ascii=False))

        # простой query по первой сущности
        if entity_names:
            e0 = entity_names[0]
            cols0 = (self.entities[0].columns or [])
            col0 = cols0[0].name if cols0 else "id"
            examples.append(json.dumps(
                {
                    "op": "query",
                    "root_entity": e0,
                    "select": [{"expr": f"{e0}.{col0}"}],
                    "limit": 10,
                },
                ensure_ascii=False,
            ))
            examples.append(json.dumps(
                {
                    "op": "query",
                    "root_entity": e0,
                    "case_sensitivity": False,
                    "filters": {
                        "type": "comparison",
                        "field": col0,
                        "op": "=",
                        "value": "Marketing",
                    },
                    "limit": 20,
                },
                ensure_ascii=False,
            ))

        # semantic_only по первой семантической сущности
        sem_entity: Optional[str] = None
        for e in self.entities:
            if any(getattr(c, "semantic", False) for c in e.columns or []):
                sem_entity = e.name
                break
        if sem_entity:
            examples.append(json.dumps(
                {
                    "op": "semantic_only",
                    "entity": sem_entity,
                    "query": "<поисковый запрос на естественном языке>",
                    "mode": "filter",
                    "top_k": 30,
                },
                ensure_ascii=False,
            ))

        # query с relation, если есть
        if relation_names:
            rel0 = self.relations[0]
            root = rel0.to_entity  # условно берём to_entity как root
            examples.append(json.dumps(
                {
                    "op": "query",
                    "root_entity": root,
                    "relations": [rel0.name],
                    "limit": 20,
                },
                ensure_ascii=False,
            ))

        # если в SchemaConfig заданы кастомные examples — переопределяем
        if schema_config and schema_config.examples:
            examples = schema_config.examples

        return ProviderInfo(
            name=self.name,
            description=description,
            capabilities=["schema", "row_query", "aggregate", "semantic_search"],
            selectors_schema=selectors_schema,
            examples=examples,
        )

    # --- protected methods ---
    def _handle_schema(self) -> SchemaResult:
        return SchemaResult(entities=self.entities, relations=self.relations)

    def _handle_semantic_only(self, req: SemanticOnlyRequest) -> SemanticOnlyResult:  # pragma: no cover - abstract
        raise NotImplementedError

    def _handle_query(self, req: RelationalQuery) -> QueryResult:  # pragma: no cover - abstract
        raise NotImplementedError


__all__ = ["RelationalDataProvider"]
