from __future__ import annotations

"""Base relational provider abstraction."""

from typing import Any, List, Optional
import json
import re

from ...core.catalog import (
    MAX_COLUMNS_PREVIEW,
    MAX_ENTITIES_PREVIEW,
    MAX_ENUM_ITEMS,
    MAX_RELATIONS_PREVIEW,
    compact_enum,
)
from ...core.models import ProviderInfo, SelectorDialectInfo
from ...core.protocols import ContextProvider, SupportsDescribe
from ..types import SelectorsDict
from ..models import (
    ComparisonOp,
    QueryResult,
    RelationalQuery,
    SchemaRequest,
    SchemaResult,
    SemanticOnlyRequest,
    SemanticOnlyResult,
    EntityDescriptor,
    RelationDescriptor,
)


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
        entity_by_name = {e.name: e for e in self.entities}
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

        schema_config = getattr(self, "_schema_config", None)

        # selectors_digest for LLM-friendly catalog view
        def _build_enum_digest(values: list[str]):
            preview, more = compact_enum(values, MAX_ENUM_ITEMS)
            return {"preview": preview, "omitted": more}

        def _columns_preview(entity: EntityDescriptor):
            cols = entity.columns or []
            preview: list[str] = []
            for c in cols[:MAX_COLUMNS_PREVIEW]:
                if c.name not in preview:
                    preview.append(c.name)

            pk_cols = [c.name for c in cols if getattr(c, "role", None) == "primary_key"]
            sem_cols = [c.name for c in cols if getattr(c, "semantic", False)]

            for name in pk_cols + sem_cols:
                if name not in preview:
                    preview.append(name)

            unique_preview = []
            for name in preview:
                if name not in unique_preview:
                    unique_preview.append(name)

            omitted = max(0, len(cols) - len(unique_preview))
            return unique_preview, omitted, pk_cols, sem_cols

        ops_digest = {
            "schema": {"summary": "Return request schema/metadata", "required": [], "optional": []},
            "semantic_only": {
                "summary": "Semantic matches only (no joined rows)",
                "required": ["entity", "query"],
                "optional": ["fields", "top_k"],
                "enums": {"entity": _build_enum_digest(entity_names)},
            },
            "query": {
                "summary": "Main query operation",
                "required": ["root_entity"],
                "optional": [
                    "select",
                    "filters",
                    "relations",
                    "semantic_clauses",
                    "group_by",
                    "aggregations",
                    "limit",
                    "offset",
                    "case_sensitivity",
                ],
                "enums": {
                    "root_entity": _build_enum_digest(entity_names),
                    "relations": _build_enum_digest(relation_names),
                },
            },
        }

        filter_ops = list(ComparisonOp.__args__) if hasattr(ComparisonOp, "__args__") else []
        aggregation_ops = ["count", "count_distinct", "sum", "min", "max", "avg"]

        entities_preview: list[dict[str, any]] = []
        entities_hints: list[dict[str, Any]] = []
        omitted_entities = 0
        entity_planning_map: dict[str, str] = {}
        if schema_config:
            entity_planning_map = {
                ec.name: ec.planning_hint for ec in schema_config.entities if ec.planning_hint
            }
        for idx, e in enumerate(self.entities):
            if idx >= MAX_ENTITIES_PREVIEW:
                omitted_entities += 1
                continue
            preview_cols, omitted_cols, pk_cols, sem_cols = _columns_preview(e)
            hint = entity_planning_map.get(e.name, "")
            entities_preview.append(
                {
                    "name": e.name,
                    "pk": pk_cols[0] if len(pk_cols) == 1 else pk_cols,
                    "semantic_fields": {"preview": sem_cols, "omitted": 0},
                    "columns_preview": {"preview": preview_cols, "omitted": omitted_cols},
                }
            )
            entities_hints.append(
                {
                    "name": e.name,
                    "label": e.label or None,
                    "pk": pk_cols[0] if len(pk_cols) == 1 else pk_cols,
                    "semantic_fields": sem_cols,
                    "columns_preview": preview_cols,
                    "hint": hint,
                }
            )

        relations_preview: list[dict[str, any]] = []
        relations_hints: list[dict[str, Any]] = []
        omitted_relations = 0
        relation_planning_map: dict[str, str] = {}
        if schema_config:
            relation_planning_map = {
                rc.name: rc.planning_hint for rc in schema_config.relations if rc.planning_hint
            }
        for idx, r in enumerate(self.relations):
            if idx >= MAX_RELATIONS_PREVIEW:
                omitted_relations += 1
                continue
            combined_hint_parts: list[str] = []
            if r.semantic_hint:
                combined_hint_parts.append(r.semantic_hint)
            if relation_planning_map.get(r.name):
                combined_hint_parts.append(relation_planning_map[r.name])
            combined_hint = "; ".join(part for part in combined_hint_parts if part)
            relations_preview.append(
                {
                    "name": r.name,
                    "from_entity": r.from_entity,
                    "to_entity": r.to_entity,
                    "cardinality": r.cardinality,
                    "join_keys": {
                        "from": {"preview": [f"{r.from_entity}.{r.join.from_column}"], "omitted": 0},
                        "to": {"preview": [f"{r.to_entity}.{r.join.to_column}"], "omitted": 0},
                    },
                }
            )
            relations_hints.append(
                {
                    "name": r.name,
                    "from_entity": r.from_entity,
                    "to_entity": r.to_entity,
                    "cardinality": r.cardinality,
                    "join_keys": {
                        "from": f"{r.from_entity}.{r.join.from_column}",
                        "to": f"{r.to_entity}.{r.join.to_column}",
                    },
                    "hint": combined_hint,
                }
            )

        selectors_digest = {
            "digest_version": "fetchgraph.selectors_digest@v1",
            "selector_kind": "relational",
            "preferred_selectors": "dsl",
            "ops": ops_digest,
            "rules": {
                "field_paths": {
                    "preferred_style": "entity.field",
                    "allow_unqualified": True,
                    "notes": "If unqualified, resolved against root_entity",
                },
                "filters": {
                    "logical_ops": ["and", "or"],
                    "comparison_ops": filter_ops,
                    "null_handling": "sql-like",
                    "case_sensitivity": {
                        "default": False,
                        "affects": ["like", "ilike", "starts", "ends"],
                    },
                },
                "semantics": {
                    "available": True,
                    "modes": ["filter", "boost"],
                    "default_top_k": 100,
                    "threshold_supported": True,
                    "notes": "Semantic clauses can filter or boost rows",
                },
                "group_by": {
                    "supported": True,
                    "notes": "GroupBy accepts [ {entity?, field, alias?} ]",
                },
                "aggregations": {
                    "supported": True,
                    "ops": aggregation_ops,
                    "notes": "AggregationSpec: {field, agg, alias?}",
                },
                "limits": {
                    "default_limit": 1000,
                    "max_limit": None,
                    "offset_supported": True,
                },
            },
            "entities": {
                "preview": entities_preview,
                "omitted": omitted_entities,
            },
            "relations": {
                "preview": relations_preview,
                "omitted": omitted_relations,
            },
            "dsl_hints": {
                "dialect_id": "fetchgraph.dsl.query_sketch@v0",
                "payload_keys": ["from", "get", "where", "with", "take"],
                "where_clause_forms": [["field", "value"], ["field", "op", "value"]],
                "supported_ops_preview": _build_enum_digest(filter_ops),
                "notes": "Use short lists; combine with 'with' to follow relations",
            },
        }

        # --- 2) Текстовое описание домена (entities/relations) ---
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
        def _first_textual_column(entity_name: str) -> Optional[str]:
            entity = entity_by_name.get(entity_name)
            if not entity:
                return None
            for col in entity.columns or []:
                if getattr(col, "semantic", False):
                    return col.name
            if entity.columns:
                return entity.columns[0].name
            return None

        examples: List[str] = []

        # schema
        examples.append(json.dumps({"op": "schema"}, ensure_ascii=False))

        relation_text_example: Optional[tuple[RelationDescriptor, str]] = None
        for rel in self.relations:
            candidate_col = _first_textual_column(rel.to_entity)
            if candidate_col:
                relation_text_example = (rel, candidate_col)
                break

        if relation_text_example:
            rel, text_col = relation_text_example
            root = rel.from_entity
            relation_example = {
                "op": "query",
                "root_entity": root,
                "relations": [rel.name],
                "select": [
                    {"expr": f"{root}.id"},
                    {"expr": f"{rel.to_entity}.{text_col}"},
                ],
                "filters": {
                    "type": "comparison",
                    "entity": rel.to_entity,
                    "field": text_col,
                    "op": "ilike",
                    "value": "%<abbr>%",
                },
                "limit": 20,
            }
            examples.append(json.dumps(relation_example, ensure_ascii=False))
        elif entity_names:
            e0 = entity_names[0]
            cols0 = (self.entities[0].columns or [])
            col0 = cols0[0].name if cols0 else "id"
            filter_example = {
                "op": "query",
                "root_entity": e0,
                "filters": {
                    "type": "comparison",
                    "field": col0,
                    "op": "=",
                    "value": "Marketing",
                },
                "limit": 20,
            }
            examples.append(json.dumps(filter_example, ensure_ascii=False))

        # semantic_only по первой семантической сущности
        sem_entity: Optional[str] = None
        for e in self.entities:
            if any(getattr(c, "semantic", False) for c in e.columns or []):
                sem_entity = e.name
                break
        if sem_entity and len(examples) < 3:
            examples.append(
                json.dumps(
                    {
                        "op": "semantic_only",
                        "entity": sem_entity,
                        "query": "<поисковый запрос на естественном языке>",
                        "top_k": 30,
                    },
                    ensure_ascii=False,
                )
            )

        # если в SchemaConfig заданы кастомные examples — переопределяем
        if schema_config and schema_config.examples:
            examples = schema_config.examples

        payload_example: dict[str, Any] = {"take": 20}
        if relation_text_example:
            rel, text_col = relation_text_example
            payload_example.update(
                {
                    "from": rel.from_entity,
                    "get": [f"{rel.from_entity}.id", f"{rel.to_entity}.{text_col}"],
                    "where": [[f"{rel.to_entity}.{text_col}", "ilike", "%ЕСП%"]],
                    "with": [rel.name],
                }
            )
        elif entity_names:
            first_entity = self.entities[0]
            cols = first_entity.columns or []
            payload_example["from"] = first_entity.name
            if cols:
                payload_example["get"] = [f"{first_entity.name}.{cols[0].name}"]
                payload_example["where"] = [[cols[0].name, "=", "<value>"]]
            if relation_names:
                payload_example["with"] = [relation_names[0]]

        dialects = [
            SelectorDialectInfo(
                id="fetchgraph.dsl.query_sketch@v0",
                description="Compact JSON5-like sketch for relational queries.",
                payload_format="json-object",
                envelope_example=json.dumps(
                    {
                        "$dsl": "fetchgraph.dsl.query_sketch@v0",
                        "payload": payload_example,
                    },
                    ensure_ascii=False,
                ),
                notes=(
                    "payload keys: from, get, where, with, take; "
                    "where: ['field','value'] or ['field','op','value']; ops follow comparison_ops"
                ),
            )
        ]

        return ProviderInfo(
            name=self.name,
            description=description,
            capabilities=["schema", "row_query", "aggregate", "semantic_search"],
            selectors_schema=selectors_schema,
            examples=examples,
            selector_dialects=dialects,
            selectors_digest=selectors_digest,
            preferred_selectors="dsl" if dialects else None,
            planning_hints=provider_hints,
            entities_hints=entities_hints,
            relations_hints=relations_hints,
        )

    # --- protected methods ---
    def _handle_schema(self) -> SchemaResult:
        return SchemaResult(entities=self.entities, relations=self.relations)

    def _handle_semantic_only(self, req: SemanticOnlyRequest) -> SemanticOnlyResult:  # pragma: no cover - abstract
        raise NotImplementedError

    def _handle_query(self, req: RelationalQuery) -> QueryResult:  # pragma: no cover - abstract
        raise NotImplementedError


__all__ = ["RelationalDataProvider"]
