from __future__ import annotations

"""Base relational provider abstraction."""

from typing import Any, Dict, List, Optional

from .core import ContextProvider, ProviderInfo, SupportsDescribe
from .relational_models import (
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

    # --- ContextProvider API ---
    def fetch(self, feature_name: str, selectors: Optional[Dict[str, Any]] = None, **kwargs) -> Any:
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
        schema = {
            "oneOf": [
                SchemaRequest.model_json_schema(),
                SemanticOnlyRequest.model_json_schema(),
                RelationalQuery.model_json_schema(),
            ]
        }
        examples = [
            '{"op":"schema"}',
            '{"op":"query","root_entity":"order","relations":["order_customer"],"filters":{"type":"comparison","entity":"order","field":"customer_id","op":"=","value":123},"limit":50}',
            '{"op":"query","root_entity":"customer","relations":["order_customer"],"semantic_clauses":[{"entity":"customer","fields":["name","notes"],"query":"фармацевтические клиенты","mode":"filter","top_k":30}],"select":[{"expr":"customer.name"}],"limit":20}',
            '{"op":"query","root_entity":"order","relations":["order_customer"],"group_by":[{"entity":"customer","field":"name"}],"aggregations":[{"field":"total","agg":"sum","alias":"total_spend"}]}',
        ]
        return ProviderInfo(
            name=self.name,
            description="Реляционный провайдер данных",
            capabilities=["schema", "row_query", "aggregate", "semantic_search"],
            selectors_schema=schema,
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
