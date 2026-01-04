from __future__ import annotations

"""Pydantic models for relational providers.

The module defines the schemas for describing entities/relations, incoming
requests, and outgoing responses that relational providers exchange. These
models are intentionally LLM-agnostic and rely solely on structured JSON
inputs/outputs.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class ColumnDescriptor(BaseModel):
    """Describe a column in an entity/table."""

    name: str
    type: str = "text"
    role: Optional[str] = None
    semantic: bool = False


class EntityDescriptor(BaseModel):
    """Describe an entity/table."""

    name: str
    label: Optional[str] = None
    columns: List[ColumnDescriptor] = Field(default_factory=list)


class RelationJoin(BaseModel):
    """Join configuration between entities."""

    from_entity: str
    from_column: str
    to_entity: str
    to_column: str
    join_type: Literal["inner", "left", "right", "outer"] = "left"


class RelationDescriptor(BaseModel):
    """Describe relation between two entities."""

    name: str
    from_entity: str
    to_entity: str
    cardinality: Literal["1_to_1", "1_to_many", "many_to_1", "many_to_many"] = "1_to_many"
    join: RelationJoin
    semantic_hint: Optional[str] = None


class SelectExpr(BaseModel):
    """Returned expression."""

    expr: str
    alias: Optional[str] = None


class ComparisonOp(str):
    """Symbolic comparison operator alias."""


class ComparisonFilter(BaseModel):
    """Basic comparison filter."""

    type: Literal["comparison"] = "comparison"
    entity: Optional[str] = None
    field: str
    op: str
    value: Any


class LogicalFilter(BaseModel):
    """Logical combination of filters."""

    type: Literal["logical"] = "logical"
    op: Literal["and", "or"] = "and"
    clauses: List["FilterClause"] = Field(default_factory=list)


FilterClause = ComparisonFilter | LogicalFilter

LogicalFilter.model_rebuild()


class SemanticClause(BaseModel):
    """Semantic search clause for an entity."""

    entity: str
    fields: List[str] = Field(default_factory=list)
    query: str
    top_k: int = 100
    mode: Literal["filter", "boost"] = "filter"
    threshold: Optional[float] = None


class AggregationSpec(BaseModel):
    """Aggregation specification."""

    field: str
    agg: str
    alias: Optional[str] = None


class GroupBySpec(BaseModel):
    """Group by specification."""

    entity: Optional[str] = None
    field: str
    alias: Optional[str] = None


class RelationalQuery(BaseModel):
    """Main relational query request."""

    op: Literal["query"] = "query"
    root_entity: str
    select: List[SelectExpr] = Field(default_factory=list)
    filters: Optional[FilterClause] = None
    relations: List[str] = Field(default_factory=list)
    semantic_clauses: List[SemanticClause] = Field(default_factory=list)
    group_by: List[GroupBySpec] = Field(default_factory=list)
    aggregations: List[AggregationSpec] = Field(default_factory=list)
    limit: Optional[int] = 1000
    offset: Optional[int] = 0
    case_sensitivity: bool = False
    """
    If False (default), string comparisons are performed in a "soft" mode:
    - case-insensitive
    - trimmed
    - tolerant to minor differences (see provider docs)
    If True, providers should use strict, case-sensitive comparisons.
    """


class SchemaRequest(BaseModel):
    """Request schema description."""

    op: Literal["schema"] = "schema"


class SemanticOnlyRequest(BaseModel):
    """Request semantic search results only."""

    op: Literal["semantic_only"] = "semantic_only"
    entity: str
    fields: List[str] = Field(default_factory=list)
    query: str
    top_k: int = 100


RelationalRequest = SchemaRequest | SemanticOnlyRequest | RelationalQuery


class RelatedEntityData(BaseModel):
    """Data for a related entity returned with a row."""

    entity: str
    data: Dict[str, Any]


class RowResult(BaseModel):
    """Single row result including related entities."""

    entity: str
    data: Dict[str, Any]
    related: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class AggregationResult(BaseModel):
    """Aggregation result."""

    key: str
    value: Any


class QueryResult(BaseModel):
    """Result of a relational query."""

    rows: List[RowResult] = Field(default_factory=list)
    aggregations: Dict[str, AggregationResult] = Field(default_factory=dict)
    meta: Dict[str, Any] = Field(default_factory=dict)


class SchemaResult(BaseModel):
    """Schema description result."""

    entities: List[EntityDescriptor] = Field(default_factory=list)
    relations: List[RelationDescriptor] = Field(default_factory=list)


class SemanticMatch(BaseModel):
    """Single semantic match."""

    entity: str
    id: Any
    score: float


class SemanticOnlyResult(BaseModel):
    """Semantic-only search result."""

    matches: List[SemanticMatch] = Field(default_factory=list)


RelationalResponse = SchemaResult | SemanticOnlyResult | QueryResult


__all__ = [
    "AggregationResult",
    "AggregationSpec",
    "ColumnDescriptor",
    "ComparisonFilter",
    "ComparisonOp",
    "EntityDescriptor",
    "FilterClause",
    "GroupBySpec",
    "LogicalFilter",
    "RelatedEntityData",
    "RelationDescriptor",
    "RelationJoin",
    "RelationalQuery",
    "RelationalRequest",
    "RelationalResponse",
    "RowResult",
    "SchemaRequest",
    "SchemaResult",
    "SelectExpr",
    "SemanticClause",
    "SemanticMatch",
    "SemanticOnlyRequest",
    "SemanticOnlyResult",
]
