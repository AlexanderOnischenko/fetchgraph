"""Relational providers and helpers."""

from .types import SelectorsDict
from .models import (
    AggregationResult,
    AggregationSpec,
    ColumnDescriptor,
    ComparisonFilter,
    ComparisonOp,
    EntityDescriptor,
    FilterClause,
    GroupBySpec,
    LogicalFilter,
    RelatedEntityData,
    RelationDescriptor,
    RelationJoin,
    RelationalQuery,
    RelationalRequest,
    RelationalResponse,
    RowResult,
    SchemaRequest,
    SchemaResult,
    SelectExpr,
    SemanticClause,
    SemanticMatch,
    SemanticOnlyRequest,
    SemanticOnlyResult,
)
from .providers import (
    CompositeRelationalProvider,
    RelationalDataProvider,
    SqlRelationalDataProvider,
)
from .semantic import (
    CsvEmbeddingBuilder,
    CsvSemanticBackend,
    CsvSemanticSource,
    PgVectorSemanticBackend,
    PgVectorSemanticSource,
    SemanticBackend,
    VectorStoreLike,
)

try:  # optional dependency
    from .providers import PandasRelationalDataProvider
except Exception:  # pragma: no cover - optional dependency path
    PandasRelationalDataProvider = None  # type: ignore[assignment]

__all__ = (
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
    "SelectorsDict",
    "SemanticClause",
    "SemanticMatch",
    "SemanticOnlyRequest",
    "SemanticOnlyResult",
    "RelationalDataProvider",
    "SqlRelationalDataProvider",
    "CompositeRelationalProvider",
    "SemanticBackend",
    "CsvSemanticBackend",
    "CsvSemanticSource",
    "CsvEmbeddingBuilder",
    "PgVectorSemanticBackend",
    "PgVectorSemanticSource",
    "VectorStoreLike",
    "PandasRelationalDataProvider",
)
