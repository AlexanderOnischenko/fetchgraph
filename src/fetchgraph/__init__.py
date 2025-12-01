from pydantic import __version__ as _pydantic_version

# Fetchgraph relies on the Pydantic v2 API (model_validate/model_dump, etc.).
# Import errors should surface early if an incompatible version is installed.
if not _pydantic_version.startswith("2"):
    raise ImportError(
        "fetchgraph requires pydantic>=2.0; detected version %s" % _pydantic_version
    )

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

from .core import (
    ContextPacker,
    BaseGraphAgent,
    make_llm_plan_generic,
    make_llm_synth_generic,
)
from .models import (
    RawLLMOutput,
    ProviderInfo,
    TaskProfile,
    ContextFetchSpec,
    BaselineSpec,
    ContextItem,
    RefetchDecision,
    Plan,
)
from .protocols import (
    ContextProvider,
    SupportsFilter,
    SupportsDescribe,
    Verifier,
    Saver,
    LLMInvoke,
)
from .relational_provider import (
    AggregationResult,
    AggregationSpec,
    ColumnDescriptor,
    CompositeRelationalProvider,
    EntityDescriptor,
    GroupBySpec,
    LogicalFilter,
    PandasRelationalDataProvider,
    SqlRelationalDataProvider,
    RelatedEntityData,
    RelationDescriptor,
    RelationJoin,
    RelationalDataProvider,
    RelationalQuery,
    RelationalRequest,
    RelationalResponse,
    RowResult,
    SchemaRequest,
    SchemaResult,
    SelectExpr,
    SemanticBackend,
    SemanticClause,
    SemanticMatch,
    SemanticOnlyRequest,
    SemanticOnlyResult,
)
from .semantic_backend import (
    CsvEmbeddingBuilder,
    CsvSemanticBackend,
    CsvSemanticSource,
    PgVectorSemanticBackend,
    PgVectorSemanticSource,
    VectorStoreLike,
)

__all__ = [
    "RawLLMOutput",
    "ProviderInfo",
    "TaskProfile",
    "ContextFetchSpec",
    "BaselineSpec",
    "ContextItem",
    "RefetchDecision",
    "Plan",
    "ContextProvider",
    "SupportsFilter",
    "SupportsDescribe",
    "Verifier",
    "Saver",
    "LLMInvoke",
    "ContextPacker",
    "BaseGraphAgent",
    "make_llm_plan_generic",
    "make_llm_synth_generic",
    "AggregationResult",
    "AggregationSpec",
    "ColumnDescriptor",
    "CompositeRelationalProvider",
    "EntityDescriptor",
    "GroupBySpec",
    "LogicalFilter",
    "SqlRelationalDataProvider",
    "RelatedEntityData",
    "RelationDescriptor",
    "RelationJoin",
    "RelationalDataProvider",
    "RelationalQuery",
    "RelationalRequest",
    "RelationalResponse",
    "RowResult",
    "SchemaRequest",
    "SchemaResult",
    "SelectExpr",
    "SemanticBackend",
    "SemanticClause",
    "SemanticMatch",
    "SemanticOnlyRequest",
    "SemanticOnlyResult",
    "CsvSemanticBackend",
    "CsvSemanticSource",
    "CsvEmbeddingBuilder",
    "PgVectorSemanticBackend",
    "PgVectorSemanticSource",
    "VectorStoreLike",
]

if PandasRelationalDataProvider is not None:
    __all__.append("PandasRelationalDataProvider")
