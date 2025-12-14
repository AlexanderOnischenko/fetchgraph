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

from .core import (  # noqa: E402
    BaselineSpec,
    BaseGraphAgent,
    ContextFetchSpec,
    ContextItem,
    ContextPacker,
    ContextProvider,
    LLMInvoke,
    Plan,
    ProviderInfo,
    RawLLMOutput,
    RefetchDecision,
    Saver,
    SupportsDescribe,
    SupportsFilter,
    TaskProfile,
    Verifier,
    create_generic_agent,
    make_llm_plan_generic,
    make_llm_synth_generic,
)
from .relational import (  # noqa: E402
    AggregationResult,
    AggregationSpec,
    ColumnDescriptor,
    CompositeRelationalProvider,
    EntityDescriptor,
    GroupBySpec,
    LogicalFilter,
    PandasRelationalDataProvider,
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
    SelectorsDict,
    SelectExpr,
    SemanticBackend,
    SemanticClause,
    SemanticMatch,
    SemanticOnlyRequest,
    SemanticOnlyResult,
    SqlRelationalDataProvider,
)
from .relational.semantic import (  # noqa: E402
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
    "create_generic_agent",
    "make_llm_plan_generic",
    "make_llm_synth_generic",
    "SelectorsDict",
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
