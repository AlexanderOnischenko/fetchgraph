from pydantic import __version__ as _pydantic_version

# Fetchgraph relies on the Pydantic v2 API (model_validate/model_dump, etc.).
# Import errors should surface early if an incompatible version is installed.
if not _pydantic_version.startswith("2"):
    raise ImportError(
        "fetchgraph requires pydantic>=2.0; detected version %s" % _pydantic_version
    )

from .core import (
    # types
    RawLLMOutput,
    ProviderInfo,
    TaskProfile,
    ContextFetchSpec,
    BaselineSpec,
    ContextItem,
    RefetchDecision,
    Plan,
    # protocols
    ContextProvider,
    SupportsFilter,
    SupportsDescribe,
    Verifier,
    Saver,
    LLMInvoke,
    # classes
    ContextPacker,
    BaseGraphAgent,
    # helpers
    make_llm_plan_generic,
    make_llm_synth_generic,
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
    "PandasRelationalDataProvider",
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
]
