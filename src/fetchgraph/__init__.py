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
    BaseGraphAgent,
    BaselineSpec,
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
    RelationalDataProvider,
    RelationalQuery,
    RelationalRequest,
    RelationalResponse,
    RelationDescriptor,
    RelationJoin,
    RowResult,
    SchemaRequest,
    SchemaResult,
    SelectExpr,
    SelectorsDict,
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
from .artifact_context import (  # noqa: E402
    RetrievalPolicy,
    ContextRequest,
    EvidenceCapsule,
    EvidencePack,
    ExplainPack,
    CaseDossier,
    ValidatorDefectPack,
    InvestigationSession,
    fetch as artifact_context_fetch,
    explain_target,
    explain_derivation,
    case_dossier,
    validator_defect_pack,
)
from .artifact_graph import (  # noqa: E402
    ArtifactGraphProvider,
    FileSystemArtifactGraphProvider,
    create_artifact_graph_provider,
)
from .pipeline_schema import (  # noqa: E402
    QueryTemplate,
    RelationRule,
    ArtifactTypeSchema,
    PipelineSchema,
    create_default_pipeline_schema,
    get_schema,
    load_schema,
)
from .adapters import (  # noqa: E402
    ArtifactContextProvider,
    create_artifact_context_provider,
)

__all__ = (
    # Core
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
    # Relational
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
    "PandasRelationalDataProvider",
    # Artifact Context Engine
    "RetrievalPolicy",
    "ContextRequest",
    "EvidenceCapsule",
    "EvidencePack",
    "ExplainPack",
    "CaseDossier",
    "ValidatorDefectPack",
    "InvestigationSession",
    "artifact_context_fetch",
    "explain_target",
    "explain_derivation",
    "case_dossier",
    "validator_defect_pack",
    # Artifact Graph
    "ArtifactGraphProvider",
    "FileSystemArtifactGraphProvider",
    "create_artifact_graph_provider",
    # Pipeline Schema
    "QueryTemplate",
    "RelationRule",
    "ArtifactTypeSchema",
    "PipelineSchema",
    "create_default_pipeline_schema",
    "get_schema",
    "load_schema",
    # Adapters
    "ArtifactContextProvider",
    "create_artifact_context_provider",
)
