# fetchgraph

Universal, library-style agent that plans what to fetch, fetches context from pluggable providers, and synthesizes an output.

**Pipeline:** PLAN → FETCH → (ASSESS/REFETCH)* → SYNTH → VERIFY → (REFINE)* → SAVE

## Install (dev)
```bash
pip install -e .
```

# Quick Start

### Selectors are JSON-only

Providers receive a `selectors` argument that **must be JSON-serializable**. The
shared alias `SelectorsDict` (exported from `fetchgraph.relational.types`) represents
`Dict[str, JSONValue]` and is used across protocols and models. The planner/LLM
produces this structure, so do not place runtime-only Python objects (e.g.
connections, DataFrames) into `selectors`; pass such hints through `**kwargs`
instead. Providers can publish the expected shape via `ProviderInfo.selectors_schema`
(a JSON Schema) and optional `examples` containing stringified JSON payloads.

Relational providers require `selectors` to include a string field `"op"` that
chooses the operation (e.g., `"schema"`, `"semantic_only"`, `"query"`). The
complete set of supported shapes is described by the schema returned from
`RelationalDataProvider.describe()`.

```python
from fetchgraph import (
  BaseGraphAgent, ContextPacker, BaselineSpec, ContextFetchSpec,
  TaskProfile, RawLLMOutput
)
from fetchgraph.core import make_llm_plan_generic, make_llm_synth_generic

# Define providers (implement ContextProvider protocol)
class SpecProvider:
    name = "spec"
    def fetch(self, feature_name, selectors=None, **kw): return {"content": f"Spec for {feature_name}"}
    def serialize(self, obj): return obj.get("content", "") if isinstance(obj, dict) else str(obj)

def dummy_llm(prompt: str, sender: str) -> str:
    if sender == "generic_plan":
        return '{"required_context":["spec"],"context_plan":[{"provider":"spec","mode":"full"}]}'
    if sender == "generic_synth":
        return "result: ok"
    return ""

profile = TaskProfile(
  task_name="Demo",
  goal="Produce YAML doc from spec",
  output_format="YAML: result: <...>"
)

agent = BaseGraphAgent(
  llm_plan=make_llm_plan_generic(dummy_llm, profile, {"spec": SpecProvider()}),
  llm_synth=make_llm_synth_generic(dummy_llm, profile),
  domain_parser=lambda raw: raw.text,  # RawLLMOutput -> Any
  saver=lambda feature_name, parsed: None,  # save side-effect
  providers={"spec": SpecProvider()},
  verifiers=[type("Ok",(),{"name":"ok","check":lambda self,out: []})()],
  packer=ContextPacker(max_tokens=2000, summarizer_llm=lambda t: t[:200]),
  baseline=[BaselineSpec(ContextFetchSpec(provider="spec"))],
)

print(agent.run("FeatureX"))
```

## Working with selectors

- **Plan-time inputs**: The planner/LLM crafts `selectors` (a `SelectorsDict`) for
  each `ContextFetchSpec`. These inputs must be JSON-serializable and should be
  validated by providers using their published JSON Schema.
- **Provider contract**: Implementations of `ContextProvider.fetch` should accept
  `selectors: Optional[SelectorsDict] = None` and treat `**kwargs` as optional
  runtime hints that may be non-serializable.
- **Schema + examples**: Providers can guide planners by returning
  `ProviderInfo(selectors_schema=..., examples=[...])` from `describe()`.

Example for a relational provider that requires an `"op"` selector:

```python
from fetchgraph import ProviderInfo
from fetchgraph.relational import SelectorsDict

class RelationalDataProvider:
    name = "relational"

    def fetch(self, feature_name: str, selectors: SelectorsDict, **kwargs):
        op = selectors.get("op")
        if not op:
            raise ValueError("selectors.op is required")
        ...  # existing logic for schema/semantic_only/query

    def describe(self) -> ProviderInfo:
        schema = {
            "oneOf": [
                {"type": "object", "required": ["op"], "properties": {"op": {"const": "schema"}}},
                {"type": "object", "required": ["op", "sql"], "properties": {"op": {"const": "query"}, "sql": {"type": "string"}}},
            ]
        }
        return ProviderInfo(
            name=self.name,
            selectors_schema=schema,
            examples=["{\"op\":\"schema\"}", "{\"op\":\"query\",\"sql\":\"select 1\"}"],
        )
```

During planning you can feed selectors into `ContextFetchSpec` to fix the
operation:

```python
fetch_spec = ContextFetchSpec(provider="relational", selectors={"op": "schema"})
```

### QuerySketch (schema-bound)

The `$dsl: "fetchgraph.dsl.query_sketch@v0"` envelope lets the planner specify
compact relational queries without manually qualifying field paths or listing
relations. During `compile_selectors`, the schema-aware binder resolves
unqualified fields to the appropriate entity, auto-inserts required relations,
and rewrites field references to the `relation.field` form expected by
providers.

Minimal payload example (unqualified field on a joined entity):

```json
{
  "$dsl": "fetchgraph.dsl.query_sketch@v0",
  "payload": {"from": "fbs", "where": [["system_name", "contains", "ЕСП"]], "take": 10}
}
```

After compilation, the native selectors include the join and fully qualified
field (e.g., `relations: ["fbs_as"]`, `filters.field: "fbs_as.system_name"`).

Known limitations:

- No fuzzy matching, aliases, or type-checking yet; resolution is purely
  schema-based.
- Ambiguity defaults to deterministic selection with warnings; set
  `ResolutionPolicy(ambiguity_strategy="ask")` to fail on ambiguous fields.
- `max_auto_join_depth` defaults to 2.

## CSV semantic backend for Pandas providers

`fetchgraph.relational.semantic.backend` ships a lightweight TF-IDF backend that turns a CSV
file into semantic embeddings and reuses them across runs. The flow is:

1. Build embeddings from a CSV once using `CsvEmbeddingBuilder` and persist them
   alongside the CSV.
2. Configure a `CsvSemanticBackend` with one or more `CsvSemanticSource` entries
   (one per entity) pointing at the CSV and saved embeddings.
3. Pass that backend into `PandasRelationalDataProvider` so semantic clauses can
   delegate matching to the precomputed vectors.

Example setup:

```python
from pathlib import Path
from fetchgraph.relational.semantic import (
    EmbeddingModel,
    CsvEmbeddingBuilder,
    CsvSemanticBackend,
    CsvSemanticSource,
)
from fetchgraph.relational import ColumnDescriptor, EntityDescriptor, PandasRelationalDataProvider

csv_path = Path("products.csv")
embedding_path = Path("products_embeddings.json")

# Build once (e.g., during deployment) to avoid recomputing embeddings at runtime.
CsvEmbeddingBuilder(
    csv_path=csv_path,
    entity="product",
    id_column="id",
    text_fields=["name", "description"],
    output_path=embedding_path,
).build()

semantic_backend = CsvSemanticBackend(
    {"product": CsvSemanticSource("product", csv_path, embedding_path)}
)

entities = [
    EntityDescriptor(
        name="product",
        columns=[ColumnDescriptor(name="id", role="primary_key"), ColumnDescriptor(name="name"), ColumnDescriptor(name="description")],
    )
]

provider = PandasRelationalDataProvider(
    name="products", entities=entities, relations=[], frames={"product": ...}, semantic_backend=semantic_backend
)
```

You can plug in an embedding model (for example, an OpenAI client) to build and
query dense embeddings instead of the default TF-IDF vectors:

```python
from fetchgraph.relational.semantic import (
    EmbeddingModel,
    CsvSemanticSource,
    CsvEmbeddingBuilder,
    CsvSemanticBackend,
)


class OpenAIEmbeddingModel:
    def __init__(self, client):
        self.client = client

    def embed_documents(self, texts):
        # replace with client.embeddings(...)
        return [[1.0, 0.0] for _ in texts]

    def embed_query(self, text):
        return self.embed_documents([text])[0]


embedding = OpenAIEmbeddingModel(client)

CsvEmbeddingBuilder(
    csv_path="fbs.csv",
    entity="fbs",
    id_column="id",
    text_fields=["name", "description"],
    output_path="fbs_embeddings.json",
    embedding_model=embedding,
).build()

csv_backend = CsvSemanticBackend(
    {
        "fbs": CsvSemanticSource(
            entity="fbs",
            csv_path=Path("fbs.csv"),
            embedding_path=Path("fbs_embeddings.json"),
        )
    },
    embedding_model=embedding,
)
```

At query time, `SemanticClause` filters sent to the relational provider will
call `semantic_backend.search(...)` with the requested entity, fields, and
query text. Fields must be a subset of the indexed CSV columns (not including
the reserved `__all__` combined projection). By default, field similarities are
summed; adjust the backend if you need a different aggregation strategy.

### pgvector / LangChain vector stores

If you already manage embeddings in PostgreSQL with ``pgvector`` via LangChain,
you can supply your existing vector stores directly:

```python
from langchain_community.vectorstores.pgvector import PGVector
from fetchgraph.relational.semantic import PgVectorSemanticBackend, PgVectorSemanticSource

vector_store = PGVector.from_existing_index(
    collection_name="product_vectors", connection_string="postgresql+psycopg://..."
)

semantic_backend = PgVectorSemanticBackend(
    {
        "product": PgVectorSemanticSource(
            entity="product",
            vector_store=vector_store,
            metadata_entity_key="entity",  # optional, defaults to "entity"
            metadata_field_key="field",    # optional, defaults to "field"
            id_metadata_keys=("id",),       # optional metadata key(s) to read the row identifier
            score_kind="distance",          # convert pgvector distances into similarity scores
        )
    }
)
```

The backend will filter returned documents by entity and requested fields using
Document metadata before converting scores into :class:`SemanticMatch` entries.

---

## LICENSE
```text
MIT License

Copyright (c) 2025 ...

Permission is hereby granted, free of charge, to any person obtaining a copy
...
```