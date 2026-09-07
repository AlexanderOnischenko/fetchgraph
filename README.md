# FetchGraph

## The problem

Most LLM applications eventually need more than one retrieval strategy.

Traditional RAG usually reduces this to:

```text
question → similarity search → chunks → prompt
```

That works well when the problem really is **"find similar text."**

But many real applications need to answer a different question:

> **What information do I need, where should I get it from, and how should I query that source?**

And the data sources themselves can be organized as RAGs, DBs, APIs, etc.

As the number of data sources grows, so does the orchestration code.

The LLM already understands the user's intent.

It should be able to plan the context it needs.

## What fetchgraph does

Fetchgraph turns context retrieval into a **schema-driven planning problem**.

Each data source becomes a **provider**.

A provider exposes two things:

### 1. How to fetch data

```python
provider.fetch(
    feature_name,
    selectors={...},
)
```

### 2. How the planner is allowed to query it

```python
ProviderInfo(
    name="orders",
    description="Orders and customer purchases",
    selectors_schema={...},
    examples=[...],
)
```

From those contracts, fetchgraph builds a provider catalog for the planner.

The planner can then produce a plan such as:

```json
{
  "required_context": [
    "orders"
  ],
  "context_plan": [
    {
      "provider": "orders",
      "mode": "full",
      "selectors": {
        "op": "query",
        "root_entity": "orders",
        "limit": 20
      }
    }
  ]
}
```

Fetchgraph executes the plan instead of asking your application to encode every retrieval path manually.

## Features

- JSON-only selectors with JSON Schema hints for planners
- Pluggable context providers (APIs, relational sources, etc.)
- Relational providers with semantic clauses
- CSV semantic backend (TF-IDF) for pandas providers
- pgvector / LangChain vector store integration
- Library-style API: no framework lock-in

# Quick Start

## Install
```bash
pip install fetchgraph
```

Connect your LLM and expose some data through a provider:

```python
import json

from fetchgraph import (
    ProviderInfo,
    TaskProfile,
    create_generic_agent,
)


class CustomerProvider:
    name = "customers"

    def describe(self) -> ProviderInfo:
        return ProviderInfo(
            name=self.name,
            description="Customer accounts, subscriptions and billing history.",
            selectors_schema={
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string"},
                },
                "additionalProperties": False,
            },
            examples=[
                '{"customer_id": "cus_123"}',
            ],
        )

    def fetch(self, feature_name, selectors=None, **kwargs):
        customer_id = (selectors or {}).get("customer_id")

        # Replace with your DB/API call.
        return load_customer(customer_id)

    def serialize(self, obj):
        return json.dumps(obj, default=str)


def llm(prompt: str, sender: str) -> str:
    # Call OpenAI, Anthropic, a local model,
    # LiteLLM, or any other LLM here.
    return my_model(prompt)


agent = create_generic_agent(
    llm_invoke=llm,
    providers={
        "customers": CustomerProvider(),
    },
    task_profile=TaskProfile(
        task_name="Customer support",
        goal="Answer questions using available customer data.",
        output_format="Plain text",
    ),
    saver=lambda name, result: None,
)

answer = agent.run(
    "Why was customer cus_123 charged twice?"
)

print(answer)
```

The important part is that the planner does **not** need hard-coded knowledge of `CustomerProvider`.

The provider describes itself:

```text
customers
├── what data it contains
├── what it can do
├── selectors JSON Schema
└── examples
```

Fetchgraph puts that contract in front of the planner. The LLM decides whether the provider is needed and generates the appropriate selectors.

---

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

## Examples

The repository contains working examples under:

```text
examples/
├── demo_qa/
└── retail_orders/
```

`demo_qa` demonstrates a schema-driven question-answering agent over relational data, including planning and evaluation cases.

See:

[examples/demo_qa](examples/demo_qa)

---

## When should I use fetchgraph?

Fetchgraph is useful when your LLM needs to work across **multiple heterogeneous sources** and the required context changes from request to request.

Good examples:

* analytics over relational business data,
* agents combining APIs and internal documentation,
* support assistants using account data plus knowledge bases,
* architecture agents reading specifications, ADRs and repositories,
* research agents that need different retrieval strategies,
* applications mixing structured and semantic search.

You probably do **not** need fetchgraph if your entire retrieval problem is:

```text
one corpus → vector search → top 5 chunks
```

A normal RAG pipeline will be simpler.

---

## The core idea

The important abstraction in fetchgraph is not a vector store and not a particular agent graph.

It is this boundary:

```text
LLM intent
    │
    ▼
structured context plan
    │
    ▼
provider contracts
    │
    ▼
real data
```

Once data sources expose machine-readable contracts, context selection itself becomes something the LLM can plan.

That is what fetchgraph is built for.

---


## LICENSE
```text
MIT License

Copyright (c) 2025 ...

Permission is hereby granted, free of charge, to any person obtaining a copy
...
```
