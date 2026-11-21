# FetchGraph

**One semantic access layer for your AI agents.**

> **Describe your data once. Let any agent query it.**

AI agents can already call databases, APIs, vector stores, and MCP tools.

The harder problem is teaching every agent what your data **means**:

- where customers, orders, products, documents, or metrics live,
- how those entities relate,
- which fields and operations are valid,
- and how a natural-language question should be translated into retrieval.

If every agent has to rediscover that from raw schemas and tool descriptions, the same retrieval logic gets rebuilt again and again.

FetchGraph moves that knowledge into a reusable layer.

```text
Agents / Copilots / Workflows
              │
              ▼
        ┌─────────────┐
        │ FetchGraph  │
        │ semantic    │
        │ access layer│
        └──────┬──────┘
               │
       ┌───────┼────────┐
       ▼       ▼        ▼
      SQL     APIs      RAG
                       / files
```

You describe each data source as a **provider**: what it contains, what it can do, and how it may be queried.

Given a natural-language task, FetchGraph can then:

1. decide which providers are relevant,
2. build a structured retrieval plan,
3. validate and normalize provider-specific selectors,
4. fetch the required data,
5. fit the evidence into a context budget,
6. and pass the assembled context to an LLM to produce the result.

The goal is simple:

> Adding the second, tenth, or hundredth agent should not require teaching each of them your data architecture again.

---

## Why FetchGraph?

Imagine your company has:

- customers in one database,
- orders in another,
- product information behind an API,
- documentation in a vector index,
- and several agents that need different combinations of that data.

You *can* give every agent direct access to all of those systems and let it figure everything out at runtime.

That works well for simple cases.

But as data sources and agents multiply, every agent starts solving the same questions:

```text
Where does this information live?
Which entity should I query?
Which relation should I follow?
Which filters are valid?
Do I need SQL, semantic search, or both?
```

FetchGraph makes those answers reusable.

For example, a provider can describe a relational source roughly as:

```text
Customers
- id
- segment
- country

Orders
- customer_id
- order_date
- amount

Customer -> Orders via customer_id

Supported operations:
- filter
- relation traversal
- aggregation
- semantic search
```

Then an application can ask:

> Which products grew fastest among enterprise customers in Germany last quarter?

The planner can use the provider descriptions to determine the relevant entities, relations, filters, and aggregations instead of requiring that logic to be hard-coded into the application.

---

## FetchGraph vs. MCP, direct SQL, and RAG

FetchGraph is not intended to replace any of them.

### MCP

MCP is a great way to expose tools and resources to agents.

FetchGraph solves a different problem:

```text
MCP:        What can the agent call?
FetchGraph: What information does this task need, and how should it be retrieved?
```

A FetchGraph application can be wrapped as a tool or exposed over MCP so multiple agents share the same data model and retrieval behavior.

### Direct SQL

For one agent and one well-understood database, letting the model inspect the schema and write SQL directly may be simpler.

FetchGraph becomes useful when you want the knowledge about entities, relations, valid operations, and retrieval rules to be **shared across agents and sources**, rather than rediscovered by every model invocation.

### RAG

Traditional RAG is excellent when the problem is:

```text
question -> similarity search -> chunks -> prompt
```

FetchGraph is useful when retrieval also involves structured operations:

```text
intent
  -> choose source
  -> choose entities
  -> filters / relations / aggregations / semantic search
  -> fetch evidence
  -> context
```

Vector search can simply be one of the provider capabilities.

---

## Core abstraction: providers

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
    capabilities=["query", "aggregate"],
    selectors_schema={...},
    examples=[...],
)
```

FetchGraph builds a catalog from these descriptions and gives it to the planner.

The planner produces structured fetch specifications rather than arbitrary application code:

```json
{
  "required_context": ["orders"],
  "context_plan": [
    {
      "provider": "orders",
      "mode": "slice",
      "selectors": {
        "op": "query",
        "root_entity": "orders",
        "limit": 20
      },
      "max_tokens": 2000
    }
  ]
}
```

Providers remain responsible for executing those selectors against the real system.

That means a provider may wrap a database client, an ODBC connection, an HTTP API, a vector store, a local dataframe, or another retrieval system without exposing those runtime objects to the planner.

---

# Quick start

## Install

```bash
pip install fetchgraph
```

Connect your LLM and expose a data source through a provider:

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
            description="Customer accounts, subscriptions, and billing history.",
            capabilities=["lookup"],
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
    # OpenAI, Anthropic, a local model, LiteLLM, etc.
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

The application does not hard-code when the customer provider should be used or which selector should be sent.

The provider describes itself:

```text
customers
├── what data it contains
├── what it can do
├── selector schema
└── examples
```

The planner uses that contract to decide whether the provider is needed and how to query it.

---

## What happens during a run?

The generic agent follows this flow:

```text
TASK
  │
  ▼
PLAN
  │   choose providers + selectors
  ▼
FETCH
  │   execute provider contracts
  ▼
PACK
  │   keep context within a token budget
  ▼
ASSESS / REFETCH   (optional)
  │
  ▼
SYNTHESIZE
  │
  ▼
VERIFY / REFINE    (optional)
  │
  ▼
RESULT
```

The planning and retrieval steps are explicit, which makes them easier to inspect and evaluate separately from final answer generation.

---

## Relational data

FetchGraph includes relational abstractions for structured business data.

The public API includes:

- `EntityDescriptor`
- `ColumnDescriptor`
- `RelationDescriptor`
- `RelationalDataProvider`
- `SqlRelationalDataProvider`
- `PandasRelationalDataProvider`
- `CompositeRelationalProvider`

Relational providers can describe entities and relations to the planner and support structured operations such as filtering, selecting fields, aggregations, relation traversal, and semantic clauses.

Internally, the planning pipeline can normalize selectors, validate them, bind field references to the schema, normalize aggregations, and retry planning when a generated plan cannot be executed.

This is the main reason to use FetchGraph instead of simply putting a raw database schema into every agent prompt: the data-access model becomes a reusable contract with a validation and execution layer behind it.

---

## Semantic retrieval

Semantic search is a provider capability rather than a special application architecture.

FetchGraph currently includes:

- a lightweight CSV semantic backend for Pandas providers,
- pluggable embedding models,
- pgvector / LangChain vector-store integration,
- semantic clauses inside relational requests.

This allows an application to combine structured and semantic retrieval rather than choosing between "SQL" and "RAG" at the application level.

---

## Selectors

Provider selectors are JSON-serializable structures generated during planning.

```python
ContextFetchSpec(
    provider="orders",
    mode="slice",
    selectors={
        "op": "query",
        "root_entity": "orders",
    },
)
```

Providers can publish the expected selector shape through `ProviderInfo.selectors_schema` and examples through `ProviderInfo.examples`.

Runtime-only Python objects such as database connections, DataFrames, or model clients should stay inside the provider or be passed as runtime hints — not inside planner-generated selectors.

---

## Current capabilities

- schema-driven provider catalog
- LLM-generated structured retrieval plans
- pluggable providers for heterogeneous sources
- JSON Schema-guided selectors
- relational entities and relations
- filtering and aggregations
- structured + semantic retrieval
- SQL and Pandas relational providers
- composite relational providers
- bounded context packing
- optional assess/refetch loop
- optional verification/refinement
- replay / planning diagnostics in the normalization pipeline
- no agent-framework lock-in

---

## When should I use FetchGraph?

FetchGraph is a good fit when:

- several agents need access to the same corporate data,
- data is spread across heterogeneous systems,
- questions require more than document similarity search,
- structured filters, joins, or aggregations matter,
- you want retrieval logic to be inspectable and reusable,
- or you want to separate context planning from the main agent reasoning loop.

You probably do **not** need FetchGraph if:

- you have one small corpus and ordinary top-k RAG is enough,
- you have one simple database and are happy to let one strong model write SQL directly,
- or there is no meaningful data model worth sharing across applications.

---

## Examples

Working examples live under:

```text
examples/
├── demo_qa/
└── retail_orders/
```

`examples/demo_qa` contains a schema-driven QA testbed with planning artifacts and evaluation cases.

`examples/retail_orders` demonstrates questions over relational business data.

---

## Direction

The long-term direction is to make the semantic layer cheaper to create and richer to reuse.

One natural extension is **schema introspection and ontology-assisted provider generation**: deriving an initial agent-readable model from database schemas, relationships, metadata, and existing corporate glossaries, then letting humans enrich it where business semantics cannot be inferred reliably.

That is a direction, not a requirement for using FetchGraph today.

The core invariant remains:

> **Describe your data once. Reuse that knowledge across agents.**

---

## License

MIT
