# FetchGraph
## One semantic data layer for all your AI agents

**Describe your corporate data once. Let any agent query it.**

Today, teams building AI agents repeatedly solve the same problem: teaching every new agent where company data lives, what tables and APIs mean, how entities relate to each other, which filters are valid, and how to retrieve the right evidence for a particular task.

Giving an agent direct SQL access does not remove this problem. The agent can inspect schemas and write queries itself, but it has to rediscover the company’s data model again and again. As the number of databases, APIs, document stores and agents grows, this retrieval logic becomes duplicated across prompts, tools, MCP servers and application code.

**FetchGraph provides a shared semantic access layer between AI agents and corporate data.**

A team connects its data sources to FetchGraph and describes what they contain in an agent-readable form: entities, relationships, available fields, supported operations and relevant business semantics. A provider might say, for example:

> Customers live here. Orders live there. Customers have segments and regions. Orders belong to customers. You may filter by date and segment and aggregate order totals.

FetchGraph can obtain part of this description automatically from database schemas, metadata and existing corporate ontologies, while teams can enrich it with the business meaning that cannot be inferred reliably.

From that point on, applications no longer need to teach every agent how each underlying source works.

An agent simply asks for information:

> “Which products grew fastest among enterprise customers in Germany last quarter?”

FetchGraph determines which sources and entities are relevant, builds a structured retrieval plan, binds it to the actual source schemas, validates the query, executes it and returns the relevant evidence.

The underlying data may come from PostgreSQL, Oracle, an ODBC source, an internal API, a document system or a semantic index. The agent operates against one consistent information layer instead of learning every physical system independently.

## The customer experience

A platform or data team configures FetchGraph once:

**Corporate data → FetchGraph providers → shared semantic model**

Application teams can then reuse it from many places:

**Agent / Copilot / Workflow → FetchGraph → relevant corporate evidence**

FetchGraph may be called directly as a library or service, or exposed to agents through MCP. Instead of maintaining an MCP server — and a growing amount of retrieval logic — for every individual data source, a company can expose one agent-facing data layer backed by many FetchGraph providers.

MCP answers:

> “What can the agent call?”

FetchGraph answers:

> “Where does the information the agent needs actually live, and how should we retrieve it correctly?”

## Why customers use FetchGraph

The main benefit is **reuse of corporate data knowledge across agents**.

Without FetchGraph, each agent must reason over physical schemas, understand joins, discover terminology and construct retrieval logic itself.

With FetchGraph, this knowledge becomes shared infrastructure:

**describe once → validate once → reuse across agents.**

This should make agent systems easier to build, easier to evaluate and less dependent on every individual model successfully rediscovering the structure of corporate data.

It also creates a natural place to improve retrieval over time. Better entity descriptions, new relationships, ontology mappings or query rules immediately become available to every agent using the layer.

## What FetchGraph is not

FetchGraph is not another general-purpose agent framework.

It does not try to replace agent reasoning, MCP, databases or RAG.

Agents still decide **what they want to know**. Databases and APIs remain the systems of record. MCP can remain the protocol through which an agent accesses FetchGraph. Vector search can remain one of the available retrieval mechanisms.

FetchGraph owns the layer in between:

> **turn an information need into validated retrieval over heterogeneous corporate data.**

## Where we want to go

The long-term goal is for connecting corporate data to AI agents to require progressively less manual modelling.

FetchGraph should be able to inspect schemas, relationships, metadata and existing enterprise glossaries and propose an initial **corporate ontology**: logical entities, relationships and their mapping to physical sources.

Humans remain able to correct and enrich that model, but they should not have to describe every table from scratch.

The resulting architecture is:

**Corporate systems → corporate ontology / semantic graph → FetchGraph → every AI agent**

The success criterion is simple:

> Adding the second, tenth or hundredth agent should not require teaching each of them the company’s data architecture again.
