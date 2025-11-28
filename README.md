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
shared alias `SelectorsDict` (see `fetchgraph/json_types.py`) represents
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
from fetchgraph.json_types import SelectorsDict
from fetchgraph.models import ProviderInfo

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

---

## LICENSE
```text
MIT License

Copyright (c) 2025 ...

Permission is hereby granted, free of charge, to any person obtaining a copy
...
```