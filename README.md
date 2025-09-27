# fetchgraph

Universal, library-style agent that plans what to fetch, fetches context from pluggable providers, and synthesizes an output.

**Pipeline:** PLAN → FETCH → (ASSESS/REFETCH)* → SYNTH → VERIFY → (REFINE)* → SAVE

## Install (dev)
```bash
pip install -e .
```

# Quick Start

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

---

## LICENSE
```text
MIT License

Copyright (c) 2025 ...

Permission is hereby granted, free of charge, to any person obtaining a copy
...
```