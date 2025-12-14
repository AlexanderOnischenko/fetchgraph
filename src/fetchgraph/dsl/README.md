# QuerySketch DSL (v0)

This module provides a minimal, tolerant parser and normalizer for the QuerySketch
JSON5-like DSL. It accepts raw strings or dictionaries, parses them into a lightweight
AST, and normalizes keys, operators, and defaults according to `spec.yaml`.

## Usage

```python
from fetchgraph.dsl import parse_query_sketch, normalize_query_sketch, parse_and_normalize

raw = "{ from: streams, where: [[\"status\", \"active\"]] }"
parsed, parse_diags = parse_query_sketch(raw)
normalized, diags = normalize_query_sketch(parsed)
# or
normalized, diags = parse_and_normalize(raw)
```

The normalized shape always includes canonical keys and a canonical `where` form:

```python
normalized.from_
normalized.get      # defaults to ["*"]
normalized.with_    # defaults to []
normalized.take     # defaults from spec (200)
normalized.where    # WhereExpr with all/any/not
```

Diagnostics collect warnings and errors encountered during parsing or normalization.
```
diags.has_errors()
for msg in diags.messages:
    print(msg.code, msg.message, msg.path, msg.severity)
```
