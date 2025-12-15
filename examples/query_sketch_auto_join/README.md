# QuerySketch auto-join example

This example shows how the `$dsl: "fetchgraph.dsl.query_sketch@v0"` envelope can
be used to reference fields on related entities without manually listing
relations. When the selectors are compiled, the schema-aware binder finds the
necessary join path, rewrites field references, and injects relations for the
native relational providers.

## Sample plan snippet

```json
{
  "context_plan": [
    {
      "provider": "rel",
      "selectors": {
        "$dsl": "fetchgraph.dsl.query_sketch@v0",
        "payload": {
          "from": "fbs",
          "where": [["system_name", "contains", "ЕСП"]],
          "take": 10
        }
      }
    }
  ]
}
```

After `compile_plan_selectors` runs (automatically in the planning pipeline),
the selector for provider `rel` will look like:

```json
{
  "op": "query",
  "root_entity": "fbs",
  "relations": ["fbs_as"],
  "filters": {"op": "contains", "field": "fbs_as.system_name", "value": "ЕСП"},
  "limit": 10,
  "offset": 0,
  "case_sensitivity": false
}
```

Notice how `system_name` was resolved onto the related `as` entity and
automatically qualified with the `fbs_as` relation while keeping the query
otherwise unchanged.
