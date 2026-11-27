# llm-output-repair integration (experimental)

The integration branch is based on Fetchgraph `dev`; `main` remains unchanged.
The dependency is pinned to an immutable Git commit in `pyproject.toml`.
**Do not publish Fetchgraph directly from this branch.** For a normal PyPI
release, replace the direct URL dependency with a released library version.

## Setup

```bash
git checkout integration/llm-output-repair
python -m pip install -e '.[dev,mcp,demo]'
```

## Private crash recording

Recording is **off** by default. Explicitly configure an absolute path outside
your Git worktree before running Fetchgraph or the demo QA benchmark:

```bash
export FETCHGRAPH_REPAIR_DUMP_DIR="$HOME/private-llm-repair/fetchgraph"
```

`JsonParser` uses the library's tolerant repair by default, retaining
JSON arrays and prose extraction. `PlanParser` defaults to conservative
`safe` policy. The Retrieval API's planning and answer parsing also use
`parse_json(policy="safe")`. Fetchgraph keeps its own domain validation:
an invalid selector, plan structure or citation remains invalid even if the
syntax was repairable.

Only final library parsing errors are captured. Raw inputs are *unredacted*.
No upload or automatic inference is performed. The library provides offline
`llm-repair replay`, explicit `promote` and `verify` commands, and
deduplication of exact repeated failures.

## Release gate

1. Run unit/regression CI and compare the demo QA baseline, including genuine
   raw errors for `qa_008` and `qa_009` if those artifacts are available.
2. Fix library compatibility issues in the library's own feature branch;
   repin its tested SHA in Fetchgraph and rerun CI.
3. Merge and publish a versioned `llm-output-repair` package.
4. Change Fetchgraph's dependency from Git SHA to the released version.
5. Merge the Fetchgraph integration into `dev`, and only then consider
   the next Fetchgraph release.

The temporary Git dependency must not be merged into Fetchgraph's
publishable `main` metadata.
