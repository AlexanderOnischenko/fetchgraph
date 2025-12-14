from __future__ import annotations
from importlib import resources

def load_pkg_text(rel_path: str) -> str:
    """Read a text resource from package (e.g., 'prompts/plan_generic.md')."""
    with resources.files("fetchgraph").joinpath(rel_path).open("r", encoding="utf-8") as f:
        return f.read()

def render_prompt(template: str, **values) -> str:
    """Very simple {key}-style renderer (no conditionals)."""
    out = template
    # longest keys first to avoid prefix collisions
    for k, v in sorted(values.items(), key=lambda kv: len(kv[0]), reverse=True):
        out = out.replace("{" + k + "}", "" if v is None else str(v))
    return out
