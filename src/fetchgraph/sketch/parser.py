"""Parser entrypoint for selector sketches.

In this minimal version, parsing is a pass-through that wraps the payload into
``SketchNode``. It exists to keep the public surface stable while the sketch
format evolves.
"""

from __future__ import annotations

from typing import Any

from .ast import SketchNode
from .normalize import normalize_sketch_payload


def parse_sketch(raw: Any) -> SketchNode:
    normalized = normalize_sketch_payload(raw)
    return SketchNode(payload=normalized)
