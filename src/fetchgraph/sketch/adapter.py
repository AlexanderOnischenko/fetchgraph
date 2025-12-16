from __future__ import annotations

from typing import Any, Dict, Optional

from ..core.models import ProviderInfo
from .compile import compile_sketch_payload
from .normalize import normalize_sketch_payload


class SketchNotAllowedError(ValueError):
    """Raised when sketch input is provided but disabled."""


SKETCH_KEYS = {"$dsl", "$sketch"}


def compile_sketch_to_native(
    selectors: Dict[str, Any], provider_info: Optional[ProviderInfo] = None
) -> Dict[str, Any]:
    """Compile a sketch envelope into native selectors.

    The function expects an envelope that includes one of the canonical sketch
    keys (``"$dsl"`` or ``"$sketch"``). The nested payload is normalized and
    compiled into a native selector structure understood by providers.
    """

    for key in SKETCH_KEYS:
        if key in selectors:
            payload = selectors[key]
            normalized = normalize_sketch_payload(payload)
            root_payload = normalized.get("payload", normalized)
            return compile_sketch_payload(root_payload, provider_info)
    if "payload" in selectors:
        normalized = normalize_sketch_payload(selectors["payload"])
        root_payload = normalized.get("payload", normalized)
        return compile_sketch_payload(root_payload, provider_info)
    raise ValueError("selectors do not contain a sketch payload")


def coerce_selectors_to_native(
    selectors: Optional[Dict[str, Any]],
    provider_info: Optional[ProviderInfo],
    *,
    allow_sketch: bool,
) -> Dict[str, Any]:
    """Normalize selectors before execution.

    - When ``allow_sketch`` is False, sketch markers are rejected with a
      user-friendly error.
    - When ``allow_sketch`` is True, sketch envelopes are compiled to the
      native selector format expected by providers.
    - Plain native selectors are passed through unchanged.
    """

    if selectors is None:
        return {}
    if not isinstance(selectors, dict):
        raise ValueError("selectors must be a JSON object")

    has_sketch = any(k in selectors for k in SKETCH_KEYS) or "payload" in selectors
    if has_sketch:
        if not allow_sketch:
            raise SketchNotAllowedError(
                "Sketch selectors are disabled for planner runtime; "
                "provide native selectors instead."
            )
        return compile_sketch_to_native(selectors, provider_info)

    return selectors
