from __future__ import annotations

from typing import Any, Dict


def normalize_sketch_payload(payload: Any) -> Dict[str, Any]:
    """Return a normalized sketch payload.

    The function ensures the payload is a mapping suitable for compilation.
    """

    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValueError("Sketch payload must be an object")
    return payload
