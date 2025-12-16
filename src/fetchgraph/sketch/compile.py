from __future__ import annotations

from typing import Any, Dict, Optional

from ..core.models import ProviderInfo
from .diagnostics import SketchDiagnostics


def compile_sketch_payload(
    payload: Dict[str, Any], provider_info: Optional[ProviderInfo] = None
) -> Dict[str, Any]:
    """Compile a normalized sketch payload into native selectors.

    The current implementation preserves the payload as-is, acting as a thin
    compatibility layer for manual/CLI usage. Future versions can plug in
    richer compilation logic while keeping this adapter stable.
    """

    diagnostics = SketchDiagnostics()
    diagnostics.add_info("Sketch payload forwarded as native selectors")
    return payload
