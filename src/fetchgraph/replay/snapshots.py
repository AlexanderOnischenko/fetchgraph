from __future__ import annotations

from typing import Dict, Mapping

from ..core.models import ProviderInfo


def snapshot_provider_info(info: ProviderInfo) -> Dict[str, object]:
    payload: Dict[str, object] = {"name": info.name}
    if info.capabilities:
        payload["capabilities"] = list(info.capabilities)
    if info.selectors_schema:
        payload["selectors_schema"] = info.selectors_schema
    return payload


def snapshot_provider_catalog(provider_catalog: Mapping[str, object]) -> Dict[str, object]:
    snapshot: Dict[str, object] = {}
    for key, info in provider_catalog.items():
        if not isinstance(info, ProviderInfo):
            continue
        snapshot[key] = snapshot_provider_info(info)
    return snapshot
