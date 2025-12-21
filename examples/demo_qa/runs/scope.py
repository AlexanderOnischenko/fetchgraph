from __future__ import annotations

import hashlib
import json
from typing import Mapping, Optional, Set


def _scope_payload(
    *,
    cases_hash: str,
    include_tags: Set[str] | None,
    exclude_tags: Set[str] | None,
    include_ids: Set[str] | None,
    exclude_ids: Set[str] | None,
) -> dict[str, object]:
    return {
        "cases_hash": cases_hash,
        "include_tags": sorted(include_tags) if include_tags else None,
        "exclude_tags": sorted(exclude_tags) if exclude_tags else None,
        "include_ids": sorted(include_ids) if include_ids else None,
        "exclude_ids": sorted(exclude_ids) if exclude_ids else None,
    }


def _scope_hash(scope: Mapping[str, object]) -> str:
    payload = json.dumps(scope, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


__all__ = ["_scope_hash", "_scope_payload"]
