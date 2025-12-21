from __future__ import annotations

from typing import Iterable, Mapping, Optional

from ..runner import RunResult


def _missed_case_ids(planned_case_ids: Iterable[str], executed_results: Mapping[str, RunResult] | None) -> set[str]:
    planned_set = set(planned_case_ids)
    if not executed_results:
        return planned_set
    try:
        executed_ids = set(executed_results.keys())
    except Exception:
        executed_ids = set()
    return planned_set - executed_ids


__all__ = ["_missed_case_ids"]
