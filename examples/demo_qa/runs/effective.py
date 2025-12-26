from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Mapping, Optional

from ..runner import RunResult, bad_statuses, load_results, summarize
from ..utils import dump_json
from .coverage import _missed_case_ids
from .layout import _effective_paths
from .io import write_results


def _load_effective_results(artifacts_dir: Path, tag: str) -> tuple[dict[str, RunResult], Optional[dict], Path]:
    results_path, meta_path = _effective_paths(artifacts_dir, tag)
    meta: Optional[dict] = None
    results: dict[str, RunResult] = {}
    if results_path.exists():
        results = load_results(results_path)
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = None
    return results, meta, results_path


def _write_effective_results(results_path: Path, results: Mapping[str, RunResult]) -> None:
    results_path.parent.mkdir(parents=True, exist_ok=True)
    ordered = [results[cid] for cid in sorted(results)]
    write_results(results_path, ordered)


def _reason_text(res: RunResult) -> str:
    if res.reason:
        return res.reason
    if res.error:
        return res.error
    expected = getattr(res, "expected_check", None)
    if expected and getattr(expected, "detail", None):
        return expected.detail
    return ""


def _build_effective_diff(
    before: Mapping[str, RunResult],
    after: Mapping[str, RunResult],
    *,
    fail_on: str,
    require_assert: bool,
    run_id: str,
    tag: str,
    note: str | None,
    run_dir: Path,
    results_path: Path,
    scope_hash: str,
) -> dict[str, object]:
    bad = bad_statuses(fail_on, require_assert)
    before_bad = {cid for cid, res in before.items() if res.status in bad}
    after_bad = {cid for cid, res in after.items() if res.status in bad}
    ids = set(before) | set(after)
    regressed: list[dict[str, object]] = []
    fixed: list[dict[str, object]] = []
    changed_bad: list[dict[str, object]] = []
    new_cases: list[dict[str, object]] = []
    other_changed: list[dict[str, object]] = []
    for cid in ids:
        prev = before.get(cid)
        cur = after.get(cid)
        prev_status = prev.status if prev else None
        cur_status = cur.status if cur else None
        if prev is None and cur is not None:
            new_cases.append({"id": cid, "to": cur_status})
            continue
        if cur is None or prev is None:
            continue
        if prev_status == cur_status:
            continue
        entry = {"id": cid, "from": prev_status, "to": cur_status, "reason": _reason_text(cur)}
        was_bad = cid in before_bad
        now_bad = cid in after_bad
        if not was_bad and now_bad:
            regressed.append(entry)
        elif was_bad and not now_bad:
            fixed.append(entry)
        elif was_bad and now_bad:
            changed_bad.append(entry)
        else:
            other_changed.append(entry)
    return {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        "tag": tag,
        "note": note,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "results_path": str(results_path),
        "fail_on": fail_on,
        "require_assert": require_assert,
        "scope_hash": scope_hash,
        "regressed": sorted(regressed, key=lambda r: r["id"]),
        "fixed": sorted(fixed, key=lambda r: r["id"]),
        "changed_bad": sorted(changed_bad, key=lambda r: r["id"]),
        "changed_other": sorted(other_changed, key=lambda r: r["id"]),
        "new_cases": sorted(new_cases, key=lambda r: r["id"]),
    }


def _append_effective_diff(tag_dir: Path, diff_entry: Mapping[str, object]) -> None:
    tag_dir.mkdir(parents=True, exist_ok=True)
    changes_path = tag_dir / "effective_changes.jsonl"
    with changes_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(diff_entry, ensure_ascii=False, sort_keys=True) + "\n")


def _load_effective_diff(tag_dir: Path) -> Optional[dict]:
    path = tag_dir / "effective_changes.jsonl"
    if not path.exists():
        return None
    last: Optional[dict] = None
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                last = json.loads(line)
            except Exception:
                continue
    return last


def _update_effective_snapshot(
    *,
    artifacts_dir: Path,
    tag: str,
    cases_hash: str,
    cases_path: Path,
    suite_case_ids: list[str],
    executed_results: list[RunResult],
    run_folder: Path,
    scope: Mapping[str, object],
    scope_hash: str,
    fail_on: str,
    require_assert: bool,
) -> tuple[Path, Path, dict[str, RunResult], dict[str, RunResult]]:
    effective_results, effective_meta, effective_results_path = _load_effective_results(artifacts_dir, tag)
    if effective_meta and effective_meta.get("cases_hash") and effective_meta["cases_hash"] != cases_hash:
        raise ValueError(
            f"Existing effective results for tag {tag!r} use a different cases_hash; refusing to merge."
        )
    if effective_meta and effective_meta.get("scope_hash") and effective_meta["scope_hash"] != scope_hash:
        raise ValueError(
            f"Existing effective results for tag {tag!r} have a different scope; refusing to merge."
        )

    planned_pool: set[str]
    if effective_meta and isinstance(effective_meta.get("planned_case_ids"), list):
        planned_pool = {str(cid) for cid in effective_meta["planned_case_ids"]}
    else:
        planned_pool = set(suite_case_ids)

    before_effective = dict(effective_results)
    for res in executed_results:
        effective_results[res.id] = res
    _write_effective_results(effective_results_path, effective_results)

    summary_counts = summarize(effective_results.values())
    executed_total = len(effective_results)
    missed_total = len(_missed_case_ids(planned_pool, effective_results))
    meta_path = effective_results_path.with_name("effective_meta.json")
    built_from = set(effective_meta.get("built_from_runs", [])) if effective_meta else set()
    built_from.add(str(run_folder))
    effective_meta_payload = {
        "tag": tag,
        "cases_hash": cases_hash,
        "cases_path": str(cases_path),
        "planned_case_ids": sorted(planned_pool),
        "planned_total": len(planned_pool),
        "executed_total": executed_total,
        "missed_total": missed_total,
        "counts": summary_counts,
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        "built_from_runs": sorted(built_from),
        "effective_results_path": str(effective_results_path),
        "scope": scope,
        "scope_hash": scope_hash,
        "fail_on": fail_on,
        "require_assert": require_assert,
    }
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    dump_json(meta_path, effective_meta_payload)
    return effective_results_path, meta_path, before_effective, effective_results


__all__ = [
    "_append_effective_diff",
    "_build_effective_diff",
    "_load_effective_results",
    "_load_effective_diff",
    "_update_effective_snapshot",
    "_write_effective_results",
]
