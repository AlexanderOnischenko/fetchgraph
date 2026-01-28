from __future__ import annotations

import copy
import collections
import filecmp
import hashlib
import json
import logging
import shutil
import textwrap
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable

logger = logging.getLogger(__name__)


def iter_events(path: Path, *, allow_bad_json: bool = False) -> Iterable[tuple[int, dict]]:
    skipped_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield idx, json.loads(line)
            except json.JSONDecodeError as exc:
                excerpt = line.replace("\t", " ").replace("\n", " ")
                if len(excerpt) > 120:
                    excerpt = f"{excerpt[:120]}…"
                if allow_bad_json:
                    skipped_count += 1
                    continue
                raise ValueError(f"Invalid JSON on line {idx}: {excerpt} in {path}") from exc
    if skipped_count:
        logger.warning("Skipped %d invalid JSON lines in %s", skipped_count, path)


def canonical_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def case_bundle_name(event_id: str, input_payload: dict) -> str:
    digest = hashlib.sha256((event_id + canonical_json(input_payload)).encode("utf-8")).hexdigest()
    return f"{event_id}__{digest[:8]}.case.json"


@dataclass(frozen=True)
class ExportSelection:
    line: int
    event: dict


def _match_meta(event: dict, *, spec_idx: int | None, provider: str | None) -> bool:
    if spec_idx is None and provider is None:
        return True
    meta = event.get("meta")
    if not isinstance(meta, dict):
        return False
    if spec_idx is not None and meta.get("spec_idx") != spec_idx:
        return False
    if provider is not None:
        p = meta.get("provider")
        if not isinstance(p, str):
            return False
        if p.lower() != provider.lower():
            return False
    return True


def _select_replay_cases(
    events_path: Path,
    *,
    replay_id: str,
    spec_idx: int | None = None,
    provider: str | None = None,
    allow_bad_json: bool = False,
) -> list[ExportSelection]:
    selected: list[ExportSelection] = []
    for line, event in iter_events(events_path, allow_bad_json=allow_bad_json):
        if event.get("type") != "replay_case":
            continue
        if event.get("id") != replay_id:
            continue
        if _match_meta(event, spec_idx=spec_idx, provider=provider):
            selected.append(ExportSelection(line=line, event=event))
    return selected


def find_replay_case_matches(
    events_path: Path,
    *,
    replay_id: str,
    spec_idx: int | None = None,
    provider: str | None = None,
    allow_bad_json: bool = False,
) -> list[ExportSelection]:
    return _select_replay_cases(
        events_path,
        replay_id=replay_id,
        spec_idx=spec_idx,
        provider=provider,
        allow_bad_json=allow_bad_json,
    )


def format_replay_case_matches(selections: list[ExportSelection], *, limit: int | None = 10) -> str:
    rows = []
    for idx, selection in enumerate(selections[:limit], start=1):
        event = selection.event
        meta = event.get("meta") or {}
        provider = meta.get("provider")
        spec_idx = meta.get("spec_idx")
        timestamp = event.get("timestamp")
        input_payload = event.get("input") if isinstance(event.get("input"), dict) else {}
        fingerprint = hashlib.sha256(canonical_json(input_payload).encode("utf-8")).hexdigest()[:8]
        preview = canonical_json(input_payload)[:80]
        rows.append(
            "  "
            f"{idx}. line={selection.line} "
            f"ts={timestamp!r} "
            f"provider={provider!r} "
            f"spec_idx={spec_idx!r} "
            f"input={fingerprint} "
            f"preview={preview!r}"
        )
    if len(selections) > (limit or 0):
        rows.append(f"  ... ({len(selections) - (limit or 0)} more)")
    return "\n".join(rows)


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    cleaned = value
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def _select_by_timestamp(selections: list[ExportSelection]) -> ExportSelection:
    with_ts = []
    for selection in selections:
        ts = _parse_timestamp(selection.event.get("timestamp"))
        if ts is not None:
            with_ts.append((ts, selection))
    if with_ts:
        return max(with_ts, key=lambda pair: pair[0])[1]
    return max(selections, key=lambda sel: sel.line)


def _select_replay_case(
    selections: list[ExportSelection],
    *,
    events_path: Path,
    selection_policy: str,
    select_index: int | None,
    require_unique: bool,
    allow_prompt: bool,
    prompt_fn: Callable[[str], str] | None,
) -> tuple[ExportSelection, str]:
    if not selections:
        raise LookupError(f"No replay_case entries found in {events_path}")
    if require_unique and len(selections) > 1:
        details = format_replay_case_matches(selections)
        raise LookupError(
            "Multiple replay_case entries matched; run with --select/--select-index/--list-matches.\n"
            f"{details}"
        )
    if len(selections) == 1:
        return selections[0], "unique"
    if select_index is not None:
        if select_index < 1 or select_index > len(selections):
            raise ValueError(f"select_index must be between 1 and {len(selections)}")
        return selections[select_index - 1], "index"

    selection_policy = selection_policy or "latest"
    if allow_prompt and prompt_fn is not None:
        details = format_replay_case_matches(selections)
        prompt = (
            "Multiple replay_case entries matched:\n"
            f"{details}\n"
            "Select entry (1..N) or press Enter for latest: "
        )
        response = prompt_fn(prompt).strip()
        if response:
            if response.isdigit():
                choice = int(response)
                if 1 <= choice <= len(selections):
                    return selections[choice - 1], "prompt"
                raise ValueError(f"Selection must be between 1 and {len(selections)}")
            raise ValueError("Selection must be a number")

    if selection_policy == "latest":
        return _select_by_timestamp(selections), "policy"
    if selection_policy == "by-timestamp":
        return _select_by_timestamp(selections), "policy"
    if selection_policy == "by-line":
        return max(selections, key=lambda sel: sel.line), "policy"
    if selection_policy == "last":
        return selections[-1], "policy"
    if selection_policy == "first":
        return selections[0], "policy"
    raise ValueError(f"Unsupported selection policy: {selection_policy}")


def index_requires(
    events_path: Path,
    *,
    allow_bad_json: bool = False,
) -> tuple[dict[str, dict], dict[str, dict]]:
    extras: Dict[str, dict] = {}
    resources: Dict[str, dict] = {}

    for _, event in iter_events(events_path, allow_bad_json=allow_bad_json):
        event_type = event.get("type")
        event_id = event.get("id")
        if event_type == "planner_input" and isinstance(event_id, str):
            extras[event_id] = event
        elif event_type == "replay_resource" and isinstance(event_id, str):
            resources[event_id] = event

    return resources, extras


def _format_replay_case_ref(replay_case: dict | None) -> str:
    if not isinstance(replay_case, dict):
        return ""
    replay_case_dict: dict = replay_case
    replay_id = replay_case_dict.get("id")
    meta_value = replay_case_dict.get("meta")
    meta = meta_value if isinstance(meta_value, dict) else {}
    provider = meta.get("provider")
    spec_idx = meta.get("spec_idx")
    details = []
    if isinstance(replay_id, str):
        details.append(f"id={replay_id!r}")
    if provider is not None:
        details.append(f"provider={provider!r}")
    if spec_idx is not None:
        details.append(f"spec_idx={spec_idx!r}")
    if not details:
        return ""
    return " (replay_case " + ", ".join(details) + ")"


def _normalize_requires(
    requires: list[dict] | list[str],
    *,
    resources: dict[str, dict],
    extras: dict[str, dict],
    events_path: Path,
) -> list[dict]:
    normalized_requires: list[dict] = []
    if isinstance(requires, list) and all(isinstance(req, str) for req in requires):
        for req_id in requires:
            if req_id in resources:
                normalized_requires.append({"kind": "resource", "id": req_id})
            elif req_id in extras:
                normalized_requires.append({"kind": "extra", "id": req_id})
            else:
                raise ValueError(
                    f"Unknown dependency {req_id!r} in {events_path}; requires must be updated to replay_case v2."
                )
        return normalized_requires
    if isinstance(requires, list):
        for req in requires:
            if not isinstance(req, dict):
                raise ValueError("requires must be a list of objects with kind/id fields")
            if req.get("kind") not in {"extra", "resource"}:
                raise ValueError("requires entries must include kind='extra' or kind='resource'")
            if not isinstance(req.get("id"), str) or not req.get("id"):
                raise ValueError("requires entries must include a non-empty id")
            normalized_requires.append({"kind": req["kind"], "id": req["id"]})
        return normalized_requires
    raise ValueError("requires must be a list of objects with kind/id fields")


def _extract_extra_requires(
    extra: dict,
    *,
    resources: dict[str, dict],
    extras: dict[str, dict],
    events_path: Path,
) -> list[dict]:
    requirements: list[dict] = []
    extra_requires = extra.get("requires")
    if isinstance(extra_requires, list) and extra_requires:
        requirements.extend(
            _normalize_requires(
                extra_requires,
                resources=resources,
                extras=extras,
                events_path=events_path,
            )
        )
    schema_ref = extra.get("schema_ref")
    if isinstance(schema_ref, str) and schema_ref:
        requirements.append({"kind": "resource", "id": schema_ref})
    input_value = extra.get("input")
    extra_input = input_value if isinstance(input_value, dict) else {}
    schema_ref = extra_input.get("schema_ref")
    if isinstance(schema_ref, str) and schema_ref:
        requirements.append({"kind": "resource", "id": schema_ref})
    input_requires = extra_input.get("requires")
    if isinstance(input_requires, list) and input_requires:
        requirements.extend(
            _normalize_requires(
                input_requires,
                resources=resources,
                extras=extras,
                events_path=events_path,
            )
        )
    return requirements


def resolve_requires(
    requires: list[dict] | list[str],
    *,
    resources: dict[str, dict],
    extras: dict[str, dict],
    events_path: Path,
    replay_case: dict | None = None,
) -> tuple[dict[str, dict], dict[str, dict]]:
    if not requires:
        return {}, {}

    normalized_requires = _normalize_requires(
        requires,
        resources=resources,
        extras=extras,
        events_path=events_path,
    )

    resolved_resources: Dict[str, dict] = {}
    resolved_extras: Dict[str, dict] = {}
    pending = collections.deque(normalized_requires)
    seen: set[tuple[str, str]] = set()
    while pending:
        req = pending.popleft()
        kind = req.get("kind")
        rid = req.get("id")
        if not isinstance(kind, str) or not isinstance(rid, str):
            raise KeyError(f"Invalid require entry {req!r} in {events_path}")
        key = (kind, rid)
        if key in seen:
            continue
        seen.add(key)
        if kind == "extra":
            if rid in extras:
                extra = extras[rid]
                resolved_extras[rid] = extra
                if isinstance(extra, dict):
                    pending.extend(
                        _extract_extra_requires(
                            extra,
                            resources=resources,
                            extras=extras,
                            events_path=events_path,
                        )
                    )
            else:
                ref = _format_replay_case_ref(replay_case)
                if rid == "planner_input_v1":
                    raise KeyError(
                        "Missing required extra 'planner_input_v1' in "
                        f"{events_path}{ref}. "
                        "Re-record the run with planner_input emit enabled, "
                        "or update the trace to include planner_input_v1."
                    )
                raise KeyError(f"Missing required extra {rid} in {events_path}{ref}")
        elif kind == "resource":
            if rid in resources:
                resolved_resources[rid] = resources[rid]
            else:
                ref = _format_replay_case_ref(replay_case)
                raise KeyError(f"Missing required resource {rid} in {events_path}{ref}")
        else:
            raise KeyError(f"Unknown require kind {kind!r} in {events_path}")

    return resolved_resources, resolved_extras


def collect_requires(
    requires: list[dict] | list[str],
    *,
    resources: dict[str, dict],
    extras: dict[str, dict],
    events_path: Path,
) -> tuple[dict[str, dict], dict[str, dict]]:
    return resolve_requires(
        requires,
        resources=resources,
        extras=extras,
        events_path=events_path,
    )


def write_case_bundle(
    out_path: Path,
    *,
    root_case: dict,
    resources: Dict[str, dict],
    extras: Dict[str, dict],
    source: dict,
    overwrite: bool = False,
) -> None:
    payload = {
        "schema": "fetchgraph.tracer.case_bundle",
        "v": 1,
        "root": root_case,
        "resources": resources,
        "extras": extras,
        "source": source,
    }
    new_text = canonical_json(payload)
    if out_path.exists() and not overwrite:
        try:
            old_payload = json.loads(out_path.read_text(encoding="utf-8"))
            old_text = canonical_json(old_payload)
        except Exception as exc:
            raise ValueError(f"Existing case bundle is unreadable: {out_path}: {exc}") from exc
        if old_text == new_text:
            logger.info("Fixture already up-to-date: %s", out_path)
            return
        raise FileExistsError(
            textwrap.dedent(
                f"""\
                Case bundle already exists and differs: {out_path}
                This is fail-fast to avoid mixing fixtures from different runs.
                Actions:
                  - delete the file, or
                  - choose a different --out directory, or
                  - rerun with --overwrite.
                """
            ).rstrip()
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(new_text, encoding="utf-8")


def copy_resource_files(
    resources: dict[str, dict],
    *,
    run_dir: Path,
    out_dir: Path,
    fixture_stem: str,
) -> None:
    planned: dict[Path, tuple[str, Path]] = {}
    for resource_id, resource in resources.items():
        if not isinstance(resource_id, str) or not resource_id:
            raise ValueError("resource_id must be a non-empty string")
        if "/" in resource_id or "\\" in resource_id or ".." in Path(resource_id).parts:
            raise ValueError(f"resource_id must be a safe path segment: {resource_id!r}")
        data_ref = resource.get("data_ref")
        if not isinstance(data_ref, dict):
            continue
        file_name = data_ref.get("file")
        if not isinstance(file_name, str) or not file_name:
            continue
        rel_path = Path(file_name)
        if rel_path.is_absolute():
            raise ValueError(f"Resource file path must be relative: {file_name}")
        if ".." in rel_path.parts:
            raise ValueError(f"Resource file path must not traverse parents: {file_name}")
        src_path = run_dir / rel_path
        if not src_path.exists():
            raise FileNotFoundError(
                f"Missing resource file for rid={resource_id!r}: "
                f"src={src_path} (run_dir={run_dir}, fixture={fixture_stem})"
            )
        dest_rel = Path("resources") / fixture_stem / resource_id / rel_path
        dest_path = out_dir / dest_rel
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        prev = planned.get(dest_path)
        if prev is not None:
            prev_id, prev_src = prev
            if prev_id != resource_id or prev_src != src_path:
                if not filecmp.cmp(prev_src, src_path, shallow=False):
                    raise FileExistsError(
                        "Resource destination collision (different contents):\n"
                        f"  dest: {dest_path}\n"
                        f"  fixture: {fixture_stem}\n"
                        f"  A: rid={prev_id!r} src={prev_src}\n"
                        f"  B: rid={resource_id!r} src={src_path}\n"
                        "Hint: make resource filenames unique or adjust resource layout."
                    )
        else:
            planned[dest_path] = (resource_id, src_path)
        if dest_path.exists():
            if not filecmp.cmp(src_path, dest_path, shallow=False):
                raise FileExistsError(
                    "Resource file collision (dest exists with different contents):\n"
                    f"  rid: {resource_id!r}\n"
                    f"  fixture: {fixture_stem}\n"
                    f"  src: {src_path}\n"
                    f"  dest: {dest_path}\n"
                    "Actions:\n"
                    "  - delete destination resources directory, or\n"
                    "  - export into a clean --out directory."
                )
        else:
            shutil.copy2(src_path, dest_path)
        data_ref["file"] = dest_rel.as_posix()


def _has_resource_files(resources: dict[str, dict]) -> bool:
    for resource in resources.values():
        data_ref = resource.get("data_ref")
        if not isinstance(data_ref, dict):
            continue
        file_name = data_ref.get("file")
        if isinstance(file_name, str) and file_name:
            return True
    return False


def export_replay_case_bundle(
    *,
    events_path: Path,
    out_dir: Path,
    replay_id: str,
    spec_idx: int | None = None,
    provider: str | None = None,
    run_dir: Path | None = None,
    allow_bad_json: bool = False,
    overwrite: bool = False,
    selection_policy: str = "latest",
    select_index: int | None = None,
    require_unique: bool = False,
    allow_prompt: bool = False,
    prompt_fn: Callable[[str], str] | None = None,
) -> Path:
    selections = _select_replay_cases(
        events_path,
        replay_id=replay_id,
        spec_idx=spec_idx,
        provider=provider,
        allow_bad_json=allow_bad_json,
    )
    if not selections:
        details = []
        if spec_idx is not None:
            details.append(f"spec_idx={spec_idx}")
        if provider is not None:
            details.append(f"provider={provider!r}")
        detail_str = f" (filters: {', '.join(details)})" if details else ""
        unfiltered = _select_replay_cases(
            events_path,
            replay_id=replay_id,
            spec_idx=None,
            provider=None,
            allow_bad_json=allow_bad_json,
        )
        if unfiltered and details:
            providers = sorted(
                {str(sel.event.get("meta", {}).get("provider")) for sel in unfiltered if sel.event.get("meta")}
            )
            spec_idxs = sorted(
                {str(sel.event.get("meta", {}).get("spec_idx")) for sel in unfiltered if sel.event.get("meta")}
            )
            hint_lines = [
                f"No replay_case id={replay_id!r} matched filters in {events_path}{detail_str}.",
                f"Available providers: {providers}",
                f"Available spec_idx: {spec_idxs}",
                "Tip: rerun without --provider/--spec-idx or choose matching values.",
            ]
            raise LookupError("\n".join(hint_lines))
        raise LookupError(f"No replay_case id={replay_id!r} found in {events_path}{detail_str}")
    selection, selection_mode = _select_replay_case(
        selections,
        events_path=events_path,
        selection_policy=selection_policy,
        select_index=select_index,
        require_unique=require_unique,
        allow_prompt=allow_prompt,
        prompt_fn=prompt_fn,
    )
    if len(selections) > 1 and selection_mode == "policy":
        input_hashes = {
            hashlib.sha256(canonical_json(sel.event.get("input") or {}).encode("utf-8")).hexdigest()
            for sel in selections
        }
        details = format_replay_case_matches(selections, limit=5)
        suffix = "Candidates differ by input payload." if len(input_hashes) > 1 else "Candidates share input."
        logger.warning(
            "Multiple replay_case entries matched; selection policy=%s chose line %s.\n%s\n%s",
            selection_policy,
            selection.line,
            details,
            suffix,
        )
    root_event = selection.event
    requires = root_event.get("requires") or []

    resources_index, extras_index = index_requires(events_path, allow_bad_json=allow_bad_json)
    resources, extras = resolve_requires(
        requires,
        resources=resources_index,
        extras=extras_index,
        events_path=events_path,
        replay_case=root_event,
    )
    resources = copy.deepcopy(resources)
    extras = copy.deepcopy(extras)
    fixture_name = case_bundle_name(replay_id, root_event["input"])
    fixture_stem = fixture_name.replace(".case.json", "")
    if _has_resource_files(resources):
        if run_dir is None:
            raise ValueError("run_dir is required to export file resources")
        if overwrite:
            shutil.rmtree(out_dir / "resources" / fixture_stem, ignore_errors=True)
        copy_resource_files(
            resources,
            run_dir=run_dir,
            out_dir=out_dir,
            fixture_stem=fixture_stem,
        )

    source = {
        "events_path": str(events_path),
        "line": selection.line,
        "run_id": root_event.get("run_id"),
        "timestamp": root_event.get("timestamp"),
        "case_id": root_event.get("case_id"),
    }
    out_path = out_dir / fixture_name
    write_case_bundle(
        out_path,
        root_case=root_event,
        resources=resources,
        extras=extras,
        source=source,
        overwrite=overwrite,
    )
    return out_path


def export_replay_case_bundles(
    *,
    events_path: Path,
    out_dir: Path,
    replay_id: str,
    spec_idx: int | None = None,
    provider: str | None = None,
    run_dir: Path | None = None,
    allow_bad_json: bool = False,
    overwrite: bool = False,
) -> list[Path]:
    selections = _select_replay_cases(
        events_path,
        replay_id=replay_id,
        spec_idx=spec_idx,
        provider=provider,
        allow_bad_json=allow_bad_json,
    )
    if not selections:
        details = []
        if spec_idx is not None:
            details.append(f"spec_idx={spec_idx}")
        if provider is not None:
            details.append(f"provider={provider!r}")
        detail_str = f" (filters: {', '.join(details)})" if details else ""
        raise LookupError(f"No replay_case id={replay_id!r} found in {events_path}{detail_str}")

    resources_index, extras_index = index_requires(events_path, allow_bad_json=allow_bad_json)
    paths: list[Path] = []
    for selection in selections:
        root_event = selection.event
        requires = root_event.get("requires") or []
        resources, extras = resolve_requires(
            requires,
            resources=resources_index,
            extras=extras_index,
            events_path=events_path,
            replay_case=root_event,
        )
        resources = copy.deepcopy(resources)
        extras = copy.deepcopy(extras)
        fixture_name = case_bundle_name(replay_id, root_event["input"])
        fixture_stem = fixture_name.replace(".case.json", "")
        if _has_resource_files(resources):
            if run_dir is None:
                raise ValueError("run_dir is required to export file resources")
            if overwrite:
                shutil.rmtree(out_dir / "resources" / fixture_stem, ignore_errors=True)
            copy_resource_files(
                resources,
                run_dir=run_dir,
                out_dir=out_dir,
                fixture_stem=fixture_stem,
            )

        source = {
            "events_path": str(events_path),
            "line": selection.line,
            "run_id": root_event.get("run_id"),
            "timestamp": root_event.get("timestamp"),
            "case_id": root_event.get("case_id"),
        }
        out_path = out_dir / fixture_name
        write_case_bundle(
            out_path,
            root_case=root_event,
            resources=resources,
            extras=extras,
            source=source,
            overwrite=overwrite,
        )
        paths.append(out_path)
    return paths
