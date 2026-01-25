from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable


def iter_events(path: Path) -> Iterable[tuple[int, dict]]:
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield idx, json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {idx} in {path}: {exc.msg}") from exc


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
) -> list[ExportSelection]:
    selected: list[ExportSelection] = []
    for line, event in iter_events(events_path):
        if event.get("type") != "replay_case":
            continue
        if event.get("id") != replay_id:
            continue
        if _match_meta(event, spec_idx=spec_idx, provider=provider):
            selected.append(ExportSelection(line=line, event=event))
    return selected


def collect_requires(
    events_path: Path,
    requires: list[dict],
) -> tuple[dict[str, dict], dict[str, dict]]:
    extras: Dict[str, dict] = {}
    resources: Dict[str, dict] = {}

    if not requires:
        return resources, extras

    for _, event in iter_events(events_path):
        event_type = event.get("type")
        event_id = event.get("id")
        if event_type == "planner_input" and isinstance(event_id, str):
            extras[event_id] = event
        elif event_type == "replay_resource" and isinstance(event_id, str):
            resources[event_id] = event

    resolved_resources: Dict[str, dict] = {}
    resolved_extras: Dict[str, dict] = {}
    for req in requires:
        kind = req.get("kind")
        rid = req.get("id")
        if kind == "extra":
            if rid in extras:
                resolved_extras[rid] = extras[rid]
            else:
                raise KeyError(f"Missing required extra {rid} in {events_path}")
        elif kind == "resource":
            if rid in resources:
                resolved_resources[rid] = resources[rid]
            else:
                raise KeyError(f"Missing required resource {rid} in {events_path}")
        else:
            raise KeyError(f"Unknown require kind {kind!r} in {events_path}")

    return resolved_resources, resolved_extras


def write_case_bundle(
    out_path: Path,
    *,
    root_case: dict,
    resources: Dict[str, dict],
    extras: Dict[str, dict],
    source: dict,
) -> None:
    if out_path.exists():
        print(f"Fixture already exists: {out_path}")
        return
    payload = {
        "schema": "fetchgraph.tracer.case_bundle",
        "v": 1,
        "root": root_case,
        "resources": resources,
        "extras": extras,
        "source": source,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(canonical_json(payload), encoding="utf-8")


def copy_resource_files(
    resources: dict[str, dict],
    *,
    run_dir: Path,
    out_dir: Path,
    fixture_stem: str,
) -> None:
    for resource in resources.values():
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
            raise FileNotFoundError(f"Resource file {src_path} not found for replay bundle")
        dest_rel = Path("resources") / fixture_stem / rel_path
        dest_path = out_dir / dest_rel
        dest_path.parent.mkdir(parents=True, exist_ok=True)
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
) -> Path:
    selections = _select_replay_cases(
        events_path,
        replay_id=replay_id,
        spec_idx=spec_idx,
        provider=provider,
    )
    if not selections:
        details = []
        if spec_idx is not None:
            details.append(f"spec_idx={spec_idx}")
        if provider is not None:
            details.append(f"provider={provider!r}")
        detail_str = f" (filters: {', '.join(details)})" if details else ""
        raise LookupError(f"No replay_case id={replay_id!r} found in {events_path}{detail_str}")
    if len(selections) > 1:
        raise LookupError(
            "Multiple replay_case entries matched; use export_replay_case_bundles to export all."
        )

    selection = selections[0]
    root_event = selection.event
    requires = root_event.get("requires") or []

    resources, extras = collect_requires(events_path, requires)
    fixture_name = case_bundle_name(replay_id, root_event["input"])
    if _has_resource_files(resources):
        if run_dir is None:
            raise ValueError("run_dir is required to export file resources")
        copy_resource_files(
            resources,
            run_dir=run_dir,
            out_dir=out_dir,
            fixture_stem=fixture_name.replace(".case.json", ""),
        )

    source = {
        "events_path": str(events_path),
        "line": selection.line,
        "run_id": root_event.get("run_id"),
        "timestamp": root_event.get("timestamp"),
    }
    out_path = out_dir / fixture_name
    write_case_bundle(out_path, root_case=root_event, resources=resources, extras=extras, source=source)
    return out_path


def export_replay_case_bundles(
    *,
    events_path: Path,
    out_dir: Path,
    replay_id: str,
    spec_idx: int | None = None,
    provider: str | None = None,
    run_dir: Path | None = None,
) -> list[Path]:
    selections = _select_replay_cases(
        events_path,
        replay_id=replay_id,
        spec_idx=spec_idx,
        provider=provider,
    )
    if not selections:
        details = []
        if spec_idx is not None:
            details.append(f"spec_idx={spec_idx}")
        if provider is not None:
            details.append(f"provider={provider!r}")
        detail_str = f" (filters: {', '.join(details)})" if details else ""
        raise LookupError(f"No replay_case id={replay_id!r} found in {events_path}{detail_str}")

    paths: list[Path] = []
    for selection in selections:
        root_event = selection.event
        requires = root_event.get("requires") or []
        resources, extras = collect_requires(events_path, requires)
        fixture_name = case_bundle_name(replay_id, root_event["input"])
        if _has_resource_files(resources):
            if run_dir is None:
                raise ValueError("run_dir is required to export file resources")
            copy_resource_files(
                resources,
                run_dir=run_dir,
                out_dir=out_dir,
                fixture_stem=fixture_name.replace(".case.json", ""),
            )

        source = {
            "events_path": str(events_path),
            "line": selection.line,
            "run_id": root_event.get("run_id"),
            "timestamp": root_event.get("timestamp"),
        }
        out_path = out_dir / fixture_name
        write_case_bundle(out_path, root_case=root_event, resources=resources, extras=extras, source=source)
        paths.append(out_path)
    return paths
