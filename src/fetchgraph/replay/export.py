from __future__ import annotations

import copy
import filecmp
import hashlib
import json
import logging
import shutil
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable

logger = logging.getLogger(__name__)


def iter_events(path: Path, *, allow_bad_json: bool = False) -> Iterable[tuple[int, dict]]:
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield idx, json.loads(line)
            except json.JSONDecodeError as exc:
                if allow_bad_json:
                    continue
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


def resolve_requires(
    requires: list[dict] | list[str],
    *,
    resources: dict[str, dict],
    extras: dict[str, dict],
    events_path: Path,
) -> tuple[dict[str, dict], dict[str, dict]]:
    if not requires:
        return {}, {}

    normalized_requires: list[dict]
    if isinstance(requires, list) and all(isinstance(req, str) for req in requires):
        normalized_requires = []
        for req_id in requires:
            if req_id in resources:
                normalized_requires.append({"kind": "resource", "id": req_id})
            elif req_id in extras:
                normalized_requires.append({"kind": "extra", "id": req_id})
            else:
                raise ValueError(
                    f"Unknown dependency {req_id!r} in {events_path}; requires must be updated to replay_case v2."
                )
    elif isinstance(requires, list):
        normalized_requires = []
        for req in requires:
            if not isinstance(req, dict):
                raise ValueError("requires must be a list of objects with kind/id fields")
            if req.get("kind") not in {"extra", "resource"}:
                raise ValueError("requires entries must include kind='extra' or kind='resource'")
            if not isinstance(req.get("id"), str) or not req.get("id"):
                raise ValueError("requires entries must include a non-empty id")
            normalized_requires.append({"kind": req["kind"], "id": req["id"]})
    else:
        raise ValueError("requires must be a list of objects with kind/id fields")

    resolved_resources: Dict[str, dict] = {}
    resolved_extras: Dict[str, dict] = {}
    for req in normalized_requires:
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


def collect_requires(
    events_path: Path,
    requires: list[dict] | list[str],
    *,
    allow_bad_json: bool = False,
) -> tuple[dict[str, dict], dict[str, dict]]:
    resources, extras = index_requires(events_path, allow_bad_json=allow_bad_json)
    if not requires:
        return resources, extras
    return resolve_requires(requires, resources=resources, extras=extras, events_path=events_path)


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
        raise LookupError(f"No replay_case id={replay_id!r} found in {events_path}{detail_str}")
    if len(selections) > 1:
        details = "\n".join(
            f"- line {sel.line} run_id={sel.event.get('run_id')!r} timestamp={sel.event.get('timestamp')!r}"
            for sel in selections[:5]
        )
        raise LookupError(
            "Multiple replay_case entries matched; use export_replay_case_bundles to export all.\n"
            f"{details}"
        )

    selection = selections[0]
    root_event = selection.event
    requires = root_event.get("requires") or []

    resources_index, extras_index = index_requires(events_path, allow_bad_json=allow_bad_json)
    resources, extras = resolve_requires(
        requires,
        resources=resources_index,
        extras=extras_index,
        events_path=events_path,
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
