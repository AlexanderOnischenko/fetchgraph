from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable


def iter_events(path: Path) -> Iterable[tuple[int, dict]]:
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle):
            line = line.strip()
            if not line:
                continue
            try:
                yield idx, json.loads(line)
            except json.JSONDecodeError:
                continue


def canonical_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fixture_name(event_id: str, input_payload: dict) -> str:
    digest = hashlib.sha256((event_id + canonical_json(input_payload)).encode("utf-8")).hexdigest()
    return f"{event_id}__{digest[:8]}.json"


def _resource_target_path(
    *,
    fixture_stem: str,
    file_name: str,
    resource_key: str,
    used_paths: set[Path],
) -> Path:
    rel_path = Path(file_name)
    if rel_path.is_absolute():
        raise SystemExit(f"Resource file path must be relative: {file_name}")
    if ".." in rel_path.parts:
        raise SystemExit(f"Resource file path must not traverse parents: {file_name}")
    base_dir = Path("resources") / fixture_stem
    if rel_path.parent != Path("."):
        dest_rel = base_dir / rel_path
    else:
        dest_rel = base_dir / rel_path.name
    if dest_rel in used_paths:
        prefix = resource_key or "resource"
        dest_rel = base_dir / f"{prefix}__{rel_path.name}"
        suffix = 1
        while dest_rel in used_paths:
            dest_rel = base_dir / f"{prefix}_{suffix}__{rel_path.name}"
            suffix += 1
    used_paths.add(dest_rel)
    return dest_rel


def _copy_resource_file(
    run_dir: Path,
    out_dir: Path,
    fixture_stem: str,
    resource_key: str,
    used_paths: set[Path],
    data_ref: dict,
) -> dict:
    file_name = data_ref.get("file")
    if not file_name:
        return data_ref
    src_path = run_dir / file_name
    if not src_path.exists():
        raise SystemExit(f"Resource file {src_path} not found for replay bundle")
    dest_rel = _resource_target_path(
        fixture_stem=fixture_stem,
        file_name=file_name,
        resource_key=resource_key,
        used_paths=used_paths,
    )
    dest_path = out_dir / dest_rel
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, dest_path)
    updated = dict(data_ref)
    updated["file"] = dest_rel.as_posix()
    return updated


@dataclass(frozen=True)
class ExportSelection:
    event_index: int
    event: dict


@dataclass(frozen=True)
class ExportContext:
    resources: Dict[str, dict]
    extras: Dict[str, dict]


def _match_meta(event: dict, *, spec_idx: int | None, provider: str | None) -> bool:
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


def select_replay_points(
    events_path: Path,
    *,
    replay_id: str,
    spec_idx: int | None = None,
    provider: str | None = None,
) -> tuple[list[ExportSelection], ExportContext]:
    selected: list[ExportSelection] = []
    resources: Dict[str, dict] = {}
    extras: Dict[str, dict] = {}

    for idx, event in iter_events(events_path):
        etype = event.get("type")
        eid = event.get("id")

        if etype == "replay_resource" and isinstance(eid, str):
            resources[eid] = event
            continue

        if etype == "planner_input" and isinstance(eid, str):
            extras[eid] = event
            continue

        if etype == "replay_point" and eid == replay_id:
            if _match_meta(event, spec_idx=spec_idx, provider=provider):
                selected.append(ExportSelection(event_index=idx, event=event))

    if not selected:
        details = []
        if spec_idx is not None:
            details.append(f"spec_idx={spec_idx}")
        if provider is not None:
            details.append(f"provider={provider!r}")
        detail_str = f" (filters: {', '.join(details)})" if details else ""
        raise SystemExit(f"No replay_point id={replay_id!r} found in {events_path}{detail_str}")
    return selected, ExportContext(resources=resources, extras=extras)


def write_fixture(event: dict, *, out_dir: Path, source: dict) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    event_id = event["id"]
    out_path = out_dir / fixture_name(event_id, event["input"])
    if out_path.exists():
        print(f"Fixture already exists: {out_path}")
        return out_path

    payload = dict(event)
    payload["source"] = source
    out_path.write_text(canonical_json(payload), encoding="utf-8")
    print(f"Wrote fixture: {out_path}")
    return out_path


def write_bundle(
    root_event: dict,
    *,
    out_dir: Path,
    run_dir: Path,
    resources: Dict[str, dict],
    extras: Dict[str, dict],
    source: dict,
    root_source: dict,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    fixture_file = fixture_name(root_event["id"], root_event["input"])
    out_path = out_dir / fixture_file
    if out_path.exists():
        print(f"Fixture already exists: {out_path}")
        return out_path

    root_name = Path(fixture_file).stem
    updated_resources: Dict[str, dict] = {}
    used_paths: set[Path] = set()
    for rid, resource in resources.items():
        rp = dict(resource)
        if "data_ref" in rp and isinstance(rp["data_ref"], dict):
            rp["data_ref"] = _copy_resource_file(
                run_dir,
                out_dir,
                root_name,
                rid,
                used_paths,
                rp["data_ref"],
            )
        updated_resources[rid] = rp

    payload = {
        "type": "replay_bundle",
        "v": 1,
        "root": dict(root_event),
        "resources": updated_resources,
        "extras": dict(extras),
        "source": dict(source),
    }
    payload["root"]["source"] = dict(root_source)

    out_path.write_text(canonical_json(payload), encoding="utf-8")
    print(f"Wrote fixture bundle: {out_path}")
    return out_path


def export_replay_fixture(
    *,
    events_path: Path,
    out_dir: Path,
    replay_id: str,
    spec_idx: int | None = None,
    provider: str | None = None,
    with_requires: bool = False,
    run_dir: Path | None = None,
    source_extra: dict | None = None,
    allow_multiple: bool = False,
) -> Path:
    selections, ctx = select_replay_points(
        events_path,
        replay_id=replay_id,
        spec_idx=spec_idx,
        provider=provider,
    )
    if len(selections) > 1 and not allow_multiple:
        raise SystemExit(
            "Multiple replay points matched. Provide --spec-idx/--provider or use --all to export all."
        )
    if len(selections) > 1 and allow_multiple:
        raise SystemExit("Use export_replay_fixtures for multiple selections.")
    selection = selections[0]
    event = selection.event

    common_source = {
        "events_path": str(events_path),
        "event_index": selection.event_index,
        **(source_extra or {}),
    }

    if not with_requires:
        if event.get("requires"):
            print("Warning: replay_point has requires; export without --with-requires may be incomplete.")
        return write_fixture(event, out_dir=out_dir, source=common_source)

    requires = event.get("requires") or []
    resolved_resources: Dict[str, dict] = {}
    resolved_extras: Dict[str, dict] = {}

    for rid in requires:
        if rid in ctx.resources:
            resolved_resources[rid] = ctx.resources[rid]
            continue
        if rid in ctx.extras:
            resolved_extras[rid] = ctx.extras[rid]
            continue
        raise SystemExit(f"Required dependency {rid!r} not found in {events_path}")

    if run_dir is None:
        raise SystemExit("--run-dir is required for --with-requires (to copy resource files)")

    root_source = {
        "events_path": str(events_path),
        "event_index": selection.event_index,
        "run_dir": str(run_dir),
        **(source_extra or {}),
    }

    return write_bundle(
        event,
        out_dir=out_dir,
        run_dir=run_dir,
        resources=resolved_resources,
        extras=resolved_extras,
        source=common_source,
        root_source=root_source,
    )


def export_replay_fixtures(
    *,
    events_path: Path,
    out_dir: Path,
    replay_id: str,
    spec_idx: int | None = None,
    provider: str | None = None,
    with_requires: bool = False,
    run_dir: Path | None = None,
    source_extra: dict | None = None,
) -> list[Path]:
    selections, ctx = select_replay_points(
        events_path,
        replay_id=replay_id,
        spec_idx=spec_idx,
        provider=provider,
    )
    paths: list[Path] = []
    for selection in selections:
        event = selection.event
        common_source = {
            "events_path": str(events_path),
            "event_index": selection.event_index,
            **(source_extra or {}),
        }
        if not with_requires:
            if event.get("requires"):
                print("Warning: replay_point has requires; export without --with-requires may be incomplete.")
            paths.append(write_fixture(event, out_dir=out_dir, source=common_source))
            continue

        requires = event.get("requires") or []
        resolved_resources: Dict[str, dict] = {}
        resolved_extras: Dict[str, dict] = {}
        for rid in requires:
            if rid in ctx.resources:
                resolved_resources[rid] = ctx.resources[rid]
                continue
            if rid in ctx.extras:
                resolved_extras[rid] = ctx.extras[rid]
                continue
            raise SystemExit(f"Required dependency {rid!r} not found in {events_path}")

        if run_dir is None:
            raise SystemExit("--run-dir is required for --with-requires (to copy resource files)")

        root_source = {
            "events_path": str(events_path),
            "event_index": selection.event_index,
            "run_dir": str(run_dir),
            **(source_extra or {}),
        }
        paths.append(
            write_bundle(
                event,
                out_dir=out_dir,
                run_dir=run_dir,
                resources=resolved_resources,
                extras=resolved_extras,
                source=common_source,
                root_source=root_source,
            )
        )
    return paths
