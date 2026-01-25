from __future__ import annotations

import filecmp
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .runtime import load_case_bundle, run_case


@dataclass(frozen=True)
class FixturePaths:
    case_path: Path
    expected_path: Path
    resources_dir: Path
    stem: str
    bucket: str


def _fixture_paths(root: Path, bucket: str, stem: str) -> FixturePaths:
    case_path = root / bucket / f"{stem}.case.json"
    expected_path = root / bucket / f"{stem}.expected.json"
    resources_dir = root / bucket / "resources" / stem
    return FixturePaths(case_path, expected_path, resources_dir, stem, bucket)


def _iter_case_paths(root: Path, bucket: str) -> Iterable[Path]:
    bucket_dir = root / bucket
    if not bucket_dir.exists():
        return []
    return sorted(bucket_dir.glob("*.case.json"))


def _validate_bundle_schema(payload: dict, *, path: Path) -> None:
    if payload.get("schema") != "fetchgraph.tracer.case_bundle" or payload.get("v") != 1:
        raise ValueError(f"Unsupported case bundle schema in {path}")


def _safe_resource_path(path: str, *, stem: str) -> Path:
    rel = Path(path)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"Invalid resource path in bundle {stem}: {path}")
    return rel


def fixture_green(
    *,
    case_path: Path,
    out_root: Path,
    validate: bool = False,
    overwrite_expected: bool = False,
    dry_run: bool = False,
) -> None:
    case_path = case_path.resolve()
    out_root = out_root.resolve()
    if out_root not in case_path.parents:
        raise ValueError(f"Case path must be under {out_root}")
    if "known_bad" not in case_path.parts:
        raise ValueError("fixture-green expects a case under known_bad")
    stem = case_path.stem.replace(".case", "")
    known_paths = _fixture_paths(out_root, "known_bad", stem)
    fixed_paths = _fixture_paths(out_root, "fixed", stem)

    payload = json.loads(case_path.read_text(encoding="utf-8"))
    _validate_bundle_schema(payload, path=case_path)
    root = payload.get("root") or {}
    observed = root.get("observed")
    if not isinstance(observed, dict):
        raise ValueError(f"Bundle missing observed payload: {case_path}")

    if fixed_paths.case_path.exists() and not dry_run:
        raise FileExistsError(f"Target case already exists: {fixed_paths.case_path}")
    if fixed_paths.expected_path.exists() and not overwrite_expected and not dry_run:
        raise FileExistsError(f"Expected already exists: {fixed_paths.expected_path}")

    resources = payload.get("resources") or {}
    resource_paths = []
    if isinstance(resources, dict):
        for resource in resources.values():
            if not isinstance(resource, dict):
                continue
            data_ref = resource.get("data_ref")
            if not isinstance(data_ref, dict):
                continue
            file_name = data_ref.get("file")
            if not isinstance(file_name, str) or not file_name:
                continue
            rel = _safe_resource_path(file_name, stem=stem)
            resource_paths.append(rel)

    if resource_paths:
        src_dir = known_paths.resources_dir
        dst_dir = fixed_paths.resources_dir
        if dst_dir.exists() and not dry_run:
            raise FileExistsError(f"Target resources already exist: {dst_dir}")

    if dry_run:
        print(f"Would write expected: {fixed_paths.expected_path}")
        print(f"Would move case: {known_paths.case_path} -> {fixed_paths.case_path}")
        if resource_paths:
            print(f"Would move resources: {known_paths.resources_dir} -> {fixed_paths.resources_dir}")
        if validate:
            print("Would validate replay output against expected")
        return

    fixed_paths.expected_path.parent.mkdir(parents=True, exist_ok=True)
    fixed_paths.expected_path.write_text(
        json.dumps(observed, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    fixed_paths.case_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(known_paths.case_path), str(fixed_paths.case_path))

    if resource_paths:
        fixed_paths.resources_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(known_paths.resources_dir), str(fixed_paths.resources_dir))

    if validate:
        import fetchgraph.tracer.handlers  # noqa: F401

        root_case, ctx = load_case_bundle(fixed_paths.case_path)
        out = run_case(root_case, ctx)
        expected = json.loads(fixed_paths.expected_path.read_text(encoding="utf-8"))
        if out != expected:
            raise AssertionError("Replay output does not match expected after fixture-green")


def fixture_rm(
    *,
    root: Path,
    name: str | None,
    pattern: str | None,
    bucket: str,
    scope: str,
    dry_run: bool,
) -> None:
    root = root.resolve()
    buckets = ["fixed", "known_bad"] if bucket == "all" else [bucket]
    matched = []
    for b in buckets:
        for case_path in _iter_case_paths(root, b):
            stem = case_path.stem.replace(".case", "")
            if name and stem != name:
                continue
            if pattern and not case_path.match(pattern):
                continue
            matched.append(_fixture_paths(root, b, stem))

    if name and not matched:
        raise FileNotFoundError(f"No fixtures found for name={name!r}")

    targets: list[Path] = []
    for paths in matched:
        if scope in ("cases", "both"):
            targets.extend([paths.case_path, paths.expected_path])
        if scope in ("resources", "both"):
            targets.append(paths.resources_dir)

    if dry_run:
        for target in targets:
            print(f"Would remove: {target}")
        return

    for target in targets:
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=True)
        elif target.exists():
            target.unlink()


def fixture_fix(
    *,
    root: Path,
    name: str,
    new_name: str,
    bucket: str,
    dry_run: bool,
) -> None:
    root = root.resolve()
    paths = _fixture_paths(root, bucket, name)
    new_paths = _fixture_paths(root, bucket, new_name)

    if not paths.case_path.exists():
        raise FileNotFoundError(f"Missing case bundle: {paths.case_path}")
    if new_paths.case_path.exists():
        raise FileExistsError(f"Target case already exists: {new_paths.case_path}")

    payload = json.loads(paths.case_path.read_text(encoding="utf-8"))
    _validate_bundle_schema(payload, path=paths.case_path)
    resources = payload.get("resources") or {}
    updated = False
    if isinstance(resources, dict):
        for resource in resources.values():
            if not isinstance(resource, dict):
                continue
            data_ref = resource.get("data_ref")
            if not isinstance(data_ref, dict):
                continue
            file_name = data_ref.get("file")
            if not isinstance(file_name, str) or not file_name:
                continue
            rel = _safe_resource_path(file_name, stem=name)
            old_prefix = Path("resources") / name
            if rel.parts[:2] == old_prefix.parts[:2]:
                new_rel = Path("resources") / new_name / Path(*rel.parts[2:])
                data_ref["file"] = new_rel.as_posix()
                updated = True

    if dry_run:
        print(f"Would rename case: {paths.case_path} -> {new_paths.case_path}")
        if paths.expected_path.exists():
            print(f"Would rename expected: {paths.expected_path} -> {new_paths.expected_path}")
        if paths.resources_dir.exists():
            print(f"Would rename resources: {paths.resources_dir} -> {new_paths.resources_dir}")
        if updated:
            print("Would update data_ref.file paths inside bundle")
        return

    new_paths.case_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(paths.case_path), str(new_paths.case_path))
    if paths.expected_path.exists():
        shutil.move(str(paths.expected_path), str(new_paths.expected_path))
    if paths.resources_dir.exists():
        if new_paths.resources_dir.exists():
            raise FileExistsError(f"Target resources already exist: {new_paths.resources_dir}")
        shutil.move(str(paths.resources_dir), str(new_paths.resources_dir))
    if updated:
        new_paths.case_path.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
            encoding="utf-8",
        )


def fixture_migrate(*, root: Path, dry_run: bool) -> None:
    root = root.resolve()
    for bucket in ("fixed", "known_bad"):
        for case_path in _iter_case_paths(root, bucket):
            stem = case_path.stem.replace(".case", "")
            payload = json.loads(case_path.read_text(encoding="utf-8"))
            _validate_bundle_schema(payload, path=case_path)
            resources = payload.get("resources") or {}
            if not isinstance(resources, dict):
                continue
            updated = False
            for resource in resources.values():
                if not isinstance(resource, dict):
                    continue
                data_ref = resource.get("data_ref")
                if not isinstance(data_ref, dict):
                    continue
                file_name = data_ref.get("file")
                if not isinstance(file_name, str) or not file_name:
                    continue
                rel = _safe_resource_path(file_name, stem=stem)
                target_rel = Path("resources") / stem / rel
                if rel == target_rel:
                    continue
                src_path = case_path.parent / rel
                if not src_path.exists():
                    raise FileNotFoundError(f"Missing resource file: {src_path}")
                dest_path = case_path.parent / target_rel
                if dest_path.exists() and not filecmp.cmp(src_path, dest_path, shallow=False):
                    raise FileExistsError(f"Resource collision at {dest_path}")
                if dry_run:
                    print(f"Would move {src_path} -> {dest_path}")
                else:
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    if not dest_path.exists():
                        shutil.move(str(src_path), str(dest_path))
                data_ref["file"] = target_rel.as_posix()
                updated = True
            if updated and not dry_run:
                case_path.write_text(
                    json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
                    encoding="utf-8",
                )
