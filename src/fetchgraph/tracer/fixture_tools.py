from __future__ import annotations

import filecmp
import json
import shutil
import subprocess
from pathlib import Path

from .fixture_layout import FixtureLayout, find_case_bundles
from .runtime import load_case_bundle, run_case


def load_bundle_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != "fetchgraph.tracer.case_bundle" or payload.get("v") != 1:
        raise ValueError(f"Not a tracer case bundle: {path}")

    root = payload.get("root")
    if not isinstance(root, dict):
        raise ValueError(f"Bundle root must be a mapping: {path}")

    root_type = root.get("type")
    root_version = root.get("v")
    if root_type is not None and root_type != "replay_case":
        raise ValueError(f"Bundle root.type must be replay_case: {path}")
    if root_version is not None and root_version != 2:
        raise ValueError(f"Bundle root.v must be 2: {path}")

    resources = payload.get("resources")
    if resources is not None and not isinstance(resources, dict):
        raise ValueError(f"Bundle resources must be a mapping: {path}")
    extras = payload.get("extras")
    if extras is not None and not isinstance(extras, dict):
        raise ValueError(f"Bundle extras must be a mapping: {path}")

    return payload


def _safe_resource_path(path: str, *, stem: str) -> Path:
    rel = Path(path)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"Invalid resource path in bundle {stem}: {path}")
    return rel


def _atomic_write_json(path: Path, payload: dict) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def _find_git_root(root: Path) -> Path | None:
    for candidate in (root, *root.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def _resolve_git_root(root: Path, mode: str) -> Path | None:
    if mode not in {"auto", "on", "off"}:
        raise ValueError(f"Unsupported git mode: {mode}")
    if mode == "off":
        return None
    git_root = _find_git_root(root)
    if git_root and shutil.which("git"):
        return git_root
    if mode == "on":
        raise ValueError("git mode is 'on' but git repository was not detected.")
    return None


def _git_relpath(path: Path, git_root: Path) -> str:
    try:
        return str(path.relative_to(git_root))
    except ValueError:
        return str(path)


def _run_git(git_root: Path, args: list[str]) -> None:
    subprocess.run(["git", "-C", str(git_root), *args], check=True)


def _move_path(src: Path, dest: Path, *, git_root: Path | None, dry_run: bool) -> None:
    if dry_run:
        if git_root:
            print(f"Would run: git -C {git_root} mv {src} {dest}")
        else:
            print(f"Would move {src} -> {dest}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    if git_root:
        _run_git(
            git_root,
            ["mv", _git_relpath(src, git_root), _git_relpath(dest, git_root)],
        )
    else:
        shutil.move(str(src), str(dest))


def _remove_path(path: Path, *, git_root: Path | None, dry_run: bool) -> None:
    if not path.exists():
        return
    if dry_run:
        if git_root:
            print(f"Would run: git -C {git_root} rm -r {path}")
        else:
            print(f"Would remove: {path}")
        return
    if git_root:
        _run_git(git_root, ["rm", "-r", _git_relpath(path, git_root)])
    else:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink()


def fixture_green(
    *,
    case_path: Path,
    out_root: Path,
    validate: bool = False,
    overwrite_expected: bool = False,
    dry_run: bool = False,
    git_mode: str = "auto",
) -> None:
    case_path = case_path.resolve()
    out_root = out_root.resolve()
    known_layout = FixtureLayout(out_root, "known_bad")
    if not case_path.is_relative_to(known_layout.bucket_dir):
        raise ValueError(f"fixture-green expects a known_bad case path, got: {case_path}")
    if not case_path.name.endswith(".case.json"):
        raise ValueError(f"fixture-green expects a .case.json bundle, got: {case_path}")
    stem = case_path.name.replace(".case.json", "")
    fixed_layout = FixtureLayout(out_root, "fixed")

    payload = load_bundle_json(case_path)
    root = payload.get("root") or {}
    observed = root.get("observed")
    if not isinstance(observed, dict):
        raise ValueError(
            "Cannot green fixture: root.observed is missing.\n"
            f"Case: {case_path}\n"
            "Hint: export observed-first replay_case bundles; green requires observed to freeze behavior."
        )

    known_case_path = known_layout.case_path(stem)
    fixed_case_path = fixed_layout.case_path(stem)
    fixed_expected_path = fixed_layout.expected_path(stem)
    known_expected_path = known_layout.expected_path(stem)
    resources_from = known_layout.resources_dir(stem)
    resources_to = fixed_layout.resources_dir(stem)

    if fixed_case_path.exists() and not dry_run:
        raise FileExistsError(f"Target case already exists: {fixed_case_path}")
    if fixed_expected_path.exists() and not overwrite_expected and not dry_run:
        raise FileExistsError(f"Expected already exists: {fixed_expected_path}")
    if resources_from.exists() and resources_to.exists() and not dry_run:
        raise FileExistsError(
            "Resources destination already exists:\n"
            f"  dest: {resources_to}\n"
            "Actions:\n"
            "  - remove destination resources directory, or\n"
            "  - run fixture-fix to rename stems, or\n"
            "  - choose a different out root."
        )

    git_root = _resolve_git_root(out_root, git_mode)
    if dry_run:
        print("fixture-green:")
        print(f"  case:   {known_case_path}")
        print(f"  move:   -> {fixed_case_path}")
        print(f"  write:  -> {fixed_expected_path} (from root.observed)")
        if resources_from.exists():
            print(f"  move:   resources -> {resources_to}")
        if git_root:
            print(f"  git:    enabled ({git_root})")
        if validate:
            print("  validate: would run")
        return

    fixed_expected_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_expected_path = fixed_expected_path.with_suffix(fixed_expected_path.suffix + ".tmp")
    tmp_expected_path.write_text(
        json.dumps(observed, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    try:
        fixed_case_path.parent.mkdir(parents=True, exist_ok=True)
        _move_path(known_case_path, fixed_case_path, git_root=git_root, dry_run=False)

        if resources_from.exists():
            _move_path(resources_from, resources_to, git_root=git_root, dry_run=False)

        if known_expected_path.exists():
            _remove_path(known_expected_path, git_root=git_root, dry_run=False)

        tmp_expected_path.replace(fixed_expected_path)
    except Exception:
        if tmp_expected_path.exists():
            tmp_expected_path.unlink()
        raise

    if validate:
        import fetchgraph.tracer.handlers  # noqa: F401

        root_case, ctx = load_case_bundle(fixed_case_path)
        out = run_case(root_case, ctx)
        expected = json.loads(fixed_expected_path.read_text(encoding="utf-8"))
        if out != expected:
            raise AssertionError("Replay output does not match expected after fixture-green")

    print("fixture-green:")
    print(f"  case:   {known_case_path}")
    print(f"  move:   -> {fixed_case_path}")
    print(f"  write:  -> {fixed_expected_path} (from root.observed)")
    if resources_from.exists():
        print(f"  move:   resources -> {resources_to}")
    if validate:
        print("  validate: OK")


def fixture_rm(
    *,
    root: Path,
    name: str | None,
    pattern: str | None,
    bucket: str,
    scope: str,
    dry_run: bool,
    git_mode: str = "auto",
) -> int:
    root = root.resolve()
    git_root = _resolve_git_root(root, git_mode)
    bucket_filter: str | None = None if bucket == "all" else bucket
    matched = find_case_bundles(root=root, bucket=bucket_filter, name=name, pattern=pattern)

    if name and not matched:
        raise FileNotFoundError(f"No fixtures found for name={name!r}")

    targets: list[Path] = []
    for case_path in matched:
        bucket_name = case_path.parent.name
        stem = case_path.name.replace(".case.json", "")
        layout = FixtureLayout(root, bucket_name)
        if scope in ("cases", "both"):
            targets.extend([layout.case_path(stem), layout.expected_path(stem)])
        if scope in ("resources", "both"):
            targets.append(layout.resources_dir(stem))

    existing_targets = [target for target in targets if target.exists()]

    if name and not existing_targets:
        raise FileNotFoundError(f"No fixtures found for name={name!r} with scope={scope}")

    if dry_run:
        for target in targets:
            _remove_path(target, git_root=git_root, dry_run=True)
        return len(existing_targets)

    for target in targets:
        _remove_path(target, git_root=git_root, dry_run=False)
    return len(existing_targets)


def fixture_fix(
    *,
    root: Path,
    name: str,
    new_name: str,
    bucket: str,
    dry_run: bool,
    git_mode: str = "auto",
) -> None:
    root = root.resolve()
    if name == new_name:
        raise ValueError("fixture-fix requires a new name different from the old name.")

    git_root = _resolve_git_root(root, git_mode)

    layout = FixtureLayout(root, bucket)
    case_path = layout.case_path(name)
    expected_path = layout.expected_path(name)
    resources_dir = layout.resources_dir(name)
    new_case_path = layout.case_path(new_name)
    new_expected_path = layout.expected_path(new_name)
    new_resources_dir = layout.resources_dir(new_name)

    if not case_path.exists():
        raise FileNotFoundError(f"Missing case bundle: {case_path}")
    if new_case_path.exists():
        raise FileExistsError(f"Target case already exists: {new_case_path}")
    if new_expected_path.exists():
        raise FileExistsError(f"Target expected already exists: {new_expected_path}")
    if new_resources_dir.exists():
        raise FileExistsError(f"Target resources already exist: {new_resources_dir}")

    payload = load_bundle_json(case_path)
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
            old_prefix = f"resources/{name}/"
            rel_posix = rel.as_posix()
            if rel_posix.startswith(old_prefix):
                data_ref["file"] = f"resources/{new_name}/{rel_posix[len(old_prefix):]}"
                updated = True

    if dry_run:
        print("fixture-fix:")
        print(f"  rename: {case_path} -> {new_case_path}")
        if expected_path.exists():
            print(f"  move:   {expected_path} -> {new_expected_path}")
        if resources_dir.exists():
            print(f"  move:   {resources_dir} -> {new_resources_dir}")
        if updated:
            print("  rewrite: data_ref.file paths updated")
        return

    new_case_path.parent.mkdir(parents=True, exist_ok=True)
    _move_path(case_path, new_case_path, git_root=git_root, dry_run=False)
    if expected_path.exists():
        _move_path(expected_path, new_expected_path, git_root=git_root, dry_run=False)
    if resources_dir.exists():
        _move_path(resources_dir, new_resources_dir, git_root=git_root, dry_run=False)
    if updated:
        _atomic_write_json(new_case_path, payload)

    print("fixture-fix:")
    print(f"  rename: {case_path} -> {new_case_path}")
    if expected_path.exists():
        print(f"  move:   {expected_path} -> {new_expected_path}")
    if resources_dir.exists():
        print(f"  move:   {resources_dir} -> {new_resources_dir}")
    if updated:
        print("  rewrite: data_ref.file paths updated")


def fixture_migrate(
    *,
    root: Path,
    bucket: str = "all",
    dry_run: bool,
    git_mode: str = "auto",
) -> tuple[int, int]:
    root = root.resolve()
    git_root = _resolve_git_root(root, git_mode)
    bucket_filter = None if bucket == "all" else bucket
    matched = find_case_bundles(root=root, bucket=bucket_filter, name=None, pattern=None)
    bundles_updated = 0
    files_moved = 0
    for case_path in matched:
        stem = case_path.name.replace(".case.json", "")
        payload = load_bundle_json(case_path)
        resources = payload.get("resources") or {}
        if not isinstance(resources, dict):
            continue
        updated = False
        for resource_id, resource in resources.items():
            if not isinstance(resource, dict):
                continue
            data_ref = resource.get("data_ref")
            if not isinstance(data_ref, dict):
                continue
            file_name = data_ref.get("file")
            if not isinstance(file_name, str) or not file_name:
                continue
            rel = _safe_resource_path(file_name, stem=stem)
            if rel.parts[:3] == ("resources", stem, resource_id):
                continue
            if rel.parts[:1] == ("resources",) and len(rel.parts) >= 2:
                rel_tail = Path(*rel.parts[2:])
            else:
                rel_tail = rel
            if not rel_tail.parts:
                rel_tail = Path(rel.name)
            target_rel = Path("resources") / stem / resource_id / rel_tail
            src_path = case_path.parent / rel
            if not src_path.exists():
                raise FileNotFoundError(f"Missing resource file: {src_path}")
            dest_path = case_path.parent / target_rel
            if dest_path.exists() and not filecmp.cmp(src_path, dest_path, shallow=False):
                raise FileExistsError(
                    "Resource collision at destination:\n"
                    f"  dest: {dest_path}\n"
                    "Hint: clean the destination or run migrate in an empty output directory."
                )
            if dry_run:
                _move_path(src_path, dest_path, git_root=git_root, dry_run=True)
            else:
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                if not dest_path.exists():
                    _move_path(src_path, dest_path, git_root=git_root, dry_run=False)
                    files_moved += 1
            data_ref["file"] = target_rel.as_posix()
            updated = True
        if updated:
            bundles_updated += 1
            if dry_run:
                print(f"Would update bundle: {case_path}")
            else:
                _atomic_write_json(case_path, payload)
    return bundles_updated, files_moved
