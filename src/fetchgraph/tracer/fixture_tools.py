from __future__ import annotations

import filecmp
import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .fixture_layout import FixtureLayout, find_case_bundles
from .runtime import load_case_bundle, run_case


@dataclass(frozen=True)
class FixtureCandidate:
    path: Path
    stem: str
    source: dict


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


def resolve_fixture_candidates(
    *,
    root: Path,
    bucket: str,
    case_id: str | None,
    name: str | None,
) -> list[FixtureCandidate]:
    if case_id and name:
        raise ValueError("Only one of case_id or name can be used.")
    candidates = find_case_bundles(root=root, bucket=bucket, name=name, pattern=None)
    results: list[FixtureCandidate] = []
    for path in candidates:
        payload = load_bundle_json(path)
        stem = path.name.replace(".case.json", "")
        source = payload.get("source")
        source_dict = source if isinstance(source, dict) else {}
        if case_id:
            source_case = source_dict.get("case") or source_dict.get("case_id")
            if source_case == case_id or stem.startswith(case_id):
                results.append(FixtureCandidate(path=path, stem=stem, source=source_dict))
        else:
            results.append(FixtureCandidate(path=path, stem=stem, source=source_dict))
    return results


def resolve_fixture_selector(
    *,
    root: Path,
    bucket: str,
    case_path: Path | None = None,
    case_id: str | None = None,
    name: str | None = None,
    select: str = "latest",
    select_index: int | None = None,
    require_unique: bool = False,
    all_matches: bool = False,
) -> list[Path]:
    if case_path is not None and (case_id or name):
        raise ValueError("Only one of case_path, case_id, or name can be used.")
    if case_path is not None:
        return [case_path]
    candidates = resolve_fixture_candidates(root=root, bucket=bucket, case_id=case_id, name=name)
    if not candidates:
        selector = case_id or name
        raise FileNotFoundError(f"No fixtures found for selector={selector!r} in {bucket}")
    if require_unique and len(candidates) > 1:
        raise FileExistsError("Multiple fixtures matched selection; use --select-index or --select.")
    if all_matches:
        print("fixture selector: bulk operation on all matches")
        for idx, candidate in enumerate(candidates, start=1):
            print(f"  {idx}. {candidate.path}")
        return [candidate.path for candidate in candidates]

    selected = select_fixture_candidate(
        candidates,
        select=select,
        select_index=select_index,
        require_unique=require_unique,
    )
    if len(candidates) > 1 and not require_unique:
        print("fixture selector: multiple candidates found, selected:")
        for idx, candidate in enumerate(candidates, start=1):
            marker = " (selected)" if candidate.path == selected.path else ""
            print(f"  {idx}. {candidate.path}{marker}")
    return [selected.path]


def select_fixture_candidate(
    candidates: list[FixtureCandidate],
    *,
    select: str = "latest",
    select_index: int | None = None,
    require_unique: bool = False,
) -> FixtureCandidate:
    if not candidates:
        raise FileNotFoundError("No fixtures matched the selection.")
    if require_unique and len(candidates) > 1:
        raise FileExistsError("Multiple fixtures matched selection; use --select-index or --select.")
    if select_index is not None:
        if select_index < 1 or select_index > len(candidates):
            raise ValueError(f"select_index must be between 1 and {len(candidates)}")
        return candidates[select_index - 1]

    def _sort_key(candidate: FixtureCandidate) -> tuple[str, float]:
        timestamp = candidate.source.get("timestamp")
        run_id = candidate.source.get("run_id")
        time_key = f"{timestamp or ''}{run_id or ''}"
        try:
            mtime = candidate.path.stat().st_mtime
        except OSError:
            mtime = 0.0
        return (time_key, mtime)

    ordered = sorted(candidates, key=_sort_key)
    if select in {"latest", "last"}:
        return ordered[-1]
    if select == "first":
        return ordered[0]
    raise ValueError(f"Unsupported select policy: {select}")


def _git_available() -> bool:
    try:
        subprocess.run(["git", "--version"], check=True, capture_output=True, text=True)
    except (OSError, subprocess.CalledProcessError):
        return False
    return True


def _is_git_repo(root: Path) -> bool:
    try:
        subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--is-inside-work-tree"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return False
    return True


def _git_tracked(root: Path, path: Path) -> bool:
    try:
        subprocess.run(
            ["git", "-C", str(root), "ls-files", "--error-unmatch", str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return False
    return True


def _git_dir_tracked(root: Path, path: Path) -> bool:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files", str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return False
    return bool(result.stdout.strip())


def _git_mv(root: Path, src: Path, dest: Path) -> None:
    subprocess.run(["git", "-C", str(root), "mv", str(src), str(dest)], check=True)


def _git_rm(root: Path, path: Path) -> None:
    subprocess.run(["git", "-C", str(root), "rm", "-r", "--", str(path)], check=True)


class _GitOps:
    def __init__(self, root: Path, mode: str) -> None:
        self.root = root
        self.mode = mode
        self.use_git = False
        if mode not in {"auto", "on", "off"}:
            raise ValueError(f"Unsupported git mode: {mode}")
        if mode == "off":
            return
        git_ok = _git_available() and _is_git_repo(root)
        if mode == "on" and not git_ok:
            raise ValueError("git mode is on but no git repository is available")
        self.use_git = git_ok

    def move(self, src: Path, dest: Path) -> None:
        if self.use_git and self._should_git_move(src):
            _git_mv(self.root, src, dest)
        else:
            shutil.move(str(src), str(dest))

    def remove(self, path: Path) -> None:
        if self.use_git and self._should_git_remove(path):
            _git_rm(self.root, path)
        else:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            elif path.exists():
                path.unlink()

    def _should_git_move(self, src: Path) -> bool:
        if src.is_dir():
            return _git_dir_tracked(self.root, src)
        return _git_tracked(self.root, src)

    def _should_git_remove(self, path: Path) -> bool:
        if path.is_dir():
            return _git_dir_tracked(self.root, path)
        return _git_tracked(self.root, path)


def _unique_backup_path(path: Path) -> Path:
    for idx in range(1, 1000):
        suffix = "" if idx == 1 else f".{idx}"
        candidate = path.with_name(f".{path.name}.rollback{suffix}")
        if not candidate.exists():
            return candidate
    raise FileExistsError(f"Unable to create rollback path for {path}")


class _MoveTransaction:
    def __init__(self, git_ops: _GitOps) -> None:
        self.git_ops = git_ops
        self.moved: list[tuple[Path, Path]] = []
        self.removed: list[tuple[Path, Path]] = []

    def move(self, src: Path, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        self.git_ops.move(src, dest)
        self.moved.append((src, dest))

    def remove(self, path: Path) -> None:
        if not path.exists():
            return
        backup = _unique_backup_path(path)
        backup.parent.mkdir(parents=True, exist_ok=True)
        self.git_ops.move(path, backup)
        self.removed.append((path, backup))

    def rollback(self) -> None:
        for src, dest in reversed(self.moved):
            if dest.exists():
                self.git_ops.move(dest, src)
        for original, backup in reversed(self.removed):
            if backup.exists():
                self.git_ops.move(backup, original)

    def commit(self) -> None:
        for _, backup in self.removed:
            if backup.exists():
                self.git_ops.remove(backup)


def fixture_green(
    *,
    case_path: Path | None = None,
    case_id: str | None = None,
    name: str | None = None,
    out_root: Path,
    validate: bool = False,
    overwrite_expected: bool = False,
    dry_run: bool = False,
    git_mode: str = "auto",
    select: str = "latest",
    select_index: int | None = None,
    require_unique: bool = False,
) -> None:
    if case_path is None and not case_id and not name:
        raise ValueError("fixture-green requires --case, --case-id, or --name.")
    if case_path is not None and (case_id or name):
        raise ValueError("fixture-green accepts only one of --case, --case-id, or --name.")
    out_root = out_root.resolve()
    git_ops = _GitOps(out_root, git_mode)
    known_layout = FixtureLayout(out_root, "known_bad")
    if case_path is None:
        candidates = resolve_fixture_candidates(
            root=out_root,
            bucket="known_bad",
            case_id=case_id,
            name=name,
        )
        if not candidates:
            selector = case_id or name
            raise FileNotFoundError(f"No fixtures found for selector={selector!r} in known_bad")
        selected = select_fixture_candidate(
            candidates,
            select=select,
            select_index=select_index,
            require_unique=require_unique,
        )
        case_path = selected.path
        if len(candidates) > 1 and not require_unique:
            print("fixture-green: multiple candidates found, selected:")
            for idx, candidate in enumerate(candidates, start=1):
                marker = " (selected)" if candidate.path == selected.path else ""
                print(f"  {idx}. {candidate.path}{marker}")
    case_path = case_path.resolve()
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

    if dry_run:
        print("fixture-green:")
        print(f"  case:   {known_case_path}")
        print(f"  move:   -> {fixed_case_path}")
        print(f"  write:  -> {fixed_expected_path} (from root.observed)")
        if resources_from.exists():
            print(f"  move:   resources -> {resources_to}")
        if validate:
            print("  validate: would run")
        return

    fixed_expected_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_expected_path = fixed_expected_path.with_suffix(fixed_expected_path.suffix + ".tmp")
    tmp_expected_path.write_text(
        json.dumps(observed, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    tx = _MoveTransaction(git_ops)
    try:
        tx.move(known_case_path, fixed_case_path)

        if resources_from.exists():
            tx.move(resources_from, resources_to)

        if known_expected_path.exists():
            tx.remove(known_expected_path)

        tmp_expected_path.replace(fixed_expected_path)
        tx.commit()
    except Exception:
        if fixed_expected_path.exists():
            fixed_expected_path.unlink()
        if tmp_expected_path.exists():
            tmp_expected_path.unlink()
        tx.rollback()
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


@dataclass(frozen=True)
class FixtureRmSummary:
    known_bad: int
    fixed: int
    resources: int


def fixture_rm(
    *,
    root: Path,
    bucket: str,
    scope: str,
    dry_run: bool,
    git_mode: str = "auto",
    case_path: Path | None = None,
    case_id: str | None = None,
    name: str | None = None,
    pattern: str | None = None,
    select: str = "latest",
    select_index: int | None = None,
    require_unique: bool = False,
    all_matches: bool = False,
) -> FixtureRmSummary:
    root = root.resolve()
    git_ops = _GitOps(root, git_mode)
    bucket_filter: str | None = None if bucket == "all" else bucket
    if case_path or case_id or name:
        if case_path is None and (case_id or name):
            candidates = resolve_fixture_candidates(
                root=root,
                bucket=bucket_filter or bucket,
                case_id=case_id,
                name=name,
            )
            if (
                len(candidates) > 1
                and not all_matches
                and select_index is None
                and not require_unique
            ):
                raise FileExistsError(
                    "Multiple fixtures matched; use --all, --select-index, or --require-unique."
                )
        selector_paths = resolve_fixture_selector(
            root=root,
            bucket=bucket_filter or bucket,
            case_path=case_path,
            case_id=case_id,
            name=name,
            select=select,
            select_index=select_index,
            require_unique=require_unique,
            all_matches=all_matches,
        )
        matched = selector_paths
    else:
        matched = find_case_bundles(root=root, bucket=bucket_filter, name=name, pattern=pattern)

    targets: list[Path] = []
    removed_case_stems: dict[str, set[str]] = {"known_bad": set(), "fixed": set()}
    removed_resources: set[str] = set()
    for case_path_item in matched:
        bucket_name = case_path_item.parent.name
        stem = case_path_item.name.replace(".case.json", "")
        layout = FixtureLayout(root, bucket_name)
        if scope in ("cases", "both"):
            case_path_item = layout.case_path(stem)
            expected_path = layout.expected_path(stem)
            targets.extend([case_path_item, expected_path])
            if case_path_item.exists() or expected_path.exists():
                removed_case_stems.setdefault(bucket_name, set()).add(stem)
        if scope in ("resources", "both"):
            resources_dir = layout.resources_dir(stem)
            targets.append(resources_dir)
            if resources_dir.exists():
                removed_resources.add(stem)

    existing_targets = [target for target in targets if target.exists()]

    if name and not existing_targets:
        raise FileNotFoundError(f"No fixtures found for name={name!r} with scope={scope}")

    if dry_run:
        for target in targets:
            print(f"Would remove: {target}")
        return FixtureRmSummary(
            known_bad=len(removed_case_stems.get("known_bad", set())),
            fixed=len(removed_case_stems.get("fixed", set())),
            resources=len(removed_resources),
        )

    for target in targets:
        git_ops.remove(target)
    return FixtureRmSummary(
        known_bad=len(removed_case_stems.get("known_bad", set())),
        fixed=len(removed_case_stems.get("fixed", set())),
        resources=len(removed_resources),
    )


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
    git_ops = _GitOps(root, git_mode)
    if name == new_name:
        raise ValueError("fixture-fix requires a new name different from the old name.")

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

    original_text = case_path.read_text(encoding="utf-8") if updated else None
    tx = _MoveTransaction(git_ops)
    try:
        tx.move(case_path, new_case_path)
        if expected_path.exists():
            tx.move(expected_path, new_expected_path)
        if resources_dir.exists():
            tx.move(resources_dir, new_resources_dir)
        if updated:
            _atomic_write_json(new_case_path, payload)
        tx.commit()
    except Exception:
        if updated and original_text is not None and new_case_path.exists():
            new_case_path.write_text(original_text, encoding="utf-8")
        tx.rollback()
        raise

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
    case_path: Path | None = None,
    case_id: str | None = None,
    name: str | None = None,
    select: str = "latest",
    select_index: int | None = None,
    require_unique: bool = False,
    all_matches: bool = False,
) -> tuple[int, int]:
    root = root.resolve()
    git_ops = _GitOps(root, git_mode)
    bucket_filter = None if bucket == "all" else bucket
    if case_path or case_id or name:
        selector_paths = resolve_fixture_selector(
            root=root,
            bucket=bucket_filter or bucket,
            case_path=case_path,
            case_id=case_id,
            name=name,
            select=select,
            select_index=select_index,
            require_unique=require_unique,
            all_matches=all_matches,
        )
        matched = selector_paths
    else:
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
                print(f"Would move {src_path} -> {dest_path}")
            else:
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                if not dest_path.exists():
                    git_ops.move(src_path, dest_path)
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


def fixture_ls(
    *,
    root: Path,
    bucket: str,
    case_id: str | None = None,
    pattern: str | None = None,
) -> list[FixtureCandidate]:
    bucket_filter = None if bucket == "all" else bucket
    candidates = find_case_bundles(root=root, bucket=bucket_filter, name=None, pattern=pattern)
    results: list[FixtureCandidate] = []
    for path in candidates:
        payload = load_bundle_json(path)
        stem = path.name.replace(".case.json", "")
        source = payload.get("source")
        source_dict = source if isinstance(source, dict) else {}
        if case_id:
            source_case = source_dict.get("case") or source_dict.get("case_id")
            if source_case != case_id and not stem.startswith(case_id):
                continue
        results.append(FixtureCandidate(path=path, stem=stem, source=source_dict))
    return results


def fixture_demote(
    *,
    root: Path,
    from_bucket: str = "fixed",
    to_bucket: str = "known_bad",
    case_path: Path | None = None,
    case_id: str | None = None,
    name: str | None = None,
    dry_run: bool = False,
    git_mode: str = "auto",
    overwrite: bool = False,
    select: str = "latest",
    select_index: int | None = None,
    require_unique: bool = False,
    all_matches: bool = False,
) -> None:
    root = root.resolve()
    if from_bucket == to_bucket:
        raise ValueError("from_bucket and to_bucket must be different")
    git_ops = _GitOps(root, git_mode)
    selected_paths = resolve_fixture_selector(
        root=root,
        bucket=from_bucket,
        case_path=case_path,
        case_id=case_id,
        name=name,
        select=select,
        select_index=select_index,
        require_unique=require_unique,
        all_matches=all_matches,
    )
    for case_path_item in selected_paths:
        stem = case_path_item.name.replace(".case.json", "")
        from_layout = FixtureLayout(root, from_bucket)
        to_layout = FixtureLayout(root, to_bucket)
        from_case = from_layout.case_path(stem)
        from_expected = from_layout.expected_path(stem)
        from_resources = from_layout.resources_dir(stem)
        to_case = to_layout.case_path(stem)
        to_expected = to_layout.expected_path(stem)
        to_resources = to_layout.resources_dir(stem)

        if not from_case.exists():
            raise FileNotFoundError(f"Missing fixture: {from_case}")
        if to_case.exists() and not overwrite and not dry_run:
            raise FileExistsError(f"Target case already exists: {to_case}")
        if to_expected.exists() and from_expected.exists() and not overwrite and not dry_run:
            raise FileExistsError(f"Target expected already exists: {to_expected}")
        if to_resources.exists() and from_resources.exists() and not overwrite and not dry_run:
            raise FileExistsError(f"Target resources already exists: {to_resources}")

        if dry_run:
            print("fixture-demote:")
            print(f"  case:   {from_case}")
            print(f"  move:   -> {to_case}")
            if from_expected.exists():
                print(f"  move:   {from_expected} -> {to_expected}")
            if from_resources.exists():
                print(f"  move:   {from_resources} -> {to_resources}")
            continue

        tx = _MoveTransaction(git_ops)
        try:
            if overwrite:
                tx.remove(to_case)
            tx.move(from_case, to_case)
            if from_expected.exists():
                if overwrite:
                    tx.remove(to_expected)
                tx.move(from_expected, to_expected)
            if from_resources.exists():
                if overwrite:
                    tx.remove(to_resources)
                tx.move(from_resources, to_resources)
            tx.commit()
        except Exception:
            tx.rollback()
            raise

        print("fixture-demote:")
        print(f"  case:   {from_case}")
        print(f"  move:   -> {to_case}")
        if from_expected.exists():
            print(f"  move:   {from_expected} -> {to_expected}")
        if from_resources.exists():
            print(f"  move:   {from_resources} -> {to_resources}")
