from __future__ import annotations

import argparse
import fnmatch
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
REPLAY_ROOT = REPO_ROOT / "tests" / "fixtures" / "replay_points"
TRACE_ROOT = REPO_ROOT / "tests" / "fixtures" / "plan_traces"
BUCKETS = ("fixed", "known_bad")


def _normalize(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _is_git_repo() -> bool:
    result = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def _git_path(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


def _git_tracked(path: Path) -> bool:
    result = subprocess.run(
        ["git", "ls-files", "--error-unmatch", _git_path(path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _git_has_tracked(path: Path) -> bool:
    result = subprocess.run(
        ["git", "ls-files", "-z", "--", _git_path(path)],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
    )
    return result.returncode == 0 and bool(result.stdout)


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def _remove_file(path: Path, *, use_git: bool, dry_run: bool) -> None:
    if dry_run:
        print(f"DRY: remove {path}")
        return
    if use_git and _git_tracked(path):
        try:
            subprocess.run(["git", "rm", "-f", _git_path(path)], cwd=REPO_ROOT, check=True)
            return
        except subprocess.CalledProcessError:
            pass
    path.unlink(missing_ok=True)


def _remove_tree(path: Path, *, use_git: bool, dry_run: bool) -> None:
    if not path.exists():
        return
    if dry_run:
        print(f"DRY: remove tree {path}")
        return
    if use_git and _git_has_tracked(path):
        try:
            subprocess.run(["git", "rm", "-r", "-f", _git_path(path)], cwd=REPO_ROOT, check=True)
            return
        except subprocess.CalledProcessError:
            pass
    shutil.rmtree(path, ignore_errors=True)


def _move_file(src: Path, dst: Path, *, use_git: bool, dry_run: bool) -> None:
    if dry_run:
        print(f"DRY: move {src} -> {dst}")
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    if use_git and (_git_tracked(src) or _git_tracked(dst)):
        try:
            subprocess.run(["git", "mv", _git_path(src), _git_path(dst)], cwd=REPO_ROOT, check=True)
            return
        except subprocess.CalledProcessError:
            pass
    shutil.move(str(src), str(dst))


def _move_tree(src: Path, dst: Path, *, use_git: bool, dry_run: bool) -> None:
    if dry_run:
        print(f"DRY: move tree {src} -> {dst}")
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    if use_git and (_git_has_tracked(src) or _git_has_tracked(dst)):
        try:
            subprocess.run(["git", "mv", _git_path(src), _git_path(dst)], cwd=REPO_ROOT, check=True)
            return
        except subprocess.CalledProcessError:
            pass
    shutil.move(str(src), str(dst))


def _rollback_moves(moves_done: list[tuple[Path, Path, bool]], *, use_git: bool) -> None:
    """
    Best-effort rollback of already executed moves.
    moves_done: list of (src, dst, is_dir) that were successfully moved src -> dst.
    Rollback tries to move dst -> src in reverse order.
    """
    if not moves_done:
        return
    print("Rolling back already moved files...", file=sys.stderr)
    for src, dst, is_dir in reversed(moves_done):
        try:
            if not dst.exists():
                print(f"ROLLBACK: skip (missing) {dst}", file=sys.stderr)
                continue
            if src.exists():
                print(f"ROLLBACK: skip (src exists) {src} <- {dst}", file=sys.stderr)
                continue
            if is_dir:
                _move_tree(dst, src, use_git=use_git, dry_run=False)
            else:
                _move_file(dst, src, use_git=use_git, dry_run=False)
            print(f"ROLLBACK: {dst} -> {src}", file=sys.stderr)
        except Exception as exc:
            print(f"ROLLBACK ERROR: failed {dst} -> {src}: {exc}", file=sys.stderr)


def _matches_filters(path: Path, *, name: Optional[str], pattern: Optional[str]) -> bool:
    if name and path.name != name and path.stem != name:
        return False
    if pattern and not fnmatch.fnmatch(path.name, pattern):
        return False
    return True


def _iter_replay_paths(bucket: Optional[str]) -> Iterable[Path]:
    buckets = [bucket] if bucket else BUCKETS
    for bkt in buckets:
        root = REPLAY_ROOT / bkt
        if not root.exists():
            continue
        for path in root.rglob("*.json"):
            if "resources" in path.parts:
                continue
            yield path


def _iter_trace_paths(bucket: Optional[str]) -> Iterable[Path]:
    buckets = [bucket] if bucket else BUCKETS
    for bkt in buckets:
        root = TRACE_ROOT / bkt
        if not root.exists():
            continue
        yield from root.rglob("*_plan_trace.txt")


def _relative(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _resource_key_for_fixture(fixture_path: Path) -> str:
    if _is_relative_to(fixture_path, REPLAY_ROOT):
        rel = fixture_path.relative_to(REPLAY_ROOT)
        if len(rel.parts) >= 2:
            bucket = rel.parts[0]
            fixture_rel = fixture_path.relative_to(REPLAY_ROOT / bucket).with_suffix("")
            return "__".join(fixture_rel.parts)
    return fixture_path.stem


def _resources_dir_for_fixture(fixture_path: Path) -> Path:
    if _is_relative_to(fixture_path, REPLAY_ROOT):
        rel = fixture_path.relative_to(REPLAY_ROOT)
        if len(rel.parts) >= 2:
            bucket = rel.parts[0]
            return REPLAY_ROOT / bucket / "resources" / _resource_key_for_fixture(fixture_path)
    return fixture_path.parent / "resources" / _resource_key_for_fixture(fixture_path)


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _resource_destination(
    fixture_path: Path,
    file_name: str,
    resource_key: str,
    used_paths: set[Path],
) -> Path:
    rel_path = Path(file_name)
    if rel_path.is_absolute() or ".." in rel_path.parts:
        raise ValueError(f"Invalid resource path: {file_name}")
    base_dir = Path("resources") / _resource_key_for_fixture(fixture_path)
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


def _bucket_from_path(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).parts[0]
    except ValueError:
        return "unknown"


def _load_case_id(path: Path) -> Optional[str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if data.get("type") == "replay_bundle":
        root = data.get("root") or {}
    else:
        root = data
    case_id = (
        root.get("case_id")
        or (root.get("meta") or {}).get("case_id")
        or (data.get("meta") or {}).get("case_id")
        or (data.get("input") or {}).get("case_id")
    )
    return case_id if isinstance(case_id, str) else None


def _select_with_case(paths: list[Path], case_id: str) -> list[Path]:
    name_matches = [path for path in paths if case_id in path.name]
    if name_matches:
        return name_matches
    return [path for path in paths if _load_case_id(path) == case_id]


def _validate_name_or_pattern(name: Optional[str], pattern: Optional[str]) -> None:
    if not name and not pattern:
        raise ValueError("Нужно задать хотя бы NAME или PATTERN.")


def _validate_name_pattern_case(name: Optional[str], pattern: Optional[str], case_id: Optional[str]) -> None:
    if not name and not pattern and not case_id:
        raise ValueError("Нужно задать хотя бы NAME, PATTERN или CASE.")


def _collect_candidates(
    *,
    scope: str,
    bucket: Optional[str],
    name: Optional[str],
    pattern: Optional[str],
) -> list[Path]:
    candidates: list[Path] = []
    if scope in ("replay", "both"):
        for path in _iter_replay_paths(bucket):
            if _matches_filters(path, name=name, pattern=pattern):
                candidates.append(path)
    if scope in ("traces", "both"):
        for path in _iter_trace_paths(bucket):
            if _matches_filters(path, name=name, pattern=pattern):
                candidates.append(path)
    return sorted(candidates, key=lambda p: _relative(p))


def cmd_rm(args: argparse.Namespace) -> int:
    name = _normalize(args.name)
    pattern = _normalize(args.pattern)
    bucket = _normalize(args.bucket)
    scope = _normalize(args.scope) or "both"
    dry_run = bool(args.dry)
    with_resources = bool(args.with_resources)

    if bucket and bucket not in BUCKETS:
        print(f"Неизвестный BUCKET: {bucket}", file=sys.stderr)
        return 1
    if scope not in ("replay", "traces", "both"):
        print(f"Неизвестный SCOPE: {scope}", file=sys.stderr)
        return 1

    try:
        _validate_name_or_pattern(name, pattern)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    candidates = _collect_candidates(scope=scope, bucket=bucket, name=name, pattern=pattern)
    if not candidates:
        print("Ничего не найдено.", file=sys.stderr)
        return 1

    use_git = _is_git_repo()
    print("Found files to remove:")
    for path in candidates:
        root = REPLAY_ROOT if _is_relative_to(path, REPLAY_ROOT) else TRACE_ROOT
        print(f"- {_bucket_from_path(path, root)}: {_relative(path)}")
        if with_resources and _is_relative_to(path, REPLAY_ROOT):
            resources_dir = _resources_dir_for_fixture(path)
            if resources_dir.exists():
                print(f"  - resources: {_relative(resources_dir)}")

    if dry_run:
        print(f"DRY: would remove {len(candidates)} files.")
        return 0

    resource_dirs: list[Path] = []
    if with_resources and scope in ("replay", "both"):
        for path in candidates:
            if _is_relative_to(path, REPLAY_ROOT):
                resources_dir = _resources_dir_for_fixture(path)
                if resources_dir.exists():
                    resource_dirs.append(resources_dir)

    for resource_dir in sorted(set(resource_dirs), key=lambda p: _relative(p)):
        _remove_tree(resource_dir, use_git=use_git, dry_run=False)
    for path in candidates:
        _remove_file(path, use_git=use_git, dry_run=False)

    if resource_dirs:
        print(f"Removed {len(candidates)} files and {len(set(resource_dirs))} resource trees.")
    else:
        print(f"Removed {len(candidates)} files.")
    return 0


def _collect_replay_known_bad(
    *,
    name: Optional[str],
    pattern: Optional[str],
    case_id: Optional[str],
) -> list[Path]:
    candidates = [path for path in _iter_replay_paths("known_bad") if _matches_filters(path, name=name, pattern=pattern)]
    if case_id:
        candidates = _select_with_case(candidates, case_id)
    return sorted(candidates, key=lambda p: _relative(p))


def _collect_traces_for_case(case_id: str) -> list[Path]:
    root = TRACE_ROOT / "known_bad"
    if not root.exists():
        return []
    return sorted(root.glob(f"*{case_id}*plan_trace*.txt"), key=lambda p: _relative(p))


def cmd_fix(args: argparse.Namespace) -> int:
    name = _normalize(args.name)
    pattern = _normalize(args.pattern)
    case_id = _normalize(args.case)
    move_traces = bool(args.move_traces)
    dry_run = bool(args.dry)

    try:
        _validate_name_pattern_case(name, pattern, case_id)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    candidates = _collect_replay_known_bad(name=name, pattern=pattern, case_id=case_id)
    if not candidates:
        print("Ничего не найдено в known_bad.", file=sys.stderr)
        return 1

    dests = []
    resource_moves: list[tuple[Path, Path]] = []
    conflicts = []
    dst_seen: dict[Path, Path] = {}
    for src in candidates:
        rel_path = src.relative_to(REPLAY_ROOT / "known_bad")
        dst = REPLAY_ROOT / "fixed" / rel_path
        if dst in dst_seen and dst_seen[dst] != src:
            conflicts.append(dst)
        if dst.exists():
            conflicts.append(dst)
        dst_seen[dst] = src
        dests.append((src, dst))
        src_resources = _resources_dir_for_fixture(src)
        if src_resources.exists():
            dst_resources = _resources_dir_for_fixture(dst)
            if dst_resources.exists():
                conflicts.append(dst_resources)
            resource_moves.append((src_resources, dst_resources))
    if conflicts:
        print("Конфликт имён в fixed:", file=sys.stderr)
        for conflict in conflicts:
            print(f"- {_relative(conflict)}", file=sys.stderr)
        return 1

    use_git = _is_git_repo()
    print("Found replay fixtures to promote:")
    for src, dst in dests:
        print(f"- {_relative(src)} -> {_relative(dst)}")
    for src, dst in resource_moves:
        print(f"  - resources: {_relative(src)} -> {_relative(dst)}")

    case_ids: set[str] = set()
    promoted_traces: list[Path] = []
    trace_dests: list[tuple[Path, Path]] = []
    if move_traces:
        for src in candidates:
            resolved_case_id = case_id or _load_case_id(src)
            if resolved_case_id:
                case_ids.add(resolved_case_id)

        for cid in sorted(case_ids):
            promoted_traces.extend(_collect_traces_for_case(cid))

        for trace in promoted_traces:
            trace_dests.append((trace, TRACE_ROOT / "fixed" / trace.name))

        trace_conflicts = [dst for _, dst in trace_dests if dst.exists()]
        if trace_conflicts:
            print("Конфликт trace в fixed:", file=sys.stderr)
            for conflict in trace_conflicts:
                print(f"- {_relative(conflict)}", file=sys.stderr)
            return 1

    if dry_run:
        print(f"DRY: would promote {len(candidates)} replay fixtures.")
        if move_traces:
            if not promoted_traces:
                print("DRY: no plan traces found to promote.")
            else:
                print(f"DRY: would also promote {len(promoted_traces)} plan traces:")
                for src, dst in trace_dests:
                    print(f"- {_relative(src)} -> {_relative(dst)}")
        return 0

    moves_done: list[tuple[Path, Path, bool]] = []
    try:
        for src, dst in dests:
            _move_file(src, dst, use_git=use_git, dry_run=False)
            moves_done.append((src, dst, False))
        for src, dst in resource_moves:
            _move_tree(src, dst, use_git=use_git, dry_run=False)
            moves_done.append((src, dst, True))

        print(f"Promoted {len(candidates)} replay fixtures to fixed.")

        if move_traces:
            if not promoted_traces:
                print("No plan traces found to promote.")
                return 0
            for src, dst in trace_dests:
                _move_file(src, dst, use_git=use_git, dry_run=False)
                moves_done.append((src, dst, False))
            print(f"Also promoted {len(promoted_traces)} plan traces.")

        return 0
    except Exception as exc:
        print("ERROR: fix failed during move. State may be partial; attempting rollback.", file=sys.stderr)
        print(f"Cause: {exc}", file=sys.stderr)
        _rollback_moves(moves_done, use_git=use_git)
        return 1


def cmd_migrate(args: argparse.Namespace) -> int:
    bucket = _normalize(args.bucket)
    dry_run = bool(args.dry)
    if bucket and bucket not in BUCKETS:
        print(f"Неизвестный BUCKET: {bucket}", file=sys.stderr)
        return 1

    buckets = [bucket] if bucket else BUCKETS
    use_git = _is_git_repo()
    total_moves = 0
    total_updates = 0

    for bkt in buckets:
        root = REPLAY_ROOT / bkt
        if not root.exists():
            continue
        fixture_candidates = [path for path in root.rglob("*.json") if "resources" not in path.parts]
        for fixture_path in sorted(fixture_candidates, key=lambda p: _relative(p)):
            original_text: str | None = None
            try:
                original_text = fixture_path.read_text(encoding="utf-8")
                data = json.loads(original_text)
            except (json.JSONDecodeError, OSError) as exc:
                print(f"Skip invalid json {_relative(fixture_path)}: {exc}", file=sys.stderr)
                continue
            if data.get("type") != "replay_bundle":
                continue
            resources = data.get("resources")
            if not isinstance(resources, dict):
                continue
            used_paths: set[Path] = set()
            moves: list[tuple[Path, Path]] = []
            changes = False
            conflicts: list[Path] = []
            for rid, resource in resources.items():
                if not isinstance(resource, dict):
                    continue
                data_ref = resource.get("data_ref")
                if not isinstance(data_ref, dict):
                    continue
                file_name = data_ref.get("file")
                if not isinstance(file_name, str) or not file_name:
                    continue
                rel_path = Path(file_name)
                if rel_path.is_absolute() or ".." in rel_path.parts:
                    print(
                        f"Skip invalid resource path {_relative(fixture_path)}: {file_name}",
                        file=sys.stderr,
                    )
                    continue
                if (
                    len(rel_path.parts) >= 2
                    and rel_path.parts[0] == "resources"
                    and rel_path.parts[1] == _resource_key_for_fixture(fixture_path)
                ):
                    used_paths.add(rel_path)
                    continue
                try:
                    dest_rel = _resource_destination(fixture_path, file_name, rid, used_paths)
                except ValueError as exc:
                    print(f"Skip resource in {_relative(fixture_path)}: {exc}", file=sys.stderr)
                    continue
                src_path = fixture_path.parent / file_name
                dst_path = fixture_path.parent / dest_rel
                if dst_path.exists():
                    conflicts.append(dst_path)
                    continue
                if not src_path.exists():
                    print(
                        f"Missing resource for {_relative(fixture_path)}: {_relative(src_path)}",
                        file=sys.stderr,
                    )
                    continue
                moves.append((src_path, dst_path))
                updated_resource = dict(resource)
                updated_data_ref = dict(data_ref)
                updated_data_ref["file"] = dest_rel.as_posix()
                updated_resource["data_ref"] = updated_data_ref
                resources[rid] = updated_resource
                changes = True

            if conflicts:
                print(f"Conflicts for {_relative(fixture_path)}:", file=sys.stderr)
                for conflict in conflicts:
                    print(f"- {_relative(conflict)}", file=sys.stderr)
                continue

            if not moves and not changes:
                continue

            print(f"Migrate resources for {_relative(fixture_path)}:")
            for src, dst in moves:
                print(f"- {_relative(src)} -> {_relative(dst)}")

            if dry_run:
                continue

            moves_done: list[tuple[Path, Path, bool]] = []
            try:
                for src, dst in moves:
                    _move_file(src, dst, use_git=use_git, dry_run=False)
                    moves_done.append((src, dst, False))

                if changes:
                    data["resources"] = resources
                    fixture_path.write_text(_canonical_json(data), encoding="utf-8")
                    total_updates += 1
                    total_moves += len(moves)
            except Exception as exc:
                print(
                    f"ERROR: migrate failed for {_relative(fixture_path)}: {exc}",
                    file=sys.stderr,
                )
                _rollback_moves(moves_done, use_git=use_git)
                if original_text is not None:
                    try:
                        fixture_path.write_text(original_text, encoding="utf-8")
                    except Exception as rollback_exc:
                        print(
                            f"ROLLBACK ERROR: failed to restore {_relative(fixture_path)}: {rollback_exc}",
                            file=sys.stderr,
                        )

    if dry_run:
        print("DRY: migration scan complete.")
    else:
        print(f"Migrated {total_moves} resources across {total_updates} fixtures.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tools for managing test fixtures.")
    sub = parser.add_subparsers(dest="command", required=True)

    rm_parser = sub.add_parser("rm", help="Remove fixtures by name or pattern.")
    rm_parser.add_argument("--name", default="")
    rm_parser.add_argument("--pattern", default="")
    rm_parser.add_argument("--bucket", default="")
    rm_parser.add_argument("--scope", default="both")
    rm_parser.add_argument("--with-resources", type=int, default=1)
    rm_parser.add_argument("--dry", type=int, default=0)
    rm_parser.set_defaults(func=cmd_rm)

    fix_parser = sub.add_parser("fix", help="Promote known_bad fixtures to fixed.")
    fix_parser.add_argument("--name", default="")
    fix_parser.add_argument("--pattern", default="")
    fix_parser.add_argument("--case", dest="case", default="")
    fix_parser.add_argument("--move-traces", type=int, default=0)
    fix_parser.add_argument("--dry", type=int, default=0)
    fix_parser.set_defaults(func=cmd_fix)

    migrate_parser = sub.add_parser("migrate", help="Migrate replay bundle resources into resources/<fixture>/")
    migrate_parser.add_argument("--bucket", default="")
    migrate_parser.add_argument("--dry", type=int, default=0)
    migrate_parser.set_defaults(func=cmd_migrate)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
