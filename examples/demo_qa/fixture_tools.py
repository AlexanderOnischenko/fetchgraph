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


def _git_tracked(path: Path) -> bool:
    result = subprocess.run(
        ["git", "ls-files", "--error-unmatch", str(path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _remove_file(path: Path, *, use_git: bool, dry_run: bool) -> None:
    if dry_run:
        print(f"DRY: remove {path}")
        return
    if use_git and _git_tracked(path):
        try:
            subprocess.run(["git", "rm", "-f", str(path)], cwd=REPO_ROOT, check=True)
            return
        except subprocess.CalledProcessError:
            pass
    path.unlink(missing_ok=True)


def _move_file(src: Path, dst: Path, *, use_git: bool, dry_run: bool) -> None:
    if dry_run:
        print(f"DRY: move {src} -> {dst}")
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    if use_git and _git_tracked(src):
        try:
            subprocess.run(["git", "mv", str(src), str(dst)], cwd=REPO_ROOT, check=True)
            return
        except subprocess.CalledProcessError:
            pass
    shutil.move(str(src), str(dst))


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
        yield from root.rglob("*.json")


def _iter_trace_paths(bucket: Optional[str]) -> Iterable[Path]:
    buckets = [bucket] if bucket else BUCKETS
    for bkt in buckets:
        root = TRACE_ROOT / bkt
        if not root.exists():
            continue
        yield from root.glob("*_plan_trace.txt")


def _relative(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


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
    case_id = root.get("case_id")
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
        print(f"- {_bucket_from_path(path, REPLAY_ROOT if 'replay_points' in str(path) else TRACE_ROOT)}: {_relative(path)}")

    if dry_run:
        print(f"DRY: would remove {len(candidates)} files.")
        return 0

    for path in candidates:
        _remove_file(path, use_git=use_git, dry_run=False)

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
    conflicts = []
    for src in candidates:
        dst = REPLAY_ROOT / "fixed" / src.name
        if dst.exists():
            conflicts.append(dst)
        dests.append((src, dst))
    if conflicts:
        print("Конфликт имён в fixed:", file=sys.stderr)
        for conflict in conflicts:
            print(f"- {_relative(conflict)}", file=sys.stderr)
        return 1

    use_git = _is_git_repo()
    print("Found replay fixtures to promote:")
    for src in candidates:
        dst = REPLAY_ROOT / "fixed" / src.name
        print(f"- {_relative(src)} -> {_relative(dst)}")

    if dry_run:
        print(f"DRY: would promote {len(candidates)} replay fixtures.")
        if move_traces:
            print("DRY: plan traces would also be promoted if found.")
        return 0

    for src, dst in dests:
        _move_file(src, dst, use_git=use_git, dry_run=False)

    print(f"Promoted {len(candidates)} replay fixtures to fixed.")

    if move_traces:
        case_ids: set[str] = set()
        for src in candidates:
            if case_id:
                case_ids.add(case_id)
                continue
            loaded_case_id = _load_case_id(src)
            if loaded_case_id:
                case_ids.add(loaded_case_id)

        promoted_traces: list[Path] = []
        for cid in sorted(case_ids):
            promoted_traces.extend(_collect_traces_for_case(cid))

        if not promoted_traces:
            print("No plan traces found to promote.")
            return 0

        for trace in promoted_traces:
            dst = TRACE_ROOT / "fixed" / trace.name
            if dst.exists():
                print(f"Конфликт trace в fixed: {_relative(dst)}", file=sys.stderr)
                return 1

        for trace in promoted_traces:
            _move_file(trace, TRACE_ROOT / "fixed" / trace.name, use_git=use_git, dry_run=False)

        print(f"Also promoted {len(promoted_traces)} plan traces.")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tools for managing test fixtures.")
    sub = parser.add_subparsers(dest="command", required=True)

    rm_parser = sub.add_parser("rm", help="Remove fixtures by name or pattern.")
    rm_parser.add_argument("--name", default="")
    rm_parser.add_argument("--pattern", default="")
    rm_parser.add_argument("--bucket", default="")
    rm_parser.add_argument("--scope", default="both")
    rm_parser.add_argument("--dry", type=int, default=0)
    rm_parser.set_defaults(func=cmd_rm)

    fix_parser = sub.add_parser("fix", help="Promote known_bad fixtures to fixed.")
    fix_parser.add_argument("--name", default="")
    fix_parser.add_argument("--pattern", default="")
    fix_parser.add_argument("--case", dest="case", default="")
    fix_parser.add_argument("--move-traces", type=int, default=0)
    fix_parser.add_argument("--dry", type=int, default=0)
    fix_parser.set_defaults(func=cmd_fix)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
