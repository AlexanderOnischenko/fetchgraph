from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from fetchgraph.tracer.resolve import (
    find_events_file,
    format_case_runs,
    format_case_run_debug,
    format_events_search,
    list_case_runs,
    scan_case_runs,
    select_case_run,
)
from fetchgraph.tracer.export import (
    export_replay_case_bundle,
    export_replay_case_bundles,
    find_replay_case_matches,
    format_replay_case_matches,
)
from fetchgraph.tracer.fixture_tools import (
    fixture_fix,
    fixture_green,
    fixture_migrate,
    fixture_rm,
)

DEFAULT_ROOT = Path("tests/fixtures/replay_cases")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetchgraph tracer utilities")
    sub = parser.add_subparsers(dest="command", required=True)

    export = sub.add_parser("export-case-bundle", help="Export replay case bundle from events.jsonl")
    export.add_argument("--events", type=Path, help="Path to events.jsonl")
    export.add_argument("--out", type=Path, required=True, help="Output directory for bundle")
    export.add_argument("--id", help="Replay case id to export")
    export.add_argument("--spec-idx", type=int, default=None, help="Filter replay_case by meta.spec_idx")
    export.add_argument(
        "--provider",
        default=None,
        help="Filter replay_case by meta.provider (case-insensitive)",
    )
    export.add_argument("--run-dir", type=Path, default=None, help="Run dir (required for file resources)")
    export.add_argument("--run-id", default=None, help="Run id (select case dir within runs root)")
    export.add_argument("--case-dir", type=Path, default=None, help="Case dir (explicit path to case)")
    export.add_argument(
        "--allow-bad-json",
        action="store_true",
        help="Skip invalid JSON lines in events.jsonl",
    )
    export.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing bundles and resource copies",
    )
    export.add_argument("--all", action="store_true", help="Export all matching replay cases")
    export.add_argument("--case", help="Case id for auto-resolving events.jsonl")
    export.add_argument("--data", type=Path, help="Data directory containing .runs")
    export.add_argument("--tag", default=None, help="Run tag filter for auto-resolve")
    export.add_argument(
        "--runs-subdir",
        default=".runs/runs",
        help="Runs subdir relative to data dir (default: .runs/runs)",
    )
    export.add_argument(
        "--pick-run",
        default="latest_non_missed",
        help="Run selection strategy (default: latest_non_missed)",
    )
    export.add_argument(
        "--print-resolve",
        action="store_true",
        help="Print resolved run_dir/events.jsonl",
    )
    export.add_argument(
        "--debug",
        action="store_true",
        help="Print debug information about candidate runs",
    )
    export.add_argument(
        "--select",
        choices=["latest", "first", "last", "by-timestamp", "by-line"],
        default="latest",
        help="Selection policy when multiple replay_case entries match",
    )
    export.add_argument("--select-index", type=int, default=None, help="Select a case run (1-based)")
    export.add_argument("--list-matches", action="store_true", help="List case runs and exit")
    export.add_argument(
        "--replay-select-index",
        type=int,
        default=None,
        help="Select a specific replay_case match (1-based)",
    )
    export.add_argument(
        "--list-replay-matches",
        action="store_true",
        help="List replay_case matches and exit",
    )
    export.add_argument("--require-unique", action="store_true", help="Error if multiple matches exist")

    green = sub.add_parser("fixture-green", help="Promote known_bad case to fixed")
    green.add_argument("--case", type=Path, required=True, help="Path to known_bad case bundle")
    green.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="Fixture root")
    green.add_argument("--validate", action="store_true", help="Validate replay output")
    green.add_argument(
        "--overwrite-expected",
        action="store_true",
        help="Overwrite existing expected output",
    )
    green.add_argument("--dry-run", action="store_true", help="Print actions without changing files")

    rm_cmd = sub.add_parser("fixture-rm", help="Remove replay fixtures")
    rm_cmd.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="Fixture root")
    rm_cmd.add_argument(
        "--bucket",
        choices=["fixed", "known_bad", "all"],
        default="all",
        help="Fixture bucket",
    )
    rm_cmd.add_argument("--name", help="Fixture stem name")
    rm_cmd.add_argument("--pattern", help="Glob pattern for fixture stems or case bundles")
    rm_cmd.add_argument(
        "--scope",
        choices=["cases", "resources", "both"],
        default="both",
        help="What to remove",
    )
    rm_cmd.add_argument("--dry-run", action="store_true", help="Print actions without changing files")

    fix_cmd = sub.add_parser("fixture-fix", help="Rename fixture stem")
    fix_cmd.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="Fixture root")
    fix_cmd.add_argument("--bucket", choices=["fixed", "known_bad"], default="fixed")
    fix_cmd.add_argument("--name", required=True, help="Old fixture stem")
    fix_cmd.add_argument("--new-name", required=True, help="New fixture stem")
    fix_cmd.add_argument("--dry-run", action="store_true", help="Print actions without changing files")

    migrate_cmd = sub.add_parser("fixture-migrate", help="Normalize resource layout")
    migrate_cmd.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="Fixture root")
    migrate_cmd.add_argument(
        "--bucket",
        choices=["fixed", "known_bad", "all"],
        default="all",
        help="Fixture bucket",
    )
    migrate_cmd.add_argument("--dry-run", action="store_true", help="Print actions without changing files")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        if args.command == "export-case-bundle":
            debug_enabled = args.debug or bool(os.getenv("DEBUG"))
            if args.events:
                if args.case or args.data or args.tag or args.run_id:
                    raise ValueError("Do not combine --events with --case/--data/--tag/--run-id.")
                events_path = args.events
                run_dir = args.case_dir or args.run_dir
            else:
                if args.case_dir and args.run_id:
                    raise ValueError("Do not combine --case-dir with --run-id.")
                if args.case_dir or args.run_dir:
                    run_dir = args.case_dir or args.run_dir
                    selection_rule = "explicit CASE_DIR" if args.case_dir else "explicit RUN_DIR"
                elif args.run_id:
                    if not args.case or not args.data:
                        raise ValueError("--case and --data are required when --run-id is provided.")
                    run_dir = _resolve_case_dir_from_run_id(
                        data_dir=args.data,
                        runs_subdir=args.runs_subdir,
                        run_id=args.run_id,
                        case_id=args.case,
                    )
                    selection_rule = f"explicit RUN_ID={args.run_id}"
                else:
                    if not args.case or not args.data:
                        raise ValueError("--case and --data are required when --events is not provided.")
                    infos, stats = scan_case_runs(
                        case_id=args.case,
                        data_dir=args.data,
                        runs_subdir=args.runs_subdir,
                    )
                    if debug_enabled:
                        print("Debug: case run candidates (most recent first):")
                        print(format_case_run_debug(infos, limit=10))
                    candidates, stats = list_case_runs(
                        case_id=args.case,
                        data_dir=args.data,
                        tag=args.tag,
                        runs_subdir=args.runs_subdir,
                    )
                    if args.list_matches:
                        if not candidates:
                            raise LookupError(
                                _format_case_run_error(
                                    stats,
                                    case_id=args.case,
                                    tag=args.tag,
                                )
                            )
                        print(format_case_runs(candidates, limit=20))
                        return 0
                    selected = select_case_run(candidates, select_index=args.select_index)
                    run_dir = selected.case_dir
                    selection_rule = "latest with events"
                    if args.tag:
                        selection_rule = f"latest with events filtered by TAG={args.tag!r}"
                    events_path = selected.events_path
                if "events_path" not in locals():
                    events_resolution = find_events_file(run_dir)
                    if not events_resolution.events_path:
                        raise FileNotFoundError(
                            _format_events_error(
                                run_dir,
                                events_resolution,
                                selection_rule=selection_rule,
                            )
                        )
                    events_path = events_resolution.events_path
                if args.print_resolve:
                    print(f"Resolved run_dir: {run_dir}")
                    print(f"Resolved events.jsonl: {events_path}")
            if args.list_replay_matches:
                if not args.id:
                    raise ValueError("--id is required to list replay_case matches.")
                selections = find_replay_case_matches(
                    events_path,
                    replay_id=args.id,
                    spec_idx=args.spec_idx,
                    provider=args.provider,
                    allow_bad_json=args.allow_bad_json,
                )
                if not selections:
                    raise LookupError(f"No replay_case id={args.id!r} found in {events_path}")
                print(format_replay_case_matches(selections, limit=20))
                return 0
            allow_prompt = (
                sys.stdin.isatty()
                and args.replay_select_index is None
                and not args.require_unique
                and not args.list_replay_matches
            )
            if not args.id:
                raise ValueError("--id is required to export replay case bundles.")
            if args.all:
                export_replay_case_bundles(
                    events_path=events_path,
                    out_dir=args.out,
                    replay_id=args.id,
                    spec_idx=args.spec_idx,
                    provider=args.provider,
                    run_dir=run_dir,
                    allow_bad_json=args.allow_bad_json,
                    overwrite=args.overwrite,
                )
            else:
                export_replay_case_bundle(
                    events_path=events_path,
                    out_dir=args.out,
                    replay_id=args.id,
                    spec_idx=args.spec_idx,
                    provider=args.provider,
                    run_dir=run_dir,
                    allow_bad_json=args.allow_bad_json,
                    overwrite=args.overwrite,
                    selection_policy=args.select,
                    select_index=args.replay_select_index,
                    require_unique=args.require_unique,
                    allow_prompt=allow_prompt,
                    prompt_fn=input,
                )
            return 0
        if args.command == "fixture-green":
            fixture_green(
                case_path=args.case,
                out_root=args.root,
                validate=args.validate,
                overwrite_expected=args.overwrite_expected,
                dry_run=args.dry_run,
            )
            return 0
        if args.command == "fixture-rm":
            removed = fixture_rm(
                root=args.root,
                bucket=args.bucket,
                name=args.name,
                pattern=args.pattern,
                scope=args.scope,
                dry_run=args.dry_run,
            )
            print(f"Removed {removed} paths")
            return 0
        if args.command == "fixture-fix":
            fixture_fix(
                root=args.root,
                bucket=args.bucket,
                name=args.name,
                new_name=args.new_name,
                dry_run=args.dry_run,
            )
            return 0
        if args.command == "fixture-migrate":
            bundles_updated, files_moved = fixture_migrate(
                root=args.root,
                bucket=args.bucket,
                dry_run=args.dry_run,
            )
            print(f"Updated {bundles_updated} bundles; moved {files_moved} files")
            return 0
    except (ValueError, FileNotFoundError, LookupError, KeyError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover - unexpected
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1
    raise SystemExit(f"Unknown command: {args.command}")


def _resolve_case_dir_from_run_id(*, data_dir: Path, runs_subdir: str, run_id: str, case_id: str) -> Path:
    runs_root = data_dir / runs_subdir / run_id / "cases"
    if not runs_root.exists():
        raise FileNotFoundError(f"Run directory does not exist: {runs_root}")
    case_dirs = sorted(runs_root.glob(f"{case_id}_*"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not case_dirs:
        raise LookupError(
            "No case directories found under run.\n"
            f"run_id: {run_id}\n"
            f"case_id: {case_id}\n"
            f"runs_root: {runs_root}"
        )
    return case_dirs[0]


def _format_case_run_error(stats, *, case_id: str, tag: str | None) -> str:
    lines = [
        "No suitable case run found.",
        f"selection_rule: {'latest with events' if not tag else f'latest with events filtered by TAG={tag!r}'}",
        f"runs_root: {stats.runs_root}",
        f"case_id: {case_id}",
        f"inspected_runs: {stats.inspected_runs}",
        f"inspected_cases: {stats.inspected_cases}",
        f"missing_cases: {stats.missing_cases}",
        f"missing_events: {stats.missing_events}",
    ]
    if tag:
        lines.append(f"tag: {tag}")
        lines.append(f"tag_mismatches: {stats.tag_mismatches}")
        if stats.recent:
            lines.append("recent_cases:")
            for info in stats.recent[:5]:
                lines.append(
                    "  "
                    f"case_dir={info.case_dir} "
                    f"tag={info.tag!r} "
                    f"events={bool(info.events.events_path)}"
                )
        lines.append("Tip: verify TAG or pass RUN_ID/CASE_DIR/EVENTS.")
    else:
        lines.append("Tip: pass TAG/RUN_ID/CASE_DIR/EVENTS for a narrower selection.")
    return "\n".join(lines)


def _format_events_error(run_dir: Path, resolution, *, selection_rule: str) -> str:
    return "\n".join(
        [
            f"Selected case_dir: {run_dir}",
            f"selection_rule: {selection_rule}",
            format_events_search(run_dir, resolution),
            "Tip: rerun the case or pass EVENTS=... explicitly.",
        ]
    )


if __name__ == "__main__":
    sys.exit(main())
