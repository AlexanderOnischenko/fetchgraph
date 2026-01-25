from __future__ import annotations

import argparse
import sys
from pathlib import Path

from fetchgraph.tracer.auto_resolve import resolve_case_events
from fetchgraph.tracer.export import export_replay_case_bundle, export_replay_case_bundles
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
    export.add_argument("--id", required=True, help="Replay case id to export")
    export.add_argument("--spec-idx", type=int, default=None, help="Filter replay_case by meta.spec_idx")
    export.add_argument(
        "--provider",
        default=None,
        help="Filter replay_case by meta.provider (case-insensitive)",
    )
    export.add_argument("--run-dir", type=Path, default=None, help="Run dir (required for file resources)")
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
            if args.events:
                if args.case or args.data or args.tag:
                    raise ValueError("Do not combine --events with --case/--data/--tag.")
                events_path = args.events
                run_dir = args.run_dir
            else:
                if args.run_dir:
                    raise ValueError("Do not combine --run-dir with auto-resolve.")
                if not args.case or not args.data:
                    raise ValueError("--case and --data are required when --events is not provided.")
                resolution = resolve_case_events(
                    case_id=args.case,
                    data_dir=args.data,
                    tag=args.tag,
                    runs_subdir=args.runs_subdir,
                    pick_run=args.pick_run,
                )
                events_path = resolution.events_path
                run_dir = resolution.case_dir
                if args.print_resolve:
                    print(f"Resolved run_dir: {resolution.case_dir}")
                    print(f"Resolved events.jsonl: {resolution.events_path}")
                    if resolution.tag:
                        print(f"Resolved tag: {resolution.tag}")
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


if __name__ == "__main__":
    sys.exit(main())
