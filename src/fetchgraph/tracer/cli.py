from __future__ import annotations

import argparse
import sys
from pathlib import Path

from fetchgraph.tracer.export import export_replay_case_bundle, export_replay_case_bundles


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetchgraph tracer utilities")
    sub = parser.add_subparsers(dest="command", required=True)

    export = sub.add_parser("export-case-bundle", help="Export replay case bundle from events.jsonl")
    export.add_argument("--events", type=Path, required=True, help="Path to events.jsonl")
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
    export.add_argument("--all", action="store_true", help="Export all matching replay cases")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.command == "export-case-bundle":
        if args.all:
            export_replay_case_bundles(
                events_path=args.events,
                out_dir=args.out,
                replay_id=args.id,
                spec_idx=args.spec_idx,
                provider=args.provider,
                run_dir=args.run_dir,
                allow_bad_json=args.allow_bad_json,
            )
        else:
            export_replay_case_bundle(
                events_path=args.events,
                out_dir=args.out,
                replay_id=args.id,
                spec_idx=args.spec_idx,
                provider=args.provider,
                run_dir=args.run_dir,
                allow_bad_json=args.allow_bad_json,
            )
        return 0
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
