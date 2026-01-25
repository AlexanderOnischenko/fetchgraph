from __future__ import annotations

import argparse
import sys
from pathlib import Path

from fetchgraph.replay.export import export_replay_case_bundle, export_replay_case_bundles


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetchgraph utilities")
    sub = parser.add_subparsers(dest="command", required=True)

    fixture = sub.add_parser("fixture", help="Export replay case bundle from events.jsonl")
    fixture.add_argument("--events", type=Path, required=True, help="Path to events.jsonl")
    fixture.add_argument("--run-dir", type=Path, default=None, help="Case run dir (needed for file resources)")
    fixture.add_argument("--id", default="plan_normalize.spec_v1", help="Replay case id to extract")
    fixture.add_argument("--spec-idx", type=int, default=None, help="Filter replay_case by meta.spec_idx")
    fixture.add_argument(
        "--provider",
        default=None,
        help="Filter replay_case by meta.provider (case-insensitive)",
    )
    fixture.add_argument(
        "--allow-bad-json",
        action="store_true",
        help="Skip invalid JSON lines in events.jsonl",
    )
    fixture.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing bundles and resource copies",
    )
    fixture.add_argument("--all", action="store_true", help="Export all matching replay cases")
    fixture.add_argument(
        "--out-dir",
        type=Path,
        default=Path("tests") / "fixtures" / "replay_cases",
        help="Output directory for replay case bundles",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.command == "fixture":
        if args.all:
            export_replay_case_bundles(
                events_path=args.events,
                out_dir=args.out_dir,
                replay_id=args.id,
                spec_idx=args.spec_idx,
                provider=args.provider,
                run_dir=args.run_dir,
                allow_bad_json=args.allow_bad_json,
                overwrite=args.overwrite,
            )
        else:
            export_replay_case_bundle(
                events_path=args.events,
                out_dir=args.out_dir,
                replay_id=args.id,
                spec_idx=args.spec_idx,
                provider=args.provider,
                run_dir=args.run_dir,
                allow_bad_json=args.allow_bad_json,
                overwrite=args.overwrite,
            )
        return 0
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
