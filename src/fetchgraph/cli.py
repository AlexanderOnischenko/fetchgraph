from __future__ import annotations

import argparse
import sys
from pathlib import Path

from fetchgraph.replay.export import export_replay_fixture, export_replay_fixtures


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetchgraph utilities")
    sub = parser.add_subparsers(dest="command", required=True)

    fixture = sub.add_parser("fixture", help="Export replay fixture from events.jsonl")
    fixture.add_argument("--events", type=Path, required=True, help="Path to events.jsonl")
    fixture.add_argument("--run-dir", type=Path, default=None, help="Case run dir (needed for --with-requires)")
    fixture.add_argument("--id", default="plan_normalize.spec_v1", help="Replay point id to extract")
    fixture.add_argument("--spec-idx", type=int, default=None, help="Filter replay_point by meta.spec_idx")
    fixture.add_argument(
        "--provider",
        default=None,
        help="Filter replay_point by meta.provider (case-insensitive)",
    )
    fixture.add_argument("--with-requires", action="store_true", help="Export replay bundle with dependencies")
    fixture.add_argument("--all", action="store_true", help="Export all matching replay points")
    fixture.add_argument(
        "--out-dir",
        type=Path,
        default=Path("tests") / "fixtures" / "replay_points",
        help="Output directory for replay fixtures",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.command == "fixture":
        if args.all:
            export_replay_fixtures(
                events_path=args.events,
                run_dir=args.run_dir,
                out_dir=args.out_dir,
                replay_id=args.id,
                spec_idx=args.spec_idx,
                provider=args.provider,
                with_requires=args.with_requires,
            )
        else:
            export_replay_fixture(
                events_path=args.events,
                run_dir=args.run_dir,
                out_dir=args.out_dir,
                replay_id=args.id,
                spec_idx=args.spec_idx,
                provider=args.provider,
                with_requires=args.with_requires,
            )
        return 0
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
