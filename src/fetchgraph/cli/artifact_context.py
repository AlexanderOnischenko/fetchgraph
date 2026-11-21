"""CLI commands for artifact context engine."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from ..artifact_context import fetch as artifact_context_fetch
from ..artifact_context.models import ContextRequest, RetrievalPolicy

logger = logging.getLogger(__name__)


def artifact_context_command(argv: list[str] | None = None) -> int:
    """CLI command for artifact context retrieval.

    Usage:
        fetchgraph artifact-context --run-id <id> --query-family EXPLAIN_TARGET \\
            --anchors '{"target_id": "AS-017"}'

    Args:
        argv: Command line arguments.

    Returns:
        Exit code (0 for success, non-zero for error).
    """
    parser = argparse.ArgumentParser(
        description="Artifact context retrieval engine",
        prog="fetchgraph artifact-context",
    )

    parser.add_argument(
        "--run-id",
        type=str,
        required=True,
        help="Run ID containing artifacts",
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Run directory path (default: auto-detect from run-id)",
    )
    parser.add_argument(
        "--query-family",
        type=str,
        required=True,
        choices=[
            "EXPLAIN_TARGET",
            "EXPLAIN_DERIVATION",
            "CASE_DOSSIER",
            "VALIDATOR_DEFECT_PACK",
        ],
        help="Query family",
    )
    parser.add_argument(
        "--anchors",
        type=str,
        default="{}",
        help="JSON object with anchor values (e.g., '{\"target_id\": \"AS-017\"}')",
    )
    parser.add_argument(
        "--pipeline-id",
        type=str,
        default="archgraph_v1",
        help="Pipeline schema ID (default: archgraph_v1)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="deterministic",
        choices=["deterministic", "assisted", "investigator"],
        help="Retrieval mode",
    )
    parser.add_argument(
        "--max-llm-calls",
        type=int,
        default=None,
        help="Maximum LLM calls",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=None,
        help="Maximum investigation steps",
    )
    parser.add_argument(
        "--max-capsules",
        type=int,
        default=20,
        help="Maximum evidence capsules",
    )
    parser.add_argument(
        "--max-total-chars",
        type=int,
        default=30000,
        help="Maximum total characters",
    )
    parser.add_argument(
        "--max-hops-upstream",
        type=int,
        default=3,
        help="Maximum upstream hops",
    )
    parser.add_argument(
        "--max-hops-downstream",
        type=int,
        default=3,
        help="Maximum downstream hops",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--format",
        type=str,
        default="json",
        choices=["json", "text"],
        help="Output format",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args(argv)

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )

    try:
        # Parse anchors
        try:
            anchors = json.loads(args.anchors)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in --anchors: {e}", file=sys.stderr)
            return 1

        if not isinstance(anchors, dict):
            print("Error: --anchors must be a JSON object", file=sys.stderr)
            return 1

        # Build policy
        policy_kwargs: Dict[str, Any] = {
            "mode": args.mode,
            "max_capsules": args.max_capsules,
            "max_total_chars": args.max_total_chars,
            "max_hops_upstream": args.max_hops_upstream,
            "max_hops_downstream": args.max_hops_downstream,
        }
        if args.max_llm_calls is not None:
            policy_kwargs["max_llm_calls"] = args.max_llm_calls
        if args.max_steps is not None:
            policy_kwargs["max_steps"] = args.max_steps

        policy = RetrievalPolicy(**policy_kwargs)

        # Build request
        request = ContextRequest(
            pipeline_id=args.pipeline_id,
            query_family=args.query_family,
            run_id=args.run_id,
            anchors=anchors,
            policy=policy,
        )

        # Determine run directory
        run_dir = args.run_dir
        if run_dir is None:
            # Auto-detect: look for .runs/<run-id> or runs/<run-id>
            run_dir = _find_run_dir(args.run_id)
            if run_dir is None:
                print(
                    f"Error: Could not find run directory for run-id '{args.run_id}'",
                    file=sys.stderr,
                )
                return 1

        logger.info("Using run directory: %s", run_dir)

        # Execute fetch
        result = artifact_context_fetch(
            request=request,
            run_dir=run_dir,
        )

        # Output result
        if args.format == "json":
            output = result.model_dump_json(indent=2)
        else:
            output = _format_as_text(result)

        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(output)
            logger.info("Output written to %s", args.output)
        else:
            print(output)

        return 0

    except Exception as e:
        logger.exception("Error: %s", e)
        print(f"Error: {e}", file=sys.stderr)
        return 1


def _find_run_dir(run_id: str) -> Optional[Path]:
    """Find run directory by run ID.

    Searches common locations:
    - .runs/<run-id>
    - runs/<run-id>
    - <run-id> (if it's a path)
    """
    # Check if run_id is already a path
    run_path = Path(run_id)
    if run_path.exists() and run_path.is_dir():
        return run_path

    # Check relative to current directory
    candidates = [
        Path(".runs") / run_id,
        Path("runs") / run_id,
        Path(run_id),
    ]

    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate

    # Check relative to data directory if available
    data_dir = Path("_demo_data")
    if data_dir.exists():
        candidate = data_dir / run_id
        if candidate.exists() and candidate.is_dir():
            return candidate

    return None


def _format_as_text(result: Any) -> str:
    """Format result as human-readable text."""
    lines = []

    if hasattr(result, "model_dump"):
        data = result.model_dump(mode="json")
    else:
        data = result

    # Format based on result type
    if "explain_target" in data:
        lines.append("=== EXPLANATION ===")
        lines.append(f"Target: {json.dumps(data.get('explain_target', {}), indent=2)}")
        lines.append(f"\nReasoning: {data.get('reasoning_summary', '')}")
        lines.append(f"\nEvidence count: {len(data.get('selected_evidence', []))}")
        lines.append(f"Provenance chain: {len(data.get('provenance_chain', []))} artifacts")

    elif "target" in data:
        lines.append("=== EVIDENCE PACK ===")
        lines.append(f"Target: {json.dumps(data.get('target', {}), indent=2)}")
        lines.append(f"\nPrimary evidence: {len(data.get('primary_evidence', []))} items")
        lines.append(f"Supporting evidence: {len(data.get('supporting_evidence', []))} items")

    elif "case_id" in data:
        lines.append("=== CASE DOSSIER ===")
        lines.append(f"Case ID: {data.get('case_id')}")
        lines.append(f"\nSymptom capsules: {len(data.get('symptom_capsules', []))}")
        lines.append(f"Upstream capsules: {len(data.get('upstream_capsules', []))}")

    elif "defect_id" in data:
        lines.append("=== VALIDATOR DEFECT ===")
        lines.append(f"Defect ID: {data.get('defect_id')}")
        lines.append(f"Validator: {data.get('validator_name')}")
        lines.append(f"\nDescription: {data.get('defect_description')}")
        lines.append(f"Evidence: {len(data.get('evidence', []))} items")

    else:
        lines.append(json.dumps(data, indent=2))

    # Add trace summary
    if "investigation_trace" in data and data["investigation_trace"]:
        lines.append(f"\n=== INVESTIGATION TRACE ({len(data['investigation_trace'])} steps) ===")
        for step in data["investigation_trace"]:
            lines.append(f"  Step {step.get('step')}: {step.get('action')}")

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """Main entry point for artifact-context CLI."""
    return artifact_context_command(argv)


if __name__ == "__main__":
    sys.exit(main())
