"""Artifact Context Engine - main entry point.

This module provides the public API for the artifact context engine:
- fetch(): Main entry point for retrieving artifact context
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Optional, Union

from .models import (
    CaseDossier,
    ContextRequest,
    EvidencePack,
    ExplainPack,
    ValidatorDefectPack,
)
from ..investigation.orchestrator import InvestigationOrchestrator, create_orchestrator

logger = logging.getLogger(__name__)


def fetch(
    request: ContextRequest,
    run_dir: Optional[Path] = None,
    run_id: Optional[str] = None,
    llm_invoke: Optional[Callable[[str, str], str]] = None,
) -> Union[EvidencePack, ExplainPack, CaseDossier, ValidatorDefectPack]:
    """Fetch artifact context based on a request.

    This is the main entry point for the artifact context engine. It executes
    an investigation to gather evidence, explain derivations, or assemble case
    dossiers.

    Args:
        request: Context request specifying what to investigate.
        run_dir: Directory containing run artifacts. If not provided,
            attempts to auto-detect from run_id.
        run_id: Run identifier. Used if run_dir is not provided.
        llm_invoke: Optional LLM invocation function for assisted modes.
            Signature: llm_invoke(prompt: str, sender: str) -> str

    Returns:
        Evidence package based on query family:
        - EvidencePack for general evidence retrieval
        - ExplainPack for explanation queries
        - CaseDossier for case investigations
        - ValidatorDefectPack for validator defects

    Raises:
        ValueError: If request is invalid or required artifacts are missing.
        FileNotFoundError: If run directory does not exist.

    Example:
        >>> from fetchgraph.artifact_context import fetch, ContextRequest, RetrievalPolicy
        >>> request = ContextRequest(
        ...     query_family="EXPLAIN_TARGET",
        ...     run_id="run-123",
        ...     anchors={"target_id": "AS-017"},
        ...     policy=RetrievalPolicy(mode="assisted"),
        ... )
        >>> result = fetch(request, run_dir=Path("path/to/run"))
        >>> print(result.explain_target)
    """
    # Resolve run directory
    if run_dir is None:
        run_dir = _resolve_run_dir(run_id or request.run_id)
        if run_dir is None:
            raise FileNotFoundError(
                f"Run directory not found for run_id='{request.run_id}'. "
                "Please provide run_dir explicitly."
            )

    # Resolve run_id
    if run_id is None:
        run_id = request.run_id or run_dir.name

    logger.info(
        "Artifact context engine fetch: query_family=%s run_id=%s run_dir=%s",
        request.query_family,
        run_id,
        run_dir,
    )

    # Create orchestrator and execute
    orchestrator = create_orchestrator(
        run_dir=run_dir,
        run_id=run_id,
        llm_invoke=llm_invoke,
    )

    return orchestrator.fetch(request)


def _resolve_run_dir(run_id: str) -> Optional[Path]:
    """Attempt to resolve run directory from run ID.

    Searches common locations for run directories.
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

    return None


# Convenience functions for common query patterns
def explain_target(
    target_id: str,
    run_dir: Path,
    run_id: Optional[str] = None,
    mode: str = "assisted",
    max_hops_upstream: int = 3,
    llm_invoke: Optional[Callable[[str, str], str]] = None,
) -> ExplainPack:
    """Explain why a specific target is present in results.

    Convenience wrapper around fetch() for EXPLAIN_TARGET queries.

    Args:
        target_id: Target identifier to explain.
        run_dir: Directory containing run artifacts.
        run_id: Optional run identifier.
        mode: Retrieval mode.
        max_hops_upstream: Maximum upstream hops to trace.
        llm_invoke: Optional LLM invocation function.

    Returns:
        ExplainPack with explanation and evidence.
    """
    request = ContextRequest(
        query_family="EXPLAIN_TARGET",
        run_id=run_id or run_dir.name,
        anchors={"target_id": target_id},
        policy={
            "mode": mode,
            "max_hops_upstream": max_hops_upstream,
        },
    )
    return fetch(request, run_dir=run_dir, run_id=run_id, llm_invoke=llm_invoke)  # type: ignore[return-value]


def explain_derivation(
    source_id: str,
    target_id: str,
    run_dir: Path,
    run_id: Optional[str] = None,
    mode: str = "assisted",
    max_hops: int = 5,
    llm_invoke: Optional[Callable[[str, str], str]] = None,
) -> ExplainPack:
    """Explain how artifacts derive from each other.

    Convenience wrapper around fetch() for EXPLAIN_DERIVATION queries.

    Args:
        source_id: Source artifact identifier.
        target_id: Target artifact identifier.
        run_dir: Directory containing run artifacts.
        run_id: Optional run identifier.
        mode: Retrieval mode.
        max_hops: Maximum hops to trace.
        llm_invoke: Optional LLM invocation function.

    Returns:
        ExplainPack with derivation explanation.
    """
    request = ContextRequest(
        query_family="EXPLAIN_DERIVATION",
        run_id=run_id or run_dir.name,
        anchors={"source_id": source_id, "target_id": target_id},
        policy={
            "mode": mode,
            "max_hops_upstream": max_hops,
            "max_hops_downstream": max_hops,
        },
    )
    return fetch(request, run_dir=run_dir, run_id=run_id, llm_invoke=llm_invoke)  # type: ignore[return-value]


def case_dossier(
    case_id: str,
    run_dir: Path,
    run_id: Optional[str] = None,
    mode: str = "investigator",
    include_upstream: bool = True,
    max_depth: int = 3,
    llm_invoke: Optional[Callable[[str, str], str]] = None,
) -> CaseDossier:
    """Assemble complete evidence package for a case.

    Convenience wrapper around fetch() for CASE_DOSSIER queries.

    Args:
        case_id: Case identifier.
        run_dir: Directory containing run artifacts.
        run_id: Optional run identifier.
        mode: Retrieval mode.
        include_upstream: Whether to include upstream evidence.
        max_depth: Maximum depth for upstream traversal.
        llm_invoke: Optional LLM invocation function.

    Returns:
        CaseDossier with complete evidence package.
    """
    request = ContextRequest(
        query_family="CASE_DOSSIER",
        run_id=run_id or run_dir.name,
        anchors={
            "case_id": case_id,
            "include_upstream": include_upstream,
            "max_depth": max_depth,
        },
        policy={"mode": mode},
    )
    return fetch(request, run_dir=run_dir, run_id=run_id, llm_invoke=llm_invoke)  # type: ignore[return-value]


def validator_defect_pack(
    defect_id: str,
    validator_name: str,
    run_dir: Path,
    run_id: Optional[str] = None,
    mode: str = "assisted",
    llm_invoke: Optional[Callable[[str, str], str]] = None,
) -> ValidatorDefectPack:
    """Prepare evidence package for a validator defect.

    Convenience wrapper around fetch() for VALIDATOR_DEFECT_PACK queries.

    Args:
        defect_id: Defect identifier.
        validator_name: Name of the validator that found the defect.
        run_dir: Directory containing run artifacts.
        run_id: Optional run identifier.
        mode: Retrieval mode.
        llm_invoke: Optional LLM invocation function.

    Returns:
        ValidatorDefectPack with defect evidence.
    """
    request = ContextRequest(
        query_family="VALIDATOR_DEFECT_PACK",
        run_id=run_id or run_dir.name,
        anchors={"defect_id": defect_id, "validator_name": validator_name},
        policy={"mode": mode},
    )
    return fetch(request, run_dir=run_dir, run_id=run_id, llm_invoke=llm_invoke)  # type: ignore[return-value]


__all__ = [
    "fetch",
    "explain_target",
    "explain_derivation",
    "case_dossier",
    "validator_defect_pack",
]
