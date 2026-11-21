"""Investigation Orchestrator - orchestrates artifact investigations.

This module provides:
- InvestigationOrchestrator: main orchestrator for running investigations
- Action execution and LLM integration
- Evidence package assembly
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

from ..artifact_context.models import (
    ActionSpec,
    CaseDossier,
    ContextRequest,
    EvidenceCapsule,
    EvidencePack,
    ExplainPack,
    InvestigationSession,
    RetrievalPolicy,
    STANDARD_ACTIONS,
    ValidatorDefectPack,
)
from ..artifact_graph.provider import ArtifactGraphProvider, create_artifact_graph_provider
from ..pipeline_schema.loader import get_schema

logger = logging.getLogger(__name__)


class InvestigationOrchestrator:
    """Orchestrator for artifact investigations.

    Manages the investigation lifecycle:
    - Initializes session with request and policy
    - Executes actions within limits
    - Optionally uses LLM for decision making
    - Assembles output packages
    """

    def __init__(
        self,
        graph_provider: ArtifactGraphProvider,
        llm_invoke: Optional[Callable[[str, str], str]] = None,
    ):
        """Initialize the orchestrator.

        Args:
            graph_provider: Provider for accessing artifacts.
            llm_invoke: Optional LLM invocation function.
        """
        self.graph_provider = graph_provider
        self.llm_invoke = llm_invoke
        self._session: Optional[InvestigationSession] = None

    def fetch(
        self,
        request: ContextRequest,
    ) -> Union[EvidencePack, ExplainPack, CaseDossier, ValidatorDefectPack]:
        """Execute an investigation and return results.

        Args:
            request: Context request specifying what to investigate.

        Returns:
            Evidence package based on query family.
        """
        # Initialize session
        session_id = str(uuid.uuid4())[:8]
        self._session = InvestigationSession(
            session_id=session_id,
            request=request,
        )

        # Load schema
        schema = get_schema(request.pipeline_id)

        # Get query template
        template = schema.get_query_template(request.query_family)
        if template is None:
            raise ValueError(f"Unknown query family: {request.query_family}")

        # Validate anchors
        self._validate_anchors(request, template)

        # Apply default policy from template
        if template.default_policy:
            policy_data = request.policy.model_dump()
            policy_data.update(template.default_policy)
            request = request.model_copy(
                update={"policy": RetrievalPolicy(**policy_data)}
            )
            self._session.request = request

        logger.info(
            "Starting investigation session=%s query_family=%s mode=%s",
            session_id,
            request.query_family,
            request.policy.mode,
        )

        # Execute investigation based on query family
        try:
            if request.query_family == "EXPLAIN_TARGET":
                result = self._explain_target(request)
            elif request.query_family == "EXPLAIN_DERIVATION":
                result = self._explain_derivation(request)
            elif request.query_family == "CASE_DOSSIER":
                result = self._case_dossier(request)
            elif request.query_family == "VALIDATOR_DEFECT_PACK":
                result = self._validator_defect_pack(request)
            else:
                raise ValueError(f"Unknown query family: {request.query_family}")

            logger.info(
                "Investigation completed session=%s total_chars=%d llm_calls=%d",
                session_id,
                self._session.total_chars,
                self._session.llm_calls_count,
            )

            return result

        except Exception as e:
            logger.exception("Investigation failed session=%s: %s", session_id, e)
            raise

    def _validate_anchors(
        self, request: ContextRequest, template: Any
    ) -> None:
        """Validate that required anchors are present."""
        for anchor in template.required_anchors:
            if anchor not in request.anchors:
                raise ValueError(
                    f"Missing required anchor: {anchor}. "
                    f"Required anchors: {template.required_anchors}"
                )

    def _explain_target(
        self, request: ContextRequest
    ) -> ExplainPack:
        """Explain why a target is present in results."""
        assert self._session is not None

        target_id = request.anchors.get("target_id")
        if not target_id:
            raise ValueError("target_id anchor is required")

        # Step 1: Find the target artifact
        self._session.add_step(
            action="SEARCH_ARTIFACTS",
            action_params={"query": str(target_id)},
            result={"status": "searching"},
        )

        artifacts = self.graph_provider.search_artifacts(str(target_id))
        if not artifacts:
            return ExplainPack(
                explain_target={"target_id": target_id},
                reasoning_summary=f"Target artifact '{target_id}' not found in run {request.run_id}",
                unresolved_gaps=[f"Target artifact '{target_id}' not found"],
                boundary={"missing": [target_id]},
                investigation_trace=self._get_trace(),
            )

        target_artifact = artifacts[0]
        target_id = target_artifact["id"]

        self._session.add_step(
            action="SEARCH_ARTIFACTS",
            action_params={"query": str(target_id)},
            result={"found": target_id, "total": len(artifacts)},
        )

        # Step 2: Expand upstream to find evidence
        self._session.add_step(
            action="EXPAND_NEIGHBORHOOD",
            action_params={
                "artifact_id": target_id,
                "direction": "upstream",
                "max_hops": request.policy.max_hops_upstream,
            },
            result={"status": "expanding"},
        )

        neighbors = self.graph_provider.expand_neighborhood(
            target_id,
            max_hops=request.policy.max_hops_upstream,
            direction="upstream",
        )

        self._session.add_step(
            action="EXPAND_NEIGHBORHOOD",
            action_params={
                "artifact_id": target_id,
                "direction": "upstream",
                "max_hops": request.policy.max_hops_upstream,
            },
            result={"found": len(neighbors)},
        )

        # Step 3: Collect evidence
        evidence: List[EvidenceCapsule] = []
        for neighbor in neighbors[: request.policy.max_capsules]:
            if self._session.total_chars >= request.policy.max_total_chars:
                break

            artifact_id = neighbor["id"]
            artifact = self.graph_provider.get_artifact(artifact_id)
            if artifact:
                fragment = self.graph_provider.get_fragment(artifact_id)
                capsule = EvidenceCapsule(
                    artifact_type=neighbor.get("type", "unknown"),
                    artifact_id=artifact_id,
                    fragment=fragment,
                    provenance=neighbor,
                )
                evidence.append(capsule)
                self._session.record_chars(len(fragment))

        # Step 4: Resolve provenance
        provenance_chain = self.graph_provider.resolve_provenance(target_id)

        # Build reasoning summary
        reasoning = self._build_reasoning_summary(
            target_id=target_id,
            evidence_count=len(evidence),
            provenance_length=len(provenance_chain),
            neighbors_count=len(neighbors),
        )

        return ExplainPack(
            explain_target={"target_id": target_id, "artifact": target_artifact},
            selected_evidence=evidence,
            reasoning_summary=reasoning,
            provenance_chain=provenance_chain,
            investigation_trace=self._get_trace(),
        )

    def _explain_derivation(
        self, request: ContextRequest
    ) -> ExplainPack:
        """Explain how artifacts derive from each other."""
        assert self._session is not None

        source_id = request.anchors.get("source_id")
        target_id = request.anchors.get("target_id")

        if not source_id or not target_id:
            raise ValueError("source_id and target_id anchors are required")

        # Find path from source to target
        self._session.add_step(
            action="EXPAND_NEIGHBORHOOD",
            action_params={
                "artifact_id": source_id,
                "direction": "downstream",
                "max_hops": request.policy.max_hops_downstream,
            },
            result={"status": "expanding from source"},
        )

        downstream = self.graph_provider.expand_neighborhood(
            source_id,
            max_hops=request.policy.max_hops_downstream,
            direction="downstream",
        )

        # Check if target is in downstream
        found_path = any(d["id"] == target_id for d in downstream)

        self._session.add_step(
            action="EXPAND_NEIGHBORHOOD",
            action_params={
                "artifact_id": source_id,
                "direction": "downstream",
                "max_hops": request.policy.max_hops_downstream,
            },
            result={
                "found_target": found_path,
                "downstream_count": len(downstream),
            },
        )

        # Collect evidence along the path
        evidence: List[EvidenceCapsule] = []
        all_ids = [source_id] + [d["id"] for d in downstream]

        for artifact_id in all_ids[: request.policy.max_capsules]:
            if self._session.total_chars >= request.policy.max_total_chars:
                break

            artifact = self.graph_provider.get_artifact(artifact_id)
            if artifact:
                fragment = self.graph_provider.get_fragment(artifact_id)
                capsule = EvidenceCapsule(
                    artifact_type="unknown",
                    artifact_id=artifact_id,
                    fragment=fragment,
                )
                evidence.append(capsule)
                self._session.record_chars(len(fragment))

        reasoning = (
            f"Found derivation path from '{source_id}' to '{target_id}': {found_path}. "
            f"Collected {len(evidence)} artifacts along the path."
        )

        return ExplainPack(
            explain_target={"source_id": source_id, "target_id": target_id},
            selected_evidence=evidence,
            reasoning_summary=reasoning,
            investigation_trace=self._get_trace(),
        )

    def _case_dossier(self, request: ContextRequest) -> CaseDossier:
        """Assemble complete evidence package for a case."""
        assert self._session is not None

        case_id = request.anchors.get("case_id")
        if not case_id:
            raise ValueError("case_id anchor is required")

        include_upstream = request.anchors.get("include_upstream", True)
        max_depth = request.anchors.get("max_depth", 3)

        # Search for case-related artifacts
        self._session.add_step(
            action="SEARCH_ARTIFACTS",
            action_params={"query": case_id},
            result={"status": "searching for case"},
        )

        case_artifacts = self.graph_provider.search_artifacts(case_id)

        self._session.add_step(
            action="SEARCH_ARTIFACTS",
            action_params={"query": case_id},
            result={"found": len(case_artifacts)},
        )

        # Separate symptom and upstream capsules
        symptom_capsules: List[EvidenceCapsule] = []
        upstream_capsules: List[EvidenceCapsule] = []

        for artifact_meta in case_artifacts[: request.policy.max_capsules]:
            if self._session.total_chars >= request.policy.max_total_chars:
                break

            artifact_id = artifact_meta["id"]
            artifact = self.graph_provider.get_artifact(artifact_id)
            if artifact:
                fragment = self.graph_provider.get_fragment(artifact_id)
                capsule = EvidenceCapsule(
                    artifact_type=artifact_meta.get("type", "unknown"),
                    artifact_id=artifact_id,
                    fragment=fragment,
                    provenance=artifact_meta,
                )

                # Classify as symptom or upstream
                if artifact_meta.get("type") in ("synthesis", "evidence"):
                    symptom_capsules.append(capsule)
                elif include_upstream:
                    upstream_capsules.append(capsule)

                self._session.record_chars(len(fragment))

        # Expand upstream if requested
        if include_upstream and case_artifacts:
            for artifact_meta in case_artifacts[:3]:
                if not self._session.can_continue():
                    break

                artifact_id = artifact_meta["id"]
                try:
                    neighbors = self.graph_provider.expand_neighborhood(
                        artifact_id,
                        max_hops=max_depth,
                        direction="upstream",
                    )

                    for neighbor in neighbors[:5]:
                        if self._session.total_chars >= request.policy.max_total_chars:
                            break

                        neighbor_id = neighbor["id"]
                        neighbor_artifact = self.graph_provider.get_artifact(
                            neighbor_id
                        )
                        if neighbor_artifact:
                            fragment = self.graph_provider.get_fragment(neighbor_id)
                            capsule = EvidenceCapsule(
                                artifact_type=neighbor.get("type", "unknown"),
                                artifact_id=neighbor_id,
                                fragment=fragment,
                                provenance=neighbor,
                            )
                            upstream_capsules.append(capsule)
                            self._session.record_chars(len(fragment))

                except Exception as e:
                    logger.warning(
                        "Failed to expand neighborhood for %s: %s", artifact_id, e
                    )

        return CaseDossier(
            case_id=case_id,
            symptom_capsules=symptom_capsules,
            upstream_capsules=upstream_capsules,
            trace=self._get_trace(),
        )

    def _validator_defect_pack(
        self, request: ContextRequest
    ) -> ValidatorDefectPack:
        """Prepare evidence package for a validator defect."""
        assert self._session is not None

        defect_id = request.anchors.get("defect_id")
        validator_name = request.anchors.get("validator_name")

        if not defect_id or not validator_name:
            raise ValueError("defect_id and validator_name anchors are required")

        # Search for defect-related artifacts
        self._session.add_step(
            action="SEARCH_ARTIFACTS",
            action_params={"query": defect_id, "artifact_type": "evidence"},
            result={"status": "searching for defect evidence"},
        )

        defect_artifacts = self.graph_provider.search_artifacts(
            defect_id, artifact_type="evidence"
        )

        self._session.add_step(
            action="SEARCH_ARTIFACTS",
            action_params={"query": defect_id, "artifact_type": "evidence"},
            result={"found": len(defect_artifacts)},
        )

        # Collect evidence
        evidence: List[EvidenceCapsule] = []
        for artifact_meta in defect_artifacts[: request.policy.max_capsules]:
            if self._session.total_chars >= request.policy.max_total_chars:
                break

            artifact_id = artifact_meta["id"]
            artifact = self.graph_provider.get_artifact(artifact_id)
            if artifact:
                fragment = self.graph_provider.get_fragment(artifact_id)
                capsule = EvidenceCapsule(
                    artifact_type="evidence",
                    artifact_id=artifact_id,
                    fragment=fragment,
                    provenance=artifact_meta,
                )
                evidence.append(capsule)
                self._session.record_chars(len(fragment))

        defect_description = f"Defect '{defect_id}' found by validator '{validator_name}'. "
        defect_description += f"Collected {len(evidence)} evidence artifacts."

        return ValidatorDefectPack(
            defect_id=defect_id,
            validator_name=validator_name,
            defect_description=defect_description,
            evidence=evidence,
            investigation_trace=self._get_trace(),
        )

    def _build_reasoning_summary(
        self,
        target_id: str,
        evidence_count: int,
        provenance_length: int,
        neighbors_count: int,
    ) -> str:
        """Build a reasoning summary for an explanation."""
        return (
            f"Target '{target_id}' was analyzed by examining {neighbors_count} upstream artifacts. "
            f"Collected {evidence_count} evidence capsules. "
            f"Provenance chain contains {provenance_length} artifacts. "
            f"The target appears to be derived from the evidence through the pipeline."
        )

    def _get_trace(self) -> List[Dict[str, Any]]:
        """Get the investigation trace."""
        assert self._session is not None
        return [
            {
                "step": step.step_number,
                "action": step.action,
                "params": step.action_params,
                "result": step.result,
                "llm_call": step.llm_call,
            }
            for step in self._session.steps
        ]


def create_orchestrator(
    run_dir: Path,
    run_id: Optional[str] = None,
    llm_invoke: Optional[Callable[[str, str], str]] = None,
) -> InvestigationOrchestrator:
    """Create an investigation orchestrator.

    Args:
        run_dir: Directory containing run artifacts.
        run_id: Optional run identifier.
        llm_invoke: Optional LLM invocation function.

    Returns:
        Configured orchestrator instance.
    """
    graph_provider = create_artifact_graph_provider(run_dir, run_id)
    return InvestigationOrchestrator(graph_provider, llm_invoke)


__all__ = [
    "InvestigationOrchestrator",
    "create_orchestrator",
]
