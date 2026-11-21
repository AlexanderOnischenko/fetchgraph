"""Data models for the Artifact Context Engine.

This module defines the core data structures used throughout the artifact context engine:
- RetrievalPolicy: constraints and limits for retrieval operations
- ContextRequest: input request specification
- EvidencePack, ExplainPack, CaseDossier: output packages
- InvestigationSession: tracking state during investigation
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, field_validator, model_validator


# =============================================================================
# Retrieval Policy
# =============================================================================
class RetrievalPolicy(BaseModel):
    """Policy controlling retrieval behavior and limits.

    Attributes:
        mode: Retrieval mode - 'deterministic', 'assisted', or 'investigator'.
        max_llm_calls: Maximum number of LLM calls allowed.
        max_steps: Maximum investigation steps.
        max_capsules: Maximum number of capsules in output.
        max_quotes: Maximum number of quotes in output.
        max_total_chars: Maximum total characters across all fragments.
        max_hops_upstream: Maximum upstream hops in graph traversal.
        max_hops_downstream: Maximum downstream hops in graph traversal.
        require_traceability: Whether each fragment must be tied to provenance.
    """

    mode: Literal["deterministic", "assisted", "investigator"] = "deterministic"
    max_llm_calls: int = 1
    max_steps: int = 1
    max_capsules: int = 20
    max_quotes: int = 20
    max_total_chars: int = 30000
    max_hops_upstream: int = 3
    max_hops_downstream: int = 3
    require_traceability: bool = True

    @model_validator(mode="after")
    def enforce_mode_limits(self) -> "RetrievalPolicy":
        """Enforce mode-specific limits."""
        if self.mode == "deterministic":
            self.max_llm_calls = min(self.max_llm_calls, 1)
        elif self.mode == "assisted":
            self.max_llm_calls = min(self.max_llm_calls, 3)
            self.max_steps = max(self.max_steps, 1)
        elif self.mode == "investigator":
            self.max_llm_calls = min(self.max_llm_calls, 6)
            self.max_steps = max(self.max_steps, 5)
        return self


# =============================================================================
# Context Request
# =============================================================================
class ContextRequest(BaseModel):
    """Request for artifact context retrieval.

    Attributes:
        pipeline_id: Identifier of the pipeline schema to use.
        query_family: Type of query - 'EXPLAIN_TARGET', 'EXPLAIN_DERIVATION',
            'CASE_DOSSIER', or 'VALIDATOR_DEFECT_PACK'.
        run_id: Identifier of the run containing artifacts.
        anchors: Seed objects or identifiers for starting retrieval.
        options: Additional options for the query.
        policy: Retrieval policy controlling behavior.
    """

    pipeline_id: str = "archgraph_v1"
    query_family: Literal[
        "EXPLAIN_TARGET",
        "EXPLAIN_DERIVATION",
        "CASE_DOSSIER",
        "VALIDATOR_DEFECT_PACK",
    ]
    run_id: str
    anchors: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] = Field(default_factory=dict)
    policy: RetrievalPolicy = Field(default_factory=RetrievalPolicy)

    model_config = {"extra": "forbid"}


# =============================================================================
# Evidence Capsule
# =============================================================================
class EvidenceCapsule(BaseModel):
    """A single piece of evidence with metadata.

    Attributes:
        artifact_type: Type of the artifact (e.g., 'plan', 'evidence', 'synthesis').
        artifact_id: Identifier of the source artifact.
        fragment: The actual content/fragment from the artifact.
        fragment_path: Path within the artifact (e.g., JSON path).
        provenance: Provenance information (run_id, step, etc.).
        relevance_score: Optional relevance score (0-1).
        quotes: Optional quotes from the fragment.
    """

    artifact_type: str
    artifact_id: str
    fragment: str
    fragment_path: Optional[str] = None
    provenance: Optional[Dict[str, Any]] = None
    relevance_score: Optional[float] = None
    quotes: List[str] = Field(default_factory=list)


# =============================================================================
# Evidence Pack
# =============================================================================
class EvidencePack(BaseModel):
    """Output package for evidence retrieval.

    Attributes:
        target: The target being explained or investigated.
        primary_evidence: Core evidence directly supporting the target.
        supporting_evidence: Additional supporting evidence.
        rejected_candidates: Evidence candidates that were rejected.
        quotes: Extracted quotes from evidence.
        boundary: What could not be retrieved (retrieval boundary).
        investigation_trace: Sequence of actions taken during investigation.
    """

    target: Dict[str, Any]
    primary_evidence: List[EvidenceCapsule] = Field(default_factory=list)
    supporting_evidence: List[EvidenceCapsule] = Field(default_factory=list)
    rejected_candidates: List[Dict[str, Any]] = Field(default_factory=list)
    quotes: List[str] = Field(default_factory=list)
    boundary: Dict[str, Any] = Field(default_factory=dict)
    investigation_trace: List[Dict[str, Any]] = Field(default_factory=list)

    def total_chars(self) -> int:
        """Calculate total characters across all fragments."""
        total = 0
        for capsule in self.primary_evidence + self.supporting_evidence:
            total += len(capsule.fragment)
        for quote in self.quotes:
            total += len(quote)
        return total

    def capsule_count(self) -> int:
        """Count total number of capsules."""
        return len(self.primary_evidence) + len(self.supporting_evidence)


# =============================================================================
# Explain Pack
# =============================================================================
class ExplainPack(BaseModel):
    """Output package for explanation queries.

    Attributes:
        explain_target: The target being explained.
        selected_evidence: Evidence selected for the explanation.
        reasoning_summary: Summary of the reasoning process.
        provenance_chain: Chain of provenance linking evidence to target.
        unresolved_gaps: Gaps that could not be resolved.
        boundary: Retrieval boundary (what could not be retrieved).
        investigation_trace: Sequence of actions taken.
    """

    explain_target: Dict[str, Any]
    selected_evidence: List[EvidenceCapsule] = Field(default_factory=list)
    reasoning_summary: str = ""
    provenance_chain: List[Dict[str, Any]] = Field(default_factory=list)
    unresolved_gaps: List[str] = Field(default_factory=list)
    boundary: Dict[str, Any] = Field(default_factory=dict)
    investigation_trace: List[Dict[str, Any]] = Field(default_factory=list)

    def total_chars(self) -> int:
        """Calculate total characters."""
        total = len(self.reasoning_summary)
        for capsule in self.selected_evidence:
            total += len(capsule.fragment)
        return total


# =============================================================================
# Case Dossier
# =============================================================================
class CaseDossier(BaseModel):
    """Output package for case dossier queries.

    Attributes:
        case_id: Identifier of the case.
        symptom_capsules: Evidence capsules describing symptoms.
        upstream_capsules: Upstream evidence capsules.
        boundary: Retrieval boundary.
        quotes: Extracted quotes.
        trace: Investigation trace.
    """

    case_id: str
    symptom_capsules: List[EvidenceCapsule] = Field(default_factory=list)
    upstream_capsules: List[EvidenceCapsule] = Field(default_factory=list)
    boundary: Dict[str, Any] = Field(default_factory=dict)
    quotes: List[str] = Field(default_factory=list)
    trace: List[Dict[str, Any]] = Field(default_factory=list)

    def total_chars(self) -> int:
        """Calculate total characters."""
        total = 0
        for capsule in self.symptom_capsules + self.upstream_capsules:
            total += len(capsule.fragment)
        for quote in self.quotes:
            total += len(quote)
        return total

    def capsule_count(self) -> int:
        """Count total capsules."""
        return len(self.symptom_capsules) + len(self.upstream_capsules)


# =============================================================================
# Validator Defect Pack
# =============================================================================
class ValidatorDefectPack(BaseModel):
    """Output package for validator defect queries.

    Attributes:
        defect_id: Identifier of the defect.
        validator_name: Name of the validator that found the defect.
        defect_description: Description of the defect.
        evidence: Evidence supporting the defect.
        boundary: Retrieval boundary.
        investigation_trace: Investigation trace.
    """

    defect_id: str
    validator_name: str
    defect_description: str
    evidence: List[EvidenceCapsule] = Field(default_factory=list)
    boundary: Dict[str, Any] = Field(default_factory=dict)
    investigation_trace: List[Dict[str, Any]] = Field(default_factory=list)

    def total_chars(self) -> int:
        """Calculate total characters."""
        total = len(self.defect_description)
        for capsule in self.evidence:
            total += len(capsule.fragment)
        return total


# =============================================================================
# Investigation Session
# =============================================================================
class InvestigationStep(BaseModel):
    """A single step in an investigation.

    Attributes:
        step_number: Step number (1-indexed).
        action: Action taken (e.g., 'LIST_ARTIFACTS', 'READ_FRAGMENT').
        action_params: Parameters passed to the action.
        result: Result of the action.
        llm_call: Whether an LLM call was made in this step.
        llm_prompt: The prompt used (if LLM was called).
        llm_response: The response (if LLM was called).
    """

    step_number: int
    action: str
    action_params: Dict[str, Any] = Field(default_factory=dict)
    result: Dict[str, Any] = Field(default_factory=dict)
    llm_call: bool = False
    llm_prompt: Optional[str] = None
    llm_response: Optional[str] = None


class InvestigationSession(BaseModel):
    """Session tracking state during investigation.

    Attributes:
        session_id: Unique session identifier.
        request: The original context request.
        steps: Investigation steps taken.
        hypotheses: Hypotheses generated during investigation.
        selected_refs: References selected as evidence.
        rejected_refs: References rejected as evidence.
        open_questions: Questions that remain unanswered.
        llm_calls_count: Number of LLM calls made.
        total_chars: Total characters accumulated.
    """

    session_id: str
    request: ContextRequest
    steps: List[InvestigationStep] = Field(default_factory=list)
    hypotheses: List[Dict[str, Any]] = Field(default_factory=list)
    selected_refs: List[Dict[str, Any]] = Field(default_factory=list)
    rejected_refs: List[Dict[str, Any]] = Field(default_factory=list)
    open_questions: List[str] = Field(default_factory=list)
    llm_calls_count: int = 0
    total_chars: int = 0

    def add_step(
        self,
        action: str,
        action_params: Dict[str, Any],
        result: Dict[str, Any],
        llm_call: bool = False,
        llm_prompt: Optional[str] = None,
        llm_response: Optional[str] = None,
    ) -> None:
        """Add a step to the session."""
        step = InvestigationStep(
            step_number=len(self.steps) + 1,
            action=action,
            action_params=action_params,
            result=result,
            llm_call=llm_call,
            llm_prompt=llm_prompt,
            llm_response=llm_response,
        )
        self.steps.append(step)
        if llm_call:
            self.llm_calls_count += 1

    def can_continue(self) -> bool:
        """Check if investigation can continue based on limits."""
        policy = self.request.policy
        if len(self.steps) >= policy.max_steps:
            return False
        if self.llm_calls_count >= policy.max_llm_calls:
            return False
        if self.total_chars >= policy.max_total_chars:
            return False
        return True

    def record_chars(self, chars: int) -> None:
        """Record additional characters."""
        self.total_chars += chars


# =============================================================================
# Action Space
# =============================================================================
class ActionSpec(BaseModel):
    """Specification for an action in the investigation action space.

    Attributes:
        name: Action name.
        description: Human-readable description.
        params: Expected parameters schema.
        returns: Description of return value.
    """

    name: str
    description: str
    params: Dict[str, Any] = Field(default_factory=dict)
    returns: str = ""


# Standard action space for investigation
STANDARD_ACTIONS: List[ActionSpec] = [
    ActionSpec(
        name="LIST_ARTIFACTS",
        description="List available artifacts in the run",
        params={"type": "object", "properties": {"artifact_type": {"type": "string"}}},
        returns="List of artifact identifiers",
    ),
    ActionSpec(
        name="READ_FRAGMENT",
        description="Read a fragment from an artifact",
        params={
            "type": "object",
            "properties": {
                "artifact_id": {"type": "string"},
                "fragment_path": {"type": "string"},
            },
        },
        returns="Fragment content as string",
    ),
    ActionSpec(
        name="EXPAND_NEIGHBORHOOD",
        description="Expand neighborhood around an artifact",
        params={
            "type": "object",
            "properties": {
                "artifact_id": {"type": "string"},
                "direction": {"type": "string", "enum": ["upstream", "downstream", "both"]},
                "max_hops": {"type": "integer"},
            },
        },
        returns="List of neighboring artifacts",
    ),
    ActionSpec(
        name="RESOLVE_PROVENANCE",
        description="Resolve provenance chain for an artifact",
        params={"type": "object", "properties": {"artifact_id": {"type": "string"}}},
        returns="Provenance chain as list of artifacts",
    ),
    ActionSpec(
        name="SEARCH_ARTIFACTS",
        description="Search artifacts by content or metadata",
        params={
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "artifact_type": {"type": "string"},
            },
        },
        returns="List of matching artifacts",
    ),
    ActionSpec(
        name="EXTRACT_QUOTES",
        description="Extract relevant quotes from a fragment",
        params={
            "type": "object",
            "properties": {
                "artifact_id": {"type": "string"},
                "fragment": {"type": "string"},
                "max_quotes": {"type": "integer"},
            },
        },
        returns="List of quote strings",
    ),
]


__all__ = [
    "RetrievalPolicy",
    "ContextRequest",
    "EvidenceCapsule",
    "EvidencePack",
    "ExplainPack",
    "CaseDossier",
    "ValidatorDefectPack",
    "InvestigationSession",
    "InvestigationStep",
    "ActionSpec",
    "STANDARD_ACTIONS",
]
