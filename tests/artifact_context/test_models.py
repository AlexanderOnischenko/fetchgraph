"""Tests for artifact context engine models."""

import json
import pytest
from fetchgraph.artifact_context.models import (
    RetrievalPolicy,
    ContextRequest,
    EvidenceCapsule,
    EvidencePack,
    ExplainPack,
    CaseDossier,
    ValidatorDefectPack,
    InvestigationSession,
    InvestigationStep,
)


class TestRetrievalPolicy:
    """Tests for RetrievalPolicy model."""

    def test_default_policy(self):
        """Test default policy values."""
        policy = RetrievalPolicy()
        assert policy.mode == "deterministic"
        assert policy.max_llm_calls == 1
        assert policy.max_steps == 1
        assert policy.max_capsules == 20
        assert policy.max_total_chars == 30000

    def test_mode_enforcement_deterministic(self):
        """Test deterministic mode enforces max 1 LLM call."""
        policy = RetrievalPolicy(mode="deterministic", max_llm_calls=10)
        assert policy.max_llm_calls == 1

    def test_mode_enforcement_assisted(self):
        """Test assisted mode enforces max 3 LLM calls."""
        policy = RetrievalPolicy(mode="assisted", max_llm_calls=10)
        assert policy.max_llm_calls <= 3

    def test_mode_enforcement_investigator(self):
        """Test investigator mode allows up to 6 LLM calls."""
        policy = RetrievalPolicy(mode="investigator", max_llm_calls=10)
        assert policy.max_llm_calls <= 6
        assert policy.max_steps >= 5

    def test_custom_policy(self):
        """Test custom policy values."""
        policy = RetrievalPolicy(
            mode="assisted",
            max_llm_calls=2,
            max_steps=3,
            max_capsules=10,
            max_total_chars=15000,
        )
        assert policy.mode == "assisted"
        assert policy.max_llm_calls == 2
        assert policy.max_steps == 3


class TestContextRequest:
    """Tests for ContextRequest model."""

    def test_minimal_request(self):
        """Test minimal request with defaults."""
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={"target_id": "AS-017"},
        )
        assert request.pipeline_id == "archgraph_v1"
        assert request.query_family == "EXPLAIN_TARGET"
        assert request.run_id == "run-123"
        assert request.anchors == {"target_id": "AS-017"}

    def test_full_request(self):
        """Test request with all fields."""
        request = ContextRequest(
            pipeline_id="custom_v1",
            query_family="CASE_DOSSIER",
            run_id="run-456",
            anchors={"case_id": "CASE-001"},
            options={"include_upstream": True},
            policy=RetrievalPolicy(mode="investigator"),
        )
        assert request.pipeline_id == "custom_v1"
        assert request.query_family == "CASE_DOSSIER"
        assert request.anchors == {"case_id": "CASE-001"}
        assert request.policy.mode == "investigator"

    def test_invalid_query_family(self):
        """Test that invalid query family raises error."""
        with pytest.raises(Exception):
            ContextRequest(
                query_family="INVALID",  # type: ignore
                run_id="run-123",
                anchors={},
            )


class TestEvidenceCapsule:
    """Tests for EvidenceCapsule model."""

    def test_minimal_capsule(self):
        """Test minimal capsule."""
        capsule = EvidenceCapsule(
            artifact_type="evidence",
            artifact_id="art-001",
            fragment="Test fragment",
        )
        assert capsule.artifact_type == "evidence"
        assert capsule.artifact_id == "art-001"
        assert capsule.fragment == "Test fragment"
        assert capsule.quotes == []

    def test_full_capsule(self):
        """Test capsule with all fields."""
        capsule = EvidenceCapsule(
            artifact_type="plan",
            artifact_id="plan-001",
            fragment="Plan content",
            fragment_path="steps.0",
            provenance={"run_id": "run-123", "step": 1},
            relevance_score=0.95,
            quotes=["Important quote"],
        )
        assert capsule.fragment_path == "steps.0"
        assert capsule.relevance_score == 0.95
        assert capsule.quotes == ["Important quote"]


class TestEvidencePack:
    """Tests for EvidencePack model."""

    def test_empty_pack(self):
        """Test empty evidence pack."""
        pack = EvidencePack(target={"id": "target-001"})
        assert pack.target == {"id": "target-001"}
        assert pack.primary_evidence == []
        assert pack.supporting_evidence == []
        assert pack.total_chars() == 0
        assert pack.capsule_count() == 0

    def test_pack_with_evidence(self):
        """Test pack with evidence capsules."""
        pack = EvidencePack(
            target={"id": "target-001"},
            primary_evidence=[
                EvidenceCapsule(
                    artifact_type="evidence",
                    artifact_id="art-001",
                    fragment="Primary evidence",
                )
            ],
            supporting_evidence=[
                EvidenceCapsule(
                    artifact_type="plan",
                    artifact_id="art-002",
                    fragment="Supporting evidence",
                )
            ],
        )
        assert pack.capsule_count() == 2
        assert pack.total_chars() > 0


class TestExplainPack:
    """Tests for ExplainPack model."""

    def test_minimal_explain_pack(self):
        """Test minimal explain pack."""
        pack = ExplainPack(explain_target={"target_id": "AS-017"})
        assert pack.explain_target == {"target_id": "AS-017"}
        assert pack.selected_evidence == []
        assert pack.reasoning_summary == ""
        assert pack.unresolved_gaps == []

    def test_explain_pack_with_reasoning(self):
        """Test explain pack with reasoning."""
        pack = ExplainPack(
            explain_target={"target_id": "AS-017"},
            reasoning_summary="Target was found through upstream traversal",
            selected_evidence=[
                EvidenceCapsule(
                    artifact_type="evidence",
                    artifact_id="art-001",
                    fragment="Evidence content",
                )
            ],
        )
        assert "upstream" in pack.reasoning_summary.lower() or "found" in pack.reasoning_summary.lower()
        assert len(pack.selected_evidence) == 1


class TestCaseDossier:
    """Tests for CaseDossier model."""

    def test_minimal_dossier(self):
        """Test minimal case dossier."""
        dossier = CaseDossier(case_id="CASE-001")
        assert dossier.case_id == "CASE-001"
        assert dossier.symptom_capsules == []
        assert dossier.upstream_capsules == []
        assert dossier.total_chars() == 0
        assert dossier.capsule_count() == 0

    def test_dossier_with_capsules(self):
        """Test dossier with symptom and upstream capsules."""
        dossier = CaseDossier(
            case_id="CASE-001",
            symptom_capsules=[
                EvidenceCapsule(
                    artifact_type="synthesis",
                    artifact_id="synth-001",
                    fragment="Symptom evidence",
                )
            ],
            upstream_capsules=[
                EvidenceCapsule(
                    artifact_type="plan",
                    artifact_id="plan-001",
                    fragment="Upstream plan",
                )
            ],
        )
        assert dossier.capsule_count() == 2


class TestValidatorDefectPack:
    """Tests for ValidatorDefectPack model."""

    def test_minimal_defect_pack(self):
        """Test minimal validator defect pack."""
        pack = ValidatorDefectPack(
            defect_id="DEF-001",
            validator_name="test_validator",
            defect_description="Test defect",
        )
        assert pack.defect_id == "DEF-001"
        assert pack.validator_name == "test_validator"
        assert pack.evidence == []

    def test_defect_pack_with_evidence(self):
        """Test defect pack with evidence."""
        pack = ValidatorDefectPack(
            defect_id="DEF-001",
            validator_name="test_validator",
            defect_description="Missing required field",
            evidence=[
                EvidenceCapsule(
                    artifact_type="evidence",
                    artifact_id="art-001",
                    fragment="Defect evidence",
                )
            ],
        )
        assert len(pack.evidence) == 1


class TestInvestigationSession:
    """Tests for InvestigationSession model."""

    def test_new_session(self):
        """Test new session initialization."""
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={"target_id": "AS-017"},
        )
        session = InvestigationSession(session_id="sess-001", request=request)
        assert session.session_id == "sess-001"
        assert session.steps == []
        assert session.llm_calls_count == 0
        assert session.total_chars == 0
        assert session.can_continue() is True

    def test_add_step(self):
        """Test adding steps to session."""
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={"target_id": "AS-017"},
        )
        session = InvestigationSession(session_id="sess-001", request=request)
        session.add_step(
            action="SEARCH_ARTIFACTS",
            action_params={"query": "AS-017"},
            result={"found": 1},
        )
        assert len(session.steps) == 1
        assert session.steps[0].action == "SEARCH_ARTIFACTS"

    def test_add_llm_step(self):
        """Test adding LLM step increments counter."""
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={"target_id": "AS-017"},
        )
        session = InvestigationSession(session_id="sess-001", request=request)
        session.add_step(
            action="READ_FRAGMENT",
            action_params={"artifact_id": "art-001"},
            result={"content": "..."},
            llm_call=True,
            llm_prompt="Test prompt",
            llm_response="Test response",
        )
        assert session.llm_calls_count == 1
        assert session.steps[0].llm_call is True

    def test_record_chars(self):
        """Test recording characters."""
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={"target_id": "AS-017"},
        )
        session = InvestigationSession(session_id="sess-001", request=request)
        session.record_chars(1000)
        session.record_chars(500)
        assert session.total_chars == 1500

    def test_can_continue_limits(self):
        """Test can_continue respects limits."""
        # Test max_steps limit
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={"target_id": "AS-017"},
            policy=RetrievalPolicy(max_steps=2, max_llm_calls=10, max_total_chars=100000),
        )
        session = InvestigationSession(session_id="sess-001", request=request)
        session.add_step("ACTION1", {}, {})
        session.add_step("ACTION2", {}, {})
        assert session.can_continue() is False
