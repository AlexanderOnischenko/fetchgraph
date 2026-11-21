"""Tests for investigation orchestrator."""

import json
import tempfile
from pathlib import Path
import pytest

from fetchgraph.artifact_context.models import (
    ContextRequest,
    RetrievalPolicy,
    EvidencePack,
    ExplainPack,
    CaseDossier,
    ValidatorDefectPack,
)
from fetchgraph.investigation.orchestrator import (
    InvestigationOrchestrator,
    create_orchestrator,
)
from fetchgraph.artifact_graph.provider import FileSystemArtifactGraphProvider


@pytest.fixture
def temp_run_dir_with_artifacts():
    """Create a temporary run directory with realistic artifacts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir)
        
        # Create plan artifact
        plan = {
            "id": "plan-001",
            "feature_name": "test_feature",
            "version": "1.0",
            "steps": [
                {"provider": "test_provider", "mode": "full", "selectors": {"key": "value"}}
            ],
            "evidence_refs": ["evidence-001"],
            "created_at": "2024-01-01T00:00:00Z",
            "run_id": "run-123",
        }
        
        # Create evidence artifact
        evidence = {
            "id": "evidence-001",
            "provider": "test_provider",
            "feature_name": "test_feature",
            "data": {"result": "success", "count": 42},
            "text": "Evidence content for test_feature",
            "plan_ref": "plan-001",
            "created_at": "2024-01-01T00:00:00Z",
            "run_id": "run-123",
            "tokens": 100,
        }
        
        # Create synthesis artifact
        synthesis = {
            "id": "synthesis-001",
            "feature_name": "test_feature",
            "answer": "Final answer for test_feature",
            "reasoning": "Based on the evidence, we conclude...",
            "evidence_refs": ["evidence-001"],
            "plan_ref": "plan-001",
            "created_at": "2024-01-01T00:00:00Z",
            "run_id": "run-123",
        }
        
        # Write artifacts
        with open(run_dir / "plan-001.json", "w") as f:
            json.dump(plan, f)
        with open(run_dir / "evidence-001.json", "w") as f:
            json.dump(evidence, f)
        with open(run_dir / "synthesis-001.json", "w") as f:
            json.dump(synthesis, f)
        
        yield run_dir


class TestInvestigationOrchestrator:
    """Tests for InvestigationOrchestrator."""

    def test_create_orchestrator(self, temp_run_dir_with_artifacts):
        """Test creating orchestrator."""
        orchestrator = create_orchestrator(temp_run_dir_with_artifacts)
        assert isinstance(orchestrator, InvestigationOrchestrator)
        assert isinstance(orchestrator.graph_provider, FileSystemArtifactGraphProvider)

    def test_fetch_explain_target(self, temp_run_dir_with_artifacts):
        """Test EXPLAIN_TARGET query."""
        orchestrator = create_orchestrator(temp_run_dir_with_artifacts)
        
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={"target_id": "synthesis-001"},
            policy=RetrievalPolicy(mode="deterministic"),
        )
        
        result = orchestrator.fetch(request)
        
        assert isinstance(result, ExplainPack)
        assert result.explain_target is not None
        assert len(result.investigation_trace) > 0

    def test_fetch_explain_derivation(self, temp_run_dir_with_artifacts):
        """Test EXPLAIN_DERIVATION query."""
        orchestrator = create_orchestrator(temp_run_dir_with_artifacts)
        
        request = ContextRequest(
            query_family="EXPLAIN_DERIVATION",
            run_id="run-123",
            anchors={"source_id": "plan-001", "target_id": "synthesis-001"},
            policy=RetrievalPolicy(mode="deterministic"),
        )
        
        result = orchestrator.fetch(request)
        
        assert isinstance(result, ExplainPack)
        assert result.explain_target is not None

    def test_fetch_case_dossier(self, temp_run_dir_with_artifacts):
        """Test CASE_DOSSIER query."""
        orchestrator = create_orchestrator(temp_run_dir_with_artifacts)
        
        request = ContextRequest(
            query_family="CASE_DOSSIER",
            run_id="run-123",
            anchors={"case_id": "test_feature"},
            policy=RetrievalPolicy(mode="deterministic"),
        )
        
        result = orchestrator.fetch(request)
        
        assert isinstance(result, CaseDossier)
        assert result.case_id == "test_feature"

    def test_fetch_validator_defect_pack(self, temp_run_dir_with_artifacts):
        """Test VALIDATOR_DEFECT_PACK query."""
        orchestrator = create_orchestrator(temp_run_dir_with_artifacts)
        
        request = ContextRequest(
            query_family="VALIDATOR_DEFECT_PACK",
            run_id="run-123",
            anchors={"defect_id": "DEF-001", "validator_name": "test_validator"},
            policy=RetrievalPolicy(mode="deterministic"),
        )
        
        result = orchestrator.fetch(request)
        
        assert isinstance(result, ValidatorDefectPack)
        assert result.defect_id == "DEF-001"
        assert result.validator_name == "test_validator"

    def test_fetch_invalid_query_family(self, temp_run_dir_with_artifacts):
        """Test that invalid query family raises error."""
        orchestrator = create_orchestrator(temp_run_dir_with_artifacts)
        
        # Pydantic will validate at request creation time
        import pytest
        from pydantic import ValidationError
        
        with pytest.raises(ValidationError):
            ContextRequest(
                query_family="INVALID",  # type: ignore
                run_id="run-123",
                anchors={},
            )

    def test_fetch_missing_anchor(self, temp_run_dir_with_artifacts):
        """Test that missing required anchor raises error."""
        orchestrator = create_orchestrator(temp_run_dir_with_artifacts)
        
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={},  # Missing target_id
        )
        
        with pytest.raises(ValueError, match="target_id"):
            orchestrator.fetch(request)

    def test_investigation_trace(self, temp_run_dir_with_artifacts):
        """Test that investigation trace is recorded."""
        orchestrator = create_orchestrator(temp_run_dir_with_artifacts)
        
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={"target_id": "synthesis-001"},
        )
        
        result = orchestrator.fetch(request)
        
        assert len(result.investigation_trace) > 0
        # First step should be SEARCH_ARTIFACTS
        assert result.investigation_trace[0]["action"] == "SEARCH_ARTIFACTS"

    def test_policy_limits_respected(self, temp_run_dir_with_artifacts):
        """Test that policy limits are respected."""
        orchestrator = create_orchestrator(temp_run_dir_with_artifacts)
        
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={"target_id": "synthesis-001"},
            policy=RetrievalPolicy(
                mode="deterministic",
                max_capsules=5,
                max_total_chars=1000,
            ),
        )
        
        result = orchestrator.fetch(request)
        
        # Should not exceed limits
        if hasattr(result, "capsule_count"):
            assert result.capsule_count() <= 5
        if hasattr(result, "total_chars"):
            assert result.total_chars() <= 1000


class TestOrchestratorWithLLM:
    """Tests for orchestrator with LLM integration."""

    def test_orchestrator_with_llm_invoke(self, temp_run_dir_with_artifacts):
        """Test orchestrator with LLM invoke function."""
        llm_calls = []
        
        def mock_llm(prompt: str, sender: str) -> str:
            llm_calls.append((prompt, sender))
            return "Mock LLM response"
        
        orchestrator = create_orchestrator(
            temp_run_dir_with_artifacts,
            llm_invoke=mock_llm,
        )
        
        request = ContextRequest(
            query_family="EXPLAIN_TARGET",
            run_id="run-123",
            anchors={"target_id": "synthesis-001"},
            policy=RetrievalPolicy(mode="assisted", max_llm_calls=1),
        )
        
        result = orchestrator.fetch(request)
        
        # LLM may or may not be called depending on implementation
        # This test ensures the integration works without errors
        assert result is not None


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_explain_target_function(self, temp_run_dir_with_artifacts):
        """Test explain_target convenience function."""
        from fetchgraph.artifact_context.engine import explain_target
        
        result = explain_target(
            target_id="synthesis-001",
            run_dir=temp_run_dir_with_artifacts,
            mode="deterministic",
        )
        
        assert isinstance(result, ExplainPack)

    def test_case_dossier_function(self, temp_run_dir_with_artifacts):
        """Test case_dossier convenience function."""
        from fetchgraph.artifact_context.engine import case_dossier
        
        result = case_dossier(
            case_id="test_feature",
            run_dir=temp_run_dir_with_artifacts,
            mode="deterministic",
        )
        
        assert isinstance(result, CaseDossier)

    def test_validator_defect_pack_function(self, temp_run_dir_with_artifacts):
        """Test validator_defect_pack convenience function."""
        from fetchgraph.artifact_context.engine import validator_defect_pack
        
        result = validator_defect_pack(
            defect_id="DEF-001",
            validator_name="test_validator",
            run_dir=temp_run_dir_with_artifacts,
            mode="deterministic",
        )
        
        assert isinstance(result, ValidatorDefectPack)
