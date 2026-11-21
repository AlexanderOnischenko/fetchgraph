"""Tests for artifact context provider adapter."""

import json
import tempfile
from pathlib import Path
import pytest

from fetchgraph.adapters.artifact_context_provider import (
    ArtifactContextProvider,
    create_artifact_context_provider,
)


@pytest.fixture
def temp_run_dir_with_artifacts():
    """Create a temporary run directory with artifacts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir)
        
        # Create test artifacts
        plan = {
            "id": "plan-001",
            "feature_name": "test_feature",
            "steps": [{"provider": "test", "mode": "full"}],
        }
        evidence = {
            "id": "evidence-001",
            "provider": "test_provider",
            "data": {"key": "value"},
            "text": "Evidence text",
        }
        synthesis = {
            "id": "synthesis-001",
            "feature_name": "test_feature",
            "answer": "Test answer",
        }
        
        with open(run_dir / "plan-001.json", "w") as f:
            json.dump(plan, f)
        with open(run_dir / "evidence-001.json", "w") as f:
            json.dump(evidence, f)
        with open(run_dir / "synthesis-001.json", "w") as f:
            json.dump(synthesis, f)
        
        yield run_dir


class TestArtifactContextProvider:
    """Tests for ArtifactContextProvider."""

    def test_create_provider(self, temp_run_dir_with_artifacts):
        """Test creating provider."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        assert provider.name == "artifact_context"
        assert provider.run_dir == temp_run_dir_with_artifacts

    def test_provider_name(self, temp_run_dir_with_artifacts):
        """Test provider name attribute."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        assert provider.name == "artifact_context"

    def test_fetch_explain_target(self, temp_run_dir_with_artifacts):
        """Test fetching with EXPLAIN_TARGET."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        result = provider.fetch(
            feature_name="EXPLAIN_TARGET",
            selectors={"target_id": "synthesis-001"},
        )
        
        assert isinstance(result, dict)
        assert "explain_target" in result or "target" in result

    def test_fetch_with_query_family(self, temp_run_dir_with_artifacts):
        """Test fetching with explicit query family."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        result = provider.fetch(
            feature_name="EXPLAIN_DERIVATION",
            selectors={"source_id": "plan-001", "target_id": "synthesis-001"},
        )
        
        assert isinstance(result, dict)

    def test_fetch_with_policy_options(self, temp_run_dir_with_artifacts):
        """Test fetching with policy options."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        result = provider.fetch(
            feature_name="EXPLAIN_TARGET",
            selectors={"target_id": "synthesis-001"},
            mode="deterministic",
            max_capsules=10,
            max_total_chars=5000,
        )
        
        assert isinstance(result, dict)

    def test_serialize_dict(self, temp_run_dir_with_artifacts):
        """Test serializing dictionary result."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        obj = {"key": "value", "nested": {"a": 1}}
        result = provider.serialize(obj)
        
        assert isinstance(result, str)
        assert "key" in result
        assert "value" in result

    def test_serialize_model(self, temp_run_dir_with_artifacts):
        """Test serializing pydantic model."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        from fetchgraph.artifact_context.models import EvidencePack
        obj = EvidencePack(target={"id": "test"})
        result = provider.serialize(obj)
        
        assert isinstance(result, str)
        assert "target" in result

    def test_build_anchors(self, temp_run_dir_with_artifacts):
        """Test anchor building from selectors."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        selectors = {
            "target_id": "AS-017",
            "case_id": "CASE-001",
            "defect_id": "DEF-001",
            "validator": "test_validator",
        }
        anchors = provider._build_anchors(selectors)
        
        assert anchors["target_id"] == "AS-017"
        assert anchors["validator_name"] == "test_validator"

    def test_build_policy(self, temp_run_dir_with_artifacts):
        """Test policy building from kwargs."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        policy = provider._build_policy({
            "mode": "assisted",
            "max_llm_calls": 2,
            "max_capsules": 15,
        })
        
        assert policy.mode == "assisted"
        assert policy.max_llm_calls == 2
        assert policy.max_capsules == 15

    def test_build_policy_empty(self, temp_run_dir_with_artifacts):
        """Test policy building with empty kwargs."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        policy = provider._build_policy({})
        
        assert policy.mode == "deterministic"
        assert policy.max_capsules == 20

    def test_map_feature_name_direct(self, temp_run_dir_with_artifacts):
        """Test mapping direct query family names."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        assert provider._map_feature_name_to_query_family("EXPLAIN_TARGET") == "EXPLAIN_TARGET"
        assert provider._map_feature_name_to_query_family("CASE_DOSSIER") == "CASE_DOSSIER"

    def test_map_feature_name_heuristic(self, temp_run_dir_with_artifacts):
        """Test mapping heuristic feature names."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        # Should map based on keywords
        assert "EXPLAIN" in provider._map_feature_name_to_query_family("explain_my_target")
        assert "CASE" in provider._map_feature_name_to_query_family("case_dossier_query") or \
               "DOSSIER" in provider._map_feature_name_to_query_family("case_dossier_query")

    def test_result_to_dict(self, temp_run_dir_with_artifacts):
        """Test converting result to dictionary."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        from fetchgraph.artifact_context.models import ExplainPack
        obj = ExplainPack(explain_target={"id": "test"})
        result = provider._result_to_dict(obj)
        
        assert isinstance(result, dict)
        assert "explain_target" in result

    def test_factory_function(self, temp_run_dir_with_artifacts):
        """Test factory function."""
        provider = create_artifact_context_provider(
            run_dir=temp_run_dir_with_artifacts,
            run_id="test-run",
        )
        
        assert isinstance(provider, ArtifactContextProvider)
        assert provider.run_id == "test-run"


class TestArtifactContextProviderIntegration:
    """Integration tests for ArtifactContextProvider."""

    def test_full_fetch_cycle(self, temp_run_dir_with_artifacts):
        """Test complete fetch cycle."""
        provider = ArtifactContextProvider(run_dir=temp_run_dir_with_artifacts)
        
        # Fetch
        result = provider.fetch(
            feature_name="EXPLAIN_TARGET",
            selectors={"target_id": "synthesis-001"},
            mode="deterministic",
        )
        
        # Serialize
        serialized = provider.serialize(result)
        
        # Verify
        assert isinstance(serialized, str)
        data = json.loads(serialized)
        assert isinstance(data, dict)
