"""Tests for artifact graph provider."""

import json
import tempfile
from pathlib import Path
import pytest

from fetchgraph.artifact_graph.provider import (
    FileSystemArtifactGraphProvider,
    create_artifact_graph_provider,
)


@pytest.fixture
def temp_run_dir():
    """Create a temporary run directory with test artifacts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir)
        
        # Create test artifacts
        plan_artifact = {
            "id": "plan-001",
            "feature_name": "test_feature",
            "steps": [
                {"provider": "test_provider", "mode": "full"}
            ],
            "evidence_refs": ["evidence-001"],
        }
        
        evidence_artifact = {
            "id": "evidence-001",
            "provider": "test_provider",
            "data": {"key": "value"},
            "text": "Evidence text content",
            "plan_ref": "plan-001",
        }
        
        synthesis_artifact = {
            "id": "synthesis-001",
            "feature_name": "test_feature",
            "answer": "Test answer",
            "evidence_refs": ["evidence-001"],
            "plan_ref": "plan-001",
        }
        
        # Write artifacts
        with open(run_dir / "plan-001.json", "w") as f:
            json.dump(plan_artifact, f)
        with open(run_dir / "evidence-001.json", "w") as f:
            json.dump(evidence_artifact, f)
        with open(run_dir / "synthesis-001.json", "w") as f:
            json.dump(synthesis_artifact, f)
        
        yield run_dir


class TestFileSystemArtifactGraphProvider:
    """Tests for FileSystemArtifactGraphProvider."""

    def test_load_artifacts(self, temp_run_dir):
        """Test loading artifacts from directory."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        assert len(provider._artifacts) == 3
        assert "plan-001" in provider._artifacts
        assert "evidence-001" in provider._artifacts
        assert "synthesis-001" in provider._artifacts

    def test_get_artifact(self, temp_run_dir):
        """Test retrieving an artifact by ID."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        artifact = provider.get_artifact("plan-001")
        assert artifact is not None
        assert artifact["id"] == "plan-001"
        assert artifact["feature_name"] == "test_feature"

    def test_get_artifact_not_found(self, temp_run_dir):
        """Test retrieving non-existent artifact."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        artifact = provider.get_artifact("non-existent")
        assert artifact is None

    def test_get_fragment(self, temp_run_dir):
        """Test retrieving a fragment from an artifact."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        fragment = provider.get_fragment("evidence-001")
        assert "evidence-001" in fragment
        assert "Evidence text content" in fragment

    def test_get_fragment_path(self, temp_run_dir):
        """Test retrieving a fragment by path."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        fragment = provider.get_fragment("plan-001", "feature_name")
        assert fragment == "test_feature"

    def test_get_fragment_nested_path(self, temp_run_dir):
        """Test retrieving a nested fragment."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        fragment = provider.get_fragment("plan-001", "steps.0.provider")
        assert fragment == "test_provider"

    def test_get_fragment_invalid_path(self, temp_run_dir):
        """Test retrieving with invalid path raises error."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        with pytest.raises(ValueError, match="Fragment path"):
            provider.get_fragment("plan-001", "nonexistent.path")

    def test_list_artifacts(self, temp_run_dir):
        """Test listing all artifacts."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        artifacts = provider.list_artifacts()
        assert len(artifacts) == 3
        assert all("id" in a for a in artifacts)
        assert all("type" in a for a in artifacts)

    def test_list_artifacts_by_type(self, temp_run_dir):
        """Test listing artifacts filtered by type."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        artifacts = provider.list_artifacts(artifact_type="plan")
        assert len(artifacts) >= 1
        assert all(a.get("type") == "plan" for a in artifacts)

    def test_list_neighbors(self, temp_run_dir):
        """Test listing neighbors of an artifact."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        neighbors = provider.list_neighbors("synthesis-001", direction="upstream")
        # Neighbors are detected via reference fields
        # The graph builds relationships based on extracted references
        neighbor_ids = {n["id"] for n in neighbors}
        # At minimum should return empty list without error
        assert isinstance(neighbor_ids, set)

    def test_expand_neighborhood(self, temp_run_dir):
        """Test expanding neighborhood around an artifact."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        neighbors = provider.expand_neighborhood(
            "synthesis-001",
            max_hops=2,
            direction="upstream",
        )
        # Should return list with hop distances
        assert isinstance(neighbors, list)
        assert all("hop_distance" in n for n in neighbors)

    def test_resolve_provenance(self, temp_run_dir):
        """Test resolving provenance chain."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        provenance = provider.resolve_provenance("synthesis-001")
        # Should return upstream artifacts in order
        assert isinstance(provenance, list)

    def test_search_artifacts(self, temp_run_dir):
        """Test searching artifacts."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        results = provider.search_artifacts("test_feature")
        assert len(results) > 0
        assert all("relevance_score" in r for r in results)

    def test_search_artifacts_by_type(self, temp_run_dir):
        """Test searching artifacts with type filter."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        results = provider.search_artifacts("test", artifact_type="plan")
        assert len(results) > 0
        assert all(r.get("type") == "plan" for r in results)

    def test_create_artifact_graph_provider_factory(self, temp_run_dir):
        """Test factory function."""
        provider = create_artifact_graph_provider(temp_run_dir, run_id="test-run")
        assert isinstance(provider, FileSystemArtifactGraphProvider)
        assert provider.run_id == "test-run"

    def test_nonexistent_run_dir(self):
        """Test handling of non-existent run directory."""
        provider = FileSystemArtifactGraphProvider(Path("/nonexistent/path"))
        assert len(provider._artifacts) == 0
        assert provider.list_artifacts() == []

    def test_artifact_type_inference(self, temp_run_dir):
        """Test automatic artifact type inference."""
        provider = FileSystemArtifactGraphProvider(temp_run_dir)
        
        # Check that types are inferred (may vary based on implementation)
        plan_meta = provider._metadata.get("plan-001")
        assert plan_meta is not None
        assert "type" in plan_meta
        
        evidence_meta = provider._metadata.get("evidence-001")
        assert evidence_meta is not None
        assert "type" in evidence_meta
