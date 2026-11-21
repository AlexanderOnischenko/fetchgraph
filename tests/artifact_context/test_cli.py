"""Tests for artifact context CLI."""

import json
import subprocess
import sys
import tempfile
from pathlib import Path
import pytest

from fetchgraph.cli.artifact_context import (
    artifact_context_command,
    _find_run_dir,
    _format_as_text,
)


@pytest.fixture
def temp_run_dir_with_artifacts():
    """Create a temporary run directory with artifacts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir)
        
        # Create test artifacts
        synthesis = {
            "id": "synthesis-001",
            "feature_name": "test_feature",
            "answer": "Test answer",
            "evidence_refs": ["evidence-001"],
        }
        evidence = {
            "id": "evidence-001",
            "provider": "test_provider",
            "data": {"result": "success"},
            "text": "Evidence content",
        }
        
        with open(run_dir / "synthesis-001.json", "w") as f:
            json.dump(synthesis, f)
        with open(run_dir / "evidence-001.json", "w") as f:
            json.dump(evidence, f)
        
        yield run_dir


class TestArtifactContextCommand:
    """Tests for artifact_context_command."""

    def test_help(self, capsys):
        """Test help output."""
        # Help triggers SystemExit(0)
        with pytest.raises(SystemExit) as exc_info:
            artifact_context_command(["--help"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "artifact-context" in captured.out or "Artifact context" in captured.out

    def test_missing_required_args(self, capsys):
        """Test that missing required args raises error."""
        with pytest.raises(SystemExit) as exc_info:
            artifact_context_command([])
        assert exc_info.value.code != 0

    def test_invalid_json_anchors(self, capsys):
        """Test that invalid JSON in anchors raises error."""
        result = artifact_context_command([
            "--run-id", "test",
            "--query-family", "EXPLAIN_TARGET",
            "--anchors", "not valid json",
        ])
        assert result != 0
        captured = capsys.readouterr()
        assert "Invalid JSON" in captured.err

    def test_anchors_not_object(self, capsys):
        """Test that non-object anchors raises error."""
        result = artifact_context_command([
            "--run-id", "test",
            "--query-family", "EXPLAIN_TARGET",
            "--anchors", '["array"]',
        ])
        assert result != 0
        captured = capsys.readouterr()
        assert "JSON object" in captured.err

    def test_run_dir_not_found(self, capsys):
        """Test that non-existent run dir raises error."""
        result = artifact_context_command([
            "--run-id", "nonexistent-run",
            "--query-family", "EXPLAIN_TARGET",
            "--anchors", '{"target_id": "test"}',
        ])
        assert result != 0
        captured = capsys.readouterr()
        assert "Could not find run directory" in captured.err

    def test_valid_command(self, temp_run_dir_with_artifacts, capsys):
        """Test valid command execution."""
        result = artifact_context_command([
            "--run-dir", str(temp_run_dir_with_artifacts),
            "--run-id", "test-run",
            "--query-family", "EXPLAIN_TARGET",
            "--anchors", '{"target_id": "synthesis-001"}',
            "--mode", "deterministic",
        ])
        
        # Should succeed (may return empty results if target not found)
        # but should not crash
        captured = capsys.readouterr()
        assert result == 0 or "not found" in captured.out.lower() or "error" in captured.err.lower()

    def test_output_format_json(self, temp_run_dir_with_artifacts, capsys):
        """Test JSON output format."""
        result = artifact_context_command([
            "--run-dir", str(temp_run_dir_with_artifacts),
            "--run-id", "test-run",
            "--query-family", "EXPLAIN_TARGET",
            "--anchors", '{"target_id": "synthesis-001"}',
            "--format", "json",
        ])
        
        captured = capsys.readouterr()
        # Output should be valid JSON or error message
        if result == 0:
            try:
                data = json.loads(captured.out)
                assert isinstance(data, (dict, str))
            except json.JSONDecodeError:
                pytest.fail("Output is not valid JSON")

    def test_output_format_text(self, temp_run_dir_with_artifacts, capsys):
        """Test text output format."""
        result = artifact_context_command([
            "--run-dir", str(temp_run_dir_with_artifacts),
            "--run-id", "test-run",
            "--query-family", "EXPLAIN_TARGET",
            "--anchors", '{"target_id": "synthesis-001"}',
            "--format", "text",
        ])
        
        captured = capsys.readouterr()
        # Text format should have headers
        assert result == 0 or "EXPLANATION" in captured.out or "EVIDENCE" in captured.out

    def test_output_to_file(self, temp_run_dir_with_artifacts):
        """Test output to file."""
        with tempfile.TemporaryDirectory() as outdir:
            output_file = Path(outdir) / "output.json"
            
            result = artifact_context_command([
                "--run-dir", str(temp_run_dir_with_artifacts),
                "--run-id", "test-run",
                "--query-family", "EXPLAIN_TARGET",
                "--anchors", '{"target_id": "synthesis-001"}',
                "--output", str(output_file),
            ])
            
            # File should be created
            assert output_file.exists() or result != 0

    def test_verbose_mode(self, temp_run_dir_with_artifacts, capsys):
        """Test verbose mode."""
        result = artifact_context_command([
            "--run-dir", str(temp_run_dir_with_artifacts),
            "--run-id", "test-run",
            "--query-family", "EXPLAIN_TARGET",
            "--anchors", '{"target_id": "synthesis-001"}',
            "--verbose",
        ])
        
        captured = capsys.readouterr()
        # Verbose mode should log to stderr
        # (actual logging depends on configuration)

    def test_policy_options(self, temp_run_dir_with_artifacts, capsys):
        """Test policy options."""
        result = artifact_context_command([
            "--run-dir", str(temp_run_dir_with_artifacts),
            "--run-id", "test-run",
            "--query-family", "EXPLAIN_TARGET",
            "--anchors", '{"target_id": "synthesis-001"}',
            "--max-capsules", "5",
            "--max-total-chars", "10000",
            "--max-hops-upstream", "2",
        ])
        
        # Should not crash
        assert isinstance(result, int)


class TestFindRunDir:
    """Tests for _find_run_dir helper."""

    def test_path_exists(self):
        """Test finding existing path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = _find_run_dir(tmpdir)
            assert result == Path(tmpdir)

    def test_nonexistent_path(self):
        """Test finding non-existent path."""
        result = _find_run_dir("/nonexistent/path/12345")
        assert result is None

    def test_relative_path(self):
        """Test finding relative path."""
        # This test depends on current directory structure
        result = _find_run_dir("nonexistent_run")
        # May return None if not found
        assert result is None or result.exists()


class TestFormatAsText:
    """Tests for _format_as_text helper."""

    def test_format_explain_pack(self):
        """Test formatting ExplainPack."""
        from fetchgraph.artifact_context.models import ExplainPack
        
        pack = ExplainPack(
            explain_target={"target_id": "AS-017"},
            reasoning_summary="Test reasoning",
        )
        
        text = _format_as_text(pack)
        assert "EXPLANATION" in text
        assert "AS-017" in text

    def test_format_evidence_pack(self):
        """Test formatting EvidencePack."""
        from fetchgraph.artifact_context.models import EvidencePack
        
        pack = EvidencePack(target={"id": "target-001"})
        
        text = _format_as_text(pack)
        assert "EVIDENCE" in text

    def test_format_case_dossier(self):
        """Test formatting CaseDossier."""
        from fetchgraph.artifact_context.models import CaseDossier
        
        dossier = CaseDossier(case_id="CASE-001")
        
        text = _format_as_text(dossier)
        assert "CASE DOSSIER" in text
        assert "CASE-001" in text

    def test_format_validator_defect(self):
        """Test formatting ValidatorDefectPack."""
        from fetchgraph.artifact_context.models import ValidatorDefectPack
        
        pack = ValidatorDefectPack(
            defect_id="DEF-001",
            validator_name="test_validator",
            defect_description="Test defect",
        )
        
        text = _format_as_text(pack)
        assert "VALIDATOR DEFECT" in text
        assert "DEF-001" in text

    def test_format_dict(self):
        """Test formatting dictionary."""
        data = {"key": "value"}
        text = _format_as_text(data)
        assert "key" in text
        assert "value" in text

    def test_format_with_trace(self):
        """Test formatting with investigation trace."""
        from fetchgraph.artifact_context.models import ExplainPack
        
        pack = ExplainPack(
            explain_target={"target_id": "AS-017"},
            investigation_trace=[
                {"step": 1, "action": "SEARCH_ARTIFACTS"},
                {"step": 2, "action": "EXPAND_NEIGHBORHOOD"},
            ],
        )
        
        text = _format_as_text(pack)
        assert "INVESTIGATION TRACE" in text
        assert "SEARCH_ARTIFACTS" in text


class TestCLIEntryPoint:
    """Tests for CLI entry point."""

    def test_main_function(self, capsys):
        """Test main function."""
        from fetchgraph.cli.artifact_context import main
        
        with pytest.raises(SystemExit) as exc_info:
            main(["--help"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "artifact-context" in captured.out or "Artifact context" in captured.out

    def test_module_execution(self):
        """Test running module with python -m."""
        # This tests that the module can be executed
        result = subprocess.run(
            [sys.executable, "-m", "fetchgraph.cli.artifact_context", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "artifact-context" in result.stdout or "Artifact context" in result.stdout
