"""Artifact Graph Provider - manages artifact graphs and traversal.

This module provides:
- ArtifactGraphProvider protocol defining the interface
- FileSystemArtifactGraphProvider implementation using JSON/YAML files
- Neighborhood expansion and traversal utilities
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Set, Tuple, runtime_checkable

from ..artifact_context.models import EvidenceCapsule

logger = logging.getLogger(__name__)


@runtime_checkable
class ArtifactGraphProvider(Protocol):
    """Protocol for artifact graph providers.

    Defines the interface for accessing artifacts and their relationships.
    """

    def get_artifact(self, artifact_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a complete artifact by ID."""
        ...

    def get_fragment(
        self, artifact_id: str, fragment_path: Optional[str] = None
    ) -> str:
        """Retrieve a fragment from an artifact.

        Args:
            artifact_id: Artifact identifier.
            fragment_path: Optional JSON path within the artifact (e.g., "plan.steps.0").

        Returns:
            Fragment as string.
        """
        ...

    def list_artifacts(
        self, artifact_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List available artifacts.

        Args:
            artifact_type: Optional filter by artifact type.

        Returns:
            List of artifact metadata (id, type, path, etc.).
        """
        ...

    def list_neighbors(
        self, artifact_id: str, direction: str = "both"
    ) -> List[Dict[str, Any]]:
        """List immediate neighbors of an artifact.

        Args:
            artifact_id: Artifact identifier.
            direction: Direction - 'upstream', 'downstream', or 'both'.

        Returns:
            List of neighboring artifact metadata.
        """
        ...

    def expand_neighborhood(
        self,
        artifact_id: str,
        max_hops: int = 3,
        direction: str = "both",
    ) -> List[Dict[str, Any]]:
        """Expand neighborhood around an artifact.

        Args:
            artifact_id: Starting artifact.
            max_hops: Maximum number of hops.
            direction: Direction - 'upstream', 'downstream', or 'both'.

        Returns:
            List of artifacts in the neighborhood with hop distances.
        """
        ...

    def resolve_provenance(self, artifact_id: str) -> List[Dict[str, Any]]:
        """Resolve provenance chain for an artifact.

        Args:
            artifact_id: Artifact identifier.

        Returns:
            List of artifacts in the provenance chain (oldest first).
        """
        ...

    def search_artifacts(
        self, query: str, artifact_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Search artifacts by content or metadata.

        Args:
            query: Search query string.
            artifact_type: Optional filter by artifact type.

        Returns:
            List of matching artifacts with relevance scores.
        """
        ...


class FileSystemArtifactGraphProvider:
    """File system based artifact graph provider.

    Reads artifacts from JSON/YAML files in a run directory.
    Builds an in-memory graph of artifact relationships.
    """

    def __init__(
        self,
        run_dir: Path,
        run_id: Optional[str] = None,
    ):
        """Initialize the provider.

        Args:
            run_dir: Directory containing run artifacts.
            run_id: Optional run identifier (used in provenance).
        """
        self.run_dir = run_dir
        self.run_id = run_id or run_dir.name
        self._artifacts: Dict[str, Dict[str, Any]] = {}
        self._graph: Dict[str, Set[str]] = {}  # artifact_id -> neighbor ids
        self._reverse_graph: Dict[str, Set[str]] = {}  # artifact_id -> parent ids
        self._metadata: Dict[str, Dict[str, Any]] = {}  # artifact_id -> metadata
        self._load_artifacts()

    def _load_artifacts(self) -> None:
        """Load all artifacts from the run directory."""
        if not self.run_dir.exists():
            logger.warning("Run directory does not exist: %s", self.run_dir)
            return

        # Load JSON files
        for json_file in self.run_dir.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                artifact_id = json_file.stem
                self._register_artifact(artifact_id, data, str(json_file))
            except (json.JSONDecodeError, IOError) as e:
                logger.warning("Failed to load artifact %s: %s", json_file, e)

        # Load YAML files if available
        try:
            import yaml

            for yaml_file in self.run_dir.glob("*.yaml"):
                try:
                    with open(yaml_file, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f)
                    artifact_id = yaml_file.stem
                    self._register_artifact(artifact_id, data, str(yaml_file))
                except (yaml.YAMLError, IOError) as e:
                    logger.warning("Failed to load artifact %s: %s", yaml_file, e)
        except ImportError:
            pass  # YAML support is optional

        # Build graph relationships
        self._build_graph()

        logger.info(
            "Loaded %d artifacts from %s", len(self._artifacts), self.run_dir
        )

    def _register_artifact(
        self, artifact_id: str, data: Dict[str, Any], file_path: str
    ) -> None:
        """Register an artifact in the internal storage."""
        self._artifacts[artifact_id] = data
        self._metadata[artifact_id] = {
            "id": artifact_id,
            "type": self._infer_artifact_type(data),
            "path": file_path,
            "run_id": self.run_id,
        }

    def _infer_artifact_type(self, data: Dict[str, Any]) -> str:
        """Infer artifact type from data structure."""
        if "plan" in data or "steps" in data:
            return "plan"
        if "evidence" in data or "citations" in data:
            return "evidence"
        if "synthesis" in data or "answer" in data:
            return "synthesis"
        if "baseline" in data:
            return "baseline"
        if "events" in data or "trace" in data:
            return "trace"
        return "unknown"

    def _build_graph(self) -> None:
        """Build graph relationships based on artifact references."""
        for artifact_id in self._artifacts:
            self._graph.setdefault(artifact_id, set())
            self._reverse_graph.setdefault(artifact_id, set())

        for artifact_id, data in self._artifacts.items():
            # Find references to other artifacts
            refs = self._extract_references(data)
            for ref_id in refs:
                if ref_id in self._artifacts:
                    self._graph[artifact_id].add(ref_id)  # downstream
                    self._reverse_graph[ref_id].add(artifact_id)  # upstream

    def _extract_references(self, data: Any, depth: int = 0) -> Set[str]:
        """Extract artifact references from data."""
        refs: Set[str] = set()
        if depth > 10:  # Prevent infinite recursion
            return refs

        if isinstance(data, dict):
            # Check for explicit reference fields
            for key in ["artifact_ref", "artifact_id", "parent_id", "derived_from"]:
                if key in data and isinstance(data[key], str):
                    refs.add(data[key])

            # Recurse into values
            for value in data.values():
                refs.update(self._extract_references(value, depth + 1))

        elif isinstance(data, list):
            for item in data:
                refs.update(self._extract_references(item, depth + 1))

        return refs

    def get_artifact(self, artifact_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a complete artifact by ID."""
        return self._artifacts.get(artifact_id)

    def get_fragment(
        self, artifact_id: str, fragment_path: Optional[str] = None
    ) -> str:
        """Retrieve a fragment from an artifact."""
        artifact = self._artifacts.get(artifact_id)
        if artifact is None:
            raise ValueError(f"Artifact not found: {artifact_id}")

        if fragment_path is None:
            return json.dumps(artifact, ensure_ascii=False, indent=2)

        # Navigate the fragment path
        try:
            parts = fragment_path.split(".")
            current: Any = artifact
            for part in parts:
                if isinstance(current, dict):
                    current = current[part]
                elif isinstance(current, list):
                    idx = int(part)
                    current = current[idx]
                else:
                    raise ValueError(f"Cannot navigate into {type(current)}")

            if isinstance(current, str):
                return current
            return json.dumps(current, ensure_ascii=False, indent=2)

        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(
                f"Fragment path '{fragment_path}' not found in {artifact_id}: {e}"
            )

    def list_artifacts(
        self, artifact_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List available artifacts."""
        result = []
        for artifact_id, metadata in self._metadata.items():
            if artifact_type is None or metadata.get("type") == artifact_type:
                result.append(metadata)
        return result

    def list_neighbors(
        self, artifact_id: str, direction: str = "both"
    ) -> List[Dict[str, Any]]:
        """List immediate neighbors of an artifact."""
        neighbor_ids: Set[str] = set()

        if direction in ("downstream", "both"):
            neighbor_ids.update(self._graph.get(artifact_id, set()))
        if direction in ("upstream", "both"):
            neighbor_ids.update(self._reverse_graph.get(artifact_id, set()))

        return [
            self._metadata[nid] for nid in neighbor_ids if nid in self._metadata
        ]

    def expand_neighborhood(
        self,
        artifact_id: str,
        max_hops: int = 3,
        direction: str = "both",
    ) -> List[Dict[str, Any]]:
        """Expand neighborhood around an artifact."""
        if artifact_id not in self._artifacts:
            raise ValueError(f"Artifact not found: {artifact_id}")

        visited: Set[str] = {artifact_id}
        frontier: Set[str] = {artifact_id}
        result: List[Tuple[str, int]] = []  # (artifact_id, hop_distance)

        for hop in range(1, max_hops + 1):
            next_frontier: Set[str] = set()

            for current_id in frontier:
                # Get neighbors based on direction
                neighbors: Set[str] = set()
                if direction in ("downstream", "both"):
                    neighbors.update(self._graph.get(current_id, set()))
                if direction in ("upstream", "both"):
                    neighbors.update(self._reverse_graph.get(current_id, set()))

                for neighbor_id in neighbors:
                    if neighbor_id not in visited:
                        visited.add(neighbor_id)
                        next_frontier.add(neighbor_id)
                        result.append((neighbor_id, hop))

            frontier = next_frontier
            if not frontier:
                break

        return [
            {
                **self._metadata[aid],
                "hop_distance": hop,
            }
            for aid, hop in result
            if aid in self._metadata
        ]

    def resolve_provenance(self, artifact_id: str) -> List[Dict[str, Any]]:
        """Resolve provenance chain for an artifact."""
        if artifact_id not in self._artifacts:
            raise ValueError(f"Artifact not found: {artifact_id}")

        chain: List[str] = []
        visited: Set[str] = set()
        current = artifact_id

        while current and current not in visited:
            visited.add(current)
            parents = self._reverse_graph.get(current, set())
            if not parents:
                break
            # Take the first parent (could be enhanced for multiple parents)
            current = next(iter(parents))
            if current != artifact_id:
                chain.append(current)

        # Return in oldest-first order
        chain.reverse()
        return [
            {
                **self._metadata[aid],
                "artifact": self._artifacts.get(aid),
            }
            for aid in chain
            if aid in self._metadata
        ]

    def search_artifacts(
        self, query: str, artifact_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Search artifacts by content or metadata."""
        results: List[Dict[str, Any]] = []
        query_lower = query.lower()

        for artifact_id, artifact in self._artifacts.items():
            metadata = self._metadata.get(artifact_id, {})

            # Filter by type if specified
            if artifact_type and metadata.get("type") != artifact_type:
                continue

            # Search in metadata
            metadata_str = json.dumps(metadata, ensure_ascii=False).lower()
            if query_lower in metadata_str:
                results.append({**metadata, "relevance_score": 0.8})
                continue

            # Search in artifact content
            artifact_str = json.dumps(artifact, ensure_ascii=False).lower()
            if query_lower in artifact_str:
                # Calculate simple relevance based on match count
                match_count = artifact_str.count(query_lower)
                score = min(1.0, 0.5 + (match_count * 0.1))
                results.append({**metadata, "relevance_score": score})

        # Sort by relevance
        results.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
        return results


def create_artifact_graph_provider(
    run_dir: Path,
    run_id: Optional[str] = None,
) -> ArtifactGraphProvider:
    """Factory function to create an artifact graph provider.

    Args:
        run_dir: Directory containing run artifacts.
        run_id: Optional run identifier.

    Returns:
        ArtifactGraphProvider instance.
    """
    return FileSystemArtifactGraphProvider(run_dir=run_dir, run_id=run_id)


__all__ = [
    "ArtifactGraphProvider",
    "FileSystemArtifactGraphProvider",
    "create_artifact_graph_provider",
]
