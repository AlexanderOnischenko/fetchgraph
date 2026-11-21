"""Adapters for integrating artifact context engine with agent pipelines."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from ..artifact_context import fetch as artifact_context_fetch
from ..artifact_context.models import (
    CaseDossier,
    ContextRequest,
    EvidencePack,
    ExplainPack,
    RetrievalPolicy,
    ValidatorDefectPack,
)
from ..core.protocols import ContextProvider

logger = logging.getLogger(__name__)


class ArtifactContextProvider:
    """Artifact context provider for agent pipeline integration.

    This provider wraps the artifact context engine and exposes it
    as a ContextProvider that can be used in agent pipelines.

    Example usage:
        provider = ArtifactContextProvider(run_dir=Path("/path/to/run"))
        result = provider.fetch(
            feature_name="EXPLAIN_TARGET",
            selectors={"target_id": "AS-017"},
            run_id="run-123",
        )
    """

    name = "artifact_context"

    def __init__(
        self,
        run_dir: Path,
        run_id: Optional[str] = None,
        llm_invoke: Optional[Callable[[str, str], str]] = None,
        default_pipeline_id: str = "archgraph_v1",
    ):
        """Initialize the provider.

        Args:
            run_dir: Directory containing run artifacts.
            run_id: Optional run identifier (defaults to run_dir name).
            llm_invoke: Optional LLM invocation function.
            default_pipeline_id: Default pipeline schema ID.
        """
        self.run_dir = run_dir
        self.run_id = run_id or run_dir.name
        self.llm_invoke = llm_invoke
        self.default_pipeline_id = default_pipeline_id

        logger.info(
            "ArtifactContextProvider initialized run_dir=%s run_id=%s",
            run_dir,
            self.run_id,
        )

    def fetch(
        self,
        feature_name: str,
        selectors: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Fetch artifact context for a feature.

        Args:
            feature_name: Query family name (e.g., 'EXPLAIN_TARGET').
            selectors: Selectors specifying what to fetch (e.g., target_id, case_id).
            **kwargs: Additional options (run_id, pipeline_id, policy, etc.).

        Returns:
            Dictionary containing the evidence/explanation package.

        Raises:
            ValueError: If required selectors are missing.
        """
        # Build context request
        query_family = self._map_feature_name_to_query_family(feature_name)
        anchors = self._build_anchors(selectors or {})
        policy = self._build_policy(kwargs)
        pipeline_id = kwargs.get("pipeline_id", self.default_pipeline_id)
        run_id = kwargs.get("run_id", self.run_id)

        request = ContextRequest(
            pipeline_id=pipeline_id,
            query_family=query_family,
            run_id=run_id,
            anchors=anchors,
            options=kwargs.get("options", {}),
            policy=policy,
        )

        logger.info(
            "Fetching artifact context query_family=%s anchors=%s",
            query_family,
            anchors,
        )

        # Execute fetch
        result = artifact_context_fetch(
            request=request,
            run_dir=self.run_dir,
            llm_invoke=self.llm_invoke,
        )

        # Convert to dictionary
        return self._result_to_dict(result)

    def serialize(self, obj: Any) -> str:
        """Serialize a result to string.

        Args:
            obj: Result object (EvidencePack, ExplainPack, etc.).

        Returns:
            JSON string representation.
        """
        if isinstance(obj, dict):
            return json.dumps(obj, ensure_ascii=False, indent=2)
        if hasattr(obj, "model_dump"):
            return json.dumps(
                obj.model_dump(mode="json"), ensure_ascii=False, indent=2
            )
        return str(obj)

    def _map_feature_name_to_query_family(self, feature_name: str) -> str:
        """Map feature name to query family.

        Supports both explicit query family names and feature-based mapping.
        """
        # Direct mapping for query family names
        query_families = {
            "EXPLAIN_TARGET",
            "EXPLAIN_DERIVATION",
            "CASE_DOSSIER",
            "VALIDATOR_DEFECT_PACK",
        }
        if feature_name in query_families:
            return feature_name

        # Feature-based mapping
        feature_lower = feature_name.lower()
        if "explain" in feature_lower and "target" in feature_lower:
            return "EXPLAIN_TARGET"
        if "explain" in feature_lower and "derivation" in feature_lower:
            return "EXPLAIN_DERIVATION"
        if "case" in feature_lower or "dossier" in feature_lower:
            return "CASE_DOSSIER"
        if "defect" in feature_lower or "validator" in feature_lower:
            return "VALIDATOR_DEFECT_PACK"

        # Default to EXPLAIN_TARGET
        logger.warning(
            "Unknown feature_name=%s, defaulting to EXPLAIN_TARGET", feature_name
        )
        return "EXPLAIN_TARGET"

    def _build_anchors(self, selectors: Dict[str, Any]) -> Dict[str, Any]:
        """Build anchors from selectors."""
        anchors = dict(selectors)

        # Handle common selector patterns
        if "target_id" in selectors:
            anchors["target_id"] = selectors["target_id"]
        if "case_id" in selectors:
            anchors["case_id"] = selectors["case_id"]
        if "defect_id" in selectors:
            anchors["defect_id"] = selectors["defect_id"]
        if "validator" in selectors:
            anchors["validator_name"] = selectors["validator"]
        if "validator_name" in selectors:
            anchors["validator_name"] = selectors["validator_name"]
        if "source_id" in selectors:
            anchors["source_id"] = selectors["source_id"]

        return anchors

    def _build_policy(self, kwargs: Dict[str, Any]) -> RetrievalPolicy:
        """Build retrieval policy from kwargs."""
        policy_data = {}

        if "mode" in kwargs:
            policy_data["mode"] = kwargs["mode"]
        if "max_llm_calls" in kwargs:
            policy_data["max_llm_calls"] = kwargs["max_llm_calls"]
        if "max_steps" in kwargs:
            policy_data["max_steps"] = kwargs["max_steps"]
        if "max_capsules" in kwargs:
            policy_data["max_capsules"] = kwargs["max_capsules"]
        if "max_quotes" in kwargs:
            policy_data["max_quotes"] = kwargs["max_quotes"]
        if "max_total_chars" in kwargs:
            policy_data["max_total_chars"] = kwargs["max_total_chars"]
        if "max_hops_upstream" in kwargs:
            policy_data["max_hops_upstream"] = kwargs["max_hops_upstream"]
        if "max_hops_downstream" in kwargs:
            policy_data["max_hops_downstream"] = kwargs["max_hops_downstream"]
        if "require_traceability" in kwargs:
            policy_data["require_traceability"] = kwargs["require_traceability"]

        return RetrievalPolicy(**policy_data) if policy_data else RetrievalPolicy()

    def _result_to_dict(
        self,
        result: Union[EvidencePack, ExplainPack, CaseDossier, ValidatorDefectPack],
    ) -> Dict[str, Any]:
        """Convert result to dictionary."""
        if hasattr(result, "model_dump"):
            return result.model_dump(mode="json")
        if isinstance(result, dict):
            return result
        return {"data": result}


def create_artifact_context_provider(
    run_dir: Path,
    run_id: Optional[str] = None,
    llm_invoke: Optional[Callable[[str, str], str]] = None,
) -> ArtifactContextProvider:
    """Factory function to create an artifact context provider.

    Args:
        run_dir: Directory containing run artifacts.
        run_id: Optional run identifier.
        llm_invoke: Optional LLM invocation function.

    Returns:
        ArtifactContextProvider instance.
    """
    return ArtifactContextProvider(run_dir=run_dir, run_id=run_id, llm_invoke=llm_invoke)


__all__ = [
    "ArtifactContextProvider",
    "create_artifact_context_provider",
]
