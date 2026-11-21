"""Pipeline Schema - extended schema for artifact context engine.

This module extends the base pipeline schema with:
- QueryTemplate: families of queries with default modes and policies
- RelationRule: rules for traversing artifact relationships
- ArtifactTypeSchema: schema definitions for artifact types
- PipelineSchema: complete pipeline schema with extensions
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class QueryTemplate(BaseModel):
    """Description of a query family.

    Attributes:
        name: Query family name (e.g., 'EXPLAIN_TARGET').
        description: Human-readable description.
        default_mode: Default retrieval mode.
        seed_policy: Policy for selecting seed artifacts.
        output_kind: Expected output type.
        required_anchors: Required anchor fields.
        optional_anchors: Optional anchor fields.
        default_policy: Default retrieval policy settings.
    """

    name: str
    description: str
    default_mode: Literal["deterministic", "assisted", "investigator"] = "deterministic"
    seed_policy: str = "explicit"  # 'explicit', 'search', 'latest'
    output_kind: Literal["evidence_pack", "explain_pack", "case_dossier", "validator_defect_pack"] = "evidence_pack"
    required_anchors: List[str] = Field(default_factory=list)
    optional_anchors: List[str] = Field(default_factory=list)
    default_policy: Dict[str, Any] = Field(default_factory=dict)


class RelationRule(BaseModel):
    """Rule for traversing artifact relationships.

    Attributes:
        from_type: Source artifact type.
        to_type: Target artifact type.
        via_field: Field name containing the relationship.
        cardinality: Relationship cardinality ('one-to-one', 'one-to-many', 'many-to-one', 'many-to-many').
        supports_explain: Whether this relationship supports explanation.
        supports_root_cause: Whether this relationship supports root cause analysis.
        direction: Direction of traversal ('upstream', 'downstream', 'bidirectional').
        weight: Optional weight for prioritization (higher = more important).
    """

    from_type: str
    to_type: str
    via_field: str
    cardinality: Literal["one-to-one", "one-to-many", "many-to-one", "many-to-many"] = "many-to-many"
    supports_explain: bool = True
    supports_root_cause: bool = False
    direction: Literal["upstream", "downstream", "bidirectional"] = "bidirectional"
    weight: float = 1.0


class ArtifactTypeSchema(BaseModel):
    """Schema definition for an artifact type.

    Attributes:
        type_name: Name of the artifact type.
        stable_keys: Keys that are stable across versions.
        id_fields: Fields that uniquely identify an instance.
        ref_fields: Fields that contain references to other artifacts.
        provenance_fields: Fields containing provenance information.
        evidence_fields: Fields containing evidence or content.
        description: Human-readable description.
    """

    type_name: str
    stable_keys: List[str] = Field(default_factory=list)
    id_fields: List[str] = Field(default_factory=list)
    ref_fields: List[str] = Field(default_factory=list)
    provenance_fields: List[str] = Field(default_factory=list)
    evidence_fields: List[str] = Field(default_factory=list)
    description: str = ""


class PipelineSchema(BaseModel):
    """Extended pipeline schema with artifact context support.

    Attributes:
        pipeline_id: Unique pipeline identifier.
        version: Schema version.
        description: Pipeline description.
        query_templates: Registered query templates.
        relation_rules: Rules for artifact relationships.
        artifact_type_schemas: Schema definitions for artifact types.
    """

    pipeline_id: str = "archgraph_v1"
    version: str = "1.0.0"
    description: str = ""
    query_templates: Dict[str, QueryTemplate] = Field(default_factory=dict)
    relation_rules: List[RelationRule] = Field(default_factory=list)
    artifact_type_schemas: Dict[str, ArtifactTypeSchema] = Field(default_factory=dict)

    def get_query_template(self, query_family: str) -> Optional[QueryTemplate]:
        """Get a query template by name."""
        return self.query_templates.get(query_family)

    def get_relation_rules(
        self, from_type: Optional[str] = None, to_type: Optional[str] = None
    ) -> List[RelationRule]:
        """Get relation rules matching optional type filters."""
        rules = self.relation_rules
        if from_type:
            rules = [r for r in rules if r.from_type == from_type]
        if to_type:
            rules = [r for r in rules if r.to_type == to_type]
        return rules

    def get_artifact_type_schema(self, type_name: str) -> Optional[ArtifactTypeSchema]:
        """Get schema for an artifact type."""
        return self.artifact_type_schemas.get(type_name)

    def find_reference_fields(self, artifact_type: str) -> List[str]:
        """Find reference fields for an artifact type."""
        schema = self.get_artifact_type_schema(artifact_type)
        if schema:
            return schema.ref_fields
        return []


def create_default_pipeline_schema() -> PipelineSchema:
    """Create a default pipeline schema with standard query templates and rules."""

    # Define standard query templates
    query_templates = {
        "EXPLAIN_TARGET": QueryTemplate(
            name="EXPLAIN_TARGET",
            description="Explain why a specific target is present in results",
            default_mode="assisted",
            seed_policy="explicit",
            output_kind="explain_pack",
            required_anchors=["target_id"],
            optional_anchors=["context"],
            default_policy={
                "max_hops_upstream": 3,
                "max_hops_downstream": 1,
                "require_traceability": True,
            },
        ),
        "EXPLAIN_DERIVATION": QueryTemplate(
            name="EXPLAIN_DERIVATION",
            description="Explain how artifacts derive from other artifacts",
            default_mode="assisted",
            seed_policy="explicit",
            output_kind="explain_pack",
            required_anchors=["source_id", "target_id"],
            default_policy={
                "max_hops_upstream": 5,
                "max_hops_downstream": 5,
                "require_traceability": True,
            },
        ),
        "CASE_DOSSIER": QueryTemplate(
            name="CASE_DOSSIER",
            description="Assemble complete evidence package for a case",
            default_mode="investigator",
            seed_policy="explicit",
            output_kind="case_dossier",
            required_anchors=["case_id"],
            optional_anchors=["include_upstream", "max_depth"],
            default_policy={
                "max_hops_upstream": 5,
                "max_hops_downstream": 3,
                "max_capsules": 20,
            },
        ),
        "VALIDATOR_DEFECT_PACK": QueryTemplate(
            name="VALIDATOR_DEFECT_PACK",
            description="Prepare evidence package for a validator defect",
            default_mode="assisted",
            seed_policy="search",
            output_kind="validator_defect_pack",
            required_anchors=["defect_id", "validator_name"],
            default_policy={
                "max_hops_upstream": 3,
                "require_traceability": True,
            },
        ),
    }

    # Define standard relation rules
    relation_rules = [
        RelationRule(
            from_type="plan",
            to_type="evidence",
            via_field="evidence_refs",
            cardinality="one-to-many",
            supports_explain=True,
            direction="downstream",
            weight=1.0,
        ),
        RelationRule(
            from_type="evidence",
            to_type="synthesis",
            via_field="synthesis_refs",
            cardinality="many-to-one",
            supports_explain=True,
            direction="downstream",
            weight=1.0,
        ),
        RelationRule(
            from_type="synthesis",
            to_type="plan",
            via_field="plan_ref",
            cardinality="many-to-one",
            supports_explain=True,
            supports_root_cause=True,
            direction="upstream",
            weight=1.0,
        ),
        RelationRule(
            from_type="baseline",
            to_type="evidence",
            via_field="baseline_refs",
            cardinality="one-to-many",
            supports_explain=True,
            direction="downstream",
            weight=0.8,
        ),
        RelationRule(
            from_type="trace",
            to_type="plan",
            via_field="plan_id",
            cardinality="many-to-one",
            supports_explain=False,
            supports_root_cause=True,
            direction="upstream",
            weight=0.5,
        ),
    ]

    # Define standard artifact type schemas
    artifact_type_schemas = {
        "plan": ArtifactTypeSchema(
            type_name="plan",
            stable_keys=["id", "version", "feature_name"],
            id_fields=["id"],
            ref_fields=["evidence_refs", "baseline_refs"],
            provenance_fields=["created_at", "run_id", "llm_model"],
            evidence_fields=["steps", "selectors"],
            description="Planning artifact containing retrieval plan",
        ),
        "evidence": ArtifactTypeSchema(
            type_name="evidence",
            stable_keys=["id", "provider", "feature_name"],
            id_fields=["id"],
            ref_fields=["plan_ref", "synthesis_refs"],
            provenance_fields=["created_at", "run_id", "tokens"],
            evidence_fields=["data", "text"],
            description="Evidence artifact containing fetched data",
        ),
        "synthesis": ArtifactTypeSchema(
            type_name="synthesis",
            stable_keys=["id", "feature_name"],
            id_fields=["id"],
            ref_fields=["evidence_refs", "plan_ref"],
            provenance_fields=["created_at", "run_id", "llm_model"],
            evidence_fields=["answer", "reasoning"],
            description="Synthesis artifact containing final answer",
        ),
        "baseline": ArtifactTypeSchema(
            type_name="baseline",
            stable_keys=["id", "provider"],
            id_fields=["id"],
            ref_fields=["evidence_refs"],
            provenance_fields=["created_at", "run_id"],
            evidence_fields=["data", "text"],
            description="Baseline artifact containing required context",
        ),
        "trace": ArtifactTypeSchema(
            type_name="trace",
            stable_keys=["id", "run_id"],
            id_fields=["id"],
            ref_fields=["plan_id", "event_refs"],
            provenance_fields=["created_at", "run_id"],
            evidence_fields=["events", "logs"],
            description="Trace artifact containing execution trace",
        ),
    }

    return PipelineSchema(
        pipeline_id="archgraph_v1",
        version="1.0.0",
        description="Default pipeline schema for artifact context engine",
        query_templates=query_templates,
        relation_rules=relation_rules,
        artifact_type_schemas=artifact_type_schemas,
    )


def load_pipeline_schema(path: Path) -> PipelineSchema:
    """Load a pipeline schema from a JSON or YAML file.

    Args:
        path: Path to the schema file.

    Returns:
        PipelineSchema instance.
    """
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        # Try YAML
        try:
            import yaml

            data = yaml.safe_load(content)
        except ImportError:
            raise ValueError("YAML support not available and JSON parsing failed")
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse YAML schema: {e}")

    return PipelineSchema.model_validate(data)


def save_pipeline_schema(schema: PipelineSchema, path: Path) -> None:
    """Save a pipeline schema to a JSON file.

    Args:
        schema: Schema to save.
        path: Output file path.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(schema.model_dump(mode="json"), f, indent=2, ensure_ascii=False)


__all__ = [
    "QueryTemplate",
    "RelationRule",
    "ArtifactTypeSchema",
    "PipelineSchema",
    "create_default_pipeline_schema",
    "load_pipeline_schema",
    "save_pipeline_schema",
]
