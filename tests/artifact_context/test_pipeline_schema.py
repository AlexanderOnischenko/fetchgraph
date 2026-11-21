"""Tests for pipeline schema module."""

import json
import tempfile
from pathlib import Path
import pytest

from fetchgraph.pipeline_schema.schema import (
    QueryTemplate,
    RelationRule,
    ArtifactTypeSchema,
    PipelineSchema,
    create_default_pipeline_schema,
    load_pipeline_schema,
    save_pipeline_schema,
)
from fetchgraph.pipeline_schema.loader import (
    SchemaRegistry,
    get_schema,
    get_registry,
)


class TestQueryTemplate:
    """Tests for QueryTemplate model."""

    def test_minimal_template(self):
        """Test minimal query template."""
        template = QueryTemplate(
            name="TEST_QUERY",
            description="Test query",
        )
        assert template.name == "TEST_QUERY"
        assert template.default_mode == "deterministic"
        assert template.output_kind == "evidence_pack"

    def test_full_template(self):
        """Test template with all fields."""
        template = QueryTemplate(
            name="EXPLAIN_TARGET",
            description="Explain a target",
            default_mode="assisted",
            seed_policy="explicit",
            output_kind="explain_pack",
            required_anchors=["target_id"],
            optional_anchors=["context"],
            default_policy={"max_hops_upstream": 3},
        )
        assert template.default_mode == "assisted"
        assert template.required_anchors == ["target_id"]
        assert template.default_policy == {"max_hops_upstream": 3}


class TestRelationRule:
    """Tests for RelationRule model."""

    def test_minimal_rule(self):
        """Test minimal relation rule."""
        rule = RelationRule(
            from_type="plan",
            to_type="evidence",
            via_field="evidence_refs",
        )
        assert rule.cardinality == "many-to-many"
        assert rule.supports_explain is True
        assert rule.direction == "bidirectional"
        assert rule.weight == 1.0

    def test_full_rule(self):
        """Test rule with all fields."""
        rule = RelationRule(
            from_type="evidence",
            to_type="synthesis",
            via_field="synthesis_refs",
            cardinality="many-to-one",
            supports_explain=True,
            supports_root_cause=False,
            direction="downstream",
            weight=0.8,
        )
        assert rule.cardinality == "many-to-one"
        assert rule.direction == "downstream"
        assert rule.weight == 0.8


class TestArtifactTypeSchema:
    """Tests for ArtifactTypeSchema model."""

    def test_minimal_schema(self):
        """Test minimal artifact type schema."""
        schema = ArtifactTypeSchema(type_name="plan")
        assert schema.type_name == "plan"
        assert schema.stable_keys == []
        assert schema.ref_fields == []

    def test_full_schema(self):
        """Test schema with all fields."""
        schema = ArtifactTypeSchema(
            type_name="evidence",
            stable_keys=["id", "provider"],
            id_fields=["id"],
            ref_fields=["plan_ref"],
            provenance_fields=["created_at", "run_id"],
            evidence_fields=["data", "text"],
            description="Evidence artifact",
        )
        assert schema.stable_keys == ["id", "provider"]
        assert schema.evidence_fields == ["data", "text"]


class TestPipelineSchema:
    """Tests for PipelineSchema model."""

    def test_default_schema(self):
        """Test default pipeline schema."""
        schema = PipelineSchema()
        assert schema.pipeline_id == "archgraph_v1"
        assert schema.query_templates == {}
        assert schema.relation_rules == []

    def test_schema_with_templates(self):
        """Test schema with query templates."""
        template = QueryTemplate(name="TEST", description="Test")
        schema = PipelineSchema(
            query_templates={"TEST": template},
        )
        assert schema.get_query_template("TEST") == template
        assert schema.get_query_template("NONEXISTENT") is None

    def test_schema_with_rules(self):
        """Test schema with relation rules."""
        rule = RelationRule(from_type="plan", to_type="evidence", via_field="refs")
        schema = PipelineSchema(relation_rules=[rule])
        
        rules = schema.get_relation_rules(from_type="plan")
        assert len(rules) == 1
        assert rules[0] == rule

    def test_schema_with_type_schemas(self):
        """Test schema with artifact type schemas."""
        type_schema = ArtifactTypeSchema(type_name="plan")
        schema = PipelineSchema(
            artifact_type_schemas={"plan": type_schema},
        )
        assert schema.get_artifact_type_schema("plan") == type_schema
        assert schema.get_artifact_type_schema("nonexistent") is None

    def test_find_reference_fields(self):
        """Test finding reference fields."""
        type_schema = ArtifactTypeSchema(
            type_name="evidence",
            ref_fields=["plan_ref", "synthesis_refs"],
        )
        schema = PipelineSchema(
            artifact_type_schemas={"evidence": type_schema},
        )
        fields = schema.find_reference_fields("evidence")
        assert fields == ["plan_ref", "synthesis_refs"]


class TestCreateDefaultPipelineSchema:
    """Tests for create_default_pipeline_schema function."""

    def test_creates_schema(self):
        """Test that default schema is created."""
        schema = create_default_pipeline_schema()
        assert schema.pipeline_id == "archgraph_v1"
        assert len(schema.query_templates) > 0
        assert len(schema.relation_rules) > 0
        assert len(schema.artifact_type_schemas) > 0

    def test_has_standard_query_templates(self):
        """Test that standard query templates are present."""
        schema = create_default_pipeline_schema()
        assert "EXPLAIN_TARGET" in schema.query_templates
        assert "EXPLAIN_DERIVATION" in schema.query_templates
        assert "CASE_DOSSIER" in schema.query_templates
        assert "VALIDATOR_DEFECT_PACK" in schema.query_templates

    def test_has_standard_relation_rules(self):
        """Test that standard relation rules are present."""
        schema = create_default_pipeline_schema()
        # Should have rules for plan->evidence, evidence->synthesis, etc.
        assert len(schema.relation_rules) >= 3

    def test_has_standard_artifact_types(self):
        """Test that standard artifact types are present."""
        schema = create_default_pipeline_schema()
        assert "plan" in schema.artifact_type_schemas
        assert "evidence" in schema.artifact_type_schemas
        assert "synthesis" in schema.artifact_type_schemas


class TestLoadSavePipelineSchema:
    """Tests for loading and saving pipeline schemas."""

    def test_save_and_load(self):
        """Test saving and loading a schema."""
        schema = create_default_pipeline_schema()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "schema.json"
            save_pipeline_schema(schema, path)
            
            loaded = load_pipeline_schema(path)
            assert loaded.pipeline_id == schema.pipeline_id
            assert len(loaded.query_templates) == len(schema.query_templates)

    def test_load_invalid_json(self):
        """Test loading invalid JSON raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "invalid.json"
            with open(path, "w") as f:
                f.write("not valid json")
            
            with pytest.raises(ValueError):
                load_pipeline_schema(path)


class TestSchemaRegistry:
    """Tests for SchemaRegistry."""

    def test_register_and_get(self):
        """Test registering and retrieving schemas."""
        registry = SchemaRegistry()
        schema = PipelineSchema(pipeline_id="test_v1")
        registry.register(schema)
        
        retrieved = registry.get("test_v1")
        assert retrieved == schema

    def test_get_or_default(self):
        """Test get_or_default creates default for archgraph_v1."""
        registry = SchemaRegistry()
        schema = registry.get_or_default("archgraph_v1")
        assert schema is not None
        assert schema.pipeline_id == "archgraph_v1"

    def test_get_unknown_raises(self):
        """Test get_or_default raises for unknown schema."""
        registry = SchemaRegistry()
        with pytest.raises(ValueError, match="Unknown pipeline"):
            registry.get_or_default("unknown_v1")

    def test_default_property(self):
        """Test default property."""
        registry = SchemaRegistry()
        default = registry.default
        assert default is not None
        assert default.pipeline_id == "archgraph_v1"

    def test_load_from_file(self):
        """Test loading schema from file."""
        registry = SchemaRegistry()
        schema = create_default_pipeline_schema()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "schema.json"
            save_pipeline_schema(schema, path)
            
            loaded = registry.load(path)
            assert loaded.pipeline_id == schema.pipeline_id
            
            # Should be registered
            retrieved = registry.get(schema.pipeline_id)
            assert retrieved == loaded


class TestGlobalRegistry:
    """Tests for global registry functions."""

    def test_get_schema(self):
        """Test get_schema function."""
        schema = get_schema("archgraph_v1")
        assert schema is not None
        assert schema.pipeline_id == "archgraph_v1"

    def test_get_registry_singleton(self):
        """Test that get_registry returns same instance."""
        registry1 = get_registry()
        registry2 = get_registry()
        assert registry1 is registry2
