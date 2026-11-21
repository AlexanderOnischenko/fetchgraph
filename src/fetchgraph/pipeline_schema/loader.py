"""Pipeline Schema Loader - loads and manages pipeline schemas."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

from .schema import (
    PipelineSchema,
    create_default_pipeline_schema,
    load_pipeline_schema,
)

logger = logging.getLogger(__name__)


class SchemaRegistry:
    """Registry for pipeline schemas.

    Provides access to loaded schemas with caching.
    """

    def __init__(self) -> None:
        """Initialize the registry."""
        self._schemas: Dict[str, PipelineSchema] = {}
        self._default_schema: Optional[PipelineSchema] = None

    def register(self, schema: PipelineSchema) -> None:
        """Register a schema.

        Args:
            schema: Schema to register.
        """
        self._schemas[schema.pipeline_id] = schema
        logger.info("Registered pipeline schema: %s", schema.pipeline_id)

    def get(self, pipeline_id: str) -> Optional[PipelineSchema]:
        """Get a schema by ID.

        Args:
            pipeline_id: Schema identifier.

        Returns:
            Schema or None if not found.
        """
        return self._schemas.get(pipeline_id)

    def load(self, path: Path, pipeline_id: Optional[str] = None) -> PipelineSchema:
        """Load a schema from file and register it.

        Args:
            path: Path to schema file.
            pipeline_id: Optional override for schema ID.

        Returns:
            Loaded schema.
        """
        schema = load_pipeline_schema(path)
        if pipeline_id:
            schema = schema.model_copy(update={"pipeline_id": pipeline_id})
        self.register(schema)
        return schema

    def get_or_default(self, pipeline_id: str) -> PipelineSchema:
        """Get a schema or return/create default.

        Args:
            pipeline_id: Schema identifier.

        Returns:
            Schema instance.
        """
        schema = self._schemas.get(pipeline_id)
        if schema is None:
            if pipeline_id == "archgraph_v1":
                schema = create_default_pipeline_schema()
                self.register(schema)
            else:
                raise ValueError(f"Unknown pipeline schema: {pipeline_id}")
        return schema

    @property
    def default(self) -> PipelineSchema:
        """Get or create the default schema."""
        if self._default_schema is None:
            self._default_schema = create_default_pipeline_schema()
            self.register(self._default_schema)
        return self._default_schema


# Global registry instance
_registry: Optional[SchemaRegistry] = None


def get_registry() -> SchemaRegistry:
    """Get the global schema registry."""
    global _registry
    if _registry is None:
        _registry = SchemaRegistry()
    return _registry


def get_schema(pipeline_id: str) -> PipelineSchema:
    """Get a schema from the global registry.

    Args:
        pipeline_id: Schema identifier.

    Returns:
        Schema instance.
    """
    return get_registry().get_or_default(pipeline_id)


def load_schema(path: Path, pipeline_id: Optional[str] = None) -> PipelineSchema:
    """Load a schema and register it in the global registry.

    Args:
        path: Path to schema file.
        pipeline_id: Optional override for schema ID.

    Returns:
        Loaded schema.
    """
    return get_registry().load(path, pipeline_id)


__all__ = [
    "SchemaRegistry",
    "get_registry",
    "get_schema",
    "load_schema",
]
