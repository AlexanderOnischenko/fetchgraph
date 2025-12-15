from __future__ import annotations

from pathlib import Path
from typing import Optional

from fetchgraph.relational.schema import (
    SchemaConfig,
    build_csv_semantic_backend,
    build_pandas_provider_from_schema,
)

from .schema_io import load_schema


def build_provider(
    data_dir: Path,
    schema_path: Path,
    *,
    enable_semantic: bool = False,
    embedding_model: Optional[object] = None,
) -> tuple[object, SchemaConfig]:
    """Build a pandas provider from schema/data directory."""

    schema = load_schema(schema_path)
    if enable_semantic:
        semantic_backend = build_csv_semantic_backend(data_dir, schema, embedding_model=embedding_model)
    else:
        semantic_backend = None
    provider = build_pandas_provider_from_schema(data_dir, schema, semantic_backend=semantic_backend)
    return provider, schema


__all__ = ["build_provider"]
