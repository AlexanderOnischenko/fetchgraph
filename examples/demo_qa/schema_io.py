from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import json

from fetchgraph.relational.schema import ColumnConfig, EntityConfig, RelationConfig, SchemaConfig


def _entity_from_dict(data: Dict[str, Any]) -> EntityConfig:
    columns = [ColumnConfig(**c) for c in data.get("columns", [])]
    return EntityConfig(
        name=data["name"],
        label=data.get("label", ""),
        source=data.get("source"),
        columns=columns,
        semantic_text_fields=data.get("semantic_text_fields", []),
        planning_hint=data.get("planning_hint", ""),
    )


def _relation_from_dict(data: Dict[str, Any]) -> RelationConfig:
    return RelationConfig(
        name=data["name"],
        from_entity=data["from_entity"],
        from_column=data["from_column"],
        to_entity=data["to_entity"],
        to_column=data["to_column"],
        cardinality=data["cardinality"],
        semantic_hint=data.get("semantic_hint"),
        planning_hint=data.get("planning_hint", ""),
    )


def load_schema(path: Path) -> SchemaConfig:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    entities = [_entity_from_dict(e) for e in data.get("entities", [])]
    relations = [_relation_from_dict(r) for r in data.get("relations", [])]
    return SchemaConfig(
        name=data["name"],
        label=data.get("label", ""),
        description=data.get("description", ""),
        planning_hints=data.get("planning_hints", []),
        examples=data.get("examples", []),
        entities=entities,
        relations=relations,
    )


def save_schema(schema: SchemaConfig, path: Path) -> None:
    from .data_gen import save_schema as _save

    _save(schema, path)


__all__ = ["load_schema", "save_schema"]
