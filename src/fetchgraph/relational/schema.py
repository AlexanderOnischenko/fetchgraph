# src/fetchgraph/relational_schema.py

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional, overload
import warnings

import pandas as pd  # type: ignore[import]

from ..protocols import ContextProvider
from .models import ColumnDescriptor, EntityDescriptor, RelationDescriptor, RelationJoin
from ..semantic_backend import (
    CsvSemanticBackend,
    CsvSemanticSource,
    CsvEmbeddingBuilder,
    EmbeddingModel,
    SemanticBackend,
)
from .providers.pandas import PandasRelationalDataProvider
from .providers.sql import SqlRelationalDataProvider

BackendKind = Literal["pandas", "sql"]

Cardinality = Literal["1_to_1", "1_to_many", "many_to_1", "many_to_many"]

@dataclass
class ColumnConfig:
    name: str
    type: Optional[str] = None
    pk: bool = False
    semantic: bool = False


@dataclass
class EntityConfig:
    name: str
    label: str = ""
    source: str | None = None                 # CSV-файл или имя таблицы
    columns: List[ColumnConfig] = field(default_factory=list)
    semantic_text_fields: List[str] = field(default_factory=list)
    # опциональная подсказка для планировщика, попадёт в description
    planning_hint: str = ""


@dataclass
class RelationConfig:
    name: str
    from_entity: str
    from_column: str
    to_entity: str
    to_column: str
    cardinality: Cardinality
    semantic_hint: str | None = None          # уже есть в RelationDescriptor
    planning_hint: str = ""                   # доп. подсказка для LLM


@dataclass
class SchemaConfig:
    name: str                                  # имя провайдера
    label: str = ""                            # человекочитаемое имя
    description: str = ""                      # общая описаловка провайдера
    entities: List[EntityConfig] = field(default_factory=list)
    relations: List[RelationConfig] = field(default_factory=list)
    planning_hints: List[str] = field(default_factory=list)   # общие подсказки
    examples: List[str] = field(default_factory=list)         # если хочешь переопределить auto-examples

# ------------------------ Валидация схемы ------------------------

def validate_schema(schema: SchemaConfig) -> None:
    entity_index: Dict[str, EntityConfig] = {e.name: e for e in schema.entities}

    for ent in schema.entities:
        if not ent.source:
            warnings.warn(f"[Schema {schema.name}] Entity '{ent.name}' has no source file/table.")
        if not ent.columns:
            warnings.warn(f"[Schema {schema.name}] Entity '{ent.name}' has no columns.")
        pk_cols = [c.name for c in ent.columns if c.pk]
        if len(pk_cols) == 0:
            warnings.warn(f"[Schema {schema.name}] Entity '{ent.name}' has no primary key.")
        elif len(pk_cols) > 1:
            warnings.warn(
                f"[Schema {schema.name}] Entity '{ent.name}' has multiple PK columns {pk_cols}; "
                "composite PK support is limited."
            )
        for fname in ent.semantic_text_fields:
            if fname not in {c.name for c in ent.columns}:
                warnings.warn(
                    f"[Schema {schema.name}] Entity '{ent.name}' semantic_text_fields refs "
                    f"unknown column '{fname}'."
                )

    for rel in schema.relations:
        if rel.from_entity not in entity_index:
            warnings.warn(
                f"[Schema {schema.name}] Relation '{rel.name}' refers to unknown from_entity '{rel.from_entity}'."
            )
            continue
        if rel.to_entity not in entity_index:
            warnings.warn(
                f"[Schema {schema.name}] Relation '{rel.name}' refers to unknown to_entity '{rel.to_entity}'."
            )
            continue

        from_cols = {c.name for c in entity_index[rel.from_entity].columns}
        to_cols = {c.name for c in entity_index[rel.to_entity].columns}
        if rel.from_column not in from_cols:
            warnings.warn(
                f"[Schema {schema.name}] Relation '{rel.name}' from_column '{rel.from_column}' "
                f"not in entity '{rel.from_entity}'."
            )
        if rel.to_column not in to_cols:
            warnings.warn(
                f"[Schema {schema.name}] Relation '{rel.name}' to_column '{rel.to_column}' "
                f"not in entity '{rel.to_entity}'."
            )


# ------------------------ Descriptor’ы / PK ------------------------

def _build_entity_descriptors(schema: SchemaConfig) -> List[EntityDescriptor]:
    ents: List[EntityDescriptor] = []
    for ent in schema.entities:
        cols = [
            ColumnDescriptor(
                name=c.name,
                type=c.type or "text",
                role="primary_key" if c.pk else None,
                semantic=c.semantic,
            )
            for c in ent.columns
        ]
        ents.append(
            EntityDescriptor(
                name=ent.name,
                label=ent.label or ent.name,
                columns=cols,
            )
        )
    return ents


def _build_relation_descriptors(schema: SchemaConfig) -> List[RelationDescriptor]:
    rels: List[RelationDescriptor] = []
    for rel in schema.relations:
        rels.append(
            RelationDescriptor(
                name=rel.name,
                from_entity=rel.from_entity,
                to_entity=rel.to_entity,
                cardinality=rel.cardinality,
                join=RelationJoin(
                    from_entity=rel.from_entity,
                    from_column=rel.from_column,
                    to_entity=rel.to_entity,
                    to_column=rel.to_column,
                ),
                semantic_hint=rel.semantic_hint,
            )
        )
    return rels


def _build_primary_keys(schema: SchemaConfig) -> Dict[str, str]:
    pk_map: Dict[str, str] = {}
    for ent in schema.entities:
        pk_cols = [c.name for c in ent.columns if c.pk]
        if len(pk_cols) == 1:
            pk_map[ent.name] = pk_cols[0]
    return pk_map


# ------------------------ SemanticBackend ------------------------

def _pick_pk_column(ent: EntityConfig) -> str:
    pk_cols = [c.name for c in ent.columns if c.pk]
    if pk_cols:
        return pk_cols[0]
    if ent.columns:
        warnings.warn(
            f"[Schema] Entity '{ent.name}' has no PK, using first column '{ent.columns[0].name}' "
            "as id_column for semantic index."
        )
        return ent.columns[0].name
    raise ValueError(f"Entity '{ent.name}' has no columns at all.")

def build_csv_semantic_backend(
    data_dir: Path,
    schema: SchemaConfig,
    *,
    embedding_model: EmbeddingModel | None = None,
) -> CsvSemanticBackend | None:
    """
    Собрать CsvSemanticBackend по SchemaConfig и каталогу данных.

    Передайте embedding_model, если нужно строить и использовать плотные вектора.
    Если хотите полностью кастомный SemanticBackend (например, PGVector),
    передайте его напрямую в билдер провайдера вместо этого хэлпера.
    """
    sources: Dict[str, CsvSemanticSource] = {}
    for ent in schema.entities:
        if not ent.semantic_text_fields:
            continue
        if not ent.source:
            warnings.warn(
                f"[Schema {schema.name}] Entity '{ent.name}' has semantic_text_fields but no source."
            )
            continue
        csv_path = data_dir / ent.source
        emb_path = csv_path.with_suffix(".embeddings.json")

        if not emb_path.exists():
            CsvEmbeddingBuilder(
                csv_path=csv_path,
                entity=ent.name,
                id_column=_pick_pk_column(ent),
                text_fields=ent.semantic_text_fields,
                output_path=emb_path,
                embedding_model=embedding_model,
            ).build()

        sources[ent.name] = CsvSemanticSource(
            entity=ent.name,
            csv_path=csv_path,
            embedding_path=emb_path,
        )

    if not sources:
        return None
    return CsvSemanticBackend(sources, embedding_model=embedding_model)


# ------------------------ Главный билдер ------------------------

def build_pandas_provider_from_schema(
    data_dir: str | Path,
    schema: SchemaConfig,
    *,
    semantic_backend: CsvSemanticBackend | None = None,
) -> PandasRelationalDataProvider:
    """
    Высокоуровневый билдер: из SchemaConfig собирает PandasRelationalDataProvider.

    Если нужен универсальный интерфейс выбора бекенда, см. :func:`build_relational_provider_from_schema`.
    Для отдельного построения CSV-бекенда с кастомной embedding model есть
    :func:`build_csv_semantic_backend`.
    """
    data_dir = Path(data_dir)

    validate_schema(schema)

    frames: Dict[str, pd.DataFrame] = {}
    for ent in schema.entities:
        if not ent.source:
            continue
        csv_path = data_dir / ent.source
        frames[ent.name] = pd.read_csv(csv_path)

    entities = _build_entity_descriptors(schema)
    relations = _build_relation_descriptors(schema)
    primary_keys = _build_primary_keys(schema)
    if semantic_backend is None:
        semantic_backend = build_csv_semantic_backend(data_dir, schema)

    provider = PandasRelationalDataProvider(
        name=schema.name,
        entities=entities,
        relations=relations,
        frames=frames,
        semantic_backend=semantic_backend,
        primary_keys=primary_keys or None,
    )

    # лёгкая привязка схемы к провайдеру, чтобы describe() мог использовать
    provider._schema_config = schema  # type: ignore[attr-defined]
    return provider


def build_sql_provider_from_schema(
    engine: Any,
    schema: SchemaConfig,
    *,
    default_schema: str | None = None,
    semantic_backend: SemanticBackend | None = None,
    table_name_resolver: Callable[[EntityConfig], str] | None = None,
) -> SqlRelationalDataProvider:
    """
    Собрать SqlRelationalDataProvider на основе SchemaConfig.

    - engine: подключение к СУБД (DB-API connection или совместимый).
    - default_schema: SQL-схема по умолчанию.
    - semantic_backend: внешний SemanticBackend (например, кастомный pgvector).
      Если не передан, SQL-провайдер создаётся без семантического поиска.
    - table_name_resolver: функция, которая по EntityConfig возвращает имя SQL-таблицы.
      По умолчанию используется ent.source (без .csv) или ent.name.
    """

    validate_schema(schema)

    table_names: Dict[str, str] = {}
    for ent in schema.entities:
        if table_name_resolver:
            table_name = table_name_resolver(ent)
        elif ent.source:
            table_name = Path(ent.source).stem if ent.source.endswith(".csv") else ent.source
        else:
            table_name = ent.name
        table_names[ent.name] = table_name

    entities = _build_entity_descriptors(schema)
    relations = _build_relation_descriptors(schema)
    primary_keys = _build_primary_keys(schema)

    provider = SqlRelationalDataProvider(
        name=schema.name,
        entities=entities,
        relations=relations,
        connection=engine,
        semantic_backend=semantic_backend,
        primary_keys=primary_keys or None,
        default_schema=default_schema,
        table_names=table_names,
    )

    provider._schema_config = schema  # type: ignore[attr-defined]
    return provider


@overload
def build_relational_provider_from_schema(
    backend: Literal["pandas"],
    schema: SchemaConfig,
    *,
    data_dir: str | Path,
    semantic_backend: CsvSemanticBackend | None = None,
) -> PandasRelationalDataProvider:
    ...


@overload
def build_relational_provider_from_schema(
    backend: Literal["sql"],
    schema: SchemaConfig,
    *,
    engine: Any,
    semantic_backend: SemanticBackend | None = None,
    default_schema: str | None = None,
    table_name_resolver: Callable[[EntityConfig], str] | None = None,
) -> SqlRelationalDataProvider:
    ...


def build_relational_provider_from_schema(
    backend: BackendKind,
    schema: SchemaConfig,
    **kwargs: Any,
) -> ContextProvider:
    """Универсальный билдер провайдера по SchemaConfig для разных бэкендов."""

    if backend == "pandas":
        if "data_dir" not in kwargs:
            raise TypeError("data_dir is required for pandas backend")
        return build_pandas_provider_from_schema(
            kwargs["data_dir"],
            schema,
            semantic_backend=kwargs.get("semantic_backend"),
        )

    if backend == "sql":
        if "engine" not in kwargs:
            raise TypeError("engine is required for sql backend")
        return build_sql_provider_from_schema(
            kwargs["engine"],
            schema,
            default_schema=kwargs.get("default_schema"),
            semantic_backend=kwargs.get("semantic_backend"),
            table_name_resolver=kwargs.get("table_name_resolver"),
        )

    raise ValueError(f"Unknown backend '{backend}'")
