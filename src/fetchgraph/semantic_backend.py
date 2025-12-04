from __future__ import annotations

"""Protocols and utilities for semantic search backends."""

from dataclasses import dataclass
import json
import math
import re
from pathlib import Path
from typing import Mapping, Protocol, Sequence, cast

from .relational_models import SemanticMatch


class SemanticBackend(Protocol):
    """Interface for semantic search backends.

    Real implementations may use vector stores like Faiss, Qdrant, or pgvector.
    """

    def search(
        self,
        entity: str,
        fields: Sequence[str] | None,
        query: str,
        top_k: int = 100,
    ) -> list[SemanticMatch]:
        """Return semantic matches for the given entity and text query."""

        ...


class VectorStoreLike(Protocol):
    """Minimal protocol for LangChain-style vector stores."""

    def similarity_search_with_score(
        self, query: str, k: int = 4, **kwargs: object
    ) -> Sequence[tuple[object, float]]:
        """Return documents with similarity scores for a query."""

        ...


class EmbeddingModel(Protocol):
    """Protocol for embedding models used by semantic backends."""

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        ...

    def embed_query(self, text: str) -> list[float]:
        ...


@dataclass(frozen=True)
class CsvSemanticSource:
    """Configuration for a CSV semantic index."""

    entity: str
    csv_path: Path
    embedding_path: Path


class CsvEmbeddingBuilder:
    """Build TF-IDF embeddings for a CSV file and persist them to disk."""

    def __init__(
        self,
        csv_path: str | Path,
        entity: str,
        id_column: str,
        text_fields: Sequence[str],
        output_path: str | Path,
        embedding_model: EmbeddingModel | None = None,
    ) -> None:
        self.csv_path = Path(csv_path)
        self.entity = entity
        self.id_column = id_column
        self.text_fields = list(text_fields)
        self.output_path = Path(output_path)
        self.embedding_model = embedding_model

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        return re.findall(r"\b\w+\b", text.lower())

    def _build_vocab(self, documents: list[list[str]]) -> list[str]:
        doc_freq: dict[str, int] = {}
        for tokens in documents:
            for tok in set(tokens):
                doc_freq[tok] = doc_freq.get(tok, 0) + 1
        return sorted(doc_freq.keys())

    @staticmethod
    def _normalize_id(value: object) -> object:
        """Convert pandas/numpy scalars to JSON-serializable Python types."""

        def is_primitive(x: object) -> bool:
            return isinstance(x, (str, int, float, bool)) or x is None

        if is_primitive(value):
            return value

        item = getattr(value, "item", None)
        if callable(item):
            try:
                coerced = item()
                if is_primitive(coerced):
                    return coerced
            except Exception:
                pass

        return str(value)

    def _idf(self, vocab: list[str], documents: list[list[str]]) -> list[float]:
        n_docs = len(documents)
        doc_freq = {tok: 0 for tok in vocab}
        for tokens in documents:
            for tok in set(tokens):
                if tok in doc_freq:
                    doc_freq[tok] += 1
        return [math.log((1 + n_docs) / (1 + doc_freq[tok])) + 1 for tok in vocab]

    @staticmethod
    def _l2_normalize(vector: Sequence[float]) -> list[float]:
        norm = math.sqrt(sum(v * v for v in vector))
        if norm == 0:
            return list(vector)
        return [v / norm for v in vector]

    def _vectorize(self, tokens: list[str], vocab: list[str], idf: list[float]) -> list[float]:
        counts: dict[str, int] = {}
        for tok in tokens:
            counts[tok] = counts.get(tok, 0) + 1
        vector = [counts.get(tok, 0) * idf[idx] for idx, tok in enumerate(vocab)]
        norm = math.sqrt(sum(v * v for v in vector))
        if norm == 0:
            return vector
        return [v / norm for v in vector]

    def build(self) -> None:
        """Read the CSV file, build embeddings, and save them to disk."""

        import pandas as pd

        df = pd.read_csv(self.csv_path)
        if self.id_column not in df.columns:
            raise KeyError(f"ID column '{self.id_column}' not found in CSV")
        for field in self.text_fields:
            if field not in df.columns:
                raise KeyError(f"Text field '{field}' not found in CSV")

        if self.embedding_model is None:
            documents: list[list[str]] = []
            per_field_documents: list[dict[str, list[str]]] = []
            id_values: list[object] = []
            for _, row in df.iterrows():
                field_tokens: dict[str, list[str]] = {}
                for field in self.text_fields:
                    value = row[field]
                    is_null = pd.isna(value)
                    is_missing = bool(is_null.any()) if hasattr(is_null, "any") else bool(is_null)
                    tokens = [] if is_missing else self._tokenize(str(value))
                    field_tokens[field] = tokens

                combined_tokens = [tok for tokens in field_tokens.values() for tok in tokens]
                documents.append(combined_tokens)
                per_field_documents.append(field_tokens)
                id_values.append(row[self.id_column])

            vocab = self._build_vocab(documents)
            idf = self._idf(vocab, documents)

            embeddings = []
            for identifier, tokens, field_tokens in zip(id_values, documents, per_field_documents):
                vectors = {
                    field: self._vectorize(field_tokens[field], vocab, idf)
                    for field in self.text_fields
                }
                vectors["__all__"] = self._vectorize(tokens, vocab, idf)

                embeddings.append({"id": self._normalize_id(identifier), "vectors": vectors})

            payload = {
                "entity": self.entity,
                "id_column": self.id_column,
                "fields": self.text_fields,
                "vocab": vocab,
                "idf": idf,
                "embeddings": embeddings,
                "kind": "tfidf",
            }
        else:
            field_texts: dict[str, list[str]] = {field: [] for field in self.text_fields}
            all_texts: list[str] = []
            ids: list[object] = []
            for _, row in df.iterrows():
                per_row_texts: list[str] = []
                for field in self.text_fields:
                    value = row[field]
                    is_null = pd.isna(value)
                    is_missing = bool(is_null.any()) if hasattr(is_null, "any") else bool(is_null)
                    text = "" if is_missing else str(value)
                    field_texts[field].append(text)
                    per_row_texts.append(text)
                all_texts.append(" ".join(per_row_texts))
                ids.append(row[self.id_column])

            all_vectors = [self._l2_normalize(vec) for vec in self.embedding_model.embed_documents(all_texts)]

            field_vectors: dict[str, list[list[float]]] = {}
            for field, texts in field_texts.items():
                vectors = self.embedding_model.embed_documents(texts)
                field_vectors[field] = [self._l2_normalize(vec) for vec in vectors]

            embeddings = []
            for idx, identifier in enumerate(ids):
                vectors = {"__all__": all_vectors[idx]}
                for field in self.text_fields:
                    vectors[field] = field_vectors[field][idx]
                embeddings.append(
                    {
                        "id": self._normalize_id(identifier),
                        "vectors": vectors,
                    }
                )

            payload = {
                "entity": self.entity,
                "id_column": self.id_column,
                "fields": self.text_fields,
                "vocab": [],
                "idf": [],
                "embeddings": embeddings,
                "kind": "dense",
            }
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f)


class CsvSemanticBackend:
    """Semantic backend that loads CSV-derived embeddings from disk."""

    def __init__(
        self, sources: Mapping[str, CsvSemanticSource], embedding_model: EmbeddingModel | None = None
    ) -> None:
        self._indices: dict[str, dict[str, object]] = {}
        self._embedding_model = embedding_model
        for entity, source in sources.items():
            if entity != source.entity:
                raise ValueError(
                    f"Entity key '{entity}' does not match source entity '{source.entity}'"
                )
            index = self._load_index(source)
            self._indices[entity] = index

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        return re.findall(r"\b\w+\b", text.lower())

    def _load_index(self, source: CsvSemanticSource) -> dict[str, object]:
        if not source.embedding_path.exists():
            raise FileNotFoundError(f"Embedding file not found: {source.embedding_path}")
        with source.embedding_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("entity") != source.entity:
            raise ValueError(
                f"Embedding entity '{data.get('entity')}' does not match expected '{source.entity}'"
            )
        # Touch the CSV file to ensure it exists and is readable.
        if not source.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {source.csv_path}")
        # Load metadata and embeddings into memory.
        vocab: list[str] = list(data.get("vocab", []))
        idf: list[float] = list(data.get("idf", []))
        embeddings = data.get("embeddings", [])
        kind = data.get("kind", "tfidf")
        ids: list[object] = []
        vectors_by_field: dict[str, list[list[float]]] = {"__all__": []}
        for field in data.get("fields", []) or []:
            vectors_by_field[field] = []

        for item in embeddings:
            ids.append(item.get("id"))
            if isinstance(item.get("vectors"), Mapping):
                stored_vectors: Mapping[str, list[float]] = item["vectors"]
                combined_vector = stored_vectors.get("__all__") or stored_vectors.get("vector")
            else:
                stored_vectors = {}
                combined_vector = item.get("vector")

            if combined_vector is None:
                combined_vector = [0.0 for _ in vocab]

            vectors_by_field.setdefault("__all__", []).append(combined_vector)

            for field in data.get("fields", []) or []:
                vectors_by_field.setdefault(field, []).append(
                    list(stored_vectors.get(field, combined_vector))
                )

        return {
            "fields": set(data.get("fields", [])),
            "vocab": vocab,
            "idf": idf,
            "ids": ids,
            "vectors_by_field": vectors_by_field,
            "kind": kind,
        }

    def _vectorize_query(self, query: str, vocab: list[str], idf: list[float]) -> list[float]:
        tokens = self._tokenize(query)
        counts: dict[str, int] = {}
        for tok in tokens:
            counts[tok] = counts.get(tok, 0) + 1
        vector = [counts.get(tok, 0) * idf[idx] for idx, tok in enumerate(vocab)]
        norm = math.sqrt(sum(v * v for v in vector))
        if norm == 0:
            return vector
        return [v / norm for v in vector]

    def search(
        self,
        entity: str,
        fields: Sequence[str] | None,
        query: str,
        top_k: int = 100,
    ) -> list[SemanticMatch]:
        if entity not in self._indices:
            raise KeyError(f"Entity '{entity}' is not indexed for semantic search")
        index = self._indices[entity]
        expected_fields: set[str] = index["fields"]  # type: ignore[assignment]
        if isinstance(fields, str):
            fields = [fields]
        normalized_fields = list(fields or [])

        if normalized_fields and not set(normalized_fields).issubset(expected_fields):
            raise ValueError(
                f"Requested fields {normalized_fields} are not a subset of indexed fields {sorted(expected_fields)}"
            )

        ids: list[object] = index["ids"]  # type: ignore[assignment]
        vectors_by_field: Mapping[str, list[list[float]]] = index[
            "vectors_by_field"
        ]  # type: ignore[assignment]
        kind: str = index.get("kind", "tfidf")  # type: ignore[assignment]

        if kind == "tfidf":
            vocab: list[str] = index["vocab"]  # type: ignore[assignment]
            idf: list[float] = index["idf"]  # type: ignore[assignment]
            query_vec = self._vectorize_query(query, vocab, idf)
            if not any(query_vec):
                return []
        elif kind == "dense":
            if self._embedding_model is None:
                raise RuntimeError("Dense embeddings require an embedding_model for queries")
            query_vec = CsvEmbeddingBuilder._l2_normalize(self._embedding_model.embed_query(query))
        else:
            raise ValueError(f"Unsupported embedding kind '{kind}' in index for entity '{entity}'")

        # Fields are summed to favor rows matching across multiple columns; use a different
        # aggregation strategy (e.g., max) if that better fits your application.
        # "__all__" is reserved for the combined embedding and is not accepted as input.
        target_fields = normalized_fields if normalized_fields else ["__all__"]

        scores: list[tuple[object, float]] = []
        for row_idx, identifier in enumerate(ids):
            score = 0.0
            for field in target_fields:
                field_vectors = vectors_by_field.get(field)
                if field_vectors is None or row_idx >= len(field_vectors):
                    continue
                vector = field_vectors[row_idx]
                score += sum(q * v for q, v in zip(query_vec, vector))
            if kind == "tfidf" and score <= 0:
                continue
            scores.append((identifier, score))

        sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)[:top_k]
        return [SemanticMatch(entity=entity, id=identifier, score=score) for identifier, score in sorted_scores]


@dataclass(frozen=True)
class PgVectorSemanticSource:
    """Configuration for a pgvector-backed semantic index."""

    entity: str
    vector_store: VectorStoreLike
    metadata_entity_key: str = "entity"
    metadata_field_key: str = "field"
    id_metadata_keys: tuple[str, ...] = ("id",)
    score_kind: str = "distance"  # "distance" (default pgvector) or "similarity"
    embedding_model: EmbeddingModel | None = None


class PgVectorSemanticBackend:
    """Semantic backend powered by a LangChain-compatible pgvector store.

    The backend accepts a mapping from entity name to :class:`VectorStoreLike`
    instances (e.g., ``langchain_community.vectorstores.pgvector.PGVector``).
    Documents are filtered by the requested entity and optional field names via
    document metadata. Metadata keys are configurable to align with your
    ingestion pipeline.
    """

    def __init__(
        self,
        sources: Mapping[str, PgVectorSemanticSource],
        embedding_model: EmbeddingModel | None = None,
    ):
        self._sources: dict[str, PgVectorSemanticSource] = {}
        self._default_embedding_model = embedding_model
        for entity, source in sources.items():
            if entity != source.entity:
                raise ValueError(
                    f"Entity key '{entity}' does not match source entity '{source.entity}'"
                )
            self._sources[entity] = source

    @staticmethod
    def _metadata(document: object) -> Mapping[str, object]:
        metadata = getattr(document, "metadata", {})
        if isinstance(metadata, Mapping):
            return metadata
        return {}

    @staticmethod
    def _normalize_score(raw_score: float, score_kind: str) -> float:
        if score_kind == "distance":
            return 1 / (1 + raw_score) if raw_score >= 0 else raw_score
        return raw_score

    def _resolve_id(self, metadata: Mapping[str, object], keys: tuple[str, ...]) -> object:
        for key in keys:
            if key in metadata:
                return metadata[key]
        return None

    def search(
        self,
        entity: str,
        fields: Sequence[str] | None,
        query: str,
        top_k: int = 100,
    ) -> list[SemanticMatch]:
        if entity not in self._sources:
            raise KeyError(f"Entity '{entity}' is not indexed for semantic search")

        source = self._sources[entity]
        if isinstance(fields, str):
            fields = [fields]
        normalized_fields = list(fields or [])

        model = source.embedding_model or self._default_embedding_model
        results: Sequence[tuple[object, float]]

        if model is None:
            raw_results = source.vector_store.similarity_search_with_score(query, k=top_k)
            results = cast(Sequence[tuple[object, float]], raw_results)
        else:
            query_vec = model.embed_query(query)
            search_with_score = getattr(
                source.vector_store, "similarity_search_with_score_by_vector", None
            )

            if callable(search_with_score):
                vector_results = search_with_score(query_vec, k=top_k)
                results = cast(Sequence[tuple[object, float]], vector_results)
            else:
                raise TypeError(
                    "Vector store does not support similarity_search_with_score_by_vector for vector queries"
                )

        matches: list[SemanticMatch] = []
        for document, raw_score in results:
            metadata = self._metadata(document)

            doc_entity = metadata.get(source.metadata_entity_key)
            if doc_entity is not None and doc_entity != entity:
                continue

            if normalized_fields:
                doc_field = metadata.get(source.metadata_field_key)
                if doc_field not in normalized_fields:
                    continue

            identifier = self._resolve_id(metadata, source.id_metadata_keys)
            if identifier is None:
                continue

            score = self._normalize_score(raw_score, source.score_kind)
            matches.append(SemanticMatch(entity=entity, id=identifier, score=score))

        return sorted(matches, key=lambda m: m.score, reverse=True)[:top_k]


__all__ = [
    "EmbeddingModel",
    "SemanticBackend",
    "CsvSemanticBackend",
    "CsvSemanticSource",
    "CsvEmbeddingBuilder",
    "PgVectorSemanticBackend",
    "PgVectorSemanticSource",
    "VectorStoreLike",
]

