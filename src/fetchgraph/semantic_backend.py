from __future__ import annotations

"""Protocols and utilities for semantic search backends."""

from dataclasses import dataclass
import json
import math
import re
from pathlib import Path
from typing import Mapping, Protocol, Sequence

import pandas as pd

from .relational_models import SemanticMatch


class SemanticBackend(Protocol):
    """Interface for semantic search backends.

    Real implementations may use vector stores like Faiss, Qdrant, or pgvector.
    """

    def search(
        self,
        entity: str,
        fields: Sequence[str],
        query: str,
        top_k: int = 100,
    ) -> list[SemanticMatch]:
        """Return semantic matches for the given entity and text query."""

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
    ) -> None:
        self.csv_path = Path(csv_path)
        self.entity = entity
        self.id_column = id_column
        self.text_fields = list(text_fields)
        self.output_path = Path(output_path)

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

        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        item = getattr(value, "item", None)
        if callable(item):
            try:
                return item()
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

        df = pd.read_csv(self.csv_path)
        if self.id_column not in df.columns:
            raise KeyError(f"ID column '{self.id_column}' not found in CSV")
        for field in self.text_fields:
            if field not in df.columns:
                raise KeyError(f"Text field '{field}' not found in CSV")

        documents: list[list[str]] = []
        id_values: list[object] = []
        for _, row in df.iterrows():
            parts = [str(row[field]) for field in self.text_fields if pd.notna(row[field])]
            tokens = self._tokenize(" ".join(parts))
            documents.append(tokens)
            id_values.append(row[self.id_column])

        vocab = self._build_vocab(documents)
        idf = self._idf(vocab, documents)

        embeddings = [
            {
                "id": self._normalize_id(identifier),
                "vector": self._vectorize(tokens, vocab, idf),
            }
            for identifier, tokens in zip(id_values, documents)
        ]

        payload = {
            "entity": self.entity,
            "id_column": self.id_column,
            "fields": self.text_fields,
            "vocab": vocab,
            "idf": idf,
            "embeddings": embeddings,
        }
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f)


class CsvSemanticBackend:
    """Semantic backend that loads CSV-derived embeddings from disk."""

    def __init__(self, sources: Mapping[str, CsvSemanticSource]) -> None:
        self._indices: dict[str, dict[str, object]] = {}
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
        ids = [item["id"] for item in embeddings]
        vectors = [item["vector"] for item in embeddings]
        return {
            "fields": set(data.get("fields", [])),
            "vocab": vocab,
            "idf": idf,
            "ids": ids,
            "vectors": vectors,
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
        fields: Sequence[str],
        query: str,
        top_k: int = 100,
    ) -> list[SemanticMatch]:
        if entity not in self._indices:
            raise KeyError(f"Entity '{entity}' is not indexed for semantic search")
        index = self._indices[entity]
        expected_fields: set[str] = index["fields"]  # type: ignore[assignment]
        if fields and not set(fields).issubset(expected_fields):
            raise ValueError(
                f"Requested fields {fields} are not a subset of indexed fields {sorted(expected_fields)}"
            )

        vocab: list[str] = index["vocab"]  # type: ignore[assignment]
        idf: list[float] = index["idf"]  # type: ignore[assignment]
        ids: list[object] = index["ids"]  # type: ignore[assignment]
        vectors: list[list[float]] = index["vectors"]  # type: ignore[assignment]

        query_vec = self._vectorize_query(query, vocab, idf)
        if not any(query_vec):
            return []

        scores: list[tuple[object, float]] = []
        for identifier, vector in zip(ids, vectors):
            score = sum(q * v for q, v in zip(query_vec, vector))
            scores.append((identifier, score))

        sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)[:top_k]
        return [SemanticMatch(entity=entity, id=identifier, score=score) for identifier, score in sorted_scores]


__all__ = [
    "SemanticBackend",
    "CsvSemanticBackend",
    "CsvSemanticSource",
    "CsvEmbeddingBuilder",
]
