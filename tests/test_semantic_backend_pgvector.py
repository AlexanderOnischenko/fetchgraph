from dataclasses import dataclass
from typing import List, Sequence, Tuple

import pytest

pytest.importorskip("pandas")

from fetchgraph.semantic_backend import (
    PgVectorSemanticBackend,
    PgVectorSemanticSource,
    VectorStoreLike,
)


@dataclass
class FakeDocument:
    metadata: dict


class FakeVectorStore(VectorStoreLike):
    def __init__(self, results: Sequence[Tuple[FakeDocument, float]]):
        self._results = list(results)
        self.queries: List[Tuple[str, int]] = []

    def similarity_search_with_score(self, query: str, k: int = 4, **kwargs: object):
        self.queries.append((query, k))
        return self._results[:k]


class FakeVectorSearchStore(FakeVectorStore):
    def __init__(self, results: Sequence[Tuple[FakeDocument, float]]):
        super().__init__(results)
        self.vector_queries: List[Tuple[List[float], int]] = []

    def similarity_search_with_score_by_vector(
        self, query: List[float], k: int = 4, **kwargs: object
    ):
        self.vector_queries.append((query, k))
        return self._results[:k]


class FakeEmbeddingModel:
    def __init__(self, vector: List[float]):
        self.vector = vector

    def embed_documents(self, texts):
        return [self.vector for _ in texts]

    def embed_query(self, text):
        return self.vector


def _backend_with_results(results: Sequence[Tuple[FakeDocument, float]]) -> PgVectorSemanticBackend:
    store = FakeVectorStore(results)
    source = PgVectorSemanticSource(
        entity="product",
        vector_store=store,
        metadata_entity_key="entity",
        metadata_field_key="field",
        id_metadata_keys=("id", "pk"),
        score_kind="distance",
    )
    return PgVectorSemanticBackend({"product": source})


def test_pgvector_backend_filters_and_sorts_by_similarity():
    backend = _backend_with_results(
        [
            (FakeDocument({"id": 1, "entity": "product", "field": "name"}), 0.05),
            (FakeDocument({"id": 2, "entity": "product", "field": "description"}), 0.3),
            (FakeDocument({"id": 3, "entity": "other"}), 0.01),
        ]
    )

    matches = backend.search("product", fields=["name", "description"], query="red", top_k=5)

    assert [m.id for m in matches] == [1, 2]
    assert matches[0].score > matches[1].score


def test_pgvector_backend_honors_field_filter():
    backend = _backend_with_results(
        [
            (FakeDocument({"id": 1, "entity": "product", "field": "name"}), 0.2),
            (FakeDocument({"id": 2, "entity": "product", "field": "description"}), 0.1),
        ]
    )

    matches = backend.search("product", fields=["description"], query="info", top_k=3)

    assert [m.id for m in matches] == [2]


def test_pgvector_backend_supports_similarity_scores():
    store = FakeVectorStore(
        [
            (FakeDocument({"pk": "row-1", "entity": "product"}), 0.8),
            (FakeDocument({"pk": "row-2", "entity": "product"}), 0.6),
        ]
    )
    backend = PgVectorSemanticBackend(
        {
            "product": PgVectorSemanticSource(
                entity="product",
                vector_store=store,
                metadata_entity_key="entity",
                metadata_field_key="field",
                id_metadata_keys=("pk",),
                score_kind="similarity",
            )
        }
    )

    matches = backend.search("product", fields=None, query="search", top_k=2)

    assert [m.id for m in matches] == ["row-1", "row-2"]
    assert matches[0].score == 0.8


def test_pgvector_backend_orders_distance_scores_before_trimming():
    backend = _backend_with_results(
        [
            (FakeDocument({"id": 1, "entity": "product", "field": "name"}), 0.5),
            (FakeDocument({"id": 2, "entity": "product", "field": "name"}), 0.1),
            (FakeDocument({"id": 3, "entity": "other", "field": "name"}), 0.01),
        ]
    )

    matches = backend.search("product", fields=None, query="text", top_k=2)

    assert [m.id for m in matches] == [2, 1]
    assert matches[0].score > matches[1].score


def test_pgvector_backend_missing_entity():
    backend = _backend_with_results([])

    with pytest.raises(KeyError):
        backend.search("unknown", fields=None, query="test")


def test_pgvector_backend_uses_embedding_model_for_vector_queries():
    store = FakeVectorSearchStore(
        [
            (FakeDocument({"id": 1, "entity": "product"}), 0.2),
            (FakeDocument({"id": 2, "entity": "product"}), 0.1),
        ]
    )
    backend = PgVectorSemanticBackend(
        {
            "product": PgVectorSemanticSource(
                entity="product",
                vector_store=store,
                metadata_entity_key="entity",
                metadata_field_key="field",
                id_metadata_keys=("id",),
                score_kind="similarity",
                embedding_model=FakeEmbeddingModel([1.0, 0.0]),
            )
        }
    )

    matches = backend.search("product", fields=None, query="ignored", top_k=1)

    assert store.vector_queries == [([1.0, 0.0], 1)]
    assert [m.id for m in matches] == [1]


def test_pgvector_backend_requires_vector_search_support_when_model_provided():
    store = FakeVectorStore([(FakeDocument({"id": 1, "entity": "product"}), 0.5)])
    backend = PgVectorSemanticBackend(
        {
            "product": PgVectorSemanticSource(
                entity="product",
                vector_store=store,
                embedding_model=FakeEmbeddingModel([0.0, 1.0]),
            )
        }
    )

    with pytest.raises(TypeError):
        backend.search("product", fields=None, query="ignored", top_k=1)
