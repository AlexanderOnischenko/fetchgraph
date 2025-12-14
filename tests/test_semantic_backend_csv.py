import json
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from fetchgraph.semantic_backend import (
    CsvEmbeddingBuilder,
    CsvSemanticBackend,
    CsvSemanticSource,
)


def _build_backend(tmp_path: Path) -> CsvSemanticBackend:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Red Gadget", "Blue Widget", "Green Gizmo"],
            "description": [
                "A shiny red gadget for everyday use",
                "Reliable blue widget with extra features",
                "Eco-friendly green gizmo made from bamboo",
            ],
        }
    )
    csv_path = tmp_path / "products.csv"
    embedding_path = tmp_path / "products_embedding.json"
    df.to_csv(csv_path, index=False)

    builder = CsvEmbeddingBuilder(
        csv_path=csv_path,
        entity="product",
        id_column="id",
        text_fields=["name", "description"],
        output_path=embedding_path,
    )
    builder.build()

    return CsvSemanticBackend(
        {"product": CsvSemanticSource(entity="product", csv_path=csv_path, embedding_path=embedding_path)}
    )


def test_csv_semantic_backend_search_ranks_by_similarity(tmp_path: Path):
    backend = _build_backend(tmp_path)
    matches = backend.search(
        "product", ["name", "description"], "red shiny gadget widget", top_k=2
    )

    assert [m.id for m in matches] == [1, 2]
    assert matches[0].score >= matches[1].score


def test_csv_semantic_backend_validates_fields(tmp_path: Path):
    backend = _build_backend(tmp_path)

    with pytest.raises(ValueError):
        backend.search("product", ["unknown_field"], "test")


def test_csv_semantic_backend_filters_zero_similarity(tmp_path: Path):
    backend = _build_backend(tmp_path)

    matches = backend.search("product", ["name", "description"], "bamboo", top_k=10)

    assert [m.id for m in matches] == [3]
    assert matches[0].score > 0


def test_csv_semantic_backend_honors_requested_fields(tmp_path: Path):
    backend = _build_backend(tmp_path)

    matches = backend.search("product", ["name"], "bamboo", top_k=10)

    assert matches == []


def test_csv_semantic_backend_accepts_string_field(tmp_path: Path):
    backend = _build_backend(tmp_path)

    matches = backend.search("product", fields="description", query="bamboo", top_k=5)

    assert [m.id for m in matches] == [3]


def test_csv_semantic_backend_end_to_end(tmp_path: Path) -> None:
    csv_path = tmp_path / "systems.csv"
    df = pd.DataFrame(
        [
            {
                "id": 1,
                "name": "CRM",
                "description": "Система управления клиентами",
                "owner": "customer_team",
            },
            {
                "id": 2,
                "name": "Payments",
                "description": "Обработка платежей и биллинга",
                "owner": "payments_team",
            },
            {
                "id": 3,
                "name": "Reporting",
                "description": "Формирование управленческой отчётности",
                "owner": "bi_team",
            },
        ]
    )
    df.to_csv(csv_path, index=False)

    embedding_path = tmp_path / "systems_embeddings.json"
    builder = CsvEmbeddingBuilder(
        csv_path=csv_path,
        entity="system",
        id_column="id",
        text_fields=["name", "description"],
        output_path=embedding_path,
    )
    builder.build()

    assert embedding_path.exists(), "Embedding file was not created"

    sources = {
        "system": CsvSemanticSource(
            entity="system",
            csv_path=csv_path,
            embedding_path=embedding_path,
        )
    }
    backend = CsvSemanticBackend(sources)

    matches = backend.search(
        entity="system",
        fields=["name", "description"],
        query="платежи и биллинг",
        top_k=3,
    )

    assert matches, "No semantic matches returned"
    top_match = matches[0]
    assert top_match.id == 2
    assert top_match.score > 0


def test_csv_embedding_builder_dense_payload(tmp_path: Path) -> None:
    csv_path = tmp_path / "dense.csv"
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["Alpha", "Beta"],
            "description": ["First alpha", "Second beta"],
        }
    )
    df.to_csv(csv_path, index=False)
    embedding_path = tmp_path / "dense_embeddings.json"

    builder = CsvEmbeddingBuilder(
        csv_path=csv_path,
        entity="demo",
        id_column="id",
        text_fields=["name", "description"],
        output_path=embedding_path,
        embedding_model=FakeEmbeddingModel(),
    )
    builder.build()

    payload = json.loads(embedding_path.read_text())
    assert payload["kind"] == "dense"
    assert payload["vocab"] == []
    assert payload["idf"] == []
    assert len(payload["embeddings"]) == 2
    assert payload["embeddings"][0]["vectors"]["__all__"] == [1.0, 0.0]
    assert payload["embeddings"][1]["vectors"]["__all__"] == [0.0, 1.0]


def test_csv_semantic_backend_dense_search(tmp_path: Path) -> None:
    csv_path = tmp_path / "dense.csv"
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["Alpha", "Beta"],
            "description": ["First alpha", "Second beta"],
        }
    )
    df.to_csv(csv_path, index=False)
    embedding_path = tmp_path / "dense_embeddings.json"

    builder = CsvEmbeddingBuilder(
        csv_path=csv_path,
        entity="demo",
        id_column="id",
        text_fields=["name", "description"],
        output_path=embedding_path,
        embedding_model=FakeEmbeddingModel(),
    )
    builder.build()

    backend = CsvSemanticBackend(
        {"demo": CsvSemanticSource(entity="demo", csv_path=csv_path, embedding_path=embedding_path)},
        embedding_model=FakeEmbeddingModel(),
    )

    matches = backend.search("demo", fields=["name", "description"], query="alpha", top_k=2)

    assert [m.id for m in matches] == [1, 2]
    assert matches[0].score > matches[1].score


def test_csv_semantic_backend_dense_requires_embedding_model(tmp_path: Path) -> None:
    csv_path = tmp_path / "dense.csv"
    df = pd.DataFrame({"id": [1], "name": ["Alpha"]})
    df.to_csv(csv_path, index=False)
    embedding_path = tmp_path / "dense_embeddings.json"

    builder = CsvEmbeddingBuilder(
        csv_path=csv_path,
        entity="demo",
        id_column="id",
        text_fields=["name"],
        output_path=embedding_path,
        embedding_model=FakeEmbeddingModel(),
    )
    builder.build()

    backend = CsvSemanticBackend(
        {"demo": CsvSemanticSource(entity="demo", csv_path=csv_path, embedding_path=embedding_path)}
    )

    with pytest.raises(RuntimeError):
        backend.search("demo", fields=["name"], query="alpha", top_k=1)
class FakeEmbeddingModel:
    def _vector_for_text(self, text: str) -> list[float]:
        normalized = text.lower()
        if "alpha" in normalized:
            return [1.0, 0.0]
        if "beta" in normalized:
            return [0.0, 1.0]
        return [0.0, 0.0]

    def embed_documents(self, texts):
        return [self._vector_for_text(text) for text in texts]

    def embed_query(self, text):
        return self._vector_for_text(text)

