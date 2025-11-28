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
    matches = backend.search("product", ["name", "description"], "red shiny gadget", top_k=2)

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
