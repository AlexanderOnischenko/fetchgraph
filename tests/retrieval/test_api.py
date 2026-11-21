"""Regressions for the public evidence-first boundary."""

import asyncio
import json
from pathlib import Path

import pytest

pytest.importorskip("jsonschema")
pytest.importorskip("httpx")

from fetchgraph import EvidenceBundle, FetchGraph, RetrievalError, answer, retrieve
from fetchgraph.retrieval.engine import RetrievalEngine

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def configured(tmp_path, monkeypatch):
    path = tmp_path / "fetchgraph.toml"
    path.write_text(f'''
version = 1
[profiles.local]
base_url = "http://127.0.0.1:8080/v1"
model = "test"
[sources.shop]
kind = "csv"
description = "Shop"
data_dir = "{ROOT / "_demo_data/shop"}"
schema = "{ROOT / "_demo_data/shop/schema.yaml"}"
inference_profiles = ["local"]
default_profile = "local"
''')
    calls = []

    async def inference(self, profile, stage, instruction, data):
        calls.append(stage)
        if stage == "plan":
            return json.dumps(
                {
                    "context_plan": [
                        {
                            "provider": "shop",
                            "selectors": {
                                "op": "query",
                                "root_entity": "orders",
                                "aggregations": [
                                    {"field": "order_id", "agg": "count", "alias": "n"}
                                ],
                            },
                        }
                    ]
                }
            )
        assert data["evidence"][0]["text"] == '{"n":1000}'
        return json.dumps({"status": "answered", "answer": "1000 [1]", "gaps": []})

    monkeypatch.setattr(RetrievalEngine, "infer", inference)
    return path, calls


def test_public_retrieve_stops_before_synthesis(configured):
    path, calls = configured
    bundle = retrieve("Count orders?", config=path)
    assert isinstance(bundle, EvidenceBundle)
    assert calls == ["plan"]
    assert bundle.diagnostics.status == "complete"
    assert bundle.diagnostics.llm_calls == 1
    assert bundle.evidence[0].data == {"n": 1000}
    executed = bundle.plan.context_plan[0]
    assert executed.provider == "shop"
    assert executed.selectors["limit"] == 100
    assert bundle.evidence[0].provenance.selectors == executed.selectors
    assert bundle.sources[0].id == "shop"
    assert EvidenceBundle.model_validate_json(bundle.model_dump_json()) == bundle
    assert "answer" not in bundle.model_dump()


def test_env_convenience_and_answer_share_retrieval(configured, monkeypatch):
    path, calls = configured
    monkeypatch.setenv("FETCHGRAPH_CONFIG", str(path))
    result = answer("Count orders?")
    assert result.answer == "1000 [1]"
    assert result.bundle.evidence[0].data == {"n": 1000}
    assert calls == ["plan", "synth"]


def test_async_entry_and_sync_guard(configured):
    path, calls = configured
    client = FetchGraph(path)

    async def exercise():
        bundle = await client.aretrieve("Count orders?")
        assert bundle.evidence[0].data == {"n": 1000}
        with pytest.raises(RuntimeError, match="aretrieve"):
            client.retrieve("Count orders?")

    asyncio.run(exercise())
    assert calls == ["plan"]


def test_ambiguous_source_rejected_before_inference(configured):
    path, calls = configured
    client = FetchGraph(path)
    client._engine.config["sources"]["other"] = client._engine.config["sources"]["shop"]
    with pytest.raises(RetrievalError) as error:
        client.retrieve("Count orders?")
    assert error.value.code == "INVALID_ARGUMENT"
    assert not calls


def test_empty_plan_keeps_insufficient_bundle(configured, monkeypatch):
    path, calls = configured

    async def unsupported(*args):
        return '{"context_plan":[]}'

    monkeypatch.setattr(RetrievalEngine, "infer", unsupported)
    bundle = retrieve("Unsupported question?", config=path)
    assert bundle.diagnostics.status == "insufficient"
    assert bundle.diagnostics.gaps
    assert not bundle.plan.context_plan
    assert not bundle.evidence


def test_root_import_does_not_load_optional_transports():
    import subprocess
    import sys

    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import fetchgraph; "
            "assert callable(fetchgraph.retrieve); "
            "assert 'mcp' not in sys.modules; "
            "assert 'httpx' not in sys.modules; "
            "assert 'jsonschema' not in sys.modules",
        ],
        check=True,
    )
