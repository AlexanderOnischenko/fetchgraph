import asyncio
import json
from pathlib import Path

import pytest

pytest.importorskip("jsonschema")
pytest.importorskip("httpx")
from fetchgraph.mcp.config import load_config
from fetchgraph.mcp.runtime import Failure, Runtime, TOOLS

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def runtime(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text(f'''
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
    return Runtime(load_config(config))


def test_count_snapshot_and_catalog(runtime):
    calls = []

    async def inference(profile, stage, instruction, data):
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

    runtime.infer = inference

    async def exercise():
        assert len((await runtime.call("fetchgraph_catalog", {}))["sources"]) == 1
        answer = await runtime.call(
            "fetchgraph_ask", {"source": "shop", "question": "Count?"}
        )
        evidence = await runtime.call(
            "fetchgraph_read_evidence", {"ref": answer["evidence"][0]["ref"]}
        )
        assert evidence["text"] == '{"n":1000}'
        assert evidence["selectors"]["limit"] == 100
        assert evidence["next_offset"] is None
        assert calls == ["plan", "synth"]

    asyncio.run(exercise())


def test_rejected_profile_and_unknown_ref(runtime):
    async def exercise():
        for tool, args, code in [
            (
                "fetchgraph_ask",
                {"source": "shop", "question": "?", "inference_profile": "unknown"},
                "PROFILE_UNKNOWN",
            ),
            ("fetchgraph_read_evidence", {"ref": "ev_" + "0" * 32}, "EVIDENCE_GONE"),
            ("fetchgraph_ask", {"source": "shop", "question": " "}, "INVALID_ARGUMENT"),
        ]:
            with pytest.raises(Failure) as error:
                await runtime.call(tool, args)
            assert error.value.code == code

    asyncio.run(exercise())


def test_contract_copy():
    assert (
        json.loads((ROOT / "docs/features/fetchgraph-mcp/tools.json").read_text())[
            "tools"
        ]
        == TOOLS
    )


def test_sqlite_zero_aggregate(runtime, tmp_path):
    import sqlite3
    from fetchgraph.mcp.runtime import dump

    path = tmp_path / "orders.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("create table orders(order_id integer)")
    source = {**runtime.config["sources"]["shop"], "kind": "sqlite", "database": path}
    pairs, gaps = runtime.fetch(
        source,
        {
            "op": "query",
            "root_entity": "orders",
            "limit": 100,
            "aggregations": [{"field": "order_id", "agg": "count", "alias": "n"}],
        },
        "Count?",
        float("inf"),
    )
    assert pairs == [("orders/aggregations/n", '{"n":0}', {"n": 0})]
    assert gaps == []
    import numpy as np
    import pandas as pd

    assert (
        dump({"missing": np.float64("nan"), "value": np.int64(7), "date": pd.NaT})
        == '{"missing":null,"value":7,"date":null}'
    )


@pytest.mark.parametrize(
    "change",
    [
        {"limit": None},
        {"limit": 1.0},
        {"limit": 101},
        {"select": [{"expr": "order_id); DROP TABLE orders"}]},
        {"aggregations": [{"field": "order_id", "agg": "execute"}]},
        {"filters": {"field": "unknown", "op": "=", "value": "x"}},
        {"extra": 1},
    ],
)
def test_unsafe_plan_never_reaches_fetch(runtime, change):
    async def inference(*args):
        return json.dumps(
            {
                "context_plan": [
                    {
                        "provider": "shop",
                        "selectors": {
                            "op": "query",
                            "root_entity": "orders",
                            **change,
                        },
                    }
                ]
            }
        )

    def forbidden_fetch(*args):
        pytest.fail("Invalid selectors reached provider")

    runtime.infer = inference
    runtime.fetch = forbidden_fetch

    async def exercise():
        with pytest.raises(Failure) as error:
            await runtime.call("fetchgraph_ask", {"source": "shop", "question": "?"})
        assert error.value.code == "INVALID_PLAN"
        assert not runtime.snapshots

    asyncio.run(exercise())


def test_unicode_snapshot_expiry_and_boundaries(runtime):
    import time

    reference = "ev_" + "a" * 32
    text = "Привет 👋" * 300
    runtime.snapshots["answer"] = (
        time.monotonic(),
        {
            reference: {
                "text": text,
                "source": "shop",
                "locator": "orders",
                "selectors": {},
            }
        },
    )

    async def exercise():
        parts = []
        offset = 0
        while True:
            page = await runtime.call(
                "fetchgraph_read_evidence",
                {
                    "ref": reference,
                    "offset": offset,
                    "length": 17,
                },
            )
            parts.append(page["text"])
            if page["next_offset"] is None:
                break
            offset = page["next_offset"]
        assert "".join(parts) == text
        page = await runtime.call(
            "fetchgraph_read_evidence", {"ref": reference, "offset": len(text)}
        )
        assert page["text"] == ""
        runtime.snapshots["answer"] = (
            time.monotonic() - 601,
            runtime.snapshots["answer"][1],
        )
        with pytest.raises(Failure) as error:
            await runtime.call("fetchgraph_read_evidence", {"ref": reference})
        assert error.value.code == "EVIDENCE_GONE"

    asyncio.run(exercise())


def test_cancelled_provider_retains_capacity(runtime):
    import threading

    entered = threading.Event()
    release = threading.Event()

    def blocked(*args):
        entered.set()
        release.wait(5)
        return [], []

    async def inference(*args):
        return json.dumps(
            {"context_plan": [{"provider": "shop", "selectors": {"op": "schema"}}]}
        )

    runtime.infer = inference
    runtime.fetch = blocked

    async def exercise():
        task = asyncio.create_task(
            runtime.call("fetchgraph_ask", {"source": "shop", "question": "?"})
        )
        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert runtime.active == 1
            assert not runtime.snapshots
            assert await runtime.call("fetchgraph_catalog", {})
        finally:
            release.set()
        for _ in range(100):
            if runtime.active == 0:
                break
            await asyncio.sleep(0.01)
        assert runtime.active == 0
        assert not runtime.snapshots

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "family,anchors",
    [
        ("EXPLAIN_TARGET", {"target_id": "target"}),
        ("EXPLAIN_DERIVATION", {"source_id": "evidence", "target_id": "target"}),
        ("CASE_DOSSIER", {"case_id": "evidence"}),
        ("VALIDATOR_DEFECT_PACK", {"defect_id": "evidence", "validator_name": "check"}),
    ],
)
def test_artifact_families(runtime, tmp_path, family, anchors):
    (tmp_path / "target.json").write_text(json.dumps({"steps": []}))
    (tmp_path / "evidence.json").write_text(
        json.dumps({"evidence": "observed evidence", "parent_id": "target"})
    )
    source = {"kind": "artifact", "run_dir": tmp_path, "run_id": "run"}
    pairs, gaps = runtime.fetch(
        source, {"family": family, "anchors": anchors}, "?", float("inf")
    )
    assert pairs
    assert all(locator.startswith("run/") for locator, text, data in pairs)
    pairs, gaps = runtime.fetch(
        source,
        {"family": "EXPLAIN_TARGET", "anchors": {"target_id": "targ"}},
        "?",
        float("inf"),
    )
    assert not pairs
    assert gaps


@pytest.mark.parametrize("selectors", [{}, {"key": "value"}])
def test_plugin_alias_and_invalid_citation_are_isolated(
    runtime, monkeypatch, selectors
):
    from fetchgraph import ProviderInfo
    import fetchgraph.retrieval.engine as module

    class Provider:
        name = "different_name"

        def describe(self):
            return ProviderInfo(
                name=self.name,
                selectors_schema={
                    "type": "object",
                    "properties": {"key": {"const": "value"}},
                    "required": list(selectors),
                    "additionalProperties": False,
                },
            )

        def fetch(self, question, selectors):
            assert selectors in ({}, {"key": "value"})
            return {"count": 42, "detail": "original data"}

        def serialize(self, obj):
            return "observed"

    monkeypatch.setattr(module, "factory", lambda name: lambda options: Provider())
    runtime.config["sources"]["shop"] = {
        **runtime.config["sources"]["shop"],
        "kind": "plugin",
        "factory": "test:provider",
        "options": {},
    }
    bad_citation = True

    async def inference(profile, stage, instruction, data):
        if stage == "plan":
            return json.dumps(
                {"context_plan": [{"provider": "shop", "selectors": selectors}]}
            )
        return json.dumps(
            {
                "status": "answered",
                "answer": "[9]" if bad_citation else "[1]",
                "gaps": [],
            }
        )

    runtime.infer = inference

    async def exercise():
        nonlocal bad_citation
        retrieval = await runtime.call(
            "fetchgraph_retrieve", {"source": "shop", "question": "?"}
        )
        assert retrieval["evidence"][0]["data"] == {
            "count": 42,
            "detail": "original data",
        }
        assert retrieval["evidence"][0]["text"] == "observed"
        snapshots_before = len(runtime.snapshots)
        with pytest.raises(Failure) as error:
            await runtime.call("fetchgraph_ask", {"source": "shop", "question": "?"})
        assert error.value.code == "INVALID_ANSWER"
        assert len(runtime.snapshots) == snapshots_before
        bad_citation = False
        answer = await runtime.call(
            "fetchgraph_ask", {"source": "shop", "question": "?"}
        )
        assert answer["evidence"][0]["locator"] == "shop"
        assert answer["evidence"][0]["source"] == "shop"

    asyncio.run(exercise())
