"""Exercise real SDK transport and HTTP inference through the CLI."""

import asyncio
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytest.importorskip("mcp")

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from test_runtime import ROOT


def test_stdio_inference_and_evidence(tmp_path):
    requests = []

    class Model(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(payload)
            if payload["model"] == "planner":
                answer = {
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
            else:
                assert (
                    '{"n":1000}'
                    in json.loads(payload["messages"][1]["content"])["evidence"][0][
                        "text"
                    ]
                )
                answer = {"status": "answered", "answer": "1000 [1]", "gaps": []}
            response = json.dumps(
                {
                    "choices": [
                        {
                            "message": {"content": json.dumps(answer)},
                            "finish_reason": "stop",
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    http = ThreadingHTTPServer(("127.0.0.1", 0), Model)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    config = tmp_path / "config.toml"
    config.write_text(f'''
version = 1
[profiles.local]
base_url = "http://127.0.0.1:{http.server_port}/v1"
model = "test"
plan_model = "planner"
synth_model = "writer"
[sources.shop]
kind = "csv"
description = "Shop"
data_dir = "{ROOT / "_demo_data/shop"}"
schema = "{ROOT / "_demo_data/shop/schema.yaml"}"
inference_profiles = ["local"]
default_profile = "local"
''')

    async def exercise():
        params = StdioServerParameters(
            command=sys.executable,
            args=[
                "-c",
                "from fetchgraph.mcp.server import main; main()",
                "--config",
                str(config),
            ],
        )
        async with stdio_client(params) as (reader, writer):
            async with ClientSession(reader, writer) as client:
                await client.initialize()
                assert len((await client.list_tools()).tools) == 4
                catalog = await client.call_tool("fetchgraph_catalog", {})
                assert not catalog.isError
                retrieval = await client.call_tool(
                    "fetchgraph_retrieve",
                    {"source": "shop", "question": "Count orders?"},
                )
                assert not retrieval.isError, retrieval
                bundle = retrieval.structuredContent
                assert bundle["evidence"][0]["data"] == {"n": 1000}
                assert bundle["diagnostics"]["llm_calls"] == 1
                assert len(requests) == 1
                page = await client.call_tool(
                    "fetchgraph_read_evidence", {"ref": bundle["evidence"][0]["ref"]}
                )
                assert page.structuredContent["text"] == bundle["evidence"][0]["text"]
                answer = await client.call_tool(
                    "fetchgraph_ask", {"source": "shop", "question": "Count orders?"}
                )
                assert not answer.isError, answer
                assert json.loads(answer.content[0].text) == answer.structuredContent
                ref = answer.structuredContent["evidence"][0]["ref"]
                page = await client.call_tool("fetchgraph_read_evidence", {"ref": ref})
                assert page.structuredContent["text"] == '{"n":1000}'
                invalid = await client.call_tool(
                    "fetchgraph_ask", {"source": "shop", "question": "?", "extra": True}
                )
                assert invalid.isError
                assert invalid.structuredContent["error"]["code"] == "INVALID_ARGUMENT"

    try:
        asyncio.run(exercise())
        from fetchgraph import retrieve

        bundle = retrieve("Count orders?", config=config)
        assert bundle.evidence[0].data == {"n": 1000}
        assert bundle.diagnostics.llm_calls == 1
    finally:
        http.shutdown()
        http.server_close()
        thread.join()
    assert [r["model"] for r in requests] == ["planner", "planner", "writer", "planner"]
