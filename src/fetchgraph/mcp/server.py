"""stdio entry point using the official MCP SDK."""

import argparse
import asyncio
import logging
import sys

from .config import load_config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    logging.getLogger("fetchgraph").setLevel(logging.CRITICAL)
    try:
        config = load_config(args.config)
    except Exception:
        print("Invalid FetchGraph MCP configuration.", file=sys.stderr)
        raise SystemExit(2) from None
    if args.check_config:
        print("FetchGraph MCP configuration valid.", file=sys.stderr)
        return
    asyncio.run(serve(config))


async def serve(config):
    import mcp.types as types
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from .runtime import Failure, Runtime, TOOLS, dump

    runtime = Runtime(config)
    server = Server("fetchgraph")

    @server.list_tools()
    async def list_tools():
        return [types.Tool.model_validate(t) for t in TOOLS]

    @server.call_tool(validate_input=False)
    async def call_tool(name, arguments):
        try:
            result = await runtime.call(name, arguments or {})
            error = False
        except Failure as exc:
            result = {
                "error": {
                    "code": exc.code,
                    "message": exc.code,
                    "retryable": exc.retryable,
                }
            }
            error = True
        except asyncio.CancelledError:
            raise
        except Exception:
            result = {
                "error": {
                    "code": "INTERNAL",
                    "message": "Internal server error.",
                    "retryable": False,
                }
            }
            error = True
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=dump(result))],
            structuredContent=result,
            isError=error,
        )

    async with stdio_server() as (reader, writer):
        await server.run(reader, writer, server.create_initialization_options())
