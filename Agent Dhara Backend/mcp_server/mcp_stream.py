"""
stdio MCP — JSON stream / snapshot files only (array-of-records files).

Run: python -m mcp_server.mcp_stream
"""
from __future__ import annotations

import mcp_server.mcp_helpers  # noqa: F401

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:
    raise SystemExit("Install MCP SDK: pip install mcp\n" + str(e)) from e

from mcp_server import mcp_tools as T

mcp = FastMCP("Agent Dhara Stream")

mcp.tool()(T.stream_json_file_preview)

if __name__ == "__main__":
    mcp.run(transport="stdio")
