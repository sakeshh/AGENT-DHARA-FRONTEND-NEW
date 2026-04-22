"""
stdio MCP — Azure Blob (assessment containers from sources.yaml) only.

Run: python -m mcp_server.mcp_azure_blob
"""
from __future__ import annotations

import mcp_server.mcp_helpers  # noqa: F401

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:
    raise SystemExit("Install MCP SDK: pip install mcp\n" + str(e)) from e

from mcp_server import mcp_tools as T

mcp = FastMCP("Agent Dhara Azure Blob")

mcp.tool()(T.azure_blob_containers_overview)
mcp.tool()(T.azure_blob_list_blobs)
mcp.tool()(T.azure_blob_preview)
mcp.tool()(T.azure_blob_download_raw)

if __name__ == "__main__":
    mcp.run(transport="stdio")
