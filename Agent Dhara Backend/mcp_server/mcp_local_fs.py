"""
stdio MCP — local filesystem only (paths relative to AGENT_DHARA_PROJECT_ROOT or absolute).

Run: python -m mcp_server.mcp_local_fs
"""
from __future__ import annotations

import mcp_server.mcp_helpers  # noqa: F401

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:
    raise SystemExit("Install MCP SDK: pip install mcp\n" + str(e)) from e

from mcp_server import mcp_tools as T

mcp = FastMCP("Agent Dhara Local FS")

mcp.tool()(T.local_folder_list)
mcp.tool()(T.local_file_preview)
mcp.tool()(T.local_file_export_raw)

if __name__ == "__main__":
    mcp.run(transport="stdio")
