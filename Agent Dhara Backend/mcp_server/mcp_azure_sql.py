"""
stdio MCP — Azure SQL only.

Run: python -m mcp_server.mcp_azure_sql

Connect in Cursor / Claude / Foundry with command + args below.
Requires: pythonnet + .NET SQL client for database_* tools.
"""
from __future__ import annotations

import mcp_server.mcp_helpers  # noqa: F401

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:
    raise SystemExit("Install MCP SDK: pip install mcp\n" + str(e)) from e

from mcp_server import mcp_tools as T

mcp = FastMCP("Agent Dhara Azure SQL")

mcp.tool()(T.database_locations_overview)
mcp.tool()(T.database_list_tables)
mcp.tool()(T.database_table_preview)
mcp.tool()(T.database_table_export_raw)

if __name__ == "__main__":
    mcp.run(transport="stdio")
