"""
Agent Dhara Data MCP Server (stdio) — all tools in one process.

For separate MCPs per data source, use instead:
  python -m mcp_server.mcp_azure_sql
  python -m mcp_server.mcp_azure_blob
  python -m mcp_server.mcp_local_fs
  python -m mcp_server.mcp_stream

See mcp_server/README_MCP.md and config/mcp-servers.example.json.
"""
from __future__ import annotations

import mcp_server.mcp_helpers  # noqa: F401 — ensures repo root on sys.path

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:
    raise SystemExit("Install MCP SDK: pip install mcp\n" + str(e)) from e

from mcp_server import mcp_tools as T

mcp = FastMCP("Agent Dhara Data")

mcp.tool()(T.sources_overview)
mcp.tool()(T.azure_blob_containers_overview)
mcp.tool()(T.azure_blob_list_blobs)
mcp.tool()(T.azure_blob_preview)
mcp.tool()(T.azure_blob_download_raw)
mcp.tool()(T.azure_blob_assess_selected)
mcp.tool()(T.local_file_preview)
mcp.tool()(T.local_file_export_raw)
mcp.tool()(T.local_folder_list)
mcp.tool()(T.rest_api_json_preview)
mcp.tool()(T.database_locations_overview)
mcp.tool()(T.database_list_tables)
mcp.tool()(T.database_table_preview)
mcp.tool()(T.database_table_export_raw)
mcp.tool()(T.database_assess_selected_tables)
mcp.tool()(T.stream_json_file_preview)
mcp.tool()(T.run_data_assessment_cli_hint)
mcp.tool()(T.local_folder_assess_selected)

if __name__ == "__main__":
    mcp.run(transport="stdio")
