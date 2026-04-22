"""
In-process MCP bridge: same tool behavior as stdio MCP servers without a subprocess.

Use this from LangGraph / FastAPI / Foundry-backed services that run inside this repo.
Each method returns the same JSON string the stdio tools return; parse with `json.loads` if needed.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, Optional

from mcp_runtime.settings import MCPRuntimeSettings


def _ensure_path(settings: MCPRuntimeSettings) -> None:
    r = settings.project_root
    if r and os.path.isdir(r) and r not in sys.path:
        sys.path.insert(0, r)
    os.environ.setdefault("AGENT_DHARA_PROJECT_ROOT", settings.project_root)
    os.environ.setdefault("AGENT_DHARA_SOURCES_PATH", settings.sources_path)


class InProcessMCPBridge:
    """
    Facade over `mcp_server.mcp_tools` grouped like the four stdio MCPs:

    - `azure_sql`  → mcp_azure_sql
    - `azure_blob` → mcp_azure_blob
    - `local`      → mcp_local_fs
    - `stream`     → mcp_stream
    """

    def __init__(self, settings: Optional[MCPRuntimeSettings] = None) -> None:
        self.settings = settings or MCPRuntimeSettings.from_env()
        _ensure_path(self.settings)
        from mcp_server import mcp_tools as T  # noqa: WPS433

        self._T = T

    # --- Azure SQL (MCP: mcp_azure_sql) ---
    def sql_locations_overview(self) -> str:
        return self._T.database_locations_overview()

    def sql_list_tables(self, location_index: int = 0) -> str:
        return self._T.database_list_tables(location_index)

    def sql_table_preview(self, table: str, max_rows: int = 25, location_index: int = 0) -> str:
        return self._T.database_table_preview(table, max_rows, location_index)

    def sql_table_export_raw(self, table: str, location_index: int = 0, output_filename: str = "") -> str:
        return self._T.database_table_export_raw(table, location_index, output_filename)

    # --- Azure Blob (MCP: mcp_azure_blob) ---
    def blob_containers_overview(self) -> str:
        return self._T.azure_blob_containers_overview()

    def blob_list(self, prefix: str = "", location_index: int = 0) -> str:
        return self._T.azure_blob_list_blobs(prefix, location_index)

    def blob_preview(self, blob_name: str, max_rows: int = 30, location_index: int = 0) -> str:
        return self._T.azure_blob_preview(blob_name, max_rows, location_index)

    def blob_download_raw(self, blob_name: str, location_index: int = 0, output_filename: str = "") -> str:
        return self._T.azure_blob_download_raw(blob_name, location_index, output_filename)

    # --- Local FS (MCP: mcp_local_fs) ---
    def local_folder_list(self, folder_path: str = "") -> str:
        return self._T.local_folder_list(folder_path)

    def local_file_preview(self, file_path: str, max_rows: int = 30) -> str:
        return self._T.local_file_preview(file_path, max_rows)

    def local_file_export_raw(self, file_path: str, output_filename: str = "") -> str:
        return self._T.local_file_export_raw(file_path, output_filename)

    # --- Stream (MCP: mcp_stream) ---
    def stream_json_preview(self, file_path: str, max_rows: int = 200) -> str:
        return self._T.stream_json_file_preview(file_path, max_rows)

    # --- Unified helpers ---
    def sources_overview(self) -> str:
        return self._T.sources_overview()

    def parse(self, json_str: str) -> Dict[str, Any]:
        return json.loads(json_str)
