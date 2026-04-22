#!/usr/bin/env python3
"""Run from project root: python scripts/verify_mcp_ready.py — exits 0 if imports and entry points look OK."""
from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.environ.setdefault("AGENT_DHARA_PROJECT_ROOT", ROOT)


def main() -> int:
    errors: list[str] = []

    try:
        import mcp.server.fastmcp  # noqa: F401
    except ImportError as e:
        errors.append(f"mcp SDK missing: pip install mcp  ({e})")

    try:
        from mcp_server import mcp_tools as T
        from mcp_server import mcp_helpers as H  # noqa: F401

        assert hasattr(T, "database_list_tables")
        assert hasattr(T, "azure_blob_list_blobs")
        assert hasattr(T, "local_folder_list")
        assert hasattr(T, "stream_json_file_preview")
    except Exception as e:
        errors.append(f"mcp_server tools import failed: {e}")

    try:
        from mcp_runtime import InProcessMCPBridge

        b = InProcessMCPBridge()
        s = b.sources_overview()
        if not s or "sources_path" not in s and "error" not in s:
            errors.append("InProcessMCPBridge.sources_overview() unexpected output")
    except Exception as e:
        errors.append(f"mcp_runtime bridge failed: {e}")

    for mod in (
        "mcp_server.agent_dhara_data_mcp",
        "mcp_server.mcp_azure_sql",
        "mcp_server.mcp_azure_blob",
        "mcp_server.mcp_local_fs",
        "mcp_server.mcp_stream",
    ):
        try:
            __import__(mod)
        except Exception as e:
            errors.append(f"import {mod}: {e}")

    try:
        from agent.evaluation_scope import MODE_ALL, interactive_select_mode  # noqa: F401
    except Exception as e:
        errors.append(f"agent.evaluation_scope: {e}")

    src = os.path.join(ROOT, "config", "sources.yaml")
    if not os.path.isfile(src):
        errors.append(f"config/sources.yaml missing at {src}")

    if errors:
        print("NOT READY:\n", "\n".join(f"  - {x}" for x in errors), file=sys.stderr)
        return 1

    print("OK: MCP modules import; mcp_runtime bridge works; config/sources.yaml exists.")
    print("Next: fill sources.yaml credentials; register MCP in Cursor per config/mcp-servers.example.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
