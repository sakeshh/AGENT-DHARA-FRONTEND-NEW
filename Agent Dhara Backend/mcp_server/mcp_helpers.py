"""Shared helpers for stdio MCP servers (paths, JSON responses, blob connector)."""
from __future__ import annotations

import json
import os
import re
import sys
import time

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from mcp_server.data_loaders import (
    azure_blob_assessment_locations,
    azure_blob_connection_at,
    df_preview_payload,
    first_filesystem_path,
    database_locations,
    get_db_connection_at,
    load_local_file_to_df,
    load_source_cfg,
    load_stream_json_file,
    mask_secrets,
    project_root,
    sources_yaml_path,
)


def ok(data: dict) -> str:
    return json.dumps(data, indent=2, default=str)


def raw_out_dir() -> str:
    d = os.path.join(project_root(), "output", "raw")
    os.makedirs(d, exist_ok=True)
    return d


def safe_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\.\-]+", "_", s)
    return s.strip("_") or "data"


def blob_connector(location_index: int = 0):
    cfg = load_source_cfg()
    if cfg.get("_error"):
        return None, cfg["_error"]
    conn = azure_blob_connection_at(cfg, location_index)
    n = len(azure_blob_assessment_locations(cfg))
    if not conn:
        return None, f"No azure_blob at location_index={location_index} ({n} container(s) configured)"
    try:
        from connectors.azure_blob_storage import AzureBlobStorageConnector

        return AzureBlobStorageConnector(conn), None
    except Exception as e:
        return None, str(e)

