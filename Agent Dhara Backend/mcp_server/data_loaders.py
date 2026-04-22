"""
Shared helpers for MCP tools: paths, masked config, local file → DataFrame.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional

import pandas as pd

_SENSITIVE_KEYS = re.compile(
    r"(password|secret|key|token|credential|connection_string|account_key)",
    re.I,
)


def project_root() -> str:
    r = os.environ.get("AGENT_DHARA_PROJECT_ROOT", "").strip()
    if r and os.path.isdir(r):
        return os.path.abspath(r)
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def sources_yaml_path() -> str:
    p = os.environ.get("AGENT_DHARA_SOURCES_PATH", "").strip()
    if p:
        if os.path.isfile(p):
            return os.path.abspath(p)
        cand = os.path.join(project_root(), p)
        if os.path.isfile(cand):
            return os.path.abspath(cand)
        return os.path.abspath(p)
    return os.path.join(project_root(), "config", "sources.yaml")


def load_source_cfg() -> Dict[str, Any]:
    path = sources_yaml_path()
    if not os.path.isfile(path):
        return {"_error": f"sources file not found: {path}", "locations": []}
    try:
        import yaml

        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except Exception as e:
        return {"_error": str(e), "locations": []}
    src = raw.get("source", raw)
    if not isinstance(src, dict):
        return {"_error": "invalid sources YAML shape", "locations": []}
    return src


def mask_secrets(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            sk = str(k)
            if _SENSITIVE_KEYS.search(sk):
                out[k] = "***" if v else v
            else:
                out[k] = mask_secrets(v)
        return out
    if isinstance(obj, list):
        return [mask_secrets(x) for x in obj]
    return obj


def database_locations(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        loc
        for loc in cfg.get("locations", [])
        if (loc.get("type") or "").lower() == "database"
    ]


def get_db_connection_at(cfg: Dict[str, Any], location_index: int) -> Optional[Dict[str, Any]]:
    locs = database_locations(cfg)
    if location_index < 0 or location_index >= len(locs):
        return None
    return locs[location_index].get("connection") or {}


def get_db_connection_cfg(cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return get_db_connection_at(cfg, 0)


def azure_blob_assessment_locations(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """All type=azure_blob entries (same order as main.py loads)."""
    return [
        loc
        for loc in cfg.get("locations", [])
        if (loc.get("type") or "").lower() == "azure_blob"
    ]


def azure_blob_connection_at(cfg: Dict[str, Any], location_index: int) -> Optional[Dict[str, Any]]:
    locs = azure_blob_assessment_locations(cfg)
    if location_index < 0 or location_index >= len(locs):
        return None
    return locs[location_index].get("connection") or {}


def first_azure_blob_connection(cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """First azure_blob location (index 0)."""
    return azure_blob_connection_at(cfg, 0)


def first_filesystem_path(cfg: Dict[str, Any]) -> Optional[str]:
    for loc in cfg.get("locations", []):
        if (loc.get("type") or "").lower() == "filesystem":
            p = loc.get("path")
            if p:
                return str(p)
    return None


def load_local_file_to_df(path: str) -> pd.DataFrame:
    path = os.path.abspath(os.path.normpath(path))
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    low = path.lower()
    if low.endswith(".csv"):
        return pd.read_csv(path, low_memory=False)
    if low.endswith(".tsv"):
        return pd.read_csv(path, sep="\t", low_memory=False)
    if low.endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.json_normalize(data, max_level=1) if data else pd.DataFrame()
        if isinstance(data, dict):
            return pd.json_normalize(data, max_level=1)
        return pd.DataFrame([{"value": data}])
    if low.endswith(".jsonl"):
        rows: List[Any] = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    rows.append({"value": line})
        return pd.json_normalize(rows, max_level=1) if rows else pd.DataFrame()
    if low.endswith(".parquet"):
        return pd.read_parquet(path)
    if low.endswith(".xlsx") or low.endswith(".xls"):
        return pd.read_excel(path, engine="openpyxl" if low.endswith(".xlsx") else None)
    raise ValueError(f"Unsupported extension: {path}")


def df_preview_payload(df: pd.DataFrame, max_rows: int = 50) -> Dict[str, Any]:
    max_rows = max(1, min(int(max_rows or 50), 500))
    n = len(df)
    cols = [str(c) for c in df.columns]
    if n == 0:
        return {"total_rows": 0, "columns": cols, "sample_rows": []}
    head = df.head(max_rows)
    try:
        sample = json.loads(head.to_json(orient="records", date_format="iso"))
    except Exception:
        sample = head.astype(str).to_dict(orient="records")
    return {
        "total_rows": int(n),
        "columns": cols,
        "sample_rows": sample,
        "preview_row_count": len(sample),
    }


def load_stream_json_file(path: str, max_rows: int) -> pd.DataFrame:
    """JSON file containing an array of records (same as main.py --stream-file style)."""
    path = os.path.abspath(os.path.normpath(path))
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Stream file must be a JSON array of objects")
    max_rows = max(1, min(int(max_rows or 1000), 50_000))
    data = data[:max_rows]
    if not data:
        return pd.DataFrame()
    if isinstance(data[0], dict):
        return pd.json_normalize(data, max_level=1)
    return pd.DataFrame({"value": data})
