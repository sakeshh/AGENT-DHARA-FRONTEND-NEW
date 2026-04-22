"""
Tool implementations shared by the unified MCP and per-source MCP servers.
Each function returns a JSON string (stdio MCP contract).
"""
from __future__ import annotations

import json
import os
import shutil
import urllib.request

from mcp_server import mcp_helpers as H


def sources_overview() -> str:
    cfg = H.load_source_cfg()
    if cfg.get("_error"):
        return H.ok({"error": cfg["_error"], "sources_path": H.sources_yaml_path()})
    safe = {
        "sources_path": H.sources_yaml_path(),
        "project_root": H.project_root(),
        "source_name": cfg.get("name"),
        "locations": [],
    }
    for i, loc in enumerate(cfg.get("locations", [])):
        safe["locations"].append(
            {
                "index": i,
                "type": loc.get("type"),
                "connector": loc.get("connector"),
                "path": loc.get("path"),
                "connection": H.mask_secrets(loc.get("connection") or {}),
            }
        )
    return H.ok(safe)


def azure_blob_containers_overview() -> str:
    cfg = H.load_source_cfg()
    if cfg.get("_error"):
        return H.ok({"error": cfg["_error"]})
    rows = []
    for i, loc in enumerate(H.azure_blob_assessment_locations(cfg)):
        c = loc.get("connection") or {}
        rows.append(
            {
                "location_index": i,
                "container": c.get("container"),
                "id": loc.get("id"),
                "label": loc.get("label"),
            }
        )
    return H.ok({"azure_blob_locations": rows, "count": len(rows)})


def azure_blob_list_blobs(prefix: str = "", location_index: int = 0) -> str:
    conn, err = H.blob_connector(location_index)
    if err:
        return H.ok({"error": err})
    try:
        names = conn.list_blobs()
        if prefix:
            names = [n for n in names if n.startswith(prefix)]
        return H.ok({"count": len(names), "blobs": sorted(names)[:2000], "truncated": len(names) > 2000})
    except Exception as e:
        return H.ok({"error": str(e)})


def azure_blob_preview(blob_name: str, max_rows: int = 30, location_index: int = 0) -> str:
    conn, err = H.blob_connector(location_index)
    if err:
        return H.ok({"error": err})
    try:
        # Safety valves for large blobs (override via env)
        max_bytes = int(os.environ.get("ASSESS_MAX_BLOB_BYTES", str(20 * 1024 * 1024)))
        df = conn.load_blob(blob_name, max_rows=int(max_rows or 30), max_bytes=max_bytes)
        prev = H.df_preview_payload(df, max_rows)
        prev["blob_name"] = blob_name
        prev["location_index"] = location_index
        return H.ok(prev)
    except Exception as e:
        return H.ok({"error": str(e), "blob_name": blob_name})


def azure_blob_assess_selected(
    blob_names_csv: str,
    location_index: int = 0,
    max_rows_per_blob: int = 5000,
) -> str:
    """
    Run the data-quality assessment on a selected set of blob files only.

    Input:
    - blob_names_csv: comma-separated blob names (exact names from azure_blob_list_blobs)
    - location_index: which azure_blob container (0-based among azure_blob locations)
    - max_rows_per_blob: safety cap per blob for sampling
    """
    cfg = H.load_source_cfg()
    if cfg.get("_error"):
        return H.ok({"error": cfg["_error"]})
    blob_locs = H.azure_blob_assessment_locations(cfg)
    if not blob_locs:
        return H.ok({"error": "No azure_blob locations configured"})
    if location_index < 0 or location_index >= len(blob_locs):
        return H.ok({"error": f"Invalid location_index={location_index} (have {len(blob_locs)})"})
    loc = blob_locs[location_index]
    names = [n.strip() for n in (blob_names_csv or "").split(",") if n.strip()]
    if not names:
        return H.ok({"error": "Provide blob_names_csv (comma-separated blob names)"})
    try:
        from agent.mcp_clients import _single_location_config  # type: ignore
        from agent.mcp_interface import load_selected_blob_datasets, run_assessment

        cfg_text = _single_location_config({"name": cfg.get("name") or "source"}, loc)
        max_bytes = int(os.environ.get("ASSESS_MAX_BLOB_BYTES", str(20 * 1024 * 1024)))
        dfs = load_selected_blob_datasets(
            cfg_text,
            location_index=0,
            blob_names=names,
            max_rows=int(max_rows_per_blob or 5000),
            max_bytes=max_bytes,
        )
        result = run_assessment(cfg_text, additional_data=dfs)
        return H.ok({"success": True, "count": len(dfs), "blob_names": list(dfs.keys()), "result": result})
    except Exception as e:
        return H.ok({"error": str(e), "location_index": location_index})


def azure_blob_download_raw(blob_name: str, location_index: int = 0, output_filename: str = "") -> str:
    conn, err = H.blob_connector(location_index)
    if err:
        return H.ok({"error": err})
    try:
        name = output_filename.strip() or H.safe_name(os.path.basename(blob_name))
        out_path = os.path.join(H.raw_out_dir(), f"blob_{location_index}__{name}")
        t0 = H.time.time()
        saved = conn.download_blob_to_file(blob_name, out_path)
        size = os.path.getsize(saved) if os.path.isfile(saved) else None
        return H.ok(
            {
                "success": True,
                "blob_name": blob_name,
                "location_index": location_index,
                "saved_path": saved,
                "bytes": size,
                "elapsed_s": round(H.time.time() - t0, 3),
            }
        )
    except Exception as e:
        return H.ok({"error": str(e), "blob_name": blob_name, "location_index": location_index})


def local_file_preview(file_path: str, max_rows: int = 30) -> str:
    root = H.project_root()
    p = file_path
    if not os.path.isabs(p):
        p = os.path.join(root, p)
    try:
        df = H.load_local_file_to_df(p)
        out = H.df_preview_payload(df, max_rows)
        out["file_path"] = os.path.abspath(p)
        return H.ok(out)
    except Exception as e:
        return H.ok({"error": str(e), "file_path": p})


def local_file_export_raw(file_path: str, output_filename: str = "") -> str:
    root = H.project_root()
    p = file_path
    if not os.path.isabs(p):
        p = os.path.join(root, p)
    p = os.path.abspath(p)
    if not os.path.isfile(p):
        return H.ok({"error": f"File not found: {p}"})
    try:
        name = output_filename.strip() or H.safe_name(os.path.basename(p))
        out_path = os.path.join(H.raw_out_dir(), f"local__{name}")
        shutil.copyfile(p, out_path)
        return H.ok({"success": True, "source_path": p, "saved_path": out_path, "bytes": os.path.getsize(out_path)})
    except Exception as e:
        return H.ok({"error": str(e), "source_path": p})


def local_folder_list(folder_path: str = "") -> str:
    cfg = H.load_source_cfg()
    root = H.project_root()
    fp = folder_path.strip() or H.first_filesystem_path(cfg) or ""
    if not fp:
        return H.ok({"error": "No folder_path and no filesystem location in sources.yaml"})
    if not os.path.isabs(fp):
        fp = os.path.join(root, fp)
    if not os.path.isdir(fp):
        return H.ok({"error": f"Not a directory: {fp}"})
    files = sorted(f for f in os.listdir(fp) if os.path.isfile(os.path.join(fp, f)))
    return H.ok({"folder": fp, "count": len(files), "files": files[:500]})


def local_folder_assess_selected(
    files_csv: str,
    folder_path: str = "",
    max_rows_per_file: int = 5000,
) -> str:
    """
    Run the data-quality assessment on a selected set of local files only.

    - files_csv: comma-separated filenames (relative to folder_path)
    - folder_path: optional; defaults to first filesystem location in sources.yaml
    - max_rows_per_file: safety cap for CSV/TSV/JSONL sampling
    """
    cfg = H.load_source_cfg()
    root = H.project_root()
    fp = folder_path.strip() or H.first_filesystem_path(cfg) or ""
    if not fp:
        return H.ok({"error": "No folder_path and no filesystem location in sources.yaml"})
    if not os.path.isabs(fp):
        fp = os.path.join(root, fp)
    fp = os.path.abspath(fp)
    if not os.path.isdir(fp):
        return H.ok({"error": f"Not a directory: {fp}"})
    names = [f.strip() for f in (files_csv or "").split(",") if f.strip()]
    if not names:
        return H.ok({"error": "Provide files_csv (comma-separated filenames)"})
    # Load selected files with basic row-capping for CSV/TSV/JSONL
    try:
        import pandas as pd
        import json

        max_rows = max(1, min(int(max_rows_per_file or 5000), 200_000))
        dfs = {}
        for name in names:
            p = os.path.join(fp, name)
            if not os.path.isfile(p):
                return H.ok({"error": f"File not found: {p}", "file": name})
            low = p.lower()
            if low.endswith(".csv"):
                df = pd.read_csv(p, low_memory=False, nrows=max_rows)
            elif low.endswith(".tsv"):
                df = pd.read_csv(p, sep="\t", low_memory=False, nrows=max_rows)
            elif low.endswith(".jsonl"):
                rows = []
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rows.append(json.loads(line))
                        except Exception:
                            rows.append({"value": line})
                        if len(rows) >= max_rows:
                            break
                df = pd.json_normalize(rows, max_level=1) if rows else pd.DataFrame()
            else:
                # fall back to existing loader (full read)
                df = H.load_local_file_to_df(p)
            dfs[name] = df

        from agent.intelligent_data_assessment import load_and_profile

        result = load_and_profile({"name": cfg.get("name") or "source", "locations": []}, additional_data=dfs)
        return H.ok({"success": True, "count": len(dfs), "files": list(dfs.keys()), "folder": fp, "result": result})
    except Exception as e:
        return H.ok({"error": str(e), "folder": fp})


def rest_api_json_preview(url: str, max_rows: int = 50) -> str:
    max_rows = max(1, min(int(max_rows or 50), 500))
    req = urllib.request.Request(url, method="GET")
    bearer = os.environ.get("AGENT_DHARA_MCP_API_BEARER", "").strip()
    header = os.environ.get("AGENT_DHARA_MCP_API_HEADER", "").strip()
    if header:
        req.add_header("Authorization", header)
    elif bearer:
        req.add_header("Authorization", f"Bearer {bearer}")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
        import pandas as pd

        if isinstance(data, list):
            df = pd.json_normalize(data, max_level=2) if data else pd.DataFrame()
        elif isinstance(data, dict):
            df = pd.json_normalize(data, max_level=2)
        else:
            df = pd.DataFrame([{"value": data}])
        out = H.df_preview_payload(df, max_rows)
        out["url"] = url
        return H.ok(out)
    except Exception as e:
        return H.ok({"error": str(e), "url": url})


def database_locations_overview() -> str:
    cfg = H.load_source_cfg()
    if cfg.get("_error"):
        return H.ok({"error": cfg["_error"]})
    rows = []
    for i, loc in enumerate(H.database_locations(cfg)):
        c = H.mask_secrets(loc.get("connection") or {})
        rows.append(
            {
                "location_index": i,
                "id": loc.get("id"),
                "label": loc.get("label"),
                "server": c.get("server"),
                "database": c.get("database"),
            }
        )
    return H.ok({"database_locations": rows, "count": len(rows)})


def database_list_tables(location_index: int = 0) -> str:
    cfg = H.load_source_cfg()
    c = H.get_db_connection_at(cfg, location_index)
    if not c:
        n = len(H.database_locations(cfg))
        return H.ok({"error": f"No database at location_index={location_index} ({n} configured)"})
    try:
        from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector

        conn = AzureSQLPythonNetConnector(c)
        tables = conn.discover_tables()
        return H.ok({"tables": tables, "count": len(tables)})
    except Exception as e:
        return H.ok({"error": str(e), "hint": "Install pythonnet and .NET SQL client if using Azure SQL"})


def database_table_preview(table: str, max_rows: int = 25, location_index: int = 0) -> str:
    cfg = H.load_source_cfg()
    c = H.get_db_connection_at(cfg, location_index)
    if not c:
        return H.ok({"error": f"No database at location_index={location_index}"})
    try:
        from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector

        conn = AzureSQLPythonNetConnector(c)
        df = conn.preview_table(table, max_rows)
        out = H.df_preview_payload(df, max_rows)
        out["table"] = table
        out["location_index"] = location_index
        return H.ok(out)
    except Exception as e:
        return H.ok({"error": str(e), "table": table})


def database_table_export_raw(table: str, location_index: int = 0, output_filename: str = "") -> str:
    cfg = H.load_source_cfg()
    c = H.get_db_connection_at(cfg, location_index)
    if not c:
        return H.ok({"error": f"No database at location_index={location_index}"})
    try:
        from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector

        conn = AzureSQLPythonNetConnector(c)
        name = output_filename.strip() or H.safe_name(table.replace(".", "__") + ".csv")
        out_path = os.path.join(H.raw_out_dir(), f"sql_{location_index}__{name}")
        t0 = H.time.time()
        saved = conn.export_table_to_csv(table, out_path)
        size = os.path.getsize(saved) if os.path.isfile(saved) else None
        return H.ok(
            {
                "success": True,
                "table": table,
                "location_index": location_index,
                "saved_path": saved,
                "bytes": size,
                "elapsed_s": round(H.time.time() - t0, 3),
            }
        )
    except Exception as e:
        return H.ok({"error": str(e), "table": table, "hint": "Install pythonnet and .NET SQL client if using Azure SQL"})


def database_assess_selected_tables(
    tables_csv: str,
    location_index: int = 0,
    max_rows_per_table: int = 5000,
) -> str:
    """
    Run the data-quality assessment on a selected set of SQL tables only.

    - tables_csv: comma-separated table names (e.g. "dbo.Users,dbo.Orders")
    - location_index: which database location (0-based among type=database entries)
    - max_rows_per_table: safety cap per table for sampling
    """
    cfg = H.load_source_cfg()
    if cfg.get("_error"):
        return H.ok({"error": cfg["_error"]})
    c = H.get_db_connection_at(cfg, location_index)
    if not c:
        n = len(H.database_locations(cfg))
        return H.ok({"error": f"No database at location_index={location_index} ({n} configured)"})
    tables = [t.strip() for t in (tables_csv or "").split(",") if t.strip()]
    if not tables:
        return H.ok({"error": "Provide tables_csv (comma-separated table names)"})
    try:
        from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
        from agent.intelligent_data_assessment import load_and_profile

        conn = AzureSQLPythonNetConnector(c)
        max_rows = max(1, min(int(max_rows_per_table or 5000), 50_000))
        dfs = {}
        for t in tables:
            try:
                dfs[t] = conn.preview_table(t, max_rows)
            except Exception as e:
                return H.ok({"error": f"Failed to load table {t}: {e}", "table": t})
        # Assess only provided datasets (no implicit loading of other tables)
        result = load_and_profile({"name": cfg.get("name") or "source", "locations": []}, additional_data=dfs)
        return H.ok({"success": True, "count": len(dfs), "tables": list(dfs.keys()), "result": result})
    except Exception as e:
        return H.ok({"error": str(e), "hint": "Ensure pythonnet and SQL client are installed; check SQL firewall rules."})


def stream_json_file_preview(file_path: str, max_rows: int = 200) -> str:
    root = H.project_root()
    p = file_path if os.path.isabs(file_path) else os.path.join(root, file_path)
    try:
        df = H.load_stream_json_file(p, max_rows)
        out = H.df_preview_payload(df, min(max_rows, 500))
        out["file_path"] = os.path.abspath(p)
        out["note"] = f"Loaded at most {max_rows} records from array start"
        return H.ok(out)
    except Exception as e:
        return H.ok({"error": str(e), "file_path": p})


def run_data_assessment_cli_hint() -> str:
    return H.ok(
        {
            "command": "python main.py --sources config/sources.yaml",
            "cwd": H.project_root(),
            "note": "Full assessment loads entire DB tables and all blobs; use CLI, not MCP, for heavy runs.",
        }
    )
