# Agent Dhara Data MCP Server

This exposes **tools** so an MCP-capable editor or Claude Desktop can list/preview data from the same places as `main.py`: **Azure Blob**, **local files**, **REST JSON**, **Azure SQL**, **JSON stream files**.

## 1. Install

From the **project root** (folder that contains `main.py`):

```bash
pip install -r requirements.txt
```

## 2. Replace these values

| What | Where |
|------|--------|
| **Connections** | `config/sources.yaml` — same as for `python main.py` (DB, Azure Blob, filesystem paths). |
| **Project path** (if MCP runs with wrong cwd) | Env `AGENT_DHARA_PROJECT_ROOT` = absolute path to project root. |
| **Different sources file** | Env `AGENT_DHARA_SOURCES_PATH` = absolute path to your YAML, or path relative to project root. |
| **REST API auth** | Env `AGENT_DHARA_MCP_API_BEARER` = token, **or** `AGENT_DHARA_MCP_API_HEADER` = full `Authorization` header value. |

## 3. MCP client config

Merge the example below into your editor’s MCP settings (or Claude Desktop config). Template file: **`config/mcp-servers.example.json`**.

### One MCP vs separate MCPs per source

- **All tools in one process:** `python -m mcp_server.agent_dhara_data_mcp` (same as before).
- **Separate stdio MCPs** (connect only what you need in Foundry / Cursor):
  - `python -m mcp_server.mcp_azure_sql` — SQL only
  - `python -m mcp_server.mcp_azure_blob` — Blob only
  - `python -m mcp_server.mcp_local_fs` — local files/folders only
  - `python -m mcp_server.mcp_stream` — JSON stream snapshot files only

`config/mcp-servers.example.json` lists entries for each.

Replace `YOUR_PYTHON` and `YOUR_PROJECT_ROOT` with your machine paths (Windows example below).

```json
{
  "mcpServers": {
    "agent-dhara-data": {
      "command": "C:\\\\Users\\\\pc\\\\AppData\\\\Local\\\\Programs\\\\Python\\\\Python312\\\\python.exe",
      "args": ["-m", "mcp_server.agent_dhara_data_mcp"],
      "cwd": "C:\\\\path\\\\to\\\\your\\\\project_root",
      "env": {
        "AGENT_DHARA_PROJECT_ROOT": "C:\\\\path\\\\to\\\\your\\\\project_root"
      }
    }
  }
}
```

- **`command`**: Run `where python` (Windows) or `which python` and paste that executable.
- **`cwd`** / **`AGENT_DHARA_PROJECT_ROOT`**: Must be the folder containing `main.py`, `config/`, `connectors/`.

If you use a venv:

```json
"command": "C:\\\\path\\\\to\\\\venv\\\\Scripts\\\\python.exe",
```

Restart the editor after saving MCP config.

## 4. Tools available

| Tool | Purpose |
|------|---------|
| `sources_overview` | Masked view of `sources.yaml` locations |
| `azure_blob_containers_overview` | Indices for each `azure_blob` in YAML |
| `azure_blob_list_blobs` | List blobs (`location_index` = which container) |
| `azure_blob_preview` | Sample rows (`location_index` selects container) |
| `azure_blob_download_raw` | Full blob → `output/raw/` (returns `saved_path`) |
| `local_file_preview` | CSV/JSON/Parquet/XLSX on disk |
| `local_file_export_raw` | Copy full file → `output/raw/` |
| `local_folder_list` | Files in a folder (or first `filesystem` path) |
| `rest_api_json_preview` | GET URL → table preview |
| `database_locations_overview` | Indices for each `database` in YAML |
| `database_list_tables` / `database_table_preview` | Azure SQL (`location_index` picks server) |
| `database_table_export_raw` | Full table → CSV under `output/raw/` |
| `stream_json_file_preview` | JSON array file (batch/stream snapshot) |
| `run_data_assessment_cli_hint` | Reminder to run full assessment via CLI |

Full assessment of **all** blobs/tables stays on the CLI: `python main.py` (too heavy for MCP).

## 5. Manual test (optional)

```bash
cd YOUR_PROJECT_ROOT
python -m mcp_server.agent_dhara_data_mcp
```

It will wait on stdio (normal). Use [MCP Inspector](https://github.com/modelcontextprotocol/inspector) or your editor’s MCP panel to talk to it.
