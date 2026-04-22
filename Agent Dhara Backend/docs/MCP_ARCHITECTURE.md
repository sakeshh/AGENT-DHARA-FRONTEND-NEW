# MCP architecture (product layout)

## Single source of truth: `config/sources.yaml`

All four data paths read connection details from the same YAML as `main.py`:

- `type: database` → Azure SQL (pythonnet + .NET)
- `type: azure_blob` → assessment containers
- `type: filesystem` → local folders
- Stream uses a **JSON array file** (no YAML block required)

Use `config/sources_azure_env.yaml` with `${VAR}` placeholders for production.

## Four stdio MCP servers (connect from Foundry / Cursor / Claude)

| Server module | Purpose |
|---------------|---------|
| `python -m mcp_server.mcp_azure_sql` | SQL tools only |
| `python -m mcp_server.mcp_azure_blob` | Blob tools only |
| `python -m mcp_server.mcp_local_fs` | Local FS tools only |
| `python -m mcp_server.mcp_stream` | Stream snapshot preview |

Unified (all tools): `python -m mcp_server.agent_dhara_data_mcp`

Shared implementations live in `mcp_server/mcp_tools.py`; helpers in `mcp_server/mcp_helpers.py`.

## In-process “MCP client” (no subprocess)

For services running inside this repository (FastAPI, LangGraph, tests):

```python
from mcp_runtime import InProcessMCPBridge

bridge = InProcessMCPBridge()
data = bridge.parse(bridge.sql_list_tables(0))
```

Set `AGENT_DHARA_PROJECT_ROOT` and optionally `AGENT_DHARA_SOURCES_PATH` (see `config/mcp.connections.example.env`).

## CLI evaluation scope (wait for user choice)

`main.py` defaults to an **interactive menu** when stdin is a TTY (`--evaluate auto`).

- `--evaluate sql|blob|local|stream|all` — non-interactive
- `--evaluate interactive` — always show menu
- Full assessment only loads the selected source types; others are skipped even if configured.

Assessment JSON includes `run_metadata.evaluation_scope`.

## Docker

Mount your project directory, set `AGENT_DHARA_PROJECT_ROOT`, mount or inject `config/sources.yaml` (or env-substituted file). Run the MCP command as the container entrypoint or run `main.py` with `--evaluate`.
