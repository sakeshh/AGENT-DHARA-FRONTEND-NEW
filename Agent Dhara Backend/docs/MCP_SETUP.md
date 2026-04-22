# MCP Server: What You Need to Fill In

## Short answer: **nothing in the code**

You do **not** need to change or fill in anything inside `mcp_server.py` or `mcp_interface.py` for the MCP server to run. It works as-is.

What *does* matter is **what the client sends** when it calls the API, and **where the server runs** (so it can reach paths or Azure if you use them).

---

## What each endpoint needs (from the **caller**, not from code)

| Endpoint | What the **client** must send | Anything to configure in code? |
|----------|-------------------------------|---------------------------------|
| **GET /** | Nothing | No |
| **POST /upload** | Form body: file (e.g. CSV). Optional `?format=html` or `?format=md` | No |
| **POST /stream** | JSON body: `{"records": [{...}, ...], "name": "stream"}` | No |
| **POST /run** | JSON body: `{"config": "<yaml or json string>"}`. That string is your sources config (same shape as `sources.yaml`). | No. Optional: set env `MCP_DEFAULT_CONFIG_PATH` so `/run` can use a file if no body is sent (see below). |
| **POST /list_tables** | JSON body: `{"config": "<yaml or json string>"}` with a `database` location. | No |
| **POST /load_path** | JSON body: `{"path": "/absolute/or/relative/path"}`. Path must exist on the machine where the MCP server is running. | No |

So: **no credentials or paths are hardcoded in the MCP code.** The client supplies config (for `/run`, `/list_tables`) or file/path (for `/upload`, `/load_path`, `/stream`).

---

## Optional: default config file for `/run`

If you want **POST /run** to use your `sources.yaml` when the client doesn’t send a config (or sends an empty one), set this **environment variable** before starting the server:

- **`MCP_DEFAULT_CONFIG_PATH`** = path to your config file (e.g. `config/sources.yaml`)

Example (PowerShell):

```powershell
$env:MCP_DEFAULT_CONFIG_PATH = "config/sources.yaml"
python -m agent.mcp_server
```

Example (Linux/macOS):

```bash
export MCP_DEFAULT_CONFIG_PATH=config/sources.yaml
python -m agent.mcp_server
```

If this env var is set, the server will use that file when the request body is missing or empty. You still don’t need to fill anything *inside* the code.

---

## Summary

| Question | Answer |
|----------|--------|
| Do I need to fill in anything in the MCP code? | **No.** |
| What do I need to provide? | For `/run` and `/list_tables`: the **client** sends the config in the request body (or use `MCP_DEFAULT_CONFIG_PATH` for `/run`). For `/upload` and `/stream`: the client sends the file or records. |
| Where do credentials live? | In the **config string** the client sends (or in the file at `MCP_DEFAULT_CONFIG_PATH`). Never hardcode them in the MCP server code. |
