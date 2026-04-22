# MCP Server: Role, How It Works, and Docker

## What is the MCP server’s role?

The **MCP server** is an **HTTP API** that exposes your Intelligent Data Assessment logic so other systems can call it instead of running the CLI.

| Who calls it | Why |
|--------------|-----|
| **Azure AI Foundry / Copilot / other agents** | To run an assessment on demand (e.g. “assess the data for this container”) by sending a POST request with config or an uploaded file. |
| **Your own apps or scripts** | To trigger assessment from a web app, pipeline, or automation without shelling out to `python main.py`. |
| **External tools (Postman, curl, Logic Apps, ADF)** | To run assessment or validate a stream of records over HTTP. |

So: **same engine as `main.py`**, but callable over the network instead of only via the command line.

---

## How does it work?

### Architecture (simple)

```
┌─────────────────────────────────────────────────────────────────┐
│  Client (browser, agent, ADF, curl, etc.)                        │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTP (POST/GET)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  MCP Server (FastAPI app in agent/mcp_server.py)                │
│  - Receives JSON/form data                                      │
│  - Calls agent/mcp_interface.py functions                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  mcp_interface.py  →  intelligent_data_assessment.py             │
│  (run_assessment, process_uploaded_file, process_stream_chunk…)  │
│  (load_and_profile, profile_dataframe, analyze_dataset_quality)  │
└─────────────────────────────────────────────────────────────────┘
```

- **mcp_server.py** = FastAPI app: defines routes, parses request bodies, returns JSON.
- **mcp_interface.py** = Thin wrapper: turns HTTP inputs (config string, file bytes, path) into calls to the assessment engine and returns the same structures as `load_and_profile()`.

### Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| **GET /** | Health | Check if the server is up. |
| **POST /run** | Run assessment | Body: `{"config": "<yaml or json string>"}`. Runs assessment using that config (database + filesystem from config; Azure Blob is not loaded by the server unless you add a step that fetches from Blob and passes `additional_data`). |
| **POST /list_tables** | List SQL tables | Body: `{"config": "..."}`. Returns table names for the database in the config. |
| **POST /stream** | Validate a batch | Body: `{"records": [{...}, ...], "name": "stream"}`. Treats the array as one dataset and returns profile + quality. |
| **POST /upload** | Assess one file | Form: file upload. Optionally `?format=html` or `?format=md` to get a report string in the response. |
| **POST /load_path** | Load from disk | Body: `{"path": "/some/dir"}`. Loads all supported files from that path; returns dataset names and count (not full data in JSON). |

So:

- **/run** = “Run full assessment from a config string” (DB + filesystem only from server’s perspective unless you extend it to pull from Blob and pass `additional_data`).
- **/upload** = “Assess this one file I’m uploading.”
- **/stream** = “Assess this list of records (e.g. a batch from a stream).”

The **role** of the MCP server is: **make the same assessment capabilities available over HTTP** for agents, apps, and pipelines.

---

## Should you use Docker?

### When Docker helps

- **Deploying the MCP server** to a cloud (e.g. Azure Container Apps, AKS, App Service with custom container): same image runs anywhere, with a fixed Python and dependency set.
- **CI/CD**: run the server or CLI in a container for tests or one-off jobs.
- **No “works on my machine”**: dev and prod use the same runtime.

### When you might skip Docker (for now)

- **Local dev and demos**: `python -m agent.mcp_server` is enough; no need for Docker to understand how the MCP server works.
- **Only running the CLI** (`main.py`) on a schedule (e.g. Azure Automation, Batch, or a VM script): containerizing the CLI is optional; Docker is more useful when you want the **HTTP server** to be always-on and reachable.

### Practical recommendation

- **Use Docker when** you want to run the **MCP server** as a long-lived HTTP service (e.g. in Azure) so that Foundry/agents or other apps can call it. Then: build an image that installs deps and runs `uvicorn agent.mcp_server:app`, and deploy that image.
- **Optional for CLI-only** usage: you can still run `main.py` in a container for portability, but it’s not required for the MCP server’s role.

If you want, we can add a minimal `Dockerfile` and `docker-compose.yml` that:
- Build an image for the MCP server (and optionally run `main.py` as a one-off).
- Use env vars or a mounted `config/` for `sources.yaml` so you don’t bake secrets into the image.

---

## Summary

| Question | Answer |
|----------|--------|
| **What is the MCP server’s role?** | Expose assessment (run, upload, stream, list_tables, load_path) over HTTP so agents, apps, and pipelines can call it instead of the CLI. |
| **How does it work?** | FastAPI app (`mcp_server.py`) receives requests → calls `mcp_interface` → which calls the same engine as `main.py` → returns JSON (or HTML/Markdown for `/upload?format=...`). |
| **Should you use Docker?** | Yes if you deploy the MCP server as a service (e.g. in Azure). Optional for local dev or CLI-only runs. |
