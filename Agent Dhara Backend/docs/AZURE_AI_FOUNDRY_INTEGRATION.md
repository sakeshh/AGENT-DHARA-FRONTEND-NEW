# Azure AI Foundry Integration

How to use **Azure AI Foundry Agents** with your Intelligent Data Assessment pipeline (Steps 1–2) and the next stages (Transformation, Orchestration, Monitoring).

---

## Roles of the Agent

| Stage | Agent role |
|-------|------------|
| **Assessment (current)** | Invoke your assessment (MCP or HTTP), pass config, optionally pass pre-loaded blob data. |
| **Transformation (Stage 3)** | Consume assessment + DQ report → suggest or generate ETL/cleansing code (SQL, Spark, Dataflow). |
| **Orchestration (Stage 4)** | Decide when to run assessment, when to run transformation, when to notify; trigger jobs. |
| **Monitoring (Stage 5)** | Read reports and metrics → summarize trends, send digests, trigger remediation. |

---

## Invoking Assessment from an Agent

### Option A: MCP server

- Run your MCP server (e.g. `python -m agent.mcp_server`) and expose it (e.g. on a VM or App Service with auth).
- In Azure AI Foundry, configure an MCP tool that points to your server.
- Agent prompts can say: “Run data assessment with this config” and pass the contents of `sources.yaml` (or a subset). The tool calls `run_assessment(config_text)` and returns the JSON result.
- If the agent has access to blob storage separately, it can load blobs, build `additional_data`, and call `run_assessment(config_text, additional_data=...)` if you add that parameter to the MCP interface.

### Option B: HTTP API

- Your FastAPI app already exposes `/run`, `/upload`, `/stream`, etc. Deploy it (e.g. Azure Container Apps or App Service).
- The agent uses a “Call HTTP API” tool: POST to `/run` with `{"config": "<yaml or json string>"}`. For file-based assessment, use `/upload` with the file and optional `?format=html` or `?format=md` to get a report in the response.

---

## Transformation (Stage 3): Agent-Generated ETL

1. **Input to the agent:**  
   - Assessment JSON (or a summary): dataset list, column profiles, and the `data_quality_issues` section (per-dataset issues + global issues).  
   - Optionally the output of `transformation_suggester.suggest_transformations(result)` so the agent sees suggested actions (trim, parse_dates, etc.).

2. **Prompt shape:**  
   - “Given this data quality report and suggested actions, generate [SQL / Spark / Dataflow expression] to cleanse the data. Prefer one statement or expression per column that has issues.”

3. **Output:**  
   - The agent returns code or expressions; your app (or a downstream job) parses and stores them (e.g. in a repo or as ADF/Synapse artifacts). You can also use the Foundry agent’s built-in code execution only in a sandbox; for production ETL, treat the output as “suggested code” and run it in your pipeline after review.

4. **Implementation:**  
   - Add an MCP tool or HTTP endpoint, e.g. `suggest_transformations_from_report(report_json: dict)` that calls `transformation_suggester.suggest_transformations(report_json)` and returns the manifest.  
   - Optionally a second tool `generate_cleansing_code(report_json, language="sql")` that sends the report + language to an Azure OpenAI/Foundry agent and returns the generated snippet.

---

## Orchestration (Stage 4): Agent as Controller

- The agent has tools (or logic) to:  
  - **List blobs** in the assessment container (or get “last run” from metadata).  
  - **Trigger assessment** (call your MCP `/run` or HTTP `/run`).  
  - **Read the report** (e.g. from Blob or from the API response).  
  - **Decide next step:**  
    - If high severity issues > N → “Notify data steward” or “Create ticket.”  
    - If suggested_transformations exist → “Trigger transformation job” (e.g. call ADF pipeline or Logic App).  
    - If no critical issues → “Log and finish.”

- Implementation:  
  - In Foundry, define a workflow or agent that has these tools and a high-level prompt: “You are the data pipeline controller. When assessment completes, review the report and decide: notify, run transformation, or finish.”  
  - The “trigger transformation” tool can be an HTTP call to ADF REST API or Logic App to start the ETL run that uses the transformation manifest.

---

## Monitoring (Stage 5): Agent as Reviewer

- **Input:**  
  - Latest assessment report (from Blob or API).  
  - Optional: historical summaries (e.g. from Application Insights or a small store of past run summaries).

- **Prompt:**  
  - “Summarize data quality trends this week: which datasets improved or worsened, and what are the top 3 recommended actions?”

- **Output:**  
  - Natural language summary or a short list of actions; optionally post to Teams/email or create work items.

- **Implementation:**  
  - Scheduled job (e.g. weekly) that: (1) fetches latest report from Blob, (2) calls an Azure OpenAI/Foundry agent with the report and the prompt above, (3) sends the agent’s reply to a channel or ticketing system.

---

## Security and Config

- Store Azure credentials (Blob, OpenAI/Foundry) in **Azure Key Vault**; have the app and the agent runtime read from Key Vault (managed identity or client secret).  
- Do not put secrets in `sources.yaml` in source control; use env vars or Key Vault references (e.g. `account_key: $(keyvault-ref)` in ADF).  
- For MCP/HTTP, use HTTPS and authentication (e.g. API key or AAD) so only authorized callers (e.g. Foundry) can trigger assessment or transformation.

---

## Summary

- **Assessment:** Expose via MCP or HTTP; agent calls it with config (and optional blob data).  
- **Transformation:** Agent takes report + optional transformation manifest → generates SQL/Spark/Dataflow; you run that in your ETL pipeline.  
- **Orchestration:** Agent triggers assessment, reads report, and decides notify / transform / finish; “transform” triggers ADF or Logic App.  
- **Monitoring:** Agent consumes latest (and optionally historical) reports and produces summaries and recommendations; schedule and send to Teams/email or tickets.

This keeps your Python assessment engine as the source of truth and uses Azure AI Foundry for intelligent orchestration, code generation, and monitoring.
