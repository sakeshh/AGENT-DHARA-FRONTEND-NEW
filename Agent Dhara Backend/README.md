# Intelligent Data Assessment & Data Accelerators

Analyse data anomalies from **Azure Blob** (and optionally Azure SQL / filesystem), run **data quality checks**, and produce reports in **JSON**, **Markdown**, and **HTML**. Upload reports back to Azure Blob. This is the first two stages of the **Data Accelerators** flow (Intelligent Data Assessment + Data Quality & Validation).

## Quick start

```bash
# Install dependencies (see requirements below)
pip install pandas pyyaml openpyxl azure-storage-blob

# Run with config — on an interactive terminal you get a menu (SQL / Blob / Local / Stream / All).
# Or fix scope: --evaluate sql|blob|local|all  (auto = menu if TTY, else all)
python main.py --sources config/sources.yaml \
  --export-json output/assessment.json \
  --export-report output/report.md \
  --export-html output/report.html \
  --export-to-azure
```

### Azure (portable setup, no secrets in repo)

- Put your secrets in environment variables (see `.env.example` for the full list).
- Use the provided template config `config/sources_azure_env.yaml` which reads `${VARS}`.

Example:

```bash
python main.py --sources config/sources_azure_env.yaml --reports-dir output/reports
```

### Run locally (sample CSV only, no Azure/SQL)

Uses `sample_data/` and skips blob/DB:

```bash
python main.py --sources config/sources_local_run.yaml --skip-azure --reports-dir output/reports
```

Outputs: `output/reports/report.*`.

Each run adds **`run_metadata`** (duration, DQ rollup) and **`transformation_suggestions`** to JSON/Markdown; the HTML report includes a **Suggested fixes** section.

**HTML report theme:** Default is **Theme 2** (executive / enterprise layout).

### Optional: AI narrative (Azure OpenAI)

After the rule-based assessment, add an **executive summary, top risks, and next steps** (aggregated metadata only — no raw cell values):

```bash
pip install openai
set AZURE_OPENAI_ENDPOINT=https://YOUR_RESOURCE.openai.azure.com/
set AZURE_OPENAI_API_KEY=your_key
set AZURE_OPENAI_DEPLOYMENT=gpt-4o-mini
python main.py --sources config/sources_local_run.yaml --skip-azure --reports-dir output/reports --llm-insights
```

JSON/Markdown/HTML include `llm_insights` and an **AI insights** section. Verify all claims against the deterministic DQ tables. `--generate-transformation-rules` remains separate (code/SQL generation).

## Project layout

- **mcp_server/** – Four separate stdio MCPs (SQL, Blob, Local, Stream) + unified server; see [docs/MCP_ARCHITECTURE.md](docs/MCP_ARCHITECTURE.md)  
- **mcp_runtime/** – In-process MCP bridge for same-repo services (Foundry backend / LangGraph)  
- **agent/** – Core assessment engine and MCP interface  
  - `intelligent_data_assessment.py` – Profiling, DQ checks, relationships  
  - `mcp_interface.py` – MCP entry points (run_assessment, list_tables, stream, upload)  
  - `mcp_server.py` – FastAPI server for MCP/HTTP  
  - `transformation_suggester.py` – Stage 3: suggested transformations from DQ report  
  - `llm_assessment_enhancer.py` – Optional Azure OpenAI narrative on assessment metadata (`--llm-insights`)  
- **connectors/** – Azure SQL (pythonnet), Azure Blob Storage  
- **config/** – sources.yaml, dq_thresholds.yaml (optional)  
- **docs/** – Roadmap, pipeline contract, code improvements, Azure AI Foundry integration  

## Documentation

| Doc | Purpose |
|-----|--------|
| [docs/IMPROVEMENTS_AND_ROADMAP.md](docs/IMPROVEMENTS_AND_ROADMAP.md) | Efficiency & robustness improvements + full Data Accelerators roadmap (Stages 3–5) |
| [docs/CODE_IMPROVEMENTS.md](docs/CODE_IMPROVEMENTS.md) | Exact code changes (Blob API, logging, thresholds, retries, MCP) |
| [docs/PIPELINE.md](docs/PIPELINE.md) | Pipeline contract for ADF / Logic App / Foundry orchestration |
| [docs/AZURE_AI_FOUNDRY_INTEGRATION.md](docs/AZURE_AI_FOUNDRY_INTEGRATION.md) | Using Azure AI Foundry Agents for transformation, orchestration, monitoring |

## End-to-end roadmap (Data Accelerators)

1. **Intelligent Data Assessment** ✅ – Schema, metadata, relationships (current).  
2. **Data Quality & Validation** ✅ – Anomalies, rules, row-level issues (current).  
3. **Smart Data Transformation** – Suggested fixes; optional agent-generated SQL/Spark (see `agent/transformation_suggester.py` and docs).  
4. **Adaptive Data Orchestration** – ADF / Logic App / Foundry to run assessment and transformation on a schedule or event.  
5. **Data Monitoring & Optimization** – Dashboards, alerts, and agent-driven summaries (Application Insights, blob metadata).  

## Configuration

- **sources.yaml** – Data sources: `database`, `filesystem`, `azure_blob`, `azure_blob_output`.  
- **Multiple sources:** Add several blocks under `locations`. **Several `azure_blob` entries** → all containers are loaded; dataset names become `{id|label|container}__<blob_path>` when there is more than one container (single container keeps plain blob paths). **Several `database` entries** → all servers are loaded; with multiple DBs, table keys are prefixed with optional `id` / `label` / `name` on each block, otherwise `{database}_{hash}__schema.table` so identical table names never overwrite each other. **Several `filesystem` paths** → already merged with disambiguated filenames.
- **config/dq_thresholds.yaml** (optional) – Severity thresholds, chunking, parallelism, and **`extended_checks`** (extra rules below). Set `extended_checks.disabled: true` to run only the baseline column checks.

### Data quality checks (report generation)

**Per column (baseline):** nulls/placeholders, leading/trailing whitespace, invalid email/phone, bad dates, invalid numeric, negatives, zeros in ID-like columns, mixed numeric/text, nested list/dict values.

**Dataset-level:** duplicate rows, duplicate primary-key candidates, potential PK hints.

**Extended checks** (configurable in `dq_thresholds.yaml`): empty dataset; duplicate or case-colliding column names; very wide tables; column names with spaces; **start/end date pairs** and `*_start`/`*_end` ordering violations; constant columns; dominant single value (skew); very high cardinality; binary-like columns; **IQR numeric outliers** and heavy skew; integer values stored as float; future / pre-1900 / very wide date spans; extremely long strings; empty strings; control characters in text.

**Cross-dataset:** orphan FK overlap hints, mixed-type consistency. **Custom rules** in YAML (`one_of`, `range`, `regex`, `not_null`).

**Table relationships:** For each pair of datasets that share a column name, the report infers **1:1**, **1:N**, **N:1**, or **M:N** from how often each shared key appears in each table. **Orphan key rows** list the child dataset, column, **row indexes**, bad values, and fix recommendations when a likely parent table is detected (unique/PK-like column vs repeating FK).

No engine covers *every* conceivable DQ rule (e.g. arbitrary business rules, ML drift); extend via `custom_rules` or downstream tools.

## Security

Do not commit secrets. Use environment variables or Azure Key Vault for `account_key`, `password`, and API keys. Use managed identity where possible for Blob and SQL.

## LangGraph-based multi-agent orchestration (experimental)

This repo includes an **experimental** LangGraph orchestrator that coordinates sub-agents (starting with extraction) across the data sources defined in `config/sources.yaml`.

### Flow (matches project plan)

- **User (UI)** selects: `Azure SQL` / `Blob` / `Local` / `Stream`
- **Supervisor/Master Agent** routes and passes selections to
- **Extract Agent** (single) which fans out to:
  - **Azure SQL MCP**
  - **Blob MCP**
  - **Local FS MCP**
  - **Stream MCP**

### Install

```bash
pip install -r requirements.txt
```

### Run from Python

Example (run extraction across all configured locations, and optionally run transformation-rule generation if requested):

```python
from agent.langgraph_orchestrator import run_orchestrator

# Select sources by id/label/name in sources.yaml, or by type ("database", "azure_blob", "filesystem").
result = run_orchestrator(
    user_request="Extract data and generate transformation rules",
    sources_path="config/sources.yaml",
    selected_sources=["database", "azure_blob"],  # or omit to run all
)

print(result.keys())  # plan, extractions, extraction_errors, transformation, ...
```

Stream example (send records directly):

```python
from agent.langgraph_orchestrator import run_orchestrator

result = run_orchestrator(
    user_request="Extract stream only",
    sources_path="config/sources.yaml",
    selected_sources=[],  # optional; stream is separate
    stream_records=[{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],
    stream_name="my_stream",
)
```

### How source selection works

- **Preferred**: set `id:` on each location in `config/sources.yaml`, then pass `selected_sources=["primary", "assessment_input"]`
- **By type**: pass `selected_sources=["database"]` (or `azure_blob`, `filesystem`, etc.)
- **Fallback**: pass `selected_sources=["database:0"]` to select the first database location
