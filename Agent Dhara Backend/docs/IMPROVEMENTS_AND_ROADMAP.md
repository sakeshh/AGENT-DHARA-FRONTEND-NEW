# Data Accelerators: Improvements & End-to-End Roadmap

This document covers **efficiency and robustness improvements** for your current Intelligent Data Assessment pipeline, and a **roadmap** to complete the full Data Accelerators flow (aligned with the five-stage diagram).

---

## Part 1: Current State & Quick Wins

### What You Have (Steps 1 & 2)

| Stage | Your Implementation |
|-------|----------------------|
| **1. Intelligent Data Assessment** | `intelligent_data_assessment.py`: schema/dtype inference, metadata, relationships, profiling |
| **2. Data Quality & Validation** | Per-column and dataset-level DQ checks, severity, row indexes, global issues (orphans, cross-dataset) |
| **Output** | JSON, Markdown, HTML reports → local + Azure Blob (assessment container → output container) |

### Efficiency & Robustness Improvements

#### 1. **Chunked / streaming processing for large files**

- **Problem:** Loading entire blobs into memory can OOM on large CSV/Parquet.
- **Action:** For files above a threshold (e.g. 100k rows), process in chunks:
  - Use `pd.read_csv(..., chunksize=50000)` or `pd.read_parquet(..., filters=...)` and iterate.
  - Run profiling (null %, unique count, semantic type) in a single pass; aggregate per-chunk stats.
  - DQ checks: run column-level rules per chunk and merge issue lists (dedupe by column + issue type, cap row_indexes at 50 per issue).

#### 2. **Config-driven thresholds**

- **Problem:** Thresholds (e.g. null % for "high" severity, parse rate for "mixed types") are hardcoded.
- **Action:** Move to a small config (YAML/JSON), e.g. `config/dq_thresholds.yaml`:
  - `null_pct_high: 0.25`, `null_pct_medium: 0.10`
  - `mixed_type_low: 0.2`, `mixed_type_high: 0.8`
  - `duplicate_row_warn_pct: 0.05`
- **Benefit:** Same codebase, different environments (staging vs prod) or use cases without code changes.

#### 3. **Structured logging and idempotent runs**

- **Problem:** `print()` makes it hard to automate, debug, and monitor.
- **Action:**
  - Use Python `logging` with a configurable level (INFO in prod, DEBUG in dev). Log dataset name, row counts, issue counts, and timing per stage.
  - Ensure runs are **idempotent**: same input (same blob list + content) → same output; use deterministic ordering (e.g. sort blob names) before processing.

#### 4. **Retries and backoff for Azure Blob**

- **Problem:** Transient network errors can fail the whole run.
- **Action:** For Blob list/download/upload, use retries with exponential backoff (e.g. `tenacity` or a small wrapper). Respect Azure rate limits and avoid slamming the storage account.

#### 5. **Parallelism where safe**

- **Problem:** Multiple blobs are processed one-by-one.
- **Action:** Use a thread pool (e.g. `concurrent.futures.ThreadPoolExecutor`) to:
  - **List** blobs once (no parallelism needed).
  - **Download + parse + profile** each blob in parallel (I/O bound). Keep DQ and relationship steps sequential after all DataFrames are in memory, or run per-dataset DQ in parallel and merge.
- **Caution:** Total memory = sum of all blob sizes; cap parallelism by max concurrent downloads if memory is a constraint.

#### 6. **Blob connector robustness**

- **Current:** `azure_blob_storage.py` has duplicate `upload_file` definitions; one takes `(file_path, dest_blob_name)`, the other `(file_path, blob_name, container)`.
- **Action:** Keep a single, clear API, e.g. `upload_file(local_path, blob_name=None, container=None)` with `container` defaulting to `self.container`. Remove the duplicate and fix call sites (e.g. `main.py` and MCP) to use the same signature.

#### 7. **MCP server and main.py**

- **`run_assessment(config_text)`** does not accept `additional_data`; so when the MCP is used, Azure Blob data must be loaded elsewhere and passed in. Either extend `run_assessment` to accept an optional `additional_data` dict, or document that the MCP flow expects the caller to inject blob-loaded DataFrames.
- **Import in main:** Use `from agent.intelligent_data_assessment import load_and_profile` consistently and avoid the `intelligent_data_assessment_agent` package name in the first try if your repo is not a package.

---

## Part 2: End-to-End Roadmap (Data Accelerators)

The diagram describes five stages. Below is how to complete them in an efficient, Azure-native way.

### Stage 3: Smart Data Transformation

**Goal:** After assessment and DQ, auto-generate or suggest transformations to fix issues (e.g. trim whitespace, fill nulls, normalize dates).

- **Option A – Rule-based (no LLM):**  
  From your DQ report (e.g. `whitespace`, `invalid_date_format`, `nulls`), generate a **transformation manifest** (JSON/YAML) that describes suggested fixes per column. A separate job (e.g. Azure Data Factory mapping data flow, Synapse notebook, or Databricks) reads this manifest and applies:
  - trim, lowercase, replace placeholders with null, date parsing, etc.
- **Option B – Azure AI Foundry Agents:**  
  Use an agent that takes **assessment summary + DQ issues** and outputs:
  - **SQL or Spark snippets** (or Dataflow expressions) for cleansing.
  - Optional: natural language description of fixes for audit.
- **Implementation:**  
  - Add a module `agent/transformation_suggester.py` that, given `load_and_profile` output, produces a structured “suggested_transformations” payload (column, issue_type, suggested_action, optional_snippet).  
  - Optionally call an Azure OpenAI / Foundry agent with a prompt that includes the DQ summary and ask for code or expressions; parse and attach to the payload.

### Stage 4: Adaptive Data Orchestration

**Goal:** Automated pipeline: ingest → assess → (optional) transform → report → upload, with scheduling and failure handling.

- **Azure-native options:**
  1. **Azure Data Factory (ADF) / Synapse Pipelines**  
     - **Copy activity:** Blob (assessment) → optional staging (e.g. Blob raw folder).  
     - **Azure Function or Logic App:** Trigger your assessment (e.g. call `main.py` via HTTP or run in Azure Automation / Container Instance).  
     - **Copy activity:** Output (JSON/MD/HTML) from run → Blob (output container).  
     - Schedule trigger (e.g. daily) and failure alerts.
  2. **Azure AI Foundry Agents**  
     - Use an **orchestrator agent** that:  
       - Lists blobs (or receives events),  
       - Calls your assessment service (MCP or HTTP),  
       - Reads the report and decides “run transformation” or “notify,”  
       - Invokes transformation job or sends alerts.  
  3. **Hybrid**  
     - ADF runs the assessment (e.g. Azure Batch, Function, or VM script).  
     - Output is written to Blob; an AI Foundry agent subscribes to “new report” (e.g. Event Grid on Blob created) and does smart routing (e.g. “high severity → send to team,” “low → log only”).

- **Implementation:**  
  - Define a small **pipeline contract**: input container + prefix, output container + prefix, and a single entrypoint (e.g. `python main.py --sources config/sources.yaml --export-json out/assessment.json --export-report out/report.md --export-html out/report.html --export-to-azure`).  
  - Document this in `docs/PIPELINE.md` and provide an ADF template (JSON) or a Logic App that invokes it.

### Stage 5: Data Monitoring & Optimization

**Goal:** Real-time or periodic health, alerts, and cost/latency insights.

- **Metrics to expose:**
  - Per run: dataset count, total rows, issue count by severity, run duration.
  - Over time: trend of issue count, new datasets, failed runs.

- **Implementation:**
  1. **Application Insights (or Log Analytics):**  
     - In `main.py` (and MCP), log structured events: `assessment_completed`, `dataset_count`, `total_issues`, `duration_seconds`, `blob_list`.  
     - Create a dashboard and alerts (e.g. “high_severity_issues > 10” or “run_duration > 1 hour”).
  2. **Blob metadata / index tags:**  
     - When uploading reports, set blob metadata or tags (e.g. `assessment_date`, `dataset_count`, `overall_status`). This allows querying “last run” or “runs with failures” without re-running assessment.
  3. **Azure AI Foundry:**  
     - An agent that periodically reads the latest report (from Blob or from your API), summarizes trends, and sends a weekly digest or triggers remediation workflows.

---

## Part 3: Suggested Implementation Order

| Phase | Focus | Deliverables |
|-------|--------|--------------|
| **Phase 1 (now)** | Efficiency & robustness | Config thresholds, logging, retries, single `upload_file` API, optional chunked reads for large files |
| **Phase 2** | Transformation | `transformation_suggester.py` + optional Foundry agent for SQL/Spark suggestions |
| **Phase 3** | Orchestration | Pipeline contract, ADF/Logic App or Foundry orchestrator, scheduled runs |
| **Phase 4** | Monitoring | Application Insights (or Log Analytics), blob metadata, dashboards and alerts |

---

## Part 4: Azure AI Foundry Agents – Where They Fit

- **Assessment (current):** Your Python engine stays the source of truth; the agent can **invoke** it (via MCP or HTTP) and pass config.
- **Transformation (Stage 3):** Agent takes DQ report → suggests or generates ETL/cleansing code (SQL, Spark, Dataflow).
- **Orchestration (Stage 4):** Agent decides “run assessment,” “run transformation,” “notify,” or “retry” based on outputs and policies.
- **Monitoring (Stage 5):** Agent consumes reports and metrics, produces summaries and triggers alerts or tickets.

Using Foundry does not replace your core assessment logic; it adds an intelligent layer on top for transformation generation, orchestration, and monitoring.

---

## Summary

- **Short term:** Improve efficiency (chunking, parallelism, config, logging, retries) and robustness (single Blob API, idempotent runs).  
- **Mid term:** Add transformation suggestions (rule-based + optional Foundry) and an orchestration layer (ADF/Logic App or Foundry).  
- **Long term:** Add monitoring (Application Insights, blob metadata, dashboards) and optional Foundry agents for summaries and remediation.

This keeps your existing assessment and DQ as the solid foundation (steps 1 & 2) and extends them into a full Data Accelerators-style pipeline in an efficient and Azure-native way.
