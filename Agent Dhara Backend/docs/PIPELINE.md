# Pipeline Contract (End-to-End)

Use this contract to wire the Intelligent Data Assessment into Azure Data Factory, Logic Apps, or Azure AI Foundry.

## Inputs

- **Config:** Path to `sources.yaml` (or equivalent JSON) with:
  - `locations[].type`: `database` | `filesystem` | `azure_blob` | `azure_blob_output`
  - For `azure_blob`: connection (account_name, account_key or connection_string, container).
  - For `azure_blob_output`: same, container = where reports are uploaded.

- **Optional:** `--skip-azure` to run without Blob (e.g. local or unit tests).

## Single Run (CLI)

```bash
python main.py \
  --sources config/sources.yaml \
  --export-json output/assessment.json \
  --export-report output/report.md \
  --export-html output/report.html \
  --export-to-azure
```

- Reads from Blob container(s) defined in `sources.yaml` (assessment input).
- Writes JSON/MD/HTML locally (and optionally to `azure_blob_output` container).

## Azure-Only Output

```bash
python main.py \
  --sources config/sources.yaml \
  --export-json output/assessment.json \
  --export-report output/report.md \
  --export-html output/report.html \
  --export-to-azure \
  --azure-only
```

- No local files; uploads only to the configured output container.

## Output Artifacts

| Artifact        | Description                    |
|----------------|--------------------------------|
| `assessment.json` | Full machine-readable assessment (datasets, relationships, DQ issues). |
| `report.md`       | Human-readable Markdown report. |
| `report.html`     | Styled HTML report.            |

Optional (Promptflow): `--export-pf-input`, `--export-pf-eval` for dq_profile and PASS/WARN/FAIL.

## Exit Codes

- `0`: Success.
- Non-zero: Config missing, import error, or critical runtime failure (see stderr).

## Orchestration (ADF / Logic App)

1. **Trigger:** Schedule or Blob-created event (e.g. new file in assessment container).
2. **Activity:** Run `main.py` as above (e.g. Azure Batch, Automation Runbook, or container task). Pass config from Key Vault or pipeline variable.
3. **On success:** Reports are in the output container; optional downstream step (e.g. notify, or call transformation job).
4. **On failure:** Alert and retry (with backoff).

## Transformation (Stage 3)

After a run, use the JSON output with `agent.transformation_suggester.suggest_transformations(result)` or `get_transformation_manifest_for_etl(result)` to drive ETL or an Azure AI Foundry agent that generates SQL/Spark/Dataflow code.
