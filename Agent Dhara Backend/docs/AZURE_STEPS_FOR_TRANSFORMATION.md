# Azure Setup for Data Transformation (Cleaned Output)

Steps to configure Azure so the project can read raw data, run assessment + transformation, and write cleaned data back to Blob Storage.

---

## 1. Create a container for cleaned data

In your storage account (same one used for `assessment` and `output`):

1. Go to **Azure Portal** → your Storage Account → **Containers**.
2. Click **+ Container**.
3. Name: `cleaned` (or any name; you’ll use this in `--cleaned-container`).
4. Leave **Public access** as Private.
5. Click **Create**.

---

## 2. Add connection to `config/sources.yaml`

Ensure `sources.yaml` has an `azure_blob_output` location (for uploads). Example:

```yaml
source:
  name: enterprise_data
  locations:
    - type: azure_blob
      connector: azure_blob_storage
      connection:
        account_name: YOUR_STORAGE_ACCOUNT
        account_key: YOUR_ACCOUNT_KEY
        container: assessment

    - type: azure_blob_output
      connector: azure_blob_storage
      connection:
        account_name: YOUR_STORAGE_ACCOUNT
        account_key: YOUR_ACCOUNT_KEY
        container: output

    # Optional: separate container for cleaned data (or reuse output)
    # --cleaned-container uses same connection as azure_blob_output
```

The `azure_blob_output` connection is reused for cleaned uploads; only the container name changes via `--cleaned-container`.

---

## 3. Run the pipeline with transformation

**Local cleaned output only (no Azure):**
```bash
python main.py --sources config/sources.yaml --skip-azure \
  --apply-transformations \
  --output-cleaned-dir output/cleaned
```

**With Azure Blob (read from `assessment`, write cleaned to `cleaned`):**
```bash
python main.py --sources config/sources.yaml \
  --apply-transformations \
  --output-cleaned-dir output/cleaned \
  --cleaned-container cleaned
```

**Full pipeline (assessment + reports + transformation + upload reports + upload cleaned):**
```bash
python main.py --sources config/sources.yaml \
  --apply-transformations \
  --output-cleaned-dir output/cleaned \
  --cleaned-container cleaned \
  --export-json output/assessment.json \
  --export-report output/report.md \
  --export-html output/report.html \
  --export-to-azure
```

---

## 4. (Optional) Automate with Azure Data Factory

1. Create an **Azure Data Factory** instance.
2. Create a **Pipeline** with:
   - **Activity 1:** Run your script (e.g. Azure Batch, Azure Automation Runbook, or Container Instance that runs `python main.py ...`).
   - Input: data in `assessment` container.
   - Output: reports in `output`, cleaned files in `cleaned`.
3. Add a **Schedule trigger** (e.g. daily) to run the pipeline.
4. Configure **Failure alerts** so you get notified if the run fails.

---

## 5. (Optional) Run in Azure Container Apps

1. Build a Docker image for the project (e.g. `docker build -t agent-dhara-backend .`).
2. Push to **Azure Container Registry**.
3. Create an **Azure Container App** that runs your script on a schedule or via an HTTP trigger.

---

## Summary

| Step | Action |
|------|--------|
| 1 | Create `cleaned` container in Storage Account |
| 2 | Set `azure_blob_output` in `sources.yaml` (account + key) |
| 3 | Run `main.py` with `--apply-transformations` and `--cleaned-container cleaned` |
| 4–5 | (Optional) Automate with ADF or Container Apps |

The transformer uses rule-based actions (trim, parse_dates, fill nulls, deduplicate, etc.) from the assessment; it does **not** execute agent-generated code. Cleaned files are written locally and/or uploaded to the `cleaned` container as CSV.
