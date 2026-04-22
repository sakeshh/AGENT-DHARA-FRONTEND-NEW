# Step 3 Production: Large-Scale ETL with Azure Execution

**Detailed step-by-step guide** to move transformation execution into Azure, using this project to generate the rules/manifest. Follow each section in order.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites — Detailed Setup](#2-prerequisites--detailed-setup)
3. [Phase 1: Generate Rules/Manifest](#3-phase-1-generate-rulesmanifest)
4. [Phase 2: Publish Manifest to Azure Blob](#4-phase-2-publish-manifest-to-azure-blob)
5. [Phase 3: Set Up Azure ETL (ADF)](#5-phase-3-set-up-azure-etl-adf)
6. [Phase 4: End-to-End Orchestration](#6-phase-4-end-to-end-orchestration)
7. [Troubleshooting](#7-troubleshooting)
8. [Governance and Production Checklist](#8-governance-and-production-checklist)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    YOUR PROJECT (Rules/Manifest Generator)                    │
│  • Load & profile data from Blob/SQL/filesystem                              │
│  • Run DQ checks and assessment                                              │
│  • AI agent generates transformation rules                                   │
│  • Output: transformation_manifest.json + assessment report                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AZURE BLOB STORAGE                                     │
│  • Container: manifests/  →  transformation_manifest.json                    │
│  • Container: output/     →  assessment report, DQ summary                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    AZURE ETL (Execution at Scale)                             │
│  • Azure Data Factory reads manifest                                         │
│  • Mapping Data Flow applies transformations                                 │
│  • Output: cleaned data to Blob/SQL                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Component | Responsibility |
|-----------|----------------|
| **This project** | Generate transformation rules and manifest |
| **Azure ETL** | Execute transformations at scale in Azure |

---

## 2. Prerequisites — Detailed Setup

Complete these before starting Phase 1.

---

### 2.1 Create Azure Storage Account (If You Don't Have One)

1. **Sign in to Azure Portal**  
   Go to: https://portal.azure.com

2. **Create a Storage Account**
   - Click **+ Create a resource**
   - Search for **Storage account**
   - Click **Create**

3. **Basics tab**
   - **Subscription:** Select your subscription
   - **Resource group:** Create new (e.g. `rg-agentdhara-etl`) or select existing
   - **Storage account name:** e.g. `agentdharaetlprod` (must be globally unique, 3–24 chars, lowercase letters and numbers only)
   - **Region:** Choose your region (e.g. East US)
   - **Performance:** Standard
   - **Redundancy:** LRS (or GRS for production)
   - Click **Review** → **Create**

4. **Wait for deployment**  
   Click **Go to resource** when done.

5. **Get connection details**
   - In the left menu: **Security + networking** → **Access keys**
   - Click **Show** next to **key1**
   - Copy **Connection string** (you will need this later)
   - Or copy **Storage account name** and **key1** separately

---

### 2.2 Create Blob Containers

1. In your Storage Account, go to **Data storage** → **Containers**

2. **Create container: `raw`** (for input data)
   - Click **+ Container**
   - Name: `raw`
   - Public access level: **Private (no anonymous access)**
   - Click **Create**

3. **Create container: `output`** (for assessment reports and manifests)
   - Click **+ Container**
   - Name: `output`
   - Public access level: **Private**
   - Click **Create**

4. **Create container: `manifests`** (for transformation manifest only)
   - Click **+ Container**
   - Name: `manifests`
   - Public access level: **Private**
   - Click **Create**

5. **Create container: `cleaned`** (for ETL output)
   - Click **+ Container**
   - Name: `cleaned`
   - Public access level: **Private**
   - Click **Create**

**✓ Checkpoint:** You should have 4 containers: `raw`, `output`, `manifests`, `cleaned`.

---

### 2.3 Upload Sample Data to `raw` (If Not Already Done)

1. Go to **Containers** → **raw** → **Upload**

2. Upload at least one CSV or JSON file (e.g. `sales.csv` or `data/customers.json`)

3. Note the **exact blob path** (e.g. `sales.csv` or `data/customers.json`) — you will need it for the manifest

---

### 2.4 Create Azure Data Factory (If You Don't Have One)

1. In Azure Portal: **+ Create a resource** → search **Data Factory**

2. Click **Create**

3. **Basics:**
   - **Subscription:** Your subscription
   - **Resource group:** Same as storage (e.g. `rg-agentdhara-etl`)
   - **Region:** Same as storage
   - **Name:** e.g. `adf-agentdhara-etl`
   - **Version:** V2
   - Click **Next** → **Next** → **Review + create** → **Create**

4. **Go to resource** when deployment completes

---

### 2.5 Configure Your Project

1. **Open your project folder** in a terminal (e.g. `c:\path\to\agent_dhara_backend`)

2. **Create `config/sources.yaml`** if it does not exist. Replace placeholders with your values:

```yaml
source:
  name: enterprise_data
  locations:
    - type: azure_blob
      connector: azure_blob_storage
      connection:
        account_name: YOUR_STORAGE_ACCOUNT_NAME
        account_key: YOUR_STORAGE_ACCOUNT_KEY
        container: raw

    - type: azure_blob_output
      connector: azure_blob_storage
      connection:
        account_name: YOUR_STORAGE_ACCOUNT_NAME
        account_key: YOUR_STORAGE_ACCOUNT_KEY
        container: output
```

**Replace:**
- `YOUR_STORAGE_ACCOUNT_NAME` → e.g. `agentdharaetlprod`
- `YOUR_STORAGE_ACCOUNT_KEY` → paste from Azure Portal (Storage Account → Access keys → key1)

3. **Create the `output` folder** in your project:
   ```
   mkdir output
   ```

4. **Install dependencies** (if not already done):
   ```bash
   pip install -r requirements.txt
   pip install pyyaml openai  # if using AI rules
   ```

5. **Optional — For AI-generated rules:** Set environment variables:
   ```bash
   set AZURE_OPENAI_ENDPOINT=https://YOUR_RESOURCE.openai.azure.com/
   set AZURE_OPENAI_API_KEY=your_key
   set AZURE_OPENAI_DEPLOYMENT=gpt-4
   ```
   (Use `export` instead of `set` on Linux/Mac.)

**✓ Checkpoint:** You can list blobs with:
```bash
python -c "
import yaml
from connectors.azure_blob_storage import AzureBlobStorageConnector
cfg = yaml.safe_load(open('config/sources.yaml'))
conn_cfg = next(loc['connection'] for loc in cfg['source']['locations'] if loc.get('type')=='azure_blob')
conn = AzureBlobStorageConnector(conn_cfg)
for b in conn.list_blobs():
    print(b)
"
```
You should see your uploaded file(s).

---

## 3. Phase 1: Generate Rules/Manifest

Follow each sub-step exactly.

---

### Step 3.1: Run Assessment and Generate Transformation Rules

1. **Open a terminal** in your project root.

2. **Run this command** (all one block; adjust paths if needed):

```bash
python main.py --sources config/sources.yaml --dq-thresholds config/dq_thresholds.yaml --generate-transformation-rules --output-pending-approval output/pending_approval.json --export-transformation-rules output/transformation_rules.json --apply-transformations
```

**If you use `--skip-azure`** (no Azure Blob), add filesystem config to `sources.yaml` or use `--stream-file` with a local JSON file.

3. **What you should see:**
   - Log messages about loading data, profiling, DQ checks
   - A line like: `Wrote pending approval to output/pending_approval.json`
   - Final JSON output in the terminal

4. **Verify files were created:**
   ```bash
   dir output
   ```
   You should see:
   - `pending_approval.json`
   - `transformation_rules.json`

**✓ Checkpoint:** Open `output/pending_approval.json` in a text editor. It should contain a `suggested_transforms` array. If it is empty, your data may have no DQ issues, or the assessment may not have loaded data — check logs.

---

### Step 3.2: Review and Approve Transforms

1. **Open** `output/pending_approval.json` in a text editor (VS Code, Notepad++, etc.)

2. **Find the `suggested_transforms` array.** Each item looks like:
   ```json
   {
     "dataset": "sales.csv",
     "column": "order_date",
     "issue_type": "invalid_date_format",
     "action": "parse_dates",
     "message": "..."
   }
   ```

3. **Decide which transforms to keep:**
   - **Keep:** Leave the item in the array
   - **Remove:** Delete the entire `{ ... }` block (including the comma before/after)

4. **Save the file as** `output/approved_transforms.json`
   - Either: File → Save As → `approved_transforms.json`
   - Or: Copy the entire file, paste into a new file named `approved_transforms.json`

5. **Ensure the JSON is valid:**
   - No trailing comma after the last item in `suggested_transforms`
   - All brackets and braces match

**✓ Checkpoint:** Your `approved_transforms.json` should look like:
```json
{
  "suggested_transforms": [
    {
      "dataset": "sales.csv",
      "column": "order_date",
      "issue_type": "invalid_date_format",
      "action": "parse_dates",
      "message": "Fix date parsing"
    }
  ],
  "instructions": "...",
  "source": "ai"
}
```

---

### Step 3.3: Create the ETL Manifest

The manifest is the file Azure ETL will read. Convert approved transforms into manifest format.

1. **Create a Python script** `scripts/build_manifest.py`:

```python
"""
Build transformation_manifest.json from approved_transforms.json for Azure ETL.
Run from project root: python scripts/build_manifest.py
"""
import json
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
APPROVED_PATH = PROJECT_ROOT / "output" / "approved_transforms.json"
MANIFEST_PATH = PROJECT_ROOT / "output" / "transformation_manifest.json"

def main():
    with open(APPROVED_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    items = data.get("suggested_transforms", [])
    datasets = {}
    by_action = {}

    for item in items:
        ds = item.get("dataset")
        col = item.get("column")
        action = (item.get("action") or item.get("suggested_action") or "").strip().lower()
        if not ds or ds == "global":
            continue
        if ds not in datasets:
            datasets[ds] = []
        datasets[ds].append({
            "column": col,
            "suggested_action": action,
            "issue_type": item.get("issue_type", "ai_suggested"),
        })
        by_action[action] = by_action.get(action, 0) + 1

    manifest = {
        "version": "1.0",
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "datasets": datasets,
        "summary": {
            "total_suggestions": len(items),
            "by_action": by_action,
        },
    }

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"Wrote manifest to {MANIFEST_PATH}")
    print(f"  Datasets: {list(datasets.keys())}")
    print(f"  Total rules: {len(items)}")

if __name__ == "__main__":
    main()
```

2. **Run the script:**
   ```bash
   python scripts/build_manifest.py
   ```

3. **Verify:**
   - `output/transformation_manifest.json` exists
   - It contains `"datasets": { "your_dataset_name": [ ... ] }`

**✓ Checkpoint:** Open `output/transformation_manifest.json`. It should look like:
```json
{
  "version": "1.0",
  "generated_at": "2025-03-10T12:00:00Z",
  "datasets": {
    "sales.csv": [
      {
        "column": "order_date",
        "suggested_action": "parse_dates",
        "issue_type": "invalid_date_format"
      }
    ]
  },
  "summary": {
    "total_suggestions": 1,
    "by_action": { "parse_dates": 1 }
  }
}
```

---

## 4. Phase 2: Publish Manifest to Azure Blob

---

### Step 4.1: Get Your Storage Connection Details

1. Azure Portal → Your Storage Account
2. **Security + networking** → **Access keys**
3. Copy **Storage account name** and **key1** (or Connection string)

---

### Step 4.2: Upload Using Azure CLI (Recommended)

1. **Install Azure CLI** if needed: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli

2. **Sign in:**
   ```bash
   az login
   ```
   A browser window opens; sign in with your Azure account.

3. **Upload the manifest** (replace placeholders):

   ```bash
   az storage blob upload --account-name YOUR_STORAGE_ACCOUNT --account-key YOUR_KEY --container-name manifests --name transformation_manifest.json --file output/transformation_manifest.json
   ```

   **Replace:**
   - `YOUR_STORAGE_ACCOUNT` → e.g. `agentdharaetlprod`
   - `YOUR_KEY` → paste key1 from Azure Portal

4. **Verify upload:**
   - Azure Portal → Storage Account → Containers → **manifests**
   - You should see `transformation_manifest.json`
   - Click it → **Edit** (or Download) to confirm content

**✓ Checkpoint:** The file `manifests/transformation_manifest.json` exists in Blob storage and its content matches your local file.

---

### Step 4.3: Alternative — Upload Using Python

If you prefer not to use Azure CLI:

1. Create `scripts/upload_manifest.py`:

```python
"""
Upload transformation_manifest.json to Azure Blob.
Run from project root: python scripts/upload_manifest.py
"""
import yaml
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = PROJECT_ROOT / "output" / "transformation_manifest.json"

def main():
    with open(PROJECT_ROOT / "config" / "sources.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    conn_cfg = None
    for loc in cfg.get("source", {}).get("locations", []):
        if (loc.get("type") or "").lower() == "azure_blob_output":
            conn_cfg = loc.get("connection", {})
            break
    if not conn_cfg:
        raise SystemExit("No azure_blob_output in sources.yaml")

    from connectors.azure_blob_storage import AzureBlobStorageConnector
    conn = AzureBlobStorageConnector(conn_cfg)

    with open(MANIFEST_PATH, "rb") as f:
        data = f.read()

    conn.upload_blob("transformation_manifest.json", data, container="manifests")
    print(f"Uploaded {MANIFEST_PATH} to container 'manifests'")

if __name__ == "__main__":
    main()
```

2. **Run:**
   ```bash
   python scripts/upload_manifest.py
   ```

**Note:** The script uses the `azure_blob` or `azure_blob_output` connection from `sources.yaml` and uploads to the `manifests` container. Ensure the storage account has a `manifests` container (create it in Phase 2).

---

## 5. Phase 3: Set Up Azure ETL (ADF)

This section configures Azure Data Factory to read the manifest and apply transformations.

---

### Step 5.1: Link Storage to Data Factory

1. **Open Azure Data Factory** in the portal  
   (e.g. `adf-agentdhara-etl`)

2. Click **Open Azure Data Factory Studio** (or **Author & Monitor**)

3. Go to **Manage** (wrench icon) → **Linked services**

4. Click **+ New**

5. Search for **Azure Blob Storage** → Select it → Continue

6. **Configure:**
   - **Name:** `ls_blob_storage`
   - **Connect via integration runtime:** AutoResolveIntegrationRuntime
   - **Authentication method:** Account key
   - **Account selection method:** From Azure subscription
   - **Azure subscription:** Your subscription
   - **Storage account name:** Select your storage account (e.g. `agentdharaetlprod`)
   - Click **Test connection** → should succeed
   - Click **Create**

**✓ Checkpoint:** `ls_blob_storage` appears in the linked services list.

---

### Step 5.2: Create Dataset for Raw Data

1. **Author** → **Datasets** → **+ New dataset**

2. Select **Azure Blob Storage** → Continue

3. **Format:** DelimitedText (for CSV) or Json — choose based on your data

4. **Name:** `ds_raw_blob`

5. **Linked service:** `ls_blob_storage`

6. **File path:**  
   - Container: `raw`  
   - Directory: leave blank or use folder path (e.g. `data`)  
   - File: leave blank (to read all files) or specify `sales.csv`

7. **First row as header:** Yes (for CSV)

8. Click **OK**

---

### Step 5.3: Create Dataset for Manifest

1. **Author** → **Datasets** → **+ New dataset**

2. Select **Azure Blob Storage** → Continue

3. **Format:** Json

4. **Name:** `ds_manifest`

5. **Linked service:** `ls_blob_storage`

6. **File path:**
   - Container: `manifests`
   - File: `transformation_manifest.json`

7. Click **OK**

---

### Step 5.4: Create Dataset for Cleaned Output

1. **Author** → **Datasets** → **+ New dataset**

2. Select **Azure Blob Storage** → Continue

3. **Format:** Same as raw (e.g. DelimitedText for CSV)

4. **Name:** `ds_cleaned_blob`

5. **Linked service:** `ls_blob_storage`

6. **File path:**
   - Container: `cleaned`
   - Directory: (e.g. `output`) or leave blank
   - File: use dynamic content, e.g. `@concat(pipeline().RunId, '.csv')` or a fixed name

7. Click **OK**

---

### Step 5.5: Create a Lookup Activity for the Manifest

1. **Author** → **Pipelines** → **+ New pipeline**

2. Name the pipeline: `pl_transform_from_manifest`

3. In the **Activities** pane, search for **Lookup**

4. Drag **Lookup** onto the canvas

5. Click the Lookup activity → **Settings** tab:
   - **Source dataset:** `ds_manifest`
   - **First row only:** Check this (we need one JSON object)

6. Rename the activity to `LookupManifest`

**✓ Checkpoint:** The Lookup reads `manifests/transformation_manifest.json` and outputs its JSON.

---

### Step 5.6: Create a Data Flow for Transformations

1. In the Activities pane, search for **Data flow**

2. Drag **Data flow** onto the canvas, below the Lookup

3. **Connect** Lookup → Data flow (drag the green success output of Lookup to the Data flow)

4. Click the Data flow activity → **Settings** tab:
   - Click **New** next to Data flow
   - Name: `df_apply_manifest`
   - Click **Create**

5. You are now inside the Data flow canvas. Add:

   **a) Source**
   - Drag **Source** from the left
   - Output stream name: `RawData`
   - Dataset: `ds_raw_blob`
   - Click away

   **b) Derived column (for transformations)**
   - Drag **Derived column** and connect it after Source
   - In **Column** settings, add derived columns based on your manifest actions:
     - For `trim`: New column or update: `columnName` = `trim(columnName)` — replace `columnName` with your actual column name
     - For `parse_dates`: `order_date` = `toTimestamp(order_date, 'yyyy-MM-dd')` — adjust format
     - For `coerce_numeric`: `amount` = `toDouble(amount)`

   **Note:** Mapping Data Flow does not read the manifest dynamically in a simple way. For a **fixed schema**, you define these expressions manually. For **dynamic** behavior, you would use a Synapse Notebook or custom activity that reads the manifest and builds the transformations. For this guide, we use a **simplified fixed mapping** — you manually add derived columns for each column in your manifest.

   **Example:** If your manifest says `sales.csv` → `order_date` → `parse_dates`:
   - In Derived column: Add/modify column `order_date`
   - Expression: `toTimestamp(order_date, 'yyyy-MM-dd')` (or the format your data uses)

   **c) Sink**
   - Drag **Sink** after Derived column
   - Dataset: `ds_cleaned_blob`
   - Click away

6. Click **Back** (arrow) to return to the pipeline canvas

---

### Step 5.7: Map Manifest Actions to Data Flow Expressions

Use this table when adding Derived column expressions:

| Manifest `suggested_action` | Data Flow expression (replace `col` with column name) |
|----------------------------|------------------------------------------------------|
| `trim` | `trim(col)` |
| `parse_dates` | `toTimestamp(col, 'yyyy-MM-dd')` or `toDate(col, 'yyyy-MM-dd')` |
| `coerce_numeric` | `toDouble(col)` |
| `fill_or_drop` (fill) | `iif(isNull(col), 0, col)` (use 0 or "" as needed) |
| `clip_or_flag` | `greatest(toDouble(col), 0)` |

---

### Step 5.8: Publish and Run the Pipeline

1. Click **Publish all** (top of the Author pane)

2. Confirm when prompted

3. Go to **Monitor** → **Pipeline runs**

4. Click **+ Trigger** → **Trigger now** on `pl_transform_from_manifest`

5. Click **OK**

6. Wait for the run to complete. Check for errors in the output.

**✓ Checkpoint:** After a successful run, the `cleaned` container should contain the output file. Go to Storage Account → Containers → cleaned and verify.

---

### Step 5.9: Optional — Parameterize Manifest Path

To make the pipeline flexible:

1. In the pipeline, go to **Parameters** (top) → **+ New**
   - Name: `manifestPath`
   - Default: `manifests/transformation_manifest.json`

2. Edit `ds_manifest` dataset:
   - File path: use expression `@pipeline().parameters.manifestPath`

3. Save and publish.

---

## 6. Phase 4: End-to-End Orchestration

To run assessment → manifest → ETL automatically:

---

### Step 6.1: Create a Master Pipeline

1. **Author** → **Pipelines** → **+ New pipeline**

2. Name: `pl_assessment_to_etl`

3. Add activities in this order:

   **A) Execute assessment** (Azure Batch, Azure Function, or Custom activity)
   - Use a **custom activity** or **Azure Batch** that runs:
     ```bash
     python main.py --sources config/sources.yaml --generate-transformation-rules --output-pending-approval output/pending.json --export-json output/assessment.json
     ```
   - This requires your code to run in Azure (e.g. Azure Batch pool, Container Instance, or Function). Configure the activity to point to your script/container.

   **B) Manual approval (optional)**
   - Add **Webhook** or **Wait** if you want human approval before ETL.

   **C) Build manifest**
   - Another activity that runs `python scripts/build_manifest.py` (after approved_transforms.json is available).

   **D) Copy manifest to Blob**
   - Use **Copy data** activity: Source = local/staging file, Sink = `manifests/transformation_manifest.json`.

   **E) Run ETL pipeline**
   - Add **Execute pipeline** activity: select `pl_transform_from_manifest`.

4. Connect them in sequence: A → B (optional) → C → D → E

5. Add a **Schedule trigger** (e.g. daily) if desired.

---

### Step 6.2: Simpler Alternative — Two Manual Steps

If full automation is not needed yet:

1. **Step 1:** Run locally:
   ```bash
   python main.py --sources config/sources.yaml --generate-transformation-rules --output-pending-approval output/pending_approval.json
   ```
   Review, approve, run `scripts/build_manifest.py`, then `scripts/upload_manifest.py`.

2. **Step 2:** In ADF, trigger `pl_transform_from_manifest` manually or on a schedule.

---

## 7. Troubleshooting

| Issue | What to check |
|-------|----------------|
| **No `suggested_transforms` in pending_approval.json** | Data may have no DQ issues. Add test data with known issues (e.g. leading spaces, invalid dates). Or use `--use-rule-based` to force rule-based suggestions. |
| **Azure OpenAI error in Step 3** | Verify `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_DEPLOYMENT`. Or skip AI: use `--use-rule-based` so rules come from transformation_suggester. |
| **Blob upload fails** | Check account name, key, container name. Ensure `manifests` container exists. |
| **ADF connection test fails** | Verify Storage Account name and key. Check network/firewall. |
| **Data Flow fails** | Check column names match your data. Verify date format in `toTimestamp()`. Ensure sink path is valid. |
| **Empty output in `cleaned`** | Verify source dataset path (container/folder/file). Check that raw data exists and format (CSV/JSON) matches. |

---

## 8. Governance and Production Checklist

- [ ] Store secrets in **Azure Key Vault**; use Key Vault references in ADF linked services
- [ ] Use **managed identity** for ADF and storage where possible
- [ ] Enable **versioning** on the `manifests` container for audit
- [ ] Add **alerts** in ADF for pipeline failures
- [ ] Document who approves transforms and when
- [ ] Use versioned manifest names (e.g. `transformation_manifest_20250310.json`) for compliance

---

## Summary

| Phase | Action |
|-------|--------|
| **1** | Run `main.py` with `--generate-transformation-rules` and `--output-pending-approval` |
| **2** | Review `pending_approval.json`, save as `approved_transforms.json` |
| **3** | Run `scripts/build_manifest.py` to create `transformation_manifest.json` |
| **4** | Upload manifest to `manifests` container (Azure CLI or Python script) |
| **5** | In ADF: Link storage, create datasets, add Lookup + Data Flow, run pipeline |
| **6** | Optionally orchestrate with a master pipeline and triggers |

Your project generates the rules; Azure ETL executes them at scale.
