# What You Need to Run This Project

## 1. Python & dependencies

- **Python 3.8+**
- Install deps:
  ```bash
  cd c:\path\to\agent_dhara_backend
  pip install -r requirements.txt
  ```

## 2. What you can run **without** any credentials

- **Local files only** (no Azure, no database):
  - Put CSV/JSON/Parquet/Excel files in `sample_data/` (or any folder).
  - Use `config/sources_local.yaml` (points to `sample_data`).
  - Run:
    ```bash
    python main.py --sources config/sources_local.yaml --skip-azure --export-json output/assessment.json --export-report output/report.md --export-html output/report.html
    ```
  - Reports are written under `output/`.

## 3. To use **Azure Blob Storage**

Fill in `config/sources.yaml` (or a copy) with:

| Setting | Where |
|--------|--------|
| **account_name** | Your storage account name |
| **account_key** | Storage account key (or use connection_string / managed identity) |
| **container** (input) | Container with your data files (e.g. `assessment`) |
| **container** (output) | Container for reports (e.g. `output`) |

Then run without `--skip-azure` and add `--export-to-azure` if you want reports uploaded to Blob.

## 4. To use **Azure SQL Database**

- **Extra install:** pythonnet and a .NET SQL client (e.g. Microsoft.Data.SqlClient) for your OS. Without this, the app still runs but skips the database step.
- In `config/sources.yaml`, under a `type: database` location, set:
  - **server** (e.g. `yourserver.database.windows.net`)
  - **database**
  - **username** / **password**

## 5. Summary: minimum to run “now”

1. `pip install -r requirements.txt`
2. Use `config/sources_local.yaml` and `sample_data/` (already set up).
3. Run:
   ```bash
   python main.py --sources config/sources_local.yaml --skip-azure --export-json output/assessment.json --export-report output/report.md --export-html output/report.html
   ```

No Azure or SQL details needed for this. To use your own Azure Blob or SQL, fill the values in `config/sources.yaml` as above.
