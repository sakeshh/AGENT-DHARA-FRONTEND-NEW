# Code Improvements Checklist

Apply these changes to your existing codebase for efficiency and robustness.

---

## 1. Azure Blob connector: single `upload_file` API

**File:** `connectors/azure_blob_storage.py`

**Problem:** Two `upload_file` methods exist (one with `dest_blob_name`, one with `blob_name`, `container`). The second overrides the first.

**Fix:** Keep only one method with a clear signature:

```python
def upload_file(self, local_path: str, blob_name: str | None = None, container: str | None = None) -> bool:
    """Upload a local file. blob_name defaults to basename(local_path); container defaults to self.container."""
    if not os.path.isfile(local_path):
        print(f"[ERROR] File not found: {local_path}")
        return False
    dest = blob_name or os.path.basename(local_path)
    target = container or self.container
    container_client = self.client.get_container_client(target)
    blob_client = container_client.get_blob_client(dest)
    try:
        with open(local_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)
        return True
    except Exception as e:
        print(f"[ERROR] Upload failed {local_path} -> {dest}: {e}")
        return False
```

Remove the duplicate `upload_file` and the older `upload_blob_bytes`-only style if you have `upload_blob(blob_name, data, container)`; ensure call sites pass `(file_path, blob_name, container)` where needed (e.g. `main.py` upload_output_to_azure).

---

## 2. Call sites for upload in main.py

**File:** `main.py`

**Current:** `upload_output_to_azure(file_path, blob_name, source_cfg, output_container)` and `upload_data_to_azure(data, blob_name, source_cfg, output_container)`.

Ensure they call the connector as:

- `connector.upload_file(file_path, blob_name=blob_name, container=output_container)`  
- `connector.upload_blob(blob_name, data, container=output_container)`  

and that `AzureBlobStorageConnector` has a single `upload_file` and a single `upload_blob(blob_name, data, container=None)` method.

---

## 3. Load DQ thresholds from config (optional)

**File:** `agent/intelligent_data_assessment.py`

- Add a function `load_dq_thresholds(path: str | None) -> dict` that reads `config/dq_thresholds.yaml` (or env-specified path) and returns a dict. If file missing, return {}.
- In `analyze_column` and `analyze_dataset_quality`, replace hardcoded values (e.g. `0.25` for null_pct_high, `0.05` for duplicate_row_pct) with `thresholds.get("severity", {}).get("null_pct_high", 0.25)` etc.
- Call `load_dq_thresholds` once at the start of `load_and_profile` (or from main) and pass the dict into the analysis functions (or attach to a small context object).

---

## 4. Structured logging

**File:** `main.py` (and optionally `agent/intelligent_data_assessment.py`)

- At the top: `import logging` and `logger = logging.getLogger(__name__)`.
- Replace `print("[INFO] ...")` with `logger.info(...)`, `print("[ERROR] ...")` with `logger.error(...)`, `print("[FATAL] ...")` with `logger.error(...)` and then `sys.exit(...)`.
- In `main.py`, add a one-time config: e.g. `logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")`.
- This allows log level and format to be driven by CLI or env (e.g. LOG_LEVEL=DEBUG).

---

## 5. Retries for Azure Blob

**File:** `connectors/azure_blob_storage.py`

- For `list_blobs`, `_download_blob_bytes`, and `upload_blob`/`upload_file`, wrap the HTTP/network part in a retry loop (e.g. 3 attempts with exponential backoff 1s, 2s, 4s) for transient errors (connection errors, 429, 5xx). You can use `tenacity` or a simple loop with `time.sleep`.
- Catch exceptions that are not retryable (e.g. 404 for a missing blob, 400 for bad request) and fail immediately.

---

## 6. MCP: support optional additional_data in run_assessment

**File:** `agent/mcp_interface.py`

- Change `run_assessment(config_text: str)` to `run_assessment(config_text: str, additional_data: dict | None = None)`.
- After `cfg = _parse_config_text(config_text)`, call `load_and_profile(cfg, additional_data=additional_data or {})` so that when the MCP is invoked with pre-loaded blob data, it is passed through.

---

## 7. Idempotent blob order

**File:** `main.py` (and anywhere that iterates blobs)

- After `blobs = connector_blob.list_blobs()`, sort: `blobs = sorted(blobs)` so that the same set of blobs always produces the same order and deterministic reports.

---

## 8. Optional: chunked reading for large files

**File:** `agent/intelligent_data_assessment.py` or a new helper

- For `load_file_datasets` and blob loading, if a file (or blob) is known to be large (e.g. by streaming the first chunk and counting rows or by blob metadata), switch to chunked processing:
  - Use `pd.read_csv(..., chunksize=50000)` and aggregate profile (e.g. null count, unique count per column) and collect DQ issues per chunk (merge and cap row_indexes).
- This is a larger change; start with a single threshold (e.g. only for files > 500k rows) and one format (CSV) to avoid OOM.

Implementing 1–2 and 4–7 gives the biggest robustness and maintainability gains with minimal risk; 3 and 8 can be added incrementally.
