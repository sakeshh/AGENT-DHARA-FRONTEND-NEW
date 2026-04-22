# """
# Upload transformation_manifest.json to Azure Blob container 'manifests'.

# Run from project root:
#   python scripts/upload_manifest.py

# Requires: config/sources.yaml with azure_blob or azure_blob_output connection
#           output/transformation_manifest.json (run scripts/build_manifest.py first)
# """
# import sys
# from pathlib import Path

# PROJECT_ROOT = Path(__file__).resolve().parent.parent
# MANIFEST_PATH = PROJECT_ROOT / "output" / "transformation_manifest.json"
# CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yaml"

# # Add project root to path
# sys.path.insert(0, str(PROJECT_ROOT))


# def main():
#     if not MANIFEST_PATH.exists():
#         print(f"[ERROR] {MANIFEST_PATH} not found.")
#         print("  First run: python scripts/build_manifest.py")
#         raise SystemExit(1)

#     if not CONFIG_PATH.exists():
#         print(f"[ERROR] {CONFIG_PATH} not found.")
#         raise SystemExit(1)

#     import yaml
#     with open(CONFIG_PATH, "r", encoding="utf-8") as f:
#         cfg = yaml.safe_load(f)

#     source_cfg = cfg.get("source", cfg)
#     locations = source_cfg.get("locations", [])
#     conn_cfg = None
#     for loc in locations:
#         if (loc.get("type") or "").lower() in ("azure_blob", "azure_blob_output"):
#             conn_cfg = loc.get("connection", {})
#             if conn_cfg:
#                 break
#     if not conn_cfg:
#         print("[ERROR] No azure_blob or azure_blob_output connection in sources.yaml")
#         raise SystemExit(1)

#     from connectors.azure_blob_storage import AzureBlobStorageConnector
#     conn = AzureBlobStorageConnector(conn_cfg)

#     with open(MANIFEST_PATH, "rb") as f:
#         data = f.read()

#     ok = conn.upload_blob(
#         "transformation_manifest.json",
#         data,
#         container="manifests",
#     )
#     if ok:
#         print(f"Uploaded {MANIFEST_PATH} to container 'manifests'")
#     else:
#         print("[ERROR] Upload failed. Check storage account and key.")
#         raise SystemExit(1)


# if __name__ == "__main__":
#     main()
