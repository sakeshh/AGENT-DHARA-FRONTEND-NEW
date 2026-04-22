# """
# Build transformation_manifest.json from approved_transforms.json for Azure ETL.

# Run from project root:
#   python scripts/build_manifest.py

# Input:  output/approved_transforms.json
# Output: output/transformation_manifest.json
# """
# import json
# from pathlib import Path
# from datetime import datetime

# PROJECT_ROOT = Path(__file__).resolve().parent.parent
# APPROVED_PATH = PROJECT_ROOT / "output" / "approved_transforms.json"
# MANIFEST_PATH = PROJECT_ROOT / "output" / "transformation_manifest.json"


# def main():
#     if not APPROVED_PATH.exists():
#         print(f"[ERROR] {APPROVED_PATH} not found.")
#         print("  First run: python main.py --generate-transformation-rules --output-pending-approval output/pending_approval.json")
#         print("  Then review and save as output/approved_transforms.json")
#         raise SystemExit(1)

#     with open(APPROVED_PATH, "r", encoding="utf-8") as f:
#         data = json.load(f)

#     items = data.get("suggested_transforms", [])
#     datasets = {}
#     by_action = {}

#     for item in items:
#         ds = item.get("dataset")
#         col = item.get("column")
#         action = (item.get("action") or item.get("suggested_action") or "").strip().lower()
#         if not ds or ds == "global":
#             continue
#         if ds not in datasets:
#             datasets[ds] = []
#         datasets[ds].append({
#             "column": col,
#             "suggested_action": action,
#             "issue_type": item.get("issue_type", "ai_suggested"),
#         })
#         by_action[action] = by_action.get(action, 0) + 1

#     manifest = {
#         "version": "1.0",
#         "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
#         "datasets": datasets,
#         "summary": {
#             "total_suggestions": len(items),
#             "by_action": by_action,
#         },
#     }

#     MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
#     with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
#         json.dump(manifest, f, indent=2, ensure_ascii=False)

#     print(f"Wrote manifest to {MANIFEST_PATH}")
#     print(f"  Datasets: {list(datasets.keys())}")
#     print(f"  Total rules: {len(items)}")


# if __name__ == "__main__":
#     main()
