"""Intelligent Data Assessment Engine.

This module profiles datasets, detects data quality issues, relationships, and generates reports.
Supported data sources: Azure SQL, filesystem (CSV/TSV/JSON/JSONL/XML/Parquet/XLSX), Azure Blob Storage
"""
from __future__ import annotations

import hashlib
import json
import os
import re

import numpy as np
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Any, Collection, Dict, List, Optional, Tuple

# Import connectors
try:
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
except ImportError:
    AzureSQLPythonNetConnector = None


# ============================================================
# DQ THRESHOLDS (config-driven)
# ============================================================

def load_dq_thresholds(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load DQ thresholds from YAML. If path is None, use env DQ_THRESHOLDS_PATH or config/dq_thresholds.yaml."""
    path = config_path or os.environ.get("DQ_THRESHOLDS_PATH")
    if not path and os.path.isdir("config"):
        path = os.path.join("config", "dq_thresholds.yaml")
    if not path or not os.path.isfile(path):
        return {}
    try:
        import yaml
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _get_threshold(thresholds: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Get nested key from thresholds, e.g. _get_threshold(t, 'severity', 'null_pct_high', default=0.25)."""
    d = thresholds
    for k in keys:
        d = (d or {}).get(k)
        if d is None:
            return default
    return d if d is not None else default


# ============================================================
# SAFE HELPERS (prevent "unhashable type: 'list'" in pandas)
# ============================================================

def _to_key(x: Any) -> Any:
    """Convert list/dict/unhashable objects into stable strings for hashing."""
    try:
        hash(x)
        return x
    except Exception:
        try:
            return json.dumps(x, sort_keys=True, ensure_ascii=False, default=str)
        except Exception:
            return repr(x)


def safe_nunique(series: pd.Series) -> int:
    """Safe nunique even when values are lists/dicts/objects."""
    try:
        return int(series.nunique(dropna=True))
    except Exception:
        return int(series.dropna().map(_to_key).nunique(dropna=True))


def safe_is_unique(series: pd.Series) -> bool:
    """Safe uniqueness check on unhashables."""
    try:
        return bool(series.is_unique and series.notna().all())
    except Exception:
        coerced = series.map(_to_key)
        return bool(coerced.is_unique and series.notna().all())


# ============================================================
# SEMANTIC & DTYPE INFERENCE
# ============================================================

def _strip(x: Any) -> Any:
    return x.strip() if isinstance(x, str) else x


def detect_semantic_type(values: pd.Series) -> str:
    """
    Lightweight semantic type detector using a small sample.
    Returns: "date" | "email" | "numeric_id" | "free_text" | "categorical" | "unknown"
    """
    sample = values.dropna().astype(str).head(100)
    if sample.empty:
        return "unknown"
    if sample.str.match(r"^\d{4}-\d{2}-\d{2}$").any():
        return "date"
    if sample.str.contains("@").any():
        return "email"
    if sample.str.fullmatch(r"\d+").all():
        return "numeric_id"
    if sample.str.len().mean() > 50:
        return "free_text"
    return "categorical"


def _dtype_inference_for_object(series: pd.Series) -> Optional[str]:
    """
    For object dtype, give a human hint for UI:
    - "string" | "numeric_like" | "datetime_like" | "boolean_like" | "nested" | "mixed" | "unknown"
    """
    s = series.dropna().map(_strip)

    # nested?
    try:
        if s.apply(lambda v: isinstance(v, (list, dict))).any():
            return "nested"
    except Exception:
        pass

    # boolean-like
    booleans = {"true", "false", "yes", "no", "0", "1"}
    try:
        if (s.astype(str).str.lower().isin(booleans).mean() > 0.8):
            return "boolean_like"
    except Exception:
        pass

    # datetime-like
    try:
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Could not infer format, so each element will be parsed individually",
            )
            dt_coerced = pd.to_datetime(s, errors="coerce")
        if (1.0 - float(dt_coerced.isna().mean())) > 0.8:
            return "datetime_like"
    except Exception:
        pass

    # numeric-like
    try:
        num = pd.to_numeric(s, errors="coerce")
        if (1.0 - float(num.isna().mean())) > 0.8:
            return "numeric_like"
    except Exception:
        pass

    # plain strings?
    try:
        if s.apply(lambda v: isinstance(v, str)).mean() > 0.8:
            return "string"
    except Exception:
        pass

    try:
        if not s.empty:
            return "mixed"
    except Exception:
        pass
    return "unknown"


# ============================================================
# DATA PROFILING (pandas dtypes + inference hint for object)
# ============================================================

def profile_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Returns a consistent profiling dictionary for a DataFrame, including:
    - row_count, column_count, data_volume_bytes
    - columns: { col: { dtype, dtype_inference?, null_percentage, unique_count, semantic_type, candidate_primary_key }}
    """
    profile: Dict[str, Any] = {
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "data_volume_bytes": int(df.memory_usage(deep=True).sum()),
        "columns": {}
    }

    for col in df.columns:
        s = df[col]
        dtype_str = str(s.dtype)
        semantic = detect_semantic_type(s)
        hint = _dtype_inference_for_object(s) if dtype_str == "object" else None

        profile["columns"][col] = {
            "dtype": dtype_str,
            "dtype_inference": hint,
            "null_percentage": float(s.isna().mean()),
            "unique_count": safe_nunique(s),
            "semantic_type": semantic,
            "candidate_primary_key": safe_is_unique(s)
        }

    return profile


# ============================================================
# SQL LOADER
# ============================================================

def _sql_location_key_prefix(loc: Dict[str, Any], conn: Dict[str, Any], db_index: int, multi_db: bool) -> str:
    """Prefix for dataset keys when multiple database locations are configured."""
    if not multi_db:
        return ""
    for k in ("id", "label", "name"):
        v = loc.get(k)
        if v and str(v).strip():
            s = re.sub(r"[^\w\-]+", "_", str(v).strip())[:48].strip("_")
            if s:
                return s + "__"
    db = str(conn.get("database") or conn.get("Database") or f"db{db_index}")
    srv = str(conn.get("server") or conn.get("Server") or "")
    h = hashlib.md5(f"{srv}|{db}".encode("utf-8")).hexdigest()[:8]
    tail = re.sub(r"[^\w]+", "_", db)[:24].strip("_") or "db"
    return f"{tail}_{h}__"


def load_sql_datasets(
    connection_cfg: Dict[str, Any], dataset_key_prefix: str = ""
) -> Dict[str, pd.DataFrame]:
    """
    Loads all discovered tables from Azure SQL using the provided connector configuration.
    Returns a dict: { "<schema>.<table>": DataFrame, ... } or prefixed keys if dataset_key_prefix set.
    """
    if AzureSQLPythonNetConnector is None:
        print("[INFO] AzureSQLPythonNetConnector not available, skipping SQL datasets")
        return {}

    p = (dataset_key_prefix or "").strip()
    if p and not p.endswith("__"):
        p = p + "__"

    datasets: Dict[str, pd.DataFrame] = {}
    try:
        connector = AzureSQLPythonNetConnector(connection_cfg)
        tables = connector.discover_tables()

        for table in tables:
            key = f"{p}{table}" if p else table
            try:
                datasets[key] = connector.load_table(table)
            except Exception as e:
                print(f"[ERROR] Failed to load table {table}: {e}")
    except Exception as e:
        print(f"[INFO] Failed to connect to SQL database: {e}")

    return datasets


# ============================================================
# JSON DEEP-FLATTEN HELPERS
# ============================================================

def _find_record_path(obj: Any, path: Optional[List[str]] = None, max_depth: int = 4) -> Optional[List[str]]:
    """Find nested list-of-dicts path for record_path (e.g., ['departments','employees'])."""
    if path is None:
        path = []
    if max_depth < 0:
        return None
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return path
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            rp = _find_record_path(v, path + [k], max_depth - 1)
            if rp:
                return rp
    return None


def _json_deep_flatten(data: Any) -> pd.DataFrame:
    from pandas import json_normalize

    if isinstance(data, list):
        if not data:
            return pd.DataFrame()
        if isinstance(data[0], dict):
            return json_normalize(data, max_level=1)
        return pd.DataFrame({"value": data})

    if not isinstance(data, dict):
        return pd.DataFrame([{"value": data}])

    record_path = _find_record_path(data, max_depth=4)
    if not record_path:
        return json_normalize(data, max_level=1)

    meta_keys: List[str] = []

    def collect_scalars(d: Dict[str, Any]) -> None:
        for k, v in d.items():
            if not isinstance(v, (list, dict)):
                if k not in meta_keys:
                    meta_keys.append(k)

    parent: Any = data
    for k in record_path[:-1]:
        if isinstance(parent, dict):
            collect_scalars(parent)
            parent = parent.get(k, {})
        else:
            break

    try:
        return json_normalize(
            data,
            record_path=record_path,
            meta=meta_keys if meta_keys else None,
            errors="ignore"
        )
    except Exception:
        return json_normalize(data, max_level=1)


def _load_json_to_df(path: str) -> pd.DataFrame:
    if path.lower().endswith(".jsonl"):
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    rows.append({"value": line})
        if not rows:
            return pd.DataFrame()
        return pd.json_normalize(rows, max_level=1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return _json_deep_flatten(data)


# ============================================================
# XML EXPLODE (one row per <Item> container when consistent)
# ============================================================

def _xml_to_df_exploded(path: str) -> pd.DataFrame:
    root = ET.parse(path).getroot()
    nodes = list(root)
    if not nodes:
        return pd.DataFrame()

    if len(set(n.tag for n in nodes)) == 1:
        records: List[Dict[str, Any]] = []
        for node in nodes:
            base: Dict[str, Any] = {}
            containers: List[ET.Element] = []
            for child in node:
                g = list(child)
                if g:
                    containers.append(child)
                else:
                    base[child.tag] = child.text

            exploded = False
            for container in containers:
                items = list(container)
                if not items:
                    continue
                if len({c.tag for c in items}) == 1:
                    exploded = True
                    for item in items:
                        row = dict(base)
                        for sub in item:
                            row[f"{container.tag}_{sub.tag}"] = sub.text
                        records.append(row)
            if not exploded:
                records.append(base)

        return pd.DataFrame(records)

    return pd.DataFrame([{c.tag: c.text for c in node} for node in nodes])


# ============================================================
# FILE LOADER (CSV, TSV, JSON, JSONL, XML, PARQUET, XLSX)
# ============================================================

def load_file_datasets(path: str) -> Dict[str, pd.DataFrame]:
    """
    Reads supported files from a local folder and returns a dict: { "<file_name>": DataFrame }
    """
    data: Dict[str, pd.DataFrame] = {}

    if not os.path.isdir(path):
        print("[INFO] Filesystem path not found:", path)
        return data

    for file in os.listdir(path):
        fp = os.path.join(path, file)
        if not os.path.isfile(fp):
            continue

        try:
            low = file.lower()
            if low.endswith(".csv"):
                data[file] = pd.read_csv(fp, low_memory=False)
            elif low.endswith(".tsv"):
                data[file] = pd.read_csv(fp, sep="\t", low_memory=False)
            elif low.endswith(".json") or low.endswith(".jsonl"):
                data[file] = _load_json_to_df(fp)
            elif low.endswith(".xml"):
                data[file] = _xml_to_df_exploded(fp)
            elif low.endswith(".parquet"):
                data[file] = pd.read_parquet(fp)
            elif low.endswith(".xlsx"):
                data[file] = pd.read_excel(fp, engine="openpyxl")
            elif low.endswith(".html") or low.endswith(".htm"):
                tables = pd.read_html(fp)
                data[file] = tables[0] if tables else pd.DataFrame()
        except Exception as e:
            print(f"[ERROR] Reading {file}: {e}")

    return data


# ============================================================
# RELATIONSHIP DETECTION (cardinality + row-level orphan checks)
# ============================================================

def _guess_parent_child_tables(
    n1: str, df1: pd.DataFrame, c1: str,
    n2: str, df2: pd.DataFrame, c2: str,
    meta1: Dict[str, Any], meta2: Dict[str, Any],
) -> Optional[Tuple[str, pd.DataFrame, str, str, pd.DataFrame, str]]:
    """
    Return (parent_ds, parent_df, parent_col, child_ds, child_df, child_col) for FK-style checks, or None.
    """
    nn1 = int(df1[c1].notna().sum())
    nn2 = int(df2[c2].notna().sum())
    if nn1 == 0 or nn2 == 0:
        return None
    u1, u2 = safe_nunique(df1[c1]), safe_nunique(df2[c2])
    r1, r2 = u1 / max(nn1, 1), u2 / max(nn2, 1)
    k1 = df1[c1].map(_to_key)
    k2 = df2[c2].map(_to_key)
    try:
        vc1 = k1.dropna().value_counts()
        vc2 = k2.dropna().value_counts()
        common = vc1.index.intersection(vc2.index)
    except Exception:
        return None
    if len(common) == 0:
        return None
    m1 = int(vc1.reindex(common).fillna(0).max())
    m2 = int(vc2.reindex(common).fillna(0).max())

    pk1 = (meta1.get("columns") or {}).get(c1, {}).get("candidate_primary_key")
    pk2 = (meta2.get("columns") or {}).get(c2, {}).get("candidate_primary_key")
    if pk1 and not pk2:
        return (n1, df1, c1, n2, df2, c2)
    if pk2 and not pk1:
        return (n2, df2, c2, n1, df1, c1)
    if r1 >= 0.995 and r2 < 0.97:
        return (n1, df1, c1, n2, df2, c2)
    if r2 >= 0.995 and r1 < 0.97:
        return (n2, df2, c2, n1, df1, c1)
    if m1 == 1 and m2 > 1:
        return (n1, df1, c1, n2, df2, c2)
    if m2 == 1 and m1 > 1:
        return (n2, df2, c2, n1, df1, c1)
    return None


def _classify_cardinality(m1: int, m2: int) -> Tuple[str, str]:
    """
    m1 = max rows per shared key in table A; m2 = max in table B.
    Returns (cardinality_code, human_summary).
    """
    if m1 <= 1 and m2 <= 1:
        return ("one_to_one", "Each key appears at most once in both tables (1:1 on overlapping keys).")
    if m1 <= 1 < m2:
        return ("one_to_many", f"Table A has at most one row per key; table B has up to {m2} rows per key (1:N from A to B).")
    if m2 <= 1 < m1:
        return ("many_to_one", f"Table B has at most one row per key; table A has up to {m1} rows per key (N:1 from A to B).")
    return ("many_to_many", f"Keys repeat on both sides (up to {m1} vs {m2} rows per key) — M:N or bridge-style.")


MAX_REL_ROW_INDEXES = 200


def analyze_cross_dataset_relationships(
    datasets: Dict[str, pd.DataFrame],
    metadata: Dict[str, Any],
) -> Dict[str, Any]:
    """
    For each pair of datasets sharing a column name (case-insensitive):
    - overlap count, cardinality (one_to_one / one_to_many / many_to_one / many_to_many)
    - Row-level orphan FK issues (child rows whose key is missing from parent)
    - Warnings for ambiguous M:N on id-like columns
    """
    relationships: List[Dict[str, Any]] = []
    row_issues: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    names = list(datasets.keys())

    for i in range(len(names)):
        n1, df1 = names[i], datasets[names[i]]
        meta1 = metadata.get(n1, {}) or {}
        for j in range(i + 1, len(names)):
            n2, df2 = names[j], datasets[names[j]]
            meta2 = metadata.get(n2, {}) or {}
            if df1.empty or df2.empty:
                continue
            common = set(map(str.lower, df1.columns)) & set(map(str.lower, df2.columns))
            for col_lower in common:
                c1 = next(x for x in df1.columns if str(x).lower() == col_lower)
                c2 = next(x for x in df2.columns if str(x).lower() == col_lower)
                try:
                    k1 = df1[c1].map(_to_key)
                    k2 = df2[c2].map(_to_key)
                    s1k = set(k1.dropna().tolist())
                    s2k = set(k2.dropna().tolist())
                    overlap = s1k & s2k
                except Exception:
                    continue
                if not overlap:
                    continue
                vc1 = k1.dropna().value_counts()
                vc2 = k2.dropna().value_counts()
                common_idx = vc1.index.intersection(vc2.index)
                m1 = int(vc1.reindex(common_idx).fillna(0).max()) if len(common_idx) else 1
                m2 = int(vc2.reindex(common_idx).fillna(0).max()) if len(common_idx) else 1
                card, summary = _classify_cardinality(m1, m2)
                rel = {
                    "from": f"{n1}.{c1}",
                    "to": f"{n2}.{c2}",
                    "dataset_a": n1,
                    "dataset_b": n2,
                    "column_a": c1,
                    "column_b": c2,
                    "overlap_count": len(overlap),
                    "cardinality": card,
                    "max_rows_per_key_a": m1,
                    "max_rows_per_key_b": m2,
                    "summary": summary,
                    "from_a_to_b": (
                        "one_to_many" if m1 <= 1 < m2 else
                        "many_to_one" if m2 <= 1 < m1 else
                        "one_to_one" if m1 <= 1 and m2 <= 1 else
                        "many_to_many"
                    ),
                }
                relationships.append(rel)

                if m1 > 1 and m2 > 1:
                    id_like = any(
                        x in col_lower for x in ("_id", "id", "key", "code", "sku")
                    )
                    sev = "medium" if id_like else "low"
                    warnings.append({
                        "severity": sev,
                        "type": "many_to_many_relationship",
                        "datasets": [n1, n2],
                        "columns": [c1, c2],
                        "message": (
                            f"{n1}.{c1} ↔ {n2}.{c2}: keys repeat on both sides "
                            f"(max {m1} rows per key in {n1}, max {m2} in {n2})."
                        ),
                        "recommendation": (
                            "If you expected a parent–child (1:N) model, deduplicate keys on the 'one' side "
                            "or fix source extraction. If M:N is correct (e.g. orders–products), model it with "
                            "a junction table and FK constraints."
                        ),
                    })

                guess = _guess_parent_child_tables(n1, df1, c1, n2, df2, c2, meta1, meta2)
                if guess:
                    _pn, pdf, pc, cn, cdf, cc = guess
                    try:
                        parent_keys = set(_to_key(x) for x in pdf[pc].dropna())
                    except Exception:
                        parent_keys = set()
                    if not parent_keys:
                        continue
                    ck = cdf[cc].map(lambda x: _to_key(x) if pd.notna(x) else None)
                    orphan = cdf[cc].notna() & ~ck.isin(parent_keys)
                    oc = int(orphan.sum())
                    if oc > 0:
                        oidx = cdf.index[orphan].tolist()[:MAX_REL_ROW_INDEXES]
                        samples = list(cdf.loc[orphan, cc].head(8))
                        row_issues.append({
                            "severity": "high",
                            "type": "orphan_foreign_key_rows",
                            "dataset": cn,
                            "column": cc,
                            "related_dataset": _pn,
                            "related_column": pc,
                            "count": oc,
                            "row_indexes": oidx,
                            "sample_values": samples,
                            "message": (
                                f"{oc} row(s) in '{cn}' column '{cc}' reference value(s) not found in "
                                f"'{_pn}'.'{pc}' (orphan / broken FK)."
                            ),
                            "recommendation": (
                                f"1) Add missing keys to '{_pn}' or remove bad rows from '{cn}'. "
                                f"2) Enforce FK in the source DB or pipeline. "
                                f"3) Trim/normalize keys (whitespace, type) if mismatch is format-only."
                            ),
                        })

    return {
        "relationships": relationships,
        "relationship_row_issues": row_issues,
        "relationship_warnings": warnings,
    }


def detect_relationships(
    datasets: Dict[str, pd.DataFrame],
    metadata: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Returns enriched relationship list (cardinality, summaries)."""
    return analyze_cross_dataset_relationships(datasets, metadata or {})["relationships"]


# ============================================================
# DATA QUALITY CHECKS (with row indexes, config-driven thresholds)
# ============================================================

PLACEHOLDERS = {
    "", " ", "-", "--", "---", "n/a", "na", "none", "null", "nil",
    "unknown", "not available", "missing"
}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"^[+()\-\.\s0-9]{7,}$")

_DEFAULT_REC = (
    "Review with domain owners; document the expected rule; add validation at ingest or in the warehouse."
)
DQ_ISSUE_RECOMMENDATIONS: Dict[str, str] = {
    "nulls": "Map source placeholders to NULL; fix upstream capture; use defaults only where business-approved.",
    "whitespace": "Trim strings in ETL (e.g. TRIM in SQL, str.strip in pandas) before load or constraint checks.",
    "invalid_email": "Reject or quarantine bad emails; validate with regex or a mailbox API at entry.",
    "invalid_phone": "Normalize to E.164 or national format; strip junk characters in staging.",
    "invalid_date_format": "Standardize to ISO-8601 in pipeline; use robust parse with explicit format/locale.",
    "invalid_numeric": "Coerce after trim; fix type in source; quarantine non-numeric rows for manual fix.",
    "negative_values": "Clip to zero if business allows, or flag rows; verify sign convention in source.",
    "suspicious_zero": "Treat 0 as missing if appropriate, or validate IDs never zero at source.",
    "mixed_types": "Cast column to single type in ETL; split into two columns if genuinely mixed semantics.",
    "nested_structure": "Flatten JSON/XML to scalar columns or child tables before relational load.",
    "duplicate_rows": "Deduplicate on business key (keep latest by timestamp); add uniqueness constraint.",
    "duplicate_primary_key": "Resolve duplicates before load; enforce PRIMARY KEY in database.",
    "potential_primary_key": "Promote column to natural key in modeling docs; add UNIQUE constraint if stable.",
    "empty_dataset": "Verify extract scope and filters; re-run load or fix source path.",
    "duplicate_column_names": "Rename duplicate columns in extract; use explicit aliases in SQL SELECT.",
    "case_insensitive_column_collision": "Rename to a single convention (snake_case); avoid Windows/Excel collisions.",
    "very_wide_table": "Split wide tables by domain or normalize repeating groups.",
    "column_name_whitespace": "Rename columns to strip/replace spaces for SQL compatibility.",
    "date_range_violation": "Swap dates if reversed by mistake; invalidate rows that violate business window.",
    "constant_column": "Drop column if no variance, or fix extract if value should vary.",
    "dominant_value_skew": "Investigate default/fill behavior; segment by dimensions to see real spread.",
    "very_high_cardinality": "Confirm not free-text in ID column; consider hashing or surrogate keys for privacy.",
    "binary_like_column": "Encode as boolean 0/1; document semantics for both values.",
    "numeric_outliers_iqr": "Winsorize, cap, or investigate fraud/measurement errors; document exclusion rules.",
    "skewed_distribution": "Apply log transform for analytics or stratify reporting; check for contamination.",
    "integer_stored_as_float": "Cast to integer type (Int64 nullable) to avoid float drift.",
    "future_dates": "Correct clock skew or data entry; set max date validation at source.",
    "ancient_dates": "Fix century typos or replace sentinel dates with NULL.",
    "very_wide_date_span": "Split historical vs operational feeds if span is implausible for one entity.",
    "extremely_long_strings": "Truncate with audit trail, or move large text to blob/document store.",
    "empty_string_values": "Normalize empty string to NULL for consistent SQL semantics.",
    "control_characters_in_text": "Strip non-printable chars in ETL; fix export encoding (UTF-8).",
    "custom_one_of": "Map invalid values to allowed enum or reject rows per data contract.",
    "custom_range": "Clip to bounds or reject; align with business limits.",
    "custom_regex": "Fix format at source or apply regex replace in staging.",
    "custom_not_null": "Backfill from upstream or drop incomplete rows per policy.",
}


def enrich_issue_with_recommendation(issue: Dict[str, Any]) -> None:
    if issue.get("recommendation"):
        return
    issue["recommendation"] = DQ_ISSUE_RECOMMENDATIONS.get(
        issue.get("type") or "", _DEFAULT_REC
    )


def dq_issue(
    sev: str,
    typ: str,
    msg: str,
    *,
    column: Optional[str] = None,
    count: Optional[int] = None,
    rows: Optional[List[int]] = None,
    sample: Optional[List[Any]] = None
) -> Dict[str, Any]:
    """
    Create a normalized DQ issue record.
    - severity: "low" | "medium" | "high"
    - row_indexes: list of 0-based indexes (capped to 50)
    - sample_values: capped to 10
    """
    return {
        "severity": sev,
        "type": typ,
        "column": column,
        "count": count,
        "row_indexes": rows[:50] if rows else [],
        "sample_values": sample[:10] if sample else [],
        "message": msg
    }


def analyze_column(
    series: pd.Series,
    col: str,
    semantic: str,
    thresholds: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Per-column data quality checks (uses thresholds when provided).
    """
    thresholds = thresholds or {}
    sev = thresholds.get("severity", {})
    null_pct_high = _get_threshold(thresholds, "severity", "null_pct_high", default=0.25)
    null_pct_medium = _get_threshold(thresholds, "severity", "null_pct_medium", default=0.10)
    invalid_numeric_pct_high = _get_threshold(thresholds, "severity", "invalid_numeric_pct_high", default=0.10)
    invalid_date_pct_high = _get_threshold(thresholds, "severity", "invalid_date_pct_high", default=0.20)
    mixed_low = _get_threshold(thresholds, "mixed_types", "parse_rate_low", default=0.20)
    mixed_high = _get_threshold(thresholds, "mixed_types", "parse_rate_high", default=0.80)

    issues: List[Dict[str, Any]] = []
    n = len(series)
    s = series.copy()
    s_stripped = s.map(_strip)

    # null/placeholder
    null_like_mask = s_stripped.isna() | s_stripped.astype(object).map(
        lambda v: isinstance(v, str) and v.lower() in PLACEHOLDERS
    )
    null_cnt = int(null_like_mask.sum())
    if null_cnt > 0:
        ratio = null_cnt / max(n, 1)
        sev_str = "high" if ratio > null_pct_high else ("medium" if ratio > null_pct_medium else "low")
        rows = s.index[null_like_mask].tolist()
        issues.append(dq_issue(sev_str, "nulls", f"{null_cnt} null/placeholder", column=col,
                               count=null_cnt, rows=rows, sample=list(s[null_like_mask].head(5))))

    # whitespace
    ws_mask = s.astype(object).map(lambda v: isinstance(v, str) and v != v.strip())
    ws_cnt = int(ws_mask.sum())
    if ws_cnt > 0:
        rows = s.index[ws_mask].tolist()
        issues.append(dq_issue("low", "whitespace", f"{ws_cnt} leading/trailing spaces",
                               column=col, count=ws_cnt, rows=rows, sample=list(s[ws_mask].head(5))))

    # email
    if semantic == "email":
        bad_email_mask = s_stripped.astype(object).map(
            lambda v: isinstance(v, str) and not EMAIL_RE.match(v)
        ) & (~null_like_mask)
        bad_cnt = int(bad_email_mask.sum())
        if bad_cnt > 0:
            rows = s.index[bad_email_mask].tolist()
            issues.append(dq_issue("medium", "invalid_email", f"{bad_cnt} invalid email(s)",
                                   column=col, count=bad_cnt, rows=rows, sample=list(s[bad_email_mask].head(5))))

    # phone
    if isinstance(col, str) and any(p in col.lower() for p in ["phone", "mobile", "contact"]):
        bad_phone_mask = s_stripped.astype(object).map(
            lambda v: isinstance(v, str) and not PHONE_RE.match(v)
        ) & (~null_like_mask)
        bad_cnt = int(bad_phone_mask.sum())
        if bad_cnt > 0:
            rows = s.index[bad_phone_mask].tolist()
            issues.append(dq_issue("medium", "invalid_phone", f"{bad_cnt} invalid phone(s)",
                                   column=col, count=bad_cnt, rows=rows, sample=list(s[bad_phone_mask].head(5))))

    # date
    if semantic == "date":
        parsed = pd.to_datetime(s_stripped, errors="coerce")
        bad_mask = parsed.isna() & (~null_like_mask)
        bad_cnt = int(bad_mask.sum())
        if bad_cnt > 0:
            rows = s.index[bad_mask].tolist()
            sev_str = "medium" if bad_cnt / max(n, 1) <= invalid_date_pct_high else "high"
            issues.append(dq_issue(sev_str, "invalid_date_format", f"{bad_cnt} bad date(s)",
                                   column=col, count=bad_cnt, rows=rows, sample=list(s[bad_mask].head(5))))

    # numeric-like validations
    if (semantic in ("numeric_id",)) or (str(s.dtype) != "object") or (
        (str(s.dtype) == "object") and (
            (1.0 - pd.to_numeric(s_stripped, errors="coerce").isna().mean()) > 0.2
        )
    ):
        num = pd.to_numeric(s_stripped, errors="coerce")
        invalid_mask = num.isna() & (~null_like_mask)
        invalid_cnt = int(invalid_mask.sum())
        if invalid_cnt > 0:
            rows = s.index[invalid_mask].tolist()
            sev_str = "medium" if invalid_cnt / max(n, 1) <= invalid_numeric_pct_high else "high"
            issues.append(dq_issue(sev_str, "invalid_numeric",
                                   f"{invalid_cnt} non-numeric value(s)",
                                   column=col, count=invalid_cnt, rows=rows, sample=list(s[invalid_mask].head(5))))

        neg_mask = num < 0
        neg_cnt = int(neg_mask.sum())
        if neg_cnt > 0:
            rows = s.index[neg_mask].tolist()
            issues.append(dq_issue("high", "negative_values",
                                   f"{neg_cnt} negative value(s)", column=col,
                                   count=neg_cnt, rows=rows, sample=list(s[neg_mask].head(5))))

        if semantic == "numeric_id":
            zero_mask = num == 0
            zero_cnt = int(zero_mask.sum())
            if zero_cnt > 0:
                rows = s.index[zero_mask].tolist()
                issues.append(dq_issue("medium", "suspicious_zero",
                                       f"{zero_cnt} zero(s) in ID-like column", column=col,
                                       count=zero_cnt, rows=rows, sample=list(s[zero_mask].head(5))))

        parse_rate = 1.0 - float(num.isna().mean())
        if mixed_low < parse_rate < mixed_high:
            issues.append(dq_issue("medium", "mixed_types",
                                   f"Mixed numeric/text (parse={round(parse_rate*100,1)}%)",
                                   column=col))

    # structural leftovers
    struct_mask = s.astype(object).map(lambda v: isinstance(v, (list, dict)))
    struct_cnt = int(struct_mask.sum())
    if struct_cnt > 0:
        rows = s.index[struct_mask].tolist()
        issues.append(dq_issue("medium", "nested_structure",
                               f"{struct_cnt} nested list/dict values", column=col,
                               count=struct_cnt, rows=rows, sample=list(s[struct_mask].head(5))))

    return issues


# Known start/end column pairs for cross-column date logic (lowercase keys).
_DATE_RANGE_PAIRS = (
    ("start_date", "end_date"),
    ("start_dt", "end_dt"),
    ("valid_from", "valid_to"),
    ("from_date", "to_date"),
    ("begin_date", "end_date"),
    ("effective_date", "expiry_date"),
    ("effective_from", "effective_to"),
    ("period_start", "period_end"),
    ("open_date", "close_date"),
)


def run_extended_dq_checks(
    df: pd.DataFrame,
    profile: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Additional DQ: empty dataset, duplicate headers, case collisions, wide table,
    constant/dominant columns, IQR outliers, skew, string length & control chars,
    future/ancient dates, cross-column date ordering, high cardinality, binary hint.
    Respects thresholds['extended_checks'] (disabled, numeric limits).
    """
    thresholds = thresholds or {}
    ext = thresholds.get("extended_checks") or {}
    if ext.get("disabled"):
        return []

    issues: List[Dict[str, Any]] = []
    n = len(df)
    dominate_pct = float(ext.get("dominant_value_pct", 0.92))
    outlier_frac = float(ext.get("outlier_row_fraction_warn", 0.003))
    max_heavy = int(ext.get("max_rows_heavy", 100_000))
    extreme_len = int(ext.get("extreme_string_len", 4000))
    wide_cols = int(ext.get("wide_table_columns", 200))
    skew_th = float(ext.get("skew_threshold", 2.0))

    if n == 0:
        issues.append(dq_issue("high", "empty_dataset", "Dataset has zero rows"))
        return issues

    # Duplicate pandas column labels
    if df.columns.duplicated().any():
        dups = list(dict.fromkeys(df.columns[df.columns.duplicated()].tolist()))
        issues.append(dq_issue(
            "high", "duplicate_column_names",
            f"Duplicate column label(s): {dups[:8]}{'…' if len(dups) > 8 else ''}",
        ))

    lower_map: Dict[str, List[str]] = {}
    for c in df.columns:
        k = str(c).lower()
        lower_map.setdefault(k, []).append(str(c))
    for _k, cols in lower_map.items():
        if len(cols) > 1:
            issues.append(dq_issue(
                "medium", "case_insensitive_column_collision",
                f"Columns differ only by case: {cols}",
            ))

    if len(df.columns) > wide_cols:
        issues.append(dq_issue(
            "low", "very_wide_table",
            f"{len(df.columns)} columns; consider narrowing or documenting schema",
        ))

    for c in df.columns:
        cn = str(c)
        if " " in cn.strip() or cn != cn.strip():
            issues.append(dq_issue(
                "low", "column_name_whitespace",
                f"Column name has leading/trailing/embedded spaces: {cn!r}",
                column=cn,
            ))

    cols_meta = profile.get("columns", {})

    # Cross-column date range violations
    cmap = {str(c).lower(): c for c in df.columns}
    for a, b in _DATE_RANGE_PAIRS:
        ca, cb = cmap.get(a), cmap.get(b)
        if not ca or not cb:
            continue
        d1 = pd.to_datetime(df[ca], errors="coerce")
        d2 = pd.to_datetime(df[cb], errors="coerce")
        bad = d2.notna() & d1.notna() & (d2 < d1)
        bc = int(bad.sum())
        if bc > 0:
            idx = df.index[bad].tolist()[:50]
            issues.append(dq_issue(
                "high", "date_range_violation",
                f"{bc} row(s): {cb!r} before {ca!r}",
                count=bc, rows=idx,
                sample=[f"{d1[i]} → {d2[i]}" for i in list(df.index[bad])[:5]],
            ))

    # Suffix-based *_start / *_end (e.g. trip_start, trip_end)
    ends_with_start = [c for c in df.columns if str(c).lower().endswith("_start")]
    for c_start in ends_with_start:
        base = str(c_start)[:-6]
        c_end = base + "_end"
        if c_end not in df.columns:
            c_alt = base + "_stop"
            c_end = c_alt if c_alt in df.columns else None
        if not c_end:
            continue
        d1 = pd.to_datetime(df[c_start], errors="coerce")
        d2 = pd.to_datetime(df[c_end], errors="coerce")
        bad = d2.notna() & d1.notna() & (d2 < d1)
        bc = int(bad.sum())
        if bc > 0:
            issues.append(dq_issue(
                "high", "date_range_violation",
                f"{bc} row(s): {c_end!r} before {c_start!r}",
                count=bc, rows=df.index[bad].tolist()[:50],
            ))

    for col, meta in cols_meta.items():
        if col not in df.columns:
            continue
        s = df[col]
        semantic = (meta.get("semantic_type") or "unknown").lower()
        null_pct = float(meta.get("null_percentage") or 0)
        uq = int(meta.get("unique_count") or 0)
        non_null = int(s.notna().sum())

        if non_null == 0:
            continue

        if uq == 1:
            issues.append(dq_issue(
                "low", "constant_column",
                "Single distinct non-null value",
                column=col,
            ))
        elif uq > 1 and non_null > 0:
            sub = s.dropna()
            if len(sub) > max_heavy:
                sub = sub.sample(max_heavy, random_state=42)
            try:
                vc = sub.value_counts()
                if len(vc) > 0:
                    top_share = float(vc.iloc[0]) / float(len(sub))
                    if top_share >= dominate_pct:
                        issues.append(dq_issue(
                            "medium", "dominant_value_skew",
                            f"~{round(top_share*100,1)}% rows share one value (top category)",
                            column=col,
                            count=int(top_share * non_null),
                            sample=[vc.index[0]],
                        ))
            except Exception:
                pass

        if n > 20 and uq >= max(2, int(0.98 * non_null)) and semantic not in ("numeric_id", "email"):
            if uq > 50:
                issues.append(dq_issue(
                    "low", "very_high_cardinality",
                    f"{uq} distinct values (~{round(100*uq/max(non_null,1),1)}% of non-null rows)",
                    column=col,
                ))

        if uq == 2 and non_null > 10:
            issues.append(dq_issue(
                "low", "binary_like_column",
                "Only two distinct values; suitable for boolean encoding",
                column=col,
            ))

        # Numeric: IQR outliers + skew + integer-stored-as-float
        num = pd.to_numeric(s.map(_strip), errors="coerce")
        parse_ok = num.notna().sum()
        if parse_ok >= max(10, int(0.85 * non_null)):
            v = num.dropna()
            if len(v) > max_heavy:
                v = v.sample(max_heavy, random_state=42)
            q1, q3 = v.quantile(0.25), v.quantile(0.75)
            iqr = float(q3 - q1)
            if iqr > 0:
                lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                out_mask = num.notna() & ((num < lo) | (num > hi))
                oc = int(out_mask.sum())
                if oc > 0 and (oc / max(n, 1)) >= outlier_frac:
                    issues.append(dq_issue(
                        "medium", "numeric_outliers_iqr",
                        f"{oc} row(s) outside 1.5×IQR [{lo:.6g}, {hi:.6g}]",
                        column=col, count=oc,
                        rows=df.index[out_mask].tolist()[:50],
                        sample=list(num[out_mask].head(5)),
                    ))
            if len(v) >= 8:
                try:
                    sk = float(v.skew())
                    if abs(sk) >= skew_th:
                        issues.append(dq_issue(
                            "low", "skewed_distribution",
                            f"Skewness ≈ {round(sk, 2)} (heavy tail on one side)",
                            column=col,
                        ))
                except Exception:
                    pass
            if str(s.dtype).startswith("float") and non_null > 0:
                nn = num.dropna()
                if len(nn) > 0 and bool(np.allclose(nn.to_numpy(), np.round(nn.to_numpy()), rtol=0, atol=1e-9)):
                    issues.append(dq_issue(
                        "low", "integer_stored_as_float",
                        "All non-null values are whole numbers; consider Int64 dtype",
                        column=col,
                    ))

        # Dates: future / ancient / span (when column is mostly datelike)
        parsed = pd.to_datetime(s.map(_strip), errors="coerce")
        date_ok = int(parsed.notna().sum())
        if semantic == "date" or date_ok >= max(5, int(0.45 * non_null)):
            valid = parsed.dropna()
            if len(valid) > 0:
                now = pd.Timestamp.now(tz=None).normalize()
                fut = (parsed > now) & parsed.notna()
                fc = int(fut.sum())
                if fc > 0:
                    issues.append(dq_issue(
                        "medium", "future_dates",
                        f"{fc} date(s) after today",
                        column=col, count=fc,
                        rows=df.index[fut].tolist()[:50],
                        sample=list(parsed[fut].head(3)),
                    ))
                ancient = parsed.notna() & (parsed < pd.Timestamp("1900-01-01"))
                ac = int(ancient.sum())
                if ac > 0:
                    issues.append(dq_issue(
                        "low", "ancient_dates",
                        f"{ac} date(s) before 1900-01-01",
                        column=col, count=ac,
                        rows=df.index[ancient].tolist()[:30],
                    ))
                span = (valid.max() - valid.min()).days
                if span > 365 * 120:
                    issues.append(dq_issue(
                        "low", "very_wide_date_span",
                        f"Span ~{span // 365} years ({valid.min().date()} → {valid.max().date()})",
                        column=col,
                    ))

        # Strings: length extremes, control characters
        if str(s.dtype) == "object" or semantic in ("email", "free_text", "categorical"):
            str_s = s[s.apply(lambda x: isinstance(x, str))]
            if len(str_s) > max_heavy:
                str_s = str_s.sample(max_heavy, random_state=42)
            try:
                lens = str_s.str.len()
                mx = int(lens.max()) if len(lens) else 0
                if mx >= extreme_len:
                    long_mask = s.map(lambda x: isinstance(x, str) and len(x) >= extreme_len)
                    lc = int(long_mask.sum())
                    issues.append(dq_issue(
                        "medium", "extremely_long_strings",
                        f"Max length {mx} chars (≥{extreme_len})",
                        column=col, count=lc,
                        rows=df.index[long_mask].tolist()[:40],
                    ))
                zm = s.map(lambda x: x == "")
                if zm.any() and semantic != "numeric_id":
                    issues.append(dq_issue(
                        "low", "empty_string_values",
                        f"{int(zm.sum())} empty string(s)",
                        column=col, count=int(zm.sum()),
                        rows=df.index[zm].tolist()[:40],
                    ))
            except Exception:
                pass
            try:
                _ctrl_pat = r"[\x00-\x08\x0b\x0c\x0e-\x1f]"
                if n <= max_heavy:
                    str_only = s.map(lambda x: x if isinstance(x, str) else None)
                    mask = str_only.notna() & str_only.str.contains(_ctrl_pat, regex=True, na=False)
                else:
                    sub = df.loc[df.sample(max_heavy, random_state=43).index, col]
                    str_only = sub.map(lambda x: x if isinstance(x, str) else None)
                    sm = str_only.notna() & str_only.str.contains(_ctrl_pat, regex=True, na=False)
                    frac = float(sm.mean()) if len(sm) else 0.0
                    mask = pd.Series(False, index=df.index)
                    if frac > 0:
                        est = max(1, int(frac * non_null))
                        issues.append(dq_issue(
                            "medium", "control_characters_in_text",
                            f"~{est} value(s) may contain control chars (estimated from {max_heavy}-row sample)",
                            column=col, count=est,
                            rows=sub.index[sm].tolist()[:40],
                        ))
                    mask = None
                if mask is not None and mask.any():
                    issues.append(dq_issue(
                        "medium", "control_characters_in_text",
                        f"{int(mask.sum())} value(s) contain control characters",
                        column=col, count=int(mask.sum()),
                        rows=df.index[mask].tolist()[:40],
                    ))
            except Exception:
                pass

    return issues


def analyze_dataset_quality(
    name: str,
    df: pd.DataFrame,
    profile: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Return a dict with dataset-level issues + summary (uses config-driven thresholds for duplicate severity).
    """
    thresholds = thresholds or {}
    dup_pct_high = _get_threshold(thresholds, "severity", "duplicate_row_pct_high", default=0.05)
    dup_pct_warn = _get_threshold(thresholds, "severity", "duplicate_row_pct_warn", default=0.02)

    issues: List[Dict[str, Any]] = []
    n = len(df)

    try:
        row_keys = df.apply(lambda r: _to_key(tuple(_to_key(v) for v in r)), axis=1)
        dup_mask = row_keys.duplicated()
        dup_rows = int(dup_mask.sum())
    except Exception:
        dup_rows, dup_mask = 0, pd.Series(False, index=df.index)

    if dup_rows > 0:
        ratio = dup_rows / max(n, 1)
        sev = "high" if ratio > dup_pct_high else ("medium" if ratio > dup_pct_warn else "low")
        rows = df.index[dup_mask].tolist()
        issues.append(dq_issue(sev, "duplicate_rows", f"{dup_rows} duplicate row(s)",
                               count=dup_rows, rows=rows))

    for col, meta in profile.get("columns", {}).items():
        semantic = meta.get("semantic_type", "categorical")
        issues.extend(analyze_column(df[col], col, semantic, thresholds))

    cpk_cols = [c for c, m in profile.get("columns", {}).items() if m.get("candidate_primary_key", False)]
    for cpk in cpk_cols:
        cdup_mask = df[cpk].duplicated()
        if cdup_mask.any():
            dup_count = int(cdup_mask.sum())
            rows = df.index[cdup_mask].tolist()
            issues.append(dq_issue("high", "duplicate_primary_key",
                                   f"{dup_count} duplicate in candidate PK",
                                   column=cpk, count=dup_count, rows=rows,
                                   sample=list(df.loc[cdup_mask, cpk].head(5))))

    if not cpk_cols and n > 0:
        for col, m in profile.get("columns", {}).items():
            if m.get("null_percentage", 1.0) <= 0.05 and m.get("unique_count", 0) >= int(0.98 * n):
                issues.append(dq_issue("low", "potential_primary_key",
                                       "Highly unique and low-null; consider as PK",
                                       column=col))

    issues.extend(run_extended_dq_checks(df, profile, thresholds))

    return {
        "issues": issues,
        "summary": {
            "issue_count": len(issues),
            "high_severity": sum(1 for i in issues if i["severity"] == "high"),
            "medium_severity": sum(1 for i in issues if i["severity"] == "medium"),
            "low_severity": sum(1 for i in issues if i["severity"] == "low")
        }
    }


# ============================================================
# GLOBAL ISSUES (orphans, cross-dataset inconsistencies)
# ============================================================

def detect_global_issues(datasets: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    - Orphan foreign keys: values present in one dataset.column but not in the counterpart
    - Cross-dataset inconsistencies: coarse mixed numeric/text indicator per column by parse-rate
    """
    global_issues = {
        "orphan_foreign_keys": [],
        "cross_dataset_inconsistencies": []
    }

    names = list(datasets.keys())
    for i in range(len(names)):
        df1 = datasets[names[i]]
        for j in range(i + 1, len(names)):
            df2 = datasets[names[j]]

            common = set(map(str.lower, df1.columns)) & set(map(str.lower, df2.columns))
            for col in common:
                c1 = next(x for x in df1.columns if x.lower() == col)
                c2 = next(x for x in df2.columns if x.lower() == col)

                s1 = df1[c1].dropna()
                s2 = df2[c2].dropna()

                try:
                    set1 = set(_to_key(v) for v in s1.tolist())
                    set2 = set(_to_key(v) for v in s2.tolist())
                except Exception:
                    continue

                only_left = list(set1 - set2)
                only_right = list(set2 - set1)

                _orph_rec = (
                    "Align keys between datasets (trim, type cast). Add missing reference rows or remove "
                    "orphan facts in the child extract. Prefer FK constraints in the source system."
                )
                if only_left:
                    global_issues["orphan_foreign_keys"].append({
                        "from": f"{names[i]}.{c1}",
                        "to": f"{names[j]}.{c2}",
                        "orphan_count": len(only_left),
                        "sample_values": only_left[:10],
                        "recommendation": _orph_rec,
                    })
                if only_right:
                    global_issues["orphan_foreign_keys"].append({
                        "from": f"{names[j]}.{c2}",
                        "to": f"{names[i]}.{c1}",
                        "orphan_count": len(only_right),
                        "sample_values": only_right[:10],
                        "recommendation": _orph_rec,
                    })

            for nm, df in ((names[i], df1), (names[j], df2)):
                for col in df.columns:
                    s = df[col].map(_strip)
                    num = pd.to_numeric(s, errors="coerce")
                    parse_rate = 1.0 - float(num.isna().mean())
                    if 0.2 < parse_rate < 0.8:
                        global_issues["cross_dataset_inconsistencies"].append({
                            "dataset": nm,
                            "column": col,
                            "message": f"Mixed numeric/text values (parse={round(parse_rate*100,1)}%)",
                            "recommendation": (
                                "Standardize to one type in staging: coerce numerics after validation, "
                                "or split into _raw and _numeric columns."
                            ),
                        })

    return global_issues


# ============================================================
# CUSTOM RULES (config-driven, applied after standard DQ)
# ============================================================

def run_custom_rules(
    datasets: Dict[str, pd.DataFrame],
    custom_rules: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Apply custom rules from config. Each rule: dataset (or "*"), column, rule, params.
    rule: one_of, not_one_of, range, regex, not_null.
    Returns extra issues per dataset name.
    """
    extra: Dict[str, List[Dict[str, Any]]] = {}
    if not custom_rules:
        return extra

    for rule_cfg in custom_rules:
        dataset_pattern = (rule_cfg.get("dataset") or "*").strip()
        column = rule_cfg.get("column")
        rule_type = (rule_cfg.get("rule") or "").strip().lower()
        params = rule_cfg.get("params")
        if not column or not rule_type:
            continue

        for ds_name, df in datasets.items():
            if dataset_pattern != "*" and dataset_pattern != ds_name:
                continue
            if column not in df.columns:
                continue
            s = df[column].dropna().astype(str)
            if s.empty:
                continue
            issues: List[Dict[str, Any]] = []
            if rule_type == "one_of" and isinstance(params, list):
                allowed = set(str(x).strip().lower() for x in params)
                bad = ~s.str.strip().str.lower().isin(allowed)
                if bad.any():
                    cnt = int(bad.sum())
                    issues.append(dq_issue("medium", "custom_one_of",
                        f"Value not in allowed list ({cnt} rows)", column=column, count=cnt,
                        rows=df.index[bad].tolist()[:50], sample=list(s[bad].head(5))))
            elif rule_type == "range" and isinstance(params, dict):
                try:
                    num = pd.to_numeric(s, errors="coerce")
                    min_v = params.get("min")
                    max_v = params.get("max")
                    bad = pd.Series(False, index=s.index)
                    if min_v is not None:
                        bad = bad | (num < float(min_v))
                    if max_v is not None:
                        bad = bad | (num > float(max_v))
                    if bad.any():
                        cnt = int(bad.sum())
                        issues.append(dq_issue("high", "custom_range",
                            f"Value outside range ({cnt} rows)", column=column, count=cnt,
                            rows=df.index[bad].tolist()[:50], sample=list(s[bad].head(5))))
                except (TypeError, ValueError):
                    pass
            elif rule_type == "regex" and isinstance(params, (str, dict)):
                pattern = params if isinstance(params, str) else params.get("pattern", "")
                if not pattern:
                    continue
                try:
                    import re as re_mod
                    pat = re_mod.compile(pattern)
                    bad = ~s.str.strip().apply(lambda v: bool(pat.match(v)) if isinstance(v, str) else False)
                    if bad.any():
                        cnt = int(bad.sum())
                        issues.append(dq_issue("medium", "custom_regex",
                            f"Value does not match pattern ({cnt} rows)", column=column, count=cnt,
                            rows=df.index[bad].tolist()[:50], sample=list(s[bad].head(5))))
                except Exception:
                    pass
            elif rule_type == "not_null":
                null_mask = df[column].isna() | (df[column].astype(str).str.strip() == "")
                if null_mask.any():
                    cnt = int(null_mask.sum())
                    issues.append(dq_issue("high", "custom_not_null",
                        f"Null or empty not allowed ({cnt} rows)", column=column, count=cnt,
                        rows=df.index[null_mask].tolist()[:50], sample=list(df.loc[null_mask, column].head(5))))
            for i in issues:
                if ds_name not in extra:
                    extra[ds_name] = []
                extra[ds_name].append(i)
    return extra


# ============================================================
# MAIN ENTRYPOINT
# ============================================================

def load_and_profile(
    source_cfg: Dict[str, Any],
    additional_data: Optional[Dict[str, pd.DataFrame]] = None,
    dq_thresholds_path: Optional[str] = None,
    dq_thresholds: Optional[Dict[str, Any]] = None,
    return_datasets: bool = False,
    location_types: Optional[Collection[str]] = None,
) -> Dict[str, Any]:
    """
    Orchestrator:
    - Iterate over source_cfg["locations"]: all database + filesystem entries (azure_blob via additional_data)
    - Multiple databases: table keys prefixed (id/label or db hash) so names never collide
    - Merge with additional_data if provided (e.g., from Azure Blob Storage)
    - Profile each dataset; per-dataset DQ; relationships; global issues.
    - dq_thresholds: optional dict (if None, loaded from dq_thresholds_path or config).
    - return_datasets: if True, add result["_datasets"] = raw DataFrames (pop before JSON serialize).
    - location_types: optional set/list of lowercase location type strings (e.g. {"database","azure_blob"}).
      If set, only those location blocks are loaded from YAML. Blob data still comes only via additional_data
      (caller should pass {} when blob is excluded). If None, all location types are processed.
    """
    thresholds = dq_thresholds
    if thresholds is None:
        thresholds = load_dq_thresholds(dq_thresholds_path)

    datasets: Dict[str, pd.DataFrame] = {}
    source_root_by_dataset: Dict[str, str] = {}

    locations = list(source_cfg.get("locations", []) or [])
    if location_types is not None:
        allowed = {str(t).lower() for t in location_types}
        locations = [loc for loc in locations if (loc.get("type") or "").lower() in allowed]
    db_locs = [loc for loc in locations if (loc.get("type") or "").lower() == "database"]
    multi_db = len(db_locs) > 1
    db_seen = 0

    for loc in locations:
        typ = (loc.get("type") or "").lower()

        if typ == "database":
            conn = loc.get("connection", {}) or {}
            prefix = _sql_location_key_prefix(loc, conn, db_seen, multi_db)
            label = (prefix.rstrip("_") if prefix else "") or "__default__"
            for table_key, df in load_sql_datasets(conn, dataset_key_prefix=prefix).items():
                datasets[table_key] = df
                source_root_by_dataset[table_key] = (
                    f"__database__:{label}" if multi_db else "__database__"
                )
            db_seen += 1

        elif typ == "filesystem":
            fp = loc.get("path")
            if fp:
                root = os.path.abspath(os.path.normpath(fp))
                for fname, df in load_file_datasets(root).items():
                    key = fname
                    if key in datasets:
                        key = f"{os.path.basename(root.rstrip(os.sep))}__{fname}"
                    if key in datasets:
                        key = f"{hashlib.md5(root.encode('utf-8')).hexdigest()[:8]}__{fname}"
                    datasets[key] = df
                    source_root_by_dataset[key] = root

    if additional_data:
        for name, df in additional_data.items():
            datasets[name] = df
            norm = (name or "").replace("\\", "/")
            parent = os.path.dirname(norm).strip("/")
            source_root_by_dataset[name] = (
                f"azure_blob:{parent}" if parent else "azure_blob:"
            )

    metadata = {}
    for name, df in datasets.items():
        meta = profile_dataframe(df)
        meta["source_root"] = source_root_by_dataset.get(name, "")
        metadata[name] = meta

    per_dataset_dq = {}
    for name, df in datasets.items():
        per_dataset_dq[name] = analyze_dataset_quality(name, df, metadata[name], thresholds)
        metadata[name]["quality"] = per_dataset_dq[name]

    # Apply custom rules from config and merge into per_dataset_dq
    custom_rules = (thresholds or {}).get("custom_rules") or []
    if isinstance(custom_rules, list):
        extra_issues = run_custom_rules(datasets, custom_rules)
        for ds_name, issues in extra_issues.items():
            if ds_name in per_dataset_dq:
                per_dataset_dq[ds_name]["issues"].extend(issues)
                per_dataset_dq[ds_name]["summary"]["issue_count"] = len(per_dataset_dq[ds_name]["issues"])
                per_dataset_dq[ds_name]["summary"]["medium_severity"] = sum(
                    1 for i in per_dataset_dq[ds_name]["issues"] if i.get("severity") == "medium"
                )
                per_dataset_dq[ds_name]["summary"]["high_severity"] = sum(
                    1 for i in per_dataset_dq[ds_name]["issues"] if i.get("severity") == "high"
                )

    rel_bundle = analyze_cross_dataset_relationships(datasets, metadata)
    relationships = rel_bundle["relationships"]
    global_issues = detect_global_issues(datasets)
    global_issues["relationship_row_issues"] = rel_bundle["relationship_row_issues"]
    global_issues["relationship_warnings"] = rel_bundle["relationship_warnings"]

    for ds_name, block in per_dataset_dq.items():
        for iss in block.get("issues", []):
            iss.setdefault("dataset", ds_name)
            enrich_issue_with_recommendation(iss)

    out = {
        "datasets": metadata,
        "relationships": relationships,
        "data_quality_issues": {
            "datasets": per_dataset_dq,
            "global_issues": global_issues
        }
    }
    if return_datasets:
        out["_datasets"] = datasets
    return out
