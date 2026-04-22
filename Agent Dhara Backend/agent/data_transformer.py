"""
Data Transformer: applies rule-based transformations to clean DataFrames.

Uses suggested_transformations from the assessment to apply: trim, parse_dates,
fill_or_drop, deduplicate, coerce_numeric, currency_normalize, word_to_number, etc.
"""

from __future__ import annotations

import json
import re
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

# RFC 5322 simplified: basic email validation
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def _safe_str(x: Any) -> str:
    """Safely convert value to string for comparison; handles NaN, bytes, numpy types."""
    if pd.isna(x):
        return ""
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8", errors="replace")
        except Exception:
            return str(x)
    return str(x).strip()


def _apply_trim(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Strip leading/trailing whitespace. Handles str, numpy.str_, bytes, None, NaN."""
    def _strip_val(x):
        if pd.isna(x):
            return x
        if isinstance(x, bytes):
            try:
                return x.decode("utf-8", errors="replace").strip()
            except Exception:
                return x
        return str(x).strip() if isinstance(x, str) or hasattr(x, "strip") else x
    df[col] = df[col].apply(_strip_val)
    return df


def _apply_parse_dates(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Parse dates with flexible formats (ISO, DD-MM-YYYY, etc.)."""
    try:
        df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed")
    except (ValueError, TypeError):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _apply_sanitize_email(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Trim and replace invalid emails with null. Fixes common issues (whitespace, malformed)."""
    def _sanitize(x):
        if pd.isna(x):
            return x
        s = str(x).strip().lower()
        if not s or s in ("nan", "none", "null", "n/a", "-"):
            return None
        if _EMAIL_RE.match(s):
            return s
        return None
    df[col] = df[col].apply(_sanitize)
    return df


def _apply_flatten_nested(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Convert nested list/dict values to JSON string for flat storage."""
    def _flatten(x):
        if pd.isna(x):
            return x
        if isinstance(x, (list, dict)):
            try:
                return json.dumps(x, ensure_ascii=False)
            except (TypeError, ValueError):
                return str(x)
        return x
    df[col] = df[col].apply(_flatten)
    return df


def _apply_zero_to_null(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Replace 0 with null in numeric/id-like columns (for suspicious_zero in IDs)."""
    num = pd.to_numeric(df[col], errors="coerce")
    mask = num == 0
    df.loc[mask, col] = pd.NA
    return df


def _apply_fill_null(df: pd.DataFrame, col: str, fill_value: Any = None) -> pd.DataFrame:
    if fill_value is not None:
        df[col] = df[col].fillna(fill_value)
    else:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)
        else:
            mode_val = df[col].mode()
            df[col] = df[col].fillna(mode_val[0] if len(mode_val) else "")
    return df


def _apply_coerce_numeric(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _apply_deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()


def _apply_clip_negative(df: pd.DataFrame, col: str, floor: float = 0) -> pd.DataFrame:
    num = pd.to_numeric(df[col], errors="coerce")
    num = num.clip(lower=floor)
    df[col] = num
    return df


def _apply_currency_normalize(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Strip currency symbols ($€£₹¥R$) and commas, then coerce to numeric."""
    def _clean(x):
        if pd.isna(x):
            return x
        s = str(x).strip().replace(",", "").replace(" ", "")
        s = re.sub(r"[$€£₹¥R\s]", "", s)
        return s
    df[col] = df[col].apply(_clean)
    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _apply_replace_values(df: pd.DataFrame, col: str, mappings: Optional[List[Tuple[Any, Any]]] = None) -> pd.DataFrame:
    """Replace specific values. mappings: [[from_val, to_val], ...]. Handles NaN, placeholders."""
    if not mappings:
        mappings = [
            ("N/A", None), ("n/a", None), ("NA", None), ("na", None),
            ("-", None), ("", None), ("null", None), ("NULL", None), ("None", None),
            ("nan", None), ("#n/a", None), ("#N/A", None), (".", None), ("#VALUE!", None),
        ]
    for from_val, to_val in mappings:
        from_str = _safe_str(from_val).lower()
        mask = df[col].astype(str).str.strip().str.lower() == from_str
        df.loc[mask, col] = to_val
    return df


def _apply_uppercase(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df[col] = df[col].apply(lambda x: x.upper() if isinstance(x, str) else x)
    return df


def _apply_lowercase(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
    return df


def _apply_fill_forward(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df[col] = df[col].ffill()
    return df


def _apply_fill_backward(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df[col] = df[col].bfill()
    return df


def _apply_fill_sequence(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Interpolate numeric sequence (101, 102, null, 104 -> 103)."""
    num = pd.to_numeric(df[col], errors="coerce")
    df[col] = num.interpolate(method="linear")
    return df


def _apply_standardize_boolean(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Map yes/no/true/false/1/0 to boolean."""
    def _to_bool(x):
        if pd.isna(x):
            return None
        s = str(x).strip().lower()
        if s in ("true", "yes", "1", "y"):
            return True
        if s in ("false", "no", "0", "n"):
            return False
        return x
    df[col] = df[col].apply(_to_bool)
    return df


def _apply_word_to_number(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Convert words like 'one hundred' to 100. Requires word2number."""
    try:
        from word2number import w2n
    except ImportError:
        return df

    def _convert(x):
        if pd.isna(x):
            return float("nan")
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            return float(x)
        s = str(x).strip()
        try:
            return float(w2n.word_to_num(s))
        except (ValueError, TypeError):
            return pd.to_numeric(s, errors="coerce")
    df[col] = df[col].apply(_convert)
    return df


def _apply_normalize_phone(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Strip non-digits from phone numbers."""
    def _digits(x):
        if pd.isna(x):
            return x
        return re.sub(r"\D", "", str(x))
    df[col] = df[col].apply(_digits)
    return df


def _apply_regex_replace(df: pd.DataFrame, col: str, pattern: str = "", replacement: str = "", params: Optional[Dict] = None) -> pd.DataFrame:
    """Replace regex pattern. params: {pattern, replacement}."""
    p = (params or {}).get("pattern") or pattern
    r = (params or {}).get("replacement") or replacement
    if not p:
        return df
    df[col] = df[col].apply(lambda x: re.sub(p, r, str(x)) if pd.notna(x) else x)
    return df


def _apply_range_clip(df: pd.DataFrame, col: str, min_val: Optional[float] = None, max_val: Optional[float] = None, params: Optional[Dict] = None) -> pd.DataFrame:
    """Clip to min/max range. params: {min, max}."""
    num = pd.to_numeric(df[col], errors="coerce")
    mn = (params or {}).get("min") if params else min_val
    mx = (params or {}).get("max") if params else max_val
    if mn is not None:
        num = num.clip(lower=mn)
    if mx is not None:
        num = num.clip(upper=mx)
    df[col] = num
    return df


# Actions we can apply automatically
_APPLIABLE_ACTIONS = {
    "trim", "parse_dates", "fill_or_drop", "coerce_numeric", "clip_or_flag",
    "deduplicate", "deduplicate_or_alert", "standardize_type", "coerce_numeric_or_flag",
    "currency_normalize", "replace_values", "uppercase", "lowercase",
    "fill_forward", "fill_backward", "fill_sequence",
    "standardize_boolean", "word_to_number", "normalize_phone",
    "regex_replace", "range_clip",
    "sanitize_email", "flatten_nested", "zero_to_null",
}
_ACTION_ALIASES = {"coerce_numeric_or_flag": "coerce_numeric", "standardize_type": "coerce_numeric"}

_INT_LIKE_COLS = {"id", "customer_id", "order_count", "quantity", "qty", "count"}


def _preserve_int_types(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast float columns to Int64 when values are whole numbers (preserves integer IDs)."""
    for col in df.columns:
        if not pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_integer_dtype(df[col]):
            continue
        col_lower = col.lower()
        is_id_like = col_lower == "id" or (col_lower.endswith("_id") and col_lower != "order_id")
        if not is_id_like:
            continue
        s = df[col]
        if s.isna().all():
            continue
        whole = (s.dropna() % 1 == 0).all()
        if whole:
            try:
                df[col] = s.astype("Int64")
            except (ValueError, TypeError):
                pass
    return df


def apply_transformations(
    datasets: Dict[str, pd.DataFrame],
    assessment_result: Dict[str, Any],
    *,
    approved_transforms: Optional[List[Dict[str, Any]]] = None,
    fill_null_strategy: str = "mode",
    drop_rows_with_null: bool = False,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    """
    Apply rule-based transformations to clean the data.

    datasets: { dataset_name: DataFrame }
    assessment_result: output of load_and_profile() (used when approved_transforms is None).
    approved_transforms: if provided, only apply these [{"dataset": "...", "column": "...", "action": "..."}].
      Column can be omitted for dataset-level actions like deduplicate.
    fill_null_strategy: "mode" | "empty" | "zero" for fill_or_drop.
    drop_rows_with_null: if True, drop rows with nulls instead of filling (for fill_or_drop).

    Returns:
      (cleaned_datasets: Dict[str, DataFrame], transform_log: Dict with applied actions)
    """
    from agent.transformation_suggester import suggest_transformations

    if approved_transforms:
        items = approved_transforms
    else:
        suggested = suggest_transformations(assessment_result)
        items = suggested.get("suggested_transformations", [])

    cleaned = {name: df.copy() for name, df in datasets.items()}
    log: List[Dict[str, Any]] = []

    col_actions: Dict[str, Dict[str, Dict[str, Any]]] = {}  # ds -> col -> {action: params}
    dataset_dedup: set = set()

    for s in items:
        ds = s.get("dataset")
        col = s.get("column")
        action = (s.get("suggested_action") or s.get("action") or "").strip().lower()
        action = _ACTION_ALIASES.get(action, action)
        params = s.get("params")
        if not ds or ds == "global":
            continue
        if action in ("deduplicate", "deduplicate_or_alert"):
            dataset_dedup.add(ds)
            continue
        if not col or action not in _APPLIABLE_ACTIONS:
            continue
        if ds not in col_actions:
            col_actions[ds] = {}
        if col not in col_actions[ds]:
            col_actions[ds][col] = {}
        col_actions[ds][col][action] = params

    # Apply column-level transforms (order: replace_values -> trim -> uppercase/lowercase -> currency -> word_to_number -> coerce -> parse_dates -> fill -> clip)
    for ds_name, cols in col_actions.items():
        if ds_name not in cleaned:
            continue
        df = cleaned[ds_name]
        for col, actions_dict in cols.items():
            if col not in df.columns:
                continue
            params = lambda a: actions_dict.get(a)

            if "replace_values" in actions_dict:
                df = _apply_replace_values(df, col, (params("replace_values") or {}).get("mappings"))
                log.append({"dataset": ds_name, "column": col, "action": "replace_values"})
            if "trim" in actions_dict:
                df = _apply_trim(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "trim"})
            if "sanitize_email" in actions_dict:
                df = _apply_sanitize_email(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "sanitize_email"})
            if "flatten_nested" in actions_dict:
                df = _apply_flatten_nested(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "flatten_nested"})
            if "zero_to_null" in actions_dict:
                df = _apply_zero_to_null(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "zero_to_null"})
            if "uppercase" in actions_dict:
                df = _apply_uppercase(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "uppercase"})
            if "lowercase" in actions_dict:
                df = _apply_lowercase(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "lowercase"})
            if "currency_normalize" in actions_dict:
                df = _apply_currency_normalize(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "currency_normalize"})
            if "word_to_number" in actions_dict:
                df = _apply_word_to_number(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "word_to_number"})
            if "regex_replace" in actions_dict:
                df = _apply_regex_replace(df, col, params=params("regex_replace"))
                log.append({"dataset": ds_name, "column": col, "action": "regex_replace"})
            if "coerce_numeric" in actions_dict or "standardize_type" in actions_dict:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    df = _apply_coerce_numeric(df, col)
                    log.append({"dataset": ds_name, "column": col, "action": "coerce_numeric"})
            if "parse_dates" in actions_dict:
                df = _apply_parse_dates(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "parse_dates"})
            if "standardize_boolean" in actions_dict:
                df = _apply_standardize_boolean(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "standardize_boolean"})
            if "normalize_phone" in actions_dict:
                df = _apply_normalize_phone(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "normalize_phone"})
            if "fill_or_drop" in actions_dict:
                if drop_rows_with_null:
                    before = len(df)
                    df = df.dropna(subset=[col])
                    log.append({"dataset": ds_name, "column": col, "action": "drop_null_rows", "rows_removed": before - len(df)})
                else:
                    fill_val = None
                    if fill_null_strategy == "empty":
                        fill_val = ""
                    elif fill_null_strategy == "zero" and pd.api.types.is_numeric_dtype(df[col]):
                        fill_val = 0
                    df = _apply_fill_null(df, col, fill_val)
                    log.append({"dataset": ds_name, "column": col, "action": "fill_null"})
            if "fill_forward" in actions_dict:
                df = _apply_fill_forward(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "fill_forward"})
            if "fill_backward" in actions_dict:
                df = _apply_fill_backward(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "fill_backward"})
            if "fill_sequence" in actions_dict:
                df = _apply_fill_sequence(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "fill_sequence"})
            if "clip_or_flag" in actions_dict:
                df = _apply_clip_negative(df, col)
                log.append({"dataset": ds_name, "column": col, "action": "clip_negative"})
            if "range_clip" in actions_dict:
                df = _apply_range_clip(df, col, params=params("range_clip"))
                log.append({"dataset": ds_name, "column": col, "action": "range_clip"})
        cleaned[ds_name] = df

    # Apply deduplicate per dataset
    for ds_name in dataset_dedup:
        if ds_name not in cleaned:
            continue
        before = len(cleaned[ds_name])
        cleaned[ds_name] = _apply_deduplicate(cleaned[ds_name])
        removed = before - len(cleaned[ds_name])
        if removed > 0:
            log.append({"dataset": ds_name, "action": "deduplicate", "rows_removed": removed})

    # Preserve integer types for id-like columns (avoid 1 -> 1.0)
    for ds_name in list(cleaned.keys()):
        cleaned[ds_name] = _preserve_int_types(cleaned[ds_name])

    return cleaned, {"applied": log, "total_actions": len(log)}


def infer_output_format(dataset_name: str) -> str:
    """Infer output format from dataset name (e.g. file extension). Default: csv."""
    name = (dataset_name or "").lower()
    if name.endswith(".json") or name.endswith(".jsonl"):
        return "json" if name.endswith(".json") and not name.endswith(".jsonl") else "jsonl"
    if name.endswith(".parquet") or name.endswith(".parq"):
        return "parquet"
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return "xlsx"
    if name.endswith(".tsv"):
        return "tsv"
    if name.endswith(".csv"):
        return "csv"
    return "csv"


def build_pending_approval_for_user(
    assessment_result: Dict[str, Any],
    agent_result: Optional[Dict[str, Any]] = None,
    use_agent_rules: bool = True,
) -> Dict[str, Any]:
    """
    Build a pending approval payload. When agent_result has structured_rules and use_agent_rules
    is True, AI-generated rules are used as suggested_transforms; otherwise fall back to
    rule-based suggestions.
    User reviews this, edits to keep only approved items, saves as approved_transforms.
    suggested_transforms: list of {dataset, column, issue_type, action, message}.
    approved_transforms format: same items, just the list of ones to apply.
    """
    from agent.transformation_suggester import suggest_transformations

    items: List[Dict[str, Any]] = []

    # Prefer AI-generated rules when available and use_agent_rules
    agent_rules = (
        agent_result.get("structured_rules", [])
        if agent_result and agent_result.get("success") and use_agent_rules
        else []
    )

    if agent_rules:
        for r in agent_rules:
            ds = r.get("dataset")
            if not ds or ds == "global":
                continue
            action = (r.get("action") or "").strip().lower()
            if action not in _APPLIABLE_ACTIONS:
                continue
            items.append({
                "dataset": ds,
                "column": r.get("column"),
                "issue_type": "ai_suggested",
                "action": action,
                "message": r.get("message", ""),
            })

    if not items:
        # Fall back to rule-based suggestions
        suggested = suggest_transformations(assessment_result)
        for s in suggested.get("suggested_transformations", []):
            ds = s.get("dataset")
            if not ds or ds == "global":
                continue
            action = (s.get("suggested_action") or "").strip().lower()
            if action not in _APPLIABLE_ACTIONS:
                continue
            items.append({
                "dataset": ds,
                "column": s.get("column"),
                "issue_type": s.get("issue_type"),
                "action": action,
                "message": s.get("message", ""),
            })

    out = {
        "suggested_transforms": items,
        "source": "ai" if agent_rules else "rule_based",
        "instructions": "Review the suggested_transforms below. Remove any you do not want to apply. Save as approved_transforms.json and run with --apply-transformations --approved-transforms-file approved_transforms.json",
    }
    if agent_result and agent_result.get("success"):
        out["agent_summary"] = agent_result.get("summary_bullets", [])
        out["agent_code_blocks"] = agent_result.get("code_blocks", [])
    return out


def write_dataframe_by_format(df: pd.DataFrame, path: str, fmt: str) -> None:
    """Write DataFrame to file in the given format (csv, json, jsonl, parquet, xlsx, tsv).
    Dates are output as YYYY-MM-DD to preserve original format.
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].apply(lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) and hasattr(x, "strftime") else None)
    if fmt == "json":
        out.to_json(path, orient="records", indent=2)
    elif fmt == "jsonl":
        out.to_json(path, orient="records", lines=True)
    elif fmt == "parquet":
        df.to_parquet(path, index=False)
    elif fmt == "xlsx":
        df.to_excel(path, index=False, engine="openpyxl")
    elif fmt == "tsv":
        df.to_csv(path, sep="\t", index=False)
    else:
        df.to_csv(path, index=False)
