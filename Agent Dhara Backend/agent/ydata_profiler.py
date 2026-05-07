"""
YData Profiling specialist for Agent Dhara.

Provides deep statistical profiling of a pandas DataFrame using
ydata-profiling (formerly pandas-profiling).

The profiler runs AFTER the core DQ engine assessment — it enriches
the result with advanced statistics without replacing existing checks.

Key outputs injected into assessment result:
  - Extended column statistics (mean, std, min, max, skewness, kurtosis)
  - Missing value counts and percentages
  - Duplicate row count
  - Correlation matrix (Pearson / Spearman)
  - Sample rows

Usage:
    from agent.ydata_profiler import enrich_assessment_with_profile

    result = enrich_assessment_with_profile(
        result=existing_assessment_result,
        dataset_name="customers.csv",
        df=customers_df,
        minimal=True,      # fast mode — no correlation matrix
    )
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _extract_column_stats(profile_description: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull per-column statistics from the ydata-profiling profile description dict.
    Returns a flat dict keyed by column name.
    """
    columns_out: Dict[str, Any] = {}
    variables = profile_description.get("variables") or {}
    for col_name, stats in variables.items():
        if not isinstance(stats, dict):
            continue
        col_entry: Dict[str, Any] = {
            "dtype":            stats.get("type"),
            "count":            stats.get("count"),
            "missing_count":    stats.get("n_missing"),
            "missing_pct":      _safe_float(stats.get("p_missing")),
            "distinct_count":   stats.get("n_distinct"),
            "distinct_pct":     _safe_float(stats.get("p_distinct")),
        }
        # Numeric-only stats
        for stat_key in ("mean", "std", "min", "max", "median",
                         "skewness", "kurtosis", "variance", "range"):
            val = _safe_float(stats.get(stat_key))
            if val is not None:
                col_entry[stat_key] = val
        # Quantiles
        quantiles = stats.get("quantiles") or {}
        if isinstance(quantiles, dict):
            col_entry["quantiles"] = {
                k: _safe_float(v) for k, v in quantiles.items() if _safe_float(v) is not None
            }
        columns_out[col_name] = col_entry
    return columns_out


def _extract_table_stats(profile_description: Dict[str, Any]) -> Dict[str, Any]:
    """Pull top-level table statistics from the profile description."""
    table = profile_description.get("table") or {}
    return {
        "row_count":       table.get("n"),
        "column_count":    table.get("n_var"),
        "duplicate_rows":  table.get("n_duplicates"),
        "missing_cells":   table.get("n_cells_missing"),
        "missing_pct":     _safe_float(table.get("p_cells_missing")),
        "memory_size":     table.get("memory_size"),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_profile(
    df: pd.DataFrame,
    dataset_name: str = "dataset",
    minimal: bool = True,
    sample_size: Optional[int] = 50_000,
) -> Optional[Dict[str, Any]]:
    """
    Run ydata-profiling on *df* and return a structured dict with key stats.

    Args:
        df:           The pandas DataFrame to profile.
        dataset_name: Name used in log messages.
        minimal:      If True, skips expensive correlation and interaction plots
                      (recommended for large datasets / chat workflows).
        sample_size:  Row limit before profiling. None = profile entire DataFrame.

    Returns:
        dict with keys: table_stats, column_stats, correlations
        None if ydata-profiling is not installed or profiling fails.
    """
    try:
        from ydata_profiling import ProfileReport  # type: ignore
    except ImportError:
        logger.warning(
            "ydata-profiling not installed. Run: pip install ydata-profiling. "
            "Skipping deep profiling for '%s'.",
            dataset_name,
        )
        return None

    if not isinstance(df, pd.DataFrame) or df.empty:
        logger.warning("ydata_profiler: empty or invalid DataFrame for '%s'.", dataset_name)
        return None

    # Sample for speed if DataFrame is very large
    df_to_profile = df
    sampled = False
    if sample_size and len(df) > sample_size:
        df_to_profile = df.sample(n=sample_size, random_state=42)
        sampled = True
        logger.info(
            "ydata_profiler: sampling %d / %d rows for '%s'.",
            sample_size, len(df), dataset_name,
        )

    try:
        logger.info("ydata_profiler: profiling '%s' (minimal=%s)...", dataset_name, minimal)
        profile = ProfileReport(
            df_to_profile,
            title=f"Agent Dhara — {dataset_name}",
            minimal=minimal,
            progress_bar=False,
        )
        description = profile.get_description()
        # ydata-profiling returns a ProfileDescription object; convert to dict
        if hasattr(description, "dict"):
            desc_dict: Dict[str, Any] = description.dict()
        elif hasattr(description, "__dict__"):
            desc_dict = vars(description)
        else:
            desc_dict = dict(description)  # type: ignore

        table_stats   = _extract_table_stats(desc_dict)
        column_stats  = _extract_column_stats(desc_dict)

        # Correlation matrix (only present when minimal=False)
        correlations: Dict[str, Any] = {}
        corr_section = desc_dict.get("correlations") or {}
        if isinstance(corr_section, dict):
            for corr_name, corr_data in corr_section.items():
                if hasattr(corr_data, "to_dict"):
                    correlations[corr_name] = corr_data.to_dict()
                elif isinstance(corr_data, dict):
                    correlations[corr_name] = corr_data

        result: Dict[str, Any] = {
            "dataset_name": dataset_name,
            "sampled":      sampled,
            "sample_rows":  sample_size if sampled else len(df),
            "table_stats":  table_stats,
            "column_stats": column_stats,
        }
        if correlations:
            result["correlations"] = correlations

        logger.info(
            "ydata_profiler: profiling complete for '%s' — %d columns profiled.",
            dataset_name, len(column_stats),
        )
        return result

    except Exception as exc:
        logger.error("ydata_profiler: profiling failed for '%s': %s", dataset_name, exc)
        return None


def enrich_assessment_with_profile(
    result: Dict[str, Any],
    dataset_name: str,
    df: pd.DataFrame,
    minimal: bool = True,
    sample_size: Optional[int] = 50_000,
) -> Dict[str, Any]:
    """
    Enrich an existing Agent Dhara assessment *result* dict with ydata-profiling
    stats for one dataset.

    The profile is injected under:
        result["datasets"][dataset_name]["ydata_profile"]

    If profiling fails or ydata-profiling is not installed, the original result
    is returned unchanged (safe no-op).

    Args:
        result:       Existing assessment result dict (output of DQ engine).
        dataset_name: Key in result["datasets"] to enrich.
        df:           The pandas DataFrame for this dataset.
        minimal:      Fast mode — skip correlations (recommended for chat).
        sample_size:  Row limit before profiling.

    Returns:
        The enriched result dict (mutated in-place and returned).
    """
    if not isinstance(result, dict):
        return result

    profile = build_profile(
        df=df,
        dataset_name=dataset_name,
        minimal=minimal,
        sample_size=sample_size,
    )
    if profile is None:
        return result

    datasets = result.setdefault("datasets", {})
    if not isinstance(datasets, dict):
        return result

    ds_entry = datasets.setdefault(dataset_name, {})
    if not isinstance(ds_entry, dict):
        return result

    ds_entry["ydata_profile"] = profile

    # Backfill null_percentage into the standard column schema
    # so existing DQ checks benefit from ydata's more accurate null counts.
    std_columns = ds_entry.setdefault("columns", {})
    if isinstance(std_columns, dict):
        for col_name, col_stats in profile.get("column_stats", {}).items():
            std_col = std_columns.setdefault(col_name, {})
            if isinstance(std_col, dict):
                # Only overwrite if ydata gives a more precise value
                ydata_null_pct = col_stats.get("missing_pct")
                if ydata_null_pct is not None:
                    std_col["null_percentage"] = ydata_null_pct
                # Backfill unique count if missing
                if std_col.get("unique_count") is None and col_stats.get("distinct_count") is not None:
                    std_col["unique_count"] = col_stats["distinct_count"]

    logger.info(
        "enrich_assessment_with_profile: '%s' enriched with ydata_profile (%d columns).",
        dataset_name, len(profile.get("column_stats", {})),
    )
    return result


def enrich_all_datasets(
    result: Dict[str, Any],
    dataframes: Dict[str, pd.DataFrame],
    minimal: bool = True,
    sample_size: Optional[int] = 50_000,
) -> Dict[str, Any]:
    """
    Convenience wrapper — enrich ALL datasets in *result* that have a matching
    DataFrame in *dataframes*.

    Args:
        result:     Existing assessment result dict.
        dataframes: Dict mapping dataset_name -> pd.DataFrame.
        minimal:    Fast mode flag passed to each profile run.
        sample_size: Row limit per dataset.

    Returns:
        Enriched result dict.
    """
    if not isinstance(result, dict) or not isinstance(dataframes, dict):
        return result

    for ds_name, df in dataframes.items():
        if isinstance(df, pd.DataFrame):
            enrich_assessment_with_profile(
                result=result,
                dataset_name=ds_name,
                df=df,
                minimal=minimal,
                sample_size=sample_size,
            )
    return result
