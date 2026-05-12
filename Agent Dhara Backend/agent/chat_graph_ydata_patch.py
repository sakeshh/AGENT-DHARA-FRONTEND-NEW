"""
chat_graph_ydata_patch.py

Safe helper injected into chat_graph.py assessment nodes.

The two insertion points in chat_graph.py are:

  1. _node_assess_selected_local_files  
     AFTER:  result = load_and_profile({"name": "local", "locations": []}, additional_data=dfs)
     ADD:    result = _enrich_with_ydata(result, dfs)

  2. _node_assess_selected_files  
     AFTER:  result = run_assessment(cfg_text, additional_data=dfs)
     ADD:    result = _enrich_with_ydata(result, dfs)

This file is imported by chat_graph.py:
    from agent.chat_graph_ydata_patch import _enrich_with_ydata
"""
from __future__ import annotations

import logging
from typing import Any, Dict

import pandas as pd

logger = logging.getLogger(__name__)


def _enrich_with_ydata(
    result: Dict[str, Any],
    dataframes: Dict[str, pd.DataFrame],
    minimal: bool = True,
    sample_size: int = 50_000,
) -> Dict[str, Any]:
    """
    Safe wrapper around enrich_all_datasets.

    Called immediately after the core DQ engine finishes assessment.
    Injects ydata-profiling stats (skewness, kurtosis, extended null%,
    duplicate row count, quantiles) into result["datasets"][name]["ydata_profile"].

    Safe no-op if:
    - ydata-profiling is not installed
    - result is not a dict
    - any other exception occurs

    Args:
        result:      Existing assessment result dict from DQ engine.
        dataframes:  Dict mapping dataset_name -> pd.DataFrame.
        minimal:     Fast mode - skip correlations (recommended for chat).
        sample_size: Max rows to profile per dataset (auto-samples large files).

    Returns:
        Enriched result dict (mutated in-place and returned).
    """
    if not isinstance(result, dict) or not dataframes:
        return result

    try:
        from agent.ydata_profiler import enrich_all_datasets  # type: ignore
        return enrich_all_datasets(
            result=result,
            dataframes=dataframes,
            minimal=minimal,
            sample_size=sample_size,
        )
    except ImportError:
        logger.info(
            "_enrich_with_ydata: ydata-profiling not installed - skipping. "
            "Install with: pip install ydata-profiling"
        )
        return result
    except Exception as exc:
        logger.warning("_enrich_with_ydata: profiling skipped due to error: %s", exc)
        return result
