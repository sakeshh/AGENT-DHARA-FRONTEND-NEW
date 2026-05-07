"""
Patch instructions for chat_graph.py — YData Profiling wire-up.

This file documents the TWO exact changes needed in chat_graph.py.
Apply them manually or use the patched functions below as a drop-in reference.

CHANGE 1: _node_assess_selected_local_files
  — After:  result = load_and_profile(...)
  — Insert: result = _enrich_with_ydata(result, dfs)

CHANGE 2: _node_assess_selected_files
  — After:  result = run_assessment(cfg_text, additional_data=dfs)
  — Insert: result = _enrich_with_ydata(result, dfs)
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
    Returns result unchanged if ydata-profiling is not installed.
    """
    try:
        from agent.ydata_profiler import enrich_all_datasets
        return enrich_all_datasets(
            result=result,
            dataframes=dataframes,
            minimal=minimal,
            sample_size=sample_size,
        )
    except Exception as exc:
        logger.warning("YData profiling skipped: %s", exc)
        return result
