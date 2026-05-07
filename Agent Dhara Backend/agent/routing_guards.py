"""
routing_guards.py
-----------------
Centralised pre-dispatch guards and message normalisation helpers used by chat_graph.py.

Usage in chat_graph.py:

    from agent.routing_guards import (
        REPORT_ACTIONS,
        guard_needs_assessment,
        normalize_source_message,
        RESET_CONTEXT_KEYS,
    )
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Actions that require a completed assessment in session context.
# Attempting these on a fresh/empty session should be blocked.
# ---------------------------------------------------------------------------
REPORT_ACTIONS: frozenset = frozenset({
    "summarize_report",
    "dq_overview",
    "dq_duplicates",
    "show_null_columns",
    "relationships_overview",
    "extract_columns",
    "show_cleaning_recommendations",
    "show_transform_suggestions",
})

# ---------------------------------------------------------------------------
# Context keys that must be wiped on reset_flow / new chat.
# ---------------------------------------------------------------------------
RESET_CONTEXT_KEYS: List[str] = [
    "selected_source",
    "selected_blob_files",
    "selected_local_files",
    "selected_tables",
    "selected_table",
    "last_assessment_result",
    "last_assessment_signature",
    "last_assessment_datasets",
    "last_step",
    "selected_db_location_index",
    "selected_blob_location_index",
    "selected_fs_location_index",
]

# ---------------------------------------------------------------------------
# Source keyword normalisation — maps bare user input to deterministic commands.
# ---------------------------------------------------------------------------
_SOURCE_ALIASES: Dict[str, str] = {
    "blob":          "select source blob",
    "azure blob":    "select source blob",
    "azure":         "select source blob",
    "database":      "select source database",
    "sql":           "select source database",
    "db":            "select source database",
    "azure sql":     "select source database",
    "local":         "select source local",
    "filesystem":    "select source local",
    "local files":   "select source local",
    "local file":    "select source local",
}


def normalize_source_message(msg: str, ctx: Dict[str, Any]) -> str:
    """
    If the session has no selected_source yet and the user typed a bare
    source keyword (e.g. "blob"), expand it to the deterministic command
    so the LLM router receives an unambiguous instruction.
    """
    if ctx.get("selected_source"):
        return msg  # source already set — never override
    stripped = (msg or "").strip().lower()
    return _SOURCE_ALIASES.get(stripped, msg)


def _flow_options_minimal() -> List[Dict[str, str]]:
    """Source-selection buttons for the guard reply payload."""
    return [
        {"id": "blob",  "text": "\u2601\ufe0f Azure Blob",  "send": "select source blob"},
        {"id": "db",    "text": "\U0001f5c4\ufe0f Database",    "send": "select source database"},
        {"id": "local", "text": "\U0001f4c1 Local Files", "send": "select source local"},
    ]


def guard_needs_assessment(
    action: str,
    ctx: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Returns a ready-made ChatState dict (reply + payload) when `action` requires
    a completed assessment but none exists yet.  Returns None if the action is
    allowed to proceed.

    Example usage in chat_graph.py dispatch:

        blocked = guard_needs_assessment(action, ctx)
        if blocked:
            return blocked
    """
    if action not in REPORT_ACTIONS:
        return None
    if ctx.get("last_assessment_result"):
        return None

    # Block — no assessment exists yet
    source_selected = ctx.get("selected_source")
    if source_selected:
        # Source chosen but no assessment yet — guide to next step
        reply = (
            "\u26a0\ufe0f No assessment has been run yet for the selected source.\n"
            "Please select your files / tables and run an assessment first."
        )
        return {
            "reply": reply,
            "payload": {
                "step": "awaiting_assessment",
                "options": [
                    {"id": "assess", "text": "\U0001f680 Run Assessment", "send": "assess selected files"},
                    {"id": "back",   "text": "\U0001f519 Back",           "send": "back"},
                    {"id": "restart","text": "\u2705 Restart",           "send": "restart"},
                ],
            },
        }

    # No source at all — restart from the top
    reply = (
        "\u26a0\ufe0f No data source selected yet.\n"
        "Please choose a source to get started."
    )
    return {
        "reply": reply,
        "payload": {
            "step": "select_source",
            "options": _flow_options_minimal(),
        },
    }


def guard_needs_source(
    action: str,
    ctx: Dict[str, Any],
    source_required_actions: Optional[frozenset] = None,
) -> Optional[Dict[str, Any]]:
    """
    Returns a guard reply when `action` requires a selected_source but none exists.
    Pass a custom frozenset of action names, or it will guard ALL non-navigation actions.
    """
    _NAV_ACTIONS = frozenset({"help", "reset_flow", "back_flow", "list_sources", "select_source", "show_selection_status"})
    if action in _NAV_ACTIONS:
        return None
    if ctx.get("selected_source"):
        return None
    if source_required_actions is not None and action not in source_required_actions:
        return None

    return {
        "reply": "\u26a0\ufe0f Please select a data source first.",
        "payload": {
            "step": "select_source",
            "options": _flow_options_minimal(),
        },
    }
