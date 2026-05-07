"""
routing_guards.py
-----------------
Centralised pre-dispatch guards and message normalisation helpers used by chat_graph.py.

This file is imported from chat_graph.py to:
- normalise bare source keywords like "blob" → "select source blob" before LLM routing
- block report-only actions when no assessment exists
- keep the set of context keys to clear on reset_flow in a single place
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Actions that require a completed assessment in session context.
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
    """Expand bare source keywords to deterministic commands on fresh sessions."""
    if ctx.get("selected_source"):
        return msg  # source already set — never override
    stripped = (msg or "").strip().lower()
    return _SOURCE_ALIASES.get(stripped, msg)


def _flow_options_minimal() -> List[Dict[str, str]]:
    """Source-selection buttons for guard replies."""
    return [
        {"id": "blob",  "text": "☁️ Azure Blob",  "send": "select source blob"},
        {"id": "db",    "text": "🗄️ Database",    "send": "select source database"},
        {"id": "local", "text": "📁 Local Files", "send": "select source local"},
    ]


def guard_needs_assessment(action: str, ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    If `action` is a report-only action but no assessment exists in context,
    return a ready-made reply that guides the user back to source/assessment.
    Otherwise return None to let dispatch continue.
    """
    if action not in REPORT_ACTIONS:
        return None
    if ctx.get("last_assessment_result"):
        return None

    source_selected = ctx.get("selected_source")
    if source_selected:
        # Source chosen but assessment not run yet
        reply = (
            "⚠️ No assessment has been run yet for the selected source.\n"
            "Please select your files / tables and run an assessment first."
        )
        return {
            "reply": reply,
            "payload": {
                "step": "awaiting_assessment",
                "options": [
                    {"id": "assess", "text": "🚀 Run Assessment", "send": "assess selected files"},
                    {"id": "back",   "text": "🔙 Back",           "send": "back"},
                    {"id": "restart","text": "✅ Restart",        "send": "restart"},
                ],
            },
        }

    # No source at all — restart from the top
    reply = (
        "⚠️ No data source selected yet.\n"
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
    """Block non-navigation actions when no selected_source exists."""
    _NAV_ACTIONS = frozenset({"help", "reset_flow", "back_flow", "list_sources", "select_source", "show_selection_status"})
    if action in _NAV_ACTIONS:
        return None
    if ctx.get("selected_source"):
        return None
    if source_required_actions is not None and action not in source_required_actions:
        return None

    return {
        "reply": "⚠️ Please select a data source first.",
        "payload": {
            "step": "select_source",
            "options": _flow_options_minimal(),
        },
    }
