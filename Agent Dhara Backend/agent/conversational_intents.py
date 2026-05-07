"""
conversational_intents.py
─────────────────────────
Post-assessment chat intent classifier for Agent Dhara.

Intent IDs
  1  REPORT_GENERATE
  2  ISSUE_LIST
  3  ISSUE_FILTER
  4  TRIAGE / PRIORITIZE
  5  CROSS_DATASET
  6  CLARIFY
  7  OUT_OF_SCOPE
  8  ADVERSARIAL

Rules
  • Returns None  →  chat_graph.py handles the message (navigation / buttons / data-source flow)
  • Returns dict  →  a specialist should handle (only when an assessment exists in context)
  • classify_intent() is the ONLY public entry-point used by chat_graph.py
  • langchain_tool_router.py / langgraph_orchestrator.py must NOT be imported anywhere
    in the active message path — they were added experimentally and break navigation
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Navigation guard
# Every value in _NAVIGATION_EXACT is the exact lowercased text sent when a
# user clicks an option button in chat_graph.py  (_flow_options / send= values).
# We also cover the common natural-language equivalents users might type.
# For ALL of these → return None immediately so chat_graph.py handles them.
# ─────────────────────────────────────────────────────────────────────────────

_NAVIGATION_EXACT: frozenset = frozenset({
    # ── Generic flow controls ──────────────────────────────────────────────
    "back", "← back", "menu", "restart", "help", "cancel",
    # ── Data-source selection / listing ───────────────────────────────────
    "list files in blob", "list blob files", "list files",
    "list local files", "list local",
    "list tables", "list sources",
    "select all files", "select all",
    "view data", "view selection",
    "selection status", "what is selected",
    # ── Assessment triggers ────────────────────────────────────────────────
    "run data quality assessment", "run assessment",
    "assess selected files", "assess selected tables",
    "assess selected local files",
    "run dq", "start assessment", "check data quality",
    # ── Post-assessment view actions (option buttons) ─────────────────────
    "top issues (short list)", "top issues", "short list",
    "narrative report summary", "narrative report",
    "relationships / joins", "relationships/joins", "relationships",
    "dq counts",
    "generate report",
    "show schema", "show metadata", "show preview",
    "view top 10 rows",
    "cleaning recommendations",
    "suggested transformations",
})

_NAVIGATION_PREFIXES: tuple = (
    # These are sent by dynamically-built option buttons
    "select source",
    "select table",
    "select file",
    "view data in",
    "list files in",
    "list tables in",
    "list blobs in",
    "assess ",
    "run assessment on",
    "preview ",
    "show schema for",
    "show preview of",
)


def _is_navigation(low: str) -> bool:
    """Return True for any option-button send value or navigation phrase."""
    s = low.strip()
    if s in _NAVIGATION_EXACT:
        return True
    for prefix in _NAVIGATION_PREFIXES:
        if s.startswith(prefix):
            return True
    # Dynamic numeric option selections: "select 2", "option 3"
    if re.match(r"^(select|option|source|table|file)\s+\d+$", s):
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Helpers kept for backward-compat (called from chat_graph.py in some builds)
# ─────────────────────────────────────────────────────────────────────────────

def select_best_response(specialist_outputs: List[str], message: str = "") -> str:  # noqa: ARG001
    for s in specialist_outputs or []:
        if isinstance(s, str) and s.strip():
            return s.strip()
    return ""


def fallback_router_intent(message: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:  # noqa: ARG001
    low = (message or "").lower().strip()
    if not low:
        return None
    if any(t in low for t in (" this", "this ", "fix this", " too", "too.")) and len(low) < 90:
        return {"intent": 6, "reason": "fallback_deictic"}
    if any(w in low for w in ("stock price", "quantum", " ipl", "fastapi")):
        return {"intent": 7, "reason": "fallback_ood"}
    return None


def _peek_assessment(context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = context.get("last_assessment_result")
    return r if isinstance(r, dict) else None


# ─────────────────────────────────────────────────────────────────────────────
# Safety / domain guards
# ─────────────────────────────────────────────────────────────────────────────

def _is_adversarial(low: str) -> bool:
    patterns = (
        "ignore the dataset", "ignore data", "tell me everything is clean",
        "everything is clean", "invent some", "invent issues", "fabricate",
        "don't analyze", "do not analyze", "dont analyze", "just say ready",
        "without checking any files", "without checking", "override the report",
        "say it's safe", "want you to say", "contradict the report",
    )
    return any(p in low for p in patterns)


def _is_ood(low: str) -> bool:
    code_keys = (
        "generate code", "write code", "etl code", "generate etl code",
        "generate etl", "write etl", "create etl", "build etl",
        "python code", "python script", "write python", "write a python",
        "write sql", "generate sql", "create sql", "sql script",
        "write script", "generate script", "create script", "build script",
        "write a script", "give me code", "give code", "show me code",
        "write me code", "code for this", "code to fix", "code to clean",
        "automate this", "automate the fix", "write automation",
        "write pipeline", "build pipeline", "create pipeline",
        "write a pipeline", "generate pipeline", "build a pipeline",
        "spark code", "pyspark", "pandas code", "write pandas",
        "write pyspark", "generate pyspark", "write spark",
        "write dbt", "generate dbt", "dbt model", "write dbt model",
        "write airflow", "generate airflow", "airflow dag", "write dag",
    )
    if any(k in low for k in code_keys):
        return True
    general_keys = (
        "stock price", "share price", "nifty", "sensex", "nyse", "nasdaq",
        "fastapi", "django app", "flask app",
        "quantum computing", "explain quantum",
        "president of the", "prime minister",
        "ipl match", "ipl ", "world cup", "super bowl",
        "latest news", "who won",
        "write a poem", "tell me a joke", "what is the weather",
        "how to cook", "recipe for",
    )
    return any(k in low for k in general_keys)


# ─────────────────────────────────────────────────────────────────────────────
# Intent detectors (unchanged from original working version)
# ─────────────────────────────────────────────────────────────────────────────

def _is_clarify(low: str, raw: str) -> bool:
    if len(raw.strip()) <= 28:
        tiny = {
            "fix this.", "fix this", "is it okay?", "is it okay",
            "compare these.", "compare these", "why is this bad?", "why is this bad",
            "what's the issue here?", "what is the issue here",
            "check this one too", "check this one too.",
        }
        if raw.strip().lower() in tiny:
            return True
    phrases = (
        "check this one too", "what should i do next", "what's the issue here",
        "what is the issue here", "use the same logic as before", "again but better",
        "do the report again", "is it okay", "why is this bad",
        "can you make this ready", "make this ready", "do this again",
        "same logic as before", "check this too",
    )
    return any(p in low for p in phrases)


def _is_cross_dataset(low: str) -> bool:
    return any(k in low for k in (
        "compare ", "between these", "cross-dataset", "cross dataset",
        "orphan foreign", "foreign keys between", "relationships between",
        "join issues", "join problem", "joining",
        "across files", "across datasets", "schema naming",
        "naming problems across", "customers.csv", "orders.csv",
    ))


def _is_triage(low: str) -> bool:
    return any(k in low for k in (
        "2 hours", "two hours", "fix first", "clean first", "before loading",
        "before warehouse", "highest priority", "prioritize", "production-ready",
        "production ready", "warehouse-ready", "warehouse ready", "etl risk",
        "riskiest", "most important", "dashboard errors", "business-team",
        "business team", "which dataset is blocked", "blocked and why",
        "safest to load", "manual-review", "manual review burden",
        "source-system", "source system", "user-entry", "user entry",
        "what should i fix", "what to fix first", "where do i start",
        "where should i start", "fix order", "order of fixing",
        "what needs fixing first", "most urgent", "urgent issues",
        "critical first", "fix critical", "tackle first",
    ))


def _is_issue_filter(low: str) -> bool:
    return any(k in low for k in (
        "null-related", "null related", "null issues", "missing values",
        "duplicate-related", "duplicate issues", "duplicate only",
        "duplicates only", "email issues", "invalid email", "phone issues",
        "identifier", "primary key",
        "only nulls", "just nulls", "show nulls", "show null",
        "only duplicates", "just duplicates", "show duplicates",
        "only email", "just email issues", "show email",
        "only phone", "just phone", "show phone issues",
        "format issues only", "type issues only",
    ))


def _is_issue_list(low: str) -> bool:
    if re.search(r"top\s*\d+", low):
        return True
    if re.search(r"\btop\s+(five|5)\b", low):
        return True
    keys = (
        "list issues", "red flags", "red flag", "what's wrong", "what is wrong",
        "whats wrong", "issues only", "main problems", "biggest problems",
        "most risky", "risky for etl", "broken pipelines",
        "break downstream pipelines", "suspicious", "clean enough to load",
        "clean enough", "production-ready or not", "use this dataset directly",
        "rows should worry", "worry me the most", "auto-fixable", "manual review",
        "which columns", "business risks", "business risk", "data engineer",
        "data engineer-focused", "auto fixable",
        "what problems", "what are the problems", "what issues",
        "what are the issues", "show me issues", "show issues",
        "what went wrong", "tell me the issues", "list the problems",
        "what should i know", "what do i need to fix",
        "give me a summary", "summarise issues", "summarize issues",
        "how bad is", "how clean is", "is this data clean",
        "is my data clean", "is the data good", "is it clean",
        "what's the status", "status of the data", "data status",
        "give me an overview", "overview of issues",
        "focused on", "should i focus", "focus on",
        "what to be aware of", "be aware of",
    )
    if any(k in low for k in keys):
        return True
    if any(k in low for k in ("analyze this", "inspect this", "check this data", "scan this")):
        if not any(x in low for x in ("full report", "markdown", "html report", "detailed report", "entire report")):
            return True
    return False


def _is_full_report(low: str) -> bool:
    return any(k in low for k in (
        "executive summary", "full narrative", "full report", "detailed report",
        "entire report", "markdown report", "html report", "narrative summary",
        "summarize the report", "summary of the report",
        "plain english summary of the report", "engineer-focused summary",
        "rank issues by severity",
        "generate a report", "generate dq report", "generate quality report",
        "generate data quality report", "create a report", "create dq report",
        "build a report", "give me a report", "show me a report", "produce a report",
    ))


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def classify_intent(message: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Returns an intent dict when the message is a post-assessment analytical
    question that a specialist should handle.

    Returns None for everything else so chat_graph.py handles it normally:
      • Option button clicks ("list files in blob", "top issues", "back" …)
      • Data-source / dataset listing / selection
      • SQL / code blocks
      • Pre-assessment messages (no assessment in context yet)
    """
    raw = (message or "").strip()
    if not raw:
        return None
    low = raw.lower()

    # ── 1. Navigation / button guard (MOST IMPORTANT) ──────────────────────
    #    Must be FIRST — before any other check.
    #    If True → chat_graph.py owns this message entirely.
    if _is_navigation(low):
        return None

    # ── 2. SQL / code blocks → chat_graph nl_query flow ───────────────────
    if low.startswith(("select ", "insert ", "update ", "delete ", "with ")):
        return None
    if raw.startswith("```"):
        return None

    # ── 3. Adversarial — always block ─────────────────────────────────────
    if _is_adversarial(low):
        return {"intent": 8, "reason": "adversarial_policy"}

    # ── 4. Out-of-domain — always block ───────────────────────────────────
    if _is_ood(low):
        return {"intent": 7, "reason": "out_of_domain"}

    has_assessment = _peek_assessment(context) is not None

    # ── 5. Clarify (vague/deictic) ─────────────────────────────────────────
    if _is_clarify(low, raw):
        return {"intent": 6, "reason": "underspecified"}

    # Cross-dataset without assessment → ask user to run one first
    if _is_cross_dataset(low) and not has_assessment:
        return {"intent": 6, "reason": "cross_dataset_needs_selection"}

    # ── 6. Keyword matching (only when assessment exists) ──────────────────
    if has_assessment:
        if _is_cross_dataset(low):
            return {"intent": 5, "reason": "cross_dataset"}
        if _is_triage(low):
            return {"intent": 4, "reason": "prioritize"}
        if _is_issue_filter(low):
            return {"intent": 3, "reason": "issue_slice"}
        if _is_issue_list(low):
            return {"intent": 2, "reason": "issue_list"}
        if _is_full_report(low):
            return {"intent": 1, "reason": "full_report"}
        # Natural language question with a "?" — treat as issue list
        if len(low.split()) >= 4 and "?" in raw:
            return {"intent": 2, "reason": "nl_question_with_assessment"}

    # ── 7. Default: let chat_graph.py handle ──────────────────────────────
    return None
