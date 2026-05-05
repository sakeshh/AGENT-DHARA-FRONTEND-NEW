"""
Conversational intent classification for post-assessment chat.

Routes natural-language DQ questions to focused specialists instead of always
emitting the generic dq_overview / full narrative.

Intent IDs (contract with eval rubric):
  1 REPORT_GENERATE — long-form narrative / executive report
  2 ISSUE_LIST — ranked top issues / “what’s wrong” / red flags
  3 ISSUE_DETAIL — null / duplicate / email / phone slices
  4 PRIORITIZE — triage, 2-hour fix window, ETL-first ordering
  5 CROSS_DATASET — relationships, orphans, compare files
  6 CLARIFY — underspecified “this / too / compare”
  7 OUT_OF_SCOPE — stocks, coding tutorials, sports, trivia
  8 ADVERSARIAL — skip analysis, invent issues, override verdict
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


def select_best_response(specialist_outputs: List[str], message: str = "") -> str:
    """
    Selector stub for a single specialist path (no parallel LLM fan-out).
    If multiple strings are ever passed, pick the first non-empty.
    """
    del message  # reserved for future relevance scoring
    for s in specialist_outputs or []:
        if isinstance(s, str) and s.strip():
            return s.strip()
    return ""


def fallback_router_intent(message: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Heuristic safety net when primary classifier returns defer (narrow triggers only)."""
    low = (message or "").lower().strip()
    if not low:
        return None
    if any(t in low for t in (" this", "this ", " this.", "fix this", " too", "too.")) and len(low) < 90:
        return {"intent": 6, "reason": "fallback_short_deictic"}
    if any(w in low for w in ("stock price", "reliance", "quantum", " ipl", "ipl ", "president", "fastapi")):
        return {"intent": 7, "reason": "fallback_keyword_ood"}
    return None


def _dataset_labels(context: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for k in ("selected_blob_files", "selected_local_files", "selected_tables"):
        for x in context.get(k) or []:
            s = str(x).strip()
            if s and s not in out:
                out.append(s)
    return out


def _peek_assessment(context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = context.get("last_assessment_result")
    return r if isinstance(r, dict) else None


def _is_adversarial(low: str) -> bool:
    patterns = (
        "ignore the dataset",
        "ignore data",
        "tell me everything is clean",
        "everything is clean",
        "invent some",
        "invent issues",
        "fabricate",
        "don't analyze",
        "do not analyze",
        "dont analyze",
        "just say ready",
        "without checking any files",
        "without checking",
        "exact row numbers",
        "override the report",
        "say it’s safe",
        "say it's safe",
        "want you to say",
        "contradict the report",
    )
    return any(p in low for p in patterns)


def _is_ood(low: str) -> bool:
    keys = (
        "stock price",
        "share price",
        "nifty",
        "sensex",
        "nyse",
        "nasdaq",
        "fastapi",
        "django app",
        "flask app",
        "write a python",
        "write python",
        "quantum computing",
        "quantum ",
        "explain quantum",
        "president of the",
        "prime minister",
        "ipl match",
        "ipl ",
        "world cup",
        "super bowl",
        "latest news",
        "who won",
    )
    return any(k in low for k in keys)


def _is_clarify(low: str, raw: str) -> bool:
    if len(raw.strip()) <= 22:
        tiny = {"fix this.", "fix this", "is it okay?", "is it okay", "compare these.", "compare these"}
        if raw.strip().lower() in tiny:
            return True
    phrases = (
        "check this one too",
        "what should i do next",
        "what's the issue here",
        "what is the issue here",
        "use the same logic as before",
        "again but better",
        "do the report again",
        "is it okay",
    )
    return any(p in low for p in phrases)


def _is_cross_dataset(low: str) -> bool:
    return any(
        k in low
        for k in (
            "compare ",
            "between these",
            "cross-dataset",
            "cross dataset",
            "orphan foreign",
            "foreign keys between",
            "relationships between",
            "across files",
            "across datasets",
            "schema naming",
            "naming problems across",
            "customers.csv",
            "orders.csv",
        )
    )


def _is_triage(low: str) -> bool:
    return any(
        k in low
        for k in (
            "2 hours",
            "two hours",
            "fix first",
            "clean first",
            "before loading",
            "before warehouse",
            "highest priority",
            "prioritize",
            "production-ready",
            "production ready",
            "warehouse-ready",
            "warehouse ready",
            "etl risk",
            "riskiest",
            "most important",
            "dashboard errors",
            "business-team",
            "business team",
            "which dataset is blocked",
            "safest to load",
            "manual-review",
            "manual review burden",
            "source-system",
            "source system",
            "user-entry",
            "user entry",
        )
    )


def _is_issue_filter(low: str) -> bool:
    return any(
        k in low
        for k in (
            "null-related",
            "null related",
            "null issues",
            "missing values",
            "duplicate-related",
            "duplicate issues",
            "duplicate only",
            "duplicates only",
            "email issues",
            "invalid email",
            "phone issues",
            "identifier",
            "primary key",
        )
    )


def _is_issue_list(low: str) -> bool:
    if re.search(r"top\s*\d+", low):
        return True
    if re.search(r"\btop\s+five\b", low) or re.search(r"\btop\s+5\b", low):
        return True
    keys = (
        "list issues",
        "red flags",
        "red flag",
        "what's wrong",
        "what is wrong",
        "whats wrong",
        "issues only",
        "main problems",
        "biggest problems",
        "most risky",
        "risky for etl",
        "broken pipelines",
        "suspicious",
        "clean enough to load",
        "clean enough",
        "production-ready or not",
        "use this dataset directly",
        "rows should worry",
        "worry me the most",
        "auto-fixable",
        "manual review",
        "which columns",
        "business risks",
        "business risk",
        "data engineer",
        "data engineer-focused",
        "auto-fixable",
        "auto fixable",
    )
    if any(k in low for k in keys):
        return True
    # Broad “inspect / analyze” without an explicit long-report ask → concise list (intent 2).
    if any(k in low for k in ("analyze this", "inspect this", "check this data", "scan this")):
        if not any(x in low for x in ("full report", "markdown", "html report", "detailed report", "entire report")):
            return True
    return False


def _is_full_report(low: str) -> bool:
    return any(
        k in low
        for k in (
            "executive summary",
            "full narrative",
            "full report",
            "detailed report",
            "entire report",
            "markdown report",
            "html report",
            "narrative summary",
            "summarize the report",
            "summary of the report",
            "plain english summary of the report",
            "engineer-focused summary",
            "data engineer-focused summary",
            "rank issues by severity",
            "generate a report",
        )
    )


def classify_intent(message: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Return {intent: 1..8, reason: str} to engage conversational specialists,
    or None to defer to the legacy LLM router.
    """
    raw = (message or "").strip()
    if not raw:
        return None
    low = raw.lower()

    # Let deterministic / SQL-ish messages fall through to existing routing.
    if low.startswith("select ") or low.startswith("insert ") or low.startswith("update "):
        return None
    if raw.strip().startswith("```"):
        return None

    if _is_adversarial(low):
        return {"intent": 8, "reason": "adversarial_policy"}
    if _is_ood(low):
        return {"intent": 7, "reason": "out_of_domain"}

    has_assessment = _peek_assessment(context) is not None

    if _is_clarify(low, raw):
        return {"intent": 6, "reason": "underspecified"}

    if _is_cross_dataset(low) and not has_assessment:
        return {"intent": 6, "reason": "cross_dataset_needs_selection"}

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

    return None
