"""
Conversational intent classification for post-assessment chat.

## ROUTING STRATEGY (v3 — 3-layer agentic pipeline)

    User message
        ↓
    [LAYER 1]  Safety checks — adversarial / OOD  (rule-based, free, instant)
        ↓ (if safe)
    [LAYER 2]  Keyword matching                    (fast, free, covers known patterns)
        ↓ (if no keyword match)
    [LAYER 3]  LLM ToolCallingAgent (LangChain)    (PRIMARY agentic router)
        ↓ (if LangChain unavailable / error)
    [LAYER 3b] Legacy LLM JSON router              (FALLBACK)
        ↓ (if LLM picks no tool)
    [LAYER 3c] Graceful "I am unable to do this task" reply

Key design rules:
  - LLM is NEVER sent raw dataset rows — only the user's message (~100 tokens max)
  - Safety checks ALWAYS run first (zero cost)
  - Keyword layer is kept for speed and zero-cost on obvious intents
  - LLM layer fires only when keyword matching returns None
  - When LLM returns intent=7 (out-of-scope), a descriptive unable-reply is surfaced
    so the user knows what IS possible, not just a blank refusal

Intent IDs:
  1  REPORT_GENERATE
  2  ISSUE_LIST
  3  ISSUE_DETAIL / ISSUE_FILTER
  4  PRIORITIZE / TRIAGE
  5  CROSS_DATASET
  6  CLARIFY
  7  OUT_OF_SCOPE
  8  ADVERSARIAL
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Graceful unable-reply — shown when LLM finds no matching tool
# More helpful than a blank OOD refusal — tells user what THEY CAN ask
# ---------------------------------------------------------------------------
_UNABLE_REPLY = (
    "I'm unable to help with that specific request. "
    "Here's what I *can* do with your assessed datasets:\n\n"
    "- 📋 **List issues** — *\"what are the top issues?\"*\n"
    "- 🔍 **Filter by type** — *\"show only null issues\"* / *\"show duplicates\"*\n"
    "- 🚦 **ETL triage** — *\"which datasets are safe to load?\"*\n"
    "- 🔗 **Cross-dataset** — *\"compare customers and orders\"*\n"
    "- 📄 **Generate report** — *\"generate a full data quality report\"*\n\n"
    "Try rephrasing your question around one of these areas."
)


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def select_best_response(specialist_outputs: List[str], message: str = "") -> str:
    del message
    for s in specialist_outputs or []:
        if isinstance(s, str) and s.strip():
            return s.strip()
    return ""


def fallback_router_intent(message: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    low = (message or "").lower().strip()
    if not low:
        return None
    if any(t in low for t in (" this", "this ", " this.", "fix this", " too", "too.")) and len(low) < 90:
        return {"intent": 6, "reason": "fallback_short_deictic"}
    if any(w in low for w in ("stock price", "reliance", "quantum", " ipl", "ipl ", "president", "fastapi")):
        return {"intent": 7, "reason": "fallback_keyword_ood"}
    return None


def get_unable_reply() -> str:
    """Return the graceful unable-to-help message for truly out-of-scope requests."""
    return _UNABLE_REPLY


def _peek_assessment(context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = context.get("last_assessment_result")
    return r if isinstance(r, dict) else None


# ---------------------------------------------------------------------------
# Safety check functions (LAYER 1 — always run first, always rule-based)
# ---------------------------------------------------------------------------

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
    """
    Out-of-domain detection — catches anything not related to DQ assessment.
    Covers: code generation, general knowledge, finance, sports, coding help.
    """
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
        "write airflow", "generate airflow", "airflow dag",
        "write dag", "generate dag",
    )
    if any(k in low for k in code_keys):
        return True

    general_keys = (
        "stock price", "share price", "nifty", "sensex", "nyse", "nasdaq",
        "fastapi", "django app", "flask app",
        "quantum computing", "quantum ", "explain quantum",
        "president of the", "prime minister",
        "ipl match", "ipl ", "world cup", "super bowl",
        "latest news", "who won",
        "write a poem", "tell me a joke", "what is the weather",
        "how to cook", "recipe for",
    )
    return any(k in low for k in general_keys)


# ---------------------------------------------------------------------------
# Keyword intent detectors (LAYER 2)
# ---------------------------------------------------------------------------

def _is_clarify(low: str, raw: str) -> bool:
    if len(raw.strip()) <= 28:
        tiny = {
            "fix this.", "fix this", "is it okay?", "is it okay",
            "compare these.", "compare these", "why is this bad?", "why is this bad",
            "what's the issue here?", "what is the issue here",
            "check this one too", "check this one too."
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
    if re.search(r"\btop\s+five\b", low) or re.search(r"\btop\s+5\b", low):
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
    """Only triggers on EXPLICIT report generation requests."""
    return any(k in low for k in (
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
        "rank issues by severity",
        "generate a report",
        "generate dq report",
        "generate quality report",
        "generate data quality report",
        "create a report",
        "create dq report",
        "build a report",
        "give me a report",
        "show me a report",
        "produce a report",
    ))


# ---------------------------------------------------------------------------
# LAYER 2: Pure keyword classifier (no LLM, no cost)
# ---------------------------------------------------------------------------

def classify_intent(message: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Layer 2: keyword-based intent classification.

    Returns a result dict when a keyword pattern matches, or None when no
    keyword pattern matches (caller should escalate to LLM router).

    Safety checks (adversarial / OOD) run here too so they are enforced even
    when this function is called directly (e.g. from tests).
    """
    raw = (message or "").strip()
    if not raw:
        return None
    low = raw.lower()

    if low.startswith("select ") or low.startswith("insert ") or low.startswith("update "):
        return None
    if raw.strip().startswith("```"):
        return None

    # Safety checks (LAYER 1) run before anything else
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

    if has_assessment and len(low.split()) >= 4 and "?" in raw:
        return {"intent": 2, "reason": "nl_question_with_assessment"}

    # No keyword match → return None so caller escalates to LLM router
    return None


# ---------------------------------------------------------------------------
# LAYER 3: LLM router (fires only when keyword matching returns None)
# ---------------------------------------------------------------------------

def _classify_via_llm(message: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Layer 3: LLM-based intent classification.

    Tries LangChain ToolCallingAgent first, falls back to legacy LLM JSON router.
    Sends ~100 tokens (user message only). Never sends raw dataset rows.

    Returns:
        dict  — intent classified by LLM
        None  — LLM completely unavailable (both LangChain and legacy failed)
    """
    # Try LangChain ToolCallingAgent (PRIMARY)
    try:
        from agent.langchain_tool_router import classify_intent_via_langchain  # type: ignore
        result = classify_intent_via_langchain(message, context)
        if result is not None:
            logger.info(
                "[LLM-LAYER3] LangChain ToolCallingAgent → intent=%d source=%s",
                result.get("intent", -1),
                result.get("source", ""),
            )
            return result
    except Exception as exc:
        logger.warning("[LLM-LAYER3] LangChain ToolCallingAgent failed: %s — trying legacy fallback", exc)

    # Try Legacy LLM JSON router (FALLBACK)
    try:
        from agent.llm_router import llm_classify_intent  # type: ignore
        result = llm_classify_intent(message)
        if result is not None:
            logger.info(
                "[LLM-LAYER3] Legacy LLM JSON router → intent=%d source=%s",
                result.get("intent", -1),
                result.get("source", ""),
            )
            return result
    except Exception as exc:
        logger.warning("[LLM-LAYER3] Legacy LLM JSON router failed: %s", exc)

    return None


# ---------------------------------------------------------------------------
# Main entry point — full 3-layer pipeline
# ---------------------------------------------------------------------------

def classify_intent_full(message: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Full 3-layer agentic routing pipeline.

    Layer 1: Safety checks (adversarial / OOD) — rule-based, instant, zero cost
    Layer 2: Keyword matching — fast, free, covers known exact patterns
    Layer 3: LLM ToolCallingAgent → LLM JSON fallback → graceful unable-reply

    Returns a dict ALWAYS (never None). Guaranteed keys: intent, reason, source.
    Extra key `unable_reply` (str) is set when intent=7 and LLM determined OOS
    so chat_graph can surface a helpful "here's what I can do" message.

    This is the preferred entry point for chat_graph.py and any new code.
    Legacy code may still call classify_intent() directly (keyword-only, returns None on miss).
    """
    raw = (message or "").strip()
    if not raw:
        return {"intent": 6, "reason": "empty_message", "source": "pre_check"}

    low = raw.lower()

    # ── LAYER 1: Safety (always first, rule-based) ─────────────────────
    if _is_adversarial(low):
        logger.info("[ROUTER] LAYER1 adversarial blocked")
        return {"intent": 8, "reason": "adversarial_policy", "source": "safety_check"}

    if _is_ood(low):
        logger.info("[ROUTER] LAYER1 OOD blocked")
        return {
            "intent": 7,
            "reason": "out_of_domain_keyword",
            "source": "safety_check",
            "unable_reply": _UNABLE_REPLY,
        }

    # ── LAYER 2: Keyword matching ─────────────────────────────────────
    kw_result = classify_intent(message, context)
    if kw_result is not None:
        logger.info(
            "[ROUTER] LAYER2 keyword match → intent=%d reason=%s",
            kw_result.get("intent", -1),
            kw_result.get("reason", ""),
        )
        kw_result.setdefault("source", "keyword")
        return kw_result

    # ── LAYER 3: LLM router (keyword miss → escalate to LLM) ──────────
    logger.info("[ROUTER] LAYER2 keyword miss — escalating to LLM router for: %s", raw[:80])
    llm_result = _classify_via_llm(message, context)

    if llm_result is not None:
        intent = llm_result.get("intent", 7)
        if intent == 7:
            # LLM deliberately found no matching tool — surface helpful reply
            logger.info("[ROUTER] LAYER3 LLM → no matching tool, returning unable-reply")
            llm_result["unable_reply"] = _UNABLE_REPLY
        return llm_result

    # ── LAYER 3c: LLM completely unavailable → graceful degradation ─────
    logger.warning("[ROUTER] All layers failed (LLM unavailable) — returning unable-reply")
    return {
        "intent": 7,
        "reason": "all_layers_failed",
        "source": "graceful_degradation",
        "unable_reply": _UNABLE_REPLY,
    }
