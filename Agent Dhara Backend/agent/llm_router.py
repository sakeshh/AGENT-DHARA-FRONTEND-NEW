"""
llm_router.py
─────────────
LLM-powered intent classifier — Phase 2 upgrade for Agent Dhara.

Usage (inside conversational_intents.py):
    from agent.llm_router import llm_classify_intent
    result = llm_classify_intent(message)   # returns same dict as classify_intent()

Contract
  • Input  : ONLY the user message (max 500 chars). Never the report or dataset.
  • Output : {"intent": int, "reason": str}  —  same schema as classify_intent()
  • Failure: Returns {"intent": 6, "reason": "llm_error:..."}  — CLARIFY is safe fallback
  • This file is NEVER imported at module load time.
    chat_graph.py must NOT import it.  Only conversational_intents.py calls it,
    and only when keyword matching returns None AND an assessment exists.

Intent mapping (same as conversational_intents.py)
  1  REPORT_GENERATE   — user wants a full narrative / markdown / HTML report
  2  ISSUE_LIST        — user wants to see issues, problems, overview
  3  ISSUE_FILTER      — user wants a filtered view (only nulls, only duplicates …)
  4  TRIAGE            — user wants priority order / ETL readiness / fix order
  5  CROSS_DATASET     — user asks about table relationships, joins, cardinality
  6  CLARIFY           — vague / out-of-scope / needs more info (safe fallback)
  7  OUT_OF_SCOPE      — completely unrelated (stocks, coding help, general knowledge)
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ─── System prompt ────────────────────────────────────────────────────────────
_SYSTEM_PROMPT = """You are a post-assessment chat intent classifier for Agent Dhara (a data quality tool).

The user has already run a data quality assessment on their datasets.
They are now asking a follow-up question IN THE CHAT BAR.
Classify the user message into exactly one intent. Return ONLY valid JSON — no extra text.

Intents:
  1 = REPORT_GENERATE  → user wants a full narrative / markdown / HTML report
  2 = ISSUE_LIST       → user wants to see what's wrong, overview, top issues
  3 = ISSUE_FILTER     → user wants a filtered view: only nulls, only duplicates, only emails …
  4 = TRIAGE           → user asks what to fix first, priority order, ETL readiness
  5 = CROSS_DATASET    → user asks about table relationships, joins, foreign keys
  6 = CLARIFY          → message is vague, too short, unclear, or needs clarification
  7 = OUT_OF_SCOPE     → completely unrelated: coding help, stocks, sports, general knowledge

Output schema (JSON only):
{"intent": <number 1-7>, "reason": "<short reason, max 10 words>"}

Classification rules:
- Data issues, nulls, duplicates, column problems          → 2 or 3
- Priority, ETL risk, what to fix, production readiness    → 4
- Table joins, foreign keys, relationships, cardinality    → 5
- Report, summary, executive brief, narrative              → 1
- Vague, single word, unclear, deictic ("this", "it")     → 6
- Coding, stocks, weather, sports, cooking                 → 7
- When unsure → default to 6 (never 7 unless clearly off-topic)
"""


# ─── LLM call ────────────────────────────────────────────────────────────────

def llm_classify_intent(message: str) -> Dict[str, Any]:
    """
    Call the configured LLM to classify a user message into an intent.

    Sends ONLY the user message (max 500 chars) — never the report or dataset.
    On any failure returns intent 6 (CLARIFY) so the user gets a helpful prompt.
    """
    truncated = (message or "").strip()[:500]
    if not truncated:
        return {"intent": 6, "reason": "empty_message"}

    try:
        raw_response = _call_llm(truncated)
        return _parse_response(raw_response)
    except Exception as exc:
        logger.warning("llm_router: LLM call failed — %s: %s", type(exc).__name__, exc)
        return {"intent": 6, "reason": f"llm_error:{type(exc).__name__}"}


def _call_llm(message: str) -> str:
    """Try LangChain first, fall back to raw OpenAI client."""
    cfg = _load_config()
    if not cfg:
        raise RuntimeError("No LLM config found — cannot call LLM router")

    try:
        return _call_via_langchain(message, cfg)
    except ImportError:
        pass

    return _call_via_openai(message, cfg)


def _call_via_langchain(message: str, cfg: Dict[str, Any]) -> str:
    from langchain_core.messages import HumanMessage, SystemMessage  # noqa: PLC0415

    use_azure = bool(cfg.get("azure_endpoint") or cfg.get("api_base"))
    if use_azure:
        from langchain_openai import AzureChatOpenAI  # noqa: PLC0415
        llm = AzureChatOpenAI(
            azure_endpoint=cfg.get("azure_endpoint") or cfg.get("api_base"),
            azure_deployment=cfg.get("deployment_name") or cfg.get("model", "gpt-4o-mini"),
            api_version=cfg.get("api_version", "2024-02-01"),
            api_key=cfg.get("api_key"),
            temperature=0,
            max_tokens=80,
        )
    else:
        from langchain_openai import ChatOpenAI  # noqa: PLC0415
        llm = ChatOpenAI(
            model=cfg.get("model", "gpt-4o-mini"),
            api_key=cfg.get("api_key"),
            temperature=0,
            max_tokens=80,
        )

    response = llm.invoke([
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=message),
    ])
    return (response.content or "").strip()


def _call_via_openai(message: str, cfg: Dict[str, Any]) -> str:
    import openai  # noqa: PLC0415

    use_azure = bool(cfg.get("azure_endpoint") or cfg.get("api_base"))
    if use_azure:
        client = openai.AzureOpenAI(
            azure_endpoint=cfg.get("azure_endpoint") or cfg.get("api_base", ""),
            api_version=cfg.get("api_version", "2024-02-01"),
            api_key=cfg.get("api_key", ""),
        )
        model = cfg.get("deployment_name") or cfg.get("model", "gpt-4o-mini")
    else:
        client = openai.OpenAI(api_key=cfg.get("api_key", ""))
        model = cfg.get("model", "gpt-4o-mini")

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": message},
        ],
        temperature=0,
        max_tokens=80,
    )
    return (resp.choices[0].message.content or "").strip()


def _parse_response(raw: str) -> Dict[str, Any]:
    """Parse JSON from LLM response. Strips markdown fences if present."""
    text = raw.strip()

    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
        if text.lower().startswith("json"):
            text = text[4:]
    text = text.strip()

    parsed = json.loads(text)
    intent_id = int(parsed.get("intent", 6))
    reason    = str(parsed.get("reason", "llm_classified"))

    if intent_id not in range(1, 8):
        logger.warning("llm_router: unexpected intent %d — defaulting to CLARIFY", intent_id)
        return {"intent": 6, "reason": "llm_invalid_intent"}

    return {"intent": intent_id, "reason": f"llm:{reason}"}


def _load_config() -> Optional[Dict[str, Any]]:
    """Load LLM config via model_config.py — same config used by the rest of the app."""
    try:
        from agent.model_config import load_llm_config  # noqa: PLC0415
        return load_llm_config() or None
    except Exception as exc:
        logger.warning("llm_router: could not load model config — %s", exc)
        return None
