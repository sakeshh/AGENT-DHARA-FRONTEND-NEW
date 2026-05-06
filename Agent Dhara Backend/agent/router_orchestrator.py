"""
Router Orchestrator — unified entry point for all intent routing.

Replaces direct calls to classify_intent() in chat_graph.py / langgraph_orchestrator.py.
Implements the layered routing strategy:

  Layer 1 → Adversarial / safety guard   (rule-based, unbypassable)
  Layer 2 → Keyword matching             (existing code, free, 0ms)
  Layer 3 → LLM Router                  (fallback, ~100 tokens)
  Layer 4 → Final fallback              (return None → handled upstream)

Usage:
    from agent.router_orchestrator import route_message

    result = route_message(user_message, context)
    # result = {"intent": int, "tool": str, "reason": str, "source": str}
    # result = None → no intent matched, caller should use generic fallback
"""
from __future__ import annotations
import logging
from typing import Any, Dict, Optional

from agent.conversational_intents import (
    classify_intent,
    fallback_router_intent,
    _is_adversarial,
    _is_ood,
)
from agent.llm_router import llm_classify_intent
from agent.agent_system_prompt import OUT_OF_SCOPE_REPLY, ADVERSARIAL_REPLY

logger = logging.getLogger(__name__)


def route_message(
    message: str,
    context: Dict[str, Any],
    use_llm_fallback: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Route a user message through all intent layers.

    Args:
        message:          Raw user message string.
        context:          Session context dict (must contain last_assessment_result).
        use_llm_fallback: Set False to disable LLM layer (testing / cost control).

    Returns:
        dict with keys: intent, tool, reason, source
        None if no layer matched.
    """
    if not message or not message.strip():
        return None

    low = message.lower().strip()

    # ── Layer 1: Safety guard (rule-based, never delegate to LLM) ────────────
    if _is_adversarial(low):
        logger.info("Router: adversarial detected")
        return {
            "intent": 8,
            "tool": "none",
            "reason": "adversarial_policy",
            "source": "safety_guard",
            "reply": ADVERSARIAL_REPLY,
        }

    if _is_ood(low):
        logger.info("Router: out-of-domain keyword detected")
        return {
            "intent": 7,
            "tool": "none",
            "reason": "out_of_domain_keyword",
            "source": "safety_guard",
            "reply": OUT_OF_SCOPE_REPLY,
        }

    # ── Layer 2a: Primary keyword classifier ─────────────────────────────────
    result = classify_intent(message, context)
    if result:
        result.setdefault("source", "keyword")
        logger.info("Router: keyword match → intent=%d", result.get("intent"))
        return result

    # ── Layer 2b: Fallback keyword heuristics ────────────────────────────────
    result = fallback_router_intent(message, context)
    if result:
        result.setdefault("source", "keyword_fallback")
        logger.info("Router: keyword fallback → intent=%d", result.get("intent"))
        return result

    # ── Layer 3: LLM Router ───────────────────────────────────────────────────
    if use_llm_fallback:
        logger.info("Router: keyword missed, calling LLM router")
        result = llm_classify_intent(message)
        if result:
            # If LLM says out-of-scope, attach the standard reply
            if result.get("tool") == "none":
                result["reply"] = OUT_OF_SCOPE_REPLY
            return result

    # ── Layer 4: No match ────────────────────────────────────────────────────
    logger.info("Router: no layer matched for message: %s", message[:80])
    return None


def route_and_get_reply(
    specialist_fn,
    message: str,
    context: Dict[str, Any],
    assessment: Dict[str, Any],
    use_llm_formatter: bool = True,
) -> str:
    """
    Convenience wrapper: route → call specialist → optionally format with LLM.

    Args:
        specialist_fn:    The specialist function to call (takes assessment, message).
        message:          Raw user message.
        context:          Session context.
        assessment:       The DQ assessment result dict.
        use_llm_formatter: Whether to pass specialist output through LLM formatter.

    Returns:
        Final reply string.
    """
    raw = specialist_fn(assessment, message)

    if use_llm_formatter and raw:
        try:
            from agent.llm_formatter import format_specialist_output
            return format_specialist_output(raw, message)
        except Exception as exc:
            logger.warning("Formatter error, using raw output: %s", exc)

    return raw
