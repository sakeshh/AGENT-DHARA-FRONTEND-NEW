"""
LangGraph-based multi-agent orchestration for Agent Dhara Backend.

This module defines a small LangGraph workflow:
- Route user request (MasterAgent.plan)
- Extract per selected source location (ExtractionAgent, parallel)
- Optionally generate transformation suggestions/rules (existing transformation agent)

The workflow is designed to be callable from:
- CLI glue code (future)
- FastAPI endpoints (future)
- other Python code (unit tests, scripts)
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Sequence, TypedDict

try:
    from langgraph.graph import END, StateGraph
except Exception as e:  # pragma: no cover
    END = None  # type: ignore
    StateGraph = None  # type: ignore
    _LANGGRAPH_IMPORT_ERROR = e
else:
    _LANGGRAPH_IMPORT_ERROR = None

from agent.master_agent import MasterAgent


class OrchestratorState(TypedDict, total=False):
    """
    Shared state passed between LangGraph nodes.
    """

    # Inputs
    user_request: str
    sources_path: str
    selected_sources: List[str]
    stream_records: List[Dict[str, Any]]
    stream_name: str

    # Derived / intermediate
    plan: Dict[str, Any]
    selected_location_count: int

    # Outputs
    extractions: List[Dict[str, Any]]
    extraction_errors: List[Dict[str, Any]]
    transformation: Dict[str, Any]


def _node_route(state: OrchestratorState) -> OrchestratorState:
    master = MasterAgent()
    p = master.plan(state.get("user_request", ""))
    return {
        "plan": {"do_extract": p.do_extract, "do_transform": p.do_transform},
    }


async def _node_extract_async(state: OrchestratorState) -> OrchestratorState:
    master = MasterAgent()
    source_root, locations = master.load_and_select_sources(
        sources_path=state.get("sources_path", "config/sources.yaml"),
        selected_sources=state.get("selected_sources") or None,
        user_request=state.get("user_request") or "",
    )

    extraction_agent = master.registry["extraction"]
    results, errors = await extraction_agent.extract_many(
        source_root=source_root,
        locations=locations,
        parallel=True,
        stream_records=state.get("stream_records"),
        stream_name=state.get("stream_name") or "stream",
    )

    # Normalize to JSON-serializable output (no dataclasses)
    extractions_out: List[Dict[str, Any]] = []
    for r in results:
        extractions_out.append(
            {
                "source": r.source_name,
                "location_type": r.location_type,
                "result": r.result,
            }
        )

    return {
        "selected_location_count": len(locations),
        "extractions": extractions_out,
        "extraction_errors": errors,
    }


def _node_extract(state: OrchestratorState) -> OrchestratorState:
    """
    Sync wrapper around the async extraction node for LangGraph.
    """
    return asyncio.run(_node_extract_async(state))


def _node_transform(state: OrchestratorState) -> OrchestratorState:
    """
    Use the existing transformation agent helper to generate suggested + LLM rules.

    We aggregate extraction results by running transformation per extraction result.
    This keeps failures isolated per source.
    """
    plan = state.get("plan") or {}
    if not plan.get("do_transform"):
        return {"transformation": {"skipped": True}}

    from agent.transformation_agent import generate_transformation_rules_with_suggestions

    outputs: List[Dict[str, Any]] = []
    for ex in state.get("extractions", []) or []:
        try:
            assessment_result = ex.get("result") or {}
            out = generate_transformation_rules_with_suggestions(assessment_result, language="sql")
            outputs.append({"source": ex.get("source"), "output": out, "success": True})
        except Exception as e:
            outputs.append({"source": ex.get("source"), "error": str(e), "success": False})

    return {"transformation": {"per_source": outputs}}


def build_orchestrator_graph():
    """
    Build and compile the LangGraph orchestrator.
    """
    if _LANGGRAPH_IMPORT_ERROR is not None or StateGraph is None:
        raise ImportError(
            "LangGraph is not installed (or failed to import). "
            "Install with: pip install -r requirements.txt"
        ) from _LANGGRAPH_IMPORT_ERROR
    g = StateGraph(OrchestratorState)
    g.add_node("route", _node_route)
    g.add_node("extract", _node_extract)
    g.add_node("transform", _node_transform)

    g.set_entry_point("route")
    g.add_edge("route", "extract")
    g.add_edge("extract", "transform")
    g.add_edge("transform", END)
    return g.compile()


def run_orchestrator(
    *,
    user_request: str,
    sources_path: str = "config/sources.yaml",
    selected_sources: Optional[Sequence[str]] = None,
    stream_records: Optional[List[Dict[str, Any]]] = None,
    stream_name: str = "stream",
) -> Dict[str, Any]:
    """
    High-level convenience wrapper.
    """
    graph = build_orchestrator_graph()
    final = graph.invoke(
        {
            "user_request": user_request,
            "sources_path": sources_path,
            "selected_sources": list(selected_sources or []),
            "stream_records": stream_records,
            "stream_name": stream_name,
        }
    )
    return dict(final)

