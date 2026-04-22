"""
Master agent for LangGraph-based orchestration.

Responsibilities:
- Maintains a registry of sub-agents (ExtractionAgent now; can be extended to Assessment/Transformation agents).
- Receives a user request and decides which steps to run (extract / assess / transform).
- Coordinates data flow between steps (via LangGraph state in `agent.langgraph_orchestrator`).

This module intentionally keeps the routing logic simple and deterministic:
we route based on keyword heuristics so the system is usable without an LLM.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_sources_config(path: str) -> Dict[str, Any]:
    """
    Load a sources config file (YAML preferred, JSON fallback).
    Returns the `source` dict (same as `main.py` / `agent.mcp_interface` expect).
    """
    text = _read_text(path)
    try:
        import yaml  # type: ignore

        raw = yaml.safe_load(text) or {}
    except Exception:
        import json

        raw = json.loads(text)
    src = raw.get("source", raw) if isinstance(raw, dict) else {}
    if not isinstance(src, dict):
        raise ValueError("Invalid sources config shape: expected mapping with key 'source'")
    src.setdefault("locations", [])
    return src


def _location_key(loc: Dict[str, Any], idx: int) -> str:
    """
    Stable key for user selection.
    Prefers explicit id/label/name, otherwise falls back to type+index.
    """
    for k in ("id", "label", "name"):
        v = loc.get(k)
        if v:
            return str(v)
    t = (loc.get("type") or "location").lower()
    return f"{t}:{idx}"


def select_locations(
    source_root: Dict[str, Any],
    selected: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Select locations from a loaded sources config.

    `selected` supports:
    - location `id`/`label`/`name` (recommended)
    - a type string (e.g. "database", "azure_blob", "filesystem")
    - a fallback "type:<index>" selector (e.g. "database:0")

    If `selected` is empty/None: returns all locations.
    """
    locs = list(source_root.get("locations", []) or [])
    if not selected:
        return locs

    def _norm(s: str) -> str:
        return str(s).strip().lower().replace("_", " ")

    raw_want = {_norm(s) for s in selected if str(s).strip()}
    # UI-friendly labels -> internal types
    # Matches planned UI options: "Azure SQL" / "Blob" / "Local" / "Stream"
    want = set(raw_want)
    # Treat "sql" as Azure SQL / database source selection
    if "sql" in want:
        want.add("azure sql")
        want.add("database")
    if "azure sql" in want:
        want.add("database")
        want.add("sql")
    if "blob" in want:
        want.add("azure blob")
        want.add("azure_blob")
        want.add("azure_blob_output")
    if "local" in want:
        want.add("filesystem")
        want.add("local fs")
        want.add("local_fs")
    if "stream" in want:
        want.add("stream")
    if not want:
        return locs

    out: List[Dict[str, Any]] = []
    for idx, loc in enumerate(locs):
        t = (loc.get("type") or "").lower()
        key = _location_key(loc, idx)
        if _norm(key) in want or _norm(t) in want:
            out.append(loc)
            continue
        # Explicit type:index selection
        if _norm(f"{t}:{idx}") in want:
            out.append(loc)
    return out


@dataclass(frozen=True)
class Plan:
    """
    A simple execution plan for the orchestrator.
    """

    do_extract: bool = True
    do_transform: bool = False


class MasterAgent:
    """
    Master agent that routes requests and coordinates sub-agents.
    """

    def __init__(self) -> None:
        from agent.extraction_agent import ExtractionAgent

        self.registry: Dict[str, Any] = {
            "extraction": ExtractionAgent(),
        }

    def plan(self, user_request: str) -> Plan:
        """
        Determine which sub-agents to trigger based on the user's request.
        """
        txt = (user_request or "").lower()
        wants_transform = any(
            k in txt
            for k in (
                "transform",
                "transformation",
                "clean",
                "cleaning",
                "suggest fixes",
                "generate rules",
                "generate transformation",
            )
        )
        return Plan(do_extract=True, do_transform=wants_transform)

    def infer_selected_sources_from_query(self, user_request: str) -> List[str]:
        """
        Infer which sources the user asked for directly in the query text.

        Examples:
        - "sql" -> ["sql"]
        - "extract from blob" -> ["blob"]
        - "sql and blob" -> ["sql", "blob"]

        If nothing is detected, returns [] (caller can treat as "all sources" or use explicit UI selection).
        """
        txt = (user_request or "").lower()
        detected: List[str] = []
        if any(k in txt for k in ("sql", "azure sql", "database")):
            detected.append("sql")
        if any(k in txt for k in ("blob", "azure blob", "storage")):
            detected.append("blob")
        if any(k in txt for k in ("local", "filesystem", "file system", "csv", "excel")):
            detected.append("local")
        if "stream" in txt:
            detected.append("stream")
        # preserve order and uniqueness
        out: List[str] = []
        for s in detected:
            if s not in out:
                out.append(s)
        return out

    def load_and_select_sources(
        self,
        *,
        sources_path: str = "config/sources.yaml",
        selected_sources: Optional[Sequence[str]] = None,
        user_request: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Load sources config and return (source_root, selected_locations).
        """
        if not os.path.isfile(sources_path):
            raise FileNotFoundError(f"sources config not found: {sources_path}")
        source_root = load_sources_config(sources_path)
        inferred = self.infer_selected_sources_from_query(user_request or "") if user_request else []
        merged: List[str] = []
        for s in list(selected_sources or []) + inferred:
            ss = str(s).strip()
            if ss and ss not in merged:
                merged.append(ss)
        locations = select_locations(source_root, merged or None)
        return source_root, locations

