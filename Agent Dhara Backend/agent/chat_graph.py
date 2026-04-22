"""
3-agent chat workflow (LangGraph):
- MasterChatAgent: uses an LLM to interpret user intent and parameters (no keyword heuristics)
- ExtractAgent: list sources/tables, show schema/preview/query (via connectors/MCP adapters)
- DataQualityAgent: run DQ checks and generate a report for selected dataset/table

The LLM produces a structured JSON "action plan" which is then executed deterministically.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, TypedDict, Tuple

from agent.master_agent import load_sources_config
from agent.model_config import load_llm_config
from agent.session_store import add_experience, list_recent_experiences, load_session, save_session

try:
    from langgraph.graph import END, StateGraph
except Exception:  # pragma: no cover
    END = None  # type: ignore
    StateGraph = None  # type: ignore


class ChatState(TypedDict, total=False):
    session_id: str
    message: str
    session: Dict[str, Any]
    action: str
    action_args: Dict[str, Any]
    reply: str
    payload: Dict[str, Any]


_MASTER_SYSTEM = """You are the Master (Supervisor) agent for a data exploration + data quality assistant.
You MUST return ONLY valid JSON and nothing else.

Your job:
- Understand the user request in natural language.
- Decide what action to take next.
- Provide the minimal arguments needed to execute it.

Allowed actions (exact strings):
help
reset_session
list_sources
select_source
list_tables
select_tables
select_table
show_schema
preview_table
row_detail
nl_query
dq_table
list_blob_files
select_blob_files
assess_selected_files
list_local_files
select_local_files
assess_selected_local_files
assess_selected_tables

Output schema:
{
  "action": "<one allowed action>",
  "args": { ... }
}

Argument rules:
- Prefer explicit names (table/blob/file) when possible.
- If the user references a specific name (table/blob/file), you may pass it directly by name.
- Never invent sources/tables/files that are not listed in the provided context.

Behavior rules:
- If the user asks to "run data quality assessment" or "check data quality issues" for the *currently selected blob files*,
  choose action=assess_selected_files.
- If the user asks to assess the *currently selected local files*, choose action=assess_selected_local_files.
- If the user asks to assess the *currently selected tables*, choose action=assess_selected_tables.

Examples (JSON only):
{"action":"list_sources","args":{}}
{"action":"reset_session","args":{}}
{"action":"select_source","args":{"index":0}}
{"action":"list_tables","args":{}}
{"action":"select_tables","args":{"indices":[1,3,4]}}
{"action":"assess_selected_tables","args":{}}
{"action":"row_detail","args":{"row_number":50}}
{"action":"list_blob_files","args":{}}
{"action":"select_blob_files","args":{"all":true}}
{"action":"assess_selected_files","args":{}}
"""


UI_ACTION_PREFIX = "__ui__:"


def _ui_action_options(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Standard UI option schema consumed by the frontend.
    Each option:
      { id, label, action, args }
    """
    out: List[Dict[str, Any]] = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        label = str(it.get("label") or "").strip()
        action = str(it.get("action") or "").strip()
        args = it.get("args") if isinstance(it.get("args"), dict) else {}
        if not label or not action:
            continue
        out.append(
            {
                "id": str(it.get("id") or f"{action}:{label}")[:120],
                "label": label,
                "action": action,
                "args": args,
            }
        )
    return out


def _try_parse_ui_action(text: str) -> Optional[Dict[str, Any]]:
    """
    Deterministic bypass for button clicks.
    Expected format: "__ui__:{...json...}" where JSON matches {"action": "...", "args": {...}}.
    """
    t = (text or "").strip()
    if not t.startswith(UI_ACTION_PREFIX):
        return None
    raw = t[len(UI_ACTION_PREFIX) :].strip()
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    action = str(obj.get("action") or "").strip()
    args = obj.get("args")
    if not isinstance(args, dict):
        args = {}
    if not action:
        return None
    return {"action": action, "args": args}


def _render_report_markdown(result: Dict[str, Any]) -> Optional[str]:
    """
    Render a formal report as Markdown when report builder is available.
    """
    try:
        import main as _main  # type: ignore

        if hasattr(_main, "build_markdown_report"):
            return _main.build_markdown_report(result)  # type: ignore
    except Exception:
        return None
    return None


def _llm_plan(*, user_text: str, session: Dict[str, Any]) -> Dict[str, Any]:
    cfg = load_llm_config(purpose="router")
    if not cfg:
        raise RuntimeError(
            "LLM routing is required but not configured. Set AZURE_OPENAI_ENDPOINT/AZURE_OPENAI_API_KEY/"
            "AZURE_OPENAI_DEPLOYMENT (and optional AZURE_OPENAI_API_VERSION) for Foundry/Azure OpenAI."
        )
    user = (user_text or "").strip()
    if not user:
        return {"action": "help", "args": {}}

    ctx = (session or {}).get("context", {}) if isinstance(session, dict) else {}
    sid = str((session or {}).get("session_id") or "default")
    # Keep context compact but useful for the model.
    context_summary = {
        "selected_source_index": ctx.get("selected_source_index"),
        "selected_db_location_index": ctx.get("selected_db_location_index"),
        "selected_blob_location_index": ctx.get("selected_blob_location_index"),
        "selected_fs_location_index": ctx.get("selected_fs_location_index"),
        "selected_table": ctx.get("selected_table"),
        "selected_tables_count": len(ctx.get("selected_tables") or []),
        "selected_blob_files_count": len(ctx.get("selected_blob_files") or []),
        "selected_local_files_count": len(ctx.get("selected_local_files") or []),
    }

    def _head(lst: Any, n: int = 30) -> Any:
        if not isinstance(lst, list):
            return None
        return lst[:n]

    available_lists = {
        "last_table_list_head": _head(ctx.get("last_table_list"), 40),
        "last_blob_list_head": _head(ctx.get("last_blob_list"), 40),
        "last_local_file_list_head": _head(ctx.get("last_local_file_list"), 40),
    }

    memory = {
        "memory_summary": ctx.get("memory_summary"),
        "recent_experiences": list(reversed(list_recent_experiences(session_id=sid, limit=10))),
    }

    prompt = json.dumps(
        {
            "user_message": user,
            "context": context_summary,
            "available": available_lists,
            "memory": memory,
        },
        ensure_ascii=False,
    )
    try:
        if cfg.provider == "azure_openai":
            from openai import AzureOpenAI  # type: ignore

            client = AzureOpenAI(api_key=cfg.api_key, api_version=cfg.api_version or "2024-02-01", azure_endpoint=cfg.endpoint)
            resp = client.chat.completions.create(
                model=cfg.model,
                messages=[{"role": "system", "content": _MASTER_SYSTEM}, {"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=220,
            )
        else:
            from openai import OpenAI  # type: ignore

            client = OpenAI(api_key=cfg.api_key)
            resp = client.chat.completions.create(
                model=cfg.model,
                messages=[{"role": "system", "content": _MASTER_SYSTEM}, {"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=220,
            )
        raw = (resp.choices[0].message.content or "").strip()
        obj = json.loads(raw)
        action = str(obj.get("action") or "").strip()
        args = obj.get("args")
        if not isinstance(args, dict):
            args = {}
        if not action:
            return {"action": "help", "args": {}}
        return {"action": action, "args": args}
    except Exception as e:
        # If the model fails to produce JSON, return a helpful message via 'help'
        return {"action": "help", "args": {"error": str(e)}}


def _node_load_session(state: ChatState) -> ChatState:
    sid = (state.get("session_id") or "default").strip() or "default"
    sess = load_session(sid)
    return {"session_id": sid, "session": sess}


def _node_route(state: ChatState) -> ChatState:
    direct = _try_parse_ui_action(state.get("message", "") or "")
    if direct:
        return {"action": str(direct.get("action") or "help"), "action_args": dict(direct.get("args") or {})}
    plan = _llm_plan(user_text=state.get("message", ""), session=state.get("session") or {})
    return {"action": str(plan.get("action") or "help"), "action_args": dict(plan.get("args") or {})}


def _node_help(state: ChatState) -> ChatState:
    err = (state.get("action_args") or {}).get("error")
    if err:
        return {"reply": f"I had trouble interpreting that. Please rephrase. (router_error={err})", "payload": {"options": []}}

    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    locs = source_root.get("locations", []) or []
    opts = []
    for i, loc in enumerate(locs):
        typ = (loc.get("type") or "").strip() or "source"
        sid = (loc.get("id") or loc.get("label") or loc.get("name") or "").strip()
        label = f"{typ}" + (f" — {sid}" if sid else "")
        opts.append({"id": f"src:{i}", "label": label, "action": "select_source", "args": {"index": i}})

    base_opts = [{"id": "reset", "label": "Start over", "action": "reset_session", "args": {}}]
    return {
        "reply": "Tell me what you want to do, or pick a data source to start.",
        "payload": {"options": _ui_action_options(base_opts + opts)},
    }


def _node_reset_session(state: ChatState) -> ChatState:
    """
    Clear current selections and cached lists so user can start from the beginning.
    """
    sess = state.get("session") or {}
    ctx = sess.get("context") if isinstance(sess.get("context"), dict) else {}
    sources_path = (ctx.get("sources_path") or "config/sources.yaml") if isinstance(ctx, dict) else "config/sources.yaml"

    # Reset session state
    sess["messages"] = []
    sess["context"] = {"sources_path": sources_path}

    # Reuse help flow to return initial source buttons
    state = {"session_id": state.get("session_id") or "default", "session": sess, "action_args": {}}
    return _node_help(state)  # type: ignore


def _node_list_sources(state: ChatState) -> ChatState:
    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    locs = source_root.get("locations", []) or []
    out = []
    for i, loc in enumerate(locs):
        out.append(
            {
                "index": i,
                "id": loc.get("id") or loc.get("label") or loc.get("name"),
                "type": loc.get("type"),
            }
        )
    opts = []
    for x in out:
        label = f"{x.get('type')}" + (f" — {x.get('id')}" if x.get("id") else "")
        opts.append({"id": f"src:{x['index']}", "label": label, "action": "select_source", "args": {"index": x["index"]}})
    return {"reply": "Select a data source.", "payload": {"sources": out, "options": _ui_action_options(opts)}}


def _node_select_source(state: ChatState) -> ChatState:
    """
    Select a specific source location (by index from 'show sources').

    Currently used to choose which Azure Blob container index to list files from.
    """
    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    locs = list(source_root.get("locations", []) or [])
    args = state.get("action_args") or {}
    idx = args.get("index")
    if idx is None:
        return {"reply": "Which source should I select? Provide an index (run 'show sources' first).", "payload": {}}
    try:
        idx = int(idx)
    except Exception:
        return {"reply": "Invalid source index. Provide a number from the sources list.", "payload": {}}
    if idx < 0 or idx >= len(locs):
        return {"reply": f"Source index out of range (0..{len(locs)-1}).", "payload": {}}
    loc = locs[idx]
    ctx = state["session"].setdefault("context", {})
    ctx["selected_source_index"] = idx
    # If it's a blob source, track blob location index among azure_blob entries too.
    if (loc.get("type") or "").lower() == "azure_blob":
        blob_locs = _azure_blob_locations(source_root)
        # Map absolute location index -> azure_blob index
        blob_abs = [i for i, l in enumerate(locs) if (l.get("type") or "").lower() == "azure_blob"]
        if idx in blob_abs:
            ctx["selected_blob_location_index"] = blob_abs.index(idx)
    # If it's a database source, track db location index among database entries.
    if (loc.get("type") or "").lower() == "database":
        db_abs = [i for i, l in enumerate(locs) if (l.get("type") or "").lower() == "database"]
        if idx in db_abs:
            ctx["selected_db_location_index"] = db_abs.index(idx)
    # If it's a filesystem source, track fs location index among filesystem entries.
    if (loc.get("type") or "").lower() == "filesystem":
        fs_abs = [i for i, l in enumerate(locs) if (l.get("type") or "").lower() == "filesystem"]
        if idx in fs_abs:
            ctx["selected_fs_location_index"] = fs_abs.index(idx)
    typ = (loc.get("type") or "").lower()
    sid = (loc.get("id") or loc.get("label") or "no-id")
    next_opts: List[Dict[str, Any]] = []
    if typ == "database":
        next_opts = [{"id": "sql:list", "label": "List tables", "action": "list_tables", "args": {}}]
    elif typ == "azure_blob":
        next_opts = [{"id": "blob:list", "label": "List files", "action": "list_blob_files", "args": {}}]
    elif typ == "filesystem":
        next_opts = [{"id": "fs:list", "label": "List local files", "action": "list_local_files", "args": {}}]
    reply = f"Selected source: **{typ}** ({sid}). Choose what to do next."
    return {"reply": reply, "payload": {"selected_source_index": idx, "options": _ui_action_options(next_opts)}}


def _azure_blob_locations(source_root: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "azure_blob"]


def _node_list_blob_files(state: ChatState) -> ChatState:
    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    blob_locs = _azure_blob_locations(source_root)
    if not blob_locs:
        return {"reply": "No Azure Blob source configured in sources.yaml.", "payload": {}}
    # Use selected blob location if previously chosen; default to first.
    blob_loc_idx = int(state["session"].get("context", {}).get("selected_blob_location_index") or 0)
    blob_loc_idx = max(0, min(blob_loc_idx, len(blob_locs) - 1))
    conn_cfg = blob_locs[blob_loc_idx].get("connection") or {}
    from connectors.azure_blob_storage import AzureBlobStorageConnector

    conn = AzureBlobStorageConnector(conn_cfg)
    names = sorted(conn.list_blobs())
    ctx = state["session"].setdefault("context", {})
    ctx["last_blob_list"] = names
    ctx["selected_blob_location_index"] = blob_loc_idx
    if not names:
        return {"reply": "No blobs found in the selected container.", "payload": {"files": [], "count": 0, "options": []}}
    opts = [{"id": "blob:all", "label": "Select all files", "action": "select_blob_files", "args": {"all": True}}]
    for n in names[:30]:
        opts.append({"id": f"blob:{n}", "label": n, "action": "select_blob_files", "args": {"names": [n]}})
    reply = f"Found **{len(names)}** blob file(s). Select file(s) to assess."
    return {"reply": reply, "payload": {"files": names, "count": len(names), "location_index": blob_loc_idx, "options": _ui_action_options(opts)}}


def _node_select_blob_files(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    available = ctx.get("last_blob_list") or []
    if not available:
        return {"reply": "No blob list cached. Run 'list files' first.", "payload": {}}
    args = state.get("action_args") or {}
    if args.get("all") is True:
        selected = list(available)
    else:
        names = args.get("names")
        indices = args.get("indices")
        selected = []
        if isinstance(names, list):
            selected = [str(n) for n in names if str(n) in available]
        elif isinstance(indices, list):
            for i in indices:
                try:
                    j = int(i) - 1
                except Exception:
                    continue
                if 0 <= j < len(available):
                    selected.append(str(available[j]))
        if not selected:
            return {"reply": "Tell me which files to select (by indices or exact names) after running 'list files'.", "payload": {}}
    ctx["selected_blob_files"] = selected
    next_opts = [
        {"id": "blob:assess", "label": "Run data quality assessment", "action": "assess_selected_files", "args": {}},
        {"id": "blob:list", "label": "List files again", "action": "list_blob_files", "args": {}},
    ]
    reply = f"Selected **{len(selected)}** blob file(s)."
    return {"reply": reply, "payload": {"selected_files": selected, "count": len(selected), "options": _ui_action_options(next_opts)}}


def _filesystem_locations(source_root: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "filesystem"]


def _node_list_local_files(state: ChatState) -> ChatState:
    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    fs_locs = _filesystem_locations(source_root)
    if not fs_locs:
        return {"reply": "No filesystem source configured in sources.yaml.", "payload": {}}
    fs_idx = int(state["session"].get("context", {}).get("selected_fs_location_index") or 0)
    fs_idx = max(0, min(fs_idx, len(fs_locs) - 1))
    root = fs_locs[fs_idx].get("path") or ""
    import os

    root_abs = os.path.abspath(root) if os.path.isabs(root) else os.path.abspath(os.path.join(os.getcwd(), root))
    if not os.path.isdir(root_abs):
        return {"reply": f"Filesystem path not found: {root_abs}", "payload": {}}
    files = sorted([f for f in os.listdir(root_abs) if os.path.isfile(os.path.join(root_abs, f))])
    ctx = state["session"].setdefault("context", {})
    ctx["last_local_file_list"] = files
    ctx["local_files_root"] = root_abs
    ctx["selected_fs_location_index"] = fs_idx
    opts = [{"id": "fs:all", "label": "Select all local files", "action": "select_local_files", "args": {"all": True}}]
    for n in files[:30]:
        opts.append({"id": f"fs:{n}", "label": n, "action": "select_local_files", "args": {"names": [n]}})
    reply = f"Found **{len(files)}** local file(s) under `{root_abs}`. Select file(s) to assess."
    return {"reply": reply, "payload": {"files": files, "count": len(files), "root": root_abs, "location_index": fs_idx, "options": _ui_action_options(opts)}}


def _node_select_local_files(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    available = ctx.get("last_local_file_list") or []
    if not available:
        return {"reply": "No local file list cached. Run 'list local files' first.", "payload": {}}
    args = state.get("action_args") or {}
    if args.get("all") is True:
        selected = list(available)
    else:
        names = args.get("names")
        indices = args.get("indices")
        selected = []
        if isinstance(names, list):
            selected = [str(n) for n in names if str(n) in available]
        elif isinstance(indices, list):
            for i in indices:
                try:
                    j = int(i) - 1
                except Exception:
                    continue
                if 0 <= j < len(available):
                    selected.append(str(available[j]))
        if not selected:
            return {"reply": "Tell me which local files to select (by indices or exact names) after running 'list local files'.", "payload": {}}
    ctx["selected_local_files"] = selected
    next_opts = [
        {"id": "fs:assess", "label": "Run data quality assessment", "action": "assess_selected_local_files", "args": {}},
        {"id": "fs:list", "label": "List local files again", "action": "list_local_files", "args": {}},
    ]
    return {"reply": f"Selected **{len(selected)}** local file(s).", "payload": {"selected_local_files": selected, "count": len(selected), "options": _ui_action_options(next_opts)}}


def _node_assess_selected_local_files(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    selected = ctx.get("selected_local_files") or []
    root = ctx.get("local_files_root") or ""
    if not selected or not root:
        return {"reply": "No local files selected. Use 'list local files' then 'select local files ...' first.", "payload": {}}
    import os
    import json
    import pandas as pd
    from agent.intelligent_data_assessment import load_and_profile

    # No hard default limits; set env ASSESS_MAX_ROWS_PER_LOCAL_FILE to cap.
    raw = (os.environ.get("ASSESS_MAX_ROWS_PER_LOCAL_FILE") or "").strip()
    max_rows = int(raw) if raw else 0
    if max_rows < 0:
        max_rows = 0

    dfs = {}
    for name in selected:
        p = os.path.join(root, name)
        if not os.path.isfile(p):
            return {"reply": f"File not found: {p}", "payload": {"file": name}}
        low = p.lower()
        nrows = (max_rows if max_rows > 0 else None)
        if low.endswith(".csv"):
            df = pd.read_csv(p, low_memory=False, nrows=nrows)
        elif low.endswith(".tsv"):
            df = pd.read_csv(p, sep="\t", low_memory=False, nrows=nrows)
        elif low.endswith(".jsonl"):
            rows = []
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        rows.append({"value": line})
                    if max_rows > 0 and len(rows) >= max_rows:
                        break
            df = pd.json_normalize(rows, max_level=1) if rows else pd.DataFrame()
        else:
            # full read for other formats
            if low.endswith((".xlsx", ".xls")):
                df = pd.read_excel(p)
            elif low.endswith(".parquet"):
                df = pd.read_parquet(p)
            else:
                df = pd.read_json(p)
        dfs[name] = df
    result = load_and_profile({"name": "local", "locations": []}, additional_data=dfs)
    sampled = f" (sampled up to {max_rows} rows/file where applicable)" if max_rows > 0 else ""
    from agent.deterministic_report_formatter import format_assessment_report
    reply = format_assessment_report(result)
    return {
        "reply": reply,
        "payload": {
            "selected_local_files": selected,
            "result": result,
            "options": _ui_action_options([{"id": "reset", "label": "Start over", "action": "reset_session", "args": {}}]),
        },
    }


def _node_assess_selected_files(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    selected = ctx.get("selected_blob_files") or []
    if not selected:
        return {"reply": "No files selected. Use 'list files' then 'select files ...' first.", "payload": {}}
    sources_path = ctx.get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    blob_locs = _azure_blob_locations(source_root)
    if not blob_locs:
        return {"reply": "No Azure Blob source configured in sources.yaml.", "payload": {}}
    blob_loc_idx = int(ctx.get("selected_blob_location_index") or 0)
    blob_loc_idx = max(0, min(blob_loc_idx, len(blob_locs) - 1))

    # Build a minimal config text with the blob location, then load only selected blobs.
    from agent.mcp_clients import _single_location_config  # type: ignore
    from agent.mcp_interface import load_selected_blob_datasets, run_assessment

    # No hard default limits; set env ASSESS_MAX_ROWS_PER_BLOB / ASSESS_MAX_BLOB_BYTES to cap.
    import os
    raw_rows = (os.environ.get("ASSESS_MAX_ROWS_PER_BLOB") or "").strip()
    raw_bytes = (os.environ.get("ASSESS_MAX_BLOB_BYTES") or "").strip()
    max_rows = int(raw_rows) if raw_rows else None
    max_bytes = int(raw_bytes) if raw_bytes else None

    cfg_text = _single_location_config({"name": source_root.get("name") or "source"}, blob_locs[blob_loc_idx])
    dfs = load_selected_blob_datasets(
        cfg_text,
        location_index=0,
        blob_names=list(selected),
        max_rows=max_rows,
        max_bytes=max_bytes,
    )
    # Run assessment purely over the loaded blobs (via additional_data).
    result = run_assessment(cfg_text, additional_data=dfs)
    from agent.deterministic_report_formatter import format_assessment_report
    reply = format_assessment_report(result)
    return {
        "reply": reply,
        "payload": {
            "selected_files": selected,
            "result": result,
            "options": _ui_action_options([{"id": "reset", "label": "Start over", "action": "reset_session", "args": {}}]),
        },
    }


def _node_list_tables(state: ChatState) -> ChatState:
    # List tables for selected database source (default: first).
    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    db_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "database"]
    if not db_locs:
        return {"reply": "No database source configured.", "payload": {}}
    db_idx = int(state["session"].get("context", {}).get("selected_db_location_index") or 0)
    db_idx = max(0, min(db_idx, len(db_locs) - 1))
    conn_cfg = db_locs[db_idx].get("connection") or {}
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector

    conn = AzureSQLPythonNetConnector(conn_cfg)
    tables = conn.discover_tables()
    ctx = state["session"].setdefault("context", {})
    ctx["last_table_list"] = tables
    ctx["selected_db_location_index"] = db_idx
    opts = [{"id": "sql:select_all", "label": "Select all tables", "action": "select_tables", "args": {"all": True}}]
    for t in tables[:40]:
        opts.append({"id": f"sql:{t}", "label": t, "action": "select_table", "args": {"name": t}})
    reply = f"Found **{len(tables)}** table(s). Select a table to explore, or select tables for assessment."
    return {"reply": reply, "payload": {"tables": tables, "location_index": db_idx, "options": _ui_action_options(opts)}}


def _node_select_tables(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    available = ctx.get("last_table_list") or []
    if not available:
        return {"reply": "No table list cached. Run 'list tables' first.", "payload": {}}
    args = state.get("action_args") or {}
    if args.get("all") is True:
        selected = list(available)
    else:
        names = args.get("names")
        indices = args.get("indices")
        selected = []
        if isinstance(names, list):
            selected = [str(n) for n in names if str(n) in available]
        elif isinstance(indices, list):
            for i in indices:
                try:
                    j = int(i) - 1
                except Exception:
                    continue
                if 0 <= j < len(available):
                    selected.append(str(available[j]))
        if not selected:
            return {"reply": "Tell me which tables to select (by indices or exact names) after running 'list tables'.", "payload": {}}
    ctx["selected_tables"] = selected
    next_opts = [
        {"id": "sql:assess", "label": "Run data quality assessment", "action": "assess_selected_tables", "args": {}},
        {"id": "sql:list", "label": "List tables again", "action": "list_tables", "args": {}},
    ]
    return {"reply": f"Selected **{len(selected)}** table(s).", "payload": {"selected_tables": selected, "count": len(selected), "options": _ui_action_options(next_opts)}}


def _node_assess_selected_tables(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    selected = ctx.get("selected_tables") or []
    if not selected:
        return {"reply": "No tables selected. Use 'list tables' then 'select tables ...' first.", "payload": {}}
    sources_path = ctx.get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    db_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "database"]
    if not db_locs:
        return {"reply": "No database source configured.", "payload": {}}
    db_idx = int(ctx.get("selected_db_location_index") or 0)
    db_idx = max(0, min(db_idx, len(db_locs) - 1))
    conn_cfg = db_locs[db_idx].get("connection") or {}
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
    from agent.intelligent_data_assessment import load_and_profile
    import os

    # No hard default limits; set env ASSESS_MAX_ROWS_PER_TABLE to cap.
    raw = (os.environ.get("ASSESS_MAX_ROWS_PER_TABLE") or "").strip()
    max_rows = int(raw) if raw else 0
    if max_rows < 0:
        max_rows = 0

    conn = AzureSQLPythonNetConnector(conn_cfg)
    rows = (max_rows if max_rows > 0 else 10_000)
    dfs = {t: conn.preview_table(t, rows) for t in selected}
    result = load_and_profile({"name": source_root.get("name") or "source", "locations": []}, additional_data=dfs)
    from agent.deterministic_report_formatter import format_assessment_report
    reply = format_assessment_report(result)
    return {
        "reply": reply,
        "payload": {
            "selected_tables": selected,
            "result": result,
            "options": _ui_action_options([{"id": "reset", "label": "Start over", "action": "reset_session", "args": {}}]),
        },
    }


def _node_select_table(state: ChatState) -> ChatState:
    args = state.get("action_args") or {}
    tname = args.get("name") or args.get("table")
    if not tname:
        return {"reply": "Tell me which table to use (exact name from 'list tables').", "payload": {}}
    ctx = state["session"].setdefault("context", {})
    ctx["selected_table"] = str(tname)
    next_opts = [
        {"id": "tbl:schema", "label": "Show schema", "action": "show_schema", "args": {}},
        {"id": "tbl:preview", "label": "Preview rows", "action": "preview_table", "args": {}},
        {"id": "tbl:dq", "label": "Data quality (sample)", "action": "dq_table", "args": {}},
    ]
    return {
        "reply": f"Selected table: **{tname}**. Choose what to do next.",
        "payload": {"selected_table": str(tname), "options": _ui_action_options(next_opts)},
    }


def _node_show_schema(state: ChatState) -> ChatState:
    table = state["session"].get("context", {}).get("selected_table")
    if not table:
        return {"reply": "No table selected. Use 'select table <schema.table>' first.", "payload": {}}
    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    db_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "database"]
    if not db_locs:
        return {"reply": "No database source configured.", "payload": {}}
    ctx = state["session"].setdefault("context", {})
    db_idx = int(ctx.get("selected_db_location_index") or 0)
    db_idx = max(0, min(db_idx, len(db_locs) - 1))
    conn_cfg = db_locs[db_idx].get("connection") or {}
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector

    conn = AzureSQLPythonNetConnector(conn_cfg)
    cols = conn.get_table_schema(table)
    lines = [f"- {c['name']}: {c['type']} nullable={c['nullable']}" for c in cols]
    return {"reply": f"Schema for {table}:\n" + "\n".join(lines), "payload": {"schema": cols}}


def _node_preview_table(state: ChatState) -> ChatState:
    table = state["session"].get("context", {}).get("selected_table")
    if not table:
        return {"reply": "No table selected. Use 'select table <schema.table>' first.", "payload": {}}
    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    db_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "database"]
    if not db_locs:
        return {"reply": "No database source configured.", "payload": {}}
    conn_cfg = db_locs[0].get("connection") or {}
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector

    conn = AzureSQLPythonNetConnector(conn_cfg)
    df = conn.preview_table(table, 10)
    # lightweight preview
    cols = list(df.columns)
    rows = df.head(10).to_dict(orient="records")
    from agent.pii_masking import mask_rows
    rows = mask_rows(rows)
    return {
        "reply": f"Preview of {table} (10 rows). Columns: {', '.join(cols[:30])}",
        "payload": {"columns": cols, "rows": rows},
    }


def _node_dq_table(state: ChatState) -> ChatState:
    table = state["session"].get("context", {}).get("selected_table")
    if not table:
        return {"reply": "No table selected. Use 'select table <schema.table>' first.", "payload": {}}
    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    db_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "database"]
    if not db_locs:
        return {"reply": "No database source configured.", "payload": {}}
    conn_cfg = db_locs[0].get("connection") or {}
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
    from agent.intelligent_data_assessment import profile_dataframe, analyze_dataset_quality, load_dq_thresholds

    conn = AzureSQLPythonNetConnector(conn_cfg)
    df = conn.preview_table(table, 500)
    profile = profile_dataframe(df)
    thresholds = load_dq_thresholds()
    dq = analyze_dataset_quality(table, df, profile, thresholds)
    summ = dq.get("summary") or {}
    reply = (
        f"Data quality summary for {table} (sampled up to 500 rows): "
        f"issues={summ.get('issue_count')}, high={summ.get('high_severity')}, "
        f"medium={summ.get('medium_severity')}, low={summ.get('low_severity')}."
    )
    return {"reply": reply, "payload": {"dq": dq}}


def _node_nl_query(state: ChatState) -> ChatState:
    table = state["session"].get("context", {}).get("selected_table")
    if not table:
        return {"reply": "No table selected. Use 'select table <schema.table>' first.", "payload": {}}
    question = state.get("message", "").strip()
    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    db_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "database"]
    if not db_locs:
        return {"reply": "No database source configured.", "payload": {}}
    conn_cfg = db_locs[0].get("connection") or {}
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector

    conn = AzureSQLPythonNetConnector(conn_cfg)
    cols = conn.get_table_schema(table)
    try:
        from agent.sql_nl_query import nl_to_sql_select

        sql = nl_to_sql_select(question=question, table=table, columns=cols, max_rows=200)
    except Exception as e:
        return {
            "reply": f"I can't translate your question to SQL yet: {e}",
            "payload": {},
        }
    try:
        df = conn.execute_select(sql, max_rows=200)
        rows = df.head(50).to_dict(orient="records")
        from agent.pii_masking import mask_rows
        rows = mask_rows(rows)
        return {"reply": f"Ran query on {table}. Returned {len(rows)} rows (showing up to 50).", "payload": {"sql": sql, "rows": rows}}
    except Exception as e:
        return {"reply": f"SQL execution failed: {e}", "payload": {"sql": sql}}


def _node_save_session(state: ChatState) -> ChatState:
    sess = state.get("session") or {}
    msg = state.get("message")
    if msg:
        sess.setdefault("messages", []).append({"role": "user", "content": msg, "ts": time.time()})
    reply = state.get("reply")
    if reply:
        sess.setdefault("messages", []).append({"role": "assistant", "content": reply, "ts": time.time()})
    # Persist an "experience" row so the agent can learn over time.
    try:
        add_experience(
            session_id=str(sess.get("session_id") or state.get("session_id") or "default"),
            user_text=str(msg) if msg else None,
            action=str(state.get("action") or "") if state.get("action") else None,
            success=True if reply else None,
            notes=None,
        )
    except Exception:
        # Best-effort memory; never block the chat.
        pass
    save_session(sess)
    return {}


def build_chat_graph():
    if StateGraph is None or END is None:
        raise ImportError("LangGraph not available")
    g = StateGraph(ChatState)
    g.add_node("load_session", _node_load_session)
    g.add_node("route", _node_route)
    g.add_node("help", _node_help)
    g.add_node("reset_session", _node_reset_session)
    g.add_node("list_sources", _node_list_sources)
    g.add_node("select_source", _node_select_source)
    g.add_node("list_tables", _node_list_tables)
    g.add_node("select_table", _node_select_table)
    g.add_node("select_tables", _node_select_tables)
    g.add_node("assess_selected_tables", _node_assess_selected_tables)
    g.add_node("list_blob_files", _node_list_blob_files)
    g.add_node("select_blob_files", _node_select_blob_files)
    g.add_node("assess_selected_files", _node_assess_selected_files)
    g.add_node("list_local_files", _node_list_local_files)
    g.add_node("select_local_files", _node_select_local_files)
    g.add_node("assess_selected_local_files", _node_assess_selected_local_files)
    g.add_node("show_schema", _node_show_schema)
    g.add_node("preview_table", _node_preview_table)
    g.add_node("dq_table", _node_dq_table)
    g.add_node("nl_query", _node_nl_query)
    g.add_node("save_session", _node_save_session)

    g.set_entry_point("load_session")
    g.add_edge("load_session", "route")

    def _branch(state: ChatState) -> str:
        return state.get("action") or "help"

    g.add_conditional_edges(
        "route",
        _branch,
        {
            "help": "help",
            "reset_session": "reset_session",
            "list_sources": "list_sources",
            "select_source": "select_source",
            "list_tables": "list_tables",
            "select_table": "select_table",
            "select_tables": "select_tables",
            "assess_selected_tables": "assess_selected_tables",
            "list_blob_files": "list_blob_files",
            "select_blob_files": "select_blob_files",
            "assess_selected_files": "assess_selected_files",
            "list_local_files": "list_local_files",
            "select_local_files": "select_local_files",
            "assess_selected_local_files": "assess_selected_local_files",
            "show_schema": "show_schema",
            "preview_table": "preview_table",
            "dq_table": "dq_table",
            "nl_query": "nl_query",
        },
    )

    for n in (
        "help",
        "reset_session",
        "list_sources",
        "select_source",
        "list_tables",
        "select_table",
        "select_tables",
        "assess_selected_tables",
        "list_blob_files",
        "select_blob_files",
        "assess_selected_files",
        "list_local_files",
        "select_local_files",
        "assess_selected_local_files",
        "show_schema",
        "preview_table",
        "dq_table",
        "nl_query",
    ):
        g.add_edge(n, "save_session")
    g.add_edge("save_session", END)
    return g.compile()


def run_chat(*, session_id: str, message: str) -> Dict[str, Any]:
    graph = build_chat_graph()
    out = graph.invoke({"session_id": session_id, "message": message})
    return dict(out)

