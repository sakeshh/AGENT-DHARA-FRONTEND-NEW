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


def _flow_options(*items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Options are consumed by the frontend to render buttons.
    Each option: {id, text, send}
    """
    out: List[Dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if not it.get("text") or not it.get("send"):
            continue
        out.append({"id": str(it.get("id") or it["text"]), "text": str(it["text"]), "send": str(it["send"])})
    return out


def _prompt_choose_action() -> Dict[str, Any]:
    reply = "📌 Choose Action:\n1. View Data in Files\n2. Generate Report"
    return {
        "reply": reply,
        "payload": {
            "step": "action",
            "options": _flow_options(
                {"id": "view", "text": "👁️ View Data", "send": "view data"},
                {"id": "report", "text": "📑 Generate Report", "send": "generate report"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


def _first_location_index(source_root: Dict[str, Any], want_type: str) -> Optional[int]:
    locs = list(((source_root or {}).get("locations") or []))
    for i, loc in enumerate(locs):
        if str(loc.get("type") or "").lower() == want_type:
            return i
    return None


_MASTER_SYSTEM = """You are the Master (Supervisor) agent for a data exploration + data quality assistant.
You MUST return ONLY valid JSON and nothing else.

Your job:
- Understand the user request in natural language.
- Decide what action to take next.
- Provide the minimal arguments needed to execute it.

Allowed actions (exact strings):
help
reset_flow
back_flow
set_action
list_sources
select_source
list_tables
select_tables
select_table
show_schema
preview_table
nl_query
dq_table
list_blob_files
select_blob_files
assess_selected_files
list_local_files
select_local_files
assess_selected_local_files
assess_selected_tables
preview_local_file
preview_blob_file

Output schema:
{
  "action": "<one allowed action>",
  "args": { ... }
}

Argument rules:
- For selections, prefer numeric indices when available lists are provided.
- If the user references a specific name (table/blob/file), you may pass it directly by name.
- Never invent sources/tables/files that are not listed in the provided context.

Behavior rules:
- If the user says "restart", choose action=reset_flow.
- If the user says "back", choose action=back_flow.
- If the user picks an action ("view data" or "generate report"), choose action=set_action with {"action":"view"} or {"action":"report"}.
- If the user asks to "run data quality assessment" or "check data quality issues" for the *currently selected blob files*,
  choose action=assess_selected_files.
- If the user asks to assess the *currently selected local files*, choose action=assess_selected_local_files.
- If the user asks to assess the *currently selected tables*, choose action=assess_selected_tables.

Examples (JSON only):
{"action":"list_sources","args":{}}
{"action":"select_source","args":{"index":0}}
{"action":"list_tables","args":{}}
{"action":"select_tables","args":{"indices":[1,3,4]}}
{"action":"assess_selected_tables","args":{}}
{"action":"list_blob_files","args":{}}
{"action":"select_blob_files","args":{"all":true}}
{"action":"assess_selected_files","args":{}}
"""


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
    # Deterministic navigation commands (do not send to LLM router).
    raw = (state.get("message", "") or "").strip().lower()
    if raw in ("back", "go back", "← back"):
        return {"action": "back_flow", "action_args": {}}
    if raw in ("restart", "reset", "start over"):
        return {"action": "reset_flow", "action_args": {}}

    # Step 1 shortcuts: data source selection by NL or number.
    sess = state.get("session") or {}
    ctx = sess.get("context", {}) if isinstance(sess, dict) else {}
    if (ctx or {}).get("selected_source_index") is None:
        want = None
        if raw in ("1", "sql", "sql database", "database"):
            want = "database"
        elif raw in ("2", "blob", "azure blob", "azure blob storage"):
            want = "azure_blob"
        elif raw in ("3", "file stream", "filesystem", "file", "stream"):
            want = "filesystem"
        if want:
            sources_path = (ctx.get("sources_path") or "config/sources.yaml") if isinstance(ctx, dict) else "config/sources.yaml"
            source_root = load_sources_config(sources_path)
            idx = _first_location_index(source_root, want)
            if idx is None:
                return {"action": "help", "action_args": {}}
            return {"action": "select_source", "action_args": {"index": idx}}

    # Step 2 shortcuts: action selection without LLM.
    if (ctx or {}).get("selected_source_index") is not None and (ctx or {}).get("selected_action") is None:
        if raw in ("view data", "view", "1"):
            return {"action": "set_action", "action_args": {"action": "view"}}
        if raw in ("generate report", "report", "2"):
            return {"action": "set_action", "action_args": {"action": "report"}}

    plan = _llm_plan(user_text=state.get("message", ""), session=state.get("session") or {})
    return {"action": str(plan.get("action") or "help"), "action_args": dict(plan.get("args") or {})}


def _node_help(state: ChatState) -> ChatState:
    err = (state.get("action_args") or {}).get("error")
    if err:
        return {"reply": f"I had trouble interpreting that. Please rephrase. (router_error={err})", "payload": {}}
    # Guided mode default
    reply = (
        "📌 Select Data Source:\n"
        "1. SQL\n"
        "2. Blob\n"
        "3. File Stream"
    )
    return {
        "reply": reply,
        "payload": {
            "step": "data_source",
            "options": _flow_options(
                {"id": "sql", "text": "1. SQL", "send": "sql"},
                {"id": "blob", "text": "2. Blob", "send": "blob"},
                {"id": "fs", "text": "3. File Stream", "send": "file stream"},
            ),
        },
    }


def _node_reset_flow(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    for k in (
        "selected_source_index",
        "selected_db_location_index",
        "selected_blob_location_index",
        "selected_fs_location_index",
        "selected_table",
        "selected_tables",
        "selected_blob_files",
        "selected_local_files",
        "last_table_list",
        "last_blob_list",
        "last_local_file_list",
    ):
        ctx.pop(k, None)
    reply = (
        "✅ Restarted\n\n"
        "📌 Select Data Source:\n"
        "1. SQL\n"
        "2. Blob\n"
        "3. File Stream"
    )
    return {
        "reply": reply,
        "payload": {
            "step": "data_source",
            "options": _flow_options(
                {"id": "sql", "text": "1. SQL", "send": "sql"},
                {"id": "blob", "text": "2. Blob", "send": "blob"},
                {"id": "fs", "text": "3. File Stream", "send": "file stream"},
            ),
        },
    }


def _node_back_flow(state: ChatState) -> ChatState:
    """
    One-step back navigation:
    - If files/tables were selected → clear selection and go back to file/table list step
    - Else if source was selected → clear source and go back to data source step
    """
    ctx = state["session"].setdefault("context", {})
    if ctx.get("selected_tables") or ctx.get("selected_table"):
        ctx.pop("selected_tables", None)
        ctx.pop("selected_table", None)
        reply = "🔙 Moved back to file/table selection.\n\n👉 List again with: `list tables`"
        return {"reply": reply, "payload": {"step": "choose_files"}}
    if ctx.get("selected_blob_files"):
        ctx.pop("selected_blob_files", None)
        reply = "🔙 Moved back to file selection.\n\n👉 List again with: `list files`"
        return {"reply": reply, "payload": {"step": "choose_files"}}
    if ctx.get("selected_local_files"):
        ctx.pop("selected_local_files", None)
        reply = "🔙 Moved back to file selection.\n\n👉 List again with: `list local files`"
        return {"reply": reply, "payload": {"step": "choose_files"}}
    # Back to source selection
    ctx.pop("selected_source_index", None)
    ctx.pop("selected_db_location_index", None)
    ctx.pop("selected_blob_location_index", None)
    ctx.pop("selected_fs_location_index", None)
    reply = (
        "🔙 Moved back to Data Source.\n\n"
        "📌 Select Data Source:\n"
        "1. SQL\n"
        "2. Blob\n"
        "3. File Stream"
    )
    return {
        "reply": reply,
        "payload": {
            "step": "data_source",
            "options": _flow_options(
                {"id": "sql", "text": "1. SQL", "send": "sql"},
                {"id": "blob", "text": "2. Blob", "send": "blob"},
                {"id": "fs", "text": "3. File Stream", "send": "file stream"},
            ),
        },
    }


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
    reply = "Available sources:\n" + "\n".join([f"- {x['index']}: {x['type']} ({x['id'] or 'no-id'})" for x in out])
    return {"reply": reply, "payload": {"sources": out}}


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
    reply = f"✅ Selected: {(loc.get('type') or '').lower()} ({loc.get('id') or loc.get('label') or 'no-id'})"
    out = _prompt_choose_action()
    out["reply"] = reply + "\n\n" + out["reply"]
    out["payload"]["selected_source_index"] = idx
    return out


def _node_set_action(state: ChatState) -> ChatState:
    """
    Step 2: user chooses View vs Report.
    Immediately lists available files/tables for the selected source.
    """
    ctx = state["session"].setdefault("context", {})
    args = state.get("action_args") or {}
    a = str(args.get("action") or "").strip().lower()
    if a in ("1", "view", "view data", "view_data"):
        ctx["selected_action"] = "view"
    elif a in ("2", "report", "generate report", "generate_report"):
        ctx["selected_action"] = "report"
    else:
        # try infer from raw user text
        raw = (state.get("message", "") or "").strip().lower()
        if "view" in raw:
            ctx["selected_action"] = "view"
        elif "report" in raw or "generate" in raw:
            ctx["selected_action"] = "report"
        else:
            return _prompt_choose_action()

    # Determine selected source type and list the right entities
    sources_path = ctx.get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    locs = list(source_root.get("locations", []) or [])
    sel_idx = ctx.get("selected_source_index")
    if sel_idx is None:
        return {"reply": "📌 Select Data Source:\n1. SQL\n2. Blob\n3. File Stream", "payload": {"step": "data_source"}}
    try:
        sel_idx = int(sel_idx)
    except Exception:
        sel_idx = 0
    sel_idx = max(0, min(sel_idx, len(locs) - 1)) if locs else 0
    sel_type = str((locs[sel_idx].get("type") if locs else "") or "").lower()

    if sel_type == "database":
        out = _node_list_tables(state)
        out["reply"] = "✅ Action: " + ("View Data" if ctx["selected_action"] == "view" else "Generate Report") + "\n\n📂 Available Tables:\n" + out["reply"].split("Available SQL tables:\n", 1)[-1] + "\n\n👉 Select table(s) by number"
        out["payload"]["step"] = "choose_files"
        return out
    if sel_type == "azure_blob":
        out = _node_list_blob_files(state)
        # keep text clean and add selection hint
        out["reply"] = "✅ Action: " + ("View Data" if ctx["selected_action"] == "view" else "Generate Report") + "\n\n📂 Available Files:\n" + out["reply"].split(":\n", 1)[-1] + "\n\n👉 Select file(s) by number"
        out["payload"]["step"] = "choose_files"
        return out
    if sel_type == "filesystem":
        out = _node_list_local_files(state)
        out["reply"] = "✅ Action: " + ("View Data" if ctx["selected_action"] == "view" else "Generate Report") + "\n\n📂 Available Files:\n" + out["reply"].split(":\n", 1)[-1] + "\n\n👉 Select file(s) by number"
        out["payload"]["step"] = "choose_files"
        return out

    return {"reply": "I only support SQL, Blob, and File Stream right now.", "payload": {"step": "data_source"}}


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
        return {"reply": "No blobs found in the selected container.", "payload": {"files": [], "count": 0}}
    # Show first 50 with indices
    preview = "\n".join([f"- {i+1}: {n}" for i, n in enumerate(names[:50])])
    if len(names) > 50:
        preview += f"\n…(+{len(names)-50} more)"
    reply = (
        f"Blob files in container (location_index={blob_loc_idx}):\n{preview}\n\n"
        "Select with: 'select files 1,3-5' or 'select files all'."
    )
    return {"reply": reply, "payload": {"files": names, "count": len(names), "location_index": blob_loc_idx}}


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
    # If user previously chose "Generate Report", run it now.
    if str(ctx.get("selected_action") or "").lower() == "report":
        out = _node_assess_selected_files(state)
        out["reply"] = "✅ Selected File(s):\n" + "\n".join([f"- {n}" for n in selected]) + "\n\n📑 Report:\n" + (out.get("reply") or "")
        out["payload"]["step"] = "report"
        out["payload"]["selected_files"] = selected
        out["payload"]["options"] = _flow_options(
            {"id": "back", "text": "🔙 Back", "send": "back"},
            {"id": "restart", "text": "✅ Restart", "send": "restart"},
        )
        return out

    reply = (
        "✅ Selected File(s):\n" + "\n".join([f"- {n}" for n in selected]) + "\n\n"
        "👉 What would you like to see? (e.g., first row, columns, last 5 rows)\n"
        "You can also type: back / restart"
    )
    return {
        "reply": reply,
        "payload": {
            "step": "view_query",
            "selected_files": selected,
            "count": len(selected),
            "options": _flow_options(
                {"id": "first", "text": "📊 Show first row", "send": "show first row"},
                {"id": "cols", "text": "📊 Show columns", "send": "show columns"},
                {"id": "head5", "text": "📊 Show top 5", "send": "show top 5 rows"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


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
    preview = "\n".join([f"- {i+1}: {n}" for i, n in enumerate(files[:50])])
    if len(files) > 50:
        preview += f"\n…(+{len(files)-50} more)"
    reply = f"Local files in `{root_abs}`:\n{preview}\n\nSelect with: 'select local files 1,3-5' or 'select local files all'."
    return {"reply": reply, "payload": {"files": files, "count": len(files), "root": root_abs, "location_index": fs_idx}}


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
    if str(ctx.get("selected_action") or "").lower() == "report":
        out = _node_assess_selected_local_files(state)
        out["reply"] = "✅ Selected File(s):\n" + "\n".join([f"- {n}" for n in selected]) + "\n\n📑 Report:\n" + (out.get("reply") or "")
        out["payload"]["step"] = "report"
        out["payload"]["selected_local_files"] = selected
        out["payload"]["options"] = _flow_options(
            {"id": "back", "text": "🔙 Back", "send": "back"},
            {"id": "restart", "text": "✅ Restart", "send": "restart"},
        )
        return out

    reply = (
        "✅ Selected File(s):\n" + "\n".join([f"- {n}" for n in selected]) + "\n\n"
        "👉 What would you like to see? (e.g., first row, columns, last 5 rows)\n"
        "You can also type: back / restart"
    )
    return {
        "reply": reply,
        "payload": {
            "step": "view_query",
            "selected_local_files": selected,
            "count": len(selected),
            "options": _flow_options(
                {"id": "first", "text": "📊 Show first row", "send": "show first row"},
                {"id": "cols", "text": "📊 Show columns", "send": "show columns"},
                {"id": "head5", "text": "📊 Show top 5", "send": "show top 5 rows"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


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
    report_md = _render_report_markdown(result)
    reply = report_md or f"Assessment complete for {len(dfs)} local file(s){sampled}."
    return {
        "reply": reply,
        "payload": {"selected_local_files": selected, "result": result, "report_markdown": report_md},
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
    report_md = _render_report_markdown(result)
    if report_md:
        reply = report_md
    else:
        dq = result.get("data_quality_issues", {}) or {}
        ds = dq.get("datasets", {}) or {}
        issue_count = 0
        high = med = low = 0
        for b in ds.values():
            s = b.get("summary") or {}
            issue_count += int(s.get("issue_count") or 0)
            high += int(s.get("high_severity") or 0)
            med += int(s.get("medium_severity") or 0)
            low += int(s.get("low_severity") or 0)
        reply = f"Assessment complete for {len(dfs)} file(s). Issues={issue_count} (high={high}, medium={med}, low={low})."
    return {"reply": reply, "payload": {"selected_files": selected, "result": result, "report_markdown": report_md}}


def _parse_view_mode(user_text: str) -> str:
    t = (user_text or "").strip().lower()
    if "first row" in t or "1st row" in t:
        return "first_row"
    if "last row" in t:
        return "last_row"
    if "columns" in t or "fields" in t:
        return "columns"
    if "shape" in t or ("rows" in t and "columns" in t):
        return "shape"
    if "tail" in t or "bottom" in t:
        return "tail"
    return "head"


def _node_preview_local_file(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    args = state.get("action_args") or {}
    available = ctx.get("last_local_file_list") or []
    root = ctx.get("local_files_root") or ""
    if not root:
        return {"reply": "No local root selected. Run 'list local files' first.", "payload": {}}

    index = args.get("index")
    name = args.get("name")
    if name:
        fname = str(name)
        if fname not in available:
            return {"reply": f"File not found in the last list: {fname}. Run 'list local files' again.", "payload": {}}
    else:
        if not available:
            return {"reply": "No local file list cached. Run 'list local files' first.", "payload": {}}
        try:
            i = int(index) - 1
        except Exception:
            i = 0
        i = max(0, min(i, len(available) - 1))
        fname = str(available[i])

    import os
    import json
    import pandas as pd

    p = os.path.join(root, fname)
    if not os.path.isfile(p):
        return {"reply": f"File not found: {p}", "payload": {"file": fname}}
    low = p.lower()

    n = args.get("n")
    try:
        n = int(n) if n is not None else 5
    except Exception:
        n = 5
    n = max(1, min(n, 50))

    if low.endswith(".csv"):
        df = pd.read_csv(p, low_memory=False)
    elif low.endswith(".tsv"):
        df = pd.read_csv(p, sep="\t", low_memory=False)
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
                if len(rows) >= 500:
                    break
        df = pd.json_normalize(rows, max_level=1) if rows else pd.DataFrame()
    elif low.endswith((".xlsx", ".xls")):
        df = pd.read_excel(p)
    elif low.endswith(".parquet"):
        df = pd.read_parquet(p)
    else:
        df = pd.read_json(p)

    mode = str(args.get("mode") or "") or _parse_view_mode(state.get("message", ""))
    if mode == "columns":
        reply = f"Columns in `{fname}` ({len(df.columns)}):\n" + "\n".join([f"- {c}" for c in df.columns.tolist()])
        return {"reply": reply, "payload": {"file": fname, "columns": df.columns.tolist()}}
    if mode == "shape":
        reply = f"Shape of `{fname}`: rows={len(df)}, columns={len(df.columns)}"
        return {"reply": reply, "payload": {"file": fname, "rows": len(df), "columns": len(df.columns)}}
    if mode == "first_row":
        row = df.head(1).to_dict(orient="records")
        return {
            "reply": json.dumps(row[0] if row else {}, ensure_ascii=False, indent=2),
            "payload": {"file": fname, "row": row[0] if row else {}},
        }
    if mode == "last_row":
        row = df.tail(1).to_dict(orient="records")
        return {
            "reply": json.dumps(row[0] if row else {}, ensure_ascii=False, indent=2),
            "payload": {"file": fname, "row": row[0] if row else {}},
        }
    if mode == "tail":
        out = df.tail(n).to_dict(orient="records")
        return {"reply": json.dumps(out, ensure_ascii=False, indent=2), "payload": {"file": fname, "rows": out}}

    out = df.head(n).to_dict(orient="records")
    return {"reply": json.dumps(out, ensure_ascii=False, indent=2), "payload": {"file": fname, "rows": out}}


def _node_preview_blob_file(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    args = state.get("action_args") or {}
    available = ctx.get("last_blob_list") or []
    if not available:
        return {"reply": "No blob list cached. Run 'list files' first.", "payload": {}}

    index = args.get("index")
    name = args.get("name")
    if name:
        blob_name = str(name)
        if blob_name not in available:
            return {"reply": f"Blob not found in the last list: {blob_name}. Run 'list files' again.", "payload": {}}
    else:
        try:
            i = int(index) - 1
        except Exception:
            i = 0
        i = max(0, min(i, len(available) - 1))
        blob_name = str(available[i])

    sources_path = ctx.get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    blob_locs = _azure_blob_locations(source_root)
    if not blob_locs:
        return {"reply": "No Azure Blob source configured in sources.yaml.", "payload": {}}
    blob_loc_idx = int(ctx.get("selected_blob_location_index") or 0)
    blob_loc_idx = max(0, min(blob_loc_idx, len(blob_locs) - 1))

    from agent.mcp_clients import _single_location_config  # type: ignore
    from agent.mcp_interface import load_selected_blob_datasets  # type: ignore

    cfg_text = _single_location_config({"name": source_root.get("name") or "source"}, blob_locs[blob_loc_idx])
    dfs = load_selected_blob_datasets(cfg_text, location_index=0, blob_names=[blob_name], max_rows=500, max_bytes=None)
    df = dfs.get(blob_name)
    if df is None:
        return {"reply": f"Couldn't load blob as a dataset: {blob_name}", "payload": {"file": blob_name}}

    n = args.get("n")
    try:
        n = int(n) if n is not None else 5
    except Exception:
        n = 5
    n = max(1, min(n, 50))

    mode = str(args.get("mode") or "") or _parse_view_mode(state.get("message", ""))
    if mode == "columns":
        reply = f"Columns in `{blob_name}` ({len(df.columns)}):\n" + "\n".join([f"- {c}" for c in df.columns.tolist()])
        return {"reply": reply, "payload": {"file": blob_name, "columns": df.columns.tolist()}}
    if mode == "shape":
        reply = f"Shape of `{blob_name}`: rows={len(df)}, columns={len(df.columns)}"
        return {"reply": reply, "payload": {"file": blob_name, "rows": len(df), "columns": len(df.columns)}}
    if mode == "first_row":
        row = df.head(1).to_dict(orient="records")
        return {
            "reply": json.dumps(row[0] if row else {}, ensure_ascii=False, indent=2),
            "payload": {"file": blob_name, "row": row[0] if row else {}},
        }
    if mode == "last_row":
        row = df.tail(1).to_dict(orient="records")
        return {
            "reply": json.dumps(row[0] if row else {}, ensure_ascii=False, indent=2),
            "payload": {"file": blob_name, "row": row[0] if row else {}},
        }
    if mode == "tail":
        out = df.tail(n).to_dict(orient="records")
        return {"reply": json.dumps(out, ensure_ascii=False, indent=2), "payload": {"file": blob_name, "rows": out}}

    out = df.head(n).to_dict(orient="records")
    return {"reply": json.dumps(out, ensure_ascii=False, indent=2), "payload": {"file": blob_name, "rows": out}}


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
    reply = "Available SQL tables:\n" + "\n".join([f"- {t}" for t in tables[:200]])
    if len(tables) > 200:
        reply += f"\n…(+{len(tables)-200} more)"
    return {"reply": reply, "payload": {"tables": tables, "location_index": db_idx}}


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
    if str(ctx.get("selected_action") or "").lower() == "report":
        out = _node_assess_selected_tables(state)
        out["reply"] = "✅ Selected Table(s):\n" + "\n".join([f"- {n}" for n in selected]) + "\n\n📑 Report:\n" + (out.get("reply") or "")
        out["payload"]["step"] = "report"
        out["payload"]["selected_tables"] = selected
        out["payload"]["options"] = _flow_options(
            {"id": "back", "text": "🔙 Back", "send": "back"},
            {"id": "restart", "text": "✅ Restart", "send": "restart"},
        )
        return out

    reply = (
        "✅ Selected Table(s):\n" + "\n".join([f"- {n}" for n in selected]) + "\n\n"
        "👉 What would you like to see? (e.g., first row, columns, last 5 rows)\n"
        "You can also type: back / restart"
    )
    return {
        "reply": reply,
        "payload": {
            "step": "view_query",
            "selected_tables": selected,
            "count": len(selected),
            "options": _flow_options(
                {"id": "head", "text": "📊 Preview top rows", "send": "preview table"},
                {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


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
    sampled = f" (sampled up to {rows} rows/table)" if max_rows > 0 else " (sampled up to 10,000 rows/table)"
    report_md = _render_report_markdown(result)
    reply = report_md or f"Assessment complete for {len(dfs)} table(s){sampled}."
    return {"reply": reply, "payload": {"selected_tables": selected, "result": result, "report_markdown": report_md}}


def _node_select_table(state: ChatState) -> ChatState:
    args = state.get("action_args") or {}
    tname = args.get("name") or args.get("table")
    if not tname:
        return {"reply": "Tell me which table to use (exact name from 'list tables').", "payload": {}}
    ctx = state["session"].setdefault("context", {})
    ctx["selected_table"] = str(tname)
    return {"reply": f"Selected table: {tname}", "payload": {"selected_table": str(tname)}}


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
    g.add_node("reset_flow", _node_reset_flow)
    g.add_node("back_flow", _node_back_flow)
    g.add_node("set_action", _node_set_action)
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
    g.add_node("preview_local_file", _node_preview_local_file)
    g.add_node("preview_blob_file", _node_preview_blob_file)
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
            "reset_flow": "reset_flow",
            "back_flow": "back_flow",
            "set_action": "set_action",
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
            "preview_local_file": "preview_local_file",
            "preview_blob_file": "preview_blob_file",
            "show_schema": "show_schema",
            "preview_table": "preview_table",
            "dq_table": "dq_table",
            "nl_query": "nl_query",
        },
    )

    for n in (
        "help",
        "reset_flow",
        "back_flow",
        "set_action",
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
        "preview_local_file",
        "preview_blob_file",
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

