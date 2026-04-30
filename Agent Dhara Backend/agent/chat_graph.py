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
- Decide what action to take next (route to the right "agent": extraction vs data quality vs navigation).
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
show_null_columns
extract_columns
dq_overview
dq_duplicates
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
- If the user asks a data-quality question (nulls, duplicates, outliers, issues, quality summary) AFTER a report was generated,
  choose a DQ action (dq_overview / show_null_columns / dq_duplicates) and answer from the latest assessment.
- If the user asks for extraction (show columns, show top rows, preview data) for selected datasets, choose an extraction action.

Examples (JSON only):
{"action":"list_sources","args":{}}
{"action":"select_source","args":{"index":0}}
{"action":"list_tables","args":{}}
{"action":"select_tables","args":{"indices":[1,3,4]}}
{"action":"assess_selected_tables","args":{}}
{"action":"list_blob_files","args":{}}
{"action":"select_blob_files","args":{"all":true}}
{"action":"assess_selected_files","args":{}}
{"action":"dq_overview","args":{}}
{"action":"extract_columns","args":{}}
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


def _render_report_html(result: Dict[str, Any]) -> Optional[str]:
    """
    Render a formal report as HTML when the report builder is available.
    """
    try:
        import main as _main  # type: ignore

        if hasattr(_main, "build_html_report"):
            return _main.build_html_report(result)  # type: ignore
    except Exception:
        return None
    return None


def _override_source_root_for_datasets(result: Dict[str, Any], dataset_names: List[str], source_root: str) -> None:
    """
    The core engine tags any `additional_data` datasets as azure_blob:* by default.
    In some chat flows we pass DataFrames that originate from SQL or local filesystem, so we
    override `datasets[ds].source_root` here to reflect the real source used.
    """
    if not isinstance(result, dict):
        return
    ds = result.get("datasets")
    if not isinstance(ds, dict):
        return
    for name in dataset_names or []:
        meta = ds.get(name)
        if isinstance(meta, dict):
            meta["source_root"] = source_root


def _theme_wrap_html(*, title: str, body_html: str) -> str:
    """
    Wrap arbitrary HTML content in the same Theme 2 CSS as the main report.
    """
    import html as html_module
    from agent.report_html_themes import get_report_html_css

    css = get_report_html_css()
    return (
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n"
        "<meta charset=\"utf-8\"/>\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>\n"
        f"<title>{html_module.escape(str(title) if title else 'Details')}</title>\n"
        "<style>\n"
        + css
        + "\n</style>\n"
        "</head>\n<body>\n"
        + '<div class="wrap">'
        + f'<header class="masthead"><div class="tagline">AGENT DHARA</div><h1>{html_module.escape(str(title))}</h1></header>'
        + '<section id="details" class="datasets-section">'
        + body_html
        + "</section></div></body></html>"
    )


def _html_table(headers: List[str], rows: List[List[Any]]) -> str:
    import html as html_module

    thead = "".join(f"<th>{html_module.escape(str(h))}</th>" for h in headers)
    if not rows:
        return (
            "<div class='table-wrap'><table class='data-table'><thead><tr>"
            + thead
            + "</tr></thead><tbody><tr><td colspan='"
            + str(len(headers) or 1)
            + "' class='muted'>(none)</td></tr></tbody></table></div>"
        )
    body = []
    for r in rows:
        tds = "".join(f"<td>{html_module.escape('' if v is None else str(v))}</td>" for v in r)
        body.append("<tr>" + tds + "</tr>")
    return (
        "<div class='table-wrap'><table class='data-table'><thead><tr>"
        + thead
        + "</tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table></div>"
    )


def _md_escape(text: Any) -> str:
    s = "" if text is None else str(text)
    return s.replace("|", "\\|").replace("\n", " ").strip()


def _make_validation(*, title: str, checks: List[Dict[str, Any]]) -> Dict[str, Any]:
    ok = True
    for c in checks or []:
        if not bool(c.get("ok", False)):
            ok = False
            break
    return {"title": title, "ok": ok, "checks": checks}


def _validate_schema_markdown(*, reply_md: str, schemas: Dict[str, Any], names: List[str]) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    shown = list(names[:10])
    checks.append(
        {
            "id": "files_in_payload",
            "ok": set(shown) == set((schemas or {}).keys()),
            "detail": f"payload.schemas has {len((schemas or {}).keys())} file(s); expected {len(shown)}.",
        }
    )
    for fname in shown:
        cols = (schemas or {}).get(fname) or []
        checks.append(
            {
                "id": f"schema_block_present::{fname}",
                "ok": f"### Schema — `{fname}`" in (reply_md or ""),
                "detail": f"Markdown contains schema section for `{fname}`.",
            }
        )
        if isinstance(cols, list) and len(cols) > 80:
            checks.append(
                {
                    "id": f"schema_truncation_notice::{fname}",
                    "ok": "_…(+".lower() in (reply_md or "").lower() and f"more columns" in (reply_md or "").lower(),
                    "detail": f"`{fname}` has {len(cols)} columns; markdown should show a truncation notice after first 80.",
                }
            )
    return _make_validation(title="Schema validation", checks=checks)


def _validate_metadata_markdown(*, reply_md: str, meta: Dict[str, Any], names: List[str]) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    shown = list(names[:15])
    checks.append(
        {
            "id": "files_in_payload",
            "ok": set(shown) == set((meta or {}).keys()),
            "detail": f"payload.metadata has {len((meta or {}).keys())} file(s); expected {len(shown)}.",
        }
    )
    for fname in shown:
        checks.append(
            {
                "id": f"metadata_row_present::{fname}",
                "ok": f"| `{fname}` |" in (reply_md or ""),
                "detail": f"Markdown table contains a row for `{fname}`.",
            }
        )
        m = (meta or {}).get(fname) or {}
        rows = m.get("rows")
        cols = m.get("columns")
        checks.append(
            {
                "id": f"metadata_values_present::{fname}",
                "ok": rows is not None or cols is not None,
                "detail": f"`{fname}` metadata rows={rows}, columns={cols}.",
            }
        )
    return _make_validation(title="Metadata validation", checks=checks)


def _validate_report_payload(*, report_md: str, result: Dict[str, Any]) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    datasets = (result or {}).get("datasets") or {}
    ds_names = list(datasets.keys()) if isinstance(datasets, dict) else []
    checks.append(
        {
            "id": "dataset_count",
            "ok": isinstance(datasets, dict),
            "detail": f"result.datasets count = {len(ds_names) if isinstance(datasets, dict) else 'n/a'}",
        }
    )
    missing = []
    for n in ds_names[:25]:
        if f"`{n}`" not in (report_md or ""):
            missing.append(n)
    checks.append(
        {
            "id": "dataset_names_in_markdown",
            "ok": len(missing) == 0,
            "detail": "All dataset names appear in report markdown." if not missing else f"Missing dataset names in markdown: {missing[:8]}",
        }
    )
    dq = (result or {}).get("data_quality_issues") or {}
    dq_ds = (dq.get("datasets") or {}) if isinstance(dq, dict) else {}
    # Basic sanity: if there are any DQ issues objects, markdown should include the "Top issues" section header.
    has_any_issues = False
    if isinstance(dq_ds, dict):
        for b in dq_ds.values():
            if isinstance(b, dict) and (b.get("issues") or []):
                has_any_issues = True
                break
    checks.append(
        {
            "id": "dq_section_present",
            "ok": (not has_any_issues) or ("Top issues" in (report_md or "")),
            "detail": "DQ issues exist -> report markdown includes issues section header.",
        }
    )
    return _make_validation(title="Report validation", checks=checks)


def _build_report_tables_markdown(result: Dict[str, Any]) -> str:
    """
    Build a presentable markdown report using tables wherever possible.
    This is used as an enhancement layer (or fallback) for chat reports.
    """
    if not isinstance(result, dict):
        return ""
    datasets = result.get("datasets") or {}
    dq = (result.get("data_quality_issues") or {}).get("datasets") or {}
    rels = result.get("relationships") or []

    parts: List[str] = []
    ds_names = list(datasets.keys()) if isinstance(datasets, dict) else []
    if len(ds_names) == 1:
        parts.append(f"## Assessment Report of `{_md_escape(ds_names[0])}`")
    else:
        parts.append("## Assessment Report")

    # Dataset summary table
    rows = []
    if isinstance(datasets, dict):
        for name, meta in datasets.items():
            meta = meta or {}
            nrows = meta.get("row_count")
            ncols = meta.get("column_count")
            src_root = meta.get("source_root") or ""
            if isinstance(src_root, str) and src_root.startswith("__database__"):
                # "__database__" or "__database__:label"
                label = src_root.split(":", 1)[1] if ":" in src_root else ""
                src = f"Azure SQL{f' ({label})' if label else ''}"
            elif isinstance(src_root, str) and src_root.startswith("azure_blob:"):
                prefix = src_root.split(":", 1)[1]
                src = f"Azure Blob{f' ({prefix})' if prefix else ''}"
            elif src_root:
                src = f"Filesystem ({src_root})"
            else:
                src = ""
            summ = (dq.get(name) or {}).get("summary") or {}
            issues = summ.get("issue_count")
            high = summ.get("high_severity")
            med = summ.get("medium_severity")
            low = summ.get("low_severity")
            rows.append(
                f"| `{_md_escape(name)}` | {_md_escape(src)} | {nrows if nrows is not None else ''} | {ncols if ncols is not None else ''} | {issues if issues is not None else 0} | {high if high is not None else 0} | {med if med is not None else 0} | {low if low is not None else 0} |"
            )
    parts.append(
        "### Datasets (summary)\n\n"
        "| Dataset | Source | Rows | Cols | Issues | High | Med | Low |\n"
        "|---|---|---:|---:|---:|---:|---:|---:|\n"
        + ("\n".join(rows) if rows else "|  |  |  |  |  |  |  |  |")
    )

    # Columns table (per dataset) - mirrors the "columns:" bullets in your screenshot.
    if isinstance(datasets, dict) and datasets:
        parts.append("### Columns (per dataset)\n")
        for name, meta in datasets.items():
            meta = meta or {}
            cols = meta.get("columns") or {}
            if not isinstance(cols, dict) or not cols:
                continue
            lines = [
                "| Column | dtype | null% | unique | semantic type | candidate_pk |",
                "|---|---|---:|---:|---|:---:|",
            ]
            # keep stable order for readability
            for col_name in sorted(cols.keys(), key=lambda x: str(x).lower()):
                c = cols.get(col_name) or {}
                dtype = _md_escape(c.get("dtype"))
                nullp = c.get("null_percentage")
                nullp_txt = f"{round(100*float(nullp), 1)}%" if isinstance(nullp, (int, float)) else ""
                uq = c.get("unique_count")
                sem = _md_escape(c.get("semantic_type"))
                cand = c.get("candidate_primary_key")
                cand_txt = "✓" if cand is True else ("✗" if cand is False else "")
                lines.append(
                    f"| `{_md_escape(col_name)}` | `{dtype}` | {nullp_txt} | {uq if isinstance(uq, int) else ''} | `{sem}` | {cand_txt} |"
                )
            parts.append(f"#### `{_md_escape(name)}`\n\n" + "\n".join(lines))

    # Per-dataset issues (top N) as table
    if isinstance(dq, dict) and dq:
        parts.append("### Top issues (per dataset)")
        for name, block in dq.items():
            issues = (block or {}).get("issues") or []
            if not isinstance(issues, list) or not issues:
                continue
            # Show everything (no truncation) – user requested full tabular view.
            top = issues
            lines = [
                "| Severity | Type | Column | Count | Message | Recommendation |",
                "|:--:|---|---|---:|---|---|",
            ]
            for it in top:
                sev = _md_escape(it.get("severity"))
                typ = _md_escape(it.get("type"))
                col = _md_escape(it.get("column"))
                cnt = it.get("count")
                msg = _md_escape(it.get("message"))
                rec = _md_escape(it.get("recommendation"))
                if isinstance(cnt, int):
                    cnt_txt = str(cnt)
                elif isinstance(cnt, float):
                    # Keep readable (some rules may emit ratios)
                    cnt_txt = str(round(cnt, 4))
                elif cnt is None:
                    cnt_txt = "-"
                else:
                    cnt_txt = _md_escape(cnt)
                lines.append(
                    f"| {sev} | `{typ}` | `{col}` | {cnt_txt} | {_md_escape(msg)} | {_md_escape(rec)} |"
                )
            # No "…(+N more)" – show all rows.
            parts.append(f"#### `{_md_escape(name)}`\n\n" + "\n".join(lines))

    # Relationships table (engine emits dataset_a/column_a + dataset_b/column_b)
    rel_rows = []
    if isinstance(rels, list) and rels:
        for r in rels:
            rel_rows.append(
                f"| `{_md_escape(r.get('dataset_a'))}` | `{_md_escape(r.get('column_a'))}` | `{_md_escape(r.get('dataset_b'))}` | `{_md_escape(r.get('column_b'))}` | `{_md_escape(r.get('cardinality'))}` | {_md_escape(r.get('overlap_count'))} |"
            )
    parts.append(
        "### Relationships\n\n"
        "| Dataset A | Column A | Dataset B | Column B | Cardinality | Shared keys |\n"
        "|---|---|---|---|---|---:|\n"
        + ("\n".join(rel_rows) if rel_rows else "| _none_ |  | _none_ |  |  |  |")
    )

    # Global issues + relationship warnings in tables
    global_issues = (
        ((result.get("data_quality_issues") or {}).get("global_issues") or {})
        if isinstance(result.get("data_quality_issues"), dict)
        else {}
    )
    if isinstance(global_issues, dict) and global_issues:
        parts.append("### Global issues\n")
        # Relationship row issues (orphans) - engine uses a list of dicts
        row_issues = global_issues.get("relationship_row_issues") or []
        if isinstance(row_issues, list) and row_issues:
            gi_rows = []
            for it in row_issues:
                gi_rows.append(
                    "| "
                    + f"`{_md_escape(it.get('dataset'))}` | `{_md_escape(it.get('column'))}` | "
                    + f"`{_md_escape(it.get('related_dataset'))}` | `{_md_escape(it.get('related_column'))}` | "
                    + f"{_md_escape(it.get('count'))} |"
                )
            parts.append(
                "#### Cross-table row issues (orphan keys)\n\n"
                "| Child dataset | FK column | Parent dataset | Parent column | Rows affected |\n"
                "|---|---|---|---|---:|\n"
                + ("\n".join(gi_rows) if gi_rows else "| _none_ |  |  |  |  |")
            )
        else:
            parts.append("#### Cross-table row issues (orphan keys)\n\n- (none)")

        warnings = global_issues.get("relationship_warnings")
        if isinstance(warnings, list) and warnings:
            w_rows = []
            for w in warnings:
                if isinstance(w, dict):
                    w_rows.append(f"| {_md_escape(w.get('severity'))} | {_md_escape(w.get('message'))} |")
                else:
                    w_rows.append(f"|  | {_md_escape(w)} |")
            parts.append(
                "#### Relationship warnings\n\n"
                "| Severity | Warning |\n"
                "|---|---|\n"
                + "\n".join(w_rows)
            )
        else:
            parts.append("#### Relationship warnings\n\n- (none)")

    return "\n\n".join([p for p in parts if p.strip()])


def _write_report_artifacts(
    *,
    result: Dict[str, Any],
    report_markdown: Optional[str] = None,
    report_html: Optional[str] = None,
    base_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Persist "fresh" report artifacts for the chat workflow.

    The CLI (`main.py --reports-dir`) writes `output/reports/report.*`, but the chat API historically
    returned reports without writing files. Users expect the output folder to update on each run.

    This function overwrites `report.json/.md/.html`.
    """
    import os
    from datetime import datetime, timezone

    if not isinstance(result, dict):
        return {}

    here = os.path.dirname(os.path.abspath(__file__))
    default_dir = os.path.abspath(os.path.join(here, "..", "output", "reports"))
    reports_dir = os.path.abspath(base_dir) if base_dir else default_dir
    os.makedirs(reports_dir, exist_ok=True)

    meta = result.setdefault("run_metadata", {}) if isinstance(result.get("run_metadata"), dict) or result.get("run_metadata") is None else {}
    if isinstance(meta, dict):
        meta["generated_at"] = datetime.now(timezone.utc).isoformat()
        # Intentionally do not include local/FS paths in the user-facing report payload.
        # This directory is internal project storage and should not be shown in the UI.

    json_bytes = json.dumps(result, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    paths = {"json": os.path.join(reports_dir, "report.json")}
    with open(paths["json"], "wb") as f:
        f.write(json_bytes)

    if report_markdown:
        md_path = os.path.join(reports_dir, "report.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(report_markdown)
        paths["md"] = md_path

    if report_html:
        html_path = os.path.join(reports_dir, "report.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(report_html)
        paths["html"] = html_path

    return {"reports_dir": reports_dir, "paths": paths}


def _pick_single_active_dataset(ctx: Dict[str, Any]) -> Optional[str]:
    """
    Choose a single dataset key to answer follow-up DQ questions.
    Priority:
    - selected_table
    - exactly 1 selected_local_files
    - exactly 1 selected_blob_files
    - exactly 1 selected_tables
    - last_assessment_datasets if exactly 1
    """
    t = (ctx or {}).get("selected_table")
    if t:
        return str(t)

    for k in ("selected_local_files", "selected_blob_files", "selected_tables"):
        lst = (ctx or {}).get(k) or []
        if isinstance(lst, list) and len(lst) == 1:
            return str(lst[0])

    last_ds = (ctx or {}).get("last_assessment_datasets") or []
    if isinstance(last_ds, list) and len(last_ds) == 1:
        return str(last_ds[0])
    return None


def _assessment_signature(ctx: Dict[str, Any]) -> Dict[str, Any]:
    def _norm_list(x: Any) -> List[str]:
        if not isinstance(x, list):
            return []
        return sorted([str(v) for v in x if str(v).strip()])

    return {
        "selected_table": str(ctx.get("selected_table") or ""),
        "selected_tables": _norm_list(ctx.get("selected_tables")),
        "selected_blob_files": _norm_list(ctx.get("selected_blob_files")),
        "selected_local_files": _norm_list(ctx.get("selected_local_files")),
        "selected_db_location_index": int(ctx.get("selected_db_location_index") or 0),
        "selected_blob_location_index": int(ctx.get("selected_blob_location_index") or 0),
        "selected_fs_location_index": int(ctx.get("selected_fs_location_index") or 0),
        "local_files_root": str(ctx.get("local_files_root") or ""),
    }


def _ensure_latest_assessment(state: ChatState) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Ensure we have a fresh assessment for the current selection (single source, multiple datasets allowed).
    Returns (result, error_message).
    """
    ctx = state["session"].setdefault("context", {})
    sig = _assessment_signature(ctx)
    prev_sig = ctx.get("last_assessment_signature")
    prev = ctx.get("last_assessment_result")
    if isinstance(prev, dict) and isinstance(prev_sig, dict) and prev_sig == sig:
        return prev, None

    # Prefer explicit multi-selection
    if ctx.get("selected_tables") or ctx.get("selected_table"):
        if not ctx.get("selected_tables") and ctx.get("selected_table"):
            ctx["selected_tables"] = [str(ctx["selected_table"])]
        out = _node_assess_selected_tables(state)
        res = out.get("payload", {}).get("result")
        if isinstance(res, dict):
            ctx["last_assessment_signature"] = sig
            return res, None
        return None, out.get("reply") or "Failed to assess selected tables."

    if ctx.get("selected_blob_files"):
        out = _node_assess_selected_files(state)
        res = out.get("payload", {}).get("result")
        if isinstance(res, dict):
            ctx["last_assessment_signature"] = sig
            return res, None
        return None, out.get("reply") or "Failed to assess selected files."

    if ctx.get("selected_local_files"):
        out = _node_assess_selected_local_files(state)
        res = out.get("payload", {}).get("result")
        if isinstance(res, dict):
            ctx["last_assessment_signature"] = sig
            return res, None
        return None, out.get("reply") or "Failed to assess selected local files."

    return None, "No datasets selected. Select one or more tables/files first, then ask again."


def _node_show_cleaning_recommendations(state: ChatState) -> ChatState:
    """
    Show LLM-assisted (or fallback) cleaning recommendations for the current selection.
    """
    result, err = _ensure_latest_assessment(state)
    if err:
        return {"reply": err, "payload": {}}
    try:
        from agent.dq_recommendations_agent import DQRecommendationsAgent, dq_recommendations_to_dict

        agent = DQRecommendationsAgent()
        merged_dq = (result.get("data_quality_issues") or {}) if isinstance(result, dict) else {}
        rec = agent.recommend(merged_dq=merged_dq, user_intent=state.get("message", "") or "")
        result = dict(result)
        result["dq_recommendations"] = dq_recommendations_to_dict(rec)
    except Exception:
        pass

    # Re-show the same action buttons so user can continue the flow.
    options = _flow_options(
        {"id": "report", "text": "📄 Generate report", "send": "generate report"},
        {"id": "head", "text": "📊 View top 10 rows", "send": "view top 10 rows"},
        {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
        {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
        # Hide the currently active option (we are already showing cleaning recommendations)
        {"id": "transform", "text": "🛠️ Suggested transformations", "send": "suggested transformations"},
        {"id": "menu", "text": "📋 Menu", "send": "menu"},
        {"id": "back", "text": "🔙 Back", "send": "back"},
        {"id": "restart", "text": "✅ Restart", "send": "restart"},
    )

    return {
        "reply": "🧹 Cleaning recommendations (based on the latest assessment):",
        "payload": {
            "step": "report",
            "result": result,
            "ui": {"show_cleaning": True, "show_transform": False, "only_panel": "cleaning"},
            "options": options,
        },
    }


def _node_show_transform_suggestions(state: ChatState) -> ChatState:
    """
    Show suggested transformations for the current selection.
    """
    result, err = _ensure_latest_assessment(state)
    if err:
        return {"reply": err, "payload": {}}
    try:
        from agent.transformation_suggester import suggest_transformations

        sug = suggest_transformations(result)
        result = dict(result)
        result["transform_suggestions"] = {"sources": {"result": sug}}
    except Exception:
        pass

    options = _flow_options(
        {"id": "report", "text": "📄 Generate report", "send": "generate report"},
        {"id": "head", "text": "📊 View top 10 rows", "send": "view top 10 rows"},
        {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
        {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
        {"id": "clean", "text": "🧹 Cleaning recommendations", "send": "cleaning recommendations"},
        # Hide the currently active option (we are already showing transform suggestions)
        {"id": "menu", "text": "📋 Menu", "send": "menu"},
        {"id": "back", "text": "🔙 Back", "send": "back"},
        {"id": "restart", "text": "✅ Restart", "send": "restart"},
    )

    return {
        "reply": "🛠️ Suggested transformations (based on the latest assessment):",
        "payload": {
            "step": "report",
            "result": result,
            "ui": {"show_cleaning": False, "show_transform": True, "only_panel": "transform"},
            "options": options,
        },
    }


def _node_show_null_columns(state: ChatState) -> ChatState:
    """
    Show columns that have nulls / placeholder-nulls based on the latest assessment result.
    Works for either a selected SQL table or a selected file (blob/local), as long as we have
    the latest assessment cached in session context.
    """
    ctx = state["session"].setdefault("context", {})
    result, err = _ensure_latest_assessment(state)
    if err:
        return {"reply": err, "payload": {}}
    datasets = result.get("datasets") or {}
    if not isinstance(datasets, dict) or not datasets:
        return {"reply": "No dataset profiles found in the last assessment.", "payload": {}}

    args = state.get("action_args") or {}
    dataset = args.get("dataset") or args.get("table") or args.get("file")
    dataset_key = str(dataset) if dataset else _pick_single_active_dataset(ctx)
    if not dataset_key:
        choices = list(datasets.keys())[:20]
        hint = "Select one dataset first (e.g. `select table ...` or select a single file), then ask again."
        return {"reply": f"I need a specific table/file to check. {hint}\n\nKnown datasets:\n- " + "\n- ".join(choices), "payload": {"datasets": list(datasets.keys())}}
    if dataset_key not in datasets:
        # Try fallback: sometimes selected file/table isn't the dataset key (e.g. prefixes). Best-effort contains match.
        matches = [k for k in datasets.keys() if dataset_key.lower() in str(k).lower()]
        if len(matches) == 1:
            dataset_key = matches[0]
        else:
            return {"reply": f"Couldn't find `{dataset_key}` in the last assessment datasets.", "payload": {"datasets": list(datasets.keys())}}

    prof = datasets.get(dataset_key) or {}
    cols = (prof.get("columns") or {}) if isinstance(prof, dict) else {}
    if not isinstance(cols, dict) or not cols:
        return {"reply": f"No column profile found for `{dataset_key}`.", "payload": {}}

    null_cols: List[Tuple[str, float]] = []
    for col, meta in cols.items():
        if not isinstance(meta, dict):
            continue
        try:
            pct = float(meta.get("null_percentage") or 0.0)
        except Exception:
            pct = 0.0
        if pct > 0:
            null_cols.append((str(col), pct))
    null_cols.sort(key=lambda x: x[1], reverse=True)

    if not null_cols:
        return {"reply": f"✅ `{dataset_key}`: No null values detected in any column (based on the last assessment sample).", "payload": {"dataset": dataset_key, "null_columns": []}}

    top = null_cols[:50]
    lines = [f"- {c}: {round(p*100, 2)}%" for c, p in top]
    more = f"\n…(+{len(null_cols)-len(top)} more)" if len(null_cols) > len(top) else ""
    return {
        "reply": f"Columns with null values in `{dataset_key}` (showing {len(top)}/{len(null_cols)}):\n" + "\n".join(lines) + more,
        "payload": {"dataset": dataset_key, "null_columns": [{"name": c, "null_percentage": p} for c, p in null_cols]},
    }


def _node_dq_overview(state: ChatState) -> ChatState:
    """
    DQ Agent: summarize quality issues across all selected datasets for the active source.
    Auto-runs assessment if needed.
    """
    ctx = state["session"].setdefault("context", {})
    result, err = _ensure_latest_assessment(state)
    if err:
        return {"reply": err, "payload": {}}

    dq = (result.get("data_quality_issues") or {}) if isinstance(result, dict) else {}
    per = (dq.get("datasets") or {}) if isinstance(dq, dict) else {}
    if not isinstance(per, dict) or not per:
        return {"reply": "No data quality section found in the latest assessment.", "payload": {}}

    rows = []
    total = {"issues": 0, "high": 0, "medium": 0, "low": 0}
    for ds_name, block in per.items():
        summ = (block or {}).get("summary") or {}
        try:
            ic = int(summ.get("issue_count") or 0)
            hi = int(summ.get("high_severity") or 0)
            me = int(summ.get("medium_severity") or 0)
            lo = int(summ.get("low_severity") or 0)
        except Exception:
            ic = hi = me = lo = 0
        total["issues"] += ic
        total["high"] += hi
        total["medium"] += me
        total["low"] += lo
        rows.append((str(ds_name), ic, hi, me, lo))

    rows.sort(key=lambda x: (x[2], x[1]), reverse=True)
    top = rows[:20]
    lines = [f"- {n}: issues={ic} (high={hi}, medium={me}, low={lo})" for n, ic, hi, me, lo in top]
    more = f"\n…(+{len(rows)-len(top)} more)" if len(rows) > len(top) else ""

    # Relationships and global issues are where multi-dataset value shows up.
    rels = result.get("relationships") or []
    global_issues = (dq.get("global_issues") or {}) if isinstance(dq, dict) else {}
    orphan_fk = (global_issues.get("orphan_foreign_keys") or []) if isinstance(global_issues, dict) else []

    rel_note = f"Relationships detected: {len(rels)}" if isinstance(rels, list) else "Relationships detected: 0"
    orphan_note = f"Orphan-FK hints: {len(orphan_fk)}" if isinstance(orphan_fk, list) else "Orphan-FK hints: 0"

    reply = (
        f"Data quality overview (selected datasets={len(rows)}): total_issues={total['issues']} "
        f"(high={total['high']}, medium={total['medium']}, low={total['low']}).\n\n"
        f"{rel_note}; {orphan_note}.\n\n"
        "Per-dataset summary:\n" + "\n".join(lines) + more
    )
    ctx["last_dq_answer"] = {"kind": "overview", "total": total, "datasets": rows}
    return {"reply": reply, "payload": {"dq_total": total, "per_dataset": [{"dataset": n, "issue_count": ic, "high": hi, "medium": me, "low": lo} for n, ic, hi, me, lo in rows]}}


def _node_dq_duplicates(state: ChatState) -> ChatState:
    """
    DQ Agent: show duplicate-row and duplicate-PK issues across selected datasets.
    Auto-runs assessment if needed.
    """
    result, err = _ensure_latest_assessment(state)
    if err:
        return {"reply": err, "payload": {}}
    dq = (result.get("data_quality_issues") or {}) if isinstance(result, dict) else {}
    per = (dq.get("datasets") or {}) if isinstance(dq, dict) else {}
    if not isinstance(per, dict) or not per:
        return {"reply": "No data quality section found in the latest assessment.", "payload": {}}

    hits = []
    for ds_name, block in per.items():
        issues = (block or {}).get("issues") or []
        if not isinstance(issues, list):
            continue
        for iss in issues:
            if not isinstance(iss, dict):
                continue
            t = str(iss.get("type") or "")
            if t in ("duplicate_rows", "duplicate_primary_key"):
                hits.append(
                    {
                        "dataset": str(ds_name),
                        "type": t,
                        "severity": str(iss.get("severity") or ""),
                        "message": str(iss.get("message") or iss.get("detail") or ""),
                        "column": iss.get("column"),
                        "count": iss.get("count"),
                    }
                )
    if not hits:
        return {"reply": "✅ No duplicate-row or duplicate-PK issues detected in the latest assessment sample.", "payload": {"duplicates": []}}

    # Sort high severity first, then count desc if present.
    sev_rank = {"high": 0, "medium": 1, "low": 2}
    def _rk(x: Dict[str, Any]) -> Tuple[int, int]:
        r = sev_rank.get(str(x.get("severity") or "").lower(), 9)
        try:
            c = int(x.get("count") or 0)
        except Exception:
            c = 0
        return (r, -c)
    hits.sort(key=_rk)
    top = hits[:30]
    lines = []
    for h in top:
        col = f".{h['column']}" if h.get("column") else ""
        cnt = f" count={h['count']}" if h.get("count") is not None else ""
        lines.append(f"- [{h['severity']}] {h['dataset']}{col}: {h['type']}{cnt} — {h['message']}")
    more = f"\n…(+{len(hits)-len(top)} more)" if len(hits) > len(top) else ""
    return {"reply": "Duplicate issues found:\n" + "\n".join(lines) + more, "payload": {"duplicates": hits}}


def _node_extract_columns(state: ChatState) -> ChatState:
    """
    Extraction Agent: list columns for all selected datasets (tables/files) from the latest assessment profile.
    Auto-runs assessment if needed.
    """
    result, err = _ensure_latest_assessment(state)
    if err:
        return {"reply": err, "payload": {}}

    datasets = (result.get("datasets") or {}) if isinstance(result, dict) else {}
    if not isinstance(datasets, dict) or not datasets:
        return {"reply": "No dataset profiles found in the latest assessment.", "payload": {}}

    out = []
    for ds_name, prof in datasets.items():
        cols = (prof or {}).get("columns") or {}
        if isinstance(cols, dict):
            out.append((str(ds_name), list(cols.keys())))
    out.sort(key=lambda x: x[0].lower())

    lines = []
    for ds, cols in out[:20]:
        show = cols[:40]
        more = f" …(+{len(cols)-len(show)} more)" if len(cols) > len(show) else ""
        lines.append(f"- {ds} ({len(cols)}): " + ", ".join(map(str, show)) + more)
    more_ds = f"\n…(+{len(out)-20} more datasets)" if len(out) > 20 else ""

    return {
        "reply": "Columns per selected dataset:\n" + "\n".join(lines) + more_ds,
        "payload": {"columns_by_dataset": [{"dataset": ds, "columns": cols} for ds, cols in out]},
    }


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
    # Greetings / empty chatter should start the guided flow (avoid LLM picking list_sources).
    if raw in ("hi", "hello", "hey", "hii", "hlo", "start", "menu", "help"):
        return {"action": "help", "action_args": {}}
    if raw in ("back", "go back", "← back"):
        return {"action": "back_flow", "action_args": {}}
    if raw in ("restart", "reset", "start over"):
        return {"action": "reset_flow", "action_args": {}}
    # Route "schema/metadata/report/preview" to table vs file handlers based on current selection.
    sess = state.get("session") or {}
    ctx = sess.get("context", {}) if isinstance(sess, dict) else {}
    has_selected_files = bool((ctx or {}).get("selected_blob_files") or (ctx or {}).get("selected_local_files"))

    if raw in ("show schema", "schema"):
        return {"action": ("show_file_schema" if has_selected_files else "show_schema"), "action_args": {}}
    if raw in ("show metadata", "metadata", "meta data", "show meta data"):
        return {"action": ("show_file_metadata" if has_selected_files else "show_metadata"), "action_args": {}}
    if raw in ("view top 10 rows", "next 10 rows", "preview top rows", "preview"):
        return {"action": ("preview_selected_file" if has_selected_files else "preview_table"), "action_args": {"n": 10}}
    if raw in ("generate report", "report", "generate a report"):
        return {"action": ("generate_report_selected_files" if has_selected_files else "generate_report_selected"), "action_args": {}}
    if raw in (
        "cleaning recommendations",
        "cleaning recommendation",
        "cleaning plan",
        "recommend cleaning",
        "cleaning recs",
    ):
        return {"action": "show_cleaning_recommendations", "action_args": {}}
    if raw in (
        "suggested transformations",
        "suggest transformations",
        "transform suggestions",
        "transformation suggestions",
        "suggested fixes",
        "suggest fixes",
    ):
        return {"action": "show_transform_suggestions", "action_args": {}}

    # Deterministic selection commands (avoid LLM dropping indices).
    # Supports:
    # - "select tables all" / "select all tables"
    # - "select tables 1,2,3" / "select files 1 2 3" / "select local files 1;2;3"
    if raw.startswith("select "):
        import re as _re

        def _parse_int_list(s: str) -> List[int]:
            out: List[int] = []
            for tok in _re.split(r"[,\s;]+", (s or "").strip()):
                if not tok:
                    continue
                try:
                    out.append(int(tok))
                except Exception:
                    continue
            return out

        # access cached lists for "all"
        sess = state.get("session") or {}
        ctx = sess.get("context", {}) if isinstance(sess, dict) else {}
        last_tables = (ctx or {}).get("last_table_list") or []
        last_blobs = (ctx or {}).get("last_blob_list") or []
        last_locals = (ctx or {}).get("last_local_file_list") or []

        if raw in ("select all tables", "select tables all", "select all table", "select table all"):
            if last_tables:
                return {"action": "select_tables", "action_args": {"all": True}}
        if raw in ("select all files", "select files all"):
            if last_blobs:
                return {"action": "select_blob_files", "action_args": {"all": True}}
        if raw in ("select all local files", "select local files all"):
            if last_locals:
                return {"action": "select_local_files", "action_args": {"all": True}}

        m = _re.match(r"^select\s+tables?\s+(.+)$", raw)
        if m:
            idxs = _parse_int_list(m.group(1))
            if idxs:
                return {"action": "select_tables", "action_args": {"indices": idxs}}
        m = _re.match(r"^select\s+files?\s+(.+)$", raw)
        if m:
            idxs = _parse_int_list(m.group(1))
            if idxs:
                return {"action": "select_blob_files", "action_args": {"indices": idxs}}
        m = _re.match(r"^select\s+local\s+files?\s+(.+)$", raw)
        if m:
            idxs = _parse_int_list(m.group(1))
            if idxs:
                return {"action": "select_local_files", "action_args": {"indices": idxs}}

    # DQ shortcuts: allow follow-up questions after a report without forcing "select table".
    if ("null" in raw or "missing" in raw) and ("column" in raw or "columns" in raw or "fields" in raw):
        return {"action": "show_null_columns", "action_args": {}}
    if any(k in raw for k in ("data quality", "dq", "quality issues", "issues summary", "quality summary", "dq summary")):
        return {"action": "dq_overview", "action_args": {}}
    if "duplicate" in raw:
        return {"action": "dq_duplicates", "action_args": {}}
    if ("show columns" in raw or "list columns" in raw or (("columns" in raw or "fields" in raw) and "show" in raw)) and "null" not in raw:
        return {"action": "extract_columns", "action_args": {}}

    # Step 1 shortcuts: data source selection by NL or number.
    # These should ALWAYS work (even mid-flow) so the UI buttons can't accidentally
    # route into NL→SQL or other actions based on stale session context.
    sess = state.get("session") or {}
    ctx = sess.get("context", {}) if isinstance(sess, dict) else {}
    want = None
    if raw in ("1", "sql", "sql database", "database"):
        want = "database"
    elif raw in ("2", "blob", "azure blob", "azure blob storage"):
        want = "azure_blob"
    elif raw in ("3", "file stream", "filesystem", "file", "stream"):
        want = "filesystem"
    if want:
        # Clear stale selections so the new data source starts cleanly.
        if isinstance(ctx, dict):
            for k in (
                "selected_source_index",
                "selected_db_location_index",
                "selected_blob_location_index",
                "selected_fs_location_index",
                "selected_action",
                "selected_table",
                "selected_tables",
                "selected_blob_files",
                "selected_local_files",
                "last_table_list",
                "last_blob_list",
                "last_local_file_list",
            ):
                ctx.pop(k, None)
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
    # If we are currently on a generated report, go back to the last table "view" menu
    # (keep selected table(s) and show the same buttons again).
    if ctx.get("last_ui_step") == "report" and (ctx.get("selected_tables") or ctx.get("selected_table")):
        selected = ctx.get("selected_tables") or []
        if not selected and ctx.get("selected_table"):
            selected = [str(ctx.get("selected_table"))]
            ctx["selected_tables"] = selected
        # Ensure we return to the view menu.
        ctx["selected_action"] = "view"
        reply = (
            "✅ Selected Table(s):\n"
            + "\n".join([f"- {n}" for n in selected])
            + "\n\n👉 What would you like to see? (e.g., first row, columns, last 5 rows)\n"
            + "You can also type: back / restart"
        )
        return {
            "reply": reply,
            "payload": {
                "step": "view_query",
                "selected_tables": selected,
                "count": len(selected),
                "options": _flow_options(
                    {"id": "head", "text": "📊 View top 10 rows", "send": "preview table"},
                    {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
                    {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
                    {"id": "report", "text": "📄 Generate report", "send": "generate report"},
                    {"id": "menu", "text": "📋 Menu", "send": "menu"},
                    {"id": "back", "text": "🔙 Back", "send": "back"},
                    {"id": "restart", "text": "✅ Restart", "send": "restart"},
                ),
            },
        }
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
    # Default flow: go straight to "View Data" after selecting a source
    # (skips the intermediate Choose Action menu).
    ctx["selected_action"] = "view"
    out = _node_set_action({"session": state["session"], "message": "view", "action_args": {"action": "view"}})
    out["reply"] = reply + "\n\n" + (out.get("reply") or "")
    out.setdefault("payload", {})["selected_source_index"] = idx
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
        out["payload"]["ui"] = {"show_cleaning": False, "show_transform": False}
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
                {"id": "report", "text": "📄 Generate report", "send": "generate report"},
                {"id": "head", "text": "📊 View top 10 rows", "send": "view top 10 rows"},
                {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
                {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
                {"id": "menu", "text": "📋 Menu", "send": "menu"},
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
        out["payload"]["ui"] = {"show_cleaning": False, "show_transform": False}
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
                {"id": "report", "text": "📄 Generate report", "send": "generate report"},
                {"id": "head", "text": "📊 View top 10 rows", "send": "view top 10 rows"},
                {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
                {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
                {"id": "menu", "text": "📋 Menu", "send": "menu"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


def _selected_file_mode_and_names(ctx: Dict[str, Any]) -> Tuple[str, List[str]]:
    """Return ('blob'|'local', names[]) based on current session context."""
    sel_blob = ctx.get("selected_blob_files") or []
    if isinstance(sel_blob, list) and sel_blob:
        return "blob", [str(x) for x in sel_blob]
    sel_local = ctx.get("selected_local_files") or []
    if isinstance(sel_local, list) and sel_local:
        return "local", [str(x) for x in sel_local]
    return "none", []


def _node_preview_selected_file(state: ChatState) -> ChatState:
    """
    Preview the currently selected file (first selected) with paging (10 rows at a time).
    For multiple selected files, this previews the first one.
    """
    ctx = state["session"].setdefault("context", {})
    mode, names = _selected_file_mode_and_names(ctx)
    if mode == "none" or not names:
        return {"reply": "No file selected. Select one or more files first.", "payload": {}}

    fname = names[0]
    args = state.get("action_args") or {}
    try:
        n = int(args.get("n") or 10)
    except Exception:
        n = 10
    n = max(1, min(n, 50))
    offset = int(ctx.get("file_preview_offset") or 0)
    offset = max(0, offset)

    # Load a batch of rows (up to 500) using existing preview nodes.
    if mode == "local":
        out = _node_preview_local_file({"session": state["session"], "message": "", "action_args": {"name": fname, "n": 500}})
    else:
        out = _node_preview_blob_file({"session": state["session"], "message": "", "action_args": {"name": fname, "n": 500}})
    rows = ((out.get("payload") or {}).get("rows") or [])
    if not isinstance(rows, list):
        rows = []
    page = rows[offset : offset + n]
    ctx["file_preview_offset"] = offset + len(page)

    rows_text = json.dumps(page, ensure_ascii=False, indent=2)
    head_label = "📊 Next 10 rows" if offset > 0 else "📊 View top 10 rows"
    # Themed HTML preview
    cols = []
    if page and isinstance(page[0], dict):
        cols = list(page[0].keys())
    html_rows = []
    for r in page[:50]:
        if isinstance(r, dict) and cols:
            html_rows.append([r.get(c) for c in cols])
        else:
            html_rows.append([json.dumps(r, ensure_ascii=False)])
    body_html = _html_table(cols if cols else ["row"], html_rows)
    ui_html = _theme_wrap_html(title=f"Preview — {fname}", body_html=body_html)
    return {
        "reply": f"Preview of `{fname}` (rows {offset + 1}–{offset + len(page)}):\n\n{rows_text}",
        "payload": {
            "step": "view_query",
            "file": fname,
            "rows": page,
            "count": len(page),
            "ui_html": ui_html,
            "options": _flow_options(
                {"id": "head", "text": head_label, "send": "view top 10 rows"},
                {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
                {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
                {"id": "report", "text": "📄 Generate report", "send": "generate report"},
                {"id": "menu", "text": "📋 Menu", "send": "menu"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


def _node_show_file_schema(state: ChatState) -> ChatState:
    """Show columns (schema) for selected blob/local files."""
    ctx = state["session"].setdefault("context", {})
    mode, names = _selected_file_mode_and_names(ctx)
    if mode == "none" or not names:
        return {"reply": "No file selected. Select one or more files first.", "payload": {}}

    blocks: List[str] = []
    schemas: Dict[str, Any] = {}
    for fname in names[:10]:
        if mode == "local":
            out = _node_preview_local_file({"session": state["session"], "message": "show columns", "action_args": {"name": fname, "mode": "columns"}})
        else:
            out = _node_preview_blob_file({"session": state["session"], "message": "show columns", "action_args": {"name": fname, "mode": "columns"}})
        cols = ((out.get("payload") or {}).get("columns") or [])
        if not isinstance(cols, list):
            cols = []
        schemas[fname] = cols
        # Markdown table for clean UI rendering.
        rows = "\n".join([f"| {i+1} | `{str(c)}` |" for i, c in enumerate(cols[:80])])
        blocks.append(
            f"### Schema — `{fname}`\n\n"
            f"| # | Column |\n"
            f"|---:|--------|\n"
            f"{rows if rows else '|  |  |'}\n"
            + (f"\n\n_…(+{len(cols)-80} more columns)_" if len(cols) > 80 else "")
        )
    if len(names) > 10:
        blocks.append(f"_…(+{len(names) - 10} more files)_")

    # Themed HTML schema
    html_parts: List[str] = []
    for fname in names[:10]:
        cols = schemas.get(fname) or []
        trows = [[i + 1, c] for i, c in enumerate(cols[:200])]
        html_parts.append(f"<h2>{fname}</h2>" + _html_table(["#", "Column"], trows))
        if isinstance(cols, list) and len(cols) > 200:
            html_parts.append(f"<p class='muted'>…(+{len(cols)-200} more columns)</p>")
    ui_html = _theme_wrap_html(title="Schema", body_html="".join(html_parts) if html_parts else "<p class='muted'>(none)</p>")

    reply_md = "\n\n".join(blocks)
    validation = _validate_schema_markdown(reply_md=reply_md, schemas=schemas, names=names)
    return {
        "reply": reply_md,
        "payload": {
            "step": "view_query",
            "schemas": schemas,
            "ui_html": ui_html,
            "validation": validation,
            "options": _flow_options(
                {"id": "head", "text": "📊 View top 10 rows", "send": "view top 10 rows"},
                {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
                {"id": "report", "text": "📄 Generate report", "send": "generate report"},
                {"id": "menu", "text": "📋 Menu", "send": "menu"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


def _node_show_file_metadata(state: ChatState) -> ChatState:
    """Show shape/basic metadata for selected blob/local files."""
    ctx = state["session"].setdefault("context", {})
    mode, names = _selected_file_mode_and_names(ctx)
    if mode == "none" or not names:
        return {"reply": "No file selected. Select one or more files first.", "payload": {}}

    meta: Dict[str, Any] = {}
    rows_md: List[str] = []
    for fname in names[:15]:
        if mode == "local":
            out = _node_preview_local_file({"session": state["session"], "message": "shape", "action_args": {"name": fname, "mode": "shape"}})
        else:
            out = _node_preview_blob_file({"session": state["session"], "message": "shape", "action_args": {"name": fname, "mode": "shape"}})
        rows = ((out.get("payload") or {}).get("rows"))
        cols = ((out.get("payload") or {}).get("columns"))
        meta[fname] = {"rows": rows, "columns": cols}
        r_txt = str(rows) if rows is not None else "unavailable"
        c_txt = str(cols) if cols is not None else "unavailable"
        rows_md.append(f"| `{fname}` | {r_txt} | {c_txt} |")

    reply_md = (
            "### Metadata — selected files\n\n"
            "| File | Rows | Columns |\n"
            "|------|-----:|--------:|\n"
            + ("\n".join(rows_md) if rows_md else "|  |  |  |")
            + (f"\n\n_…(+{len(names) - 15} more files)_" if len(names) > 15 else "")
        )
    validation = _validate_metadata_markdown(reply_md=reply_md, meta=meta, names=names)
    return {
        "reply": reply_md,
        "payload": {
            "step": "view_query",
            "metadata": meta,
            "ui_html": _theme_wrap_html(
                title="Metadata",
                body_html=_html_table(
                    ["File", "Rows", "Columns"],
                    [[k, v.get("rows"), v.get("columns")] for k, v in (meta or {}).items()],
                ),
            ),
            "validation": validation,
            "options": _flow_options(
                {"id": "head", "text": "📊 View top 10 rows", "send": "view top 10 rows"},
                {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
                {"id": "report", "text": "📄 Generate report", "send": "generate report"},
                {"id": "menu", "text": "📋 Menu", "send": "menu"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


def _node_generate_report_selected_files(state: ChatState) -> ChatState:
    """Generate assessment report for selected blob/local files."""
    ctx = state["session"].setdefault("context", {})
    mode, names = _selected_file_mode_and_names(ctx)
    if mode == "none" or not names:
        return {"reply": "No file selected. Select one or more files first.", "payload": {}}

    ctx["selected_action"] = "report"
    out = _node_assess_selected_files(state) if mode == "blob" else _node_assess_selected_local_files(state)
    out["payload"] = out.get("payload") or {}
    out["payload"]["step"] = "report"
    out["payload"]["ui"] = {"show_cleaning": False, "show_transform": False}
    ctx["last_ui_step"] = "report"
    out["payload"]["options"] = _flow_options(
        {"id": "head", "text": "📊 View top 10 rows", "send": "view top 10 rows"},
        {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
        {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
        {"id": "clean", "text": "🧹 Cleaning recommendations", "send": "cleaning recommendations"},
        {"id": "transform", "text": "🛠️ Suggested transformations", "send": "suggested transformations"},
        {"id": "menu", "text": "📋 Menu", "send": "menu"},
        {"id": "back", "text": "🔙 Back", "send": "back"},
        {"id": "restart", "text": "✅ Restart", "send": "restart"},
    )
    return out


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

    dfs = {}
    for name in selected:
        p = os.path.join(root, name)
        if not os.path.isfile(p):
            return {"reply": f"File not found: {p}", "payload": {"file": name}}
        low = p.lower()
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
    _override_source_root_for_datasets(result, list(dfs.keys()), os.path.abspath(root))
    # Only return the tabular report in chat (no legacy/freeform report text).
    report_md = _build_report_tables_markdown(result)
    report_html = _render_report_html(result)
    reply = report_md or f"Assessment complete for {len(dfs)} local file(s)."
    artifacts = _write_report_artifacts(result=result, report_markdown=report_md, report_html=report_html)
    validation = _validate_report_payload(report_md=report_md or "", result=result if isinstance(result, dict) else {})
    # Cache for follow-up DQ questions
    ctx["last_assessment_result"] = result
    ctx["last_assessment_datasets"] = list((result.get("datasets") or {}).keys()) if isinstance(result, dict) else []
    return {
        "reply": reply,
        "payload": {
            "selected_local_files": selected,
            "result": result,
            "report_markdown": report_md,
            "report_html": report_html,
            "report_files": artifacts,
            "validation": validation,
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

    cfg_text = _single_location_config({"name": source_root.get("name") or "source"}, blob_locs[blob_loc_idx])
    dfs = load_selected_blob_datasets(
        cfg_text,
        location_index=0,
        blob_names=list(selected),
        max_rows=None,
        max_bytes=None,
    )
    # Run assessment purely over the loaded blobs (via additional_data).
    result = run_assessment(cfg_text, additional_data=dfs)
    # Only return the tabular report in chat (no legacy/freeform report text).
    report_md = _build_report_tables_markdown(result)
    report_html = _render_report_html(result)
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
    artifacts = _write_report_artifacts(result=result, report_markdown=report_md, report_html=report_html)
    validation = _validate_report_payload(report_md=report_md or "", result=result if isinstance(result, dict) else {})
    # Cache for follow-up DQ questions
    ctx["last_assessment_result"] = result
    ctx["last_assessment_datasets"] = list((result.get("datasets") or {}).keys()) if isinstance(result, dict) else []
    return {
        "reply": reply,
        "payload": {
            "selected_files": selected,
            "result": result,
            "report_markdown": report_md,
            "report_html": report_html,
            "report_files": artifacts,
            "validation": validation,
        },
    }


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
    dfs = load_selected_blob_datasets(cfg_text, location_index=0, blob_names=[blob_name], max_rows=None, max_bytes=None)
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
    preview_items = [{"index": i, "name": str(t)} for i, t in enumerate(tables)]
    reply = "Available SQL tables:\n" + "\n".join([f"- {i+1}: {t}" for i, t in enumerate(tables[:200])])
    if len(tables) > 200:
        reply += f"\n…(+{len(tables)-200} more)"
    return {"reply": reply, "payload": {"tables": preview_items, "count": len(tables), "location_index": db_idx}}


def _node_select_tables(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    available = ctx.get("last_table_list") or []
    if not available:
        return {"reply": "No table list cached. Run 'list tables' first.", "payload": {}}
    args = state.get("action_args") or {}

    def _parse_indices_arg(v: Any) -> List[int]:
        if v is None:
            return []
        if isinstance(v, (int, float)):
            try:
                return [int(v)]
            except Exception:
                return []
        if isinstance(v, str):
            # Accept "1,2,3" / "1 2 3" / "1;2;3"
            import re as _re

            out: List[int] = []
            for tok in _re.split(r"[,\s;]+", v.strip()):
                if not tok:
                    continue
                try:
                    out.append(int(tok))
                except Exception:
                    continue
            return out
        if isinstance(v, list):
            out = []
            for item in v:
                out.extend(_parse_indices_arg(item))
            return out
        return []

    if args.get("all") is True:
        selected = list(available)
    else:
        names = args.get("names")
        indices_raw = args.get("indices")
        indices = _parse_indices_arg(indices_raw)
        selected = []
        if isinstance(names, list):
            selected = [str(n) for n in names if str(n) in available]
        elif isinstance(names, str) and names.strip():
            # Accept a single exact table name string
            s = names.strip()
            if s in available:
                selected = [s]
        elif indices:
            for i in indices:
                j = i - 1
                if 0 <= j < len(available):
                    selected.append(str(available[j]))
        if not selected:
            return {"reply": "Tell me which tables to select (by indices or exact names) after running 'list tables'.", "payload": {}}
    ctx["selected_tables"] = selected
    # Convenience: if exactly one table is selected, treat it as the active table for schema/preview/NL queries.
    if len(selected) == 1:
        ctx["selected_table"] = str(selected[0])
    # Reset preview paging whenever table selection changes.
    ctx["table_preview_offset"] = 0
    if str(ctx.get("selected_action") or "").lower() == "report":
        out = _node_assess_selected_tables(state)
        out["reply"] = "✅ Selected Table(s):\n" + "\n".join([f"- {n}" for n in selected]) + "\n\n📑 Report:\n" + (out.get("reply") or "")
        out["payload"]["step"] = "report"
        out["payload"]["ui"] = {"show_cleaning": False, "show_transform": False}
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
                {"id": "report", "text": "📄 Generate report", "send": "generate report"},
                {"id": "head", "text": "📊 View top 10 rows", "send": "preview table"},
                {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
                {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
                {"id": "menu", "text": "📋 Menu", "send": "menu"},
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

    conn = AzureSQLPythonNetConnector(conn_cfg)
    dfs = {t: conn.load_table(t) for t in selected}
    result = load_and_profile({"name": source_root.get("name") or "source", "locations": []}, additional_data=dfs)
    # Ensure source_root reflects Azure SQL (not azure_blob from `additional_data` default).
    label = (
        (db_locs[db_idx].get("id") or db_locs[db_idx].get("label") or db_locs[db_idx].get("name") or "").strip()
        or (conn_cfg.get("database") or "").strip()
        or "__default__"
    )
    _override_source_root_for_datasets(result, list(dfs.keys()), f"__database__:{label}")
    # Only return the tabular report in chat (no legacy/freeform report text).
    report_md = _build_report_tables_markdown(result)
    report_html = _render_report_html(result)
    reply = report_md or f"Assessment complete for {len(dfs)} table(s)."
    artifacts = _write_report_artifacts(result=result, report_markdown=report_md, report_html=report_html)
    validation = _validate_report_payload(report_md=report_md or "", result=result if isinstance(result, dict) else {})
    # Cache for follow-up DQ questions
    ctx["last_assessment_result"] = result
    ctx["last_assessment_datasets"] = list((result.get("datasets") or {}).keys()) if isinstance(result, dict) else []
    return {
        "reply": reply,
        "payload": {
            "selected_tables": selected,
            "result": result,
            "report_markdown": report_md,
            "report_html": report_html,
            "report_files": artifacts,
            "validation": validation,
        },
    }


def _node_generate_report_selected(state: ChatState) -> ChatState:
    """
    Generate a report for the currently selected table(s) without going back
    to the Choose Action step.
    """
    ctx = state["session"].setdefault("context", {})
    selected = ctx.get("selected_tables") or []
    if not selected:
        table = ctx.get("selected_table")
        if table:
            selected = [str(table)]
            ctx["selected_tables"] = selected
    if not selected:
        return {"reply": "No table selected. Select a table first, then click Generate report.", "payload": {}}

    ctx["selected_action"] = "report"
    out = _node_assess_selected_tables(state)
    out["payload"] = out.get("payload") or {}
    out["payload"]["step"] = "report"
    out["payload"]["ui"] = {"show_cleaning": False, "show_transform": False}
    # Mark step so "back" can return to the table menu.
    ctx["last_ui_step"] = "report"
    out["payload"]["options"] = _flow_options(
        {"id": "head", "text": "📊 View top 10 rows", "send": "preview table"},
        {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
        {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
        {"id": "clean", "text": "🧹 Cleaning recommendations", "send": "cleaning recommendations"},
        {"id": "transform", "text": "🛠️ Suggested transformations", "send": "suggested transformations"},
        # Hide "Generate report" right after generating a report; it will reappear
        # when the user selects another action (schema/rows/metadata) that returns
        # the standard view menu.
        {"id": "menu", "text": "📋 Menu", "send": "menu"},
        {"id": "back", "text": "🔙 Back", "send": "back"},
        {"id": "restart", "text": "✅ Restart", "send": "restart"},
    )
    return out


def _node_select_table(state: ChatState) -> ChatState:
    args = state.get("action_args") or {}
    ctx = state["session"].setdefault("context", {})
    available = ctx.get("last_table_list") or []

    tname = args.get("name") or args.get("table")
    idx = args.get("index")
    if not tname and idx is not None and available:
        try:
            j = int(idx) - 1
        except Exception:
            j = -1
        if 0 <= j < len(available):
            tname = available[j]

    if not tname:
        hint = "Run 'list tables' then use: select table 1 (or select table dbo.TableName)."
        return {"reply": f"Tell me which table to use. {hint}", "payload": {}}
    ctx["selected_table"] = str(tname)
    return {"reply": f"Selected table: {tname}", "payload": {"selected_table": str(tname)}}


def _node_show_schema(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    selected_tables = ctx.get("selected_tables") or []
    if not isinstance(selected_tables, list):
        selected_tables = []

    table = ctx.get("selected_table")
    if not table and len(selected_tables) == 1:
        table = selected_tables[0]
        ctx["selected_table"] = str(table)
    if not table and not selected_tables:
        return {
            "reply": "No table selected. Select one or more tables first.",
            "payload": {},
        }
    tables = [str(t) for t in (selected_tables if selected_tables else [table])]
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
    blocks: List[str] = []
    schema_map: Dict[str, Any] = {}
    for t in tables[:10]:
        cols = conn.get_table_schema(t)
        schema_map[str(t)] = cols
        # Markdown table for clean UI rendering.
        rows_md = []
        for i, c in enumerate(cols[:200]):
            name = str(c.get("name") or "")
            typ = str(c.get("type") or "")
            nul = str(c.get("nullable") or "")
            rows_md.append(f"| {i+1} | `{name}` | `{typ}` | {nul} |")
        blocks.append(
            f"### Schema — `{t}`\n\n"
            "| # | Column | Type | Nullable |\n"
            "|---:|--------|------|:--------:|\n"
            + ("\n".join(rows_md) if rows_md else "|  |  |  |  |")
            + (f"\n\n_…(+{len(cols)-200} more columns)_" if len(cols) > 200 else "")
        )
    if len(tables) > 10:
        blocks.append(f"_…(+{len(tables) - 10} more tables)_")

    # Themed HTML schema
    html_parts: List[str] = []
    for t in tables[:10]:
        cols = schema_map.get(str(t)) or []
        trows = []
        for i, c in enumerate((cols or [])[:200]):
            trows.append([i + 1, c.get("name"), c.get("type"), c.get("nullable")])
        html_parts.append(f"<h2>{str(t)}</h2>" + _html_table(["#", "Column", "Type", "Nullable"], trows))
        if isinstance(cols, list) and len(cols) > 200:
            html_parts.append(f"<p class='muted'>…(+{len(cols)-200} more columns)</p>")
    ui_html = _theme_wrap_html(title="Schema", body_html="".join(html_parts) if html_parts else "<p class='muted'>(none)</p>")
    return {
        "reply": "\n\n".join(blocks),
        "payload": {
            "step": "view_query",
            "schemas": schema_map,
            "ui_html": ui_html,
            "options": _flow_options(
                {"id": "head", "text": "📊 Next 10 rows", "send": "preview table"},
                {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
                {"id": "report", "text": "📄 Generate report", "send": "generate report"},
                {"id": "menu", "text": "📋 Menu", "send": "menu"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


def _node_preview_table(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    sel = ctx.get("selected_tables") or []
    if not isinstance(sel, list):
        sel = []
    table = ctx.get("selected_table")
    # If multiple tables are selected, default previews/paging to the first table.
    if not table and sel:
        table = sel[0]
        ctx["selected_table"] = str(table)
    if not table and len(sel) == 1:
        table = sel[0]
        ctx["selected_table"] = str(table)
    if not table:
        return {"reply": "No table selected. Select one or more tables first.", "payload": {}}
    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    db_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "database"]
    if not db_locs:
        return {"reply": "No database source configured.", "payload": {}}
    db_idx = int(ctx.get("selected_db_location_index") or 0)
    db_idx = max(0, min(db_idx, len(db_locs) - 1))
    conn_cfg = db_locs[db_idx].get("connection") or {}
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector

    conn = AzureSQLPythonNetConnector(conn_cfg)
    args = state.get("action_args") or {}
    n = args.get("n") or args.get("rows") or args.get("limit")
    try:
        n = int(n) if n is not None else 10
    except Exception:
        n = 10
    n = max(1, min(n, 50))

    offset = int(ctx.get("table_preview_offset") or 0)
    offset = max(0, offset)

    # Use a stable-ish paging strategy without requiring a known ordering column.
    # Note: Without an ORDER BY on a deterministic key, SQL Server does not guarantee consistent ordering across calls.
    try:
        from connectors.azure_sql_pythonnet import SqlCommand

        conn_raw = conn._connect()
        conn_raw.Open()
        try:
            table_q = conn._quote_two_part_name(table)
            sql = f"""
WITH numbered AS (
  SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS __rn
  FROM {table_q}
)
SELECT * FROM numbered
WHERE __rn > @offset AND __rn <= (@offset + @limit)
ORDER BY __rn
"""
            cmd = SqlCommand(sql, conn_raw)
            cmd.Parameters.AddWithValue("@offset", int(offset))
            cmd.Parameters.AddWithValue("@limit", int(n))
            reader = cmd.ExecuteReader()
            df = conn._read_reader_to_df(reader)
            if "__rn" in df.columns:
                df = df.drop(columns=["__rn"])
        finally:
            conn_raw.Close()
    except Exception:
        # Fallback to TOP N if paging query fails.
        df = conn.preview_table(table, n)
    # lightweight preview
    cols = list(df.columns)
    rows = df.head(n).to_dict(orient="records")
    from agent.pii_masking import mask_rows
    rows = mask_rows(rows)

    rows_text = json.dumps(rows, ensure_ascii=False, indent=2)
    # Advance offset by how many rows we returned (even if fewer than requested).
    next_offset = offset + len(rows)
    ctx["table_preview_offset"] = next_offset
    # After the first page, switch the button label to "Next 10 rows".
    head_label = "📊 Next 10 rows" if next_offset > 0 else "📊 View top 10 rows"
    # Themed HTML preview
    trows = []
    for r in rows[:50]:
        if isinstance(r, dict):
            trows.append([r.get(c) for c in cols])
        else:
            trows.append([json.dumps(r, ensure_ascii=False)])
    body_html = _html_table(cols if cols else ["row"], trows)
    ui_html = _theme_wrap_html(title=f"Preview — {table}", body_html=body_html)
    return {
        "reply": f"Preview of {table} (rows {offset + 1}–{offset + len(rows)}). Columns: {', '.join(cols[:30])}\n\n{rows_text}",
        "payload": {
            "step": "view_query",
            "table": str(table),
            "columns": cols,
            "rows": rows,
            "count": len(rows),
            "preview_offset": next_offset,
            "ui_html": ui_html,
            "options": _flow_options(
                {"id": "head", "text": head_label, "send": "preview table"},
                {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
                {"id": "meta", "text": "ℹ️ Show metadata", "send": "show metadata"},
                {"id": "report", "text": "📄 Generate report", "send": "generate report"},
                {"id": "menu", "text": "📋 Menu", "send": "menu"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


def _node_show_metadata(state: ChatState) -> ChatState:
    ctx = state["session"].setdefault("context", {})
    selected_tables = ctx.get("selected_tables") or []
    if not isinstance(selected_tables, list):
        selected_tables = []

    table = ctx.get("selected_table")
    if not table and len(selected_tables) == 1:
        table = selected_tables[0]
        ctx["selected_table"] = str(table)
    if not table and not selected_tables:
        return {"reply": "No table selected. Select one or more tables first.", "payload": {}}
    tables = [str(t) for t in (selected_tables if selected_tables else [table])]

    sources_path = state["session"].get("context", {}).get("sources_path") or "config/sources.yaml"
    source_root = load_sources_config(sources_path)
    db_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "database"]
    if not db_locs:
        return {"reply": "No database source configured.", "payload": {}}
    db_idx = int(ctx.get("selected_db_location_index") or 0)
    db_idx = max(0, min(db_idx, len(db_locs) - 1))
    conn_cfg = db_locs[db_idx].get("connection") or {}

    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector, SqlCommand

    conn = AzureSQLPythonNetConnector(conn_cfg)

    def _split_schema_name(t: str) -> tuple[str, str]:
        if "." in t:
            a, b = t.split(".", 1)
            return a, b
        return "dbo", t

    def _get_row_count(full_name: str) -> int | None:
        try:
            c = conn._connect()
            c.Open()
            try:
                cmd = SqlCommand(
                    """
SELECT SUM(row_count) AS row_count
FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID(@full_name)
  AND index_id IN (0, 1)
""",
                    c,
                )
                cmd.Parameters.AddWithValue("@full_name", full_name)
                reader = cmd.ExecuteReader()
                if reader.Read() and not reader.IsDBNull(0):
                    return int(reader.GetValue(0))
                return None
            finally:
                c.Close()
        except Exception:
            return None

    meta: Dict[str, Any] = {}
    rows_md: List[str] = []
    for t in tables[:15]:
        sch, nm = _split_schema_name(t)
        cols = conn.get_table_schema(t)
        col_count = len(cols)
        nullable = sum(1 for c in cols if str(c.get("nullable") or "").lower() in ("yes", "true", "1"))
        rc = _get_row_count(f"{sch}.{nm}")
        meta[t] = {"row_count": rc, "column_count": col_count, "nullable_columns": nullable}
        rc_txt = f"{rc:,}" if isinstance(rc, int) else "unavailable"
        rows_md.append(f"| `{sch}.{nm}` | {rc_txt} | {col_count} | {nullable} |")

    offset_now = int(ctx.get("table_preview_offset") or 0)
    reply = (
        "### Metadata — selected tables\n\n"
        "| Table | Rows (approx) | Columns | Nullable cols |\n"
        "|------|--------------:|--------:|--------------:|\n"
        + ("\n".join(rows_md) if rows_md else "|  |  |  |  |")
        + (f"\n\n_…(+{len(tables) - 15} more tables)_" if len(tables) > 15 else "")
        + f"\n\n_Current preview offset_: **{offset_now}**"
    )
    return {
        "reply": reply,
        "payload": {
            "tables": tables,
            "metadata": meta,
            "ui_html": _theme_wrap_html(
                title="Metadata",
                body_html=_html_table(
                    ["Table", "Rows (approx)", "Columns", "Nullable cols"],
                    [
                        [k, v.get("row_count"), v.get("column_count"), v.get("nullable_columns")]
                        for k, v in (meta or {}).items()
                    ],
                ),
            ),
            "options": _flow_options(
                {"id": "head", "text": "📊 Next 10 rows", "send": "preview table"},
                {"id": "schema", "text": "📊 Show schema", "send": "show schema"},
                {"id": "report", "text": "📄 Generate report", "send": "generate report"},
                {"id": "menu", "text": "📋 Menu", "send": "menu"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
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
    df = conn.load_table(table)
    profile = profile_dataframe(df)
    thresholds = load_dq_thresholds()
    dq = analyze_dataset_quality(table, df, profile, thresholds)
    summ = dq.get("summary") or {}
    reply = (
        f"Data quality summary for {table}: "
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

        sql = nl_to_sql_select(question=question, table=table, columns=cols, max_rows=None)
    except Exception as e:
        return {
            "reply": f"I can't translate your question to SQL yet: {e}",
            "payload": {},
        }
    try:
        df = conn.execute_select(sql, max_rows=None)
        rows = df.head(50).to_dict(orient="records")
        from agent.pii_masking import mask_rows
        rows = mask_rows(rows)
        return {"reply": f"Ran query on {table}. Returned {len(rows)} rows (showing up to 50).", "payload": {"sql": sql, "rows": rows}}
    except Exception as e:
        return {"reply": f"SQL execution failed: {e}", "payload": {"sql": sql}}


def _node_save_session(state: ChatState) -> ChatState:
    sess = state.get("session") or {}
    # Track last UI step for deterministic back behavior.
    try:
        ctx = sess.setdefault("context", {})
        if isinstance(ctx, dict):
            p = state.get("payload") or {}
            step = p.get("step") if isinstance(p, dict) else None
            if step:
                ctx["last_ui_step"] = str(step)
    except Exception:
        pass
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
    g.add_node("preview_selected_file", _node_preview_selected_file)
    g.add_node("show_file_schema", _node_show_file_schema)
    g.add_node("show_file_metadata", _node_show_file_metadata)
    g.add_node("generate_report_selected_files", _node_generate_report_selected_files)
    g.add_node("show_schema", _node_show_schema)
    g.add_node("preview_table", _node_preview_table)
    g.add_node("show_metadata", _node_show_metadata)
    g.add_node("generate_report_selected", _node_generate_report_selected)
    g.add_node("show_cleaning_recommendations", _node_show_cleaning_recommendations)
    g.add_node("show_transform_suggestions", _node_show_transform_suggestions)
    g.add_node("dq_table", _node_dq_table)
    g.add_node("nl_query", _node_nl_query)
    g.add_node("show_null_columns", _node_show_null_columns)
    g.add_node("dq_overview", _node_dq_overview)
    g.add_node("dq_duplicates", _node_dq_duplicates)
    g.add_node("extract_columns", _node_extract_columns)
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
            "preview_selected_file": "preview_selected_file",
            "show_file_schema": "show_file_schema",
            "show_file_metadata": "show_file_metadata",
            "generate_report_selected_files": "generate_report_selected_files",
            "show_schema": "show_schema",
            "preview_table": "preview_table",
            "show_metadata": "show_metadata",
            "generate_report_selected": "generate_report_selected",
            "show_cleaning_recommendations": "show_cleaning_recommendations",
            "show_transform_suggestions": "show_transform_suggestions",
            "dq_table": "dq_table",
            "nl_query": "nl_query",
            "show_null_columns": "show_null_columns",
            "dq_overview": "dq_overview",
            "dq_duplicates": "dq_duplicates",
            "extract_columns": "extract_columns",
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
        "preview_selected_file",
        "show_file_schema",
        "show_file_metadata",
        "generate_report_selected_files",
        "show_schema",
        "preview_table",
        "show_metadata",
        "generate_report_selected",
        "show_cleaning_recommendations",
        "show_transform_suggestions",
        "dq_table",
        "nl_query",
        "show_null_columns",
        "dq_overview",
        "dq_duplicates",
        "extract_columns",
    ):
        g.add_edge(n, "save_session")
    g.add_edge("save_session", END)
    return g.compile()


def run_chat(*, session_id: str, message: str) -> Dict[str, Any]:
    graph = build_chat_graph()
    out = graph.invoke({"session_id": session_id, "message": message})
    return dict(out)

