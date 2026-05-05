from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _collect_for_filter(result: Dict[str, Any], wanted_types: set[str]) -> List[Tuple[str, Dict[str, Any]]]:
    out: List[Tuple[str, Dict[str, Any]]] = []
    dq = (result.get("data_quality_issues") or {}) if isinstance(result, dict) else {}
    per = dq.get("datasets") or {}
    if not isinstance(per, dict):
        return out
    for ds_name, block in per.items():
        issues = (block or {}).get("issues") or []
        if not isinstance(issues, list):
            continue
        for iss in issues:
            if not isinstance(iss, dict):
                continue
            t = str(iss.get("type") or "").lower()
            if not wanted_types or t in wanted_types or any(w in t for w in wanted_types):
                out.append((str(ds_name), iss))
    return out


def _infer_filter_types(user_message: str) -> Tuple[set[str], str]:
    low = user_message.lower()
    types: set[str] = set()
    label = "filtered"
    if "null" in low or "missing" in low:
        types.update({"nulls", "null"})
        label = "null / missing"
    if "duplicate" in low:
        types.update({"duplicate_rows", "duplicate_primary_key"})
        label = "duplicate-related" if "null" not in low else label + " + duplicates"
    if "email" in low:
        types.add("invalid_email")
        label = "email-related"
    if "phone" in low:
        types.update({"invalid_phone", "mixed_phone_formats"})
        label = "phone-related"
    if "identifier" in low or "primary key" in low or " pk" in low:
        types.update({"duplicate_primary_key", "suspicious_zero"})
        label = "identifier / PK-related"
    return types, label


def format_issue_filter(result: Dict[str, Any], user_message: str) -> str:
    wanted, label = _infer_filter_types(user_message)
    if not wanted:
        return (
            "### Which issue family?\n\n"
            "Say one of: **nulls**, **duplicates**, **emails**, **phones**, or paste a **column name**.\n\n"
            "_I’m not showing the full report here — tell me the slice you want._"
        )
    rows = _collect_for_filter(result, wanted)
    if not rows:
        return f"No issues matched **{label}** in the latest assessment (or no filter detected — try: “null issues”, “duplicate issues”, “email issues”)."

    lines = [
        f"### {label.title()} issues",
        "",
        "| Dataset | Type | Severity | Column | Count | Message |",
        "|---|---|:--:|---|---:|---|",
    ]
    for ds, iss in rows[:40]:
        t = str(iss.get("type") or "")
        sev = str(iss.get("severity") or "")
        col = iss.get("column") or ""
        cnt = iss.get("count", "")
        msg = str(iss.get("message") or "")[:80].replace("|", "/").replace("\n", " ")
        lines.append(f"| `{ds}` | `{t}` | {sev} | `{col}` | {cnt} | {msg} |")
    if len(rows) > 40:
        lines.append(f"\n_…{len(rows) - 40} more rows omitted._")
    lines.append("")
    lines.append("_ETL: quarantine flagged rows, fix upstream capture, then re-assess._")
    return "\n".join(lines)
