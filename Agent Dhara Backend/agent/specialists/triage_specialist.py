from __future__ import annotations

from typing import Any, Dict, List, Tuple

from agent.specialists.top_issues_specialist import _collect_flat, _issue_count


def format_triage(result: Dict[str, Any], user_message: str = "") -> str:
    """
    Time-boxed / ETL-first triage from HIGH → MEDIUM → LOW themes.
    """
    flat = _collect_flat(result)
    if not flat:
        return "No issues available for triage in the latest assessment."

    highs: List[Tuple[str, Dict[str, Any]]] = []
    meds: List[Tuple[str, Dict[str, Any]]] = []
    lows: List[Tuple[str, Dict[str, Any]]] = []
    for ds, iss in flat:
        sev = str(iss.get("severity") or "").lower()
        if sev == "high":
            highs.append((ds, iss))
        elif sev == "medium":
            meds.append((ds, iss))
        else:
            lows.append((ds, iss))

    highs.sort(key=lambda it: _issue_count(it[1]), reverse=True)
    meds.sort(key=lambda it: _issue_count(it[1]), reverse=True)
    lows.sort(key=lambda it: _issue_count(it[1]), reverse=True)

    def _lines(title: str, items: List[Tuple[str, Dict[str, Any]]], k: int) -> List[str]:
        out: List[str] = [f"#### {title}", ""]
        if not items:
            out.append("_None in this scan._\n")
            return out
        for ds, iss in items[:k]:
            typ = str(iss.get("type") or "")
            col = iss.get("column") or "?"
            cnt = _issue_count(iss)
            why = str(iss.get("recommendation") or iss.get("message") or "")[:140].replace("\n", " ")
            out.append(f"- **`{typ}`** on `{ds}` · column `{col}` · ~{cnt} rows — {why}")
        out.append("")
        return out

    parts: List[str] = [
        "### 2-hour ETL triage (priority order)",
        "",
        "**HIGH impact — fix first** (blocks reliable loads / joins / typing):",
        "",
    ]
    parts.extend(_lines("Now", highs, 8))
    parts.append("**MEDIUM impact — fix today** (analytics drift, inconsistent formats):")
    parts.append("")
    parts.extend(_lines("Today", meds, 6))
    parts.append("**LOW impact — schedule** (cosmetic / low blast radius):")
    parts.append("")
    parts.extend(_lines("Later", lows, 5))
    parts.append("_Grounded in the latest assessment; re-run after each cleaning wave._")
    return "\n".join(parts)
