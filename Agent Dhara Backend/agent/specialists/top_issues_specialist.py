from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple


def _sev_rank(s: str) -> int:
    s = (s or "").lower()
    if s == "high":
        return 3
    if s == "medium":
        return 2
    if s == "low":
        return 1
    return 0


def _issue_count(iss: Dict[str, Any]) -> int:
    c = iss.get("count")
    try:
        if c is not None and str(c).strip() != "" and str(c) != "-":
            return int(c)
    except Exception:
        pass
    return 0


def _collect_flat(result: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
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
            if isinstance(iss, dict):
                out.append((str(ds_name), iss))
    return out


def _parse_top_n(user_message: str, default: int = 5) -> int:
    m = re.search(r"top\s*(\d+)", user_message.lower())
    if m:
        try:
            return max(1, min(25, int(m.group(1))))
        except Exception:
            pass
    if re.search(r"\bfive\b|\b5\b", user_message.lower()) and "issue" in user_message.lower():
        return 5
    return default


def format_top_issues(result: Dict[str, Any], user_message: str = "") -> str:
    """
    Ranked top-N issues only (no full profile tables).
    """
    flat = _collect_flat(result)
    if not flat:
        return "No per-dataset issues were found in the latest assessment."

    flat.sort(
        key=lambda it: (_sev_rank(str(it[1].get("severity") or "")), _issue_count(it[1])),
        reverse=True,
    )

    n = _parse_top_n(user_message or "", 5)
    lines: List[str] = [
        f"### Top **{n}** data-quality issues (ranked)",
        "",
        "_Grounded in the latest assessment only — not invented._",
        "",
    ]
    used = 0
    seen_key: set[str] = set()
    for ds, iss in flat:
        if used >= n:
            break
        typ = str(iss.get("type") or "")
        sev = str(iss.get("severity") or "")
        col = iss.get("column")
        cnt = _issue_count(iss)
        msg = str(iss.get("message") or iss.get("detail") or "").strip().replace("\n", " ")
        if len(msg) > 120:
            msg = msg[:117] + "..."
        key = f"{ds}|{typ}|{col}|{sev}"
        if key in seen_key:
            continue
        seen_key.add(key)
        used += 1
        col_s = f"`{col}`" if col else "(dataset-wide)"
        cnt_s = f"~{cnt} rows" if cnt > 0 else "count n/a"
        impact = "May break joins, typing, or downstream metrics." if sev.lower() == "high" else "Worth fixing before broad consumption."
        lines.append(f"{used}. **`{typ}`** ({sev.upper()}) — {cnt_s} — column {col_s} — _{msg}_ — {impact}")
    return "\n".join(lines)
