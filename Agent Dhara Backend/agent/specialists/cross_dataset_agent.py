from __future__ import annotations

import re
from typing import Any, Dict, List, Set


def _dataset_names(result: Dict[str, Any]) -> Set[str]:
    names: Set[str] = set()
    ds = (result.get("datasets") or {}) if isinstance(result, dict) else {}
    if isinstance(ds, dict):
        names.update(str(k) for k in ds.keys())
    dq = (result.get("data_quality_issues") or {}) if isinstance(result, dict) else {}
    per = dq.get("datasets") or {}
    if isinstance(per, dict):
        names.update(str(k) for k in per.keys())
    return names


def _tokens_files(message: str) -> List[str]:
    return re.findall(r"[\w\.\(\)\-]+\.(?:csv|json|xml|parquet)", message, flags=re.I)


def format_cross_dataset(result: Dict[str, Any], user_message: str) -> str:
    """
    Relationships + orphan hints; flags filenames in the question that are not in-session.
    """
    have = _dataset_names(result)
    asked_files = [t for t in _tokens_files(user_message or "")]
    missing = [f for f in asked_files if f not in have]

    rels = result.get("relationships") or []
    rels = rels if isinstance(rels, list) else []

    dq = (result.get("data_quality_issues") or {}) if isinstance(result, dict) else {}
    gbl = (dq.get("global_issues") or {}) if isinstance(dq, dict) else {}
    orphans = gbl.get("orphan_foreign_keys") if isinstance(gbl, dict) else []
    orphans = orphans if isinstance(orphans, list) else []

    lines: List[str] = [
        "### Cross-dataset view (latest assessment)",
        "",
    ]
    if missing:
        lines.append(
            f"**Note:** You mentioned {', '.join('`' + m + '`' for m in missing)} — "
            f"those files are **not** in the current assessment selection. "
            f"Available datasets: {', '.join('`' + n + '`' for n in sorted(have))}."
        )
        lines.append("")

    if rels:
        lines.append("#### Inferred links (overlap scan)")
        lines.append("")
        lines.append("| Dataset A | Col A | Dataset B | Col B | Cardinality | Overlap |")
        lines.append("|---|---|---|---|---:|---:|")
        for rel in rels[:20]:
            if not isinstance(rel, dict):
                continue
            a = rel.get("dataset_a") or rel.get("from") or ""
            b = rel.get("dataset_b") or rel.get("to") or ""
            ca = rel.get("column_a") or ""
            cb = rel.get("column_b") or ""
            card = rel.get("cardinality") or ""
            ov = rel.get("overlap_count", "")
            lines.append(f"| `{a}` | `{ca}` | `{b}` | `{cb}` | {card} | {ov} |")
        lines.append("")
    else:
        lines.append("_No inferred relationships in this run._")
        lines.append("")

    if orphans:
        lines.append("#### Orphan / dangling key hints")
        for o in orphans[:15]:
            lines.append(f"- {o}")
        lines.append("")
    else:
        lines.append("**Orphan FK hints:** none detected in this scan.")
        lines.append("")

    lines.append("_Use these overlaps to sanity-check joins; validate keys with the business before production loads._")
    return "\n".join(lines)
