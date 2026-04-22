from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _pct(x: Any) -> str:
    try:
        v = float(x)
        return f"{round(v * 100, 2)}%"
    except Exception:
        return "—"


def _md_table(rows: List[List[str]], headers: List[str]) -> str:
    if not headers:
        return ""
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for r in rows:
        rr = (r + [""] * len(headers))[: len(headers)]
        out.append("| " + " | ".join(rr) + " |")
    return "\n".join(out)


def _dq_rollup(result: Dict[str, Any]) -> Tuple[int, int, int]:
    rm = (result.get("run_metadata") or {}).get("dq_issue_totals") or {}
    return _int(rm.get("high")), _int(rm.get("medium")), _int(rm.get("low"))


def _datasets_summary(result: Dict[str, Any]) -> Tuple[int, int, int]:
    datasets = result.get("datasets") or {}
    if not isinstance(datasets, dict):
        return 0, 0, 0
    total_datasets = len(datasets)
    total_rows = 0
    total_cols = 0
    for meta in datasets.values():
        if not isinstance(meta, dict):
            continue
        total_rows += _int(meta.get("row_count"))
        total_cols += _int(meta.get("column_count"))
    return total_datasets, total_rows, total_cols


def _collect_issue_rows(result: Dict[str, Any], *, limit_per_dataset: int = 12) -> List[Dict[str, Any]]:
    dq = result.get("data_quality_issues") or {}
    ds = (dq.get("datasets") or {}) if isinstance(dq, dict) else {}
    rows: List[Dict[str, Any]] = []
    if not isinstance(ds, dict):
        return rows
    for ds_name, block in ds.items():
        if not isinstance(block, dict):
            continue
        issues = block.get("issues") or []
        if not isinstance(issues, list):
            continue
        for it in issues[: max(0, limit_per_dataset)]:
            if not isinstance(it, dict):
                continue
            rows.append(
                {
                    "dataset": str(ds_name),
                    "type": str(it.get("type") or ""),
                    "severity": str(it.get("severity") or ""),
                    "column": str(it.get("column") or ""),
                    "message": str(it.get("message") or ""),
                    "recommendation": str(it.get("recommendation") or ""),
                }
            )
    return rows


def _top_issue_types(issue_rows: List[Dict[str, Any]], *, top_n: int = 6) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = {}
    for r in issue_rows:
        t = (r.get("type") or "").strip() or "unknown"
        counts[t] = counts.get(t, 0) + 1
    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))[: max(0, top_n)]


def _recommendations(issue_rows: List[Dict[str, Any]], *, top_n: int = 8) -> List[str]:
    seen = set()
    recs: List[str] = []
    for r in issue_rows:
        rec = (r.get("recommendation") or "").strip()
        if not rec:
            continue
        # keep it compact
        rec = rec.replace("\n", " ").strip()
        if rec in seen:
            continue
        seen.add(rec)
        recs.append(rec)
        if len(recs) >= top_n:
            break
    if recs:
        return recs
    # fallback general advice if no recommendations exist in payload
    return [
        "Standardize formats (dates → ISO-8601, numerics → numeric types) and validate at ingestion.",
        "Trim leading/trailing whitespace and normalize placeholder values to NULL.",
        "Add referential integrity checks (FK constraints or orphan detection) in downstream pipelines.",
    ][:top_n]


def format_assessment_report(result: Dict[str, Any]) -> str:
    """
    Deterministic (no-LLM) formatter for assessment results.
    Produces a consistent, stakeholder-friendly Markdown report.
    """
    total_datasets, total_rows, total_cols = _datasets_summary(result)
    high, med, low = _dq_rollup(result)
    issue_rows = _collect_issue_rows(result, limit_per_dataset=20)
    top_types = _top_issue_types(issue_rows)

    # Per dataset breakdown (metadata + tiny issue rollup)
    datasets = result.get("datasets") or {}
    dq = result.get("data_quality_issues") or {}
    dq_ds = (dq.get("datasets") or {}) if isinstance(dq, dict) else {}

    breakdown_lines: List[str] = []
    if isinstance(datasets, dict) and datasets:
        for name, meta in datasets.items():
            if not isinstance(meta, dict):
                continue
            rows = _int(meta.get("row_count"))
            cols = _int(meta.get("column_count"))
            bytes_ = _int(meta.get("data_volume_bytes"))
            summ = ((dq_ds.get(name) or {}).get("summary") or {}) if isinstance(dq_ds, dict) else {}
            breakdown_lines.append(
                f"- **{name}**\n"
                f"  - Metadata: {rows} rows, {cols} columns, {bytes_} bytes\n"
                f"  - DQ rollup: high={_int(summ.get('high_severity'))}, medium={_int(summ.get('medium_severity'))}, low={_int(summ.get('low_severity'))}"
            )

    # Table of issues (compact)
    table_rows: List[List[str]] = []
    for r in issue_rows[:60]:
        table_rows.append(
            [
                r.get("type") or "—",
                (r.get("message") or "—")[:180],
                f"`{r.get('dataset')}`" if r.get("dataset") else "—",
                r.get("severity") or "—",
            ]
        )

    md: List[str] = []
    md.append("## 1. Executive Summary\n")
    md.append(
        f"The assessment covers **{total_datasets}** dataset(s) with approximately **{total_rows}** rows and **{total_cols}** columns.\n"
        f"Overall data quality signals show **high={high}**, **medium={med}**, **low={low}** issue counts."
    )

    md.append("\n## 2. High-Level Findings\n")
    if top_types:
        md.append("**Most frequent issue types (sample-based):**")
        for t, c in top_types:
            md.append(f"- **{t}**: {c}")
    else:
        md.append("- No issues were detected in the sampled assessment output.")

    md.append("\n## 3. Detailed Analysis\n")
    md.append("- **Completeness**: check for null/placeholder spikes and upstream capture gaps.\n"
              "- **Consistency**: normalize whitespace, casing, and mixed-type columns.\n"
              "- **Validity**: enforce date/numeric parsing rules and reject invalid formats.\n"
              "- **Integrity**: review cross-dataset orphan keys and relationship warnings (if any).")

    md.append("\n## 4. File / Source Breakdown\n")
    md.append("\n".join(breakdown_lines) if breakdown_lines else "- (no dataset metadata available)")

    md.append("\n## 5. Data Quality Issues (Tabular Format)\n")
    md.append(
        _md_table(
            table_rows,
            headers=["Issue Type", "Description", "Columns / Files", "Severity"],
        )
        or "_(no issues table)_"
    )

    md.append("\n## 6. Recommendations\n")
    for rec in _recommendations(issue_rows):
        md.append(f"- {rec}")

    return "\n".join(md).strip() + "\n"

