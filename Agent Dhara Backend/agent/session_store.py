from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional


def _db_path() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(root, "output")
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, "chat_sessions.sqlite3")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), timeout=30)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
          session_id TEXT PRIMARY KEY,
          created_at REAL NOT NULL,
          updated_at REAL NOT NULL,
          payload_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS experiences (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          session_id TEXT NOT NULL,
          ts REAL NOT NULL,
          user_text TEXT,
          action TEXT,
          success INTEGER,
          notes TEXT,
          FOREIGN KEY(session_id) REFERENCES sessions(session_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_experiences_session_ts ON experiences(session_id, ts DESC)")
    return conn


# FIX (2026-05-07): New sessions now initialise last_step as
# 'awaiting_source_selection' instead of 'unknown'.  This ensures the
# guard_fresh_session_fallback() in routing_guards.py fires correctly and
# the frontend status display never shows 'unknown'.
_DEFAULT_SESSION_PAYLOAD: Dict[str, Any] = {
    "selected_source": None,
    "selected_blob_files": [],
    "selected_local_files": [],
    "selected_tables": [],
    "selected_table": None,
    "last_assessment_result": None,
    "last_assessment_signature": None,
    "last_assessment_datasets": [],
    "last_step": "awaiting_source_selection",  # FIX: was absent / 'unknown'
    "selected_db_location_index": None,
    "selected_blob_location_index": None,
    "selected_fs_location_index": None,
}


def load_session(session_id: str) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    now = time.time()
    conn = _connect()
    try:
        row = conn.execute("SELECT payload_json FROM sessions WHERE session_id = ?", (sid,)).fetchone()
        if not row:
            # Brand-new session — persist immediately with correct defaults
            payload = dict(_DEFAULT_SESSION_PAYLOAD)
            conn.execute(
                "INSERT INTO sessions (session_id, created_at, updated_at, payload_json) VALUES (?,?,?,?)",
                (sid, now, now, json.dumps(payload)),
            )
            conn.commit()
            return payload
        payload = json.loads(row[0])
        # Back-fill missing last_step for sessions created before this fix
        if payload.get("last_step") in (None, "", "unknown") and not payload.get("selected_source"):
            payload["last_step"] = "awaiting_source_selection"
        return payload
    finally:
        conn.close()


def save_session(session_id: str, payload: Dict[str, Any]) -> None:
    sid = (session_id or "default").strip() or "default"
    now = time.time()
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO sessions (session_id, created_at, updated_at, payload_json)
            VALUES (?,?,?,?)
            ON CONFLICT(session_id) DO UPDATE SET updated_at=excluded.updated_at,
                                                   payload_json=excluded.payload_json
            """,
            (sid, now, now, json.dumps(payload)),
        )
        conn.commit()
    finally:
        conn.close()


def reset_session(session_id: str) -> Dict[str, Any]:
    """Wipe a session back to clean defaults and persist."""
    payload = dict(_DEFAULT_SESSION_PAYLOAD)
    save_session(session_id, payload)
    return payload


def log_experience(
    session_id: str,
    user_text: Optional[str],
    action: Optional[str],
    success: bool,
    notes: Optional[str] = None,
) -> None:
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO experiences (session_id, ts, user_text, action, success, notes) VALUES (?,?,?,?,?,?)",
            (sid, time.time(), user_text, action, int(success), notes),
        )
        conn.commit()
    finally:
        conn.close()


def get_experiences(
    session_id: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT ts, user_text, action, success, notes FROM experiences "
            "WHERE session_id = ? ORDER BY ts DESC LIMIT ?",
            (sid, limit),
        ).fetchall()
        return [
            {
                "ts": r[0],
                "user_text": r[1],
                "action": r[2],
                "success": bool(r[3]),
                "notes": r[4],
            }
            for r in rows
        ]
    finally:
        conn.close()
