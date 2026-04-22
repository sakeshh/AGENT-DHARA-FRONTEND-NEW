"""
Central settings for MCP servers and in-process clients.

Connection strings live in `config/sources.yaml` (same as `main.py`), with optional
`${ENV_VAR}` expansion. Override paths via environment variables below.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class MCPRuntimeSettings:
    """Paths and flags; credentials are read from sources.yaml, not duplicated here."""

    project_root: str
    sources_path: str

    @classmethod
    def from_env(cls) -> "MCPRuntimeSettings":
        root = os.environ.get("AGENT_DHARA_PROJECT_ROOT", "").strip()
        if not root:
            root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        src = os.environ.get("AGENT_DHARA_SOURCES_PATH", "").strip()
        if not src:
            src = os.path.join(root, "config", "sources.yaml")
        elif not os.path.isabs(src):
            src = os.path.normpath(os.path.join(root, src))
        return cls(project_root=os.path.abspath(root), sources_path=os.path.abspath(src))
