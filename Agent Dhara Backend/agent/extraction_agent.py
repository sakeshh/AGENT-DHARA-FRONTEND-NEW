"""
Extraction agent.

Matches the planned flow chart:
User -> Supervisor/Master Agent -> Extract Agent -> {Azure SQL MCP, Blob MCP, Local FS MCP, Stream MCP}

The "MCPs" here are explicit in-process adapters (`agent.mcp_clients`) that reuse the
existing MCP-facing logic in `agent.mcp_interface.py`.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from agent.mcp_clients import StreamMCP, mcp_for_location_type


def _location_display_name(loc: Dict[str, Any], idx: int) -> str:
    for k in ("id", "label", "name"):
        v = loc.get(k)
        if v:
            return str(v)
    t = (loc.get("type") or "location").lower()
    return f"{t}_{idx}"


@dataclass(frozen=True)
class ExtractionResult:
    """
    Output of a single-source extraction run.

    `result` is the return value of `agent.mcp_interface.run_assessment()` for a config containing
    only the one selected location. That structure includes datasets, relationships, and DQ issues.
    """

    source_name: str
    location_type: str
    result: Dict[str, Any]


class ExtractionAgent:
    """
    Triggers extraction for selected source locations.

    Each location is extracted independently, which allows parallel execution and
    per-source error isolation.
    """

    def __init__(self) -> None:
        # MCP adapters are created per location type (database/blob/filesystem/stream).
        pass

    def extract_one(self, source_root: Dict[str, Any], loc: Dict[str, Any], idx: int) -> ExtractionResult:
        """
        Synchronous single-source extraction.
        """
        location_type = str((loc.get("type") or "")).lower()
        mcp = mcp_for_location_type(location_type)
        if isinstance(mcp, StreamMCP):
            raise ValueError("Stream sources require records input; use extract_stream_records().")
        res = mcp.extract(source_root=source_root, location=loc)
        return ExtractionResult(
            source_name=_location_display_name(loc, idx),
            location_type=location_type,
            result=res,
        )

    def extract_stream_records(self, *, records: List[Dict[str, Any]], name: str = "stream") -> ExtractionResult:
        """
        Stream extraction (does not rely on sources.yaml).
        """
        mcp = StreamMCP()
        res = mcp.extract_records(records=records, name=name)
        return ExtractionResult(source_name=name, location_type="stream", result=res)

    async def extract_many(
        self,
        *,
        source_root: Dict[str, Any],
        locations: List[Dict[str, Any]],
        parallel: bool = True,
        stream_records: Optional[List[Dict[str, Any]]] = None,
        stream_name: str = "stream",
    ) -> Tuple[List[ExtractionResult], List[Dict[str, Any]]]:
        """
        Extract multiple locations.

        Returns (results, errors) where errors are JSON-serializable dicts.
        """
        results: List[ExtractionResult] = []
        errors: List[Dict[str, Any]] = []

        if stream_records is not None:
            try:
                results.append(self.extract_stream_records(records=stream_records, name=stream_name))
            except Exception as e:
                errors.append({"source": stream_name, "type": "stream", "error": str(e)})

        async def _run(idx: int, loc: Dict[str, Any]) -> None:
            try:
                r = await asyncio.to_thread(self.extract_one, source_root, loc, idx)
                results.append(r)
            except Exception as e:
                errors.append(
                    {
                        "source": _location_display_name(loc, idx),
                        "type": str((loc.get("type") or "")).lower(),
                        "error": str(e),
                    }
                )

        if parallel:
            await asyncio.gather(*[_run(i, loc) for i, loc in enumerate(locations)])
        else:
            for i, loc in enumerate(locations):
                await _run(i, loc)

        return results, errors

