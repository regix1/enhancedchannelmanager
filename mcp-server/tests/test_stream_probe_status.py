"""get_stream_probe_status reads stored test-play stats and leaves probe errors out."""
import json

import pytest
from unittest.mock import AsyncMock, patch

from mcp.server.fastmcp import FastMCP


def _register_streams() -> FastMCP:
    mcp = FastMCP("test")
    from tools.streams import register

    register(mcp)
    return mcp


def _text(result) -> str:
    return result[0][0].text


@pytest.mark.asyncio
async def test_probe_status_reports_stored_results_and_missing_streams():
    client = AsyncMock()
    client.call_endpoint.return_value = {
        "2215148": {
            "stream_id": 2215148,
            "stream_name": "NEXT | THE GOLICS | Tue 06 Oct 10:00 EDT (US)",
            "probe_status": "failed",
            "error_message": "403 Forbidden http://user:secret@provider/2215148.ts",
            "last_probed": "2026-10-06T15:05:00Z",
            "measured_bitrate": 0,
            "is_black_screen": False,
            "black_screen_checked_at": None,
            "consecutive_failures": 2,
            "resolution": None,
        },
    }
    with patch("tools.streams.get_ecm_client", return_value=client):
        result = await _register_streams().call_tool(
            "get_stream_probe_status", {"stream_ids": [2215148, 2215920]},
        )
    report = json.loads(_text(result))
    assert report["missing"] == [2215920]
    assert report["streams"]["2215148"]["probe_status"] == "failed"
    assert "error_message" not in report["streams"]["2215148"]
    assert "secret" not in _text(result)
    call = client.call_endpoint.call_args
    assert call.args[0].name == "stream_stats_by_ids"
    assert call.kwargs == {"body": {"stream_ids": [2215148, 2215920]}}
