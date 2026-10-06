"""get_cpu_profile reads the backend's in-process thread sample."""
import json

import pytest
from unittest.mock import AsyncMock, patch

from mcp.server.fastmcp import FastMCP


def _register_system() -> FastMCP:
    mcp = FastMCP("test")
    from tools.system import register

    register(mcp)
    return mcp


def _text(result) -> str:
    return result[0][0].text


@pytest.mark.asyncio
async def test_cpu_profile_forwards_the_sample_length_and_returns_the_report():
    profile = {
        "seconds": 5.0, "cpu_percent": 97.5, "samples": 240, "busy_thread_samples": 230,
        "top": [{"frame": "programme_times (services/epg_programmes.py:260)", "share_percent": 64.2}],
    }
    client = AsyncMock()
    client.call_endpoint.return_value = profile
    with patch("tools.system.get_ecm_client", return_value=client):
        result = await _register_system().call_tool("get_cpu_profile", {"seconds": 5})
    assert json.loads(_text(result)) == profile
    call = client.call_endpoint.call_args
    assert call.args[0].name == "health_cpu_profile"
    assert call.args[0].method == "GET"
    assert call.kwargs == {"query": {"seconds": 5}}
