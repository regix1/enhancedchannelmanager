"""get_channel_pipeline_run_failures reads a run's stored log and keeps only failed actions."""
import json

import pytest
from unittest.mock import AsyncMock, patch

from mcp.server.fastmcp import FastMCP


def _register_pipeline() -> FastMCP:
    mcp = FastMCP("test")
    from tools.channel_pipeline import register

    register(mcp)
    return mcp


def _text(result) -> str:
    return result[0][0].text


@pytest.mark.asyncio
async def test_run_failures_returns_failed_actions_with_their_errors():
    run = {
        "id": 2073,
        "status": "completed_with_errors",
        "error_message": "3 action(s) failed during this run",
        "execution_log": [
            {"stream_name": "[Pass 5] Update Profile Groups", "actions_executed": [{
                "type": "dummy_epg_refresh", "description": "Update groups", "success": False,
                "error": "Failed to update dummy EPG profile groups: Guide admission changed before group update",
            }]},
            {"stream_name": "MLB 01 : Dodgers x Braves", "actions_executed": [{
                "type": "event_sync_promote", "description": "Created channel", "success": True, "error": None,
            }]},
            {"stream_name": "[Pass 5 Retry] Dodgers X Braves", "actions_executed": [{
                "type": "assign_epg", "description": "No guide row", "success": False, "error": "deferred",
            }]},
        ],
    }
    client = AsyncMock()
    client.call_endpoint.return_value = run
    with patch("tools.channel_pipeline.get_ecm_client", return_value=client):
        result = await _register_pipeline().call_tool(
            "get_channel_pipeline_run_failures", {"execution_id": 2073, "limit": 1},
        )
    report = json.loads(_text(result))
    assert report["failure_count"] == 2
    assert report["failures"] == [{
        "step": "[Pass 5] Update Profile Groups",
        "type": "dummy_epg_refresh",
        "description": "Update groups",
        "error": "Failed to update dummy EPG profile groups: Guide admission changed before group update",
    }]
    call = client.call_endpoint.call_args
    assert call.args[0].name == "ac_get_execution"
    assert call.kwargs == {"path_args": {"execution_id": 2073}, "query": {"include_log": "true"}}


@pytest.mark.asyncio
async def test_run_failures_report_the_summary_steps_missing_from_the_log():
    run = {
        "id": 2115,
        "status": "completed_with_errors",
        "error_message": (
            "3 action(s) failed during this run ('[Pass 5 EPG retry]' assign_epg: "
            "No EPG data match found; None dummy_epg_refresh: 1 staged event "
            "promotion(s) remain pending). Some channels may have been left in an "
            "inconsistent state (e.g. exclusive channel-profile membership not fully "
            "enforced). It is safe to retry manually by rerunning the pipeline."
        ),
        "execution_log": [
            {"stream_name": "[Pass 5 EPG retry]", "actions_executed": [{
                "type": "assign_epg", "description": "Assign EPG", "success": False,
                "error": "No EPG data match found",
            }]},
        ],
    }
    client = AsyncMock()
    client.call_endpoint.return_value = run
    with patch("tools.channel_pipeline.get_ecm_client", return_value=client):
        result = await _register_pipeline().call_tool(
            "get_channel_pipeline_run_failures", {"execution_id": 2115},
        )
    report = json.loads(_text(result))
    assert report["failure_count"] == 1
    assert report["summary_failure_count"] == 3
    assert report["summary_failed_steps"] == [
        "'[Pass 5 EPG retry]' assign_epg: No EPG data match found",
        "None dummy_epg_refresh: 1 staged event promotion(s) remain pending",
    ]
