"""TDD tests for bd-1wq7z.19 — create_task_schedule timezone parameter.

Tests are written RED-first: they assert the fix before it exists.
After implementation the tests must turn GREEN.
"""
import pytest
import copy
import json
from unittest.mock import AsyncMock, patch

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from tools.tasks import register


def _make_client(return_value=None, side_effect=None):
    mock = AsyncMock()
    if side_effect is not None:
        mock.call_endpoint.side_effect = side_effect
    else:
        mock.call_endpoint.return_value = return_value
    return mock


def _task_response():
    alerts = {
        "send_alerts": True, "alert_on_success": False, "alert_on_warning": True,
        "alert_on_error": True, "alert_on_info": False, "send_to_email": False,
        "send_to_discord": True, "send_to_telegram": False, "show_notifications": True,
    }
    cadence = {
        "schedule_type": "manual", "interval_seconds": None, "cron_expression": None,
        "schedule_time": None, "timezone": "UTC",
    }
    stored = {
        "id": 4, "task_id": "stream_probe", "task_name": "Stream Probe",
        "description": None, "enabled": True, **cadence,
        "config": {"accounts": [2, 18]}, **alerts,
        "created_at": "2026-09-20T03:00:00Z", "updated_at": "2026-09-20T03:00:01Z",
        "last_run_at": None, "next_run_at": None,
    }
    schedule = {
        "id": 9, "task_id": "stream_probe", "name": "Same name", "enabled": False,
        "schedule_type": "daily", "interval_seconds": None, "schedule_time": "03:00",
        "timezone": "America/Chicago", "days_of_week": [], "day_of_month": None,
        "week_parity": None, "parameters": {"accounts": [18, 2]},
        "next_run_at": None, "last_run_at": None, "created_at": "2026-09-20T03:00:00Z",
        "updated_at": "2026-09-20T03:00:01Z", "description": "Daily at 03:00 America/Chicago",
    }
    return {
        "task_id": "stream_probe", "task_name": "Stream Probe", "task_description": "Probe streams",
        "status": "running", "enabled": True, "effective_enabled": True,
        "config": {"accounts": [18, 2]}, "schedule": cadence,
        "progress": {
            "total": 2, "current": 1, "percentage": 50.0, "status": "Probing",
            "current_item": "Selected stream", "success_count": 1, "failed_count": 0,
            "skipped_count": 0, "started_at": "2026-09-20T03:00:00Z",
        },
        "last_run": "2026-09-20T03:00:00Z", "next_run": None, **alerts, "stored": stored,
        "schedules": [schedule, {**schedule, "id": 7, "enabled": True}, {**schedule, "id": 3, "name": None}],
        "future_field": {"ordered": [3, 1, 2]},
    }


def _engine_status():
    return {
        "running": True, "check_interval": 5, "max_concurrent": 4,
        "active_tasks": ["stream_probe", "epg_refresh"], "active_task_count": 2,
        "registered_task_count": 7, "future_field": {"ordered": [9, 4]},
    }


class TestTaskDetails:
    @pytest.mark.asyncio
    async def test_complete_selected_task_retains_stored_runtime_and_ordered_values(self):
        mcp = FastMCP("task-details")
        register(mcp)
        task = _task_response()
        engine = _engine_status()
        client = _make_client(side_effect=[task, engine])

        with patch("tools.tasks.get_ecm_client", return_value=client):
            result = await mcp.call_tool("list_tasks", {"task_id": "stream_probe", "details": True})

        shown = json.loads(result[0][0].text)
        assert set(shown) == {"task", "engine"}
        assert shown["task"] == {**task, "schedules": sorted(task["schedules"], key=lambda row: row["id"])}
        assert shown["engine"] == {**engine, "active_tasks": ["epg_refresh", "stream_probe"]}
        assert shown["task"]["stored"]["config"] == {"accounts": [2, 18]}
        assert shown["task"]["config"] == {"accounts": [18, 2]}
        assert [row["id"] for row in shown["task"]["schedules"]] == [3, 7, 9]
        assert shown["task"]["schedules"][0]["name"] is None
        calls = client.call_endpoint.call_args_list
        assert [call.args[0].name for call in calls] == ["tasks_get", "tasks_engine_status"]
        assert calls[0].kwargs == {"path_args": {"task_id": "stream_probe"}, "query": {"details": True}}
        assert calls[1].kwargs == {}

    @pytest.mark.asyncio
    async def test_default_and_selected_summaries_keep_the_same_presentation(self):
        mcp = FastMCP("task-summaries")
        register(mcp)
        task = {"task_id": "stream_probe", "task_name": "Stream Probe", "enabled": True, "status": "idle"}
        client = _make_client(side_effect=[{"tasks": [task]}, task])
        with patch("tools.tasks.get_ecm_client", return_value=client):
            default = await mcp.call_tool("list_tasks", {})
            selected = await mcp.call_tool("list_tasks", {"task_id": "stream_probe"})
        expected = "Found 1 tasks:\n  Stream Probe (id=stream_probe) — enabled, status: idle, last run: never"
        assert default[0][0].text == selected[0][0].text == expected
        assert [call.args[0].name for call in client.call_endpoint.call_args_list] == ["tasks_list", "tasks_get"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("task_id", [None, "", "   "])
    async def test_details_requires_one_nonempty_task_before_http(self, task_id):
        mcp = FastMCP("task-selection")
        register(mcp)
        client = _make_client()
        with patch("tools.tasks.get_ecm_client", return_value=client):
            result = await mcp.call_tool("list_tasks", {"task_id": task_id, "details": True})
        assert result[0][0].text.startswith("Cannot return complete task details:")
        client.call_endpoint.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("arguments", [{"task_id": 7, "details": True}, {"task_id": "stream_probe", "details": "true"}])
    async def test_details_rejects_coerced_input_types_before_http(self, arguments):
        mcp = FastMCP("task-types")
        register(mcp)
        client = _make_client()
        with patch("tools.tasks.get_ecm_client", return_value=client), pytest.raises(ToolError):
            await mcp.call_tool("list_tasks", arguments)
        client.call_endpoint.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("scope,key", [
        ("stored", key) for key in _task_response()["stored"]
    ] + [
        ("schedule", key) for key in _task_response()["schedules"][0]
    ] + [("task", key) for key in _task_response() if key != "future_field"] + [
        ("engine", key) for key in _engine_status() if key != "future_field"
    ])
    async def test_missing_required_fields_refuse_the_entire_object(self, scope, key):
        task = _task_response()
        engine = _engine_status()
        target = {"stored": task["stored"], "schedule": task["schedules"][0], "task": task, "engine": engine}[scope]
        del target[key]
        mcp = FastMCP("task-missing")
        register(mcp)
        client = _make_client(side_effect=[task, engine])
        with patch("tools.tasks.get_ecm_client", return_value=client):
            result = await mcp.call_tool("list_tasks", {"task_id": "stream_probe", "details": True})
        assert result[0][0].text.startswith("Cannot return complete task details:")
        assert not result[0][0].text.startswith("{")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("scope,key,value", [
        ("schedule", "id", True), ("schedule", "id", 7), ("schedule", "task_id", "another_task"),
        ("schedule", "enabled", 1), ("schedule", "created_at", None), ("schedule", "updated_at", "2026-09-20T03:00:00"),
        ("schedule", "parameters", []), ("schedule", "days_of_week", [7]), ("schedule", "name", False),
        ("schedule", "schedule_type", "unknown"), ("schedule", "schedule_time", "25:00"),
        ("schedule", "timezone", "Invalid/Zone"), ("schedule", "week_parity", 2),
        ("stored", "config", []), ("stored", "created_at", None), ("stored", "enabled", False),
        ("task", "effective_enabled", False), ("task", "enabled", 1), ("task", "config", None),
        ("engine", "active_tasks", ["stream_probe", "stream_probe"]), ("engine", "active_task_count", 0),
        ("engine", "check_interval", 0), ("engine", "max_concurrent", True),
        ("engine", "registered_task_count", -1),
    ])
    async def test_invalid_fields_refuse_complete_details(self, scope, key, value):
        task = copy.deepcopy(_task_response())
        engine = _engine_status()
        target = {"stored": task["stored"], "schedule": task["schedules"][0], "task": task, "engine": engine}[scope]
        target[key] = value
        mcp = FastMCP("task-invalid")
        register(mcp)
        client = _make_client(side_effect=[task, engine])
        with patch("tools.tasks.get_ecm_client", return_value=client):
            result = await mcp.call_tool("list_tasks", {"task_id": "stream_probe", "details": True})
        assert result[0][0].text.startswith("Cannot return complete task details:")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [
        {"apiKey": "<synthetic-value>"}, {"API_KEY": None}, {"request-Headers": {}}, {"cookie": "<synthetic-value>"},
        {"safe": "https://example.test/path"}, {"safe": "Bearer " + "x" * 24},
        {"safe": "[REDACTED]"}, {"safe": "<redacted>"}, {"safe": "***"},
        {"safe": "<redacted sha256:123456789012>"}, {"safe": "//example.test/private"},
        {"safe": "ghp_" + "x" * 32}, {"safe": "sk-" + "x" * 32},
        {"safe": float("nan")}, {"safe": "x" * 65537}, {"safe": list(range(2001))},
    ])
    async def test_unsafe_nested_values_refuse_without_disclosure(self, value, caplog):
        task = _task_response()
        task["future_field"] = value
        mcp = FastMCP("task-safe")
        register(mcp)
        client = _make_client(side_effect=[task, _engine_status()])
        with patch("tools.tasks.get_ecm_client", return_value=client):
            result = await mcp.call_tool("list_tasks", {"task_id": "stream_probe", "details": True})
        text = result[0][0].text
        assert text.startswith("Cannot return complete task details:")
        assert "<synthetic-value>" not in text + caplog.text
        assert "example.test" not in text + caplog.text
        assert "x" * 24 not in text + caplog.text

    def test_strict_safety_bounds_depth_and_preserves_default_rule_details(self):
        from tools.channel_pipeline import _rule_details

        nested = {}
        for _ in range(13):
            nested = {"child": nested}
        assert _rule_details(nested, fields=frozenset(nested), strict=True).startswith("Cannot return complete task details:")
        rule = {"description": "https://example.test/help", "event_sync_config": {"ordered": [2, 18]}}
        assert json.loads(_rule_details(rule)) == rule


class TestCreateTaskScheduleTimezone:
    """bd-1wq7z.19 — timezone must be passed through to the backend body."""

    @pytest.mark.asyncio
    async def test_timezone_in_body_when_provided(self):
        """When caller passes timezone it must appear in the POST body."""
        from tools.tasks import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        mock_client = _make_client(
            return_value={"id": 7, "description": "daily 08:00 America/Chicago"}
        )

        with patch("tools.tasks.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool(
                "create_task_schedule",
                {
                    "task_id": "m3u_refresh",
                    "schedule_type": "daily",
                    "schedule_time": "08:00",
                    "timezone": "America/Chicago",
                },
            )

        # Verify call_endpoint was called with a body that contains timezone
        call_kwargs = mock_client.call_endpoint.call_args
        body = call_kwargs.kwargs.get("body") or call_kwargs[1].get("body")
        assert body is not None, "call_endpoint must be called with a body kwarg"
        assert "timezone" in body, "body must include 'timezone'"
        assert body["timezone"] == "America/Chicago"

        text = result[0][0].text
        assert "Schedule created" in text

    @pytest.mark.asyncio
    async def test_default_timezone_utc(self):
        """When timezone is not provided the default 'UTC' must be sent in body."""
        from tools.tasks import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        mock_client = _make_client(
            return_value={"id": 8, "description": "interval 3600s"}
        )

        with patch("tools.tasks.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool(
                "create_task_schedule",
                {
                    "task_id": "stream_probe",
                    "schedule_type": "interval",
                    "interval_seconds": 3600,
                },
            )

        call_kwargs = mock_client.call_endpoint.call_args
        body = call_kwargs.kwargs.get("body") or call_kwargs[1].get("body")
        assert body is not None
        # Default timezone should be "UTC"
        assert "timezone" in body, "body must include 'timezone' even when not provided by caller"
        assert body["timezone"] == "UTC"

    @pytest.mark.asyncio
    async def test_timezone_not_in_body_when_explicitly_none(self):
        """Passing timezone=None explicitly should still send the default 'UTC'."""
        from tools.tasks import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        mock_client = _make_client(return_value={"id": 9, "description": "monthly"})

        with patch("tools.tasks.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool(
                "create_task_schedule",
                {
                    "task_id": "epg_refresh",
                    "schedule_type": "monthly",
                    "schedule_time": "03:00",
                    "day_of_month": 1,
                    "timezone": None,
                },
            )

        call_kwargs = mock_client.call_endpoint.call_args
        body = call_kwargs.kwargs.get("body") or call_kwargs[1].get("body")
        assert body is not None
        # When None is passed, default "UTC" should still be sent
        assert "timezone" in body
        assert body["timezone"] == "UTC"

    @pytest.mark.asyncio
    async def test_america_new_york_timezone(self):
        """A non-UTC timezone passes through correctly."""
        from tools.tasks import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        mock_client = _make_client(
            return_value={"id": 10, "description": "weekly Monday 09:00 America/New_York"}
        )

        with patch("tools.tasks.get_ecm_client", return_value=mock_client):
            await mcp.call_tool(
                "create_task_schedule",
                {
                    "task_id": "m3u_refresh",
                    "schedule_type": "weekly",
                    "schedule_time": "09:00",
                    "days_of_week": [1],
                    "timezone": "America/New_York",
                },
            )

        call_kwargs = mock_client.call_endpoint.call_args
        body = call_kwargs.kwargs.get("body") or call_kwargs[1].get("body")
        assert body["timezone"] == "America/New_York"

    @pytest.mark.asyncio
    async def test_error_returns_message(self):
        """API errors are caught and reported gracefully."""
        from tools.tasks import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)

        mock_client = _make_client(side_effect=Exception("backend unavailable"))

        with patch("tools.tasks.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool(
                "create_task_schedule",
                {"task_id": "m3u_refresh", "schedule_type": "daily"},
            )

        text = result[0][0].text
        assert "Error" in text
        assert "backend unavailable" in text
