"""Task execution acceptance and resumable wait contracts."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable
from typing import Any
from unittest.mock import patch

import httpx
import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from tools.tasks import register


STARTED_AT = "2026-09-20T03:00:00Z"
TASK_ID = "m3u_refresh"
EXECUTION_ID = 41


def _registry() -> FastMCP:
    mcp = FastMCP("task-execution-test")
    register(mcp)
    return mcp


def _text(result: Any) -> str:
    return result[0][0].text


def _accepted(**changes: Any) -> dict[str, Any]:
    response = {
        "status": "accepted",
        "task_id": TASK_ID,
        "execution_id": EXECUTION_ID,
        "started_at": STARTED_AT,
    }
    response.update(changes)
    return response


def _execution(status: str = "running", **changes: Any) -> dict[str, Any]:
    response = {
        "id": EXECUTION_ID,
        "task_id": TASK_ID,
        "started_at": STARTED_AT,
        "completed_at": None,
        "duration_seconds": None,
        "status": status,
        "success": None,
        "message": None,
        "error": None,
        "total_items": 0,
        "success_count": 0,
        "failed_count": 0,
        "skipped_count": 0,
        "details": None,
        "triggered_by": "manual",
    }
    response.update(changes)
    return response


class ScriptedClient:
    """Return scripted HTTP outcomes while rejecting overlapping requests."""

    def __init__(self, outcomes: list[Any]):
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.active = 0
        self.max_active = 0

    async def call_endpoint(self, endpoint: Any, **kwargs: Any) -> Any:
        assert self.active == 0, "task polling must not overlap requests"
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.calls.append((endpoint.name, kwargs))
        try:
            assert self.outcomes, f"unexpected call to {endpoint.name}"
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            if callable(outcome):
                outcome = outcome()
            if isinstance(outcome, Awaitable):
                outcome = await outcome
            return outcome
        finally:
            self.active -= 1


def _http_error(status_code: int) -> RuntimeError:
    request = httpx.Request("GET", "http://backend/api/tasks/x/executions/1")
    response = httpx.Response(status_code, request=request)
    cause = httpx.HTTPStatusError(
        f"HTTP {status_code}",
        request=request,
        response=response,
    )
    error = RuntimeError(f"backend returned {status_code}")
    error.__cause__ = cause
    return error


@pytest.mark.asyncio
async def test_run_task_defaults_to_waiting_for_the_exact_terminal_execution():
    client = ScriptedClient([
        _accepted(),
        _execution(),
        _execution(
            "completed",
            completed_at="2026-09-20T03:00:08Z",
            duration_seconds=8.0,
            success=True,
            message="Refreshed 12 sources",
            total_items=12,
            success_count=12,
        ),
    ])
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    with (
        patch("tools.tasks.get_ecm_client", return_value=client),
        patch("tools.tasks.asyncio.sleep", side_effect=sleep),
    ):
        result = await _registry().call_tool("run_task", {"task_id": TASK_ID})

    body = json.loads(_text(result))
    assert body["status"] == "completed"
    assert body["task_id"] == TASK_ID
    assert body["id"] == EXECUTION_ID
    assert body["message"] == "Refreshed 12 sources"
    assert [name for name, _ in client.calls] == [
        "tasks_start",
        "tasks_execution",
        "tasks_execution",
    ]
    assert all(call[1]["timeout"] == 30.0 for call in client.calls)
    assert client.calls[0][1] == {
        "path_args": {"task_id": TASK_ID},
        "timeout": 30.0,
    }
    assert client.calls[1][1]["path_args"] == {
        "task_id": TASK_ID,
        "execution_id": EXECUTION_ID,
    }
    assert client.calls[1][1]["query"] == {"started_at": STARTED_AT}
    assert delays == [5.0]
    assert client.max_active == 1


@pytest.mark.asyncio
async def test_run_task_explicit_false_returns_acceptance_without_a_read():
    client = ScriptedClient([_accepted()])

    with patch("tools.tasks.get_ecm_client", return_value=client):
        result = await _registry().call_tool(
            "run_task",
            {"task_id": TASK_ID, "wait_for_completion": False},
        )

    assert json.loads(_text(result)) == _accepted()
    assert [name for name, _ in client.calls] == ["tasks_start"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        _accepted(status="running"),
        _accepted(task_id="another_task"),
        _accepted(execution_id=0),
        _accepted(execution_id=True),
        _accepted(started_at="2026-09-20T03:00:00"),
        ["not", "an", "object"],
    ],
)
async def test_run_task_rejects_malformed_acceptance_without_a_read(response: Any):
    client = ScriptedClient([response])

    with patch("tools.tasks.get_ecm_client", return_value=client):
        result = await _registry().call_tool("run_task", {"task_id": TASK_ID})

    assert "Error running task" in _text(result)
    assert [name for name, _ in client.calls] == ["tasks_start"]


@pytest.mark.asyncio
async def test_snapshot_reader_makes_one_exact_get():
    client = ScriptedClient([_execution()])

    with patch("tools.tasks.get_ecm_client", return_value=client):
        result = await _registry().call_tool(
            "get_task_execution",
            {
                "task_id": TASK_ID,
                "execution_id": EXECUTION_ID,
                "started_at": "2026-09-19T22:00:00-05:00",
            },
        )

    assert json.loads(_text(result))["status"] == "running"
    assert [name for name, _ in client.calls] == ["tasks_execution"]
    assert client.calls[0][1]["query"] == {
        "started_at": "2026-09-19T22:00:00-05:00",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("elapsed_limit", [300.0, 600.0, 86400.0])
async def test_wait_has_no_total_duration_limit(elapsed_limit: float):
    elapsed = 0.0
    delays: list[float] = []

    def read() -> dict[str, Any]:
        if elapsed <= elapsed_limit:
            return _execution()
        return _execution("completed", success=True)

    client = ScriptedClient([read, read])

    async def sleep(delay: float) -> None:
        nonlocal elapsed
        assert delay > 0
        delays.append(delay)
        elapsed = elapsed_limit + 1.0

    with (
        patch("tools.tasks.get_ecm_client", return_value=client),
        patch("tools.tasks.asyncio.sleep", side_effect=sleep),
    ):
        result = await _registry().call_tool(
            "get_task_execution",
            {
                "task_id": TASK_ID,
                "execution_id": EXECUTION_ID,
                "started_at": STARTED_AT,
                "wait_for_completion": True,
            },
        )

    assert json.loads(_text(result))["status"] == "completed"
    assert elapsed > elapsed_limit
    assert delays == [5.0]
    assert client.max_active == 1


@pytest.mark.asyncio
async def test_wait_retries_transient_transport_and_wrapped_status_failures():
    request = httpx.Request("GET", "http://backend/api/tasks/x/executions/1")
    client = ScriptedClient([
        TimeoutError("attempt timed out"),
        httpx.ConnectError("connection reset", request=request),
        _http_error(503),
        _execution(),
        TimeoutError("retry delay resets after a valid response"),
        _execution("completed_with_warnings", success=True, message="Some items were skipped"),
    ])
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    with (
        patch("tools.tasks.get_ecm_client", return_value=client),
        patch("tools.tasks.asyncio.sleep", side_effect=sleep),
    ):
        result = await _registry().call_tool(
            "get_task_execution",
            {
                "task_id": TASK_ID,
                "execution_id": EXECUTION_ID,
                "started_at": STARTED_AT,
                "wait_for_completion": True,
            },
        )

    body = json.loads(_text(result))
    assert body["status"] == "completed_with_warnings"
    assert body["message"] == "Some items were skipped"
    assert delays == [1.0, 2.0, 4.0, 5.0, 1.0]
    assert client.max_active == 1


@pytest.mark.asyncio
async def test_transient_retry_delay_caps_at_thirty_seconds():
    client = ScriptedClient([
        TimeoutError("one"),
        TimeoutError("two"),
        TimeoutError("three"),
        TimeoutError("four"),
        TimeoutError("five"),
        TimeoutError("six"),
        TimeoutError("seven"),
        TimeoutError("eight"),
        _execution("completed", success=True),
    ])
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    with (
        patch("tools.tasks.get_ecm_client", return_value=client),
        patch("tools.tasks.asyncio.sleep", side_effect=sleep),
    ):
        result = await _registry().call_tool(
            "get_task_execution",
            {
                "task_id": TASK_ID,
                "execution_id": EXECUTION_ID,
                "started_at": STARTED_AT,
                "wait_for_completion": True,
            },
        )

    assert json.loads(_text(result))["status"] == "completed"
    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503, 504])
async def test_wait_retries_each_allowed_http_status(status_code: int):
    client = ScriptedClient([_http_error(status_code), _execution("completed", success=True)])
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    with (
        patch("tools.tasks.get_ecm_client", return_value=client),
        patch("tools.tasks.asyncio.sleep", side_effect=sleep),
    ):
        result = await _registry().call_tool(
            "get_task_execution",
            {
                "task_id": TASK_ID,
                "execution_id": EXECUTION_ID,
                "started_at": STARTED_AT,
                "wait_for_completion": True,
            },
        )

    assert json.loads(_text(result))["status"] == "completed"
    assert delays == [1.0]


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [401, 403, 404, 422])
async def test_wait_does_not_retry_nontransient_http_status(status_code: int):
    client = ScriptedClient([_http_error(status_code)])
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    with (
        patch("tools.tasks.get_ecm_client", return_value=client),
        patch("tools.tasks.asyncio.sleep", side_effect=sleep),
    ):
        result = await _registry().call_tool(
            "get_task_execution",
            {
                "task_id": TASK_ID,
                "execution_id": EXECUTION_ID,
                "started_at": STARTED_AT,
                "wait_for_completion": True,
            },
        )

    text = _text(result)
    assert "Task execution read failed" in text
    assert f'"execution_id": {EXECUTION_ID}' in text
    assert len(client.calls) == 1
    assert delays == []


@pytest.mark.asyncio
async def test_snapshot_does_not_retry_a_transient_failure():
    client = ScriptedClient([TimeoutError("snapshot timed out")])
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    with (
        patch("tools.tasks.get_ecm_client", return_value=client),
        patch("tools.tasks.asyncio.sleep", side_effect=sleep),
    ):
        result = await _registry().call_tool(
            "get_task_execution",
            {
                "task_id": TASK_ID,
                "execution_id": EXECUTION_ID,
                "started_at": STARTED_AT,
            },
        )

    assert "Task execution read failed" in _text(result)
    assert len(client.calls) == 1
    assert delays == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        _execution(task_id="another_task"),
        _execution(id=0),
        _execution(id=EXECUTION_ID + 1),
        _execution(started_at="2026-09-20T03:00:00"),
        _execution(started_at="2026-09-20T03:01:00Z"),
        _execution(status="mystery"),
        ["not", "an", "object"],
    ],
)
async def test_reader_rejects_malformed_or_mismatched_execution(response: Any):
    client = ScriptedClient([response])

    with patch("tools.tasks.get_ecm_client", return_value=client):
        result = await _registry().call_tool(
            "get_task_execution",
            {
                "task_id": TASK_ID,
                "execution_id": EXECUTION_ID,
                "started_at": STARTED_AT,
                "wait_for_completion": True,
            },
        )

    assert "Task execution read failed" in _text(result)
    assert len(client.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("execution_id", [0, -1, True])
async def test_reader_rejects_invalid_execution_id_before_http(execution_id: Any):
    client = ScriptedClient([])

    with patch("tools.tasks.get_ecm_client", return_value=client):
        with pytest.raises(ToolError):
            await _registry().call_tool(
                "get_task_execution",
                {
                    "task_id": TASK_ID,
                    "execution_id": execution_id,
                    "started_at": STARTED_AT,
                },
            )

    assert client.calls == []


@pytest.mark.asyncio
async def test_reader_rejects_naive_started_at_before_http():
    client = ScriptedClient([])

    with patch("tools.tasks.get_ecm_client", return_value=client):
        result = await _registry().call_tool(
            "get_task_execution",
            {
                "task_id": TASK_ID,
                "execution_id": EXECUTION_ID,
                "started_at": "2026-09-20T03:00:00",
            },
        )

    assert "Task execution read failed" in _text(result)
    assert client.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected_status"),
    [
        (_execution("completed", success=True), "completed"),
        (_execution("completed_with_warnings", success=True, message="Skipped one"), "completed_with_warnings"),
        (_execution("failed", success=False, error="Source refused the request"), "failed"),
        (_execution("cancelled", success=False, message="Cancelled by operator"), "cancelled"),
        (_execution("terminated", success=False, error="Service restarted"), "terminated"),
    ],
)
async def test_terminal_outcomes_remain_distinct_json(response: dict[str, Any], expected_status: str):
    client = ScriptedClient([response])

    with patch("tools.tasks.get_ecm_client", return_value=client):
        result = await _registry().call_tool(
            "get_task_execution",
            {
                "task_id": TASK_ID,
                "execution_id": EXECUTION_ID,
                "started_at": STARTED_AT,
                "wait_for_completion": True,
            },
        )

    body = json.loads(_text(result))
    assert body["status"] == expected_status
    assert body.get("message") == response.get("message")
    assert body.get("error") == response.get("error")


@pytest.mark.asyncio
async def test_cancelled_wait_can_resume_without_repeating_start_or_cancelling_backend():
    read_started = asyncio.Event()
    release_read = asyncio.Event()

    async def blocked_read() -> dict[str, Any]:
        read_started.set()
        await release_read.wait()
        return _execution()

    client = ScriptedClient([
        _accepted(),
        blocked_read,
        _execution("completed", success=True),
    ])
    mcp = _registry()

    with patch("tools.tasks.get_ecm_client", return_value=client):
        accepted = await mcp.call_tool(
            "run_task",
            {"task_id": TASK_ID, "wait_for_completion": False},
        )
        identity = json.loads(_text(accepted))
        arguments = {
            "task_id": identity["task_id"],
            "execution_id": identity["execution_id"],
            "started_at": identity["started_at"],
            "wait_for_completion": True,
        }
        waiting = asyncio.create_task(mcp.call_tool("get_task_execution", arguments))
        await read_started.wait()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        resumed = await mcp.call_tool("get_task_execution", arguments)

    assert json.loads(_text(resumed))["status"] == "completed"
    names = [name for name, _ in client.calls]
    assert names.count("tasks_start") == 1
    assert names.count("tasks_execution") == 2
    assert "tasks_run" not in names
    assert "tasks_cancel" not in names


@pytest.mark.asyncio
async def test_acceptance_timeout_is_uncertain_and_never_repeats_the_post():
    client = ScriptedClient([TimeoutError("acceptance timed out")])

    with patch("tools.tasks.get_ecm_client", return_value=client):
        result = await _registry().call_tool("run_task", {"task_id": TASK_ID})

    text = _text(result)
    assert "may have started" in text
    assert "Do not call run_task again" in text
    assert [name for name, _ in client.calls] == ["tasks_start"]


@pytest.mark.asyncio
async def test_missing_additive_start_route_does_not_fall_back_to_legacy_run():
    client = ScriptedClient([_http_error(404)])

    with patch("tools.tasks.get_ecm_client", return_value=client):
        result = await _registry().call_tool("run_task", {"task_id": TASK_ID})

    assert "Error running task" in _text(result)
    assert [name for name, _ in client.calls] == ["tasks_start"]


def test_task_execution_reader_is_read_only_and_run_remains_mutating():
    from tools import register_all_tools
    from tools._safety_policy import SAFETY_INVENTORY, ToolSafety

    mcp = FastMCP("task-safety-test")
    register_all_tools(mcp)

    assert SAFETY_INVENTORY["get_task_execution"] is ToolSafety.READ_ONLY
    assert SAFETY_INVENTORY["run_task"] is ToolSafety.MUTATING
    reader = mcp._tool_manager._tools["get_task_execution"]
    runner = mcp._tool_manager._tools["run_task"]
    assert reader.annotations is not None
    assert reader.annotations.readOnlyHint is True
    assert reader.annotations.destructiveHint is False
    assert runner.annotations is not None
    assert runner.annotations.readOnlyHint is False
    assert runner.annotations.destructiveHint is False
