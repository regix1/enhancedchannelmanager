"""Task management tools."""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Annotated

import httpx

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from _endpoint_contracts import ENDPOINTS
from ecm_client import get_ecm_client

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_SECONDS = 30.0
_POLL_DELAY_SECONDS = 5.0
_FIRST_RETRY_DELAY_SECONDS = 1.0
_MAX_RETRY_DELAY_SECONDS = 30.0
_TERMINAL_EXECUTION_STATUSES = frozenset(
    {"completed", "completed_with_warnings", "failed", "cancelled", "terminated"}
)
_EXECUTION_STATUSES = _TERMINAL_EXECUTION_STATUSES | {"running"}
_RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


def _parse_started_at(value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("started_at must be a non-empty timestamp string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("started_at must be an ISO 8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("started_at must include a timezone offset")
    return parsed.astimezone(timezone.utc)


def _validate_acceptance(task_id: str, response: object) -> dict:
    if not isinstance(response, dict):
        raise ValueError("task acceptance must be a JSON object")
    if response.get("status") != "accepted":
        raise ValueError("task acceptance status must be accepted")
    if response.get("task_id") != task_id:
        raise ValueError("task acceptance task_id does not match the requested task")
    execution_id = response.get("execution_id")
    if isinstance(execution_id, bool) or not isinstance(execution_id, int) or execution_id <= 0:
        raise ValueError("task acceptance execution_id must be a positive integer")
    started_at = response.get("started_at")
    _parse_started_at(started_at)
    return {
        "status": response["status"],
        "task_id": response["task_id"],
        "execution_id": execution_id,
        "started_at": started_at,
    }


def _validate_execution(
    task_id: str,
    execution_id: int,
    started_at: datetime,
    response: object,
) -> dict:
    if not isinstance(response, dict):
        raise ValueError("task execution must be a JSON object")
    response_id = response.get("id")
    if isinstance(response_id, bool) or not isinstance(response_id, int) or response_id <= 0:
        raise ValueError("task execution id must be a positive integer")
    if response_id != execution_id:
        raise ValueError("task execution id does not match the requested execution")
    if response.get("task_id") != task_id:
        raise ValueError("task execution task_id does not match the requested task")
    response_started_at = _parse_started_at(response.get("started_at"))
    if response_started_at != started_at:
        raise ValueError("task execution started_at does not match the requested execution")
    status = response.get("status")
    if status not in _EXECUTION_STATUSES:
        raise ValueError("task execution status is not recognized")
    return response


def _is_retryable_read_error(error: Exception) -> bool:
    current: BaseException | None = error
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, (TimeoutError, httpx.TransportError)):
            return True
        if isinstance(current, httpx.HTTPStatusError):
            return current.response.status_code in _RETRYABLE_HTTP_STATUSES
        current = current.__cause__
    return False


def register(mcp: FastMCP):
    @mcp.tool()
    async def list_tasks() -> str:
        """List all scheduled tasks and their status."""
        try:
            client = get_ecm_client()
            resp = await client.call_endpoint(ENDPOINTS["tasks_list"])
            # Backend returns {"tasks": [...]}; unwrap defensively
            # (same class as bd-pvw35 / GH #222 — iterating the dict gives string keys).
            tasks = resp.get("tasks", []) if isinstance(resp, dict) else (resp or [])

            if not tasks:
                return "No tasks configured."

            lines = [f"Found {len(tasks)} tasks:"]
            for t in tasks:
                tid = t.get("task_id", t.get("id", "?"))
                raw_name = t.get("task_name") or t.get("name")
                name = raw_name if raw_name else tid.replace("_", " ").title() if tid != "?" else "Unknown"
                # vkktd.5: ``enabled`` is only the PARENT scheduled_tasks gate.
                # Firing ALSO requires >=1 enabled child schedule; the backend
                # exposes the combined firing state as ``effective_enabled``
                # (vkktd.3). Surface the TRUE firing state so an AI agent never
                # treats a gated-off task (enabled parent, no active schedule)
                # as live — the exact "reads Enabled but won't run" trap this
                # epic exists to eliminate. When the field is absent (older
                # backend), fall back to the parent gate.
                enabled_gate = bool(t.get("enabled"))
                effective = t.get("effective_enabled")
                if effective is None:
                    effective = enabled_gate
                if effective:
                    enabled = "enabled"
                elif enabled_gate:
                    enabled = "enabled but WON'T RUN (no active schedule)"
                else:
                    enabled = "disabled"
                last_run = t.get("last_run", "never")
                status = t.get("status", "idle")
                lines.append(f"  {name} (id={tid}) — {enabled}, status: {status}, last run: {last_run}")

            return "\n".join(lines)
        except Exception as e:
            logger.error("[MCP] list_tasks failed: %s", e)
            return f"Error listing tasks: {e}"

    @mcp.tool()
    async def get_task_execution(
        task_id: str,
        execution_id: Annotated[int, Field(strict=True, gt=0)],
        started_at: str,
        wait_for_completion: bool = False,
    ) -> str:
        """Read one exact task execution, optionally waiting until it is terminal.

        Args:
            task_id: The task ID returned by run_task.
            execution_id: The positive execution ID returned by run_task.
            started_at: The timezone-aware start timestamp returned by run_task.
            wait_for_completion: Wait without an overall deadline when true.
        """
        identity = {
            "task_id": task_id,
            "execution_id": execution_id,
            "started_at": started_at,
        }
        try:
            if not task_id:
                raise ValueError("task_id must be non-empty")
            if isinstance(execution_id, bool) or not isinstance(execution_id, int) or execution_id <= 0:
                raise ValueError("execution_id must be a positive integer")
            expected_started_at = _parse_started_at(started_at)
            client = get_ecm_client()
            retry_delay = _FIRST_RETRY_DELAY_SECONDS
            while True:
                try:
                    response = await client.call_endpoint(
                        ENDPOINTS["tasks_execution"],
                        path_args={"task_id": task_id, "execution_id": execution_id},
                        query={"started_at": started_at},
                        timeout=_REQUEST_TIMEOUT_SECONDS,
                    )
                except Exception as error:
                    if not wait_for_completion or not _is_retryable_read_error(error):
                        raise
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, _MAX_RETRY_DELAY_SECONDS)
                    continue

                execution = _validate_execution(
                    task_id,
                    execution_id,
                    expected_started_at,
                    response,
                )
                if not wait_for_completion or execution["status"] in _TERMINAL_EXECUTION_STATUSES:
                    return json.dumps(execution, sort_keys=True)
                retry_delay = _FIRST_RETRY_DELAY_SECONDS
                await asyncio.sleep(_POLL_DELAY_SECONDS)
        except Exception as error:
            logger.error("[MCP] get_task_execution failed: %s", error)
            return f"Task execution read failed for {json.dumps(identity, sort_keys=True)}: {error}"

    @mcp.tool()
    async def run_task(task_id: str, wait_for_completion: bool = True) -> str:
        """Run a scheduled task immediately.

        Args:
            task_id: The task ID to run (e.g., 'm3u_refresh', 'stream_probe').
            wait_for_completion: Wait for the terminal execution by default. Set
                false to return the accepted execution identity immediately.
        """
        try:
            if not task_id:
                raise ValueError("task_id must be non-empty")
            client = get_ecm_client()
            response = await client.call_endpoint(
                ENDPOINTS["tasks_start"],
                path_args={"task_id": task_id},
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            acceptance = _validate_acceptance(task_id, response)
            if not wait_for_completion:
                return json.dumps(acceptance, sort_keys=True)
            return await get_task_execution(
                acceptance["task_id"],
                acceptance["execution_id"],
                acceptance["started_at"],
                wait_for_completion=True,
            )
        except (TimeoutError, httpx.TransportError) as error:
            logger.error("[MCP] run_task acceptance failed: %s", error)
            return (
                f"Task '{task_id}' may have started, but its acceptance response was not received. "
                f"Do not call run_task again: {error}"
            )
        except Exception as error:
            logger.error("[MCP] run_task failed: %s", error)
            return f"Error running task '{task_id}': {error}"

    @mcp.tool()
    async def cancel_task(task_id: str) -> str:
        """Cancel a currently running task.

        Args:
            task_id: The task ID to cancel
        """
        try:
            client = get_ecm_client()
            result = await client.call_endpoint(ENDPOINTS["tasks_cancel"], path_args={"task_id": task_id})
            if isinstance(result, dict):
                status = result.get("status", "")
                msg = result.get("message", "")
                # bd-1wq7z.12: backend returns HTTP 200 {"status": "not_running", ...}
                # when the task isn't running — don't hardcode "cancelled".
                if status == "not_running":
                    return f"Task '{task_id}' was not running. {msg}".rstrip()
                return f"Task '{task_id}' cancelled. {msg}".rstrip()
            return f"Task '{task_id}' cancelled."
        except Exception as e:
            logger.error("[MCP] cancel_task failed: %s", e)
            return f"Error cancelling task '{task_id}': {e}"

    @mcp.tool()
    async def get_task_history(task_id: str | None = None, limit: int = 10) -> str:
        """View task execution history.

        Args:
            task_id: Optional specific task ID to get history for. If omitted, returns all task history.
            limit: Number of history entries to return (default 10)
        """
        try:
            client = get_ecm_client()
            if task_id:
                result = await client.call_endpoint(
                    ENDPOINTS["tasks_history"], path_args={"task_id": task_id}, query={"limit": limit},
                )
            else:
                result = await client.call_endpoint(ENDPOINTS["tasks_history_all"], query={"limit": limit})

            history = result.get("history", []) if isinstance(result, dict) else result

            if not history:
                scope = f"for task '{task_id}'" if task_id else ""
                return f"No task history {scope}."

            lines = [f"Task history ({len(history)} entries):"]
            for h in history[:limit]:
                name = h.get("task_name", h.get("task_id", "?"))
                status = h.get("status", "?")
                started = h.get("started_at", h.get("timestamp", "?"))
                duration = h.get("duration_seconds", h.get("duration", 0))
                dur_str = f"{duration:.1f}s" if duration else "?"
                lines.append(f"  {name}: {status} ({dur_str}) — {started}")

            return "\n".join(lines)
        except Exception as e:
            logger.error("[MCP] get_task_history failed: %s", e)
            return f"Error getting task history: {e}"

    @mcp.tool()
    async def list_task_schedules(task_id: str) -> str:
        """List schedules for a specific task.

        Args:
            task_id: The task ID to list schedules for
        """
        try:
            client = get_ecm_client()
            schedules = await client.call_endpoint(ENDPOINTS["tasks_list_schedules"], path_args={"task_id": task_id})

            items = schedules if isinstance(schedules, list) else schedules.get("schedules", [])

            if not items:
                return f"No schedules configured for task '{task_id}'."

            lines = [f"Schedules for '{task_id}' ({len(items)}):"]
            for s in items:
                sid = s.get("id", "?")
                stype = s.get("schedule_type", "?")
                desc = s.get("description", "")
                enabled = "enabled" if s.get("enabled") else "disabled"
                next_run = s.get("next_run_at", s.get("next_run", "?"))
                desc_info = f" — {desc}" if desc else ""
                lines.append(f"  #{sid}: {stype}{desc_info} ({enabled}), next: {next_run}")

            return "\n".join(lines)
        except Exception as e:
            logger.error("[MCP] list_task_schedules failed: %s", e)
            return f"Error listing schedules for '{task_id}': {e}"

    @mcp.tool()
    async def create_task_schedule(
        task_id: str,
        schedule_type: str,
        schedule_time: str | None = None,
        interval_seconds: int | None = None,
        days_of_week: list[int] | None = None,
        day_of_month: int | None = None,
        enabled: bool = True,
        name: str | None = None,
        timezone: str | None = None,
        parameters: dict | None = None,
    ) -> str:
        """Create a new schedule for a task.

        The backend supports these schedule types (a cron-expression form does
        NOT exist — passing one was silently rejected: drift fixed in bd-vtghg
        Phase 2, hence this signature change from ``cron_expression``):

        Args:
            task_id: The task ID to schedule
            schedule_type: One of 'interval', 'daily', 'weekly', 'biweekly', 'monthly'
            schedule_time: HH:MM time-of-day (for daily/weekly/biweekly/monthly)
            interval_seconds: Interval in seconds (for schedule_type='interval')
            days_of_week: List of day numbers 0=Sunday..6=Saturday (for weekly/biweekly)
            day_of_month: Day of month 1-31, or -1 for last day (for monthly)
            enabled: Whether the schedule is active (default True)
            name: Optional display name for the schedule
            timezone: IANA timezone name for the schedule (e.g. 'America/Chicago',
                'Europe/London'). Defaults to 'UTC'. Schedules stored as UTC will
                fire at the wrong local time if the operator is in a different zone.
            parameters: Task-specific parameters passed to the task on each
                scheduled run (e.g. channel_groups, batch_size — shape depends
                on task_id; see GET /api/tasks/{task_id}/parameter-schema via
                the ECM UI for the accepted keys of a given task).
        """
        try:
            client = get_ecm_client()
            payload: dict = {
                "schedule_type": schedule_type,
                "enabled": enabled,
                "timezone": timezone if timezone is not None else "UTC",
            }
            if schedule_time is not None:
                payload["schedule_time"] = schedule_time
            if interval_seconds is not None:
                payload["interval_seconds"] = interval_seconds
            if days_of_week is not None:
                payload["days_of_week"] = days_of_week
            if day_of_month is not None:
                payload["day_of_month"] = day_of_month
            if name is not None:
                payload["name"] = name
            if parameters is not None:
                payload["parameters"] = parameters

            result = await client.call_endpoint(
                ENDPOINTS["tasks_create_schedule"], path_args={"task_id": task_id}, body=payload,
            )
            if isinstance(result, dict):
                sid = result.get("id", "?")
                desc = result.get("description", schedule_type)
                return f"Schedule created for '{task_id}': {desc} (id={sid})"
            return f"Schedule created for '{task_id}' ({schedule_type})."
        except Exception as e:
            logger.error("[MCP] create_task_schedule failed: %s", e)
            return f"Error creating schedule: {e}"

    @mcp.tool()
    async def delete_task_schedule(task_id: str, schedule_id: int) -> str:
        """Delete a task schedule.

        Args:
            task_id: The task ID the schedule belongs to
            schedule_id: The schedule ID to delete
        """
        try:
            client = get_ecm_client()
            await client.call_endpoint(
                ENDPOINTS["tasks_delete_schedule"],
                path_args={"task_id": task_id, "schedule_id": schedule_id},
            )
            # Read-back: confirm the schedule is gone from the task's list.
            try:
                schedules = await client.call_endpoint(
                    ENDPOINTS["tasks_list_schedules"], path_args={"task_id": task_id},
                )
                items = schedules if isinstance(schedules, list) else (schedules or {}).get("schedules", [])
                still_present = any(isinstance(s, dict) and s.get("id") == schedule_id for s in items)
            except Exception:
                still_present = None
            if still_present is True:
                return f"WARNING: requested deletion of schedule {schedule_id} but it still appears on task '{task_id}'."
            return f"Schedule {schedule_id} deleted from task '{task_id}'."
        except Exception as e:
            logger.error("[MCP] delete_task_schedule failed: %s", e)
            return f"Error deleting schedule: {e}"
