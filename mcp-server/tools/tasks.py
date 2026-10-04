"""Task management tools."""
import asyncio
import json
import logging
import math
from datetime import datetime, timezone
from typing import Annotated
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from _endpoint_contracts import ENDPOINTS
from ecm_client import get_ecm_client
from tools.channel_pipeline import _rule_details

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
    *,
    complete: bool = False,
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
    if complete:
        required = ENDPOINTS["tasks_execution"].response_fields
        if not required <= response.keys():
            raise ValueError("task execution is incomplete")
        schedule_id = response["schedule_id"]
        if schedule_id is not None and (type(schedule_id) is not int or schedule_id <= 0):
            raise ValueError("task execution schedule_id must be null or a positive integer")
        completed_at = response["completed_at"]
        if completed_at is not None:
            if _parse_started_at(completed_at) < response_started_at:
                raise ValueError("task execution completion precedes its start")
        elif status in _TERMINAL_EXECUTION_STATUSES:
            raise ValueError("terminal task execution requires completed_at")
        if response["triggered_by"] not in {"scheduled", "manual", "api"}:
            raise ValueError("task execution trigger is not recognized")
        for key in ("total_items", "success_count", "failed_count", "skipped_count"):
            if type(response[key]) is not int or response[key] < 0:
                raise ValueError("task execution counters must be nonnegative integers")
        duration = response["duration_seconds"]
        if duration is not None and (
            type(duration) not in (int, float) or not math.isfinite(duration) or duration < 0
        ):
            raise ValueError("task execution duration is invalid")
        if response["success"] is not None and type(response["success"]) is not bool:
            raise ValueError("task execution success must be null or a boolean")
        for key in ("message", "error"):
            if response[key] is not None and not isinstance(response[key], str):
                raise ValueError("task execution text is invalid")
        if response["details"] is not None and not isinstance(response["details"], dict):
            raise ValueError("task execution details must be null or an object")
    return response


def _validate_task(task_id: str, response: object, engine: object) -> dict:
    if not isinstance(response, dict) or not isinstance(engine, dict):
        raise ValueError("task and engine must be JSON objects")
    alerts = {
        "send_alerts", "alert_on_success", "alert_on_warning", "alert_on_error",
        "alert_on_info", "send_to_email", "send_to_discord", "send_to_telegram",
        "show_notifications",
    }
    required = {
        "task_id", "task_name", "task_description", "status", "enabled", "progress",
        "schedule", "last_run", "next_run", "config", "effective_enabled", "stored", "schedules",
    } | alerts
    if not required <= response.keys() or response["task_id"] != task_id:
        raise ValueError("selected task is incomplete or mismatched")
    if response["status"] not in {"idle", "scheduled", "running", "paused", "cancelled", "completed", "failed"}:
        raise ValueError("task status is not recognized")
    for key in ("task_name", "task_description"):
        if not isinstance(response[key], str):
            raise ValueError("task text is invalid")
    for key in alerts | {"enabled", "effective_enabled"}:
        if type(response[key]) is not bool:
            raise ValueError("task gates must be booleans")
    if not isinstance(response["config"], dict):
        raise ValueError("runtime task config must be an object")
    for key in ("last_run", "next_run"):
        if response[key] is not None:
            _parse_started_at(response[key])
    progress = response["progress"]
    if not isinstance(progress, dict) or not {
        "total", "current", "percentage", "status", "current_item", "success_count",
        "failed_count", "skipped_count", "started_at",
    } <= progress.keys():
        raise ValueError("task progress is incomplete")
    for key in ("total", "current", "success_count", "failed_count", "skipped_count"):
        if type(progress[key]) is not int or progress[key] < 0:
            raise ValueError("task progress counters are invalid")
    if type(progress["percentage"]) not in (int, float) or not math.isfinite(progress["percentage"]):
        raise ValueError("task progress percentage is invalid")
    if not isinstance(progress["status"], str) or not isinstance(progress["current_item"], str):
        raise ValueError("task progress text is invalid")
    if progress["started_at"] is not None:
        _parse_started_at(progress["started_at"])
    stored = response["stored"]
    parent_fields = {
        "id", "task_id", "task_name", "description", "enabled", "schedule_type",
        "interval_seconds", "cron_expression", "schedule_time", "timezone", "config",
        "created_at", "updated_at", "last_run_at", "next_run_at",
    } | alerts
    if not isinstance(stored, dict) or not parent_fields <= stored.keys():
        raise ValueError("stored task is incomplete")
    if stored["task_id"] != task_id or type(stored["id"]) is not int or stored["id"] <= 0:
        raise ValueError("stored task identity is invalid")
    if not isinstance(stored["task_name"], str) or (
        stored["description"] is not None and not isinstance(stored["description"], str)
    ):
        raise ValueError("stored task text is invalid")
    if stored["config"] is not None and not isinstance(stored["config"], dict):
        raise ValueError("stored task config must be null or an object")
    for key in alerts | {"enabled"}:
        if type(stored[key]) is not bool or stored[key] != response[key]:
            raise ValueError("stored task gates are invalid or inconsistent")
    for key in ("created_at", "updated_at", "last_run_at", "next_run_at"):
        if stored[key] is not None or key in {"created_at", "updated_at"}:
            _parse_started_at(stored[key])
    schedules = response["schedules"]
    if not isinstance(schedules, list):
        raise ValueError("task schedules must be an array")
    seen: set[int] = set()
    for schedule in schedules:
        schedule_fields = {
            "id", "task_id", "name", "enabled", "schedule_type", "interval_seconds",
            "schedule_time", "timezone", "days_of_week", "day_of_month", "week_parity",
            "parameters", "next_run_at", "last_run_at", "created_at", "updated_at", "description",
        }
        if not isinstance(schedule, dict) or not schedule_fields <= schedule.keys():
            raise ValueError("task schedule is incomplete")
        row_id = schedule["id"]
        if type(row_id) is not int or row_id <= 0 or row_id in seen or schedule["task_id"] != task_id:
            raise ValueError("task schedule identity is invalid or duplicated")
        seen.add(row_id)
        if type(schedule["enabled"]) is not bool or not isinstance(schedule["parameters"], dict):
            raise ValueError("task schedule gate or parameters are invalid")
        if (schedule["name"] is not None and not isinstance(schedule["name"], str)) or not isinstance(schedule["description"], str):
            raise ValueError("task schedule text is invalid")
        days = schedule["days_of_week"]
        if not isinstance(days, list) or any(type(day) is not int or day not in range(7) for day in days):
            raise ValueError("task schedule days are invalid")
        cadence = schedule["schedule_type"]
        if cadence not in {"interval", "daily", "weekly", "biweekly", "monthly"}:
            raise ValueError("task schedule cadence is invalid")
        if cadence in {"weekly", "biweekly"} and not days:
            raise ValueError("weekly task schedule requires days")
        month_day = schedule["day_of_month"]
        if month_day is not None and (type(month_day) is not int or month_day not in {-1, *range(1, 32)}):
            raise ValueError("task schedule month day is invalid")
        if cadence == "monthly" and month_day is None:
            raise ValueError("monthly task schedule requires a day")
        parity = schedule["week_parity"]
        if parity is not None and (type(parity) is not int or parity not in {0, 1}):
            raise ValueError("task schedule week parity is invalid")
        for key in ("created_at", "updated_at", "last_run_at", "next_run_at"):
            if schedule[key] is not None or key in {"created_at", "updated_at"}:
                _parse_started_at(schedule[key])
    runtime_schedule = response["schedule"]
    if not isinstance(runtime_schedule, dict) or not {
        "schedule_type", "interval_seconds", "cron_expression", "schedule_time", "timezone",
    } <= runtime_schedule.keys():
        raise ValueError("runtime schedule is incomplete")
    for row in [stored, runtime_schedule, *schedules]:
        cadence = row["schedule_type"]
        if row is stored or row is runtime_schedule:
            if cadence not in {"manual", "interval", "cron", "daily"}:
                raise ValueError("parent task cadence is invalid")
            if row["cron_expression"] is not None and not isinstance(row["cron_expression"], str):
                raise ValueError("task cron expression is invalid")
            if cadence == "cron" and not row["cron_expression"]:
                raise ValueError("cron task requires an expression")
        interval = row["interval_seconds"]
        if interval is not None and (type(interval) is not int or interval <= 0):
            raise ValueError("task interval is invalid")
        if cadence == "interval" and interval is None:
            raise ValueError("interval task requires an interval")
        schedule_time = row["schedule_time"]
        if schedule_time is not None:
            if not isinstance(schedule_time, str) or len(schedule_time) != 5:
                raise ValueError("task schedule time is invalid")
            datetime.strptime(schedule_time, "%H:%M")
        if cadence in {"daily", "weekly", "biweekly", "monthly"} and schedule_time is None:
            raise ValueError("calendar task requires a time")
        if row["timezone"] is not None:
            if not isinstance(row["timezone"], str) or not row["timezone"]:
                raise ValueError("task timezone is invalid")
            try:
                ZoneInfo(row["timezone"])
            except (ValueError, ZoneInfoNotFoundError) as error:
                raise ValueError("task timezone is invalid") from error
    effective = stored["enabled"] and (not schedules or any(row["enabled"] for row in schedules))
    if response["effective_enabled"] != effective:
        raise ValueError("task effective gate is inconsistent")
    if not {
        "running", "check_interval", "max_concurrent", "active_tasks",
        "active_task_count", "registered_task_count",
    } <= engine.keys():
        raise ValueError("task engine is incomplete")
    if type(engine["running"]) is not bool:
        raise ValueError("task engine running gate is invalid")
    for key in ("check_interval", "max_concurrent"):
        if type(engine[key]) is not int or engine[key] <= 0:
            raise ValueError("task engine capacity is invalid")
    active = engine["active_tasks"]
    if not isinstance(active, list) or any(not isinstance(item, str) or not item.strip() for item in active):
        raise ValueError("active task membership is invalid")
    if len(set(active)) != len(active) or type(engine["active_task_count"]) is not int or engine["active_task_count"] != len(active):
        raise ValueError("active task membership count is invalid")
    if type(engine["registered_task_count"]) is not int or engine["registered_task_count"] < 0:
        raise ValueError("registered task count is invalid")
    return {
        "task": {**response, "schedules": sorted(schedules, key=lambda row: row["id"])},
        "engine": {**engine, "active_tasks": sorted(active)},
    }


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
    async def list_tasks(
        task_id: Annotated[str | None, Field(strict=True)] = None,
        details: Annotated[bool, Field(strict=True)] = False,
    ) -> str:
        """List task summaries or read complete safe details for one exact task.

        Args:
            task_id: Select one task. Required for details.
            details: Return stored settings, runtime state, schedules and engine state.
        """
        try:
            if task_id is not None and (not isinstance(task_id, str) or not task_id.strip()):
                raise ValueError("task_id must be non-empty")
            if type(details) is not bool or (details and task_id is None):
                raise ValueError("details requires one task_id")
            client = get_ecm_client()
            if task_id is not None:
                resp = await client.call_endpoint(
                    ENDPOINTS["tasks_get"], path_args={"task_id": task_id},
                    query={"details": True} if details else None,
                )
                if details:
                    engine = await client.call_endpoint(ENDPOINTS["tasks_engine_status"])
                    result = _validate_task(task_id, resp, engine)
                    return _rule_details(result, fields=frozenset(result), strict=True)
                if not isinstance(resp, dict) or resp.get("task_id") != task_id:
                    raise ValueError("selected task identity is invalid")
                tasks = [resp]
            else:
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
            if details:
                logger.error("[MCP] Detailed task read failed")
                return "Cannot return complete task details: task read failed or required fields are invalid."
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
                checked = _rule_details(execution, fields=frozenset(execution), strict=True)
                if not checked.startswith("{"):
                    return checked
                if not wait_for_completion or execution["status"] in _TERMINAL_EXECUTION_STATUSES:
                    return checked
                retry_delay = _FIRST_RETRY_DELAY_SECONDS
                await asyncio.sleep(_POLL_DELAY_SECONDS)
        except Exception:
            logger.error("[MCP] Task execution read failed")
            checked = _rule_details(identity, fields=frozenset(identity), strict=True)
            if checked.startswith("{"):
                return f"Task execution read failed for {checked}."
            return "Task execution read failed."

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
    async def get_task_history(
        task_id: Annotated[str | None, Field(strict=True)] = None,
        limit: Annotated[int, Field(strict=True)] = 10,
        offset: Annotated[int, Field(strict=True)] = 0,
        details: Annotated[bool, Field(strict=True)] = False,
    ) -> str:
        """View task execution history.

        Args:
            task_id: Optional specific task ID to get history for. If omitted, returns all task history.
            limit: Number of history entries to return (default 10)
            offset: Number of records to skip.
            details: Return one complete safe bounded page for a selected task.
        """
        try:
            if task_id is not None and (not isinstance(task_id, str) or not task_id.strip()):
                raise ValueError("task_id must be non-empty")
            if type(details) is not bool:
                raise ValueError("details must be a boolean")
            if details and (
                task_id is None or type(limit) is not int or not 1 <= limit <= 100
                or type(offset) is not int or offset < 0
            ):
                raise ValueError("details requires one task and a bounded integer page")
            client = get_ecm_client()
            query = {"limit": limit + 1 if details else limit}
            if details or offset:
                query["offset"] = offset
            if task_id:
                result = await client.call_endpoint(
                    ENDPOINTS["tasks_history"], path_args={"task_id": task_id}, query=query,
                )
            else:
                result = await client.call_endpoint(ENDPOINTS["tasks_history_all"], query=query)

            if details:
                if not isinstance(result, dict) or not isinstance(result.get("history"), list):
                    raise ValueError("task history is incomplete")
                history = result["history"]
                if len(history) > limit + 1:
                    raise ValueError("task history exceeds the requested page")
                seen: set[int] = set()
                previous: tuple[datetime, int] | None = None
                for row in history:
                    if not isinstance(row, dict):
                        raise ValueError("task history record must be an object")
                    started_at = _parse_started_at(row.get("started_at"))
                    execution = _validate_execution(task_id, row.get("id"), started_at, row, complete=True)
                    row_id = execution["id"]
                    identity = (started_at, row_id)
                    if row_id in seen or (previous is not None and identity >= previous):
                        raise ValueError("task history identities are duplicated or out of order")
                    seen.add(row_id)
                    previous = identity
                checked = _rule_details(result, fields=frozenset(result), strict=True)
                if not checked.startswith("{"):
                    return checked
                page = {
                    "task_id": task_id, "limit": limit, "offset": offset,
                    "has_more": len(history) > limit, "history": history[:limit],
                }
                for key, value in result.items():
                    if key != "history":
                        if key in page:
                            raise ValueError("task history contains conflicting page fields")
                        page[key] = value
                return _rule_details(page, fields=frozenset(page), strict=True)

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
            if details:
                logger.error("[MCP] Detailed task history read failed")
                return "Cannot return complete task details: history read failed or required fields are invalid."
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
