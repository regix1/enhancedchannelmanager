"""Publish and deliver generated guide documents through the shared workflow."""
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from dispatcharr_client import get_client
from task_registry import register_task
from task_scheduler import ScheduleConfig, ScheduleType, TaskResult, TaskScheduler

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 5
MAX_WAIT_SECONDS = 300


async def wait_for_epg_source_refresh(
    client,
    source_id: int,
    source_name: str,
    poll_interval: int = POLL_INTERVAL_SECONDS,
    max_wait: int = MAX_WAIT_SECONDS,
    *,
    initial_source: dict | None = None,
    trigger: bool = True,
    cancelled: Callable[[], bool] | None = None,
) -> bool:
    """Trigger or observe a source refresh, returning only confirmed completion."""
    if cancelled is not None and cancelled():
        return False
    if initial_source is None:
        initial_source = await client.get_epg_source(source_id)
    initial_updated = initial_source.get("updated_at") or initial_source.get("last_updated")
    if cancelled is not None and cancelled():
        return False
    if trigger:
        await client.refresh_epg_source(source_id)

    started = time.monotonic()
    running_states = {
        "fetching", "processing", "parsing", "loading", "pending",
        "running", "queued", "refreshing",
    }
    observed_running = str(initial_source.get("status") or "").strip().lower() in running_states
    while True:
        if cancelled is not None and cancelled():
            return False
        remaining = max_wait - (time.monotonic() - started)
        if remaining <= 0:
            logger.warning("[EPG-REFRESH] Timeout waiting for source %s", source_id)
            return False
        await asyncio.sleep(min(max(0, poll_interval), remaining))
        if cancelled is not None and cancelled():
            return False
        current_source = await client.get_epg_source(source_id)
        status = str(current_source.get("status") or "").strip().lower()
        current_updated = current_source.get("updated_at") or current_source.get("last_updated")
        if status in {"error", "failed", "failure", "cancelled", "canceled"}:
            logger.warning("[EPG-REFRESH] Source %s ended with status %s", source_id, status)
            return False
        if status in running_states:
            observed_running = True
            continue
        succeeded = status in {"success", "completed", "complete", "ok", "done"}
        changed = bool(current_updated and current_updated != initial_updated)
        if (succeeded and (changed or observed_running)) or (not status and changed):
            logger.info("[EPG-REFRESH] Source %s refresh complete", source_id)
            return True


@register_task
class DummyEPGRefreshTask(TaskScheduler):
    """Run the full guide reconciliation on the manual refresh schedule."""

    task_id = "dummy_epg_refresh"
    task_name = "Dummy EPG Refresh"
    task_description = "Publish generated guide data and reconcile its event channels"

    def __init__(self, schedule_config: Optional[ScheduleConfig] = None):
        if schedule_config is None:
            schedule_config = ScheduleConfig(schedule_type=ScheduleType.MANUAL)
        super().__init__(schedule_config)

    async def _regenerate_xmltv(self):
        """Publish complete documents without owning visibility or delivery."""
        from cache import get_cache
        from concurrency import run_cpu_bound
        from database import get_session
        from models import DummyEPGProfile
        from services.epg_programmes import _fetch_all_channels, prepare_profiles
        from services.epg_publication import publication_lock, publish_profiles

        session = get_session()
        try:
            profiles = [
                row.to_dict()
                for row in session.query(DummyEPGProfile).filter(
                    DummyEPGProfile.enabled == True  # noqa: E712
                ).all()
            ]
        finally:
            session.close()

        client = get_client()
        channel_map = await _fetch_all_channels(client)
        prepared, coverage = await prepare_profiles(
            profiles, channel_map, client, wait_for_sources=True,
        )
        async with publication_lock:
            result = await run_cpu_bound(
                publish_profiles,
                prepared,
                channel_map,
                coverage,
                observations={},
                now=datetime.now(timezone.utc),
            )
            if result.superseded:
                return result
            cache = get_cache()
            cache.invalidate_prefix("dummy_epg_xmltv")
            for scope, document in result.xmltv_by_scope.items():
                key = (
                    "dummy_epg_xmltv_all" if scope == "all"
                    else f"dummy_epg_xmltv_{scope.split(':', 1)[1]}"
                )
                cache.set(key, document)
            return result

    async def execute(self) -> TaskResult:
        from tasks.event_visibility import reconcile_profiles

        return await reconcile_profiles(self, wait_for_sources=True)
