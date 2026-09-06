"""
Dummy EPG Refresh Task.

Scheduled task to regenerate ECM dummy EPG XMLTV data and refresh
matching sources in Dispatcharr.
"""
import asyncio
import time
from typing import Callable
import logging
from datetime import datetime
from typing import Optional

from dispatcharr_client import get_client
from task_scheduler import TaskScheduler, TaskResult, ScheduleConfig, ScheduleType
from task_registry import register_task

logger = logging.getLogger(__name__)

# Polling configuration for waiting for refresh completion
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
    running_states = {"fetching", "processing", "parsing", "loading", "pending", "running", "queued", "refreshing"}
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
    """
    Regenerate ECM dummy EPG XMLTV cache and refresh matching
    Dispatcharr EPG sources.

    Pipeline:
    1. Regenerate all ECM XMLTV cache (same as POST /api/dummy-epg/generate)
    2. Find Dispatcharr EPG sources whose URL contains /api/dummy-epg/xmltv
    3. Trigger refresh for each matching source
    4. Poll until refresh completes
    """

    task_id = "dummy_epg_refresh"
    task_name = "Dummy EPG Refresh"
    task_description = "Regenerate ECM dummy EPG data and refresh in Dispatcharr"

    def __init__(self, schedule_config: Optional[ScheduleConfig] = None):
        if schedule_config is None:
            schedule_config = ScheduleConfig(
                schedule_type=ScheduleType.MANUAL,
            )
        super().__init__(schedule_config)

    async def _regenerate_xmltv(self) -> int:
        """Regenerate combined and profile guides from the shared source inputs."""
        from database import get_session
        from models import DummyEPGProfile
        from dummy_epg_engine import generate_xmltv
        from cache import get_cache
        from concurrency import run_cpu_bound
        from services.epg_programmes import _fetch_all_channels, can_cache, prepare_profiles

        cache = get_cache()
        db = get_session()
        try:
            profiles = db.query(DummyEPGProfile).filter(
                DummyEPGProfile.enabled == True  # noqa: E712
            ).all()
            if not profiles:
                cache.invalidate_prefix("dummy_epg_xmltv")
                return 0
            client = get_client()
            channel_map = await _fetch_all_channels(client)
            profile_data, _coverage = await prepare_profiles(
                [profile.to_dict() for profile in profiles], channel_map, client,
                wait_for_sources=True,
            )
            xml_string = await run_cpu_bound(generate_xmltv, profile_data, channel_map)
            per_profile = {}
            for profile in profile_data:
                per_profile[profile["id"]] = await run_cpu_bound(
                    generate_xmltv, [profile], channel_map,
                )
            # Only drop the published guide once this run has one to put in its place.
            # A scan that timed out composes a guide of empty channels, and discarding
            # the last good one for that leaves nothing to serve until the next run.
            if can_cache(_coverage):
                cache.invalidate_prefix("dummy_epg_xmltv")
                cache.set("dummy_epg_xmltv_all", xml_string)
                for profile_id, per_xml in per_profile.items():
                    cache.set(f"dummy_epg_xmltv_{profile_id}", per_xml)
            logger.info("[%s] Regenerated XMLTV for %s profiles", self.task_id, len(profiles))
            return len(profiles)
        finally:
            db.close()

    async def execute(self) -> TaskResult:
        """Execute the dummy EPG refresh pipeline."""
        client = get_client()
        started_at = datetime.utcnow()

        # Step 1: Regenerate XMLTV cache
        self._set_progress(status="regenerating", current_item="Regenerating XMLTV...")

        try:
            profile_count = await self._regenerate_xmltv()
            logger.info("[%s] Regenerated %s profiles", self.task_id, profile_count)
        except Exception as e:
            logger.exception("[%s] Failed to regenerate XMLTV: %s", self.task_id, e)
            return TaskResult(
                success=False,
                message=f"Failed to regenerate XMLTV: {e}",
                error=str(e),
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )

        if self._cancel_requested:
            return TaskResult(
                success=False, message="Cancelled", error="CANCELLED",
                started_at=started_at, completed_at=datetime.utcnow(),
            )

        # Step 2: Find matching Dispatcharr sources
        self._set_progress(status="finding_sources", current_item="Finding Dispatcharr sources...")

        try:
            all_sources = await client.get_epg_sources()
        except Exception as e:
            logger.exception("[%s] Failed to fetch EPG sources: %s", self.task_id, e)
            return TaskResult(
                success=True,
                message=f"Regenerated {profile_count} profiles, but failed to fetch Dispatcharr sources: {e}",
                started_at=started_at,
                completed_at=datetime.utcnow(),
                total_items=profile_count,
                success_count=profile_count,
            )

        matching = [
            s for s in all_sources
            if s.get("is_active") and s.get("url") and "/api/dummy-epg/xmltv" in s["url"]
        ]

        if not matching:
            logger.info("[%s] No matching Dispatcharr sources to refresh", self.task_id)
            return TaskResult(
                success=True,
                message=f"Regenerated {profile_count} profiles, no Dispatcharr sources to refresh",
                started_at=started_at,
                completed_at=datetime.utcnow(),
                total_items=profile_count,
                success_count=profile_count,
            )

        # Step 3: Refresh each matching source
        self._set_progress(
            total=len(matching), current=0, status="refreshing",
            current_item=f"Refreshing {len(matching)} sources in Dispatcharr...",
        )

        success_count = 0
        failed_count = 0
        refreshed = []
        errors = []

        for i, source in enumerate(matching):
            if self._cancel_requested:
                break

            source_id = source["id"]
            source_name = source.get("name", f"Source {source_id}")
            self._set_progress(
                current=i + 1,
                current_item=f"Refreshing {source_name}...",
            )

            try:
                completed = await wait_for_epg_source_refresh(
                    client, source_id, source_name,
                    poll_interval=POLL_INTERVAL_SECONDS, max_wait=MAX_WAIT_SECONDS,
                    cancelled=lambda: self._cancel_requested,
                )
                if self._cancel_requested:
                    break
                if not completed:
                    raise RuntimeError("EPG source refresh did not complete successfully")
                success_count += 1
                refreshed.append(source_name)
                self._increment_progress(success_count=1)
            except Exception as e:
                logger.error("[%s] Failed to refresh %s: %s", self.task_id, source_name, e)
                failed_count += 1
                errors.append(f"{source_name}: {e}")
                self._increment_progress(failed_count=1)

        self._set_progress(
            success_count=success_count,
            failed_count=failed_count,
            status="completed" if not self._cancel_requested else "cancelled",
        )

        duration = (datetime.utcnow() - started_at).total_seconds()
        logger.info(
            "[%s] Finished in %.1fs: regenerated %s profiles, refreshed %s/%s sources",
            self.task_id, duration, profile_count, success_count, len(matching),
        )

        if self._cancel_requested:
            return TaskResult(
                success=False, message="Cancelled", error="CANCELLED",
                started_at=started_at, completed_at=datetime.utcnow(),
                total_items=len(matching), success_count=success_count,
                failed_count=failed_count,
                details={"profiles_regenerated": profile_count, "refreshed": refreshed, "errors": errors},
            )

        msg = f"Regenerated {profile_count} profiles, refreshed {success_count} Dispatcharr sources"
        if failed_count:
            msg += f", {failed_count} failed"

        return TaskResult(
            success=failed_count == 0 or success_count > 0,
            message=msg,
            started_at=started_at,
            completed_at=datetime.utcnow(),
            total_items=len(matching),
            success_count=success_count,
            failed_count=failed_count,
            details={"profiles_regenerated": profile_count, "refreshed": refreshed, "errors": errors},
        )
