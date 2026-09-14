"""Quickly reveal hidden event channels when their streams begin flowing."""
from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional

from cache import get_cache
from database import get_session
from dispatcharr_client import get_client
from models import DummyEPGProfile
from task_registry import register_task
from task_scheduler import ScheduleConfig, ScheduleType, TaskResult, TaskScheduler

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 300
FLOW_MAX_AGE = timedelta(minutes=5)
MAX_CHANNELS_PER_RUN = 12


def _current_xmltv_ids(guide_text: str, now: datetime) -> set[str]:
    """Return outward channel ids carrying a real programme at ``now``."""
    from services.epg_programmes import _placeholder, programme_times

    root = ET.fromstring(guide_text)
    current = set()
    for programme in root.findall("programme"):
        try:
            begin, end = programme_times(programme)
        except ValueError:
            continue
        if begin <= now < end and not _placeholder(programme):
            channel = programme.get("channel")
            if channel:
                current.add(channel)
    return current


def _round_robin(rows: list, cursor: int, limit: int) -> tuple[list, int]:
    """Take a fair bounded slice so persistent failures cannot starve peers."""
    if not rows or limit <= 0:
        return [], 0
    start = cursor % len(rows)
    count = min(limit, len(rows))
    selected = [rows[(start + offset) % len(rows)] for offset in range(count)]
    return selected, (start + count) % len(rows)


@register_task
class EventVisibilityTask(TaskScheduler):
    """Probe only hidden event slots whose published guide says they are live."""

    task_id = "event_visibility"
    task_name = "Event Visibility Check"
    task_description = "Reveal scheduled PPV and ESPN+ channels when measured stream flow begins"

    def __init__(self, schedule_config: Optional[ScheduleConfig] = None):
        if schedule_config is None:
            schedule_config = ScheduleConfig(
                schedule_type=ScheduleType.INTERVAL,
                interval_seconds=CHECK_INTERVAL_SECONDS,
                timezone="America/Chicago",
            )
        super().__init__(schedule_config)
        self._cursor = 0

    async def execute(self) -> TaskResult:
        started_at = datetime.utcnow()
        now = datetime.now(timezone.utc)

        from routers.dummy_epg import XMLTV_CACHE_TTL

        guide_text = get_cache().get("dummy_epg_xmltv_all", ttl=XMLTV_CACHE_TTL)
        if not guide_text:
            return TaskResult(
                success=True,
                message="Published event guide is not ready",
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )

        try:
            current_xmltv_ids = await asyncio.to_thread(
                _current_xmltv_ids, guide_text, now,
            )
        except (ET.ParseError, TypeError, ValueError) as exc:
            logger.warning("[%s] Published event guide could not be read: %s", self.task_id, exc)
            return TaskResult(
                success=False,
                message="Published event guide could not be read",
                error="GUIDE_UNREADABLE",
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )

        session = get_session()
        try:
            profiles = [
                profile.to_dict()
                for profile in session.query(DummyEPGProfile).filter(
                    DummyEPGProfile.enabled == True  # noqa: E712
                ).all()
            ]
        finally:
            session.close()

        wanted = {
            group_id
            for profile in profiles
            for group_id in profile.get("hide_empty_group_ids") or []
        }
        if not wanted:
            return TaskResult(
                success=True,
                message="No event groups use automatic visibility",
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )

        from dummy_epg_engine import get_xmltv_id
        from services.epg_programmes import _fetch_all_channels

        client = get_client()
        channel_map = await _fetch_all_channels(client)
        candidates = []
        candidate_ids = set()
        for profile in profiles:
            profile_groups = set(profile.get("hide_empty_group_ids") or [])
            for channel_id, channel in channel_map.items():
                if (
                    channel.get("channel_group_id") not in profile_groups
                    or not channel.get("hidden_from_output")
                    or channel_id in candidate_ids
                ):
                    continue
                xmltv_id = get_xmltv_id({"channel_id": channel_id}, channel, profile)
                if xmltv_id in current_xmltv_ids:
                    candidates.append((channel_id, channel))
                    candidate_ids.add(channel_id)

        candidates.sort(key=lambda row: (row[1].get("channel_number") or 999999, row[0]))
        selected, self._cursor = _round_robin(
            candidates, self._cursor, MAX_CHANNELS_PER_RUN,
        )
        if not selected:
            return TaskResult(
                success=True,
                message="No hidden event channels are currently scheduled",
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )

        stream_ids = {
            stream.get("id") if isinstance(stream, dict) else stream
            for _, channel in selected
            for stream in channel.get("streams") or []
        }
        stream_ids.discard(None)

        self._set_progress(
            total=len(selected),
            status="probing",
            current_item="Checking scheduled hidden event channels",
        )

        from services.event_sync_stream_health import collect_stream_flow

        flow = await collect_stream_flow(
            stream_ids,
            client=client,
            checked_after=now - FLOW_MAX_AGE,
            probe_missing=True,
            cancelled=lambda: self._cancel_requested,
        )
        if self._cancel_requested:
            return TaskResult(
                success=False,
                message="Event visibility check cancelled",
                error="CANCELLED",
                started_at=started_at,
                completed_at=datetime.utcnow(),
                total_items=len(selected),
            )

        shown = []
        failed = 0
        for index, (channel_id, channel) in enumerate(selected, start=1):
            states = [
                flow.get(stream.get("id") if isinstance(stream, dict) else stream)
                for stream in channel.get("streams") or []
            ]
            if any(state is True for state in states):
                try:
                    await client.update_channel(channel_id, {"hidden_from_output": False})
                    shown.append(channel_id)
                except Exception as exc:
                    failed += 1
                    logger.warning(
                        "[%s] Could not reveal channel %s: %s",
                        self.task_id, channel_id, exc,
                    )
            self._set_progress(
                total=len(selected),
                current=index,
                success_count=len(shown),
                failed_count=failed,
                status="probing",
            )

        if shown:
            from emby_client import request_guide_refresh

            await request_guide_refresh()

        return TaskResult(
            success=True,
            message=f"Checked {len(selected)} scheduled hidden channel(s), revealed {len(shown)}",
            started_at=started_at,
            completed_at=datetime.utcnow(),
            total_items=len(selected),
            success_count=len(shown),
            failed_count=failed,
            skipped_count=len(selected) - len(shown) - failed,
            details={"revealed_channel_ids": shown},
        )
