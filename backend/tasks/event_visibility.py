"""Keep event-channel visibility aligned with current guide coverage."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from database import get_session
from dispatcharr_client import get_client
from models import DummyEPGProfile
from task_registry import register_task
from task_scheduler import ScheduleConfig, ScheduleType, TaskResult, TaskScheduler

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 300
FLOW_MAX_AGE = timedelta(minutes=5)
MAX_CHANNELS_PER_RUN = 12


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
    """Show current flowing event slots and hide slots whose events ended."""

    task_id = "event_visibility"
    task_name = "Event Visibility Check"
    task_description = "Keep PPV and ESPN+ visibility aligned with active guide and stream flow"

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

        from services.epg_programmes import _fetch_all_channels, can_cache, prepare_profiles

        client = get_client()
        channel_map = await _fetch_all_channels(client)
        _, coverage = await prepare_profiles(profiles, channel_map, client)
        if not can_cache(coverage):
            return TaskResult(
                success=True,
                message="Published event guide is not ready",
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )
        current_channel_ids = {
            row["channel_id"]
            for row in coverage.get("channels", ())
            if row.get("current") is not None
        }

        event_channels = [
            (channel_id, channel)
            for channel_id, channel in channel_map.items()
            if channel.get("channel_group_id") in wanted
        ]
        ended = [
            (channel_id, channel)
            for channel_id, channel in event_channels
            if not channel.get("hidden_from_output")
            and channel_id not in current_channel_ids
        ]
        candidates = [
            (channel_id, channel)
            for channel_id, channel in event_channels
            if channel.get("hidden_from_output")
            and channel_id in current_channel_ids
        ]

        candidates.sort(key=lambda row: (row[1].get("channel_number") or 999999, row[0]))
        selected, self._cursor = _round_robin(
            candidates, self._cursor, MAX_CHANNELS_PER_RUN,
        )
        if not selected and not ended:
            return TaskResult(
                success=True,
                message="No event channel visibility changes are needed",
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )

        hidden_now = []
        failed = 0
        for channel_id, _ in ended:
            try:
                await client.update_channel(
                    channel_id, {"hidden_from_output": True},
                )
                hidden_now.append(channel_id)
            except Exception as exc:
                failed += 1
                logger.warning(
                    "[%s] Could not hide channel %s: %s",
                    self.task_id,
                    channel_id,
                    exc,
                )

        stream_ids = {
            stream.get("id") if isinstance(stream, dict) else stream
            for _, channel in selected
            for stream in channel.get("streams") or []
        }
        stream_ids.discard(None)

        flow = {}
        if selected:
            self._set_progress(
                total=len(selected) + len(ended),
                current=len(ended),
                success_count=len(hidden_now),
                failed_count=failed,
                status="probing",
                current_item="Checking scheduled hidden event channels",
            )

            from services.event_sync_stream_health import collect_stream_flow

            flow = await collect_stream_flow(
                stream_ids,
                client=client,
                checked_after=now - FLOW_MAX_AGE,
                probe_missing=True,
                probe_while_busy=True,
                cancelled=lambda: self._cancel_requested,
            )
        if self._cancel_requested:
            if hidden_now:
                from emby_client import request_guide_refresh

                await request_guide_refresh()
            return TaskResult(
                success=False,
                message="Event visibility check cancelled",
                error="CANCELLED",
                started_at=started_at,
                completed_at=datetime.utcnow(),
                total_items=len(selected) + len(ended),
            )

        shown = []
        reveal_failed = 0
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
                    reveal_failed += 1
                    failed += 1
                    logger.warning(
                        "[%s] Could not reveal channel %s: %s",
                        self.task_id, channel_id, exc,
                    )
            self._set_progress(
                total=len(selected) + len(ended),
                current=index + len(ended),
                success_count=len(shown) + len(hidden_now),
                failed_count=failed,
                status="probing",
            )

        if shown or hidden_now:
            from emby_client import request_guide_refresh

            await request_guide_refresh()

        return TaskResult(
            success=True,
            message=(
                f"Checked {len(selected)} scheduled hidden channel(s), "
                f"revealed {len(shown)}, hid {len(hidden_now)} ended channel(s)"
            ),
            started_at=started_at,
            completed_at=datetime.utcnow(),
            total_items=len(selected) + len(ended),
            success_count=len(shown) + len(hidden_now),
            failed_count=failed,
            skipped_count=len(selected) - len(shown) - reveal_failed,
            details={
                "revealed_channel_ids": shown,
                "hidden_channel_ids": hidden_now,
            },
        )
