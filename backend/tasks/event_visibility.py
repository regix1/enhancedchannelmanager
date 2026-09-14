"""Keep event-channel visibility aligned with current guide coverage."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytz

from database import get_session
from dispatcharr_client import get_client
from models import DummyEPGProfile
from task_registry import register_task
from task_scheduler import ScheduleConfig, ScheduleType, TaskResult, TaskScheduler

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 300
FLOW_MAX_AGE = timedelta(minutes=5)
MAX_CHANNELS_PER_RUN = 12
MATCH_STREAM_PAGE_SIZE = 500
MAX_MATCH_STREAMS = 10000


def _round_robin(rows: list, cursor: int, limit: int) -> tuple[list, int]:
    """Take a fair bounded slice so persistent failures cannot starve peers."""
    if not rows or limit <= 0:
        return [], 0
    start = cursor % len(rows)
    count = min(limit, len(rows))
    selected = [rows[(start + offset) % len(rows)] for offset in range(count)]
    return selected, (start + count) % len(rows)


def _stream_id(stream) -> int | None:
    return stream.get("id") if isinstance(stream, dict) else stream


def _stream_group_id(stream) -> int | None:
    if not isinstance(stream, dict):
        return None
    group = stream.get("channel_group_id")
    if group is None:
        group = stream.get("channel_group")
    return group.get("id") if isinstance(group, dict) else group


def _guide_name(current: dict | None, event_timezone: str) -> str | None:
    """Render one current guide row in the event matcher's default shape."""
    if not isinstance(current, dict):
        return None
    title = " ".join(
        str(current.get("title") or "")
        .replace("ᴸᶦᵛᵉ", "")
        .replace("ᴺᵉʷ", "")
        .split()
    )
    if not title:
        return None
    from services.epg_programmes import _placeholder

    import xml.etree.ElementTree as ET

    marker = ET.Element("programme")
    ET.SubElement(marker, "title").text = title
    if _placeholder(marker):
        return None
    try:
        start = datetime.fromisoformat(
            str(current["start"]).replace("Z", "+00:00")
        )
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        local = start.astimezone(pytz.timezone(event_timezone))
    except (KeyError, TypeError, ValueError, pytz.UnknownTimeZoneError):
        return None
    clock = local.strftime("%I:%M %p").lstrip("0")
    return f"{title} @ {local.strftime('%b')} {local.day} {clock}"


async def _fetch_match_streams(client, group_ids: list[int]) -> tuple[list, dict[int, int]]:
    """Fetch the configured event-name stream groups without partial results."""
    from services.event_sync_resolver import SecondaryStream

    streams = []
    groups_by_stream = {}
    for group_id in group_ids:
        group_name = await client._channel_group_name_for_id(group_id)
        if not group_name:
            raise ValueError(f"Stream group {group_id} no longer exists.")
        page = 1
        while True:
            response = await client.get_streams(
                page=page,
                page_size=MATCH_STREAM_PAGE_SIZE,
                channel_group_name=group_name,
            )
            rows = (
                response.get("results", [])
                if isinstance(response, dict)
                else (response or [])
            )
            for row in rows:
                stream_id = row.get("id")
                name = row.get("name")
                if stream_id is None or not name:
                    continue
                streams.append(SecondaryStream(
                    name=name,
                    group_id=group_id,
                    stream_id=stream_id,
                    is_stale=row.get("is_stale"),
                ))
                groups_by_stream[stream_id] = group_id
            if len(streams) > MAX_MATCH_STREAMS:
                raise ValueError(
                    f"Guide-match stream scan exceeds {MAX_MATCH_STREAMS} streams."
                )
            if not isinstance(response, dict) or not response.get("next"):
                break
            page += 1
    return streams, groups_by_stream


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
        coverage_by_channel = {
            row["channel_id"]: row for row in coverage.get("channels", ())
        }
        current_channel_ids = {
            channel_id for channel_id, row in coverage_by_channel.items()
            if row.get("current") is not None
        }

        match_groups_by_target = {}
        timezone_by_target = {}
        for profile in sorted(profiles, key=lambda row: row.get("id") or 0):
            match_group_ids = profile.get("stream_match_group_ids") or []
            for group_id in profile.get("hide_empty_group_ids") or []:
                if match_group_ids:
                    match_groups_by_target.setdefault(group_id, match_group_ids)
                    timezone_by_target.setdefault(
                        group_id, profile.get("event_timezone") or "US/Eastern",
                    )

        event_channels = [
            (channel_id, channel)
            for channel_id, channel in channel_map.items()
            if channel.get("channel_group_id") in wanted
        ]
        ended = [
            (channel_id, channel)
            for channel_id, channel in event_channels
            if channel_id not in current_channel_ids
        ]
        candidates = [
            (channel_id, channel)
            for channel_id, channel in event_channels
            if channel_id in current_channel_ids
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

        all_match_group_ids = list(dict.fromkeys(
            group_id
            for group_ids in match_groups_by_target.values()
            for group_id in group_ids
        ))
        match_streams = []
        groups_by_stream = {}
        match_scan_ready = True
        if all_match_group_ids:
            try:
                match_streams, groups_by_stream = await _fetch_match_streams(
                    client, all_match_group_ids,
                )
            except Exception as exc:
                match_scan_ready = False
                logger.warning(
                    "[%s] Could not load guide-match stream groups: %s",
                    self.task_id,
                    exc,
                )

        hidden_now = []
        stream_updates = []
        failed = 0
        for channel_id, channel in ended:
            update = {}
            if not channel.get("hidden_from_output"):
                update["hidden_from_output"] = True
            group_ids = match_groups_by_target.get(channel.get("channel_group_id"), [])
            if match_scan_ready and group_ids:
                fallback_ids = [
                    _stream_id(stream)
                    for stream in channel.get("streams") or []
                    if (_stream_group_id(stream) or groups_by_stream.get(_stream_id(stream)))
                    not in set(group_ids)
                ]
                fallback_ids = [stream_id for stream_id in fallback_ids if stream_id is not None]
                if fallback_ids != [
                    _stream_id(stream) for stream in channel.get("streams") or []
                ]:
                    update["streams"] = fallback_ids
            if not update:
                continue
            try:
                await client.update_channel(channel_id, update)
                if "hidden_from_output" in update:
                    hidden_now.append(channel_id)
                if "streams" in update:
                    stream_updates.append(channel_id)
            except Exception as exc:
                failed += 1
                logger.warning(
                    "[%s] Could not hide channel %s: %s",
                    self.task_id,
                    channel_id,
                    exc,
                )

        guide_name_to_ids = {}
        for channel_id, channel in selected:
            target_group_id = channel.get("channel_group_id")
            if target_group_id not in match_groups_by_target:
                continue
            guide_name = _guide_name(
                (coverage_by_channel.get(channel_id) or {}).get("current"),
                timezone_by_target.get(target_group_id, "US/Eastern"),
            )
            if guide_name:
                guide_name_to_ids.setdefault(guide_name, []).append(channel_id)

        matches_by_channel = {}
        if match_scan_ready and guide_name_to_ids and match_streams:
            from services.event_sync_resolver import (
                DISPOSITION_WOULD_ATTACH,
                resolve_event_sync,
            )
            from concurrency import run_cpu_bound

            try:
                resolution = await run_cpu_bound(
                    resolve_event_sync,
                    {
                        "master_group_id": 0,
                        "secondary_group_ids": all_match_group_ids,
                        "time_window_minutes": 30,
                        "enforce_time_window": True,
                        "attach_threshold": 0.8,
                        "assume_current_date": False,
                    },
                    sorted(guide_name_to_ids),
                    match_streams,
                    now=now,
                )
            except Exception as exc:
                match_scan_ready = False
                logger.warning(
                    "[%s] Could not match streams to the current guide: %s",
                    self.task_id,
                    exc,
                )
            else:
                for resolved in resolution.resolved:
                    if resolved.disposition != DISPOSITION_WOULD_ATTACH:
                        continue
                    for channel_id in guide_name_to_ids.get(
                        resolved.best.master_name, [],
                    ):
                        channel = channel_map.get(channel_id)
                        if channel is None:
                            continue
                        allowed = match_groups_by_target.get(
                            channel.get("channel_group_id"), [],
                        )
                        if resolved.stream.group_id not in allowed:
                            continue
                        matches_by_channel.setdefault(channel_id, []).append(
                            resolved.stream
                        )

        stream_ids = {
            _stream_id(stream)
            for _, channel in selected
            for stream in channel.get("streams") or []
        } | {
            row.stream_id
            for rows in matches_by_channel.values()
            for row in rows
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
            if hidden_now or stream_updates:
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
        hidden_failed = []
        for index, (channel_id, channel) in enumerate(selected, start=1):
            attached_ids = [
                _stream_id(stream) for stream in channel.get("streams") or []
            ]
            target_group_id = channel.get("channel_group_id")
            group_ids = (
                match_groups_by_target.get(target_group_id, [])
                if match_scan_ready else []
            )
            group_set = set(group_ids)
            rank = {group_id: position for position, group_id in enumerate(group_ids)}
            fallback_ids = [
                stream_id
                for stream, stream_id in zip(channel.get("streams") or [], attached_ids)
                if stream_id is not None
                and (_stream_group_id(stream) or groups_by_stream.get(stream_id))
                not in group_set
            ]
            matched = sorted(
                matches_by_channel.get(channel_id, []),
                key=lambda row: (rank.get(row.group_id, len(rank)), row.stream_id or 0),
            )
            primary_ids = [
                row.stream_id for row in matched
                if row.stream_id is not None and flow.get(row.stream_id) is True
            ]
            primary_ids.extend(
                row.stream_id for row in matched
                if row.stream_id is not None
                and row.stream_id in attached_ids
                and flow.get(row.stream_id) is None
                and row.stream_id not in primary_ids
            )
            desired_ids = primary_ids + [
                stream_id for stream_id in fallback_ids
                if stream_id not in primary_ids
            ]
            update = {}
            if match_scan_ready and group_ids and desired_ids != attached_ids:
                update["streams"] = desired_ids

            states = [flow.get(stream_id) for stream_id in desired_ids]
            if any(state is True for state in states):
                hide = False
            elif states and all(state is False for state in states):
                hide = True
            elif not states:
                hide = True
            else:
                hide = bool(channel.get("hidden_from_output"))
            if bool(channel.get("hidden_from_output")) is not hide:
                update["hidden_from_output"] = hide

            if update:
                try:
                    await client.update_channel(channel_id, update)
                    if "streams" in update:
                        stream_updates.append(channel_id)
                    if "hidden_from_output" in update:
                        (hidden_failed if hide else shown).append(channel_id)
                except Exception as exc:
                    failed += 1
                    logger.warning(
                        "[%s] Could not update event channel %s: %s",
                        self.task_id, channel_id, exc,
                    )
            self._set_progress(
                total=len(selected) + len(ended),
                current=index + len(ended),
                success_count=len(
                    set(shown)
                    | set(hidden_now)
                    | set(hidden_failed)
                    | set(stream_updates)
                ),
                failed_count=failed,
                status="probing",
            )

        if shown or hidden_now or hidden_failed or stream_updates:
            from emby_client import request_guide_refresh

            await request_guide_refresh()

        changed_channel_ids = (
            set(shown)
            | set(hidden_now)
            | set(hidden_failed)
            | set(stream_updates)
        )
        total_items = len(selected) + len(ended)
        return TaskResult(
            success=True,
            message=(
                f"Checked {len(selected)} active event channel(s), "
                f"revealed {len(shown)}, hid {len(hidden_now) + len(hidden_failed)}, "
                f"updated streams on {len(stream_updates)} channel(s)"
            ),
            started_at=started_at,
            completed_at=datetime.utcnow(),
            total_items=total_items,
            success_count=len(changed_channel_ids),
            failed_count=failed,
            skipped_count=max(0, total_items - len(changed_channel_ids) - failed),
            details={
                "revealed_channel_ids": shown,
                "hidden_channel_ids": hidden_now + hidden_failed,
                "stream_updated_channel_ids": stream_updates,
            },
        )
