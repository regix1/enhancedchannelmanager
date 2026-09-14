"""Focused checks for the quick hidden-event visibility task."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from task_scheduler import ScheduleType
from tasks.event_visibility import (
    CHECK_INTERVAL_SECONDS,
    EventVisibilityTask,
    _guide_name,
    _round_robin,
)


def _coverage(current: bool = True):
    return {
        "sources": [{"status": "ready"}],
        "channels": [{
            "channel_id": 10,
            "current": {"title": "Current event"} if current else None,
        }],
    }


def _profile(stream_match_group_ids=None):
    return SimpleNamespace(to_dict=lambda: {
        "id": 1,
        "enabled": True,
        "hide_empty_group_ids": [65, 2479],
        "stream_match_group_ids": stream_match_group_ids or [],
        "event_timezone": "US/Eastern",
        "tvg_id_template": "ecm-{channel_id}",
    })


def _session(profile=None):
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [profile or _profile()]
    return session


def test_default_schedule_checks_every_five_minutes():
    task = EventVisibilityTask()

    assert task.schedule_config.schedule_type is ScheduleType.INTERVAL
    assert task.schedule_config.interval_seconds == CHECK_INTERVAL_SECONDS == 300
    assert task.schedule_config.timezone == "America/Chicago"


def test_round_robin_does_not_starve_later_channels():
    first, cursor = _round_robin([1, 2, 3, 4], 0, 2)
    second, cursor = _round_robin([1, 2, 3, 4], cursor, 2)

    assert first == [1, 2]
    assert second == [3, 4]
    assert cursor == 0


def test_rtv_programme_matches_trex_event_name():
    from services.event_sync_resolver import SecondaryStream, resolve_event_sync

    guide_name = _guide_name({
        "title": "The Pat McAfee Show  ᴸᶦᵛᵉ",
        "start": "2026-09-14T15:55:00+00:00",
    }, "US/Eastern")
    resolution = resolve_event_sync(
        {
            "master_group_id": 0,
            "secondary_group_ids": [1558],
            "time_window_minutes": 30,
            "enforce_time_window": True,
            "attach_threshold": 0.8,
            "assume_current_date": False,
        },
        [guide_name],
        [SecondaryStream(
            name=(
                "NEXT | THE PAT MCAFEE SHOW | Mon 14 Sep 12:00 EDT (US) | "
                "8K EXCLUSIVE | US: ESPN+ PPV 8"
            ),
            group_id=1558,
            stream_id=210,
        )],
        now=datetime(2026, 9, 14, 18, tzinfo=timezone.utc),
    )

    assert resolution.resolved[0].disposition == "would_attach"


@pytest.mark.asyncio
async def test_reveals_flowing_hidden_channel_and_refreshes_emby():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10,
            "name": "PPV 10",
            "channel_number": 900,
            "channel_group_id": 65,
            "hidden_from_output": True,
            "streams": [{"id": 110, "name": "PPV stream"}],
        }
    }
    flow = AsyncMock(return_value={110: True})
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([_profile().to_dict()], _coverage()))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=flow), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success is True
    assert result.success_count == 1
    assert result.details == {
        "revealed_channel_ids": [10],
        "hidden_channel_ids": [],
        "stream_updated_channel_ids": [],
    }
    client.update_channel.assert_awaited_once_with(10, {"hidden_from_output": False})
    refresh_emby.assert_awaited_once_with()
    assert flow.await_args.kwargs["probe_missing"] is True
    assert flow.await_args.kwargs["probe_while_busy"] is True


@pytest.mark.asyncio
async def test_leaves_idle_hidden_channel_alone_without_probe():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10,
            "name": "PPV 10",
            "channel_number": 900,
            "channel_group_id": 65,
            "hidden_from_output": True,
            "streams": [{"id": 110, "name": "PPV stream"}],
        }
    }
    flow = AsyncMock()
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([_profile().to_dict()], _coverage(False)))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=flow), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success is True
    assert result.total_items == 1
    flow.assert_not_awaited()
    client.update_channel.assert_not_awaited()
    refresh_emby.assert_not_awaited()


@pytest.mark.asyncio
async def test_hides_ended_visible_channel_and_refreshes_emby():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10,
            "name": "PPV 10",
            "channel_number": 900,
            "channel_group_id": 65,
            "hidden_from_output": False,
            "streams": [{"id": 110, "name": "PPV stream"}],
        }
    }
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([_profile().to_dict()], _coverage(False)))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=AsyncMock()) as flow, \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success_count == 1
    assert result.details == {
        "revealed_channel_ids": [],
        "hidden_channel_ids": [10],
        "stream_updated_channel_ids": [],
    }
    client.update_channel.assert_awaited_once_with(
        10, {"hidden_from_output": True},
    )
    flow.assert_not_awaited()
    refresh_emby.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_waits_for_ready_source_rows():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10,
            "name": "ESPN+ 00",
            "channel_number": 8000,
            "channel_group_id": 2479,
            "hidden_from_output": True,
            "streams": [{"id": 110, "name": "ESPN+ stream"}],
        }
    }
    flow = AsyncMock()

    with patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([_profile().to_dict()], _coverage(False)))), \
         patch("services.epg_programmes.can_cache", return_value=False), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=flow), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        result = await task.execute()

    assert result.message == "Published event guide is not ready"
    client.update_channel.assert_not_awaited()
    flow.assert_not_awaited()


@pytest.mark.asyncio
async def test_does_not_reveal_channel_without_measured_flow():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10,
            "name": "PPV 10",
            "channel_number": 900,
            "channel_group_id": 65,
            "hidden_from_output": True,
            "streams": [{"id": 110, "name": "PPV stream"}],
        }
    }
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([_profile().to_dict()], _coverage()))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=AsyncMock(return_value={110: False})), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success is True
    assert result.success_count == 0
    assert result.skipped_count == 1
    client.update_channel.assert_not_awaited()
    refresh_emby.assert_not_awaited()


@pytest.mark.asyncio
async def test_places_working_guide_matches_in_group_priority_before_fallback():
    task = EventVisibilityTask()
    client = AsyncMock()
    current = {
        "title": "The Pat McAfee Show  ᴸᶦᵛᵉ",
        "start": "2026-09-14T15:55:00+00:00",
    }
    guide_name = _guide_name(current, "US/Eastern")
    channels = {
        10: {
            "id": 10,
            "name": "ESPN+ 06",
            "channel_number": 8006,
            "channel_group_id": 2479,
            "hidden_from_output": False,
            "streams": [{"id": 110, "name": "ESPN PLUS 06:", "channel_group_id": 754}],
        }
    }
    profile = _profile([1558, 1557])
    primary_match = SimpleNamespace(
        name="NEXT | THE PAT MCAFEE SHOW | Mon 14 Sep 12:00 EDT | US: ESPN+ PPV 8",
        group_id=1558,
        stream_id=210,
    )
    secondary_match = SimpleNamespace(
        name="US (ESPN+ 8) | The Pat McAfee Show (2026-09-14 12:00:00)",
        group_id=1557,
        stream_id=220,
    )
    resolution = SimpleNamespace(resolved=[
        SimpleNamespace(
            disposition="would_attach",
            best=SimpleNamespace(master_name=guide_name),
            stream=secondary_match,
        ),
        SimpleNamespace(
            disposition="would_attach",
            best=SimpleNamespace(master_name=guide_name),
            stream=primary_match,
        ),
    ])
    coverage = {
        "sources": [{"status": "ready"}],
        "channels": [{"channel_id": 10, "current": current}],
    }
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.get_session", return_value=_session(profile)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([profile.to_dict()], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             [secondary_match, primary_match], {210: 1558, 220: 1557},
         ))), \
         patch("services.event_sync_resolver.resolve_event_sync", return_value=resolution), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=AsyncMock(return_value={110: False, 210: True, 220: True})), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    client.update_channel.assert_awaited_once_with(10, {"streams": [210, 220, 110]})
    assert result.details["stream_updated_channel_ids"] == [10]
    refresh_emby.assert_awaited_once_with()
