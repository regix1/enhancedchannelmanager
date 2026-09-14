"""Focused checks for the quick hidden-event visibility task."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from task_scheduler import ScheduleType
from tasks.event_visibility import (
    CHECK_INTERVAL_SECONDS,
    EventVisibilityTask,
    _current_xmltv_ids,
    _round_robin,
)


NOW = datetime(2026, 9, 14, 23, 0, tzinfo=timezone.utc)


def _guide(title: str = "Current event") -> str:
    return (
        "<tv><programme channel=\"ecm-10\" "
        "start=\"20260914220000 +0000\" stop=\"20260915010000 +0000\">"
        f"<title>{title}</title></programme></tv>"
    )


def _profile():
    return SimpleNamespace(to_dict=lambda: {
        "id": 1,
        "enabled": True,
        "hide_empty_group_ids": [65, 2479],
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


def test_current_xmltv_ids_excludes_placeholder_programmes():
    assert _current_xmltv_ids(_guide(), NOW) == {"ecm-10"}
    assert _current_xmltv_ids(_guide("Programming unavailable"), NOW) == set()


def test_round_robin_does_not_starve_later_channels():
    first, cursor = _round_robin([1, 2, 3, 4], 0, 2)
    second, cursor = _round_robin([1, 2, 3, 4], cursor, 2)

    assert first == [1, 2]
    assert second == [3, 4]
    assert cursor == 0


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
    cache = MagicMock()
    cache.get.return_value = _guide()
    flow = AsyncMock(return_value={110: True})
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.get_cache", return_value=cache), \
         patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("tasks.event_visibility._current_xmltv_ids", return_value={"ecm-10"}), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=flow), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success is True
    assert result.success_count == 1
    assert result.details == {"revealed_channel_ids": [10]}
    client.update_channel.assert_awaited_once_with(10, {"hidden_from_output": False})
    refresh_emby.assert_awaited_once_with()
    assert flow.await_args.kwargs["probe_missing"] is True


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
    cache = MagicMock()
    cache.get.return_value = _guide("Programming unavailable")
    flow = AsyncMock()
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.get_cache", return_value=cache), \
         patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=flow), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success is True
    assert result.total_items == 0
    flow.assert_not_awaited()
    client.update_channel.assert_not_awaited()
    refresh_emby.assert_not_awaited()


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
    cache = MagicMock()
    cache.get.return_value = _guide()
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.get_cache", return_value=cache), \
         patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("tasks.event_visibility._current_xmltv_ids", return_value={"ecm-10"}), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=AsyncMock(return_value={110: False})), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success is True
    assert result.success_count == 0
    assert result.skipped_count == 1
    client.update_channel.assert_not_awaited()
    refresh_emby.assert_not_awaited()
