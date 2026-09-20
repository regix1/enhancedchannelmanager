"""Focused checks for the quick hidden-event visibility task."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from task_scheduler import ScheduleType
from tasks.event_visibility import (
    CHECK_INTERVAL_SECONDS,
    EventVisibilityTask,
    _espn_slot,
    _guide_name,
    _link_dummy_epg,
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


@pytest.mark.parametrize("name, stream, expected", [
    ("ESPN+ 00", False, 0),
    ("ESPN+ 62", False, 62),
    ("ESPN PLUS 62:", True, 62),
    ("NCAAF 54: Murray State at Oklahoma State 7pm", True, None),
])
def test_espn_slot_uses_only_numbered_iptorrents_names(name, stream, expected):
    assert _espn_slot(name, stream=stream) == expected


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


def test_rtv_programme_matches_dateless_trex_event_name_today():
    from services.event_sync_resolver import SecondaryStream, resolve_event_sync

    guide_name = _guide_name({
        "title": "Murray State vs. Oklahoma State",
        "start": "2026-09-19T22:55:00+00:00",
    }, "US/Eastern")
    resolution = resolve_event_sync(
        {
            "master_group_id": 0,
            "secondary_group_ids": [1520],
            "time_window_minutes": 30,
            "enforce_time_window": True,
            "attach_threshold": 0.8,
            "assume_current_date": True,
        },
        [guide_name],
        [SecondaryStream(
            name="NCAAF 54: Murray State at Oklahoma State 7pm",
            group_id=1520,
            stream_id=2126837,
        )],
        now=datetime(2026, 9, 19, 23, tzinfo=timezone.utc),
    )

    assert resolution.resolved[0].disposition == "would_attach"


@pytest.mark.asyncio
async def test_reveals_current_hidden_channel_and_refreshes_emby():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10,
            "name": "PPV 10",
            "channel_number": 900,
            "channel_group_id": 65,
            "epg_data_id": 1,
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
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success is True
    assert result.success_count == 1
    assert result.details == {
        "revealed_channel_ids": [10],
        "hidden_channel_ids": [],
        "stream_updated_channel_ids": [],
        "epg_linked_channel_ids": [],
    }
    client.update_channel.assert_awaited_once_with(10, {"hidden_from_output": False})
    refresh_emby.assert_awaited_once_with()


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
            "epg_data_id": 1,
            "hidden_from_output": True,
            "streams": [{"id": 110, "name": "PPV stream"}],
        }
    }
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([_profile().to_dict()], _coverage(False)))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success is True
    assert result.total_items == 1
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
            "epg_data_id": 1,
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
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success_count == 1
    assert result.details == {
        "revealed_channel_ids": [],
        "hidden_channel_ids": [10],
        "stream_updated_channel_ids": [],
        "epg_linked_channel_ids": [],
    }
    client.update_channel.assert_awaited_once_with(
        10, {"hidden_from_output": True},
    )
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
    with patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([_profile().to_dict()], _coverage(False)))), \
         patch("services.epg_programmes.can_cache", return_value=False), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        result = await task.execute()

    assert result.message == "Published event guide is not ready"
    client.update_channel.assert_not_awaited()


@pytest.mark.asyncio
async def test_reveals_current_channel_without_probe():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10,
            "name": "PPV 10",
            "channel_number": 900,
            "channel_group_id": 65,
            "epg_data_id": 1,
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
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.success is True
    assert result.success_count == 1
    client.update_channel.assert_awaited_once_with(10, {"hidden_from_output": False})
    refresh_emby.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_places_guide_matches_in_group_priority_before_fallback():
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
            "epg_data_id": 1,
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
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    client.update_channel.assert_awaited_once_with(10, {"streams": [210, 220, 110]})
    assert result.details["stream_updated_channel_ids"] == [10]
    refresh_emby.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_places_numbered_iptorrents_slot_without_title_matching():
    task = EventVisibilityTask()
    client = AsyncMock()
    current = {
        "title": "Murray State vs. Oklahoma State",
        "start": "2026-09-19T22:55:00+00:00",
    }
    channels = {
        10: {
            "id": 10,
            "name": "ESPN+ 62",
            "channel_number": 8062,
            "channel_group_id": 2479,
            "epg_data_id": 1,
            "hidden_from_output": True,
            "streams": [],
        }
    }
    profile = _profile([754])
    ipt_slot = SimpleNamespace(
        name="ESPN PLUS 62:",
        group_id=754,
        stream_id=1679941,
    )
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
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([ipt_slot], {1679941: 754}))), \
         patch("services.event_sync_resolver.resolve_event_sync") as resolve, \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    client.update_channel.assert_awaited_once_with(
        10,
        {"streams": [1679941], "hidden_from_output": False},
    )
    assert result.details["stream_updated_channel_ids"] == [10]
    resolve.assert_not_called()
    refresh_emby.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_orders_titled_iptorrents_then_trex_then_numbered_slot():
    task = EventVisibilityTask()
    client = AsyncMock()
    current = {
        "title": "Murray State vs. Oklahoma State",
        "start": "2026-09-19T22:55:00+00:00",
    }
    guide_name = _guide_name(current, "US/Eastern")
    channels = {
        10: {
            "id": 10,
            "name": "ESPN+ 62",
            "channel_number": 8062,
            "channel_group_id": 2479,
            "epg_data_id": 1,
            "hidden_from_output": False,
            "streams": [{
                "id": 1679941,
                "name": "ESPN PLUS 62:",
                "channel_group_id": 754,
            }],
        }
    }
    profile = _profile([754, 1558, 1557, 1520])
    ipt_title = SimpleNamespace(
        name="US (ESPN+ 404) | Murray State at Oklahoma State (2026-09-19 19:00:00)",
        group_id=754,
        stream_id=220,
    )
    trex_title = SimpleNamespace(
        name="NCAAF 54: Murray State at Oklahoma State 7pm",
        group_id=1520,
        stream_id=210,
    )
    ipt_slot = SimpleNamespace(
        name="ESPN PLUS 62:",
        group_id=754,
        stream_id=1679941,
    )
    resolution = SimpleNamespace(resolved=[
        SimpleNamespace(
            disposition="would_attach",
            best=SimpleNamespace(master_name=guide_name),
            stream=trex_title,
        ),
        SimpleNamespace(
            disposition="would_attach",
            best=SimpleNamespace(master_name=guide_name),
            stream=ipt_title,
        ),
    ])
    coverage = {
        "sources": [{"status": "ready"}],
        "channels": [{"channel_id": 10, "current": current}],
    }

    with patch("tasks.event_visibility.get_session", return_value=_session(profile)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([profile.to_dict()], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             [trex_title, ipt_slot, ipt_title],
             {210: 1520, 220: 754, 1679941: 754},
         ))), \
         patch("services.event_sync_resolver.resolve_event_sync", return_value=resolution) as resolve, \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        await task.execute()

    client.update_channel.assert_awaited_once_with(
        10,
        {"streams": [220, 210, 1679941]},
    )
    passed_streams = resolve.call_args.args[2]
    assert [stream.stream_id for stream in passed_streams] == [210, 220]


@pytest.mark.asyncio
async def test_ended_channel_keeps_numbered_iptorrents_fallback():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10,
            "name": "ESPN+ 62",
            "channel_number": 8062,
            "channel_group_id": 2479,
            "epg_data_id": 1,
            "hidden_from_output": False,
            "streams": [
                {"id": 220, "name": "US (ESPN+ 404) | Finished Event", "channel_group_id": 754},
                {"id": 210, "name": "NCAAF 54: Finished Event", "channel_group_id": 1520},
                {"id": 1679941, "name": "ESPN PLUS 62:", "channel_group_id": 754},
            ],
        }
    }
    profile = _profile([754, 1558, 1557, 1520])
    match_streams = [
        SimpleNamespace(name=stream["name"], group_id=stream["channel_group_id"], stream_id=stream["id"])
        for stream in channels[10]["streams"]
    ]

    with patch("tasks.event_visibility.get_session", return_value=_session(profile)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([profile.to_dict()], _coverage(False)))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             match_streams,
             {220: 754, 210: 1520, 1679941: 754},
         ))), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        await task.execute()

    client.update_channel.assert_awaited_once_with(
        10,
        {"hidden_from_output": True, "streams": [1679941]},
    )


@pytest.mark.asyncio
async def test_processes_every_current_channel_in_one_run():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        channel_id: {
            "id": channel_id,
            "name": f"ESPN+ {channel_id}",
            "channel_number": 8000 + channel_id,
            "channel_group_id": 2479,
            "epg_data_id": 1,
            "hidden_from_output": True,
            "streams": [1000 + channel_id],
        }
        for channel_id in range(1, 21)
    }
    coverage = {
        "sources": [{"status": "ready"}],
        "channels": [
            {"channel_id": channel_id, "current": {"title": f"Event {channel_id}"}}
            for channel_id in channels
        ],
    }

    with patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([_profile().to_dict()], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        result = await task.execute()

    assert result.total_items == 20
    assert result.details["revealed_channel_ids"] == list(range(1, 21))
    assert client.update_channel.await_count == 20


@pytest.mark.asyncio
async def test_links_generated_epg_rows_and_returns_sources_to_refresh():
    client = AsyncMock()
    client.get_epg_sources.return_value = [{
        "id": 46,
        "name": "ECM Dummy EPG",
        "url": "http://ecm/api/dummy-epg/xmltv/1",
        "is_active": True,
    }]
    client.get_epg_data.return_value = [{
        "id": 501,
        "tvg_id": "ecm-10",
        "epg_source": 46,
    }]
    channels = [(10, {"id": 10, "epg_data_id": None})]

    linked, sources = await _link_dummy_epg(client, channels)

    assert linked == [10]
    assert sources == {46: "ECM Dummy EPG"}
    assert channels[0][1]["epg_data_id"] == 501
    client.update_channel.assert_awaited_once_with(10, {"epg_data_id": 501})
    client.get_epg_data.assert_awaited_once_with(
        epg_source=46,
        max_results=10000,
    )


@pytest.mark.asyncio
async def test_new_epg_link_reimports_programmes_before_emby_refresh():
    task = EventVisibilityTask()
    client = AsyncMock()
    client.get_epg_sources.return_value = [{
        "id": 46,
        "name": "ECM Dummy EPG",
        "url": "http://ecm/api/dummy-epg/xmltv/1",
        "is_active": True,
    }]
    client.get_epg_data.return_value = [{
        "id": 501,
        "tvg_id": "ecm-10",
        "epg_source": 46,
    }]
    channels = {
        10: {
            "id": 10,
            "name": "ESPN+ 62",
            "channel_number": 8062,
            "channel_group_id": 2479,
            "epg_data_id": None,
            "hidden_from_output": True,
            "streams": [110],
        }
    }
    wait_refresh = AsyncMock(return_value=True)
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.get_session", return_value=_session()), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([_profile().to_dict()], _coverage()))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", new=wait_refresh), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    assert result.details["epg_linked_channel_ids"] == [10]
    assert client.update_channel.await_args_list == [
        ((10, {"epg_data_id": 501}),),
        ((10, {"hidden_from_output": False}),),
    ]
    wait_refresh.assert_awaited_once_with(
        client,
        46,
        "ECM Dummy EPG",
        cancelled=wait_refresh.await_args.kwargs["cancelled"],
    )
    refresh_emby.assert_awaited_once_with()
