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
    _slot_key,
    _ufc_slot,
)


def _coverage(current: bool = True):
    return {
        "sources": [{"status": "ready"}],
        "channels": [{
            "channel_id": 10,
            "current": {"title": "Current event"} if current else None,
        }],
    }


def _profile(stream_match_group_ids=None, pattern_variants=None, **overrides):
    values = {
        "id": 1,
        "enabled": True,
        "hide_empty_group_ids": [16, 65, 2479],
        "stream_match_group_ids": stream_match_group_ids or [],
        "pattern_variants": pattern_variants or [],
        "event_timezone": "US/Eastern",
        "program_duration": 180,
        "tvg_id_template": "ecm-{channel_id}",
    }
    values.update(overrides)
    return SimpleNamespace(to_dict=lambda: dict(values))


def _session(profile=None, profiles=None):
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = (
        profiles if profiles is not None else [profile or _profile()]
    )
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


@pytest.mark.parametrize("name, stream, expected", [
    ("UFC01", False, 1),
    ("UFC 09", False, 9),
    ("UFC 02:", True, 2),
    ("UFC INT09", True, 9),
    ("UFC 02 : CRYPTO.COM UFC 331", True, None),
])
def test_ufc_slot_uses_only_numbered_names(name, stream, expected):
    assert _ufc_slot(name, stream=stream) == expected


def test_slot_key_keeps_channel_families_separate():
    assert _slot_key("ESPN+ 01") == ("espn", 1)
    assert _slot_key("UFC01") == ("ufc", 1)


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


def test_ufc_guide_matches_titled_iptorrents_event():
    from services.event_sync_matcher import DEFAULT_EVENT_PATTERNS
    from services.event_sync_resolver import SecondaryStream, resolve_event_sync

    guide_name = _guide_name({
        "title": "UFC 331: Van vs. Pantoja 2",
        "start": "2026-09-20T01:00:00+00:00",
    }, "US/Eastern")
    resolution = resolve_event_sync(
        {
            "master_group_id": 0,
            "secondary_group_ids": [2462],
            "time_window_minutes": 30,
            "enforce_time_window": True,
            "attach_threshold": 0.8,
            "assume_current_date": True,
            "patterns": list(DEFAULT_EVENT_PATTERNS),
        },
        [guide_name],
        [SecondaryStream(
            name="LIVE EVENT 02   9pm UFC 331 Van v Pantoja 2",
            group_id=2462,
            stream_id=2134594,
        )],
        now=datetime(2026, 9, 20, 1, 30, tzinfo=timezone.utc),
    )

    assert resolution.resolved[0].disposition == "would_attach"


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse", [False, True])
async def test_keeps_numbered_slots_in_their_match_groups(reverse):
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10,
            "name": "ESPN+ 06",
            "channel_number": 8006,
            "channel_group_id": 65,
            "epg_data_id": 1,
            "hidden_from_output": False,
            "streams": [{"id": 9010, "name": "Fallback A", "channel_group_id": 900}],
        },
        20: {
            "id": 20,
            "name": "ESPN+ 06",
            "channel_number": 9006,
            "channel_group_id": 2479,
            "epg_data_id": 2,
            "hidden_from_output": False,
            "streams": [{"id": 9020, "name": "Fallback B", "channel_group_id": 900}],
        },
    }
    first = _profile(
        [101, 303], id=1, hide_empty_group_ids=[65], channel_group_ids=[65],
    )
    second = _profile(
        [202, 303], id=2, hide_empty_group_ids=[2479], channel_group_ids=[2479],
    )
    profiles = [second, first] if reverse else [first, second]
    streams = [
        SimpleNamespace(name="ESPN PLUS 06:", group_id=101, stream_id=1101),
        SimpleNamespace(name="ESPN PLUS 06:", group_id=202, stream_id=2202),
        SimpleNamespace(name="ESPN PLUS 06:", group_id=303, stream_id=3303),
    ]
    coverage = {
        "sources": [{"status": "ready"}],
        "channels": [
            {"channel_id": 10, "current": {"title": "Event A"}},
            {"channel_id": 20, "current": {"title": "Event B"}},
        ],
    }

    with patch("tasks.event_visibility.get_session", return_value=_session(profiles=profiles)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([row.to_dict() for row in profiles], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             streams, {1101: 101, 2202: 202, 3303: 303},
         ))), \
         patch("services.event_sync_resolver.resolve_event_sync"), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        await task.execute()

    assert client.update_channel.await_args_list == [
        ((10, {"streams": [9010, 1101, 3303]}),),
        ((20, {"streams": [9020, 2202, 3303]}),),
    ]


@pytest.mark.asyncio
async def test_resolves_guide_names_within_each_target_group():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10, "name": "PPV 10", "channel_number": 10,
            "channel_group_id": 65, "epg_data_id": 1,
            "hidden_from_output": False,
            "streams": [{"id": 9010, "name": "Fallback A", "channel_group_id": 900}],
        },
        20: {
            "id": 20, "name": "ESPN event", "channel_number": 20,
            "channel_group_id": 2479, "epg_data_id": 2,
            "hidden_from_output": False,
            "streams": [{"id": 9020, "name": "Fallback B", "channel_group_id": 900}],
        },
    }
    first = _profile(
        [102, 101], id=1, hide_empty_group_ids=[65], channel_group_ids=[65],
        event_timezone="US/Pacific",
    )
    second = _profile(
        [202, 201], id=2, hide_empty_group_ids=[2479], channel_group_ids=[2479],
        event_timezone="US/Eastern",
    )
    profiles = [first, second]
    currents = {
        10: {"title": "Target A", "start": "2026-09-20T01:00:00+00:00"},
        20: {"title": "Target B", "start": "2026-09-20T01:00:00+00:00"},
    }
    coverage = {
        "sources": [{"status": "ready"}],
        "channels": [
            {"channel_id": channel_id, "current": current}
            for channel_id, current in currents.items()
        ],
    }
    streams = [
        SimpleNamespace(name="Target B @ Sep 19 09:00 PM", group_id=201, stream_id=2101),
        SimpleNamespace(name="Target A @ Sep 19 06:00 PM", group_id=101, stream_id=1101),
        SimpleNamespace(name="Target B @ Sep 19 09:00 PM", group_id=202, stream_id=2202),
        SimpleNamespace(name="Target A @ Sep 19 06:00 PM", group_id=102, stream_id=1202),
    ]
    calls = []

    def resolve(config, names, candidates, *, now):
        calls.append((config, names, candidates, now))
        return SimpleNamespace(resolved=[
            SimpleNamespace(
                disposition="would_attach",
                best=SimpleNamespace(master_name=names[0]),
                stream=stream,
            )
            for stream in candidates
        ])

    with patch("tasks.event_visibility.get_session", return_value=_session(profiles=profiles)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([row.to_dict() for row in profiles], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             streams, {stream.stream_id: stream.group_id for stream in streams},
         ))), \
         patch("services.event_sync_resolver.resolve_event_sync", side_effect=resolve), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        await task.execute()

    assert len(calls) == 2
    assert calls[0][0]["secondary_group_ids"] == [102, 101]
    assert calls[1][0]["secondary_group_ids"] == [202, 201]
    assert calls[0][1] == ["Target A @ Sep 19 6:00 PM"]
    assert calls[1][1] == ["Target B @ Sep 19 9:00 PM"]
    assert [stream.stream_id for stream in calls[0][2]] == [1101, 1202]
    assert [stream.stream_id for stream in calls[1][2]] == [2101, 2202]
    assert client.update_channel.await_args_list == [
        ((10, {"streams": [1202, 1101, 9010]}),),
        ((20, {"streams": [2202, 2101, 9020]}),),
    ]


@pytest.mark.asyncio
async def test_keeps_profile_patterns_in_its_target_groups():
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {"id": 10, "name": "PPV 10", "channel_number": 10, "channel_group_id": 65,
             "epg_data_id": 1, "hidden_from_output": False, "streams": []},
        20: {"id": 20, "name": "ESPN event", "channel_number": 20, "channel_group_id": 2479,
             "epg_data_id": 2, "hidden_from_output": False, "streams": []},
    }
    first_pattern = {"name": "first-format", "title_pattern": r"^FIRST (?P<title>.+)$"}
    second_pattern = {"name": "second-format", "title_pattern": r"^SECOND (?P<title>.+)$"}
    profiles = [
        _profile([101], [first_pattern], id=1, hide_empty_group_ids=[65], event_timezone="US/Pacific"),
        _profile([202], [second_pattern], id=2, hide_empty_group_ids=[2479], event_timezone="US/Eastern"),
    ]
    coverage = {
        "sources": [{"status": "ready"}],
        "channels": [
            {"channel_id": 10, "current": {"title": "A", "start": "2026-09-20T01:00:00+00:00"}},
            {"channel_id": 20, "current": {"title": "B", "start": "2026-09-20T01:00:00+00:00"}},
        ],
    }
    streams = [
        SimpleNamespace(name="FIRST A", group_id=101, stream_id=1101),
        SimpleNamespace(name="SECOND B", group_id=202, stream_id=2202),
    ]
    configs = []

    def resolve(config, names, candidates, *, now):
        configs.append(config)
        return SimpleNamespace(resolved=[])

    with patch("tasks.event_visibility.get_session", return_value=_session(profiles=profiles)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([row.to_dict() for row in profiles], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             streams, {1101: 101, 2202: 202},
         ))), \
         patch("services.event_sync_resolver.resolve_event_sync", side_effect=resolve), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        await task.execute()

    assert len(configs) == 2
    assert first_pattern in configs[0]["patterns"]
    assert second_pattern not in configs[0]["patterns"]
    assert second_pattern in configs[1]["patterns"]
    assert first_pattern not in configs[1]["patterns"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_site", ["scan", "resolve"])
async def test_keeps_streams_when_profile_matching_fails(failure_site):
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10, "name": "PPV 10", "channel_number": 10,
            "channel_group_id": 65, "epg_data_id": 1,
            "hidden_from_output": True,
            "streams": [{"id": 9010, "name": "Fallback", "channel_group_id": 900}],
        },
    }
    profile = _profile([101], hide_empty_group_ids=[65], channel_group_ids=[65])
    coverage = {
        "sources": [{"status": "ready"}],
        "channels": [{
            "channel_id": 10,
            "current": {"title": "Target A", "start": "2026-09-20T01:00:00+00:00"},
        }],
    }
    stream = SimpleNamespace(name="Target A @ Sep 19 09:00 PM", group_id=101, stream_id=1101)
    fetched = AsyncMock(return_value=([stream], {1101: 101}))
    resolve_error = None
    if failure_site == "scan":
        fetched.side_effect = RuntimeError("scan failed")
    else:
        resolve_error = RuntimeError("resolve failed")

    with patch("tasks.event_visibility.get_session", return_value=_session(profile)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([profile.to_dict()], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=fetched), \
         patch("services.event_sync_resolver.resolve_event_sync", side_effect=resolve_error), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        await task.execute()

    client.update_channel.assert_awaited_once_with(10, {"hidden_from_output": False})


@pytest.mark.asyncio
@pytest.mark.parametrize("title, event_timezone, now", [
    (
        "LIVE EVENT 02   9pm UFC 331 Van v Pantoja 2",
        "US/Eastern",
        datetime(2026, 9, 20, 1, 30, tzinfo=timezone.utc),
    ),
    (
        "UFC 02 : CRYPTO.COM UFC 331: PRELIMS start:2026 09 20 01:00:00 stop:2026 09 20 04:00:00",
        "UTC",
        datetime(2026, 9, 20, 1, 30, tzinfo=timezone.utc),
    ),
])
async def test_bootstraps_active_ufc_titled_slot_before_numbered_fallback(
    title, event_timezone, now,
):
    task = EventVisibilityTask()
    client = AsyncMock()
    client.get_epg_sources.return_value = [{
        "id": 46,
        "name": "ECM Dummy EPG",
        "url": "http://ecm/api/dummy-epg/xmltv/2",
        "is_active": True,
    }]
    channels = {
        10: {
            "id": 10, "name": "UFC02", "channel_number": 8102,
            "channel_group_id": 16, "epg_data_id": 1,
            "hidden_from_output": True,
            "streams": [{"id": 1868499, "name": "UFC 02", "channel_group_id": 2462}],
        },
    }
    profile = _profile(
        [2462], id=2, hide_empty_group_ids=[16], channel_group_ids=[16],
        event_timezone=event_timezone, program_duration=180,
    )
    titled = SimpleNamespace(name=title, group_id=2462, stream_id=2134594)
    fallback = SimpleNamespace(name="UFC 02", group_id=2462, stream_id=1868499)
    coverage = {
        "sources": [{"status": "ready"}],
        "channels": [{"channel_id": 10, "current": None}],
    }
    clock = MagicMock(wraps=datetime)
    clock.now.return_value = now
    refresh_emby = AsyncMock()
    wait_refresh = AsyncMock(return_value=True)
    guide_cache = MagicMock()

    with patch("tasks.event_visibility.datetime", clock), \
         patch("tasks.event_visibility.get_session", return_value=_session(profile)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([profile.to_dict()], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             [fallback, titled], {1868499: 2462, 2134594: 2462},
         ))), \
         patch("services.event_sync_resolver.resolve_event_sync") as resolve, \
         patch("cache.get_cache", return_value=guide_cache), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", new=wait_refresh), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        result = await task.execute()

    client.update_channel.assert_awaited_once_with(
        10, {"streams": [2134594, 1868499], "hidden_from_output": False},
    )
    assert result.details["stream_updated_channel_ids"] == [10]
    resolve.assert_not_called()
    guide_cache.invalidate_prefix.assert_called_once_with("dummy_epg_xmltv")
    wait_refresh.assert_awaited_once_with(
        client,
        46,
        "ECM Dummy EPG",
        cancelled=wait_refresh.await_args.kwargs["cancelled"],
    )
    refresh_emby.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "hidden, update_fails, attempts, source_completes, refreshes_emby",
    [
        pytest.param(True, False, True, True, True, id="visibility-only"),
        pytest.param(False, False, True, True, True, id="idempotent-channel"),
        pytest.param(True, True, False, True, False, id="failed-channel-update"),
        pytest.param(False, False, True, False, False, id="source-incomplete"),
    ],
)
async def test_retries_active_ufc_guide_publication(
    hidden, update_fails, attempts, source_completes, refreshes_emby,
):
    task = EventVisibilityTask()
    client = AsyncMock()
    client.get_epg_sources.return_value = [{
        "id": 46,
        "name": "ECM Dummy EPG",
        "url": "http://ecm/api/dummy-epg/xmltv/2",
        "is_active": True,
    }]
    title = "LIVE EVENT 02   9pm UFC 331 Van v Pantoja 2"
    channels = {
        10: {
            "id": 10, "name": "UFC02", "channel_number": 8102,
            "channel_group_id": 16, "epg_data_id": 1,
            "hidden_from_output": hidden,
            "streams": [
                {"id": 2134594, "name": title, "channel_group_id": 2462},
                {"id": 1868499, "name": "UFC 02", "channel_group_id": 2462},
            ],
        },
    }
    profile = _profile(
        [2462], id=2, hide_empty_group_ids=[16], channel_group_ids=[16],
        event_timezone="US/Eastern", program_duration=180,
    )
    titled = SimpleNamespace(name=title, group_id=2462, stream_id=2134594)
    fallback = SimpleNamespace(name="UFC 02", group_id=2462, stream_id=1868499)
    coverage = {
        "sources": [{"status": "ready"}],
        "channels": [{"channel_id": 10, "current": None}],
    }
    if update_fails:
        client.update_channel.side_effect = RuntimeError("update failed")
    clock = MagicMock(wraps=datetime)
    clock.now.return_value = datetime(2026, 9, 20, 1, 30, tzinfo=timezone.utc)
    guide_cache = MagicMock()
    wait_refresh = AsyncMock(return_value=source_completes)
    refresh_emby = AsyncMock()

    with patch("tasks.event_visibility.datetime", clock), \
         patch("tasks.event_visibility.get_session", return_value=_session(profile)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([profile.to_dict()], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             [fallback, titled], {1868499: 2462, 2134594: 2462},
         ))), \
         patch("cache.get_cache", return_value=guide_cache), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", new=wait_refresh), \
         patch("emby_client.request_guide_refresh", new=refresh_emby):
        await task.execute()

    if hidden:
        client.update_channel.assert_awaited_once_with(
            10, {"hidden_from_output": False},
        )
    else:
        client.update_channel.assert_not_awaited()
    if attempts:
        guide_cache.invalidate_prefix.assert_called_once_with("dummy_epg_xmltv")
        wait_refresh.assert_awaited_once_with(
            client,
            46,
            "ECM Dummy EPG",
            cancelled=wait_refresh.await_args.kwargs["cancelled"],
        )
    else:
        guide_cache.invalidate_prefix.assert_not_called()
        wait_refresh.assert_not_awaited()
    if refreshes_emby:
        refresh_emby.assert_awaited_once_with()
    else:
        refresh_emby.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("now", [
    datetime(2026, 9, 20, 0, 59, tzinfo=timezone.utc),
    datetime(2026, 9, 20, 4, 0, tzinfo=timezone.utc),
])
async def test_keeps_ufc_titled_slot_outside_profile_window(now):
    task = EventVisibilityTask()
    client = AsyncMock()
    title = (
        "UFC 02 : CRYPTO.COM UFC 331: PRELIMS "
        "start:2026 09 20 01:00:00 stop:2026 09 20 04:00:00"
    )
    channels = {
        10: {
            "id": 10, "name": "UFC02", "channel_number": 8102,
            "channel_group_id": 16, "epg_data_id": 1,
            "hidden_from_output": False,
            "streams": [
                {"id": 2134594, "name": title, "channel_group_id": 2462},
                {"id": 1868499, "name": "UFC 02", "channel_group_id": 2462},
            ],
        },
    }
    profile = _profile(
        [2462], id=2, hide_empty_group_ids=[16], channel_group_ids=[16],
        event_timezone="UTC", program_duration=180,
    )
    streams = [
        SimpleNamespace(name=title, group_id=2462, stream_id=2134594),
        SimpleNamespace(name="UFC 02", group_id=2462, stream_id=1868499),
    ]
    coverage = {"sources": [{"status": "ready"}], "channels": [{"channel_id": 10, "current": None}]}
    clock = MagicMock(wraps=datetime)
    clock.now.return_value = now

    with patch("tasks.event_visibility.datetime", clock), \
         patch("tasks.event_visibility.get_session", return_value=_session(profile)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([profile.to_dict()], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             streams, {2134594: 2462, 1868499: 2462},
         ))), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        await task.execute()

    client.update_channel.assert_awaited_once_with(
        10, {"hidden_from_output": True, "streams": [1868499]},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("name, group_id", [
    ("LIVE EVENT 02   9pm UFC 331 Van v Pantoja 2", 9999),
    ("9pm UFC 331 Van v Pantoja 2", 2462),
    pytest.param(
        "LIVE EVENT 02   9pm Boxing Championship",
        2462,
        id="non-ufc-live-event",
    ),
])
async def test_rejects_ufc_bootstrap_without_owned_slot(name, group_id):
    task = EventVisibilityTask()
    client = AsyncMock()
    channels = {
        10: {
            "id": 10, "name": "UFC02", "channel_number": 8102,
            "channel_group_id": 16, "epg_data_id": 1,
            "hidden_from_output": False,
            "streams": [{"id": 1868499, "name": "UFC 02", "channel_group_id": 2462}],
        },
    }
    profile = _profile([2462], id=2, hide_empty_group_ids=[16], channel_group_ids=[16])
    stream = SimpleNamespace(name=name, group_id=group_id, stream_id=2134594)
    fallback = SimpleNamespace(name="UFC 02", group_id=2462, stream_id=1868499)
    coverage = {"sources": [{"status": "ready"}], "channels": [{"channel_id": 10, "current": None}]}
    clock = MagicMock(wraps=datetime)
    clock.now.return_value = datetime(2026, 9, 20, 1, 30, tzinfo=timezone.utc)

    with patch("tasks.event_visibility.datetime", clock), \
         patch("tasks.event_visibility.get_session", return_value=_session(profile)), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([profile.to_dict()], coverage))), \
         patch("services.epg_programmes.can_cache", return_value=True), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             [fallback, stream], {1868499: 2462, 2134594: group_id},
         ))), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        await task.execute()

    client.update_channel.assert_awaited_once_with(10, {"hidden_from_output": True})


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
async def test_orders_titled_ufc_stream_before_numbered_slot():
    task = EventVisibilityTask()
    client = AsyncMock()
    current = {
        "title": "Crypto.com UFC 331: Prelims",
        "start": "2026-09-20T00:55:00+00:00",
    }
    guide_name = _guide_name(current, "US/Eastern")
    channels = {
        10: {
            "id": 10,
            "name": "UFC02",
            "channel_number": 8102,
            "channel_group_id": 16,
            "epg_data_id": 1,
            "hidden_from_output": False,
            "streams": [{
                "id": 1868499,
                "name": "UFC 02",
                "channel_group_id": 2462,
            }],
        }
    }
    ufc_pattern = {
        "name": "ufc-parenthesized-date",
        "title_pattern": (
            r"^US\s+\(UFC(?:\s+INT)?\s*\d+\)\s*\|\s*(?P<title>.+?)\s*"
            r"\((?P<year>\d{4})\s+(?P<month>\d{2})\s+(?P<day>\d{2})\s+"
            r"(?P<hour>\d{2}):(?P<minute>\d{2}):[0-5]\d\)\s*$"
        ),
    }
    profile = _profile([2462], [ufc_pattern])
    titled = SimpleNamespace(
        name=(
            "UFC 02 : CRYPTO.COM UFC 331: PRELIMS "
            "start:2026 09 20 00:55:00 stop:2026 09 20 04:00:00"
        ),
        group_id=2462,
        stream_id=2087027,
    )
    fallback = SimpleNamespace(
        name="UFC 02",
        group_id=2462,
        stream_id=1868499,
    )
    resolution = SimpleNamespace(resolved=[SimpleNamespace(
        disposition="would_attach",
        best=SimpleNamespace(master_name=guide_name),
        stream=titled,
    )])
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
             [fallback, titled], {1868499: 2462, 2087027: 2462},
         ))), \
         patch("services.event_sync_resolver.resolve_event_sync", return_value=resolution) as resolve, \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        await task.execute()

    client.update_channel.assert_awaited_once_with(
        10,
        {"streams": [2087027, 1868499]},
    )
    assert resolve.call_args.args[0]["patterns"][-1] == ufc_pattern


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
async def test_links_generated_epg_row_when_dispatcharr_omits_empty_link():
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
    channels = [(10, {"id": 10})]

    linked, sources = await _link_dummy_epg(client, channels)

    assert linked == [10]
    assert sources == {46: "ECM Dummy EPG"}
    client.update_channel.assert_awaited_once_with(10, {"epg_data_id": 501})


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
