"""Immutable-base control for retained guide reconciliation."""
import asyncio
import copy
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.orm import sessionmaker

import database


@pytest.fixture(autouse=True)
async def isolated_guide_state(monkeypatch, test_engine, tmp_path):
    from cache import get_cache
    from services import epg_programmes as guides

    sessions = sessionmaker(
        autocommit=False, autoflush=False, bind=test_engine, expire_on_commit=False,
    )
    monkeypatch.setattr(database, "_SessionLocal", sessions)
    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    guides._SOURCE_CACHE.clear()
    guides._SOURCE_LOADS.clear()
    guides._CATALOGUE_CACHE.clear()
    guides._CATALOGUE_LOADS.clear()
    get_cache().clear()
    yield
    tasks = [*guides._SOURCE_LOADS.values(), *guides._CATALOGUE_LOADS.values()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    guides._SOURCE_LOADS.clear()
    guides._SOURCE_CACHE.clear()
    guides._CATALOGUE_LOADS.clear()
    guides._CATALOGUE_CACHE.clear()
    get_cache().clear()


def _config():
    return {
        "secondary": [],
        "time_window_minutes": 30,
        "enforce_time_window": True,
        "attach_threshold": 0.8,
        "assume_current_date": True,
        "demote_stale_dateless": True,
        "use_default_patterns": True,
        "slot_patterns": [],
    }


def _profile(profile_id, group_id, source_ids):
    values = {
        "id": profile_id,
        "name": f"Guide {profile_id}",
        "enabled": True,
        "channel_group_ids": [group_id],
        "hide_empty_group_ids": [group_id],
        "stream_match_group_ids": [],
        "event_sync_config": _config(),
        "event_timezone": "UTC",
        "output_timezone": "UTC",
        "program_duration": 180,
        "pattern_variants": [],
        "epg_source_ids": source_ids,
        "tvg_id_template": "event-{channel_id}",
        "channel_assignments": [{
            "channel_id": profile_id * 10,
            "channel_name": f"Arena {profile_id}",
        }],
    }
    return values


def _stored(scope, document, *, channels=None):
    return {
        "scope": scope,
        "xmltv": document,
        "revision": 1,
        "state": {
            "published_at": "2026-09-19T10:00:00+00:00",
            "xmltv_hash": "b" * 64,
            "channels": list(channels or []),
            "delivery": {
                "required_dispatcharr_hashes": {},
                "confirmed_dispatcharr_hashes": {},
                "pending_emby": True,
            },
        },
    }


@pytest.mark.asyncio
async def test_retained_placeholder_continues_safe_profile_work():
    """The immutable base fails this fixed-behavior assertion at its early return."""
    from services import epg_publication
    from tasks import dummy_epg_refresh
    from tasks import event_visibility

    retained_xml = "<tv><channel id='event-10'/></tv>"
    profiles = [
        _profile(1, 7, [100]),
        _profile(2, 8, []),
    ]
    rows = [SimpleNamespace(to_dict=lambda value=value: dict(value), enabled=True)
            for value in profiles]
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = rows
    channels = {
        10: {
            "id": 10,
            "name": "Arena 1",
            "channel_group_id": 7,
            "hidden_from_output": False,
            "epg_data_id": 500,
            "streams": [],
        },
        20: {
            "id": 20,
            "name": "Arena 2",
            "channel_group_id": 8,
            "hidden_from_output": False,
            "epg_data_id": None,
            "streams": [],
        },
    }
    coverage = {
        "sources": [{
            "source_id": 100,
            "status": "error",
            "last_success": None,
        }],
        "profiles": {
            "1": {"profile_id": 1, "can_publish": False, "reason_codes": ["GUIDE_SOURCES_PENDING"]},
            "2": {"profile_id": 2, "can_publish": True, "reason_codes": []},
        },
        "channels": [
            {"profile_id": 1, "channel_id": 10, "current": None},
            {"profile_id": 2, "channel_id": 20, "current": None},
        ],
    }
    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[])
    client.update_channel = AsyncMock()
    cache = MagicMock()
    emby = AsyncMock(return_value=True)

    with ExitStack() as stack:
        stack.enter_context(patch("database.get_session", return_value=session))
        stack.enter_context(patch("tasks.dummy_epg_refresh.get_client", return_value=client))
        stack.enter_context(patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)))
        stack.enter_context(patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=(copy.deepcopy(profiles), coverage))))
        stack.enter_context(patch("cache.get_cache", return_value=cache))
        stack.enter_context(patch("emby_client.request_guide_refresh", new=emby))

        if hasattr(event_visibility, "reconcile_profiles"):
            result = epg_publication.PublicationResult(
                published_profile_ids=(2,),
                retained_profile_ids=(1,),
                xmltv_by_scope={
                    "all": "<tv><channel id='event-10'/><channel id='event-20'/></tv>",
                    "profile:1": retained_xml,
                    "profile:2": "<tv><channel id='event-20'/></tv>",
                },
                reason_codes=("GUIDE_SOURCES_PENDING",),
            )
            publications = {
                "all": _stored("all", result.xmltv_by_scope["all"]),
                "profile:1": _stored("profile:1", retained_xml, channels=[{
                    "channel_id": 10,
                    "events": [{
                        "start": "2020-01-01T00:00:00+00:00",
                        "stop": "2030-01-01T00:00:00+00:00",
                    }],
                }]),
                "profile:2": _stored("profile:2", result.xmltv_by_scope["profile:2"]),
            }

            def read(scope):
                return publications.get(scope)

            def update(scope, *, expected_revision, required_dispatcharr_hashes=None,
                       confirmed_dispatcharr_hashes=None, pending_emby=None):
                row = publications[scope]
                if row["revision"] != expected_revision:
                    return None
                if required_dispatcharr_hashes is not None:
                    row["state"]["delivery"]["required_dispatcharr_hashes"] = dict(required_dispatcharr_hashes)
                if confirmed_dispatcharr_hashes is not None:
                    row["state"]["delivery"]["confirmed_dispatcharr_hashes"] = dict(confirmed_dispatcharr_hashes)
                if pending_emby is not None:
                    row["state"]["delivery"]["pending_emby"] = pending_emby
                row["revision"] += 1
                return row["revision"]

            stack.enter_context(patch("tasks.event_visibility._load_profiles", side_effect=[
                (copy.deepcopy(profiles), []), (copy.deepcopy(profiles), []),
            ]))
            stack.enter_context(patch("tasks.event_visibility.get_client", return_value=client))
            stack.enter_context(patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}))))
            stack.enter_context(patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=result)))
            stack.enter_context(patch("services.epg_publication.read_publication", side_effect=read))
            stack.enter_context(patch("services.epg_publication.update_delivery", side_effect=update))

        outcome = await dummy_epg_refresh.DummyEPGRefreshTask().execute()

    assert outcome.success is False
    assert outcome.completed_degraded is True
    assert outcome.details["retained_profile_ids"] == [1]
    assert outcome.details["published_profile_ids"] == [2]
    assert outcome.details["hidden_channel_ids"] == [20]
    assert client.update_channel.await_args_list == [
        ((20, {"hidden_from_output": True}),),
    ]
    cache.set.assert_any_call("dummy_epg_xmltv_1", retained_xml)
    emby.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_visibility_task_waits_for_source_and_reveals_channel(monkeypatch):
    from services import epg_programmes as guides
    from services.epg_publication import publish_profiles, read_publication
    from tasks import event_visibility

    now = datetime.now(timezone.utc).replace(microsecond=0)
    active_start = now - timedelta(minutes=10)
    active_stop = now + timedelta(minutes=80)
    profile_one = _profile(1, 7, [100])
    profile_one["channel_mappings"] = [
        {"channel_id": 10, "source_id": 100, "tvg_id": "external-10"},
        {"channel_id": 11, "source_id": 100, "tvg_id": "external-11"},
    ]
    profile_one["event_sync_config"] = {
        **_config(),
        "secondary": [
            {"group_id": 9, "m3u_account_id": None},
            {"group_id": 10, "m3u_account_id": None},
        ],
        "slot_patterns": [{
            "name": "Arena",
            "channel_pattern": r"Arena (?P<slot>\d+)",
            "fallback_pattern": r"Backup (?P<slot>\d+)",
            "event_patterns": [r"LIVE (?P<slot>\d+) .+"],
            "bootstrap": False,
        }],
    }
    profile_two = _profile(2, 8, [])
    profiles = [profile_one, profile_two]
    stream_rows = {
        50: {"id": 50, "name": "Outside", "channel_group_id": 5},
        101: {
            "id": 101,
            "name": f"LIVE 1 Main Event @ {active_start.strftime('%b %d %I:%M %p')}",
            "channel_group_id": 9,
        },
        102: {"id": 102, "name": "Backup 1", "channel_group_id": 9},
        103: {"id": 103, "name": "Overflow", "channel_group_id": 9},
        201: {"id": 201, "name": "Other A", "channel_group_id": 10},
        202: {"id": 202, "name": "Other B", "channel_group_id": 10},
    }
    channels = {
        10: {
            "id": 10, "name": "Arena 0", "channel_number": 100,
            "channel_group_id": 7, "hidden_from_output": False,
            "epg_data_id": 500, "streams": [50, 102], "tvg_id": "event-10",
        },
        11: {
            "id": 11, "name": "Arena 1", "channel_number": 101,
            "channel_group_id": 7, "hidden_from_output": True,
            "epg_data_id": 501, "streams": [50, 102], "tvg_id": "event-11",
        },
        20: {
            "id": 20, "name": "Arena 2", "channel_number": 102,
            "channel_group_id": 8, "hidden_from_output": False,
            "epg_data_id": 920, "streams": [], "tvg_id": "event-20",
        },
    }
    sources = [
        {
            "id": 100, "name": "External", "source_type": "xmltv",
            "is_active": True, "url": "https://guide.example/100.xml", "priority": 0,
        },
        {
            "id": 46, "name": "Generated one", "source_type": "xmltv",
            "is_active": True, "url": "http://ecm/api/dummy-epg/xmltv/1", "priority": 0,
        },
        {
            "id": 47, "name": "Generated two", "source_type": "xmltv",
            "is_active": True, "url": "http://ecm/api/dummy-epg/xmltv/2", "priority": 0,
        },
    ]
    first_document = (
        '<tv><channel id="external-10"><display-name>External 10</display-name></channel></tv>'
    ).encode()
    active_document = (
        '<tv><channel id="external-10"><display-name>External 10</display-name></channel>'
        '<channel id="external-11"><display-name>External 11</display-name></channel>'
        f'<programme channel="external-11" start="{active_start.strftime("%Y%m%d%H%M%S +0000")}" '
        f'stop="{active_stop.strftime("%Y%m%d%H%M%S +0000")}"><title>Main Event</title></programme></tv>'
    ).encode()
    source_started = asyncio.Event()
    source_release = asyncio.Event()
    source_calls = []
    link_ready = True
    oversized = False

    async def stream_xmltv(source, **options):
        source_calls.append(source["id"])
        if len(source_calls) == 2:
            source_started.set()
            await source_release.wait()
            yield active_document
        else:
            yield first_document

    async def get_channels(**kwargs):
        return [copy.deepcopy(channel) for channel in channels.values()]

    async def get_streams_by_ids(ids):
        return [copy.deepcopy(stream_rows[stream_id]) for stream_id in ids]

    async def get_streams(**kwargs):
        if kwargs["channel_group_name"] == "Match one":
            rows = [stream_rows[101], stream_rows[102]]
            if oversized:
                rows.append(stream_rows[103])
        else:
            rows = [stream_rows[201], stream_rows[202]]
        return {"results": copy.deepcopy(rows), "next": None}

    async def get_link(link):
        if link == 501 and not link_ready:
            raise RuntimeError("generated link pending")
        rows = {
            500: {"id": 500, "epg_source": 46, "tvg_id": "event-10"},
            501: {"id": 501, "epg_source": 46, "tvg_id": "event-11"},
            920: {"id": 920, "epg_source": 47, "tvg_id": "event-20"},
        }
        return copy.deepcopy(rows[link])

    async def get_guide_rows(**kwargs):
        if kwargs["epg_source"] == 46:
            return [
                {"id": 500, "epg_source": 46, "tvg_id": "event-10"},
                {"id": 501, "epg_source": 46, "tvg_id": "event-11"},
            ]
        return [{"id": 920, "epg_source": 47, "tvg_id": "event-20"}]

    updates = []

    async def update_channel(channel_id, values):
        updates.append((channel_id, copy.deepcopy(values)))
        channels[channel_id].update(copy.deepcopy(values))

    client = MagicMock()
    client.get_channels = AsyncMock(side_effect=get_channels)
    client.get_streams_by_ids = AsyncMock(side_effect=get_streams_by_ids)
    client._channel_group_name_for_id = AsyncMock(
        side_effect=lambda group_id: {9: "Match one", 10: "Match two"}.get(group_id),
    )
    client.get_streams = AsyncMock(side_effect=get_streams)
    client.get_epg_sources = AsyncMock(return_value=copy.deepcopy(sources))
    client.get_epg_data_by_id = AsyncMock(side_effect=get_link)
    client.get_epg_data = AsyncMock(side_effect=get_guide_rows)
    client.update_channel = AsyncMock(side_effect=update_channel)

    seed_one = copy.deepcopy(profile_one)
    seed_one["channel_group_ids"] = []
    seed_one["channel_assignments"] = [{"channel_id": 10, "channel_name": "Arena 0"}]
    monkeypatch.setattr(guides, "stream_xmltv", stream_xmltv)
    seed_channels = {10: copy.deepcopy(channels[10]), 20: copy.deepcopy(channels[20])}
    prepared, coverage = await guides.prepare_profiles(
        [seed_one, profile_two], seed_channels, client, now=now, wait_for_sources=True,
    )
    seeded = publish_profiles(prepared, seed_channels, coverage, observations={}, now=now)
    before_profile = read_publication("profile:1")["xmltv"]
    before_all = read_publication("all")["xmltv"]
    assert seeded.published_profile_ids == (1, 2)
    entry = next(iter(guides._SOURCE_CACHE.values()))
    entry["checked"] -= guides.SOURCE_RETRY + 1

    async def imported(*args, **kwargs):
        return True

    async def emby_refresh():
        return None

    with patch("tasks.event_visibility._load_profiles", return_value=(copy.deepcopy(profiles), [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", side_effect=imported), \
         patch("emby_client.request_guide_refresh", side_effect=emby_refresh), \
         patch("tasks.event_visibility.MAX_MATCH_STREAMS", 2):
        run = asyncio.create_task(event_visibility.EventVisibilityTask().execute())
        await asyncio.wait_for(source_started.wait(), timeout=1)
        assert run.done() is False
        source_release.set()
        first = await asyncio.wait_for(run, timeout=2)

        first_profile = read_publication("profile:1")["xmltv"]
        update_count = len(updates)
        oversized = True
        second = await event_visibility.EventVisibilityTask().execute()

    assert first.details["published_profile_ids"] == [1, 2]
    assert first.details["retained_profile_ids"] == []
    assert first.details["revealed_channel_ids"] == [11]
    assert first.details["hidden_channel_ids"] == [10, 20]
    assert channels[11]["hidden_from_output"] is False
    assert channels[11]["epg_data_id"] == 501
    assert channels[11]["streams"] == [101, 50, 102]
    assert first_profile != before_profile
    assert read_publication("all")["xmltv"] != before_all
    assert len(source_calls) == 2
    assert second.details["retained_profile_ids"] == [1]
    assert second.details["published_profile_ids"] == [2]
    assert read_publication("profile:1")["xmltv"] == first_profile
    assert len(updates) == update_count
    assert len(source_calls) == 2


@pytest.mark.asyncio
async def test_ordinary_refresh_moves_idle_to_active_and_retains_after_bad_inputs(monkeypatch):
    from services import epg_programmes as guides
    from services.epg_publication import read_publication
    from tasks.dummy_epg_refresh import DummyEPGRefreshTask

    now = datetime.now(timezone.utc).replace(microsecond=0)
    active_start = now - timedelta(minutes=10)
    active_stop = now + timedelta(minutes=80)
    selected = _profile(1, 7, [100])
    selected["channel_mappings"] = [
        {"channel_id": 10, "source_id": 100, "tvg_id": "external-10"},
    ]
    channels = {
        10: {
            "id": 10, "name": "Arena 1", "channel_number": 101,
            "channel_group_id": 7, "hidden_from_output": False,
            "epg_data_id": 901,
            "epg_data": {"id": 901, "epg_source": 46, "tvg_id": "event-10"},
            "streams": [], "tvg_id": "event-10",
        },
    }
    sources = [
        {
            "id": 100, "name": "External", "source_type": "xmltv",
            "is_active": True, "url": "https://guide.example/100.xml", "priority": 0,
        },
        {
            "id": 46, "name": "Generated", "source_type": "xmltv",
            "is_active": True, "url": "http://ecm/api/dummy-epg/xmltv/1", "priority": 0,
        },
    ]
    empty_document = (
        '<tv><channel id="external-10"><display-name>External 10</display-name></channel></tv>'
    ).encode()
    active_document = (
        '<tv><channel id="external-10"><display-name>External 10</display-name></channel>'
        f'<programme channel="external-10" start="{active_start.strftime("%Y%m%d%H%M%S +0000")}" '
        f'stop="{active_stop.strftime("%Y%m%d%H%M%S +0000")}"><title>Main Event</title></programme></tv>'
    ).encode()
    mode = "empty"
    source_calls = []

    async def stream_xmltv(source, **options):
        source_calls.append(mode)
        if mode == "cancel":
            raise asyncio.CancelledError()
        if mode == "malformed":
            yield active_document[:-5]
        elif mode == "active":
            yield active_document
        else:
            yield empty_document

    async def get_channels(**kwargs):
        return [copy.deepcopy(channel) for channel in channels.values()]

    async def update_channel(channel_id, values):
        channels[channel_id].update(copy.deepcopy(values))

    client = MagicMock()
    client.get_channels = AsyncMock(side_effect=get_channels)
    client.get_streams_by_ids = AsyncMock(return_value=[])
    client.get_epg_sources = AsyncMock(return_value=copy.deepcopy(sources))
    client.get_epg_data_by_id = AsyncMock(return_value={
        "id": 901, "epg_source": 46, "tvg_id": "event-10",
    })
    client.get_epg_data = AsyncMock(return_value=[{
        "id": 901, "epg_source": 46, "tvg_id": "event-10",
    }])
    client.update_channel = AsyncMock(side_effect=update_channel)
    monkeypatch.setattr(guides, "stream_xmltv", stream_xmltv)

    async def imported(*args, **kwargs):
        return True

    async def emby_refresh():
        return None

    with patch("tasks.event_visibility._load_profiles", return_value=([copy.deepcopy(selected)], [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=client), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", side_effect=imported), \
         patch("emby_client.request_guide_refresh", side_effect=emby_refresh):
        first = await DummyEPGRefreshTask().execute()
        assert first.details["hidden_channel_ids"] == [10], {
            key: first.details[key]
            for key in (
                "idle_channel_count", "active_channel_count", "unknown_channel_count",
                "source_reason_codes", "reason_codes", "epg_linked_channel_ids",
            )
        }
        assert channels[10]["hidden_from_output"] is True

        mode = "active"
        entry = next(iter(guides._SOURCE_CACHE.values()))
        entry["checked"] -= guides.SOURCE_TTL + 1
        second = await DummyEPGRefreshTask().execute()
        assert second.details["revealed_channel_ids"] == [10]
        assert channels[10]["hidden_from_output"] is False

        before_xml = read_publication("profile:1")["xmltv"]
        before_success = next(iter(guides._SOURCE_CACHE.values()))["success"]
        update_count = client.update_channel.await_count
        mode = "malformed"
        next(iter(guides._SOURCE_CACHE.values()))["checked"] -= guides.SOURCE_TTL + 1
        await DummyEPGRefreshTask().execute()
        assert read_publication("profile:1")["xmltv"] == before_xml
        assert next(iter(guides._SOURCE_CACHE.values()))["success"] == before_success
        assert client.update_channel.await_count == update_count

        mode = "cancel"
        next(iter(guides._SOURCE_CACHE.values()))["checked"] -= guides.SOURCE_RETRY + 1
        await DummyEPGRefreshTask().execute()

    assert read_publication("profile:1")["xmltv"] == before_xml
    assert next(iter(guides._SOURCE_CACHE.values()))["success"] == before_success
    assert client.update_channel.await_count == update_count
    assert source_calls == ["empty", "active", "malformed", "cancel"]
