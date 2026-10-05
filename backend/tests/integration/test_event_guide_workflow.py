"""Immutable-base control for retained guide reconciliation."""
import asyncio
import copy
import hashlib
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
import xml.etree.ElementTree as ET

import pytest
from sqlalchemy.orm import sessionmaker

import database

ATTEMPT_ADMITTED_AT = datetime.now(timezone.utc)
ATTEMPT_EXPIRES_AT = ATTEMPT_ADMITTED_AT + timedelta(hours=24)


@pytest.fixture(autouse=True)
async def isolated_guide_state(monkeypatch, test_engine, tmp_path):
    from cache import get_cache
    from services import epg_programmes as guides

    sessions = sessionmaker(
        autocommit=False, autoflush=False, bind=test_engine, expire_on_commit=False,
    )
    monkeypatch.setattr(database, "_SessionLocal", sessions)
    monkeypatch.setattr("services.epg_publication.get_session", sessions)
    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    guides._SOURCE_CACHE.clear()
    guides._SOURCE_LOADS.clear()
    guides._SOURCE_EXPIRIES.clear()
    guides._CATALOGUE_CACHE.clear()
    guides._CATALOGUE_LOADS.clear()
    guides._CATALOGUE_EXPIRIES.clear()
    get_cache().clear()
    yield
    tasks = [*guides._SOURCE_LOADS.values(), *guides._CATALOGUE_LOADS.values()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    guides._SOURCE_LOADS.clear()
    guides._SOURCE_EXPIRIES.clear()
    guides._SOURCE_CACHE.clear()
    guides._CATALOGUE_LOADS.clear()
    guides._CATALOGUE_EXPIRIES.clear()
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
    guide_attempt = None
    if scope.startswith("profile:"):
        guide_attempt = {
            "attempt_id": "1" * 32,
            "config_hash": "b" * 64,
            "admitted_at": ATTEMPT_ADMITTED_AT.isoformat(),
            "expires_at": ATTEMPT_EXPIRES_AT.isoformat(),
            "stage": "preparing",
        }
    return {
        "scope": scope,
        "xmltv": document,
        "revision": 1,
        "state": {
            "published": True,
            "published_at": "2026-09-19T10:00:00+00:00",
            "xmltv_hash": hashlib.sha256(document.encode()).hexdigest(),
            "config_hash": "b" * 64,
            "channels": list(channels or []),
            "delivery": {
                "required_dispatcharr_hashes": {},
                "confirmed_dispatcharr_hashes": {},
                "pending_emby": True,
                "guide_attempt": guide_attempt,
                "source_refreshes": {},
                "pending_channels": {},
            },
        },
    }


def _admit(publications):
    def begin(scope, *, expected_revision, expected_hash, profile, now, pending_channels=None):
        row = publications.get(scope)
        assert row is not None
        assert row["revision"] == expected_revision
        assert row["state"]["xmltv_hash"] == expected_hash
        assert scope == f"profile:{profile['id']}"
        assert now.tzinfo is not None
        assert pending_channels in (None, {})
        return row

    return begin


def test_generated_document_waits_for_import_completion_before_replacing_old_row():
    from services.epg_publication import publish_profiles

    now = datetime(2026, 10, 3, 18, 45, tzinfo=timezone.utc)
    prepared = _profile(1, 65, [46])
    prepared.update({
        "name_source": "channel",
        "title_pattern": r"(?P<title>.+)",
        "title_template": "{title}",
        "tvg_id_template": "ecm-{channel_id}",
        "channel_assignments": [{"channel_id": 5040, "channel_name": "Reported event"}],
        "guide_start": datetime(2026, 10, 3, 15, tzinfo=timezone.utc),
        "guide_stop": datetime(2026, 10, 5, 4, tzinfo=timezone.utc),
        "source_programmes": {5040: []},
    })
    channels = {
        5040: {
            "id": 5040,
            "name": "Atlantic Sun Conference Norfolk St Vs. Bellarmine @ Sep 18 10:00 AM",
            "channel_number": 912,
            "channel_group_id": 65,
            "hidden_from_output": False,
            "streams": [],
        },
    }
    coverage = {
        "profiles": {"1": {"profile_id": 1, "can_publish": True, "reason_codes": []}},
    }
    imported_rows = {
        "ecm-5040": [{
            "title": "Programming unavailable",
            "start": "2026-10-03T15:00:00+00:00",
            "stop": "2026-10-05T04:00:00+00:00",
        }],
    }
    before_import = copy.deepcopy(imported_rows)

    result = publish_profiles(
        [prepared], channels, coverage, observations={}, now=now,
    )
    document = ET.fromstring(result.xmltv_by_scope["profile:1"])

    assert imported_rows == before_import
    imported_rows["ecm-5040"] = [
        {
            "title": row.findtext("title"),
            "start": row.attrib["start"],
            "stop": row.attrib["stop"],
        }
        for row in document.findall("programme[@channel='ecm-5040']")
    ]
    assert imported_rows["ecm-5040"] == []
    assert document.find("channel[@id='ecm-5040']") is not None


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

    async def update_channel(channel_id, values):
        channels[channel_id].update(values)

    client.update_channel = AsyncMock(side_effect=update_channel)
    client.get_channel = AsyncMock(side_effect=lambda channel_id: copy.deepcopy(channels[channel_id]))
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

            def update(scope, *, expected_revision, expected_hash=None,
                       expected_config_hash=None, expected_attempt_id=None,
                       required_dispatcharr_hashes=None, confirmed_dispatcharr_hashes=None,
                       pending_emby=None, source_refreshes=None):
                row = publications[scope]
                if row["revision"] != expected_revision:
                    return None
                if expected_hash is not None:
                    assert row["state"]["xmltv_hash"] == expected_hash
                if expected_config_hash is not None:
                    assert row["state"]["config_hash"] == expected_config_hash
                if expected_attempt_id is not None:
                    assert row["state"]["delivery"]["guide_attempt"]["attempt_id"] == expected_attempt_id
                if required_dispatcharr_hashes is not None:
                    row["state"]["delivery"]["required_dispatcharr_hashes"] = dict(required_dispatcharr_hashes)
                if confirmed_dispatcharr_hashes is not None:
                    row["state"]["delivery"]["confirmed_dispatcharr_hashes"] = dict(confirmed_dispatcharr_hashes)
                if pending_emby is not None:
                    row["state"]["delivery"]["pending_emby"] = pending_emby
                if source_refreshes is not None:
                    row["state"]["delivery"]["source_refreshes"] = copy.deepcopy(source_refreshes)
                row["revision"] += 1
                return row["revision"]

            async def commit(function, *args, **kwargs):
                assert function.__name__ == "publish_profiles"
                assert set(kwargs["expected"]) == {"profile:1", "profile:2"}
                assert all(
                    set(claim) == {
                        "revision", "xmltv_hash", "config_hash", "attempt_id",
                    }
                    for claim in kwargs["expected"].values()
                )
                return result

            stack.enter_context(patch(
                "tasks.event_visibility._load_profiles",
                return_value=(copy.deepcopy(profiles), []),
            ))
            stack.enter_context(patch("tasks.event_visibility.get_client", return_value=client))
            stack.enter_context(patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}, {}))))
            stack.enter_context(patch("concurrency.run_cpu_bound", side_effect=commit))
            stack.enter_context(patch("services.epg_publication.read_publication", side_effect=read))
            stack.enter_context(patch(
                "services.epg_publication.begin_delivery",
                side_effect=_admit(publications),
            ))
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
    from tasks.dummy_epg_refresh import wait_for_epg_source_refresh

    now = datetime.now(timezone.utc).replace(
        hour=0, minute=5, second=0, microsecond=0,
    )
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
    for stream_id, row in stream_rows.items():
        row.update(url=f"https://streams.example/{stream_id}", m3u_account=None)
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
    import_ready = False
    oversized = False
    imports = {46: 0, 47: 0}

    async def get_epg_source(source_id):
        source = next(row for row in sources if row["id"] == source_id)
        if source.get("status") == "running" and import_ready:
            source.update(status="success", updated_at=str(imports[source_id]))
        return copy.deepcopy(source)

    async def refresh_source(source_id):
        imports[source_id] += 1
        source = next(row for row in sources if row["id"] == source_id)
        if source_id == 46 and imports[source_id] > 1 and not import_ready:
            source["status"] = "running"
        else:
            source.update(status="success", updated_at=str(imports[source_id]))

    async def load_stats(ids):
        return {
            stream_id: {
                "stream_name": stream_rows[stream_id]["name"],
                "probe_status": "success", "measured_bitrate": 1000,
                "last_probed": now.isoformat(), "is_black_screen": False,
                "black_screen_checked_at": now.isoformat(),
            }
            for stream_id in ids
        }

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
        ids = kwargs.get("ids")
        if ids is not None:
            assert isinstance(ids, frozenset) and ids
            assert all(type(value) is int and value > 0 for value in ids)
            assert kwargs["max_results"] == len(ids)
            expires_at = kwargs["expires_at"]
            assert expires_at is None
            assert kwargs.get("epg_source") is None
            assert not kwargs.get("search")
            return [await get_link(link) for link in sorted(ids)]
        if kwargs["epg_source"] == 46:
            return [
                {"id": 500, "epg_source": 46, "tvg_id": "event-10"},
                {"id": 501, "epg_source": 46, "tvg_id": "event-11"},
            ]
        return [{"id": 920, "epg_source": 47, "tvg_id": "event-20"}]

    updates = []
    programme_reads = []

    async def update_channel(channel_id, values):
        updates.append((channel_id, copy.deepcopy(values)))
        channels[channel_id].update(copy.deepcopy(values))

    async def get_epg_programmes(epg_ids, *, expires_at):
        assert epg_ids == frozenset({501})
        assert (await get_epg_source(46))["status"] == "success"
        publication = read_publication("profile:1")
        phase = next(iter(publication["state"]["delivery"]["source_refreshes"].values()))
        assert phase["links"] == {"10": 500, "11": 501}
        assert phase["completed"] is True
        assert phase["pending_links"] is None
        attempt = publication["state"]["delivery"]["guide_attempt"]
        assert expires_at is None
        assert attempt["expires_at"] is None
        evidence = next(
            item for item in publication["state"]["channels"]
            if item["channel_id"] == 11
        )
        current = next(
            item for item in evidence["events"]
            if datetime.fromisoformat(item["start"]) <= now
            < datetime.fromisoformat(item["stop"])
        )
        row = {
            "epg_data_id": 501,
            "tvg_id": "event-11",
            "title": current["title"],
            "start_time": current["start"],
            "end_time": current["stop"],
        }
        programme_reads.append(copy.deepcopy(row))
        return [row]

    client = MagicMock()
    client.base_url = "http://dispatcharr.test"
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
    client.get_channel = AsyncMock(
        side_effect=lambda channel_id: copy.deepcopy(channels[channel_id]),
    )
    client.get_epg_grid = AsyncMock(return_value=[])
    client.get_epg_programmes = AsyncMock(side_effect=get_epg_programmes)
    client.get_epg_source = AsyncMock(side_effect=get_epg_source)
    client.refresh_epg_source = AsyncMock(side_effect=refresh_source)

    seed_one = copy.deepcopy(profile_one)
    seed_one["channel_group_ids"] = []
    seed_one["channel_assignments"] = [{"channel_id": 10, "channel_name": "Arena 0"}]
    monkeypatch.setattr(guides, "stream_xmltv", stream_xmltv)
    seed_channels = {10: copy.deepcopy(channels[10]), 20: copy.deepcopy(channels[20])}
    prepared, coverage = await guides.prepare_profiles(
        [seed_one, profile_two],
        seed_channels,
        client,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=24),
        now=now,
        wait_for_sources=True,
    )
    seeded = publish_profiles(prepared, seed_channels, coverage, observations={}, now=now)
    before_profile = read_publication("profile:1")["xmltv"]
    before_all = read_publication("all")["xmltv"]
    assert seeded.published_profile_ids == (1, 2)
    entry = next(iter(guides._SOURCE_CACHE.values()))
    entry["checked"] -= guides.SOURCE_RETRY + 1

    async def imported(*args, **kwargs):
        assert kwargs["wait"] is False
        assert kwargs["expires_at"] is None
        return await wait_for_epg_source_refresh(*args, **kwargs)

    async def emby_refresh():
        return None

    with patch("tasks.event_visibility.datetime", wraps=datetime) as visibility_clock, \
         patch("tasks.event_visibility._load_profiles", return_value=(copy.deepcopy(profiles), [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.event_sync_stream_health._load_stats", side_effect=load_stats), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", side_effect=imported), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=AsyncMock(
             return_value={101: True},
         )), \
         patch("emby_client.request_guide_refresh", side_effect=emby_refresh), \
         patch("tasks.event_visibility.MAX_MATCH_STREAMS", 2):
        visibility_clock.now.return_value = now
        run = asyncio.create_task(event_visibility.EventVisibilityTask().execute())
        await asyncio.wait_for(source_started.wait(), timeout=1)
        first = await asyncio.wait_for(run, timeout=2)
        assert source_release.is_set() is False
        assert first.details["published_profile_ids"] == [2]
        assert first.details["retained_profile_ids"] == [1]
        assert first.details["hidden_channel_ids"] == [20]
        assert read_publication("profile:1")["xmltv"] == before_profile
        now += timedelta(minutes=2)
        visibility_clock.now.return_value = now
        source_release.set()
        await asyncio.gather(*list(guides._SOURCE_LOADS.values()))
        pending = await event_visibility.EventVisibilityTask().execute()
        assert pending.details["revealed_channel_ids"] == []
        assert pending.details["hidden_channel_ids"] == [10]
        assert channels[11]["hidden_from_output"] is True
        assert channels[11]["streams"] == [50, 102]
        now += timedelta(minutes=31)
        visibility_clock.now.return_value = now
        import_ready = True
        converged = await event_visibility.EventVisibilityTask().execute()

        first_profile = read_publication("profile:1")["xmltv"]
        first_document = ET.fromstring(first_profile)
        update_count = len(updates)
        oversized = True
        retained = await event_visibility.EventVisibilityTask().execute()

    assert converged.details["published_profile_ids"] == [1, 2]
    assert converged.details["retained_profile_ids"] == []
    assert converged.details["revealed_channel_ids"] == [11]
    assert converged.details["hidden_channel_ids"] == []
    assert channels[11]["hidden_from_output"] is False
    assert channels[11]["epg_data_id"] == 501
    assert channels[11]["streams"] == [101, 50, 102]
    assert first_document.find("channel[@id='event-10']") is not None
    assert first_document.find("channel[@id='event-11']") is not None
    assert first_document.findall("programme[@channel='event-10']") == []
    assert [row.findtext("title") for row in first_document.findall(
        "programme[@channel='event-11']"
    )] == ["Main Event"]
    published_programme = first_document.find("programme[@channel='event-11']")
    assert published_programme is not None
    assert programme_reads == [{
        "epg_data_id": 501,
        "tvg_id": "event-11",
        "title": published_programme.findtext("title"),
        "start_time": datetime.strptime(
            published_programme.get("start"), "%Y%m%d%H%M%S %z",
        ).isoformat(),
        "end_time": datetime.strptime(
            published_programme.get("stop"), "%Y%m%d%H%M%S %z",
        ).isoformat(),
    }]
    published_start = datetime.fromisoformat(programme_reads[0]["start_time"])
    assert published_start == now.replace(hour=0, minute=0)
    assert active_start < published_start
    assert all(
        row.findtext("title") != "Programming unavailable"
        for row in first_document.findall("programme")
    )
    assert first_profile != before_profile
    assert read_publication("all")["xmltv"] != before_all
    assert len(source_calls) == 2
    assert retained.details["retained_profile_ids"] == [1]
    assert retained.details["published_profile_ids"] == [2]
    assert read_publication("profile:1")["xmltv"] == first_profile
    assert len(updates) == update_count
    assert len(source_calls) == 2


@pytest.mark.asyncio
async def test_ordinary_refresh_moves_idle_to_active_and_retains_after_bad_inputs(monkeypatch):
    from services import epg_programmes as guides
    from services.epg_publication import read_publication
    from tasks.dummy_epg_refresh import DummyEPGRefreshTask, wait_for_epg_source_refresh
    from tasks.event_visibility import _guide_name

    now = datetime.now(timezone.utc).replace(
        hour=0, minute=5, second=0, microsecond=0,
    )
    active_start = now - timedelta(minutes=10)
    active_stop = now + timedelta(minutes=80)
    selected = _profile(1, 7, [100])
    selected["event_sync_config"] = {
        **_config(),
        "secondary": [{"group_id": 9, "m3u_account_id": None}],
    }
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

    programme_reads = []

    async def get_epg_programmes(epg_ids, *, expires_at):
        assert epg_ids == frozenset({901})
        publication = read_publication("profile:1")
        attempt = publication["state"]["delivery"]["guide_attempt"]
        assert expires_at is None
        assert attempt["expires_at"] is None
        if mode != "active":
            return []
        evidence = next(
            item for item in publication["state"]["channels"]
            if item["channel_id"] == 10
        )
        current = next(
            item for item in evidence["events"]
            if datetime.fromisoformat(item["start"]) <= now
            < datetime.fromisoformat(item["stop"])
        )
        row = {
            "epg_data_id": 901,
            "tvg_id": "event-10",
            "title": current["title"],
            "start_time": current["start"],
            "end_time": current["stop"],
        }
        programme_reads.append(copy.deepcopy(row))
        return [row]

    client = MagicMock()
    client.base_url = "http://dispatcharr.test"
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
    client.get_channel = AsyncMock(
        side_effect=lambda channel_id: copy.deepcopy(channels[channel_id]),
    )
    client.get_epg_grid = AsyncMock(return_value=[])
    client.get_epg_programmes = AsyncMock(side_effect=get_epg_programmes)
    client.refresh_epg_source = AsyncMock()
    monkeypatch.setattr(guides, "stream_xmltv", stream_xmltv)

    matched_stream = SimpleNamespace(
        stream_id=101,
        name=_guide_name({"title": "Main Event", "start": active_start.isoformat()}, "UTC"),
        group_id=9,
        provider=None,
        provider_id=None,
        name_seen_before_today=None,
        is_stale=False,
    )
    stream_row = {
        "id": 101, "name": matched_stream.name, "channel_group_id": 9,
        "m3u_account": None, "url": "https://streams.example/101",
    }
    client.get_streams_by_ids = AsyncMock(side_effect=lambda ids: [
        copy.deepcopy(stream_row) for stream_id in ids if stream_id == 101
    ])
    client.get_epg_source = AsyncMock(side_effect=lambda source_id: copy.deepcopy(
        next(source for source in sources if source["id"] == source_id)
    ))

    async def refresh_source(source_id):
        source = next(row for row in sources if row["id"] == source_id)
        source.update(status="success", updated_at=str(client.refresh_epg_source.await_count))

    client.refresh_epg_source.side_effect = refresh_source

    async def load_stats(ids):
        return {stream_id: {
            "stream_name": matched_stream.name,
            "probe_status": "success", "measured_bitrate": 1000,
            "last_probed": now.isoformat(), "is_black_screen": False,
            "black_screen_checked_at": now.isoformat(),
        } for stream_id in ids}

    async def imported(*args, **kwargs):
        assert kwargs["wait"] is True
        assert kwargs["expires_at"] is None
        kwargs["poll_interval"] = 0
        return await wait_for_epg_source_refresh(*args, **kwargs)

    async def emby_refresh():
        return None

    with patch("tasks.event_visibility.datetime", wraps=datetime) as visibility_clock, \
         patch("tasks.dummy_epg_refresh.datetime", wraps=datetime) as refresh_clock, \
         patch("tasks.event_visibility._load_profiles", return_value=([copy.deepcopy(selected)], [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=client), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             [matched_stream], {(9, None)}, {},
             {101: (matched_stream.name, None, 9, stream_row["url"])},
         ))), \
         patch("services.event_sync_stream_health._load_stats", side_effect=load_stats), \
         patch("services.event_sync_stream_health.collect_stream_flow", new=AsyncMock(
             return_value={101: True},
         )), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", side_effect=imported), \
         patch("emby_client.request_guide_refresh", side_effect=emby_refresh):
        visibility_clock.now.return_value = now
        refresh_clock.now.return_value = now
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
        published = ET.fromstring(read_publication("profile:1")["xmltv"])
        published_programme = published.find("programme[@channel='event-10']")
        assert published_programme is not None
        assert programme_reads == [{
            "epg_data_id": 901,
            "tvg_id": "event-10",
            "title": published_programme.findtext("title"),
            "start_time": datetime.strptime(
                published_programme.get("start"), "%Y%m%d%H%M%S %z",
            ).isoformat(),
            "end_time": datetime.strptime(
                published_programme.get("stop"), "%Y%m%d%H%M%S %z",
            ).isoformat(),
        }]
        published_start = datetime.fromisoformat(programme_reads[0]["start_time"])
        assert published_start == now.replace(hour=0, minute=0)
        assert active_start < published_start

        before_xml = read_publication("profile:1")["xmltv"]
        before_success = next(iter(guides._SOURCE_CACHE.values()))["success"]
        update_count = client.update_channel.await_count
        mode = "malformed"
        next(iter(guides._SOURCE_CACHE.values()))["checked"] -= guides.SOURCE_TTL + 1
        await DummyEPGRefreshTask().execute()
        assert read_publication("profile:1")["xmltv"] == before_xml
        assert next(iter(guides._SOURCE_CACHE.values()))["success"] == before_success
        assert client.update_channel.await_count == update_count, client.update_channel.await_args_list

        mode = "cancel"
        next(iter(guides._SOURCE_CACHE.values()))["checked"] -= guides.SOURCE_RETRY + 1
        await DummyEPGRefreshTask().execute()

    assert read_publication("profile:1")["xmltv"] == before_xml
    assert next(iter(guides._SOURCE_CACHE.values()))["success"] == before_success
    assert client.update_channel.await_count == update_count
    assert source_calls == ["empty", "active", "malformed", "cancel"]
