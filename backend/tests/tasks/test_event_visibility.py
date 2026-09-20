"""Focused checks for the configured profile reconciliation workflow."""
import asyncio
import copy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.epg_publication import PublicationResult
from task_scheduler import ScheduleType
from tasks.event_visibility import (
    CHECK_INTERVAL_SECONDS,
    EventVisibilityTask,
    _await_preparation,
    _fetch_match_streams,
    _delivery_plan,
    _generated_scope,
    _guide_name,
    _plan_profile,
    _slot_key,
    reconcile_profiles,
)


def _config(scopes=None):
    return {
        "secondary": list(scopes or []),
        "time_window_minutes": 30,
        "enforce_time_window": True,
        "attach_threshold": 0.8,
        "assume_current_date": True,
        "demote_stale_dateless": True,
        "use_default_patterns": True,
        "slot_patterns": [{
            "name": "Arena",
            "channel_pattern": r"Arena (?P<slot>\d+)",
            "fallback_pattern": r"Backup (?P<slot>\d+)",
            "event_patterns": [r"LIVE (?P<slot>\d+) .+"],
            "bootstrap": True,
        }],
    }


def _profile(**overrides):
    values = {
        "id": 1,
        "name": "Arena",
        "enabled": True,
        "channel_group_ids": [7],
        "hide_empty_group_ids": [7],
        "stream_match_group_ids": [],
        "event_sync_config": _config(),
        "event_timezone": "UTC",
        "output_timezone": "UTC",
        "program_duration": 180,
        "pattern_variants": [],
        "epg_source_ids": [],
        "tvg_id_template": "custom-{channel_id}",
        "channel_assignments": [
            {"channel_id": 10, "channel_name": "Arena 1"},
            {"channel_id": 20, "channel_name": "Arena 2"},
        ],
    }
    values.update(overrides)
    return values


def _publication(scope, *, revision=1, pending=True, confirmed=None, channels=None):
    value_hash = "a" * 64
    return {
        "scope": scope,
        "xmltv": "<tv/>",
        "revision": revision,
        "state": {
            "published_at": "2026-09-20T12:00:00+00:00",
            "xmltv_hash": value_hash,
            "channels": list(channels or []),
            "delivery": {
                "required_dispatcharr_hashes": {},
                "confirmed_dispatcharr_hashes": dict(confirmed or {}),
                "pending_emby": pending,
            },
        },
    }


def test_default_schedule_checks_every_five_minutes():
    task = EventVisibilityTask()

    assert task.schedule_config.schedule_type is ScheduleType.INTERVAL
    assert task.schedule_config.interval_seconds == CHECK_INTERVAL_SECONDS == 300
    assert task.schedule_config.timezone == "America/Chicago"


@pytest.mark.parametrize(
    "url, expected",
    [
        ("http://ecm/api/dummy-epg/xmltv", "all"),
        ("http://ecm/api/dummy-epg/xmltv/?key=1", "all"),
        ("http://ecm/api/dummy-epg/xmltv/42?key=1", "profile:42"),
        ("http://ecm/api/dummy-epg/xmltv/0", None),
        ("http://ecm/prefix/api/dummy-epg/xmltv/42", None),
        ("http://ecm/api/dummy-epg/xmltv/42/extra", None),
        ("http://ecm/not-api/dummy-epg/xmltv/42", None),
    ],
)
def test_generated_scope_requires_an_exact_path(url, expected):
    assert _generated_scope({"url": url}) == expected


def test_unchanged_confirmed_hash_does_not_import_again():
    row = _publication(
        "profile:1",
        confirmed={"46": "a" * 64},
    )

    required, confirmed, pending = _delivery_plan(row, [{"id": 46}])

    assert required == {"46": "a" * 64}
    assert confirmed == required
    assert pending == {}


def test_changed_hash_remains_pending_until_confirmed():
    row = _publication(
        "profile:1",
        confirmed={"46": "b" * 64},
    )

    required, confirmed, pending = _delivery_plan(row, [{"id": 46}])

    assert required == {"46": "a" * 64}
    assert confirmed == {}
    assert pending == {46: "a" * 64}


def test_slot_key_uses_configured_families_and_normalizes_numbers():
    config = _config()

    assert _slot_key("Arena 007", config) == ("Arena", "7")
    assert _slot_key("Backup 07", config, role="fallback") == ("Arena", "7")
    assert _slot_key("LIVE 7 Main Event", config, role="event") == ("Arena", "7")
    assert _slot_key("UFC07", config) is None


def test_guide_name_uses_profile_timezone_and_rejects_placeholders():
    current = {
        "title": "The Main Event ᴸᶦᵛᵉ",
        "start": "2026-09-20T01:00:00+00:00",
    }

    assert _guide_name(current, "US/Eastern") == "The Main Event @ Sep 19 9:00 PM"
    assert _guide_name({**current, "title": "No events scheduled"}, "UTC") is None


@pytest.mark.asyncio
async def test_fetch_match_streams_honors_account_scope_and_pagination():
    client = MagicMock()
    client._channel_group_name_for_id = AsyncMock(return_value="Events")
    client.get_streams = AsyncMock(side_effect=[
        {
            "results": [{
                "id": 11,
                "name": "LIVE 1 Main Event",
                "m3u_account": {"id": 4},
            }],
            "next": "page-2",
        },
        {
            "results": [{
                "id": 12,
                "name": "Backup 1",
                "m3u_account": 4,
            }],
            "next": None,
        },
    ])

    streams, complete, failures = await _fetch_match_streams(
        client, [{"group_id": 9, "m3u_account_id": 4}],
    )

    assert [stream.stream_id for stream in streams] == [11, 12]
    assert all(stream.provider_id == 4 for stream in streams)
    assert complete == {(9, 4)}
    assert failures == {}
    assert client.get_streams.await_args_list[0].kwargs["m3u_account"] == 4
    assert client.get_streams.await_args_list[1].kwargs["page"] == 2


@pytest.mark.asyncio
async def test_fetch_match_streams_isolates_a_failed_scope():
    client = MagicMock()
    client._channel_group_name_for_id = AsyncMock(side_effect=["One", "Two"])
    client.get_streams = AsyncMock(side_effect=[
        RuntimeError("first unavailable"),
        {"results": [{"id": 22, "name": "Backup 2"}], "next": None},
    ])

    streams, complete, failures = await _fetch_match_streams(
        client,
        [
            {"group_id": 1, "m3u_account_id": None},
            {"group_id": 2, "m3u_account_id": None},
        ],
    )

    assert [stream.stream_id for stream in streams] == [22]
    assert complete == {(2, None)}
    assert failures == {(1, None): "RuntimeError"}


def test_profile_plan_keeps_incomplete_inventory_unknown():
    profile = _profile(
        event_sync_config=_config([{"group_id": 9, "m3u_account_id": 4}]),
    )
    channels = {
        10: {
            "id": 10,
            "name": "Arena 1",
            "channel_group_id": 7,
            "hidden_from_output": False,
            "streams": [{"id": 11, "channel_group_id": 9, "m3u_account": 4}],
        },
    }
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [{"profile_id": 1, "channel_id": 10, "current": None}],
    }

    result = _plan_profile(
        profile,
        profile["event_sync_config"],
        channels,
        coverage,
        [],
        set(),
        None,
        datetime(2026, 9, 20, tzinfo=timezone.utc),
    )

    assert result["states"] == {10: "unknown"}
    assert result["desired"] == {}
    assert result["observations"] is None


def test_profile_plan_marks_complete_empty_inventory_idle_and_keeps_outside_streams():
    scope = {"group_id": 9, "m3u_account_id": 4}
    profile = _profile(event_sync_config=_config([scope]))
    channels = {
        10: {
            "id": 10,
            "name": "Arena 1",
            "channel_group_id": 7,
            "hidden_from_output": False,
            "epg_data": {"id": 900, "epg_source": 46, "tvg_id": "custom-10"},
            "streams": [
                {"id": 11, "channel_group_id": 9, "m3u_account": 4},
                {"id": 50, "channel_group_id": 5, "m3u_account": 8},
            ],
        },
    }
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [{"profile_id": 1, "channel_id": 10, "current": None}],
    }

    result = _plan_profile(
        profile,
        profile["event_sync_config"],
        channels,
        coverage,
        [],
        {(9, 4)},
        None,
        datetime(2026, 9, 20, tzinfo=timezone.utc),
        {46},
    )

    assert result["states"] == {10: "idle"}
    assert result["desired"] == {10: [50]}
    assert result["observations"] == []


@pytest.mark.parametrize(
    "guide_row, expected",
    [
        (None, "unknown"),
        ({"id": 900, "epg_source": 99, "tvg_id": "custom-10"}, "unknown"),
        ({"id": 900, "epg_source": 46, "tvg_id": "other-10"}, "unknown"),
        ({"id": 900, "epg_source": 46, "tvg_id": "custom-10"}, "idle"),
    ],
)
def test_first_publication_idle_requires_exact_generated_guide_link(guide_row, expected):
    profile = _profile(channel_assignments=[{"channel_id": 10, "channel_name": "Arena 1"}])
    channel = {
        "id": 10,
        "name": "Arena 1",
        "channel_group_id": 7,
        "hidden_from_output": False,
        "streams": [],
    }
    if guide_row is not None:
        channel["epg_data"] = guide_row
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [{"profile_id": 1, "channel_id": 10, "current": None}],
    }

    result = _plan_profile(
        profile,
        profile["event_sync_config"],
        {10: channel},
        coverage,
        [],
        set(),
        None,
        datetime(2026, 9, 20, tzinfo=timezone.utc),
        {46},
    )

    assert result["states"] == {10: expected}
    assert result["desired"] == ({10: []} if expected == "idle" else {})


@pytest.mark.parametrize("bootstrap, expected", [(False, "unknown"), (True, "active")])
def test_event_stream_activation_honors_family_bootstrap_without_current_guide(bootstrap, expected):
    from services.event_slots import classify_event_slot
    from services.event_sync_resolver import SecondaryStream

    now = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
    config = _config([{"group_id": 9, "m3u_account_id": None}])
    config["slot_patterns"][0]["bootstrap"] = bootstrap
    profile = _profile(
        channel_assignments=[{"channel_id": 10, "channel_name": "Arena 1"}],
        event_sync_config=config,
    )
    channels = {
        10: {
            "id": 10,
            "name": "Arena 1",
            "channel_group_id": 7,
            "hidden_from_output": False,
            "streams": [],
        },
    }
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [{"profile_id": 1, "channel_id": 10, "current": None}],
    }
    parsed = SimpleNamespace(
        start=now - timedelta(minutes=10),
        title="Main Event",
        matched_pattern="test-pattern",
    )
    preview = classify_event_slot("LIVE 1 Main Event", config, role="event")

    with patch("services.event_sync_matcher.parse_event_name", return_value=parsed):
        result = _plan_profile(
            profile,
            config,
            channels,
            coverage,
            [SecondaryStream(
                name="LIVE 1 Main Event",
                group_id=9,
                stream_id=90,
                is_stale=False,
            )],
            {(9, None)},
            None,
            now,
        )

    assert preview == {
        "family": "Arena",
        "slot": "1",
        "role": "event",
        "validation_issues": [],
    }
    assert result["states"] == {10: expected}
    assert result["observations"] == ([] if not bootstrap else [{
        "family": "Arena",
        "slot": "1",
        "stream_id": 90,
        "normalized_name": "live 1 main event",
        "start": (now - timedelta(minutes=10)).isoformat(),
        "expires_at": (now + timedelta(minutes=170)).isoformat(),
        "title": "Main Event",
        "matched_variant": "test-pattern",
        "provisional": False,
    }])


def test_conflicting_event_slots_are_unknown_and_stale_fallbacks_are_not_attached():
    from services.event_sync_resolver import SecondaryStream

    scope = {"group_id": 9, "m3u_account_id": None}
    config = _config([scope])
    config["slot_patterns"].append({
        "name": "Second",
        "channel_pattern": r"Second (?P<slot>\d+)",
        "fallback_pattern": None,
        "event_patterns": [r"LIVE (?P<slot>\d+) .+"],
        "bootstrap": True,
    })
    profile = _profile(event_sync_config=config)
    channels = {
        10: {
            "id": 10,
            "name": "Arena 1",
            "channel_group_id": 7,
            "hidden_from_output": False,
            "streams": [],
        },
    }
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [{"profile_id": 1, "channel_id": 10, "current": None}],
    }
    streams = [
        SecondaryStream(
            name="LIVE 1 Main Event", group_id=9, stream_id=90, is_stale=False,
        ),
        SecondaryStream(
            name="Backup 1", group_id=9, stream_id=91, is_stale=True,
        ),
    ]

    result = _plan_profile(
        profile,
        config,
        channels,
        coverage,
        streams,
        {(9, None)},
        None,
        datetime(2026, 9, 20, tzinfo=timezone.utc),
    )

    assert result["states"] == {10: "unknown"}
    assert result["desired"] == {}


@pytest.mark.asyncio
async def test_reconciliation_orders_hide_import_link_reveal_and_emby():
    profile = _profile()
    prepared = _profile()
    channels = {
        10: {
            "id": 10,
            "name": "Arena 1",
            "channel_number": 101,
            "channel_group_id": 7,
            "hidden_from_output": True,
            "epg_data_id": None,
            "streams": [{"id": 501, "channel_group_id": 5}],
        },
        20: {
            "id": 20,
            "name": "Arena 2",
            "channel_number": 102,
            "channel_group_id": 7,
            "hidden_from_output": False,
            "epg_data_id": None,
            "streams": [{"id": 502, "channel_group_id": 5}],
        },
    }
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [
            {
                "profile_id": 1,
                "channel_id": 10,
                "current": {
                    "title": "Main Event",
                    "start": "2026-09-20T12:00:00+00:00",
                },
            },
            {"profile_id": 1, "channel_id": 20, "current": None},
        ],
    }
    result = PublicationResult(
        published_profile_ids=(1,),
        xmltv_by_scope={"all": "<tv/>", "profile:1": "<tv/>"},
        hashes_by_scope={"all": "a" * 64, "profile:1": "a" * 64},
    )
    publications = {
        "all": _publication("all"),
        "profile:1": _publication("profile:1"),
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

    order = []
    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[{
        "id": 46,
        "name": "Generated profile",
        "url": "http://ecm/api/dummy-epg/xmltv/1?key=ignored",
        "is_active": True,
    }])

    async def update_channel(channel_id, values):
        order.append(("channel", channel_id, tuple(sorted(values))))

    async def guide_rows(**kwargs):
        order.append(("rows", kwargs["epg_source"]))
        return [{"id": 900, "tvg_id": "custom-10", "epg_source": 46}]

    client.update_channel = AsyncMock(side_effect=update_channel)
    client.get_epg_data = AsyncMock(side_effect=guide_rows)
    task = EventVisibilityTask()

    async def import_source(*args, **kwargs):
        order.append(("import", args[1]))
        return True

    async def emby():
        order.append(("emby",))
        return None

    with patch("tasks.event_visibility._load_profiles", side_effect=[
        ([profile], []), ([profile], []),
    ]), patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([prepared], coverage))), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}))), \
         patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=result)), \
         patch("services.epg_publication.read_publication", side_effect=read), \
         patch("services.epg_publication.update_delivery", side_effect=update), \
         patch("cache.get_cache"), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", side_effect=import_source), \
         patch("emby_client.request_guide_refresh", side_effect=emby):
        outcome = await reconcile_profiles(task, wait_for_sources=True)

    assert outcome.success is True
    assert outcome.completed_degraded is False
    assert outcome.details == {
        "configured_profile_count": 1,
        "published_profile_ids": [1],
        "retained_profile_ids": [],
        "unavailable_profile_ids": [],
        "publication_times": {"1": "2026-09-20T12:00:00+00:00"},
        "source_reason_codes": {"1": []},
        "idle_channel_count": 1,
        "active_channel_count": 1,
        "unknown_channel_count": 0,
        "stream_updated_channel_ids": [],
        "epg_linked_channel_ids": [10],
        "revealed_channel_ids": [10],
        "hidden_channel_ids": [20],
        "pending_source_hashes": {},
        "emby_request_outcome": "disabled",
        "pending_emby": False,
        "delivery_pending": False,
        "reason_codes": [],
    }
    assert order == [
        ("channel", 20, ("hidden_from_output",)),
        ("import", 46),
        ("rows", 46),
        ("channel", 10, ("epg_data_id",)),
        ("channel", 10, ("hidden_from_output",)),
        ("emby",),
    ]


@pytest.mark.asyncio
async def test_reconciliation_keeps_unconfirmed_import_pending():
    profile = _profile(channel_assignments=[])
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [],
    }
    publication = PublicationResult(
        published_profile_ids=(1,),
        xmltv_by_scope={"profile:1": "<tv/>"},
    )
    stored = _publication("profile:1", pending=False)

    def update(scope, *, expected_revision, required_dispatcharr_hashes=None,
               confirmed_dispatcharr_hashes=None, pending_emby=None):
        assert stored["revision"] == expected_revision
        if required_dispatcharr_hashes is not None:
            stored["state"]["delivery"]["required_dispatcharr_hashes"] = dict(required_dispatcharr_hashes)
        if confirmed_dispatcharr_hashes is not None:
            stored["state"]["delivery"]["confirmed_dispatcharr_hashes"] = dict(confirmed_dispatcharr_hashes)
        if pending_emby is not None:
            stored["state"]["delivery"]["pending_emby"] = pending_emby
        stored["revision"] += 1
        return stored["revision"]

    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[{
        "id": 46,
        "name": "Generated profile",
        "url": "http://ecm/api/dummy-epg/xmltv/1",
        "is_active": True,
    }])
    task = EventVisibilityTask()
    with patch("tasks.event_visibility._load_profiles", side_effect=[
        ([profile], []), ([profile], []),
    ]), patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([copy.deepcopy(profile)], coverage))), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}))), \
         patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=publication)), \
         patch("services.epg_publication.read_publication", side_effect=lambda scope: stored), \
         patch("services.epg_publication.update_delivery", side_effect=update), \
         patch("cache.get_cache"), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", new=AsyncMock(return_value=False)), \
         patch("emby_client.request_guide_refresh", new=AsyncMock(return_value=None)):
        outcome = await reconcile_profiles(task, wait_for_sources=False)

    assert outcome.success is False
    assert outcome.completed_degraded is True
    assert outcome.error == "GUIDE_IMPORT_PENDING"
    assert outcome.details["pending_source_hashes"] == {"46": "a" * 64}
    assert outcome.details["delivery_pending"] is True


@pytest.mark.asyncio
async def test_reconciliation_cancellation_prevents_publication_and_mutation():
    profile = _profile()
    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[])
    client.update_channel = AsyncMock()
    task = EventVisibilityTask()
    task._cancel_requested = True

    with patch("tasks.event_visibility._load_profiles", return_value=([profile], [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([profile], {
             "profiles": {"1": {"can_publish": True, "reason_codes": []}},
             "channels": [],
         }))), patch("services.epg_publication.read_publication", return_value=None), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}))), \
         patch("concurrency.run_cpu_bound", new=AsyncMock()) as publish:
        outcome = await reconcile_profiles(task, wait_for_sources=False)

    assert outcome.success is False
    assert outcome.error == "CANCELLED"
    publish.assert_not_awaited()
    client.update_channel.assert_not_awaited()


@pytest.mark.asyncio
async def test_preparation_wait_observes_cancellation_without_cancelling_shared_loads():
    entered = asyncio.Event()
    stopped = asyncio.Event()
    cancel_requested = False

    async def prepare():
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    waiting = asyncio.create_task(
        _await_preparation(prepare(), lambda: cancel_requested)
    )
    await entered.wait()
    cancel_requested = True

    assert await waiting is None
    assert stopped.is_set()


@pytest.mark.asyncio
async def test_cancellation_after_publication_preserves_commit_and_stops_external_delivery():
    profile = _profile(channel_assignments=[])
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [],
    }
    publication = PublicationResult(
        published_profile_ids=(1,),
        xmltv_by_scope={"profile:1": "<tv/>"},
    )
    stored = _publication("profile:1", pending=True)
    task = EventVisibilityTask()
    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[])
    client.update_channel = AsyncMock()
    cache = MagicMock()

    async def publish(*args, **kwargs):
        task._cancel_requested = True
        return publication

    with patch("tasks.event_visibility._load_profiles", return_value=([profile], [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=(
             [copy.deepcopy(profile)], coverage,
         ))), patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(
             return_value=([], set(), {}),
         )), patch("concurrency.run_cpu_bound", side_effect=publish), \
         patch("services.epg_publication.read_publication", side_effect=[None, stored]), \
         patch("cache.get_cache", return_value=cache), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()) as emby:
        outcome = await reconcile_profiles(task, wait_for_sources=True)

    assert outcome.error == "CANCELLED"
    assert outcome.completed_degraded is True
    assert outcome.details["published_profile_ids"] == [1]
    assert outcome.details["pending_emby"] is True
    cache.invalidate_prefix.assert_not_called()
    client.update_channel.assert_not_awaited()
    emby.assert_not_awaited()


@pytest.mark.asyncio
async def test_channel_change_persists_emby_retry_across_stop_and_restart():
    scope = {"group_id": 9, "m3u_account_id": None}
    profile = _profile(
        channel_assignments=[{"channel_id": 10, "channel_name": "Arena 1"}],
        event_sync_config=_config([scope]),
    )
    first_channels = {
        10: {
            "id": 10,
            "name": "Arena 1",
            "channel_group_id": 7,
            "hidden_from_output": False,
            "epg_data_id": 900,
            "streams": [{"id": 501, "channel_group_id": 9}],
        },
    }
    restarted_channels = copy.deepcopy(first_channels)
    restarted_channels[10]["streams"] = []
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [{
            "profile_id": 1,
            "channel_id": 10,
            "current": {
                "title": "Main Event",
                "start": "2026-09-20T12:00:00+00:00",
            },
        }],
    }
    publication = PublicationResult(
        published_profile_ids=(1,),
        xmltv_by_scope={"profile:1": "<tv/>"},
    )
    stored = _publication("profile:1", pending=False)

    def update(scope_name, *, expected_revision, required_dispatcharr_hashes=None,
               confirmed_dispatcharr_hashes=None, pending_emby=None):
        assert scope_name == "profile:1"
        assert stored["revision"] == expected_revision
        if required_dispatcharr_hashes is not None:
            stored["state"]["delivery"]["required_dispatcharr_hashes"] = dict(
                required_dispatcharr_hashes
            )
        if confirmed_dispatcharr_hashes is not None:
            stored["state"]["delivery"]["confirmed_dispatcharr_hashes"] = dict(
                confirmed_dispatcharr_hashes
            )
        if pending_emby is not None:
            stored["state"]["delivery"]["pending_emby"] = pending_emby
        stored["revision"] += 1
        return stored["revision"]

    stopped_task = EventVisibilityTask()
    restarted_task = EventVisibilityTask()
    channel_updates = []

    async def update_channel(channel_id, values):
        channel_updates.append((channel_id, values))
        stopped_task._cancel_requested = True

    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[])
    client.update_channel = AsyncMock(side_effect=update_channel)
    emby = AsyncMock(return_value=True)

    with patch("tasks.event_visibility._load_profiles", return_value=([profile], [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(side_effect=[
             first_channels, restarted_channels,
         ])), patch("services.epg_programmes.prepare_profiles", new=AsyncMock(side_effect=[
             ([copy.deepcopy(profile)], copy.deepcopy(coverage)),
             ([copy.deepcopy(profile)], copy.deepcopy(coverage)),
         ])), patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(
             return_value=([], {(9, None)}, {}),
         )), patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=publication)), \
         patch("services.epg_publication.read_publication", side_effect=lambda name: stored), \
         patch("services.epg_publication.update_delivery", side_effect=update), \
         patch("cache.get_cache"), \
         patch("emby_client.request_guide_refresh", emby):
        stopped = await reconcile_profiles(stopped_task, wait_for_sources=True)
        assert stored["state"]["delivery"]["pending_emby"] is True
        restarted = await reconcile_profiles(restarted_task, wait_for_sources=True)

    assert stopped.error == "CANCELLED"
    assert stopped.completed_degraded is True
    assert stopped.details["pending_emby"] is True
    assert restarted.details["emby_request_outcome"] == "accepted"
    assert restarted.details["pending_emby"] is False
    assert stored["state"]["delivery"]["pending_emby"] is False
    assert channel_updates == [(10, {"streams": []})]
    emby.assert_awaited_once()
