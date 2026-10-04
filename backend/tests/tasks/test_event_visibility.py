"""Focused checks for the configured profile reconciliation workflow."""
import asyncio
import copy
import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from services.epg_publication import PublicationResult
from stream_prober import StreamProber
from task_scheduler import ScheduleType
from tasks.event_visibility import (
    CHECK_INTERVAL_SECONDS,
    EventVisibilityTask,
    _await_preparation,
    _fetch_match_streams,
    _delivery_plan,
    _generated_scope,
    _guide_name,
    _owned_lock,
    _plan_profile,
    _slot_key,
    reconcile_profiles,
)

ATTEMPT_ADMITTED_AT = datetime.now(timezone.utc)
ATTEMPT_EXPIRES_AT = ATTEMPT_ADMITTED_AT + timedelta(hours=24)


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


def _publication(scope, *, revision=1, pending=True, confirmed=None, channels=None, attempt=True):
    value_hash = hashlib.sha256("<tv/>".encode()).hexdigest()
    guide_attempt = None
    if attempt and scope.startswith("profile:"):
        guide_attempt = {
            "attempt_id": "1" * 32,
            "config_hash": "b" * 64,
            "admitted_at": ATTEMPT_ADMITTED_AT.isoformat(),
            "expires_at": ATTEMPT_EXPIRES_AT.isoformat(),
            "stage": "preparing",
        }
    return {
        "scope": scope,
        "xmltv": "<tv/>",
        "revision": revision,
        "state": {
            "published": True,
            "published_at": "2026-09-20T12:00:00+00:00",
            "xmltv_hash": value_hash,
            "config_hash": "b" * 64,
            "channels": list(channels or []),
            "delivery": {
                "required_dispatcharr_hashes": {},
                "confirmed_dispatcharr_hashes": dict(confirmed or {}),
                "pending_emby": pending,
                "guide_attempt": guide_attempt,
                "source_refreshes": {},
                "pending_channels": {},
            },
        },
    }


def _admit(publications):
    def begin(scope, *, expected_revision, expected_hash, profile, now, pending_channels=None):
        row = publications.get(scope)
        if row is None:
            assert expected_revision == 0
            assert expected_hash is None
            row = _publication(scope)
            row["state"]["published"] = False
            publications[scope] = row
        else:
            assert row["revision"] == expected_revision
            assert row["state"]["xmltv_hash"] == expected_hash
        assert scope == f"profile:{profile['id']}"
        assert now.tzinfo is not None
        assert pending_channels in (None, {})
        return row

    return begin


def _publication_run(result):
    async def run(function, *args, **kwargs):
        assert function.__name__ == "publish_profiles"
        expected = kwargs["expected"]
        assert expected
        assert all(
            set(claim) == {"revision", "xmltv_hash", "config_hash", "attempt_id"}
            and claim["revision"] > 0
            and claim["attempt_id"]
            for claim in expected.values()
        )
        return result

    return AsyncMock(side_effect=run)


def test_default_schedule_checks_every_five_minutes():
    task = EventVisibilityTask()

    assert task.schedule_config.schedule_type is ScheduleType.INTERVAL
    assert task.schedule_config.interval_seconds == CHECK_INTERVAL_SECONDS == 300
    assert task.schedule_config.timezone == "America/Chicago"


@pytest.mark.asyncio
async def test_default_execution_does_not_wait():
    task = EventVisibilityTask()
    outcome = MagicMock()

    with patch(
        "tasks.event_visibility.reconcile_profiles",
        new=AsyncMock(return_value=outcome),
    ) as reconcile:
        assert await task.execute() is outcome

    reconcile.assert_awaited_once_with(task, wait_for_sources=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("wait_for_sources", [False, True])
async def test_reconciliation_requests_recovery_without_changing_wait_mode(wait_for_sources):
    profile = _profile()
    task = EventVisibilityTask()
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [],
    }

    async def finish_preparation(*args, **kwargs):
        task._cancel_requested = True
        return [copy.deepcopy(profile)], coverage

    prepare = AsyncMock(side_effect=finish_preparation)
    with patch("tasks.event_visibility._load_profiles", return_value=([profile], [])), \
         patch("tasks.event_visibility.get_client", return_value=MagicMock()), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=prepare), \
         patch("services.epg_publication.read_publication", return_value=None), \
         patch("services.epg_publication.begin_delivery", side_effect=_admit({})):
        outcome = await reconcile_profiles(task, wait_for_sources=wait_for_sources)

    assert outcome.error == "CANCELLED"
    assert prepare.await_args.kwargs["wait_for_sources"] is wait_for_sources
    assert prepare.await_args.kwargs["recover_sources"] is True


@pytest.mark.asyncio
async def test_reconciliation_prepares_profiles_with_their_stored_expiries():
    profiles = [_profile(), {**_profile(), "id": 2, "name": "Arena Two"}]
    first_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    second_expiry = datetime.now(timezone.utc) + timedelta(hours=2)
    publications = {
        "profile:1": _publication("profile:1"),
        "profile:2": _publication("profile:2"),
    }
    publications["profile:1"]["state"]["delivery"]["guide_attempt"]["expires_at"] = first_expiry.isoformat()
    publications["profile:2"]["state"]["delivery"]["guide_attempt"]["expires_at"] = second_expiry.isoformat()
    task = EventVisibilityTask()

    async def prepare(selected, *args, **kwargs):
        if selected[0]["id"] == 2:
            task._cancel_requested = True
        profile_id = selected[0]["id"]
        return selected, {"profiles": {str(profile_id): {
            "profile_id": profile_id,
            "owned_channel_ids": [],
            "can_publish": True,
            "reason_codes": [],
        }}, "channels": []}

    preparation = AsyncMock(side_effect=prepare)
    with patch("tasks.event_visibility._load_profiles", return_value=(profiles, [])), \
         patch("tasks.event_visibility.get_client", return_value=MagicMock()), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=preparation), \
         patch("services.epg_publication.read_publication", side_effect=publications.get), \
         patch("services.epg_publication.begin_delivery", side_effect=_admit(publications)):
        outcome = await reconcile_profiles(task, wait_for_sources=False)

    assert outcome.error == "CANCELLED"
    assert [call.kwargs["expires_at"] for call in preparation.await_args_list] == [
        first_expiry,
        second_expiry,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["rule", "profile"])
async def test_reconciliation_allows_runtime_updates(change):
    before = ATTEMPT_ADMITTED_AT - timedelta(minutes=1)
    profile = _profile(
        channel_assignments=[],
        created_at=before.isoformat(),
        updated_at=before.isoformat(),
        last_generated_at=before.isoformat(),
    )
    rule = SimpleNamespace(
        id=3, enabled=True, event_sync_config={"secondary": []},
        last_run_at=before, match_count=1, updated_at=before,
    )
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [],
    }
    publication = PublicationResult(
        published_profile_ids=(1,),
        xmltv_by_scope={"profile:1": "<tv/>"},
    )
    stored = _publication("profile:1", pending=False)
    publications = {"profile:1": stored}
    task = EventVisibilityTask()
    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[])
    client.update_channel = AsyncMock()

    async def prepare(*args, **kwargs):
        assert kwargs["expires_at"] == ATTEMPT_EXPIRES_AT
        if change == "rule":
            rule.last_run_at = ATTEMPT_ADMITTED_AT
            rule.match_count = 0
            rule.updated_at = ATTEMPT_ADMITTED_AT
        else:
            profile["last_generated_at"] = ATTEMPT_ADMITTED_AT.isoformat()
            profile["created_at"] = ATTEMPT_ADMITTED_AT.isoformat()
            profile["updated_at"] = ATTEMPT_ADMITTED_AT.isoformat()
        return [copy.deepcopy(profile)], coverage

    async def publish(*args, **kwargs):
        assert kwargs["expected"]["profile:1"]["attempt_id"] == "1" * 32
        task._cancel_requested = True
        return publication

    preparation = AsyncMock(side_effect=prepare)
    publishing = AsyncMock(side_effect=publish)
    with patch("tasks.event_visibility._load_profiles", return_value=([profile], [rule])) as load, \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=preparation), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}))), \
         patch("concurrency.run_cpu_bound", new=publishing), \
         patch("services.epg_publication.read_publication", side_effect=publications.get), \
         patch("services.epg_publication.begin_delivery", side_effect=_admit(publications)), \
         patch("cache.get_cache"), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()) as emby:
        outcome = await reconcile_profiles(task, wait_for_sources=False)

    assert preparation.await_count == 1
    assert load.call_count == 2
    publishing.assert_awaited_once()
    assert outcome.error == "CANCELLED"
    assert outcome.details["published_profile_ids"] == [1]
    assert "GUIDE_SOURCES_PENDING" not in outcome.details["reason_codes"]
    client.update_channel.assert_not_awaited()
    emby.assert_not_awaited()
    if change == "rule":
        assert rule.last_run_at == ATTEMPT_ADMITTED_AT
        assert rule.match_count == 0


@pytest.mark.asyncio
async def test_reconciliation_copies_only_present_profile_mapping_checks():
    profiles = [
        _profile(id=7, name="Arena Seven", channel_assignments=[]),
        _profile(id=12, name="Arena Twelve", channel_assignments=[]),
    ]
    mapping = {
        "captured_at": "2026-10-04T19:00:00+00:00",
        "counts": {
            "linked": 1,
            "pending": 1,
            "active_cached": 1,
            "active_uncached": 0,
            "error_cached": 0,
            "error_uncached": 0,
            "ready_value": 0,
            "unresolved": 0,
        },
        "links": [{
            "channel_id": 70,
            "link_id": 700,
            "active": True,
            "error": False,
            "value_present": True,
            "cached": True,
            "cached_row_id": 700,
            "cached_row_matches_link": True,
            "cached_source_id": 50,
            "cached_source_kind": "external",
            "checked_age_seconds": 61.0,
            "load_expires_at": ATTEMPT_EXPIRES_AT.isoformat(),
        }],
    }
    publications = {
        f"profile:{profile['id']}": _publication(f"profile:{profile['id']}", pending=False)
        for profile in profiles
    }
    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[])
    client.update_channel = AsyncMock()
    task = EventVisibilityTask()

    async def prepare(selected, *args, **kwargs):
        profile_id = selected[0]["id"]
        record = {"can_publish": True, "reason_codes": []}
        if profile_id == 7:
            record["mapping_checks"] = mapping
        return selected, {"profiles": {str(profile_id): record}, "channels": []}

    async def publish(*args, **kwargs):
        task._cancel_requested = True
        return PublicationResult(
            published_profile_ids=(7, 12),
            xmltv_by_scope={"profile:7": "<tv/>", "profile:12": "<tv/>"},
        )

    with patch("tasks.event_visibility._load_profiles", return_value=(profiles, [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(side_effect=prepare)), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}))), \
         patch("concurrency.run_cpu_bound", new=AsyncMock(side_effect=publish)), \
         patch("services.epg_publication.read_publication", side_effect=publications.get), \
         patch("services.epg_publication.begin_delivery", side_effect=_admit(publications)), \
         patch("cache.get_cache"), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()):
        outcome = await reconcile_profiles(task, wait_for_sources=False)

    assert outcome.error == "CANCELLED"
    assert outcome.details["mapping_checks"] == {"7": mapping}
    assert "12" not in outcome.details["mapping_checks"]
    assert outcome.details["mapping_checks"]["7"] is not mapping
    mapping["counts"]["linked"] = 99
    assert outcome.details["mapping_checks"]["7"]["counts"]["linked"] == 1
    assert outcome.details["mapping_checks"]["7"]["captured_at"] == "2026-10-04T19:00:00+00:00"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["pattern", "timezone", "secondary"])
async def test_reconciliation_rejects_changed_config(change):
    profile = _profile(channel_assignments=[])
    rule = SimpleNamespace(
        id=3, enabled=True, event_sync_config={"secondary": []},
        updated_at=ATTEMPT_ADMITTED_AT,
    )
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [],
    }
    stored = _publication("profile:1", pending=False)
    publications = {"profile:1": stored}
    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[])
    client.update_channel = AsyncMock()
    client.refresh_epg_source = AsyncMock()

    async def prepare(*args, **kwargs):
        assert kwargs["expires_at"] == ATTEMPT_EXPIRES_AT
        if change == "pattern":
            profile["title_pattern"] = rf"^(?P<title>Event {preparation.await_count})$"
        elif change == "timezone":
            profile["event_timezone"] = (
                "America/Chicago" if preparation.await_count == 1 else "US/Eastern"
            )
        else:
            rule.event_sync_config["secondary"].append({
                "group_id": 5 + preparation.await_count,
                "m3u_account_id": 2,
            })
        return [copy.deepcopy(profile)], coverage

    preparation = AsyncMock(side_effect=prepare)
    publishing = _publication_run(PublicationResult())
    with patch("tasks.event_visibility._load_profiles", return_value=([profile], [rule])) as load, \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=preparation), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}))), \
         patch("concurrency.run_cpu_bound", new=publishing), \
         patch("services.epg_publication.read_publication", side_effect=publications.get), \
         patch("services.epg_publication.begin_delivery", side_effect=_admit(publications)), \
         patch("emby_client.request_guide_refresh", new=AsyncMock()) as emby:
        outcome = await reconcile_profiles(EventVisibilityTask(), wait_for_sources=False)

    assert preparation.await_count == 2
    assert load.call_count == 4
    assert outcome.success is False
    assert outcome.error == "GUIDE_SOURCES_PENDING"
    assert outcome.details["reason_codes"] == ["GUIDE_SOURCES_PENDING"]
    assert outcome.details["published_profile_ids"] == []
    publishing.assert_not_awaited()
    client.update_channel.assert_not_awaited()
    client.refresh_epg_source.assert_not_awaited()
    emby.assert_not_awaited()


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
    value_hash = hashlib.sha256("<tv/>".encode()).hexdigest()
    row = _publication(
        "profile:1",
        confirmed={"46": value_hash},
    )

    required, confirmed, pending = _delivery_plan(row, [{"id": 46}])

    assert required == {"46": value_hash}
    assert confirmed == required
    assert pending == {}


def test_changed_hash_remains_pending_until_confirmed():
    row = _publication(
        "profile:1",
        confirmed={"46": "b" * 64},
    )

    required, confirmed, pending = _delivery_plan(row, [{"id": 46}])

    value_hash = hashlib.sha256("<tv/>".encode()).hexdigest()
    assert required == {"46": value_hash}
    assert confirmed == {}
    assert pending == {46: value_hash}


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


@pytest.mark.asyncio
async def test_fetch_match_streams_applies_the_limit_to_each_scope(monkeypatch):
    monkeypatch.setattr("tasks.event_visibility.MAX_MATCH_STREAMS", 2)
    client = MagicMock()
    client._channel_group_name_for_id = AsyncMock(side_effect=["One", "Two"])
    client.get_streams = AsyncMock(side_effect=[
        {
            "results": [
                {"id": 11, "name": "One A"},
                {"id": 12, "name": "One B"},
            ],
            "next": None,
        },
        {
            "results": [
                {"id": 21, "name": "Two A"},
                {"id": 22, "name": "Two B"},
            ],
            "next": None,
        },
    ])

    streams, complete, failures = await _fetch_match_streams(
        client,
        [
            {"group_id": 1, "m3u_account_id": None},
            {"group_id": 2, "m3u_account_id": None},
        ],
    )

    assert [stream.stream_id for stream in streams] == [11, 12, 21, 22]
    assert complete == {(1, None), (2, None)}
    assert failures == {}


@pytest.mark.asyncio
async def test_fetch_match_streams_discards_a_scope_after_a_later_page_fails():
    client = MagicMock()
    client._channel_group_name_for_id = AsyncMock(side_effect=["One", "Two"])
    client.get_streams = AsyncMock(side_effect=[
        {"results": [{"id": 11, "name": "One A"}], "next": "page-2"},
        RuntimeError("later page unavailable"),
        {"results": [{"id": 22, "name": "Two A"}], "next": None},
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


@pytest.mark.asyncio
async def test_fetch_match_streams_discards_an_oversized_scope_and_continues(monkeypatch):
    monkeypatch.setattr("tasks.event_visibility.MAX_MATCH_STREAMS", 2)
    client = MagicMock()
    client._channel_group_name_for_id = AsyncMock(side_effect=["One", "Two"])
    client.get_streams = AsyncMock(side_effect=[
        {
            "results": [
                {"id": 11, "name": "One A"},
                {"id": 12, "name": "One B"},
                {"id": 13, "name": "One C"},
            ],
            "next": None,
        },
        {"results": [{"id": 22, "name": "Two A"}], "next": None},
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
    assert failures == {(1, None): "ValueError"}


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


@pytest.mark.parametrize("source", ["current", "retained", "interval"])
def test_placeholder_activity_does_not_keep_complete_owned_channel_active(source):
    now = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
    placeholder = {
        "channel_id": 10,
        "title": "Programming unavailable",
        "start": (now - timedelta(minutes=30)).isoformat(),
        "stop": (now + timedelta(minutes=30)).isoformat(),
    }
    current = placeholder if source == "current" else None
    retained = None
    intervals = {}
    if source == "retained":
        retained = _publication(
            "profile:1",
            channels=[{"channel_id": 10, "events": [placeholder]}],
        )
    elif source == "interval":
        intervals = {10: [placeholder]}
    profile = _profile(
        epg_source_ids=[100],
        event_intervals=intervals,
        channel_assignments=[{"channel_id": 10, "channel_name": "Arena 1"}],
    )
    channel = {
        "id": 10,
        "name": "Arena 1",
        "channel_group_id": 7,
        "hidden_from_output": False,
        "epg_data": {"id": 900, "epg_source": 46, "tvg_id": "custom-10"},
        "streams": [],
    }
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [{"profile_id": 1, "channel_id": 10, "current": current}],
    }

    result = _plan_profile(
        profile,
        profile["event_sync_config"],
        {10: channel},
        coverage,
        [],
        set(),
        retained,
        now,
        {46},
    )

    assert result["states"] == {10: "idle"}
    assert result["desired"] == {10: []}
    assert result["profile"]["event_intervals"] == {}


@pytest.mark.parametrize(
    "complete, guide_row",
    [
        (False, {"id": 900, "epg_source": 46, "tvg_id": "custom-10"}),
        (True, None),
        (True, {"id": 900, "epg_source": 99, "tvg_id": "custom-10"}),
        (True, {"id": 900, "epg_source": 46, "tvg_id": "foreign-10"}),
    ],
)
def test_placeholder_current_keeps_incomplete_or_foreign_ownership_unknown(complete, guide_row):
    now = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
    scope = {"group_id": 9, "m3u_account_id": None}
    config = _config([scope])
    profile = _profile(
        epg_source_ids=[100],
        event_sync_config=config,
        channel_assignments=[{"channel_id": 10, "channel_name": "Arena 1"}],
    )
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
        "channels": [{
            "profile_id": 1,
            "channel_id": 10,
            "current": {
                "title": "Programming unavailable",
                "start": (now - timedelta(minutes=30)).isoformat(),
                "stop": (now + timedelta(minutes=30)).isoformat(),
            },
        }],
    }

    result = _plan_profile(
        profile,
        config,
        {10: channel},
        coverage,
        [],
        {(9, None)} if complete else set(),
        None,
        now,
        {46},
    )

    assert result["states"] == {10: "unknown"}
    assert result["desired"] == {}


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


def test_source_free_no_slot_interval_stays_active_and_renders_programme():
    from dummy_epg_engine import generate_xmltv
    from xml.etree import ElementTree as ET

    now = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
    interval = {
        "channel_id": 10,
        "title": "Falcons vs Wolves",
        "start": (now - timedelta(minutes=10)).isoformat(),
        "stop": (now + timedelta(hours=2)).isoformat(),
    }
    config = _config()
    config["slot_patterns"] = []
    profile = _profile(
        epg_source_ids=[],
        event_sync_config=config,
        event_intervals={10: [interval]},
        channel_assignments=[{"channel_id": 10, "channel_name": "Arena"}],
    )
    channels = {
        10: {
            "id": 10,
            "name": "Arena",
            "channel_group_id": 7,
            "hidden_from_output": True,
            "streams": [],
        },
    }
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [{"profile_id": 1, "channel_id": 10, "current": None}],
    }

    result = _plan_profile(
        profile,
        config,
        channels,
        coverage,
        [],
        set(),
        None,
        now,
    )
    document = ET.fromstring(generate_xmltv([result["profile"]], channels))
    programme = document.find("programme")

    assert result["states"] == {10: "active"}
    assert result["profile"]["event_intervals"] == {10: [interval]}
    assert programme is not None
    assert programme.get("channel") == "custom-10"
    assert programme.findtext("title") == "Falcons vs Wolves"


def test_equal_start_slot_identities_remain_ambiguous():
    from services.event_sync_resolver import SecondaryStream

    now = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
    scope = {"group_id": 9, "m3u_account_id": None}
    config = _config([scope])
    config["slot_patterns"][0]["bootstrap"] = True
    profile = _profile(event_sync_config=config)
    channels = {
        10: {
            "id": 10,
            "name": "Arena 1",
            "channel_group_id": 7,
            "hidden_from_output": True,
            "streams": [],
        },
    }
    coverage = {
        "profiles": {"1": {"can_publish": True, "reason_codes": []}},
        "channels": [{"profile_id": 1, "channel_id": 10, "current": None}],
    }
    streams = [
        SecondaryStream(name="LIVE 1 Falcons", group_id=9, stream_id=90),
        SecondaryStream(name="LIVE 1 Wolves", group_id=9, stream_id=91),
    ]

    def parsed(stream_name, *args, **kwargs):
        return SimpleNamespace(
            start=now - timedelta(minutes=10),
            title=stream_name.rsplit(" ", 1)[-1],
            matched_pattern="test-pattern",
        )

    with patch("services.event_sync_matcher.parse_event_name", side_effect=parsed):
        result = _plan_profile(
            profile,
            config,
            channels,
            coverage,
            streams,
            {(9, None)},
            None,
            now,
        )

    assert result["states"] == {10: "unknown"}
    assert result["desired"] == {}
    assert result["observations"] == []


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
@pytest.mark.parametrize("flow_case", ["positive", "unknown", "dark", "incomplete"])
async def test_reconciliation_orders_hide_import_link_reveal_and_emby(flow_case):
    master_url = "https://media.example/master.m3u8"
    media_url = "https://media.example/media.m3u8"
    segment_url = "https://media.example/segment.ts"
    unknown_url = "https://media.example/unknown.m3u8"
    routes = {
        master_url: (
            b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nmedia.m3u8\n",
            "application/vnd.apple.mpegurl",
        ),
        media_url: (
            b"#EXTM3U\n#EXTINF:1.0,\nsegment.ts\n",
            "application/vnd.apple.mpegurl",
        ),
        segment_url: (b"event-media", "video/mp2t"),
        unknown_url: (b"#EXTM3U\n", "application/vnd.apple.mpegurl"),
    }

    @asynccontextmanager
    async def request(url, **_kwargs):
        body, content_type = routes[url]
        response = httpx.Response(
            200,
            request=httpx.Request("GET", url),
            content=body,
            headers={"Content-Type": content_type},
        )
        response.extensions["ssrf_logical_url"] = url
        try:
            yield response
        finally:
            await response.aclose()

    prober = StreamProber(
        client=MagicMock(),
        bitrate_sample_duration=1,
        black_screen_detection_enabled=False,
    )
    with patch("stream_prober._probe_stream_request", request):
        measured = await prober._measure_stream_bitrate(master_url)
        unknown_measurement = await prober._measure_stream_bitrate(unknown_url)

    assert measured == len(b"event-media") * 8
    assert unknown_measurement is None

    stream_scope = {"group_id": 5, "m3u_account_id": None}
    profile = _profile(event_sync_config=_config([stream_scope]))
    prepared = copy.deepcopy(profile)
    event_start = datetime.now(timezone.utc) - timedelta(minutes=5)
    event_stop = event_start + timedelta(hours=3)
    matched_stream = SimpleNamespace(
        stream_id=501,
        name=_guide_name({"title": "Main Event", "start": event_start.isoformat()}, "UTC"),
        group_id=5,
        provider=None,
        provider_id=None,
        name_seen_before_today=None,
        is_stale=False,
    )
    checked = datetime.now(timezone.utc)
    stat = {
        "stream_name": matched_stream.name,
        "probe_status": "success",
        "measured_bitrate": (
            unknown_measurement if flow_case == "unknown" else measured
        ),
        "last_probed": checked.isoformat(),
    }
    if flow_case != "incomplete":
        stat.update({
            "is_black_screen": flow_case == "dark",
            "black_screen_checked_at": checked.isoformat(),
        })
    load_stats = AsyncMock(return_value={501: stat})
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
                        "start": event_start.isoformat(),
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
        "profile:1": _publication("profile:1", channels=[{
            "channel_id": 10,
            "events": [{
                "title": "Main Event",
                "start": event_start.isoformat(),
                "stop": event_stop.isoformat(),
            }],
        }]),
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
        channels[channel_id].update(values)

    async def guide_rows(**kwargs):
        order.append(("rows", kwargs["epg_source"]))
        return [{"id": 900, "tvg_id": "custom-10", "epg_source": 46}]

    client.update_channel = AsyncMock(side_effect=update_channel)
    client.get_channel = AsyncMock(side_effect=lambda channel_id: copy.deepcopy(channels[channel_id]))
    client.get_epg_data = AsyncMock(side_effect=guide_rows)
    client.get_epg_grid = AsyncMock(return_value=[{
        "tvg_id": "custom-10",
        "title": "Main Event",
        "start": event_start.isoformat(),
        "stop": event_stop.isoformat(),
    }])
    client.refresh_epg_source = AsyncMock()
    task = EventVisibilityTask()

    async def import_source(*args, **kwargs):
        order.append(("import", args[1]))
        return True

    async def emby():
        order.append(("emby",))
        return None

    with patch("tasks.event_visibility._load_profiles", return_value=([profile], [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([prepared], coverage))), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=(
             [matched_stream], {(5, None)}, {},
         ))), \
         patch("services.event_sync_stream_health._load_stats", new=load_stats), \
         patch("services.event_sync_stream_health._probe_and_collect_failures", new=AsyncMock()), \
         patch("concurrency.run_cpu_bound", new=_publication_run(result)), \
         patch("services.epg_publication.read_publication", side_effect=read), \
         patch("services.epg_publication.begin_delivery", side_effect=_admit(publications)), \
         patch("services.epg_publication.update_delivery", side_effect=update), \
         patch("cache.get_cache"), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", side_effect=import_source), \
         patch("emby_client.request_guide_refresh", side_effect=emby):
        outcome = await reconcile_profiles(task, wait_for_sources=True)

    assert outcome.success is True
    assert outcome.completed_degraded is False
    assert load_stats.await_count >= 1
    if flow_case == "positive":
        assert outcome.details == {
            "configured_profile_count": 1,
            "published_profile_ids": [1],
            "retained_profile_ids": [],
            "unavailable_profile_ids": [],
            "publication_times": {"1": "2026-09-20T12:00:00+00:00"},
            "source_reason_codes": {"1": []},
            "mapping_checks": {},
            "idle_channel_count": 1,
            "active_channel_count": 1,
            "unknown_channel_count": 0,
            "stream_updated_channel_ids": [20],
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
            ("channel", 20, ("streams",)),
            ("emby",),
        ]
    else:
        assert channels[10]["hidden_from_output"] is True
        assert 10 not in outcome.details["revealed_channel_ids"]
        assert (
            "channel", 10, ("hidden_from_output",)
        ) not in order


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

    publications = {"profile:1": stored}

    def update(scope, *, expected_revision, expected_hash=None,
               expected_config_hash=None, expected_attempt_id=None,
               required_dispatcharr_hashes=None, confirmed_dispatcharr_hashes=None,
               pending_emby=None, source_refreshes=None):
        assert stored["revision"] == expected_revision
        if expected_hash is not None:
            assert stored["state"]["xmltv_hash"] == expected_hash
        if expected_config_hash is not None:
            assert stored["state"]["config_hash"] == expected_config_hash
        if expected_attempt_id is not None:
            assert stored["state"]["delivery"]["guide_attempt"]["attempt_id"] == expected_attempt_id
        if required_dispatcharr_hashes is not None:
            stored["state"]["delivery"]["required_dispatcharr_hashes"] = dict(required_dispatcharr_hashes)
        if confirmed_dispatcharr_hashes is not None:
            stored["state"]["delivery"]["confirmed_dispatcharr_hashes"] = dict(confirmed_dispatcharr_hashes)
        if pending_emby is not None:
            stored["state"]["delivery"]["pending_emby"] = pending_emby
        if source_refreshes is not None:
            stored["state"]["delivery"]["source_refreshes"] = copy.deepcopy(source_refreshes)
        stored["revision"] += 1
        return stored["revision"]

    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[{
        "id": 46,
        "name": "Generated profile",
        "url": "http://ecm/api/dummy-epg/xmltv/1",
        "is_active": True,
    }])
    client.refresh_epg_source = AsyncMock()

    async def observe(*args, **kwargs):
        assert args[1] == 46
        assert kwargs["wait"] is False
        assert kwargs["expires_at"].isoformat() == ATTEMPT_EXPIRES_AT.isoformat()
        assert kwargs["progress"]["attempt_id"] == "1" * 32
        return False

    task = EventVisibilityTask()
    with patch("tasks.event_visibility._load_profiles", return_value=([profile], [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([copy.deepcopy(profile)], coverage))), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}))), \
         patch("concurrency.run_cpu_bound", new=_publication_run(publication)), \
         patch("services.epg_publication.read_publication", side_effect=lambda scope: stored), \
         patch("services.epg_publication.begin_delivery", side_effect=_admit(publications)), \
         patch("services.epg_publication.update_delivery", side_effect=update), \
         patch("cache.get_cache"), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", side_effect=observe), \
         patch("emby_client.request_guide_refresh", new=AsyncMock(return_value=None)):
        outcome = await reconcile_profiles(task, wait_for_sources=False)

    assert outcome.success is False
    assert outcome.completed_degraded is True
    assert outcome.error == "GUIDE_IMPORT_PENDING"
    assert outcome.details["pending_source_hashes"] == {
        "profile:1:46": stored["state"]["xmltv_hash"],
    }
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
         patch("services.epg_publication.begin_delivery", side_effect=_admit({})), \
         patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}))), \
         patch("concurrency.run_cpu_bound", new=AsyncMock()) as publish:
        outcome = await reconcile_profiles(task, wait_for_sources=False)

    assert outcome.success is False
    assert outcome.error == "CANCELLED"
    publish.assert_not_awaited()
    client.update_channel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [
    [{"id": 10}, {"id": 10}],
    {"results": [{"id": 10}], "count": 2, "next": None},
    [{"id": None}],
])
async def test_reconciliation_reports_the_failed_preparation_stage(response):
    profile = _profile()
    client = MagicMock()
    client.get_channels = AsyncMock(return_value=response)
    client.update_channel = AsyncMock()
    client.refresh_epg_source = AsyncMock()
    task = EventVisibilityTask()

    with patch("tasks.event_visibility._load_profiles", return_value=([profile], [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
             patch("services.epg_publication.read_publication", return_value=None), \
             patch("services.epg_publication.begin_delivery", side_effect=_admit({})), \
         patch("services.epg_publication.publish_profiles") as publish, \
         patch("emby_client.request_guide_refresh", new_callable=AsyncMock) as emby:
        outcome = await reconcile_profiles(task, wait_for_sources=False)

    assert outcome.success is False
    assert outcome.error == "GUIDE_UNAVAILABLE"
    assert outcome.details["configured_profile_count"] == 1
    assert outcome.details["failure_stage"] == "channels"
    assert outcome.details["failure_type"] == "ValueError"
    publish.assert_not_called()
    client.update_channel.assert_not_awaited()
    client.refresh_epg_source.assert_not_awaited()
    emby.assert_not_awaited()


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
async def test_import_wait_releases_publication_ownership():
    lock = asyncio.Lock()
    import_started = asyncio.Event()
    import_release = asyncio.Event()
    scheduled_finished = asyncio.Event()

    async def manual_refresh():
        async with _owned_lock(lock) as release:
            release()
            import_started.set()
            await import_release.wait()

    async def scheduled_refresh():
        await import_started.wait()
        async with lock:
            scheduled_finished.set()

    manual = asyncio.create_task(manual_refresh())
    scheduled = asyncio.create_task(scheduled_refresh())
    await asyncio.wait_for(scheduled_finished.wait(), timeout=1)
    assert manual.done() is False
    import_release.set()
    await asyncio.gather(manual, scheduled)


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
    publications = {"profile:1": stored}
    task = EventVisibilityTask()
    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[])
    client.update_channel = AsyncMock()
    cache = MagicMock()

    async def publish(*args, **kwargs):
        assert kwargs["expected"]
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
             patch("services.epg_publication.begin_delivery", side_effect=_admit({})), \
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
    publications = {"profile:1": stored}

    def update(scope_name, *, expected_revision, expected_hash=None,
               expected_config_hash=None, expected_attempt_id=None,
               required_dispatcharr_hashes=None, confirmed_dispatcharr_hashes=None,
               pending_emby=None, source_refreshes=None):
        assert scope_name == "profile:1"
        assert stored["revision"] == expected_revision
        if expected_hash is not None:
            assert stored["state"]["xmltv_hash"] == expected_hash
        if expected_config_hash is not None:
            assert stored["state"]["config_hash"] == expected_config_hash
        if expected_attempt_id is not None:
            assert stored["state"]["delivery"]["guide_attempt"]["attempt_id"] == expected_attempt_id
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
        if source_refreshes is not None:
            stored["state"]["delivery"]["source_refreshes"] = copy.deepcopy(source_refreshes)
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
    client.get_channel = AsyncMock(side_effect=[
        copy.deepcopy(first_channels[10]),
        copy.deepcopy(restarted_channels[10]),
        copy.deepcopy(restarted_channels[10]),
    ])
    emby = AsyncMock(return_value=True)

    with patch("tasks.event_visibility._load_profiles", return_value=([profile], [])), \
         patch("tasks.event_visibility.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(side_effect=[
             *[first_channels] * 5, restarted_channels,
         ])), patch("services.epg_programmes.prepare_profiles", new=AsyncMock(side_effect=[
             *[
                 ([copy.deepcopy(profile)], copy.deepcopy(coverage))
                 for _ in range(5)
             ],
             ([copy.deepcopy(profile)], copy.deepcopy(coverage)),
         ])), patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(
             return_value=([], {(9, None)}, {}),
             )), patch("concurrency.run_cpu_bound", new=_publication_run(publication)), \
             patch("services.epg_publication.read_publication", side_effect=lambda name: stored), \
             patch("services.epg_publication.begin_delivery", side_effect=_admit(publications)), \
             patch("services.epg_publication.update_delivery", side_effect=update), \
         patch("cache.get_cache"), \
         patch("emby_client.request_guide_refresh", emby):
        for stage in ("allocated", "failed", "expired", "allocation_unknown"):
            stored["state"]["delivery"]["pending_channels"] = {
                "arena:main-event": {"channel_id": 10, "stage": stage},
            }
            await reconcile_profiles(EventVisibilityTask(), wait_for_sources=True)
            assert channel_updates == []
        stored["state"]["delivery"]["pending_channels"] = {}
        stopped = await reconcile_profiles(stopped_task, wait_for_sources=True)
        assert stored["state"]["delivery"]["pending_emby"] is True
        restarted = await reconcile_profiles(restarted_task, wait_for_sources=True)

    assert stopped.error == "CANCELLED"
    assert stopped.completed_degraded is True
    assert stopped.details["pending_emby"] is True
    assert restarted.details["emby_request_outcome"] == "accepted"
    assert restarted.details["pending_emby"] is False
    assert stored["state"]["delivery"]["pending_emby"] is False
    assert channel_updates == [
        (10, {"streams": []}),
        (10, {"hidden_from_output": True}),
    ]
    emby.assert_awaited_once()
