"""Refresh polling and task entry points share confirmed workflow outcomes."""
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.epg_publication import PublicationResult, _config_hash
from task_scheduler import TaskResult
from tasks.dummy_epg_refresh import DummyEPGRefreshTask, wait_for_epg_source_refresh
from tasks.event_visibility import EventVisibilityTask


EXPIRES_AT = datetime.now(timezone.utc) + timedelta(hours=1)


def admitted_publication(profile, *, expires_at=EXPIRES_AT):
    return {
        "scope": f"profile:{profile['id']}",
        "xmltv": '<?xml version="1.0"?><tv></tv>',
        "revision": 3,
        "state": {
            "xmltv_hash": "a" * 64,
            "config_hash": _config_hash(profile),
            "delivery": {
                "guide_attempt": {
                    "attempt_id": "1" * 32,
                    "config_hash": _config_hash(profile),
                    "admitted_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
                    "expires_at": expires_at.isoformat(),
                    "stage": "preparing",
                },
                "pending_channels": {},
            },
        },
    }


@pytest.mark.asyncio
async def test_unchanged_timestamp_never_means_finished():
    client = MagicMock()
    client.get_epg_source = AsyncMock(return_value={"updated_at": "old", "status": "success"})
    client.refresh_epg_source = AsyncMock()
    assert not await wait_for_epg_source_refresh(
        client, 1, "Guide", expires_at=EXPIRES_AT, wait=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["processing", "error", "failed"])
async def test_changed_timestamp_requires_successful_state(status):
    client = MagicMock()
    client.get_epg_source = AsyncMock(side_effect=[
        {"updated_at": "old", "status": "success"},
        {"updated_at": "new", "status": status},
    ])
    client.refresh_epg_source = AsyncMock()
    assert not await wait_for_epg_source_refresh(
        client, 1, "Guide", expires_at=EXPIRES_AT, wait=False,
    )


@pytest.mark.asyncio
async def test_successful_transition_completes_without_timestamp():
    client = MagicMock()
    client.get_epg_source = AsyncMock(side_effect=[
        {"status": "success"}, {"status": "processing"}, {"status": "success"},
    ])
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()):
        assert await wait_for_epg_source_refresh(
            client, 1, "Guide", poll_interval=0, expires_at=EXPIRES_AT,
        )
    client.refresh_epg_source.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_cancelled_wait_does_not_trigger_refresh():
    client = MagicMock()
    client.refresh_epg_source = AsyncMock()
    assert not await wait_for_epg_source_refresh(
        client, 1, "Guide", expires_at=EXPIRES_AT, cancelled=lambda: True,
    )
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_waiting_for_existing_refresh_does_not_trigger_it_again():
    client = MagicMock()
    client.get_epg_source = AsyncMock(return_value={"updated_at": "new", "status": "success"})
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()):
        assert await wait_for_epg_source_refresh(
            client, 1, "Guide", expires_at=EXPIRES_AT,
            initial_source={"updated_at": "old"}, trigger=False,
        )
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_processing_state_can_complete_without_timestamps():
    client = MagicMock()
    client.get_epg_source = AsyncMock(return_value={"status": "success"})
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()):
        assert await wait_for_epg_source_refresh(
            client, 1, "Guide", expires_at=EXPIRES_AT,
            initial_source={"status": "processing"}, trigger=False,
        )
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_source_lifetime_never_triggers_refresh():
    client = MagicMock()
    client.get_epg_source = AsyncMock()
    client.refresh_epg_source = AsyncMock()

    completed = await wait_for_epg_source_refresh(
        client,
        1,
        "Guide",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    assert completed is False
    client.get_epg_source.assert_not_awaited()
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task, wait_for_sources",
    [(EventVisibilityTask(), False), (DummyEPGRefreshTask(), True)],
)
async def test_both_tasks_use_the_shared_reconciliation(task, wait_for_sources):
    expected = TaskResult(success=True, message="done")
    shared = AsyncMock(return_value=expected)

    with patch("tasks.event_visibility.reconcile_profiles", new=shared):
        result = await task.execute()

    assert result is expected
    shared.assert_awaited_once_with(task, wait_for_sources=wait_for_sources)


@pytest.mark.asyncio
async def test_publication_only_entry_returns_explicit_result_and_updates_cache_after_commit():
    profile = MagicMock()
    saved_profile = {"id": 7, "enabled": True, "epg_source_ids": []}
    profile.to_dict.return_value = saved_profile
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [profile]
    client = MagicMock()
    channels = {9: {"id": 9, "name": "Station", "channel_group_id": 4}}
    prepared = [{**saved_profile, "channel_assignments": []}]
    coverage = {"profiles": {"7": {
        "owned_channel_ids": [], "can_publish": True, "reason_codes": [],
    }}}
    publication = PublicationResult(
        published_profile_ids=(7,),
        xmltv_by_scope={"all": "<tv/>", "profile:7": "<tv/>"},
    )
    cache = MagicMock()

    with patch("database.get_session", return_value=session), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=(prepared, coverage))), \
         patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=publication)) as publish, \
         patch("cache.get_cache", return_value=cache):
        result = await DummyEPGRefreshTask()._regenerate_xmltv(
            publications={7: admitted_publication(saved_profile)},
        )

    assert result is publication
    publish.assert_awaited_once()
    cache.invalidate_prefix.assert_called_once_with("dummy_epg_xmltv")
    cache.set.assert_any_call("dummy_epg_xmltv_all", "<tv/>")
    cache.set.assert_any_call("dummy_epg_xmltv_7", "<tv/>")


@pytest.mark.asyncio
async def test_superseded_publication_does_not_update_cache():
    profile = MagicMock()
    saved_profile = {"id": 7, "enabled": True, "epg_source_ids": []}
    profile.to_dict.return_value = saved_profile
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [profile]
    publication = PublicationResult(superseded=True, reason_codes=("GUIDE_PUBLICATION_SUPERSEDED",))
    cache = MagicMock()

    with patch("database.get_session", return_value=session), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=MagicMock()), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([saved_profile], {
             "profiles": {"7": {
                 "owned_channel_ids": [], "can_publish": True, "reason_codes": [],
             }},
         }))), patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=publication)), \
         patch("cache.get_cache", return_value=cache):
        result = await DummyEPGRefreshTask()._regenerate_xmltv(
            publications={7: admitted_publication(saved_profile)},
        )

    assert result.superseded is True
    cache.invalidate_prefix.assert_not_called()
    cache.set.assert_not_called()


def test_dummy_task_has_no_duplicate_visibility_writer():
    assert not hasattr(DummyEPGRefreshTask, "_apply_empty_channel_visibility")


@pytest.mark.asyncio
async def test_regeneration_passes_each_stored_profile_expiry():
    saved = [
        {"id": 7, "enabled": True, "epg_source_ids": []},
        {"id": 8, "enabled": True, "epg_source_ids": []},
    ]
    rows = [MagicMock(to_dict=MagicMock(return_value=profile)) for profile in saved]
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = rows
    prepare = AsyncMock(side_effect=lambda profiles, *args, **kwargs: (
        profiles,
        {"profiles": {str(profiles[0]["id"]): {
            "owned_channel_ids": [], "can_publish": True, "reason_codes": [],
        }}},
    ))
    first_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    second_expiry = datetime.now(timezone.utc) + timedelta(hours=2)
    publish = AsyncMock(return_value=PublicationResult())

    with patch("database.get_session", return_value=session), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=MagicMock()), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=prepare), \
         patch("concurrency.run_cpu_bound", new=publish), \
         patch("cache.get_cache"):
        await DummyEPGRefreshTask()._regenerate_xmltv(
            publications={
                7: admitted_publication(saved[0], expires_at=first_expiry),
                8: admitted_publication(saved[1], expires_at=second_expiry),
            },
            wait_for_sources=False,
        )

    assert [call.kwargs["expires_at"] for call in prepare.await_args_list] == [
        first_expiry,
        second_expiry,
    ]
    assert all(
        call.kwargs["wait_for_sources"] is False
        and call.kwargs["recover_sources"] is True
        for call in prepare.await_args_list
    )
    expected = publish.await_args.kwargs["expected"]
    assert expected["profile:7"]["attempt_id"] == "1" * 32
    assert expected["profile:8"]["attempt_id"] == "1" * 32
    assert expected["profile:7"]["revision"] == 3
    assert expected["profile:8"]["revision"] == 3


@pytest.mark.asyncio
async def test_expired_profile_starts_no_preparation_while_eligible_profile_progresses():
    saved = [
        {"id": 7, "enabled": True, "epg_source_ids": [70]},
        {"id": 8, "enabled": True, "epg_source_ids": [80]},
    ]
    rows = [MagicMock(to_dict=MagicMock(return_value=profile)) for profile in saved]
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = rows
    prepare = AsyncMock(return_value=(
        [saved[1]],
        {"profiles": {"8": {
            "owned_channel_ids": [], "can_publish": True, "reason_codes": [],
        }}},
    ))
    expired = datetime.now(timezone.utc) - timedelta(seconds=1)
    eligible = datetime.now(timezone.utc) + timedelta(hours=1)

    with patch("database.get_session", return_value=session), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=MagicMock()), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=prepare), \
         patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=PublicationResult(
             published_profile_ids=(8,), unavailable_profile_ids=(7,),
         ))), patch("cache.get_cache"):
        result = await DummyEPGRefreshTask()._regenerate_xmltv(
            publications={
                7: admitted_publication(saved[0], expires_at=expired),
                8: admitted_publication(saved[1], expires_at=eligible),
            },
            wait_for_sources=False,
        )

    assert result.published_profile_ids == (8,)
    prepare.assert_awaited_once()
    assert prepare.await_args.args[0] == [saved[1]]
    assert prepare.await_args.kwargs["expires_at"] == eligible
