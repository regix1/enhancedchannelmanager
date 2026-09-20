"""Refresh polling and task entry points share confirmed workflow outcomes."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.epg_publication import PublicationResult
from task_scheduler import TaskResult
from tasks.dummy_epg_refresh import DummyEPGRefreshTask, wait_for_epg_source_refresh
from tasks.event_visibility import EventVisibilityTask


@pytest.mark.asyncio
async def test_unchanged_timestamp_never_means_finished():
    client = MagicMock()
    client.get_epg_source = AsyncMock(return_value={"updated_at": "old", "status": "success"})
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()), \
         patch("tasks.dummy_epg_refresh.time.monotonic", side_effect=[0, 0, 31, 61]):
        assert not await wait_for_epg_source_refresh(client, 1, "Guide", max_wait=60)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["processing", "error", "failed"])
async def test_changed_timestamp_requires_successful_state(status):
    client = MagicMock()
    client.get_epg_source = AsyncMock(side_effect=[
        {"updated_at": "old", "status": "success"},
        {"updated_at": "new", "status": status},
    ])
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()), \
         patch("tasks.dummy_epg_refresh.time.monotonic", side_effect=[0, 0, 61]):
        assert not await wait_for_epg_source_refresh(client, 1, "Guide", max_wait=60)


@pytest.mark.asyncio
async def test_successful_transition_completes_without_timestamp():
    client = MagicMock()
    client.get_epg_source = AsyncMock(side_effect=[
        {"status": "success"}, {"status": "processing"}, {"status": "success"},
    ])
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()):
        assert await wait_for_epg_source_refresh(client, 1, "Guide", poll_interval=0)
    client.refresh_epg_source.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_cancelled_wait_does_not_trigger_refresh():
    client = MagicMock()
    client.refresh_epg_source = AsyncMock()
    assert not await wait_for_epg_source_refresh(client, 1, "Guide", cancelled=lambda: True)
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_waiting_for_existing_refresh_does_not_trigger_it_again():
    client = MagicMock()
    client.get_epg_source = AsyncMock(return_value={"updated_at": "new", "status": "success"})
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()):
        assert await wait_for_epg_source_refresh(
            client, 1, "Guide", initial_source={"updated_at": "old"}, trigger=False,
        )
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_processing_state_can_complete_without_timestamps():
    client = MagicMock()
    client.get_epg_source = AsyncMock(return_value={"status": "success"})
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()):
        assert await wait_for_epg_source_refresh(
            client, 1, "Guide", initial_source={"status": "processing"}, trigger=False,
        )
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
    profile.to_dict.return_value = {"id": 7, "enabled": True}
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [profile]
    client = MagicMock()
    channels = {9: {"id": 9, "name": "Station", "channel_group_id": 4}}
    prepared = [{"id": 7, "enabled": True, "channel_assignments": [{"channel_id": 9}]}]
    coverage = {"profiles": {"7": {"can_publish": True, "reason_codes": []}}}
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
        result = await DummyEPGRefreshTask()._regenerate_xmltv()

    assert result is publication
    publish.assert_awaited_once()
    cache.invalidate_prefix.assert_called_once_with("dummy_epg_xmltv")
    cache.set.assert_any_call("dummy_epg_xmltv_all", "<tv/>")
    cache.set.assert_any_call("dummy_epg_xmltv_7", "<tv/>")


@pytest.mark.asyncio
async def test_superseded_publication_does_not_update_cache():
    profile = MagicMock()
    profile.to_dict.return_value = {"id": 7, "enabled": True}
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [profile]
    publication = PublicationResult(superseded=True, reason_codes=("GUIDE_PUBLICATION_SUPERSEDED",))
    cache = MagicMock()

    with patch("database.get_session", return_value=session), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=MagicMock()), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([profile.to_dict.return_value], {
             "profiles": {"7": {"can_publish": True, "reason_codes": []}},
         }))), patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=publication)), \
         patch("cache.get_cache", return_value=cache):
        result = await DummyEPGRefreshTask()._regenerate_xmltv()

    assert result.superseded is True
    cache.invalidate_prefix.assert_not_called()
    cache.set.assert_not_called()


def test_dummy_task_has_no_duplicate_visibility_writer():
    assert not hasattr(DummyEPGRefreshTask, "_apply_empty_channel_visibility")
