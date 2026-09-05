"""Refresh completion follows observed source state, including cancellation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tasks.dummy_epg_refresh import wait_for_epg_source_refresh


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
@pytest.mark.parametrize("dummy", [False, True])
@pytest.mark.parametrize("outcome", ["timeout", "cancelled", "success"])
async def test_tasks_count_only_completed_sources(dummy, outcome):
    from tasks.dummy_epg_refresh import DummyEPGRefreshTask
    from tasks.epg_refresh import EPGRefreshTask

    task = DummyEPGRefreshTask() if dummy else EPGRefreshTask()
    module = "tasks.dummy_epg_refresh" if dummy else "tasks.epg_refresh"
    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[{
        "id": 1, "name": "Guide", "is_active": True,
        "url": "http://ecm/api/dummy-epg/xmltv",
    }])

    async def finish(*args, **kwargs):
        task._cancel_requested = outcome == "cancelled"
        return outcome == "success"

    with patch(module + ".get_client", return_value=client), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", side_effect=finish), \
         patch.object(DummyEPGRefreshTask, "_regenerate_xmltv", new=AsyncMock(return_value=1)):
        result = await task.execute()
    assert result.success_count == (1 if outcome == "success" else 0)
    assert result.success is (outcome == "success")
    assert result.failed_count == (1 if outcome == "timeout" else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, "ready", "pending", "error", "stale", "artwork"])
async def test_scheduled_generation_uses_shared_preparation_once(status):
    from tasks.dummy_epg_refresh import DummyEPGRefreshTask

    profile = MagicMock()
    profile.to_dict.return_value = {"id": 7, "channel_group_ids": [4]}
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = [profile]
    channels = {9: {"id": 9, "name": "Station", "channel_group": 4, "streams": []}}
    prepared = [{"id": 7, "channel_assignments": [{"channel_id": 9}]}]
    coverage = {"sources": [] if status is None else [{"source_id": 1, "status": "ready" if status == "artwork" else status}],
                "artwork_pending": status == "artwork"}
    client = MagicMock()
    with patch("database.get_session", return_value=db), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=client), \
         patch("cache.get_cache") as cache, \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)) as fetch, \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=(prepared, coverage))) as prepare, \
         patch("concurrency.run_cpu_bound", new=AsyncMock(return_value="<tv/>")) as render:
        assert await DummyEPGRefreshTask()._regenerate_xmltv() == 1
    fetch.assert_awaited_once_with(client)
    prepare.assert_awaited_once_with([profile.to_dict.return_value], channels, client, wait_for_sources=True)
    assert render.await_count == 2
    assert render.await_args_list[0].args[1:] == (prepared, channels)
    if status in {None, "ready"}:
        cache.return_value.set.assert_any_call("dummy_epg_xmltv_all", "<tv/>")
        cache.return_value.set.assert_any_call("dummy_epg_xmltv_7", "<tv/>")
    else:
        cache.return_value.set.assert_not_called()
    cache.return_value.invalidate_prefix.assert_called_once_with("dummy_epg_xmltv")
