"""Unit tests for StreamProber.probe_streams_by_ids — async bulk probe.

Covers the redesign tracked in enhancedchannelmanager-znc76.5: the on-demand
bulk probe must run through the SAME progress / results / history envelope as
probe_all_streams, so a manual bulk run shows up in get_probe_progress /
get_probe_results just like a scheduled probe-all run (the "manual probes don't
feed the results envelope" half of the bug).
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from stream_prober import StreamProber


def _make_prober(client, **kwargs):
    # Patch history persistence to disk so unit tests don't touch /config.
    prober = StreamProber(client=client, **kwargs)
    prober._persist_probe_history = lambda: None
    return prober


async def _wait_for_gate_waiters(prober, count: int = 1):
    for _ in range(100):
        if len(prober._probe_condition._waiters) >= count:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {count} account-gate waiter(s)")


@pytest.mark.asyncio
async def test_bulk_run_populates_results_envelope():
    """probe_streams_by_ids writes the shared envelope that get_probe_results reads."""
    client = AsyncMock()
    prober = _make_prober(client)

    streams = [
        {"id": 10, "url": "http://example.com/10", "name": "Stream 10"},
        {"id": 11, "url": "http://example.com/11", "name": "Stream 11"},
        {"id": 12, "url": "http://example.com/12", "name": "Stream 12"},
    ]
    outcomes = {
        10: {"stream_id": 10, "probe_status": "success"},
        11: {"stream_id": 11, "probe_status": "failed", "error_message": "404"},
        12: {"stream_id": 12, "probe_status": "timeout", "error_message": "timed out"},
    }

    async def fake_probe_stream(sid, url, name):
        return outcomes[sid]

    with patch.object(prober, "_fetch_all_streams", AsyncMock(return_value=streams)), \
         patch.object(prober, "probe_stream", side_effect=fake_probe_stream):
        result = await prober.probe_streams_by_ids([10, 11, 12])

    # Envelope tallies on the return value.
    assert result["status"] == "completed"
    assert result["total"] == 3
    assert result["success"] == 1
    assert result["failed"] == 2  # failed + timeout

    # The SHARED envelope (what get_probe_results / get_probe_progress expose)
    # reflects this bulk run.
    envelope = prober.get_probe_results()
    assert envelope["success_count"] == 1
    assert envelope["failed_count"] == 2
    assert {s["id"] for s in envelope["success_streams"]} == {10}
    assert {s["id"] for s in envelope["failed_streams"]} == {11, 12}

    progress = prober.get_probe_progress()
    assert progress["in_progress"] is False
    assert progress["status"] == "completed"
    assert progress["total"] == 3
    assert progress["success_count"] == 1
    assert progress["failed_count"] == 2

    # And the run is recorded in history (shared with probe-all).
    history = prober.get_probe_history()
    assert len(history) == 1
    assert history[0]["total"] == 3
    assert history[0]["success_count"] == 1
    assert history[0]["failed_count"] == 2


@pytest.mark.asyncio
async def test_bulk_run_rejected_when_probe_in_progress():
    """Single-probe-at-a-time invariant: returns already_running, probes nothing."""
    client = AsyncMock()
    prober = _make_prober(client)
    prober._probing_in_progress = True  # Simulate a probe already running.

    probe_stream_mock = AsyncMock()
    with patch.object(prober, "_fetch_all_streams", AsyncMock()) as fetch_mock, \
         patch.object(prober, "probe_stream", probe_stream_mock):
        result = await prober.probe_streams_by_ids([10, 11])

    assert result["status"] == "already_running"
    fetch_mock.assert_not_called()
    probe_stream_mock.assert_not_called()
    # The in-progress flag must be left untouched (we did not own the run).
    assert prober._probing_in_progress is True


@pytest.mark.asyncio
async def test_bulk_run_honors_auto_reorder_setting():
    """When auto_reorder_after_probe is on, bulk probe reorders affected channels."""
    client = AsyncMock()
    prober = _make_prober(client, auto_reorder_after_probe=True)

    streams = [{"id": 10, "url": "http://example.com/10", "name": "Stream 10"}]

    async def fake_probe_stream(sid, url, name):
        return {"stream_id": sid, "probe_status": "success"}

    reorder_mock = AsyncMock(return_value=[{"channel_id": 1, "channel_name": "Ch", "stream_count": 2}])

    with patch.object(prober, "_fetch_all_streams", AsyncMock(return_value=streams)), \
         patch.object(prober, "probe_stream", side_effect=fake_probe_stream), \
         patch.object(prober, "_auto_reorder_channels_for_streams", reorder_mock):
        await prober.probe_streams_by_ids([10])

    reorder_mock.assert_awaited_once_with([10])


@pytest.mark.asyncio
async def test_bulk_run_skips_reorder_when_setting_off():
    """Default (auto_reorder_after_probe off): bulk probe does not reorder."""
    client = AsyncMock()
    prober = _make_prober(client)  # default auto_reorder_after_probe=False

    streams = [{"id": 10, "url": "http://example.com/10", "name": "Stream 10"}]

    async def fake_probe_stream(sid, url, name):
        return {"stream_id": sid, "probe_status": "success"}

    reorder_mock = AsyncMock()
    with patch.object(prober, "_fetch_all_streams", AsyncMock(return_value=streams)), \
         patch.object(prober, "probe_stream", side_effect=fake_probe_stream), \
         patch.object(prober, "_auto_reorder_channels_for_streams", reorder_mock):
        await prober.probe_streams_by_ids([10])

    reorder_mock.assert_not_called()


@pytest.mark.asyncio
async def test_bulk_run_counts_missing_streams_as_failed():
    """Stream IDs not present in Dispatcharr are tallied as failures, not dropped."""
    client = AsyncMock()
    prober = _make_prober(client)

    streams = [{"id": 10, "url": "http://example.com/10", "name": "Stream 10"}]

    async def fake_probe_stream(sid, url, name):
        return {"stream_id": sid, "probe_status": "success"}

    with patch.object(prober, "_fetch_all_streams", AsyncMock(return_value=streams)), \
         patch.object(prober, "probe_stream", side_effect=fake_probe_stream):
        # 99 does not exist in Dispatcharr.
        result = await prober.probe_streams_by_ids([10, 99])

    assert result["total"] == 2
    assert result["success"] == 1
    assert result["failed"] == 1
    failed = prober.get_probe_results()["failed_streams"]
    assert any(s["id"] == 99 for s in failed)


@pytest.mark.asyncio
async def test_scheduled_probe_includes_hidden_channel_streams():
    client = AsyncMock()
    client.get_channel_groups.return_value = [{"id": 65, "name": "Live Events/PPV"}]
    client.get_channels.return_value = {
        "results": [{
            "id": 10,
            "name": "Hidden event slot",
            "channel_group_id": 65,
            "channel_number": 900,
            "hidden_from_output": True,
            "streams": [110],
        }],
        "next": None,
    }
    prober = _make_prober(client)

    stream_ids, channels, numbers = await prober._fetch_channel_stream_ids(
        ["Live Events/PPV"],
    )

    assert stream_ids == {110}
    assert channels == {110: ["Hidden event slot"]}
    assert numbers == {110: 900}
    client.get_channels.assert_awaited_once_with(
        page=1, page_size=500, visibility_filter="all",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("first_event", [False, True])
async def test_account_limit_refresh_preserves_held_claim(first_event):
    prober = _make_prober(AsyncMock(), max_concurrent_probes=2)
    prober.account_probe_limits = {2: 1}
    first = prober.semaphore_for_account(2, event=first_event)
    await first.__aenter__()
    second_entered = asyncio.Event()

    async def enter_second():
        async with prober.semaphore_for_account(2, event=not first_event):
            second_entered.set()

    second = asyncio.create_task(enter_second())
    await _wait_for_gate_waiters(prober)
    assert not second_entered.is_set()

    with patch(
        "config.get_settings",
        return_value=MagicMock(probe_concurrency_by_account={}),
    ), patch(
        "services.probe_limits.account_probe_limits",
        AsyncMock(return_value={2: 1}),
    ):
        await prober.refresh_account_probe_limits()

    await _wait_for_gate_waiters(prober)
    assert not second_entered.is_set()
    await first.__aexit__(None, None, None)
    await asyncio.wait_for(second, timeout=1)
    assert second_entered.is_set()
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
async def test_account_limit_reduction_waits_for_all_excess_holders():
    prober = _make_prober(AsyncMock(), max_concurrent_probes=3)
    prober.account_probe_limits = {2: 2}
    first = prober.semaphore_for_account(2)
    second = prober.semaphore_for_account(2)
    await first.__aenter__()
    await second.__aenter__()
    third_entered = asyncio.Event()

    async def enter_third():
        async with prober.semaphore_for_account(2):
            third_entered.set()

    with patch(
        "config.get_settings",
        return_value=MagicMock(probe_concurrency_by_account={}),
    ), patch(
        "services.probe_limits.account_probe_limits",
        AsyncMock(return_value={2: 1}),
    ):
        await prober.refresh_account_probe_limits()

    third = asyncio.create_task(enter_third())
    await _wait_for_gate_waiters(prober)
    await first.__aexit__(None, None, None)
    await _wait_for_gate_waiters(prober)
    assert not third_entered.is_set()
    await second.__aexit__(None, None, None)
    await asyncio.wait_for(third, timeout=1)
    assert third_entered.is_set()
    assert prober._account_active == {}


@pytest.mark.asyncio
async def test_account_limit_increase_wakes_waiters():
    prober = _make_prober(AsyncMock(), max_concurrent_probes=3)
    prober.account_probe_limits = {2: 1}
    first = prober.semaphore_for_account(2)
    await first.__aenter__()
    entered = [asyncio.Event(), asyncio.Event()]

    async def enter(index):
        async with prober.semaphore_for_account(2):
            entered[index].set()

    tasks = [asyncio.create_task(enter(index)) for index in range(2)]
    await _wait_for_gate_waiters(prober, 2)
    with patch(
        "config.get_settings",
        return_value=MagicMock(probe_concurrency_by_account={}),
    ), patch(
        "services.probe_limits.account_probe_limits",
        AsyncMock(return_value={2: 3}),
    ):
        await prober.refresh_account_probe_limits()

    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
    assert all(event.is_set() for event in entered)
    await first.__aexit__(None, None, None)
    assert prober._account_active == {}


@pytest.mark.asyncio
async def test_scoped_account_limit_refresh_merges_without_resetting_claims():
    prober = _make_prober(AsyncMock(), max_concurrent_probes=3)
    prober.account_probe_limits = {2: 1, 18: 2}
    unrelated = prober.semaphore_for_account(18)
    await unrelated.__aenter__()
    read_limits = AsyncMock(return_value={2: 3})

    with patch(
        "config.get_settings",
        return_value=MagicMock(probe_concurrency_by_account={}),
    ), patch(
        "services.probe_limits.account_probe_limits",
        read_limits,
    ):
        await prober.refresh_account_probe_limits(account_ids={2})

    assert prober.account_probe_limits == {2: 3, 18: 2}
    assert prober._account_active == {18: 1}
    read_limits.assert_awaited_once_with(
        prober.client, {}, account_ids={2},
    )
    await unrelated.__aexit__(None, None, None)
    assert prober._account_active == {}


@pytest.mark.asyncio
async def test_scoped_account_limit_refresh_keeps_known_selected_ceiling_without_result():
    prober = _make_prober(AsyncMock(), max_concurrent_probes=3)
    prober.account_probe_limits = {2: 1, 18: 2}

    with patch(
        "config.get_settings",
        return_value=MagicMock(probe_concurrency_by_account={}),
    ), patch(
        "services.probe_limits.account_probe_limits",
        AsyncMock(return_value={}),
    ):
        await prober.refresh_account_probe_limits(account_ids={2})

    assert prober.account_probe_limits == {2: 1, 18: 2}


@pytest.mark.asyncio
async def test_cancelled_account_claims_leave_no_occupancy():
    prober = _make_prober(AsyncMock(), max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    holder_started = asyncio.Event()
    hold = asyncio.Event()

    async def holder():
        async with prober.semaphore_for_account(2, event=True):
            holder_started.set()
            await hold.wait()

    active = asyncio.create_task(holder())
    await holder_started.wait()

    waiting = asyncio.create_task(holder())
    await _wait_for_gate_waiters(prober)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert prober._account_active == {2: 1}
    assert prober._event_probes == 1

    active.cancel()
    with pytest.raises(asyncio.CancelledError):
        await active
    assert prober._account_active == {}
    assert prober._event_probes == 0

    async with prober.semaphore_for_account(2, event=True):
        assert prober._account_active == {2: 1}


@pytest.mark.asyncio
async def test_ordinary_accounts_keep_independent_capacity():
    prober = _make_prober(AsyncMock(), max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1, 18: 1}
    release = asyncio.Event()
    entered = {2: asyncio.Event(), 18: asyncio.Event()}

    async def hold(account):
        async with prober.semaphore_for_account(account):
            entered[account].set()
            await release.wait()

    tasks = [asyncio.create_task(hold(account)) for account in entered]
    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in entered.values())),
        timeout=1,
    )
    assert prober._account_active == {2: 1, 18: 1}
    release.set()
    await asyncio.gather(*tasks)
    assert prober._account_active == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    ["scheduled_parallel", "scheduled_sequential", "on_demand"],
)
async def test_bulk_paths_wait_for_active_event_claim(mode):
    client = AsyncMock()
    client.get_m3u_accounts.return_value = [{
        "id": 2,
        "name": "Account 2",
        "max_streams": 0,
        "profiles": [],
    }]
    client.get_channel_stats.return_value = {"channels": []}
    prober = _make_prober(
        client,
        parallel_probing_enabled=mode == "scheduled_parallel",
        max_concurrent_probes=1,
        refresh_m3us_before_probe=False,
        auto_reorder_after_probe=False,
    )
    prober.account_probe_limits = {2: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    prober._create_probe_notification = AsyncMock()
    prober._update_probe_notification = AsyncMock()
    prober._finalize_probe_notification = AsyncMock()
    prober._fetch_all_streams = AsyncMock(return_value=[{
        "id": 10,
        "url": "http://example.com/10",
        "name": "Stream 10",
        "m3u_account": 2,
    }])
    prober._fetch_channel_stream_ids = AsyncMock(return_value=(
        {10}, {10: ["Channel 10"]}, {10: 10},
    ))
    media_started = asyncio.Event()

    async def probe(*_args):
        media_started.set()
        return {"probe_status": "success"}

    prober.probe_stream = AsyncMock(side_effect=probe)
    event_claim = prober.semaphore_for_account(2, event=True)
    await event_claim.__aenter__()
    if mode == "on_demand":
        bulk = asyncio.create_task(prober.probe_streams_by_ids([10]))
    else:
        bulk = asyncio.create_task(prober.probe_all_streams(
            skip_m3u_refresh=True,
        ))

    await _wait_for_gate_waiters(prober)
    assert not media_started.is_set()
    await event_claim.__aexit__(None, None, None)
    await asyncio.wait_for(bulk, timeout=3)
    assert media_started.is_set()
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
async def test_cancelled_sequential_probe_does_not_start_queued_media():
    client = AsyncMock()
    client.get_m3u_accounts.return_value = [{
        "id": 2,
        "name": "Account 2",
        "max_streams": 0,
        "profiles": [],
    }]
    client.get_channel_stats.return_value = {"channels": []}
    prober = _make_prober(
        client,
        parallel_probing_enabled=False,
        max_concurrent_probes=1,
        refresh_m3us_before_probe=False,
        auto_reorder_after_probe=False,
    )
    prober.account_probe_limits = {2: 1}
    prober._create_probe_notification = AsyncMock()
    prober._update_probe_notification = AsyncMock()
    prober._finalize_probe_notification = AsyncMock()
    prober._fetch_all_streams = AsyncMock(return_value=[{
        "id": 10,
        "url": "http://example.com/10",
        "name": "Stream 10",
        "m3u_account": 2,
    }])
    prober._fetch_channel_stream_ids = AsyncMock(return_value=(
        {10}, {10: ["Channel 10"]}, {10: 10},
    ))
    prober.probe_stream = AsyncMock(return_value={"probe_status": "success"})

    event_claim = prober.semaphore_for_account(2, event=True)
    await event_claim.__aenter__()
    bulk = asyncio.create_task(prober.probe_all_streams(skip_m3u_refresh=True))
    await _wait_for_gate_waiters(prober)
    prober._probe_cancelled = True
    await event_claim.__aexit__(None, None, None)
    await asyncio.wait_for(bulk, timeout=2)

    prober.probe_stream.assert_not_awaited()
    assert prober._account_active == {}
    assert prober._event_probes == 0
