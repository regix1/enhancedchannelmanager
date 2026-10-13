"""Task-level contracts for scheduled stream probing."""
from unittest.mock import AsyncMock

import pytest

from tasks.stream_probe import StreamProbeTask


@pytest.mark.asyncio
async def test_scheduled_probe_does_not_refresh_m3u_inventory():
    prober = AsyncMock()
    prober._probing_in_progress = False
    prober.probe_timeout = 30
    prober.max_concurrent_probes = 1
    prober._probe_progress_success_count = 1
    prober._probe_progress_failed_count = 0
    prober._probe_progress_skipped_count = 0
    prober._probe_progress_total = 1
    prober._probe_progress_black_screen_count = 0
    prober._probe_progress_low_fps_count = 0
    prober._probe_success_streams = []
    prober._probe_failed_streams = []
    prober._failure_breakdown.return_value = []

    task = StreamProbeTask()
    task.set_prober(prober)
    result = await task.execute()

    assert result.success is True
    prober.probe_all_streams.assert_awaited_once()
    assert prober.probe_all_streams.await_args.kwargs["channel_groups_override"] is None
    assert prober.probe_all_streams.await_args.kwargs["skip_m3u_refresh"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("override", [False, True])
@pytest.mark.parametrize("change", ["none", "live_save", "rebuild"])
async def test_settings_saved_during_a_scheduled_probe_stand(override, change):
    import asyncio
    from unittest.mock import MagicMock

    from stream_prober import StreamProber

    old = StreamProber(MagicMock(), probe_timeout=30, max_concurrent_probes=4)
    started, release = asyncio.Event(), asyncio.Event()

    async def probe_all_streams(*args, **kwargs):
        started.set()
        await release.wait()

    old.probe_all_streams = probe_all_streams
    task = StreamProbeTask()
    task._send_alerts = False
    if override:
        task.update_config({"timeout": 45, "max_concurrent": 3})
    task.set_prober(old)
    running = asyncio.create_task(task.execute())
    await started.wait()
    target = old
    if change == "live_save":
        # The two writes the Settings save makes on the live prober.
        old.update_probing_settings(True, 2)
        old.probe_timeout = 60
    elif change == "rebuild":
        target = StreamProber(MagicMock(), probe_timeout=60, max_concurrent_probes=2)
        task.set_prober(target)
    release.set()
    result = await running

    assert result.success is True
    assert (target.probe_timeout, target.max_concurrent_probes) == ((30, 4) if change == "none" else (60, 2))
