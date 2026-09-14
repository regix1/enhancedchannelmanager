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
