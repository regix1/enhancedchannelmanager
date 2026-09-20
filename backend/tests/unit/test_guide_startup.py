"""Startup recovery uses the same terminal result as scheduled guide refreshes."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from main import _rebuild_guide_on_startup
from task_scheduler import TaskResult


@pytest.fixture
def refresh(monkeypatch):
    import task_engine

    run = AsyncMock(return_value=TaskResult(success=True))
    monkeypatch.setattr(task_engine, "get_engine", lambda: Mock(run_task=run))
    return run


@pytest.mark.asyncio
async def test_startup_awaits_refresh_without_requiring_the_old_cache(refresh):
    started = asyncio.Event()
    release = asyncio.Event()

    async def execute(*args):
        started.set()
        await release.wait()
        return TaskResult(success=True)

    refresh.side_effect = execute
    job = asyncio.create_task(_rebuild_guide_on_startup())
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        assert not job.done()
        release.set()
        await job
        refresh.assert_awaited_once_with("dummy_epg_refresh")
    finally:
        job.cancel()
        await asyncio.gather(job, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("first", [
    TaskResult(success=False, error="GUIDE_UNAVAILABLE"),
    TaskResult(success=False, error="GUIDE_SOURCES_PENDING", completed_degraded=True),
    TaskResult(success=False, error="ALREADY_RUNNING"),
    RuntimeError("refresh failed"),
])
async def test_startup_retries_failed_or_pending_refreshes(refresh, monkeypatch, first):
    from services.epg_programmes import SOURCE_RETRY

    pause = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", pause)
    refresh.side_effect = [first, TaskResult(success=True)]
    await _rebuild_guide_on_startup()
    assert refresh.await_count == 2
    pause.assert_awaited_once_with(SOURCE_RETRY)


@pytest.mark.asyncio
async def test_startup_stops_after_bounded_retries(refresh, monkeypatch):
    pause = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", pause)
    refresh.return_value = TaskResult(success=False, error="GUIDE_UNAVAILABLE")
    await _rebuild_guide_on_startup()
    assert refresh.await_count == 3
    assert pause.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [
    None,
    TaskResult(success=False, error="ENGINE_STOPPING"),
    TaskResult(success=False, error="CANCELLED"),
])
async def test_startup_does_not_retry_unavailable_or_stopped_tasks(refresh, result):
    refresh.return_value = result
    await _rebuild_guide_on_startup()
    assert refresh.await_count == 1


@pytest.mark.asyncio
async def test_startup_propagates_cancellation(refresh):
    refresh.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await _rebuild_guide_on_startup()
    assert refresh.await_count == 1
