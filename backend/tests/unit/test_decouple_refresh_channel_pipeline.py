"""ADR-011 (bd-ka7j9) — decouple M3U refresh from auto-creation (event-driven).

Phase 2 follow-up to the exo4j circuit-breaker. M3U refresh no longer
hard-chains auto-creation as a side-effect; instead:

  - A SUCCESSFUL M3U refresh advances the ``last_m3u_refresh_completed_at``
    watermark in settings (Q1: on EVERY successful refresh, change-gated NO).
  - The interval-scheduled ``ChannelPipelineTask`` decides FOR ITSELF whether to
    run via a top-of-run AUTO-FIRE GUARD: enabled AND breaker clear AND
    >=1 enabled+run_on_refresh rule AND refresh watermark newer than the
    consumed watermark. On run it advances the consumed watermark and runs
    ONLY ``enabled AND run_on_refresh=True`` rules (Q2).
  - The manual pipeline ``/run`` path stays UN-gated (covered elsewhere).

These tests assert the decoupling itself: the refresh task no longer invokes
auto-creation, the watermark advances, and the auto-fire guard fires iff all
four conditions hold.
"""
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from task_scheduler import ScheduleType


# ---------------------------------------------------------------------------
# Step 1 — M3U refresh advances the watermark and NO LONGER calls auto-creation
# ---------------------------------------------------------------------------
def _make_refresh_task():
    from tasks.m3u_refresh import M3URefreshTask
    task = M3URefreshTask()
    task.account_ids = []
    task.skip_inactive = True
    return task


@pytest.mark.asyncio
async def test_refresh_advances_watermark_on_success():
    """A successful refresh advances last_m3u_refresh_completed_at via save_settings."""
    account = {"id": 1, "name": "Prov", "is_active": True}

    client = MagicMock()
    client.get_m3u_accounts = AsyncMock(return_value=[account])
    client.get_channel_groups = AsyncMock(return_value=[{"id": 1, "name": "G"}])
    client.refresh_m3u_account = AsyncMock(return_value=None)
    client.get_m3u_account = AsyncMock(side_effect=[
        {"id": 1, "updated_at": "2026-01-01T00:00:00Z", "channel_groups": []},
        {"id": 1, "updated_at": "2026-01-02T00:00:00Z", "channel_groups": []},
    ])

    settings = MagicMock(last_m3u_refresh_completed_at="")

    with patch("tasks.m3u_refresh.get_client", return_value=client), \
         patch("tasks.m3u_refresh.capture_m3u_changes", new=AsyncMock(return_value=None)), \
         patch("tasks.m3u_refresh.POLL_INTERVAL_SECONDS", 0), \
         patch("tasks.m3u_refresh.asyncio.sleep", new=AsyncMock(return_value=None)), \
         patch("tasks.m3u_refresh.get_settings", return_value=settings), \
         patch("tasks.m3u_refresh.save_settings") as mock_save:
        task = _make_refresh_task()
        result = await task.execute()

    assert result.success is True
    # Watermark was advanced and persisted.
    mock_save.assert_called_once()
    assert settings.last_m3u_refresh_completed_at != ""


@pytest.mark.asyncio
async def test_refresh_does_not_invoke_auto_creation():
    """The hard chain is gone: refresh never calls run_auto_creation_after_refresh
    nor the auto-creation engine."""
    account = {"id": 1, "name": "Prov", "is_active": True}

    client = MagicMock()
    client.get_m3u_accounts = AsyncMock(return_value=[account])
    client.get_channel_groups = AsyncMock(return_value=[{"id": 1, "name": "G"}])
    client.refresh_m3u_account = AsyncMock(return_value=None)
    client.get_m3u_account = AsyncMock(side_effect=[
        {"id": 1, "updated_at": "2026-01-01T00:00:00Z", "channel_groups": []},
        {"id": 1, "updated_at": "2026-01-02T00:00:00Z", "channel_groups": []},
    ])

    settings = MagicMock(last_m3u_refresh_completed_at="")

    # tasks.m3u_refresh must not even import the auto-creation module.
    import tasks.m3u_refresh as m3u_mod
    src = open(m3u_mod.__file__).read()
    assert "run_auto_creation_after_refresh" not in src, (
        "m3u_refresh must not reference run_auto_creation_after_refresh"
    )
    assert "from tasks.channel_pipeline import" not in src, (
        "m3u_refresh must not import from tasks.channel_pipeline"
    )

    with patch("tasks.m3u_refresh.get_client", return_value=client), \
         patch("tasks.m3u_refresh.capture_m3u_changes", new=AsyncMock(return_value=None)), \
         patch("tasks.m3u_refresh.POLL_INTERVAL_SECONDS", 0), \
         patch("tasks.m3u_refresh.asyncio.sleep", new=AsyncMock(return_value=None)), \
         patch("tasks.m3u_refresh.get_settings", return_value=settings), \
         patch("tasks.m3u_refresh.save_settings"):
        task = _make_refresh_task()
        result = await task.execute()

    assert result.success is True


@pytest.mark.asyncio
async def test_refresh_with_failures_does_not_advance_watermark():
    """A run where the account refresh raises must NOT advance the watermark."""
    account = {"id": 1, "name": "Prov", "is_active": True}

    client = MagicMock()
    client.get_m3u_accounts = AsyncMock(return_value=[account])
    client.get_channel_groups = AsyncMock(return_value=[{"id": 1, "name": "G"}])
    client.refresh_m3u_account = AsyncMock(side_effect=RuntimeError("boom"))
    client.get_m3u_account = AsyncMock(return_value={"id": 1, "channel_groups": []})

    settings = MagicMock(last_m3u_refresh_completed_at="")

    with patch("tasks.m3u_refresh.get_client", return_value=client), \
         patch("tasks.m3u_refresh.capture_m3u_changes", new=AsyncMock(return_value=None)), \
         patch("tasks.m3u_refresh.POLL_INTERVAL_SECONDS", 0), \
         patch("tasks.m3u_refresh.asyncio.sleep", new=AsyncMock(return_value=None)), \
         patch("tasks.m3u_refresh.get_settings", return_value=settings), \
         patch("tasks.m3u_refresh.save_settings") as mock_save:
        task = _make_refresh_task()
        await task.execute()

    # No fully-successful account -> watermark not advanced.
    mock_save.assert_not_called()


# ---------------------------------------------------------------------------
# Step 2 — ChannelPipelineTask interval schedule + auto-fire guard
# ---------------------------------------------------------------------------
def test_auto_creation_task_has_interval_schedule():
    """The default ChannelPipelineTask schedule is INTERVAL ~60s so the engine
    ticks it (the guard then decides whether to actually run)."""
    from tasks.channel_pipeline import ChannelPipelineTask
    from task_engine import DEFAULT_CHECK_INTERVAL

    task = ChannelPipelineTask()
    assert task.schedule_config.schedule_type == ScheduleType.INTERVAL
    assert task.schedule_config.interval_seconds == DEFAULT_CHECK_INTERVAL


def _patch_settings(**kwargs):
    base = dict(
        auto_creation_run_on_refresh_disabled=False,
        last_m3u_refresh_completed_at="",
        last_auto_creation_consumed_refresh_at="",
    )
    base.update(kwargs)
    return MagicMock(**base)


async def _run_autofire(settings, rules, engine_result=None, env=None, rule_ids=None):
    """Run ChannelPipelineTask.execute() with the given settings + run_on_refresh rules.

    Returns (result, engine_mock, save_settings_mock).
    """
    from tasks.channel_pipeline import ChannelPipelineTask
    from cache import Cache

    fake_engine = MagicMock()
    fake_engine.run_pipeline = AsyncMock(
        return_value=engine_result or {
            "channels_created": 0, "channels_updated": 0,
            "streams_matched": 0, "streams_evaluated": 0,
        }
    )

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = rules
    session.query.return_value.filter.return_value.count.return_value = len(rules)

    env = env or {}
    with patch.dict(os.environ, env, clear=False), \
         patch("cache.get_cache", return_value=Cache()), \
         patch("tasks.channel_pipeline.get_settings", return_value=settings), \
         patch("tasks.channel_pipeline.save_settings") as mock_save, \
         patch("services.notification_service.create_notification_internal", new=AsyncMock()), \
         patch("channel_pipeline_engine.get_channel_pipeline_engine", return_value=fake_engine), \
         patch("channel_pipeline_engine.init_channel_pipeline_engine", new=AsyncMock(return_value=fake_engine)), \
         patch("dispatcharr_client.get_client", return_value=MagicMock()), \
         patch("tasks.channel_pipeline.get_client", return_value=MagicMock()), \
         patch("database.get_session", return_value=session), \
         patch("journal.log_entry"):
        if "ECM_DISABLE_RUN_ON_REFRESH" not in env:
            os.environ.pop("ECM_DISABLE_RUN_ON_REFRESH", None)
        task = ChannelPipelineTask()
        # i2xad: scheduled auto-creation is opt-in (default_enabled=False). These
        # tests exercise AUTO-FIRE GUARD conditions (b)/(c)/(d), which only apply
        # once condition (a) "enabled" holds — i.e. after an operator opts in.
        task._enabled = True
        task.rule_ids = rule_ids or []
        result = await task.execute()

    return result, fake_engine, mock_save


def _rule(rid=1, name="R1"):
    r = MagicMock(id=rid)
    r.name = name
    return r


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["due", "aware_due", "recent", "aware_recent", "opt_out", "manual_only", "disabled", "scope", "breaker"])
async def test_event_only_tick_keeps_the_refresh_watermark(case):
    settings = _patch_settings(last_m3u_refresh_completed_at="2026-01-01",
                               last_auto_creation_consumed_refresh_at="2026-01-01")
    rule = _rule(22)
    config = {"auto_run": True, "enabled": True, "retire_finished_events": True}
    rule.last_run_at = datetime.utcnow() - timedelta(minutes=6)
    if case in {"recent", "aware_recent"}:
        rule.last_run_at = datetime.utcnow()
    elif case == "opt_out":
        config["retire_finished_events"] = False
    elif case == "manual_only":
        config["auto_run"] = False
    elif case == "disabled":
        config["enabled"] = False
    elif case == "breaker":
        settings.auto_creation_run_on_refresh_disabled = True
    if case.startswith("aware"):
        rule.last_run_at = rule.last_run_at.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=5)))
    rule.get_event_sync_config.return_value = config
    result, engine, save = await _run_autofire(
        settings, [_rule(1), rule], rule_ids=[99] if case == "scope" else [22],
    )
    assert result.success
    if case in {"due", "aware_due"}:
        engine.run_pipeline.assert_awaited_once()
        assert engine.run_pipeline.await_args.kwargs["rule_ids"] == [22]
        assert engine.run_pipeline.await_args.kwargs["triggered_by"] == "scheduled"
    else:
        engine.run_pipeline.assert_not_awaited()
    save.assert_not_called()
    assert settings.last_auto_creation_consumed_refresh_at == "2026-01-01"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["offline", "ALREADY_RUNNING", "cancelled"])
async def test_scheduled_attempts_wait_after_failure(failure):
    import asyncio
    from cache import Cache
    from tasks.channel_pipeline import ChannelPipelineTask

    settings = _patch_settings()
    rule = _rule(22)
    rule.last_run_at = None
    rule.get_event_sync_config.return_value = {"auto_run": True, "retire_finished_events": True}
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [rule]
    engine = MagicMock()
    engine.run_pipeline = AsyncMock(side_effect=asyncio.CancelledError() if failure == "cancelled" else RuntimeError(failure))
    task = ChannelPipelineTask()
    task._enabled = True
    cache = Cache()
    with patch("tasks.channel_pipeline.get_settings", return_value=settings), \
         patch("database.get_session", return_value=session), \
         patch("channel_pipeline_engine.get_channel_pipeline_engine", return_value=engine), \
         patch("tasks.channel_pipeline.get_client", return_value=MagicMock()), \
         patch("services.notification_service.create_notification_internal", new=AsyncMock()) as notify, \
         patch("cache.get_cache", return_value=cache), \
         patch("cache.time.time") as clock:
        clock.return_value = 1000
        if failure == "cancelled":
            with pytest.raises(asyncio.CancelledError):
                await task.execute()
        else:
            assert not (await task.execute()).success
        clock.return_value = 1060
        second = ChannelPipelineTask()
        second._enabled = True
        skipped = await second.execute()
        assert skipped.success
        assert skipped.suppress_completion_notification is True
        clock.return_value = 1301
        if failure == "cancelled":
            with pytest.raises(asyncio.CancelledError):
                await second.execute()
        else:
            assert not (await second.execute()).success
    assert engine.run_pipeline.await_count == 2
    titles = [call.kwargs["title"] for call in notify.await_args_list]
    assert titles == ([] if failure == "cancelled" else ["Auto-Creation: Failed"] * 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("removed", [0, 1])
async def test_scheduled_no_change_pass_is_quiet(removed):
    from tasks.channel_pipeline import ChannelPipelineTask

    task = ChannelPipelineTask()
    engine = MagicMock()
    engine.run_pipeline = AsyncMock(return_value={"channels_removed": removed})
    with patch("channel_pipeline_engine.get_channel_pipeline_engine", return_value=engine), \
         patch("tasks.channel_pipeline.get_client", return_value=MagicMock()), \
         patch("services.notification_service.create_notification_internal", new=AsyncMock()) as notify:
        result = await task._run_post_refresh_pipeline([22], ["Events"], datetime.utcnow(), triggered_by="scheduled")
        first_calls = list(notify.await_args_list)
        notify.reset_mock()
        engine.run_pipeline.side_effect = RuntimeError("offline")
        failed = await task._run_post_refresh_pipeline([22], ["Events"], datetime.utcnow(), triggered_by="manual")
        assert failed.success is False
        assert failed.suppress_completion_notification is False
        assert [call.kwargs["title"] for call in notify.await_args_list] == ["Auto-Creation: Starting", "Auto-Creation: Failed"]
    assert result.success
    assert result.suppress_completion_notification is (removed == 0)
    if removed:
        assert [call.kwargs["title"] for call in first_calls] == ["Auto-Creation: 1 removed"]
    else:
        assert first_calls == []


@pytest.mark.asyncio
async def test_autofire_runs_when_all_conditions_hold():
    """Enabled + breaker clear + run_on_refresh rule + watermark newer -> runs,
    advances the consumed watermark, runs only run_on_refresh rules."""
    settings = _patch_settings(
        last_m3u_refresh_completed_at="2026-01-02T00:00:00+00:00",
        last_auto_creation_consumed_refresh_at="2026-01-01T00:00:00+00:00",
    )
    result, engine, mock_save = await _run_autofire(
        settings, rules=[_rule()],
        engine_result={"channels_created": 3, "channels_updated": 0,
                       "streams_matched": 3, "streams_evaluated": 5},
    )

    assert result.success is True
    engine.run_pipeline.assert_awaited_once()
    # Ran only the run_on_refresh rule set.
    kwargs = engine.run_pipeline.call_args.kwargs
    assert kwargs["rule_ids"] == [1]
    assert kwargs["dry_run"] is False
    # Consumed watermark advanced to the refresh watermark and persisted.
    assert settings.last_auto_creation_consumed_refresh_at == "2026-01-02T00:00:00+00:00"
    mock_save.assert_called_once()


@pytest.mark.asyncio
async def test_autofire_skips_when_breaker_set():
    settings = _patch_settings(
        auto_creation_run_on_refresh_disabled=True,
        last_m3u_refresh_completed_at="2026-01-02T00:00:00+00:00",
        last_auto_creation_consumed_refresh_at="2026-01-01T00:00:00+00:00",
    )
    result, engine, mock_save = await _run_autofire(settings, rules=[_rule()])
    engine.run_pipeline.assert_not_awaited()
    # Watermark NOT consumed when suppressed.
    mock_save.assert_not_called()


@pytest.mark.asyncio
async def test_autofire_skips_when_break_glass_env_set():
    settings = _patch_settings(
        last_m3u_refresh_completed_at="2026-01-02T00:00:00+00:00",
        last_auto_creation_consumed_refresh_at="2026-01-01T00:00:00+00:00",
    )
    result, engine, mock_save = await _run_autofire(
        settings, rules=[_rule()], env={"ECM_DISABLE_RUN_ON_REFRESH": "1"}
    )
    engine.run_pipeline.assert_not_awaited()
    mock_save.assert_not_called()


@pytest.mark.asyncio
async def test_autofire_skips_when_no_run_on_refresh_rules():
    settings = _patch_settings(
        last_m3u_refresh_completed_at="2026-01-02T00:00:00+00:00",
        last_auto_creation_consumed_refresh_at="2026-01-01T00:00:00+00:00",
    )
    result, engine, mock_save = await _run_autofire(settings, rules=[])
    engine.run_pipeline.assert_not_awaited()
    mock_save.assert_not_called()


@pytest.mark.asyncio
async def test_autofire_skips_when_watermark_not_newer():
    """No new refresh since the last consumed watermark -> nothing to do."""
    settings = _patch_settings(
        last_m3u_refresh_completed_at="2026-01-01T00:00:00+00:00",
        last_auto_creation_consumed_refresh_at="2026-01-01T00:00:00+00:00",
    )
    result, engine, mock_save = await _run_autofire(settings, rules=[_rule()])
    engine.run_pipeline.assert_not_awaited()
    mock_save.assert_not_called()


@pytest.mark.asyncio
async def test_autofire_skips_when_no_refresh_yet():
    """Empty refresh watermark (never refreshed) -> nothing to do."""
    settings = _patch_settings(
        last_m3u_refresh_completed_at="",
        last_auto_creation_consumed_refresh_at="",
    )
    result, engine, mock_save = await _run_autofire(settings, rules=[_rule()])
    engine.run_pipeline.assert_not_awaited()
    mock_save.assert_not_called()


@pytest.mark.asyncio
async def test_autofire_first_ever_refresh_fires():
    """First refresh ever (consumed empty, refresh set) -> fires once."""
    settings = _patch_settings(
        last_m3u_refresh_completed_at="2026-01-02T00:00:00+00:00",
        last_auto_creation_consumed_refresh_at="",
    )
    result, engine, mock_save = await _run_autofire(settings, rules=[_rule()])
    engine.run_pipeline.assert_awaited_once()
    assert settings.last_auto_creation_consumed_refresh_at == "2026-01-02T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Step 2 — no double-fire across overlapping ~60s ticks (engine guard)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_no_double_fire_when_already_running():
    """The engine's "already running" guard prevents an overlapping tick from
    starting a second auto_creation run while one is in flight."""
    from task_engine import TaskEngine

    engine = TaskEngine()
    # Simulate a run already in flight for the auto_creation task.
    engine._active_tasks.add("auto_creation")

    result = await engine._execute_task("auto_creation", triggered_by="scheduled")

    assert result is not None
    assert result.error == "ALREADY_RUNNING"
    assert result.success is False
