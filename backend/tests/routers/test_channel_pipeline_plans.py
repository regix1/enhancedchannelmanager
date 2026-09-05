from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from tests.unit.test_event_sync_promotion import (
    db_session_factory, promotion_candidates, retirement,
)

import pytest

from routers.channel_pipeline import (
    RunPipelineRequest, prepare_auto_creation_pipeline, run_auto_creation_pipeline,
)
from services.mutation_plan_store import mutation_plan_store


@pytest.mark.asyncio
async def test_prepare_pipeline_is_unrecorded_and_materializes_server_plan():
    engine = AsyncMock()
    engine.run_pipeline.return_value = {
        "execution_id": None,
        "dry_run_results": [{"stream_id": 4, "would_create": True}],
        "channels_created": 1,
    }
    engine.client = AsyncMock()
    engine._existing_channels = []
    with patch("routers.channel_pipeline._ensure_engine", AsyncMock(return_value=engine)), patch(
        "channel_pipeline_engine.ChannelPipelineEngine", return_value=engine
    ):
        response = await prepare_auto_creation_pipeline(
            RunPipelineRequest(dry_run=False, rule_ids=[3]), _admin=None
        )
    assert response["preview"]["channels_created"] == 1
    assert "execution_id" not in response["preview"]
    engine.run_pipeline.assert_awaited_once_with(
        dry_run=False, triggered_by="api", m3u_account_ids=None,
        rule_ids=[3], record_execution=False, plan_only=True, skip_prerefresh=True,
    )
    consumed = mutation_plan_store.consume(
        response["plan_id"], "channel_pipeline", response["plan_hash"], principal="api"
    )
    assert consumed.payload["request"]["rule_ids"] == [3]


@pytest.mark.asyncio
async def test_legacy_pipeline_execute_is_denied_only_to_mcp_principal():
    with pytest.raises(Exception) as caught:
        await run_auto_creation_pipeline(
            RunPipelineRequest(dry_run=False), _admin=None, caller_is_mcp=True
        )
    assert getattr(caught.value, "status_code", None) == 409

    engine = AsyncMock()
    def consume_coroutine(coro, **_kwargs):
        coro.close()
    with patch("routers.channel_pipeline._ensure_engine", AsyncMock(return_value=engine)), patch(
        "routers.channel_pipeline._create_pending_execution", return_value=91
    ), patch(
        "routers.channel_pipeline._supervise_background_pipeline",
        side_effect=consume_coroutine,
    ):
        response = await run_auto_creation_pipeline(
            RunPipelineRequest(dry_run=False), _admin=None, caller_is_mcp=False
        )
    assert response.status_code == 202


@pytest.mark.asyncio
async def test_prerefresh_prepare_only_resolves_accounts_and_does_not_refresh_or_plan_writes():
    engine = AsyncMock()
    rule = MagicMock()
    rule.get_event_sync_config.return_value = {
        "refresh_providers_before_run": True,
        "master_group_id": 4,
        "secondary_group_ids": [5],
    }
    engine._load_rules.return_value = [rule]
    engine._resolve_event_sync_refresh_accounts.return_value = {9, 7}
    with patch("routers.channel_pipeline._ensure_engine", AsyncMock(return_value=engine)):
        response = await prepare_auto_creation_pipeline(
            RunPipelineRequest(dry_run=False), _admin=None
        )
    assert response["phase"] == "refresh"
    assert response["preview"] == {"m3u_account_ids_to_refresh": [7, 9]}
    engine.run_pipeline.assert_not_awaited()

@pytest.fixture
def event_plan(promotion_candidates, monkeypatch):
    from copy import deepcopy
    from types import SimpleNamespace
    from channel_pipeline_engine import ChannelPipelineEngine
    from channel_pipeline_executor import ActionExecutor, ExecutionContext
    from routers import channel_pipeline

    setup = promotion_candidates
    setup["later_write"] = None
    setup["promotions"] = []
    live_engine = ChannelPipelineEngine(setup["client"])

    async def run(engine, **kwargs):
        engine._existing_channels = deepcopy(list(setup["state"].channels.values()))
        executor = ActionExecutor(
            engine.client, deepcopy(engine._existing_channels),
            managed_channel_ids=[], plan_only=kwargs.get("plan_only", False),
        )
        setup["batches"].append([])
        promotion = await executor._execute_event_sync_promotion(
            setup["rule"].id, setup["rule"].name, setup["config"],
            SimpleNamespace(resolved=setup["rows"]), ExecutionContext(),
        )
        setup["promotions"].append(promotion)
        if setup["later_write"] == "attach":
            await engine.client.update_channel(800, {"streams": [7000, 7001]})
        elif setup["later_write"] == "retire":
            await engine.client.delete_channel(800)
        elif setup["later_write"] == "profile":
            await engine.client.update_profile_channel(1, 800, {"enabled": True})
        return {
            "channels_created": promotion["promoted_created"],
            "event_sync": [{"rule_id": setup["rule"].id, "promotion": promotion}],
        }

    monkeypatch.setattr(ChannelPipelineEngine, "run_pipeline", run)
    monkeypatch.setattr(ChannelPipelineEngine, "_load_rules", AsyncMock(return_value=[setup["rule"]]))
    monkeypatch.setattr(ChannelPipelineEngine, "_update_rule_stats", AsyncMock())
    monkeypatch.setattr(channel_pipeline, "_ensure_engine", AsyncMock(return_value=live_engine))
    monkeypatch.setattr(channel_pipeline, "get_session", setup["session_factory"])
    monkeypatch.setattr("journal.log_entries", MagicMock())
    setup["client"].get_channel_profiles = AsyncMock(return_value=[{"id": 1, "channels": []}])
    setup["client"].update_profile_channel = AsyncMock(return_value={})
    with patch("channel_pipeline_executor.datetime") as clock:
        clock.now.return_value = setup["clock"]
        clock.fromisoformat.side_effect = datetime.fromisoformat
        yield setup


@pytest.mark.asyncio
@pytest.mark.parametrize("first_health", ["failed", "unknown", "missing_url"])
async def test_empty_event_prepare_reaches_next_candidate_and_commits(event_plan, first_health):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = event_plan
    setup["first_health"] = first_health
    if first_health == "missing_url":
        setup["streams"][0].pop("url")
    request = RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id])
    first = await prepare_auto_creation_pipeline(request, _admin=None)
    assert first["preview"]["channels_created"] == 0
    setup["client"].create_channel.assert_not_awaited()
    second = await prepare_auto_creation_pipeline(request, _admin=None)
    assert second["preview"]["channels_created"] == 1
    setup["client"].create_channel.assert_not_awaited()
    response = await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(plan_id=second["plan_id"], plan_hash=second["plan_hash"]),
        _admin=None,
    )
    assert response.status_code == 202
    setup["client"].create_channel.assert_awaited_once()
    assert "Zulu Event" in next(iter(setup["state"].channels.values()))["name"]
    assert setup["batches"] == [[] if first_health == "missing_url" else [7301], [7302], []]
    assert all("probe_after" not in promotion for promotion in setup["promotions"])


@pytest.mark.asyncio
@pytest.mark.parametrize("later_write", ["attach", "retire", "profile"])
async def test_event_prepare_keeps_selection_when_later_phases_write(event_plan, later_write):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = event_plan
    setup["later_write"] = later_write
    setup["state"].channels[800] = {
        "id": 800, "name": "Existing Channel", "streams": [7000], "channel_group_id": 999,
    }
    prepared = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]), _admin=None,
    )
    assert prepared["preview"]["channels_created"] == 0
    response = await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(plan_id=prepared["plan_id"], plan_hash=prepared["plan_hash"]),
        _admin=None,
    )
    assert response.status_code == 202
    assert setup["batches"] == [[7301], []]
    setup["client"].create_channel.assert_not_awaited()
    if later_write == "attach":
        assert setup["state"].channels[800]["streams"] == [7000, 7001]
    elif later_write == "retire":
        assert 800 not in setup["state"].channels
    else:
        setup["client"].update_profile_channel.assert_awaited_once_with(1, 800, {"enabled": True})

@pytest.mark.asyncio
async def test_event_confirmation_rejects_changed_selection(event_plan):
    from types import SimpleNamespace
    from channel_pipeline_executor import ActionExecutor, ExecutionContext
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = event_plan
    setup["later_write"] = "attach"
    setup["state"].channels[800] = {
        "id": 800, "name": "Existing Channel", "streams": [7000], "channel_group_id": 999,
    }
    prepared = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]), _admin=None,
    )
    setup["batches"].append([])
    await ActionExecutor(setup["client"], [])._execute_event_sync_promotion(
        setup["rule"].id, setup["rule"].name, setup["config"],
        SimpleNamespace(resolved=setup["rows"]), ExecutionContext(),
    )
    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(plan_id=prepared["plan_id"], plan_hash=prepared["plan_hash"]),
            _admin=None,
        )
    assert getattr(caught.value, "status_code", None) == 409
    assert "decision inputs drifted" in caught.value.detail
    assert setup["batches"] == [[7301], [], [7302]]
    setup["client"].create_channel.assert_not_awaited()
    setup["client"].update_channel.assert_not_awaited()
    assert setup["state"].channels[800]["streams"] == [7000]
