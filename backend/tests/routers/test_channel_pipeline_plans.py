import json
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, patch

from tests.unit.test_event_sync_promotion import (
    _clock, db_session_factory, promotion_candidates, retirement,
)
from tests.unit.test_event_sync_dummy_epg import (
    _dummy_entry, _refresh_results, _wire_epg,
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
    from services.epg_publication import read_publication
    from services.event_sync_promote import promoted_channel_name

    setup = promotion_candidates
    current_streams = {
        stream["id"]: stream for stream in setup["streams"]
    }
    setup["rows"] = [
        replace(
            row,
            stream=replace(
                row.stream,
                is_stale=current_streams[row.stream.stream_id].get("is_stale"),
            ),
        )
        for row in setup["rows"]
    ]
    setup["later_write"] = None
    setup["promotions"] = []
    setup["create_receipt_stages"] = []
    setup["completion_owners"] = []
    source_id = setup["epg_sources"][0]["id"]
    profile_id = setup["config"]["dummy_epg_profile_id"]
    first_name = promoted_channel_name(setup["rows"][0].result.parsed)
    headers, regenerate, wait_refresh = _wire_epg(
        setup["state"],
        setup["client"],
        setup["session_factory"],
        initial_entries=[],
        source_url=setup["epg_sources"][0]["url"],
        regenerated_entries=[_dummy_entry(501, 900, first_name, source_id)],
        profile_id=profile_id,
        source_id=source_id,
        now=setup["clock"],
    )
    setup["guide_headers"] = headers
    setup["regenerate"] = regenerate
    setup["wait_refresh"] = wait_refresh
    setup["profile_before"] = None
    profile_session = setup["session_factory"]()
    try:
        from models import DummyEPGProfile

        setup["profile_before"] = profile_session.get(
            DummyEPGProfile, profile_id,
        ).to_dict()
    finally:
        profile_session.close()

    create_channel = setup["client"].create_channel.side_effect

    async def create_with_uuid(value):
        publication = read_publication(f"profile:{profile_id}")
        receipt = next(
            receipt for receipt in publication["state"]["delivery"][
                "pending_channels"
            ].values()
            if receipt.get("channel_id") is None
        )
        setup["create_receipt_stages"].append(receipt["stage"])
        channel = await create_channel(value)
        channel["uuid"] = f"event-{channel['id']}"
        setup["state"].channels[channel["id"]]["uuid"] = channel["uuid"]
        return deepcopy(channel)

    setup["client"].create_channel.side_effect = create_with_uuid
    live_engine = ChannelPipelineEngine(setup["client"])
    complete_event_replay = ChannelPipelineEngine.complete_event_replay

    async def complete_with_current_owner(engine, executor, results):
        from models import ChannelPipelineRule

        session = setup["session_factory"]()
        try:
            stored = session.get(ChannelPipelineRule, setup["rule"].id)
            setup["completion_owners"].append(stored.get_managed_channel_ids())
        finally:
            session.close()
        await complete_event_replay(engine, executor, results)

    async def run(engine, **kwargs):
        from models import ChannelPipelineRule

        engine._existing_channels = deepcopy(list(setup["state"].channels.values()))
        rule_session = setup["session_factory"]()
        try:
            stored_rule = rule_session.get(ChannelPipelineRule, setup["rule"].id)
            managed_channel_ids = stored_rule.get_managed_channel_ids()
        finally:
            rule_session.close()
        executor = ActionExecutor(
            engine.client, deepcopy(engine._existing_channels),
            existing_groups=[{"id": setup["config"]["promote_target_group_id"], "name": "Events"}],
            managed_channel_ids=managed_channel_ids,
            plan_only=kwargs.get("plan_only", False),
            epg_sources=deepcopy(setup["epg_sources"]),
        )
        setup["batches"].append([])
        exec_ctx = ExecutionContext()
        promotion = await executor._execute_event_sync_promotion(
            setup["rule"].id, setup["rule"].name, setup["config"],
            SimpleNamespace(resolved=setup["rows"]), exec_ctx,
        )
        promote_entries = promotion.pop("promote_entries")
        setup["promotions"].append(promotion)
        if setup["later_write"] == "attach":
            await engine.client.update_channel(800, {"streams": [7000, 7001]})
        elif setup["later_write"] == "retire":
            await engine.client.delete_channel(800)
        elif setup["later_write"] == "profile":
            await engine.client.update_profile_channel(1, 800, {"enabled": True})
        result = _refresh_results()
        result.update({
            "channels_created": promotion["promoted_created"],
            "event_sync": [{"rule_id": setup["rule"].id, "promotion": promotion}],
            "execution_log": [
                {
                    "stream_id": (entry.get("match") or {}).get("secondary_stream_id"),
                    "stream_name": (entry.get("match") or {}).get("secondary_stream_name"),
                    "m3u_account_id": None,
                    "rules_evaluated": [],
                    "actions_executed": [entry],
                }
                for entry in promote_entries
            ],
            "created_entities": list(exec_ctx.created_entities),
        })
        return result

    async def load_rules(rule_ids=None):
        from models import ChannelPipelineRule

        rule_session = setup["session_factory"]()
        try:
            stored_rule = rule_session.get(ChannelPipelineRule, setup["rule"].id)
            if rule_ids and stored_rule.id not in rule_ids:
                return []
            return [stored_rule]
        finally:
            rule_session.close()

    monkeypatch.setattr(ChannelPipelineEngine, "run_pipeline", run)
    monkeypatch.setattr(
        ChannelPipelineEngine, "complete_event_replay", complete_with_current_owner,
    )
    monkeypatch.setattr(
        ChannelPipelineEngine,
        "_load_rules",
        AsyncMock(side_effect=load_rules),
    )
    monkeypatch.setattr(ChannelPipelineEngine, "_update_rule_stats", AsyncMock())
    monkeypatch.setattr(channel_pipeline, "_ensure_engine", AsyncMock(return_value=live_engine))
    monkeypatch.setattr(channel_pipeline, "get_session", setup["session_factory"])
    monkeypatch.setattr("journal.log_entries", MagicMock())
    task = MagicMock()
    task.return_value._regenerate_xmltv = regenerate
    monkeypatch.setattr("tasks.dummy_epg_refresh.DummyEPGRefreshTask", task)
    monkeypatch.setattr(
        "tasks.dummy_epg_refresh.wait_for_epg_source_refresh", wait_refresh,
    )
    monkeypatch.setattr(
        "channel_pipeline_executor.datetime", _clock(lambda: setup["clock"]),
    )
    monkeypatch.setattr(
        "channel_pipeline_engine.datetime", _clock(lambda: setup["clock"]),
    )
    monkeypatch.setattr(
        "services.event_sync_stream_health.datetime",
        _clock(lambda: setup["clock"]),
    )
    setup["client"].get_channel_profiles = AsyncMock(return_value=[{"id": 1, "channels": []}])
    setup["client"].update_profile_channel = AsyncMock(return_value={})
    yield setup


@pytest.mark.asyncio
@pytest.mark.parametrize("first_health", ["failed", "unknown", "missing_url"])
async def test_empty_event_prepare_reaches_next_candidate_and_commits(event_plan, first_health):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline
    from services.event_sync_promote import promoted_channel_name

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
    setup["guide_headers"][:] = [_dummy_entry(
        501,
        900,
        promoted_channel_name(setup["rows"][1].result.parsed),
        setup["epg_sources"][0]["id"],
    )]
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
async def test_event_prepare_is_read_only_stable_and_commit_completes(event_plan):
    import journal
    from models import ChannelPipelineExecution, ChannelPipelineRule, DummyEPGProfile
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline
    from services.epg_publication import read_publication
    from services.event_sync_promote import promoted_channel_name

    setup = event_plan
    setup["first_health"] = "success"
    request = RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id])
    scope = f"profile:{setup['config']['dummy_epg_profile_id']}"

    def stored_state():
        session = setup["session_factory"]()
        try:
            rule = session.get(ChannelPipelineRule, setup["rule"].id)
            profile = session.get(
                DummyEPGProfile, setup["config"]["dummy_epg_profile_id"],
            )
            return rule.get_managed_channel_ids(), profile.to_dict()
        finally:
            session.close()

    assert read_publication(scope) is None
    assert setup["state"].channels == {}
    assert stored_state() == ([], setup["profile_before"])
    first = await prepare_auto_creation_pipeline(request, _admin=None)
    second = await prepare_auto_creation_pipeline(request, _admin=None)
    assert first["plan_hash"] == second["plan_hash"]
    assert first["preview"] == second["preview"]
    assert first["preview"]["channels_created"] == 1
    assert read_publication(scope) is None
    assert setup["state"].channels == {}
    assert stored_state() == ([], setup["profile_before"])
    setup["client"].create_channel.assert_not_awaited()
    setup["client"].update_profile_channel.assert_not_awaited()
    journal.log_entries.assert_not_called()

    setup["guide_headers"][:] = [_dummy_entry(
        501,
        900,
        promoted_channel_name(setup["rows"][0].result.parsed),
        setup["epg_sources"][0]["id"],
    )]
    response = await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(
            plan_id=second["plan_id"], plan_hash=second["plan_hash"],
        ),
        _admin=None,
    )

    body = json.loads(response.body)
    assert body["status"] == "completed"
    assert setup["create_receipt_stages"] == ["allocating"]
    assert setup["completion_owners"] == [[900]]
    channel = setup["state"].channels[900]
    assert channel["uuid"] == "event-900"
    assert channel["hidden_from_output"] is False
    assert channel["streams"] == [7301]
    assert channel["epg_data_id"] == 501
    publication = read_publication(scope)
    receipt = next(iter(
        publication["state"]["delivery"]["pending_channels"].values()
    ))
    assert receipt["stage"] == "complete"
    assert receipt["channel_id"] == 900
    assert receipt["channel_uuid"] == "event-900"
    managed, profile = stored_state()
    assert managed == [900]
    assert profile == setup["profile_before"]
    session = setup["session_factory"]()
    try:
        execution = session.get(ChannelPipelineExecution, body["execution_id"])
        assert execution.status == "completed"
        assert execution.channels_created == 1
    finally:
        session.close()
    assert journal.log_entries.call_count >= 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("field", "changed", "detail"), [
    ("name", "Changed Event", "pipeline decision inputs drifted"),
    ("channel_group_id", 999, "event stream identity drifted"),
    ("m3u_account", 99, "event stream identity drifted"),
    ("is_stale", True, "pipeline decision inputs drifted"),
])
async def test_event_commit_rejects_stream_identity_drift(
    event_plan, field, changed, detail,
):
    from routers.channel_pipeline import (
        CommitPipelinePlanRequest,
        commit_auto_creation_pipeline,
    )

    setup = event_plan
    setup["first_health"] = "success"
    prepared = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]),
        _admin=None,
    )
    setup["streams"][0][field] = changed

    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=prepared["plan_id"],
                plan_hash=prepared["plan_hash"],
            ),
            _admin=None,
        )

    assert getattr(caught.value, "status_code", None) == 409
    assert detail in caught.value.detail
    setup["client"].create_channel.assert_not_awaited()


@pytest.mark.asyncio
async def test_two_events_share_one_profile_and_complete_in_order(event_plan):
    from models import ChannelPipelineRule
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline
    from services.epg_publication import read_publication
    from services.event_sync_promote import promoted_channel_name

    setup = event_plan
    setup["first_health"] = "success"
    setup["config"]["max_promote_per_run"] = 2
    setup["rule"].set_event_sync_config(setup["config"])
    setup["db"].commit()
    setup["guide_headers"][:] = [
        _dummy_entry(
            501 + index,
            900 + index,
            promoted_channel_name(row.result.parsed),
            setup["epg_sources"][0]["id"],
        )
        for index, row in enumerate(setup["rows"])
    ]
    prepared = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]),
        _admin=None,
    )
    assert prepared["preview"]["channels_created"] == 2

    response = await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(
            plan_id=prepared["plan_id"], plan_hash=prepared["plan_hash"],
        ),
        _admin=None,
    )

    assert json.loads(response.body)["status"] == "completed"
    assert setup["client"].create_channel.await_count == 2
    assert setup["create_receipt_stages"] == ["allocating", "allocating"]
    assert setup["completion_owners"] == [[900], [900, 901]]
    assert setup["state"].channels[900]["streams"] == [7301]
    assert setup["state"].channels[901]["streams"] == [7302]
    assert setup["state"].channels[900]["hidden_from_output"] is False
    assert setup["state"].channels[901]["hidden_from_output"] is False
    publication = read_publication(
        f"profile:{setup['config']['dummy_epg_profile_id']}"
    )
    receipts = publication["state"]["delivery"]["pending_channels"]
    assert {receipt["stage"] for receipt in receipts.values()} == {"complete"}
    assert {receipt["channel_id"] for receipt in receipts.values()} == {900, 901}
    session = setup["session_factory"]()
    try:
        stored = session.get(ChannelPipelineRule, setup["rule"].id)
        assert stored.get_managed_channel_ids() == [900, 901]
    finally:
        session.close()


def test_event_ownership_selection_preserves_default_recovery(event_plan):
    from types import SimpleNamespace

    from models import ChannelPipelineRule
    from routers.channel_pipeline import _retain_event_ownership

    setup = event_plan
    rule_id = setup["rule"].id
    target = setup["config"]["promote_target_group_id"]
    earlier = {"rule_id": rule_id, "target_group_id": target, "promo": {"channel_ids": [900]}}
    staged = {"rule_id": rule_id, "target_group_id": target, "promo": {"channel_ids": [901]}}
    executor = SimpleNamespace(
        _replayed_event_work=[earlier, staged],
        _channel_by_id={
            channel_id: {"id": channel_id, "channel_group_id": target}
            for channel_id in (900, 901)
        },
    )
    session = setup["session_factory"]()
    try:
        rule = session.get(ChannelPipelineRule, rule_id)
        rule.set_managed_channel_ids([800])
        session.commit()
    finally:
        session.close()

    _retain_event_ownership(executor, works=[staged])
    session = setup["session_factory"]()
    try:
        assert session.get(ChannelPipelineRule, rule_id).get_managed_channel_ids() == [800, 901]
    finally:
        session.close()

    _retain_event_ownership(executor)
    session = setup["session_factory"]()
    try:
        assert session.get(ChannelPipelineRule, rule_id).get_managed_channel_ids() == [800, 901, 900]
    finally:
        session.close()


@pytest.mark.asyncio
async def test_replay_does_not_register_an_earlier_removed_owner(event_plan, monkeypatch):
    from channel_pipeline_engine import ChannelPipelineEngine
    from models import ChannelPipelineRule
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline
    from services.event_sync_promote import promoted_channel_name

    setup = event_plan
    setup["first_health"] = "success"
    setup["config"]["max_promote_per_run"] = 2
    setup["rule"].set_event_sync_config(setup["config"])
    setup["db"].commit()
    setup["guide_headers"][:] = [
        _dummy_entry(
            501 + index, 900 + index,
            promoted_channel_name(row.result.parsed), setup["epg_sources"][0]["id"],
        )
        for index, row in enumerate(setup["rows"])
    ]
    complete_event_replay = ChannelPipelineEngine.complete_event_replay
    completions = 0

    async def complete_then_remove_owner(engine, executor, results):
        nonlocal completions
        await complete_event_replay(engine, executor, results)
        completions += 1
        if completions == 1:
            session = setup["session_factory"]()
            try:
                rule = session.get(ChannelPipelineRule, setup["rule"].id)
                rule.set_managed_channel_ids([])
                session.commit()
            finally:
                session.close()

    monkeypatch.setattr(
        ChannelPipelineEngine, "complete_event_replay", complete_then_remove_owner,
    )
    prepared = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]), _admin=None,
    )
    response = await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(plan_id=prepared["plan_id"], plan_hash=prepared["plan_hash"]),
        _admin=None,
    )

    assert json.loads(response.body)["status"] == "completed"
    assert completions == 2
    assert setup["completion_owners"] == [[900], [901]]
    assert setup["state"].channels[900]["streams"] == [7301]
    assert setup["state"].channels[901]["streams"] == [7302]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["complete", "failed", "expired", "allocation_unknown", "allocating"])
async def test_replay_registers_only_successful_nonterminal_staging(event_plan, monkeypatch, stage):
    from channel_pipeline_engine import ChannelPipelineEngine
    from channel_pipeline_executor import ActionExecutor
    from routers import channel_pipeline
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = event_plan
    setup["first_health"] = "success"
    prepared = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]), _admin=None,
    )
    replay = ActionExecutor.replay_event_promotion

    async def replay_with_stage(executor, operation, result_id):
        staged = await replay(executor, operation, result_id)
        assert staged["stage"] == "allocated"
        return {**staged, "stage": stage}

    ownership = MagicMock(wraps=channel_pipeline._retain_event_ownership)
    complete = AsyncMock()
    monkeypatch.setattr(ActionExecutor, "replay_event_promotion", replay_with_stage)
    monkeypatch.setattr(channel_pipeline, "_retain_event_ownership", ownership)
    monkeypatch.setattr(ChannelPipelineEngine, "complete_event_replay", complete)

    await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(plan_id=prepared["plan_id"], plan_hash=prepared["plan_hash"]),
        _admin=None,
    )

    ownership.assert_not_called()
    complete.assert_awaited_once()


@pytest.mark.asyncio
async def test_replay_ownership_commit_failure_prevents_completion(event_plan, monkeypatch):
    from channel_pipeline_engine import ChannelPipelineEngine
    from models import ChannelPipelineRule
    from routers import channel_pipeline
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = event_plan
    setup["first_health"] = "success"
    prepared = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]), _admin=None,
    )
    retain = channel_pipeline._retain_event_ownership
    attempts = []
    failure = RuntimeError("ownership commit failed")

    def retain_with_failed_commit(executor, *, works=None):
        if works is None:
            return retain(executor)
        assert works == executor._replayed_event_work[-1:]
        session = setup["session_factory"]()
        session.commit = MagicMock(side_effect=failure)
        rollback = MagicMock(wraps=session.rollback)
        session.rollback = rollback
        with patch.object(channel_pipeline, "get_session", return_value=session):
            with pytest.raises(RuntimeError) as caught:
                retain(executor, works=works)
        assert caught.value is failure
        session.commit.assert_called_once()
        rollback.assert_called_once()
        current = setup["session_factory"]()
        try:
            assert current.get(ChannelPipelineRule, setup["rule"].id).get_managed_channel_ids() == []
        finally:
            current.close()
        attempts.append(works[0]["promo"]["channel_ids"][:])
        raise failure

    complete = AsyncMock()
    monkeypatch.setattr(channel_pipeline, "_retain_event_ownership", retain_with_failed_commit)
    monkeypatch.setattr(ChannelPipelineEngine, "complete_event_replay", complete)
    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(plan_id=prepared["plan_id"], plan_hash=prepared["plan_hash"]),
            _admin=None,
        )

    assert getattr(caught.value, "status_code", None) == 502
    assert attempts == [[900]]
    complete.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True])
async def test_replay_removed_owner_cannot_mutate_or_close_receipt(event_plan, monkeypatch, expired):
    from copy import deepcopy
    from datetime import datetime

    from channel_pipeline_executor import ActionExecutor
    from models import ChannelPipelineRule
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline
    from services.epg_publication import read_publication

    setup = event_plan
    setup["first_health"] = "success"
    finish = ActionExecutor._finish_event_promotions
    denied = []

    async def finish_after_owner_removal(executor):
        before = deepcopy(read_publication(f"profile:{setup['config']['dummy_epg_profile_id']}"))
        receipt = next(iter(before["state"]["delivery"]["pending_channels"].values()))
        assert receipt["stage"] == "importing"
        session = setup["session_factory"]()
        try:
            rule = session.get(ChannelPipelineRule, receipt["rule_id"])
            assert rule.get_managed_channel_ids() == [900]
            rule.set_managed_channel_ids([])
            session.commit()
        finally:
            session.close()
        if expired:
            setup["clock"] = datetime.fromisoformat(receipt["expires_at"])
        channel = deepcopy(setup["state"].channels[900])
        setup["client"].update_channel.reset_mock()
        result = await finish(executor)
        assert read_publication(before["scope"]) == before
        assert setup["state"].channels[900] == channel
        setup["client"].update_channel.assert_not_awaited()
        denied.append(receipt["attempt_id"])
        return result

    monkeypatch.setattr(ActionExecutor, "_finish_event_promotions", finish_after_owner_removal)
    prepared = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]), _admin=None,
    )
    await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(plan_id=prepared["plan_id"], plan_hash=prepared["plan_hash"]),
        _admin=None,
    )

    assert len(denied) == 1
    assert setup["completion_owners"] == [[900]]
    assert setup["state"].channels[900]["hidden_from_output"] is True
    assert setup["state"].channels[900]["streams"] == []


@pytest.mark.asyncio
async def test_intervening_publication_writer_blocks_second_event(
    event_plan, monkeypatch,
):
    from channel_pipeline_engine import ChannelPipelineEngine
    from models import ChannelPipelineRule, GuidePublication
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline
    from services.epg_publication import read_publication
    from services.event_sync_promote import promoted_channel_name

    setup = event_plan
    setup["first_health"] = "success"
    setup["config"]["max_promote_per_run"] = 2
    setup["rule"].set_event_sync_config(setup["config"])
    setup["db"].commit()
    setup["guide_headers"][:] = [
        _dummy_entry(
            501 + index,
            900 + index,
            promoted_channel_name(row.result.parsed),
            setup["epg_sources"][0]["id"],
        )
        for index, row in enumerate(setup["rows"])
    ]
    original_complete = ChannelPipelineEngine.complete_event_replay
    completions = 0

    async def complete_with_intervening_write(engine, executor, results):
        nonlocal completions
        await original_complete(engine, executor, results)
        completions += 1
        if completions != 1:
            return
        session = setup["session_factory"]()
        try:
            row = session.get(
                GuidePublication,
                f"profile:{setup['config']['dummy_epg_profile_id']}",
            )
            row.revision += 1
            session.commit()
        finally:
            session.close()

    monkeypatch.setattr(
        ChannelPipelineEngine,
        "complete_event_replay",
        complete_with_intervening_write,
    )
    prepared = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]),
        _admin=None,
    )
    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=prepared["plan_id"], plan_hash=prepared["plan_hash"],
            ),
            _admin=None,
        )

    assert getattr(caught.value, "status_code", None) == 502
    assert setup["client"].create_channel.await_count == 1
    assert 900 in setup["state"].channels
    assert 901 not in setup["state"].channels
    publication = read_publication(
        f"profile:{setup['config']['dummy_epg_profile_id']}"
    )
    receipts = publication["state"]["delivery"]["pending_channels"]
    assert {receipt["channel_id"] for receipt in receipts.values()} == {900}
    session = setup["session_factory"]()
    try:
        rule = session.get(ChannelPipelineRule, setup["rule"].id)
        assert rule.get_managed_channel_ids() == [900]
    finally:
        session.close()


@pytest.mark.asyncio
async def test_cancelled_publication_resumes_same_channel_and_expiry(event_plan):
    import asyncio

    from models import ChannelPipelineExecution, ChannelPipelineRule
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline
    from services.epg_publication import read_publication
    from services.event_sync_promote import promoted_channel_name

    setup = event_plan
    setup["first_health"] = "success"
    setup["rows"] = setup["rows"][:1]
    setup["streams"][:] = setup["streams"][:1]
    setup["guide_headers"][:] = [_dummy_entry(
        501,
        900,
        promoted_channel_name(setup["rows"][0].result.parsed),
        setup["epg_sources"][0]["id"],
    )]
    publish = setup["regenerate"].side_effect
    cancelled = False

    async def cancel_after_publication(*args, **kwargs):
        nonlocal cancelled
        result = await publish(*args, **kwargs)
        if not cancelled:
            cancelled = True
            raise asyncio.CancelledError()
        return result

    setup["regenerate"].side_effect = cancel_after_publication
    first = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]),
        _admin=None,
    )
    with pytest.raises(asyncio.CancelledError):
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=first["plan_id"], plan_hash=first["plan_hash"],
            ),
            _admin=None,
        )

    assert setup["client"].create_channel.await_count == 1
    assert setup["state"].channels[900]["hidden_from_output"] is True
    assert setup["state"].channels[900]["streams"] == []
    scope = f"profile:{setup['config']['dummy_epg_profile_id']}"
    interrupted = read_publication(scope)
    interrupted_receipt = next(iter(
        interrupted["state"]["delivery"]["pending_channels"].values()
    ))
    assert interrupted_receipt["channel_id"] == 900
    assert interrupted_receipt["stage"] == "allocated"
    stored_attempt = interrupted_receipt["attempt_id"]
    stored_expiry = interrupted_receipt["expires_at"]
    session = setup["session_factory"]()
    try:
        rule = session.get(ChannelPipelineRule, setup["rule"].id)
        assert rule.get_managed_channel_ids() == [900]
        failed = session.query(ChannelPipelineExecution).filter(
            ChannelPipelineExecution.status == "failed"
        ).all()
        assert len(failed) == 1
    finally:
        session.close()

    second = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]),
        _admin=None,
    )
    response = await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(
            plan_id=second["plan_id"], plan_hash=second["plan_hash"],
        ),
        _admin=None,
    )

    assert json.loads(response.body)["status"] == "completed"
    assert setup["client"].create_channel.await_count == 1
    completed = read_publication(scope)
    completed_receipt = next(iter(
        completed["state"]["delivery"]["pending_channels"].values()
    ))
    assert completed_receipt["stage"] == "complete", (
        f"stage={completed_receipt['stage']} "
        f"reason={completed_receipt['reason']} "
        f"regenerations={setup['regenerate'].await_count} "
        f"refresh_waits={setup['wait_refresh'].await_count} "
        f"source_refreshes={setup['client'].refresh_epg_source.await_count} "
        f"programme_reads={setup['client'].get_epg_grid.await_count} "
        f"guide_rows={len(setup['state'].guide_rows)} "
        f"programmes={len(setup['state'].guide_programmes)} "
        f"updates={setup['state'].update_channel_calls} "
        f"response={json.loads(response.body)}"
    )
    assert setup["state"].channels[900]["hidden_from_output"] is False
    assert setup["state"].channels[900]["streams"] == [7301]
    assert completed_receipt["attempt_id"] == stored_attempt
    assert completed_receipt["expires_at"] == stored_expiry


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


@pytest.mark.asyncio
async def test_dedicated_replay_persists_new_membership_before_real_guide_completion(event_plan):
    from channel_pipeline_schema import validate_event_sync_config
    from models import ChannelPipelineRule, DummyEPGProfile
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline
    from tests.event_sync_fixtures import dedicated_event_sync_config

    setup = event_plan
    config = dedicated_event_sync_config(
        secondary=[{"group_id": 20, "m3u_account_id": 2}],
        dummy_epg_profile_id=setup["config"]["dummy_epg_profile_id"],
        promote_target_group_id=setup["config"]["promote_target_group_id"],
        max_promote_per_run=1,
    )
    session = setup["session_factory"]()
    try:
        profile = session.get(DummyEPGProfile, config["dummy_epg_profile_id"])
        profile.set_channel_group_ids([config["promote_target_group_id"]])
        profile.set_hide_empty_group_ids([config["promote_target_group_id"]])
        profile.set_epg_source_ids([])
        profile.set_channel_mappings([])
        profile.set_event_sync_config({"secondary": config["secondary"], "slot_patterns": [], "assume_current_date": False, "use_default_patterns": False})
        session.commit()
        assert validate_event_sync_config(config) == []
        rule = session.get(ChannelPipelineRule, setup["rule"].id)
        rule.set_event_sync_config(config)
        session.commit()
    finally:
        session.close()
    setup["config"] = config
    setup["client"].get_channel_groups.return_value = [{"id": config["promote_target_group_id"], "name": "Events"}]
    setup["first_health"] = "success"
    setup["epg_sources"][0]["is_active"] = True
    setup["state"].guide_sources[0]["is_active"] = True
    prepared = await prepare_auto_creation_pipeline(RunPipelineRequest(dry_run=False, rule_ids=[setup["rule"].id]), _admin=None)
    assert prepared["preview"]["channels_created"] == 1
    setup["client"].create_channel.assert_not_awaited()
    result = await commit_auto_creation_pipeline(CommitPipelinePlanRequest(plan_id=prepared["plan_id"], plan_hash=prepared["plan_hash"]), _admin=None)
    assert json.loads(result.body)["status"] == "completed"
    assert setup["completion_owners"] == [[900]]
    assert setup["state"].channels[900]["epg_data_id"] == 501
    assert setup["state"].channels[900]["streams"] == [7301]
    assert setup["state"].channels[900]["hidden_from_output"] is False
    assert setup["create_receipt_stages"] == ["allocating"]
