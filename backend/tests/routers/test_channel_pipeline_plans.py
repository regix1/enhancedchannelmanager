import asyncio
import hashlib
import json
from copy import deepcopy
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


def test_prepared_probe_operation_window_is_240_seconds():
    from routers.channel_pipeline import _PREPARED_PROBE_SECONDS

    assert _PREPARED_PROBE_SECONDS == 240.0


async def _serve_pipeline_pipe():
    """Serve real staged pipeline calls over a private test subprocess pipe."""
    import sys
    import tempfile
    from pathlib import Path

    import database
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from channel_pipeline_engine import ChannelPipelineEngine
    from models import ChannelPipelineRule, StreamStats
    import routers.channel_pipeline as pipeline_router
    from routers.channel_pipeline import (
        CommitPipelinePlanRequest,
        commit_auto_creation_pipeline,
        get_auto_creation_execution,
    )
    from stream_prober import StreamProber
    from tests.unit.test_channel_pipeline_engine import _mk_smart_sort_settings

    temporary = tempfile.TemporaryDirectory(prefix="ecm-pipeline-pipe-")
    engine = create_engine(f"sqlite:///{Path(temporary.name) / 'pipeline.db'}")
    database.Base.metadata.create_all(bind=engine)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    db = sessions()
    quality_rule = ChannelPipelineRule(
        name="Quality order",
        enabled=True,
        priority=0,
        m3u_account_id=1,
        conditions=json.dumps([{
            "type": "stream_name_contains", "value": "Sports",
        }]),
        actions=json.dumps([{
            "type": "create_channel",
            "name_template": "Sports",
            "if_exists": "merge",
        }]),
        sort_field="quality",
        sort_order="desc",
        probe_on_sort=True,
        stream_sort_field="quality",
        stream_sort_order="desc",
        skip_struck_streams=True,
        orphan_action="none",
        allow_manual_channel_merge=True,
        match_scope_target_group=False,
    )
    quality_rule.set_managed_channel_ids([200])
    refresh_rule = ChannelPipelineRule(
        name="Refresh first",
        enabled=True,
        priority=-1,
        conditions="[]",
        actions="[]",
    )
    refresh_rule.set_event_sync_config({
        "refresh_providers_before_run": True,
        "master": {"group_id": 50, "m3u_account_id": 1},
        "secondary": [],
    })
    db.add_all([
        quality_rule,
        refresh_rule,
        StreamStats(
            stream_id=102,
            stream_name="Sports recovered",
            probe_status="failed",
            consecutive_failures=3,
            resolution="3840x2160",
        ),
        StreamStats(
            stream_id=103,
            stream_name="Sports failed",
            probe_status="failed",
            consecutive_failures=3,
            resolution="1920x1080",
        ),
        StreamStats(
            stream_id=104,
            stream_name="Sports cached",
            probe_status="success",
            consecutive_failures=0,
            resolution="1280x720",
            fps="30",
            video_codec="h264",
        ),
    ])
    db.commit()
    quality_rule_id = quality_rule.id
    refresh_rule_id = refresh_rule.id
    db.close()

    streams = {
        101: {
            "id": 101, "name": "Sports new", "url": "http://media/101",
            "m3u_account": {"id": 1, "name": "Primary"},
        },
        102: {
            "id": 102, "name": "Sports recovered", "url": "http://media/102",
            "m3u_account": {"id": 1, "name": "Primary"},
        },
        103: {
            "id": 103, "name": "Sports failed", "url": "http://media/103",
            "m3u_account": {"id": 1, "name": "Primary"},
        },
        104: {
            "id": 104, "name": "Sports cached", "url": "http://media/104",
            "m3u_account": {"id": 1, "name": "Primary"},
        },
        105: {
            "id": 105, "name": "Foreign", "url": "http://media/105",
            "m3u_account": {"id": 2, "name": "Foreign"},
        },
    }
    channels = {
        200: {
            "id": 200,
            "name": "Sports",
            "streams": [104, 101, 102],
            "channel_group_id": None,
            "epg_data_id": 7,
            "tvg_id": "sports",
            "logo_id": None,
            "auto_created": False,
        },
        201: {
            "id": 201,
            "name": "Foreign",
            "streams": [105],
            "channel_group_id": None,
            "epg_data_id": 8,
            "tvg_id": "foreign",
            "logo_id": None,
            "auto_created": False,
        },
    }

    async def get_channels(page=1, page_size=100, **_kwargs):
        return {
            "count": len(channels),
            "results": deepcopy(list(channels.values())),
            "next": None,
        }

    async def get_streams(page=1, page_size=1000, m3u_account=None):
        selected = [
            stream for stream in streams.values()
            if stream["m3u_account"]["id"] == m3u_account
        ]
        return {"count": len(selected), "results": deepcopy(selected)}

    async def get_streams_by_ids(stream_ids):
        return [deepcopy(streams[stream_id]) for stream_id in stream_ids]

    async def get_channel(channel_id):
        return deepcopy(channels[channel_id])

    async def update_channel(channel_id, changes):
        channel = channels[channel_id]
        for key, value in changes.items():
            channel[key] = deepcopy(value)
        return deepcopy(channel)

    client = MagicMock()
    client.get_channels = AsyncMock(side_effect=get_channels)
    client.get_channel_groups = AsyncMock(return_value=[])
    client.get_channel_profiles = AsyncMock(return_value=[])
    client.get_m3u_accounts = AsyncMock(return_value=[
        {"id": 1, "name": "Primary"},
        {"id": 2, "name": "Foreign"},
    ])
    client.get_streams = AsyncMock(side_effect=get_streams)
    client.get_streams_by_ids = AsyncMock(side_effect=get_streams_by_ids)
    client.get_channel = AsyncMock(side_effect=get_channel)
    client.get_channel_streams = AsyncMock(
        side_effect=lambda channel_id: list(channels[channel_id]["streams"])
    )
    client.update_channel = AsyncMock(side_effect=update_channel)
    client.create_channel = AsyncMock(
        side_effect=AssertionError("the existing channel must be reused")
    )
    client.delete_channel = AsyncMock()
    client.delete_channel_group = AsyncMock()
    client.update_profile_channel = AsyncMock()
    client.assign_channel_numbers = AsyncMock(return_value={})

    settings = _mk_smart_sort_settings(
        stream_sort_priority=["resolution"],
        stream_sort_enabled={"resolution": True},
        deprioritize_failed_streams=True,
        failed_stream_sort_order=["failed", "black_screen", "low_fps"],
    )
    settings.strike_threshold = 3
    settings.timezone_preference = "both"
    settings.include_channel_number_in_name = False
    settings.channel_number_separator = "-"
    settings.default_channel_profile_ids = []
    settings.auto_rename_channel_number = False
    settings.auto_creation_excluded_terms = []
    settings.auto_creation_excluded_groups = []
    settings.auto_creation_exclude_auto_sync_groups = False
    settings.max_auto_creation_log_entries = 1000
    settings.max_auto_created_channels_per_run = 0

    prober = StreamProber(
        client=client, max_concurrent_probes=1, probe_retry_count=0,
        black_screen_detection_enabled=False,
    )
    prober.account_probe_limits = {1: 1}
    prober.refresh_account_probe_limits = AsyncMock()

    async def read_media(url, *, expires_at=None):
        if url.endswith("/103"):
            raise RuntimeError("synthetic failed observation")
        height = 2160 if url.endswith("/102") else 1080
        return {
            "streams": [{
                "codec_type": "video",
                "width": 3840 if height == 2160 else 1920,
                "height": height,
                "codec_name": "h264",
                "r_frame_rate": "30/1",
            }, {
                "codec_type": "audio", "codec_name": "aac", "channels": 2,
            }],
            "format": {"format_name": "hls", "bit_rate": "5000000"},
        }

    async def measure_media(url, *, expires_at=None):
        return 8_000_000 if url.endswith("/102") else 5_000_000

    prober._run_ffprobe = AsyncMock(side_effect=read_media)
    prober._measure_stream_bitrate = AsyncMock(side_effect=measure_media)
    prober._push_stats_to_dispatcharr = AsyncMock()
    live_engine = ChannelPipelineEngine(client)

    refresh_task = MagicMock()

    async def refresh_accounts():
        refresh_db = sessions()
        try:
            stored = refresh_db.get(ChannelPipelineRule, refresh_rule_id)
            stored.enabled = False
            refresh_db.commit()
        finally:
            refresh_db.close()
        return {"success": True}

    refresh_task.execute = AsyncMock(side_effect=refresh_accounts)
    refresh_task.update_config = MagicMock()

    patches = [
        patch("channel_pipeline_engine.get_session", sessions),
        patch("routers.channel_pipeline.get_session", sessions),
        patch("stream_prober.get_session", sessions),
        patch("channel_pipeline_engine.get_settings", return_value=settings),
        patch("routers.channel_pipeline._ensure_engine", AsyncMock(return_value=live_engine)),
        patch("stream_prober.get_prober", return_value=prober),
        patch("channel_pipeline_engine.request_guide_refresh", AsyncMock()),
        patch("journal.log_entries", MagicMock()),
        patch("tasks.m3u_refresh.M3URefreshTask", return_value=refresh_task),
    ]
    for active in patches:
        active.start()
    original_probe_seconds = pipeline_router._PREPARED_PROBE_SECONDS
    probe_lock_held = False

    def response_body(response):
        if hasattr(response, "body"):
            return json.loads(response.body)
        if hasattr(response, "model_dump"):
            return response.model_dump()
        return response

    try:
        print(json.dumps({"ready": True, "quality_rule_id": quality_rule_id}), flush=True)
        while True:
            line = await asyncio.to_thread(sys.stdin.readline)
            if not line:
                break
            request = json.loads(line)
            if request["operation"] == "close":
                print(json.dumps({"closed": True}), flush=True)
                break
            try:
                if request["operation"] == "prepare":
                    response = await prepare_auto_creation_pipeline(
                        RunPipelineRequest(**request["body"]), _admin=None,
                    )
                elif request["operation"] == "commit":
                    response = await commit_auto_creation_pipeline(
                        CommitPipelinePlanRequest(**request["body"]), _admin=None,
                    )
                elif request["operation"] == "execution":
                    response = await get_auto_creation_execution(
                        request["execution_id"],
                    )
                elif request["operation"] == "hold_probe_lock":
                    pipeline_router._PREPARED_PROBE_SECONDS = request["seconds"]
                    await pipeline_router._MCP_PLANNED_RUN_LOCK.acquire()
                    probe_lock_held = True
                    response = {"held": True}
                elif request["operation"] == "release_probe_lock":
                    if probe_lock_held:
                        pipeline_router._MCP_PLANNED_RUN_LOCK.release()
                        probe_lock_held = False
                    pipeline_router._PREPARED_PROBE_SECONDS = original_probe_seconds
                    response = {"released": True}
                elif request["operation"] == "state":
                    response = {
                        "channels": channels,
                        "writes": [
                            {"args": call.args, "kwargs": call.kwargs}
                            for call in client.update_channel.await_args_list
                        ],
                        "media_calls": prober._run_ffprobe.await_count,
                        "refresh_calls": refresh_task.execute.await_count,
                    }
                else:
                    raise ValueError("unknown pipe operation")
                print(json.dumps({"ok": True, "response": response_body(response)}), flush=True)
            except Exception as exc:
                print(json.dumps({
                    "ok": False,
                    "status": getattr(exc, "status_code", None),
                    "detail": getattr(exc, "detail", None),
                    "error": str(getattr(exc, "detail", exc)),
                }), flush=True)
    finally:
        if probe_lock_held:
            pipeline_router._MCP_PLANNED_RUN_LOCK.release()
        pipeline_router._PREPARED_PROBE_SECONDS = original_probe_seconds
        for active in reversed(patches):
            active.stop()
        database.Base.metadata.drop_all(bind=engine)
        engine.dispose()
        temporary.cleanup()


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


@pytest.mark.asyncio
async def test_prepare_pipeline_materializes_exact_probe_plan_without_channel_writes():
    engine = AsyncMock()
    engine.run_pipeline.return_value = {
        "planned_sort_probes": [{
            "stream_id": 41,
            "rule_id": 7,
            "stream_name": "Sports",
            "m3u_account_id": 3,
            "stream_url_hash": "a" * 64,
        }],
        "dry_run_results": [{
            "stream_id": 41,
            "stream_name": "Sports",
            "rule_id": 7,
            "phase": "probe",
            "action": "Would probe stream for quality sorting",
        }, {
            "stream_id": 41,
            "would_create": True,
        }],
        "channels_created": 1,
    }
    engine.client = AsyncMock()
    engine._existing_channels = []
    engine._load_rules.return_value = []
    with patch("routers.channel_pipeline._ensure_engine", AsyncMock(return_value=engine)), patch(
        "channel_pipeline_engine.ChannelPipelineEngine", return_value=engine
    ):
        response = await prepare_auto_creation_pipeline(
            RunPipelineRequest(dry_run=False, rule_ids=[7]), _admin=None
        )

    assert response["phase"] == "probe"
    assert response["write_count"] == response["unique_target_count"] == 1
    assert response["preview"]["stream_ids"] == [41]
    assert response["preview"]["dry_run_results"] == [{
        "stream_id": 41,
        "stream_name": "Sports",
        "rule_id": 7,
        "phase": "probe",
        "action": "Would probe stream for quality sorting",
    }]
    plan = mutation_plan_store.consume(
        response["plan_id"],
        "channel_pipeline_probe",
        response["plan_hash"],
        principal="api",
    )
    assert plan.payload["sort_probes"][0]["stream_url_hash"] == "a" * 64
    assert "url" not in response["preview"]["dry_run_results"][0]


@pytest.mark.asyncio
@pytest.mark.parametrize(("probe_count", "status_code"), [(499, None), (500, 413)])
async def test_probe_plan_enforces_distinct_target_hard_cap(probe_count, status_code):
    from routers.channel_pipeline import _materialize_pipeline_plan

    decision = {
        "request": {"m3u_account_ids": None, "rule_ids": [7]},
        "result": {"dry_run_results": []},
        "write_plan": {
            "writes": [],
            "channel_preconditions": {},
            "group_preconditions": {},
            "profile_preconditions": {},
        },
        "snapshot": [],
        "sort_probes": [
            {
                "stream_id": stream_id,
                "rule_id": 7,
                "stream_name": f"Stream {stream_id}",
                "m3u_account_id": 3,
                "stream_url_hash": "a" * 64,
            }
            for stream_id in range(1, probe_count + 1)
        ],
    }
    with patch(
        "routers.channel_pipeline._compute_pipeline_plan_payload",
        AsyncMock(return_value=decision),
    ):
        if status_code is not None:
            with pytest.raises(Exception) as caught:
                await _materialize_pipeline_plan(RunPipelineRequest(dry_run=False))
            assert getattr(caught.value, "status_code", None) == status_code
            return
        response = await _materialize_pipeline_plan(RunPipelineRequest(dry_run=False))

    assert response["write_count"] == response["unique_target_count"] == 499
    mutation_plan_store.consume(
        response["plan_id"], "channel_pipeline_probe", response["plan_hash"],
        principal="api",
    )


@pytest.mark.asyncio
async def test_late_final_plan_computation_stores_no_execute_plan():
    from datetime import datetime, timedelta, timezone
    from routers.channel_pipeline import _materialize_pipeline_plan

    decision = {
        "request": {"m3u_account_ids": None, "rule_ids": [7]},
        "result": {"dry_run_results": []},
        "write_plan": {
            "writes": [],
            "channel_preconditions": {},
            "group_preconditions": {},
            "profile_preconditions": {},
        },
        "snapshot": [],
        "sort_probes": [],
    }

    async def compute(_request):
        await asyncio.sleep(0.03)
        return decision

    with patch(
        "routers.channel_pipeline._compute_pipeline_plan_payload",
        side_effect=compute,
    ), patch.object(
        mutation_plan_store, "create", wraps=mutation_plan_store.create,
    ) as create_plan:
        with pytest.raises(asyncio.TimeoutError):
            await _materialize_pipeline_plan(
                RunPipelineRequest(dry_run=False),
                probe_before_run=False,
                expires_at=datetime.now(timezone.utc) + timedelta(milliseconds=10),
            )

    create_plan.assert_not_called()


@pytest.fixture
def probe_plan(monkeypatch):
    from channel_pipeline_engine import ChannelPipelineEngine
    from routers.channel_pipeline import _canonical_pipeline_decision
    from services.mutation_plan_store import canonical_hash
    from stream_prober import StreamProber

    stream_url = "http://media.example/41"
    probe = {
        "stream_id": 41,
        "rule_id": 7,
        "stream_name": "Sports",
        "m3u_account_id": 3,
        "stream_url_hash": hashlib.sha256(stream_url.encode("utf-8")).hexdigest(),
    }
    decision = {
        "request": {"m3u_account_ids": [3], "rule_ids": [7]},
        "result": {
            "dry_run_results": [{
                "stream_id": 41,
                "stream_name": "Sports",
                "rule_id": 7,
                "phase": "probe",
                "action": "Would probe stream for quality sorting",
            }],
            "channels_created": 1,
        },
        "write_plan": {
            "writes": [],
            "channel_preconditions": {},
            "group_preconditions": {},
            "profile_preconditions": {},
        },
        "snapshot": [],
        "sort_probes": [probe],
        "accounting": {"write_count": 1, "unique_target_count": 1},
    }
    plan = mutation_plan_store.create(
        "channel_pipeline_probe",
        decision,
        canonical_hash(_canonical_pipeline_decision(decision)),
        "api",
    )
    client = AsyncMock()
    client.get_streams_by_ids.return_value = [{
        "id": 41,
        "name": "Sports",
        "url": stream_url,
        "m3u_account": {"id": 3, "name": "Provider"},
    }]
    live_engine = MagicMock()
    live_engine.client = client
    rule = MagicMock()
    rule.id = 7
    rule.name = "Quality"
    rule.sort_field = "quality"
    rule.stream_sort_field = "quality"
    rule.probe_on_sort = True
    rule.get_event_sync_config.return_value = None
    prober = StreamProber(client=client, max_concurrent_probes=1)
    prober.account_probe_limits = {3: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock(return_value={"probe_status": "failed"})
    next_decision = deepcopy(decision)
    next_decision.pop("accounting")
    compute = AsyncMock(side_effect=[
        deepcopy(decision),
        deepcopy(next_decision),
        deepcopy(next_decision),
    ])
    load_stats = AsyncMock()
    load_rules = AsyncMock(return_value=[rule])
    setup = {
        "plan": plan,
        "probe": probe,
        "decision": decision,
        "next_decision": next_decision,
        "compute": compute,
        "client": client,
        "rule": rule,
        "prober": prober,
        "load_stats": load_stats,
        "load_rules": load_rules,
    }
    monkeypatch.setattr("routers.channel_pipeline._ensure_engine", AsyncMock(return_value=live_engine))
    monkeypatch.setattr("routers.channel_pipeline._compute_pipeline_plan_payload", compute)
    monkeypatch.setattr("stream_prober.get_prober", lambda: setup["prober"])
    monkeypatch.setattr(ChannelPipelineEngine, "_load_stream_stats", load_stats)
    monkeypatch.setattr(ChannelPipelineEngine, "_load_rules", load_rules)
    return setup


@pytest.mark.asyncio
async def test_probe_commit_uses_exact_identity_and_returns_new_execute_plan(probe_plan):
    from routers.channel_pipeline import (
        CommitPipelinePlanRequest,
        _materialize_pipeline_plan,
        commit_auto_creation_pipeline,
    )

    setup = probe_plan
    response = await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(
            plan_id=setup["plan"].plan_id,
            plan_hash=setup["plan"].payload_hash,
            phase="probe",
        ),
        _admin=None,
    )

    assert response["requires_confirmation"] is True
    assert response["completed_phase"] == "probe"
    assert response["phase"] == "execute"
    assert response["preview"]["dry_run_results"] == []
    setup["client"].get_streams_by_ids.assert_awaited_once_with([41])
    call = setup["prober"].probe_stream.await_args
    assert call.args == (41, "http://media.example/41", "Sports")
    assert call.kwargs["content"] is False
    assert call.kwargs["expires_at"].tzinfo is not None
    setup["prober"].refresh_account_probe_limits.assert_awaited_once_with(
        account_ids={3},
    )
    assert setup["prober"]._account_active == {}
    execute_plan = mutation_plan_store.consume(
        response["plan_id"], "channel_pipeline", response["plan_hash"],
        principal="api",
    )
    assert execute_plan.payload["sort_probes"] == [setup["probe"]]

    retry = await _materialize_pipeline_plan(
        RunPipelineRequest(dry_run=False, rule_ids=[7], m3u_account_ids=[3])
    )
    assert retry["phase"] == "probe"
    mutation_plan_store.consume(
        retry["plan_id"], "channel_pipeline_probe", retry["plan_hash"],
        principal="api",
    )


@pytest.mark.asyncio
async def test_probe_completion_starts_next_plan_review_window(probe_plan):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = probe_plan
    clock = [100.0]

    async def complete_probe(*_args, **_kwargs):
        clock[0] = 350.0
        return {"probe_status": "failed"}

    setup["prober"].probe_stream.side_effect = complete_probe
    with patch("services.mutation_plan_store.time.time", side_effect=lambda: clock[0]):
        response = await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=setup["plan"].plan_id,
                plan_hash=setup["plan"].payload_hash,
                phase="probe",
            ),
            _admin=None,
        )
        mutation_plan_store.consume(
            response["plan_id"], "channel_pipeline", response["plan_hash"],
            principal="api",
        )

    assert response["expires_at"] == 650.0


@pytest.mark.asyncio
async def test_probe_route_persists_results_and_replays_fresh_exact_write(
    test_engine, monkeypatch,
):
    from sqlalchemy.orm import sessionmaker

    from channel_pipeline_engine import ChannelPipelineEngine
    from models import ChannelPipelineRule, StreamStats
    from routers.channel_pipeline import (
        CommitPipelinePlanRequest,
        commit_auto_creation_pipeline,
    )
    from stream_prober import StreamProber
    from tests.unit.test_channel_pipeline_engine import _mk_smart_sort_settings

    sessions = sessionmaker(bind=test_engine, expire_on_commit=False)
    db = sessions()
    rule = ChannelPipelineRule(
        name="Quality order",
        enabled=True,
        priority=0,
        m3u_account_id=1,
        conditions=json.dumps([{
            "type": "stream_name_contains", "value": "Sports",
        }]),
        actions=json.dumps([{
            "type": "create_channel",
            "name_template": "Sports",
            "if_exists": "merge",
        }]),
        sort_field="quality",
        sort_order="desc",
        probe_on_sort=True,
        stream_sort_field="quality",
        stream_sort_order="desc",
        skip_struck_streams=True,
        orphan_action="none",
        allow_manual_channel_merge=True,
        match_scope_target_group=False,
    )
    rule.set_managed_channel_ids([200])
    db.add(rule)
    db.add_all([
        StreamStats(
            stream_id=102,
            stream_name="Sports recovered",
            probe_status="failed",
            consecutive_failures=3,
            resolution="3840x2160",
        ),
        StreamStats(
            stream_id=103,
            stream_name="Sports failed",
            probe_status="failed",
            consecutive_failures=3,
            resolution="1920x1080",
        ),
        StreamStats(
            stream_id=104,
            stream_name="Sports cached",
            probe_status="success",
            consecutive_failures=0,
            resolution="1280x720",
            fps="30",
            video_codec="h264",
        ),
    ])
    db.commit()
    rule_id = rule.id
    db.close()

    streams = {
        101: {
            "id": 101, "name": "Sports new", "url": "http://media/101",
            "m3u_account": {"id": 1, "name": "Primary"},
        },
        102: {
            "id": 102, "name": "Sports recovered", "url": "http://media/102",
            "m3u_account": {"id": 1, "name": "Primary"},
        },
        103: {
            "id": 103, "name": "Sports failed", "url": "http://media/103",
            "m3u_account": {"id": 1, "name": "Primary"},
        },
        104: {
            "id": 104, "name": "Sports cached", "url": "http://media/104",
            "m3u_account": {"id": 1, "name": "Primary"},
        },
        105: {
            "id": 105, "name": "Foreign", "url": "http://media/105",
            "m3u_account": {"id": 2, "name": "Foreign"},
        },
    }
    channels = {
        200: {
            "id": 200,
            "name": "Sports",
            "streams": [104, 101, 102],
            "channel_group_id": None,
            "epg_data_id": 7,
            "tvg_id": "sports",
            "logo_id": None,
            "auto_created": False,
        },
        201: {
            "id": 201,
            "name": "Foreign",
            "streams": [105],
            "channel_group_id": None,
            "epg_data_id": 8,
            "tvg_id": "foreign",
            "logo_id": None,
            "auto_created": False,
        },
    }
    foreign_channel = deepcopy(channels[201])
    foreign_stream = deepcopy(streams[105])

    async def get_channels(page=1, page_size=100, **_kwargs):
        assert page in (None, 1)
        return {
            "count": len(channels),
            "results": deepcopy(list(channels.values())),
            "next": None,
        }

    async def get_streams(page=1, page_size=1000, m3u_account=None):
        assert page == 1
        selected = [
            stream for stream in streams.values()
            if stream["m3u_account"]["id"] == m3u_account
        ]
        return {"count": len(selected), "results": deepcopy(selected)}

    async def get_streams_by_ids(stream_ids):
        return [deepcopy(streams[stream_id]) for stream_id in stream_ids]

    async def get_channel(channel_id):
        return deepcopy(channels[channel_id])

    async def update_channel(channel_id, changes):
        channel = channels[channel_id]
        for key, value in changes.items():
            channel[key] = deepcopy(value)
        return deepcopy(channel)

    client = MagicMock()
    client.get_channels = AsyncMock(side_effect=get_channels)
    client.get_channel_groups = AsyncMock(return_value=[])
    client.get_channel_profiles = AsyncMock(return_value=[])
    client.get_m3u_accounts = AsyncMock(return_value=[
        {"id": 1, "name": "Primary"},
        {"id": 2, "name": "Foreign"},
    ])
    client.get_streams = AsyncMock(side_effect=get_streams)
    client.get_streams_by_ids = AsyncMock(side_effect=get_streams_by_ids)
    client.get_channel = AsyncMock(side_effect=get_channel)
    client.get_channel_streams = AsyncMock(
        side_effect=lambda channel_id: list(channels[channel_id]["streams"])
    )
    client.update_channel = AsyncMock(side_effect=update_channel)
    client.create_channel = AsyncMock(
        side_effect=AssertionError("the existing channel must be reused")
    )
    client.delete_channel = AsyncMock()
    client.delete_channel_group = AsyncMock()
    client.update_profile_channel = AsyncMock()
    client.assign_channel_numbers = AsyncMock(return_value={})

    settings = _mk_smart_sort_settings(
        stream_sort_priority=["resolution"],
        stream_sort_enabled={"resolution": True},
        deprioritize_failed_streams=True,
        failed_stream_sort_order=["failed", "black_screen", "low_fps"],
    )
    settings.strike_threshold = 3
    settings.timezone_preference = "both"
    settings.include_channel_number_in_name = False
    settings.channel_number_separator = "-"
    settings.default_channel_profile_ids = []
    settings.auto_rename_channel_number = False
    settings.auto_creation_excluded_terms = []
    settings.auto_creation_excluded_groups = []
    settings.auto_creation_exclude_auto_sync_groups = False
    settings.max_auto_creation_log_entries = 1000
    settings.max_auto_created_channels_per_run = 0

    prober = StreamProber(
        client=client, max_concurrent_probes=1, probe_retry_count=0,
        black_screen_detection_enabled=False,
    )
    prober.account_probe_limits = {1: 1}
    prober.refresh_account_probe_limits = AsyncMock()

    async def read_media(url, *, expires_at=None):
        if url.endswith("/103"):
            raise RuntimeError("synthetic failed observation")
        height = 2160 if url.endswith("/102") else 1080
        return {
            "streams": [{
                "codec_type": "video",
                "width": 3840 if height == 2160 else 1920,
                "height": height,
                "codec_name": "h264",
                "r_frame_rate": "30/1",
            }, {
                "codec_type": "audio", "codec_name": "aac", "channels": 2,
            }],
            "format": {"format_name": "hls", "bit_rate": "5000000"},
        }

    async def measure_media(url, *, expires_at=None):
        return 8_000_000 if url.endswith("/102") else 5_000_000

    prober._run_ffprobe = AsyncMock(side_effect=read_media)
    prober._measure_stream_bitrate = AsyncMock(side_effect=measure_media)
    prober._push_stats_to_dispatcharr = AsyncMock()
    live_engine = ChannelPipelineEngine(client)

    monkeypatch.setattr("channel_pipeline_engine.get_session", sessions)
    monkeypatch.setattr("routers.channel_pipeline.get_session", sessions)
    monkeypatch.setattr("stream_prober.get_session", sessions)
    monkeypatch.setattr("channel_pipeline_engine.get_settings", lambda: settings)
    monkeypatch.setattr(
        "routers.channel_pipeline._ensure_engine",
        AsyncMock(return_value=live_engine),
    )
    monkeypatch.setattr("stream_prober.get_prober", lambda: prober)
    monkeypatch.setattr("channel_pipeline_engine.request_guide_refresh", AsyncMock())
    monkeypatch.setattr("journal.log_entries", MagicMock())

    prepared = await prepare_auto_creation_pipeline(
        RunPipelineRequest(dry_run=False, rule_ids=[rule_id]), _admin=None,
    )

    assert prepared["phase"] == "probe"
    assert set(prepared["preview"]["stream_ids"]) == {101, 102, 103}
    assert 104 not in prepared["preview"]["stream_ids"]
    assert 105 not in prepared["preview"]["stream_ids"]
    prober._run_ffprobe.assert_not_awaited()
    client.update_channel.assert_not_awaited()
    client.create_channel.assert_not_awaited()
    client.delete_channel.assert_not_awaited()
    client.update_profile_channel.assert_not_awaited()

    before_probe = sessions()
    try:
        assert before_probe.query(StreamStats).filter_by(
            stream_id=102,
        ).one().consecutive_failures == 3
        assert before_probe.query(StreamStats).filter_by(
            stream_id=103,
        ).one().consecutive_failures == 3
        assert before_probe.query(StreamStats).filter_by(stream_id=101).first() is None
    finally:
        before_probe.close()

    completed = await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(
            plan_id=prepared["plan_id"],
            plan_hash=prepared["plan_hash"],
            phase="probe",
        ),
        _admin=None,
    )

    assert completed["requires_confirmation"] is True
    assert completed["completed_phase"] == "probe"
    assert completed["phase"] == "execute"
    assert completed["write_count"] == 1
    assert completed["unique_target_count"] == 1
    assert completed["preview"]["streams_matched"] == 3
    assert completed["preview"]["rule_match_counts"] == {rule_id: 3}
    assert prober._run_ffprobe.await_count == 3
    assert prober._measure_stream_bitrate.await_count == 3
    client.update_channel.assert_not_awaited()

    after_probe = sessions()
    try:
        recovered = after_probe.query(StreamStats).filter_by(stream_id=102).one()
        first = after_probe.query(StreamStats).filter_by(stream_id=101).one()
        failed = after_probe.query(StreamStats).filter_by(stream_id=103).one()
        assert first.probe_status == "success"
        assert first.resolution == "1920x1080"
        assert recovered.probe_status == "success"
        assert recovered.consecutive_failures == 0
        assert recovered.resolution == "3840x2160"
        assert failed.probe_status == "failed"
        assert failed.consecutive_failures == 4
    finally:
        after_probe.close()

    for phase in ("execute", "probe"):
        with pytest.raises(Exception) as caught:
            await commit_auto_creation_pipeline(
                CommitPipelinePlanRequest(
                    plan_id=prepared["plan_id"],
                    plan_hash=prepared["plan_hash"],
                    phase=phase,
                ),
                _admin=None,
            )
        assert getattr(caught.value, "status_code", None) == 409
        client.update_channel.assert_not_awaited()

    response = await commit_auto_creation_pipeline(
        CommitPipelinePlanRequest(
            plan_id=completed["plan_id"],
            plan_hash=completed["plan_hash"],
            phase="execute",
        ),
        _admin=None,
    )
    body = json.loads(response.body)
    assert body["status"] == "completed"
    client.update_channel.assert_awaited_once_with(
        200, {"streams": [102, 101, 104]},
    )
    assert channels[200]["streams"] == [102, 101, 104]
    assert channels[201] == foreign_channel
    assert streams[105] == foreign_stream

    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=completed["plan_id"],
                plan_hash=completed["plan_hash"],
                phase="execute",
            ),
            _admin=None,
        )
    assert getattr(caught.value, "status_code", None) == 409
    assert client.update_channel.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("field", "value"), [
    ("name", "Changed Sports"),
    ("url", "http://media.example/changed"),
    ("m3u_account", {"id": 8, "name": "Changed Provider"}),
])
async def test_probe_commit_rejects_exact_identity_drift_before_media(
    probe_plan, field, value,
):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = probe_plan
    setup["client"].get_streams_by_ids.return_value[0][field] = value
    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=setup["plan"].plan_id,
                plan_hash=setup["plan"].payload_hash,
                phase="probe",
            ),
            _admin=None,
        )

    assert getattr(caught.value, "status_code", None) == 409
    assert "identities drifted" in str(getattr(caught.value, "detail", ""))
    setup["prober"].probe_stream.assert_not_awaited()
    assert setup["compute"].await_count == 1


@pytest.mark.asyncio
async def test_probe_commit_rejects_incomplete_batch_before_media(probe_plan):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = probe_plan
    setup["client"].get_streams_by_ids.return_value = []
    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=setup["plan"].plan_id,
                plan_hash=setup["plan"].payload_hash,
                phase="probe",
            ),
            _admin=None,
        )

    assert getattr(caught.value, "status_code", None) == 409
    setup["prober"].probe_stream.assert_not_awaited()


@pytest.mark.asyncio
async def test_probe_commit_requires_available_prober(probe_plan):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = probe_plan
    prober = setup["prober"]
    setup["prober"] = None
    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=setup["plan"].plan_id,
                plan_hash=setup["plan"].payload_hash,
                phase="probe",
            ),
            _admin=None,
        )

    assert getattr(caught.value, "status_code", None) == 503
    prober.probe_stream.assert_not_awaited()
    assert setup["compute"].await_count == 1


@pytest.mark.asyncio
async def test_probe_commit_rejects_rule_drift_before_media(probe_plan):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = probe_plan
    setup["load_rules"].return_value = []
    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=setup["plan"].plan_id,
                plan_hash=setup["plan"].payload_hash,
                phase="probe",
            ),
            _admin=None,
        )

    assert getattr(caught.value, "status_code", None) == 409
    assert "rules drifted" in str(getattr(caught.value, "detail", ""))
    setup["prober"].probe_stream.assert_not_awaited()


@pytest.mark.asyncio
async def test_probe_commit_cancellation_releases_account_permit_and_creates_no_next_plan(
    probe_plan,
):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = probe_plan
    setup["prober"].probe_stream.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=setup["plan"].plan_id,
                plan_hash=setup["plan"].payload_hash,
                phase="probe",
            ),
            _admin=None,
        )

    assert setup["prober"]._account_active == {}
    assert setup["compute"].await_count == 1


@pytest.mark.asyncio
async def test_probe_commit_lock_expiry_returns_terminal_receipt_and_stays_consumed(
    probe_plan, monkeypatch,
):
    from routers.channel_pipeline import (
        CommitPipelinePlanRequest,
        _MCP_PLANNED_RUN_LOCK,
        commit_auto_creation_pipeline,
    )

    setup = probe_plan
    monkeypatch.setattr(
        "routers.channel_pipeline._PREPARED_PROBE_SECONDS", 0.03,
        raising=False,
    )
    await _MCP_PLANNED_RUN_LOCK.acquire()
    try:
        with pytest.raises(Exception) as caught:
            await asyncio.wait_for(
                commit_auto_creation_pipeline(
                    CommitPipelinePlanRequest(
                        plan_id=setup["plan"].plan_id,
                        plan_hash=setup["plan"].payload_hash,
                        phase="probe",
                    ),
                    _admin=None,
                ),
                timeout=0.5,
            )
    finally:
        _MCP_PLANNED_RUN_LOCK.release()

    assert getattr(caught.value, "status_code", None) == 504
    detail = caught.value.detail
    assert set(detail) == {
        "phase", "status", "plan_id", "stage", "expires_at", "message",
    }
    assert detail == {
        "phase": "probe",
        "status": "expired",
        "plan_id": setup["plan"].plan_id,
        "stage": "lock",
        "expires_at": detail["expires_at"],
        "message": (
            "Prepared stream probes expired. Completed observations were retained. "
            "Prepare a new plan; channel writes were not applied."
        ),
    }
    assert detail["expires_at"].endswith("+00:00")
    assert setup["compute"].await_count == 0
    setup["prober"].probe_stream.assert_not_awaited()
    await asyncio.sleep(0)
    setup["prober"].probe_stream.assert_not_awaited()

    with pytest.raises(Exception) as replay:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=setup["plan"].plan_id,
                plan_hash=setup["plan"].payload_hash,
                phase="probe",
            ),
            _admin=None,
        )
    assert getattr(replay.value, "status_code", None) == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked_stage", ["preflight", "probe", "plan"])
async def test_probe_commit_expiry_reports_coarse_stage_and_creates_no_next_plan(
    probe_plan, monkeypatch, blocked_stage,
):
    from routers.channel_pipeline import (
        CommitPipelinePlanRequest,
        commit_auto_creation_pipeline,
    )
    from services.mutation_plan_store import mutation_plan_store

    setup = probe_plan
    monkeypatch.setattr(
        "routers.channel_pipeline._PREPARED_PROBE_SECONDS", 0.03,
    )
    blocked = asyncio.Event()

    async def wait_forever(*_args, **_kwargs):
        blocked.set()
        await asyncio.Event().wait()

    if blocked_stage == "preflight":
        setup["compute"].side_effect = wait_forever
    elif blocked_stage == "probe":
        setup["prober"].probe_stream.side_effect = wait_forever
    else:
        calls = 0

        async def compute(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                return deepcopy(setup["decision"])
            return await wait_forever()

        setup["compute"].side_effect = compute

    with patch.object(
        mutation_plan_store, "create", wraps=mutation_plan_store.create,
    ) as create_plan:
        with pytest.raises(Exception) as caught:
            await commit_auto_creation_pipeline(
                CommitPipelinePlanRequest(
                    plan_id=setup["plan"].plan_id,
                    plan_hash=setup["plan"].payload_hash,
                    phase="probe",
                ),
                _admin=None,
            )

    assert blocked.is_set()
    assert getattr(caught.value, "status_code", None) == 504
    assert caught.value.detail["stage"] == blocked_stage
    assert caught.value.detail["plan_id"] == setup["plan"].plan_id
    assert setup["prober"]._account_active == {}
    create_plan.assert_not_called()


@pytest.mark.asyncio
async def test_probe_commit_identity_read_uses_operation_expiry(
    probe_plan, monkeypatch,
):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = probe_plan
    monkeypatch.setattr(
        "routers.channel_pipeline._PREPARED_PROBE_SECONDS", 0.03,
    )
    identity_started = asyncio.Event()

    async def wait_for_identity(*_args, **_kwargs):
        identity_started.set()
        await asyncio.Event().wait()

    setup["client"].get_streams_by_ids.side_effect = wait_for_identity

    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=setup["plan"].plan_id,
                plan_hash=setup["plan"].payload_hash,
                phase="probe",
            ),
            _admin=None,
        )

    assert identity_started.is_set()
    assert getattr(caught.value, "status_code", None) == 504
    assert caught.value.detail["stage"] == "preflight"
    setup["prober"].probe_stream.assert_not_awaited()
    assert setup["prober"]._account_active == {}


@pytest.mark.asyncio
async def test_probe_commit_dependency_timeout_before_expiry_keeps_dependency_meaning(
    probe_plan,
):
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline

    setup = probe_plan
    setup["compute"].side_effect = asyncio.TimeoutError("dependency timeout")

    with pytest.raises(asyncio.TimeoutError, match="dependency timeout"):
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=setup["plan"].plan_id,
                plan_hash=setup["plan"].payload_hash,
                phase="probe",
            ),
            _admin=None,
        )

    setup["prober"].probe_stream.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_probe_phase_does_not_consume_plan():
    from routers.channel_pipeline import CommitPipelinePlanRequest, commit_auto_creation_pipeline
    from services.mutation_plan_store import canonical_hash

    decision = {"request": {}, "result": {}, "write_plan": {}, "snapshot": [], "sort_probes": []}
    plan = mutation_plan_store.create(
        "channel_pipeline", decision, canonical_hash(decision), "api"
    )
    with pytest.raises(Exception) as caught:
        await commit_auto_creation_pipeline(
            CommitPipelinePlanRequest(
                plan_id=plan.plan_id,
                plan_hash=plan.payload_hash,
                phase="unknown",
            ),
            _admin=None,
        )

    assert getattr(caught.value, "status_code", None) == 409
    mutation_plan_store.consume(
        plan.plan_id, "channel_pipeline", plan.payload_hash, principal="api"
    )

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
        f"programme_reads={setup['client'].get_epg_programmes.await_count} "
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
