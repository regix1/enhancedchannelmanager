import ast
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from services.pipeline_write_plan import (
    EVENT_PROMOTE_METHOD, PIPELINE_INTERNAL_SIDE_EFFECTS, PIPELINE_WRITE_METHODS,
    EventAllocationClient, PartialReplayError, PlanningDispatcharrClient, PipelineWritePlan,
    PlannedWrite, journal_entries_for_plan, replay_write_plan,
)


def _event_write(result_id=-1, *, stream_count=1, stale_count=0):
    def row(stream_id, stale=False):
        return {
            "stream": {
                "name": f"Event stream {stream_id}",
                "group_id": 20,
                "stream_id": stream_id,
                "provider": "Source",
                "provider_id": 3,
                "name_seen_before_today": False,
                "is_stale": stale,
            },
            "disposition": "unmatched",
            "parsed": {
                "raw_name": f"Event stream {stream_id}",
                "title": "Event",
                "start": "2026-07-11T20:00:00+00:00",
                "teams": None,
                "matched_pattern": "slot-title-at-datetime",
            },
        }

    rows = [row(index + 1) for index in range(stream_count)]
    stale = [row(100 + index, True) for index in range(stale_count)]
    operation = {
        "rule_id": 4,
        "rule_name": "Events",
        "config": {
            "promote_target_group_id": 30,
            "dummy_epg_profile_id": 7,
        },
        "profile_id": 7,
        "profile_hash": "a" * 64,
        "source_id": 9,
        "source_hashes": [{
            "endpoint_hash": "b" * 64,
            "source_url_hash": "c" * 64,
        }],
        "expected_revision": 0,
        "expected_hash": None,
        "channel_uuid": None,
        "unit": {
            "event_key": "event-key",
            "channel_name": "Event",
            "dateless": False,
            "action": "create",
            "existing_channel_id": None,
            "rows": rows,
        },
        "allocation_writes": [{
            "method": "create_channel",
            "args": [{
                "name": "Event",
                "channel_group_id": 30,
                "channel_number": 1,
                "streams": [],
                "hidden_from_output": True,
            }],
            "kwargs": {},
            "result_id": result_id,
        }],
        "default_profile_ids": [],
        "stale_streams": stale,
        "working_stream_ids": [row["stream"]["stream_id"] for row in rows],
    }
    return PlannedWrite(EVENT_PROMOTE_METHOD, [operation], {}, result_id)


def test_authoritative_accounting_counts_nested_operations_and_unique_targets():
    plan = PipelineWritePlan(writes=[
        PlannedWrite("assign_channel_numbers", [[1, 2, 3], 10], {}),
        PlannedWrite("update_channel", [1, {"name": "x"}], {}),
        PlannedWrite("update_profile_channel", [9, 2, {"enabled": True}], {}),
        PlannedWrite("create_channel", [{"name": "new"}], {}),
    ])
    assert plan.accounting() == {"write_count": 4, "unique_target_count": 4}


def test_event_accounting_counts_approved_nested_bounds():
    plan = PipelineWritePlan(writes=[
        _event_write(stream_count=2, stale_count=1),
    ])
    assert plan.accounting() == {"write_count": 6, "unique_target_count": 1}


def test_event_accounting_reaches_499_and_500_without_collapsing_nested_writes():
    ordinary = [
        PlannedWrite("create_logo", [{"name": f"Logo {index}"}], {}, -(index + 2))
        for index in range(496)
    ]
    below = PipelineWritePlan(writes=[_event_write(), *ordinary[:495]])
    at_cap = PipelineWritePlan(writes=[_event_write(), *ordinary])
    assert below.accounting()["write_count"] == 499
    assert at_cap.accounting()["write_count"] == 500


@pytest.mark.asyncio
async def test_event_allocation_full_preflight_refuses_omission_before_write():
    live = AsyncMock()
    write = _event_write()
    approved = write.args[0]["allocation_writes"] + [{
        "method": "update_profile_channel",
        "args": [7, -1, {"enabled": False}],
        "kwargs": {},
        "result_id": None,
    }]
    check = EventAllocationClient(live, approved, -1, forward=False)
    result = await check.create_channel(write.args[0]["allocation_writes"][0]["args"][0])
    assert result["id"] == -1
    with pytest.raises(ValueError, match="omitted"):
        check.finish()
    live.create_channel.assert_not_awaited()
    live.update_profile_channel.assert_not_awaited()


def test_every_pipeline_dispatcharr_write_chokepoint_is_recorded():
    root = Path(__file__).parents[2]
    discovered = set()
    for filename in (root / "channel_pipeline_engine.py", root / "channel_pipeline_executor.py"):
        tree = ast.parse(filename.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                owner = node.func.value
                if isinstance(owner, ast.Attribute) and owner.attr == "client":
                    if node.func.attr.startswith(("create_", "update_", "delete_", "assign_")):
                        discovered.add(node.func.attr)
    assert discovered <= PIPELINE_WRITE_METHODS


def test_pipeline_side_effect_ast_inventory_is_not_limited_to_client_calls():
    root = Path(__file__).parents[2]
    sink_names = {
        "commit", "log_entries", "probe_stream", "_batch_probe_streams",
        "_probe_unprobed_streams", "_capture_snapshot", "_save_execution",
        "_update_rule_stats", "_record_conflict", "_refresh_dummy_epg_and_retry",
        "_prerefresh_event_sync_providers",
    }
    discovered = set()
    for filename in (root / "channel_pipeline_engine.py", root / "channel_pipeline_executor.py"):
        tree = ast.parse(filename.read_text())
        discovered.update(
            node.func.attr for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr in sink_names
        )
    # Dangerous mutant: removing the broad commit/journal/prober inventory
    # makes this fail even if every ``self.client`` write remains wrapped.
    assert {"commit", "log_entries", "probe_stream"} <= discovered


def test_internal_side_effect_parity_inventory_is_explicit_and_complete():
    assert PIPELINE_INTERNAL_SIDE_EFFECTS == {
        "execution_record", "rollback_snapshot", "journal_entries",
        "event_review_candidates", "rule_statistics", "conflict_records",
        "database_commit", "managed_channel_ledger", "stream_probe",
        "provider_refresh", "dummy_epg_refresh", "xmltv_cache",
        "notification", "live_data_refresh",
    }


@pytest.mark.asyncio
async def test_recorder_uses_deterministic_temp_ids_and_never_writes():
    live = AsyncMock()
    planner = PlanningDispatcharrClient(live)
    one = await planner.create_channel({"name": "One"})
    two = await planner.create_channel_group("Two")
    assert (one["id"], two["id"]) == (-1, -2)
    live.create_channel.assert_not_awaited()
    live.create_channel_group.assert_not_awaited()


@pytest.mark.asyncio
async def test_replay_validates_all_preconditions_before_first_write_and_remaps_ids():
    live = AsyncMock()
    live.get_channel.return_value = {"id": 7, "name": "Old", "streams": []}
    live.create_channel.return_value = {"id": 101}
    plan = PipelineWritePlan(
        writes=[
            PlannedWrite("create_channel", [{"name": "New"}], {}),
            PlannedWrite("update_channel", [-1, {"streams": [5]}], {}),
        ],
        channel_preconditions={"7": {"id": 7, "name": "Old", "streams": []}},
    )
    _, remap = await replay_write_plan(live, plan)
    assert remap == {-1: 101}
    live.update_channel.assert_awaited_once_with(101, {"streams": [5]})


@pytest.mark.asyncio
async def test_drift_rejects_before_any_replay_write():
    live = AsyncMock()
    live.get_channel.return_value = {"id": 7, "name": "Changed", "streams": []}
    plan = PipelineWritePlan(
        writes=[PlannedWrite("delete_channel", [7], {})],
        channel_preconditions={"7": {"id": 7, "name": "Old", "streams": []}},
    )
    with pytest.raises(ValueError, match="drifted"):
        await replay_write_plan(live, plan)
    live.delete_channel.assert_not_awaited()


@pytest.mark.asyncio
async def test_event_replay_requires_callback_before_any_write():
    live = AsyncMock()
    plan = PipelineWritePlan(writes=[_event_write()])
    with pytest.raises(ValueError, match="support is required"):
        await replay_write_plan(live, plan)
    live.create_channel.assert_not_awaited()


@pytest.mark.asyncio
async def test_event_result_remaps_later_ordinary_writes_once():
    live = AsyncMock()
    live.create_channel.return_value = {"id": 101}
    live.update_channel.return_value = {"id": 202}
    event = AsyncMock(return_value={"id": 202})
    plan = PipelineWritePlan(writes=[
        PlannedWrite("create_channel", [{"name": "Ordinary"}], {}, -1),
        _event_write(result_id=-2),
        PlannedWrite("update_channel", [-2, {"name": "Updated"}], {}),
    ])
    _, remap = await replay_write_plan(live, plan, event_operation=event)
    assert remap == {-1: 101, -2: 202}
    live.update_channel.assert_awaited_once_with(202, {"name": "Updated"})
    event.assert_awaited_once()


@pytest.mark.asyncio
async def test_later_failure_retains_event_recovery_and_ordinary_compensation():
    live = AsyncMock()
    live.create_channel.return_value = {"id": 101}
    live.update_channel.side_effect = RuntimeError("later failure")
    event = AsyncMock(return_value={"id": 202})
    plan = PipelineWritePlan(writes=[
        PlannedWrite("create_channel", [{"name": "Ordinary"}], {}, -1),
        _event_write(result_id=-2),
        PlannedWrite("update_channel", [-2, {"name": "Updated"}], {}),
    ])
    with pytest.raises(PartialReplayError) as caught:
        await replay_write_plan(live, plan, event_operation=event)
    live.delete_channel.assert_awaited_once_with(101)
    assert all(call.args != (202,) for call in live.delete_channel.await_args_list)
    assert any("recovery-retained" in value for value in caught.value.completed)


@pytest.mark.asyncio
async def test_event_failure_reports_recovery_only_when_callback_proves_it():
    live = AsyncMock()
    plan = PipelineWritePlan(writes=[_event_write()])

    async def fail_without_recovery(*_args):
        raise ValueError("preflight rejected")

    with pytest.raises(PartialReplayError) as unretained:
        await replay_write_plan(live, plan, event_operation=fail_without_recovery)
    assert unretained.value.completed == []

    async def fail_with_recovery(*_args):
        error = ValueError("admitted write failed")
        error.event_recovery_retained = True
        raise error

    with pytest.raises(PartialReplayError) as retained:
        await replay_write_plan(live, plan, event_operation=fail_with_recovery)
    assert retained.value.completed == [
        f"{EVENT_PROMOTE_METHOD}:-1:recovery-retained"
    ]


def test_event_operation_has_no_synthetic_journal_entry():
    plan = PipelineWritePlan(writes=[
        _event_write(),
        PlannedWrite("update_channel", [7, {"name": "Updated"}], {}),
    ])
    entries = journal_entries_for_plan(plan, {-1: 22}, 5)
    assert len(entries) == 1
    assert entries[0]["entity_id"] == 7
