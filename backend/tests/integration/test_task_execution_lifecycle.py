"""Durable task admission and exact execution identity tests."""

import asyncio
import gc
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from export_models import SyncTarget
from models import TaskExecution
from task_engine import TaskEngine, TaskHistoryError
from task_scheduler import TaskResult, TaskScheduler


class _GatedTask(TaskScheduler):
    task_id = "test_task_execution_gate"
    task_name = "Task execution gate"

    def __init__(self, entered: asyncio.Event, release: asyncio.Event):
        super().__init__()
        self.entered = entered
        self.release = release
        self.executions = 0

    async def execute(self) -> TaskResult:
        self.executions += 1
        self.entered.set()
        await self.release.wait()
        return TaskResult(success=True, message="finished", total_items=1, success_count=1)


class _FailingTask(TaskScheduler):
    task_id = "test_task_execution_failure"
    task_name = "Task execution failure"

    async def execute(self) -> TaskResult:
        raise RuntimeError("controlled task failure")


class _ValidatingTask(TaskScheduler):
    task_id = "test_task_execution_validation"
    task_name = "Task execution validation"

    def __init__(self, entered: asyncio.Event, release: asyncio.Event):
        super().__init__()
        self.entered = entered
        self.release = release
        self.executions = 0

    async def validate_config(self) -> tuple[bool, str]:
        self.entered.set()
        await self.release.wait()
        return True, ""

    async def execute(self) -> TaskResult:
        self.executions += 1
        return TaskResult(success=True, message="unexpected")


class _InvalidTask(TaskScheduler):
    task_id = "test_task_execution_invalid"
    task_name = "Task execution invalid"

    def __init__(self):
        super().__init__()
        self.executions = 0

    async def validate_config(self) -> tuple[bool, str]:
        return False, "controlled invalid configuration"

    async def execute(self) -> TaskResult:
        self.executions += 1
        return TaskResult(success=True, message="unexpected")


def _engine_harness(test_engine, task):
    sessions = sessionmaker(bind=test_engine, expire_on_commit=False)
    registry = MagicMock()
    registry.get_task_instance.side_effect = (
        lambda task_id: task if task_id == task.task_id else None
    )
    engine = TaskEngine()
    engine._notify_task_result = AsyncMock()
    return engine, sessions, registry


@pytest.mark.asyncio
async def test_start_task_commits_running_row_before_body_finishes(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
    ):
        admitted = await engine.start_task(task.task_id)
        await asyncio.wait_for(entered.wait(), timeout=1)

        session = sessions()
        try:
            row = session.query(TaskExecution).one()
            assert row.id == admitted.execution_id
            assert row.task_id == admitted.task_id
            assert row.started_at == admitted.started_at
            assert row.status == "running"
        finally:
            session.close()

        assert not admitted.completion.done()
        release.set()
        result = await asyncio.wait_for(asyncio.shield(admitted.completion), timeout=1)

    assert result.success is True
    assert task.executions == 1


@pytest.mark.asyncio
async def test_async_http_start_returns_accepted_while_body_is_gated(
    async_client, test_engine
):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.get_engine", return_value=engine),
        patch("task_engine.log_entry"),
    ):
        response = await async_client.post(f"/api/tasks/{task.task_id}/runs")
        await asyncio.wait_for(entered.wait(), timeout=1)
        admitted = engine._runs[task.task_id]

        assert response.status_code == 202
        assert response.json()["execution_id"] == admitted.execution_id
        assert not admitted.completion.done()
        session = sessions()
        try:
            assert session.get(TaskExecution, admitted.execution_id).status == "running"
        finally:
            session.close()

        release.set()
        result = await asyncio.wait_for(asyncio.shield(admitted.completion), timeout=1)

    assert result.success is True


@pytest.mark.asyncio
async def test_legacy_http_run_keeps_waiting_for_terminal_result(
    async_client, test_engine
):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.get_engine", return_value=engine),
        patch("task_engine.log_entry"),
    ):
        request = asyncio.create_task(
            async_client.post(f"/api/tasks/{task.task_id}/run")
        )
        await asyncio.wait_for(entered.wait(), timeout=1)
        await asyncio.sleep(0)
        assert not request.done()
        release.set()
        response = await asyncio.wait_for(request, timeout=1)

    assert response.status_code == 200
    assert response.json()["success"] is True


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_cancel_engine_owned_body(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
    ):
        waiter = asyncio.create_task(engine.run_task(task.task_id))
        await asyncio.wait_for(entered.wait(), timeout=1)
        admitted = engine._runs[task.task_id]
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        assert not admitted.completion.done()
        release.set()
        result = await asyncio.wait_for(asyncio.shield(admitted.completion), timeout=1)

        session = sessions()
        try:
            row = session.get(TaskExecution, admitted.execution_id)
            assert row.status == "completed"
            assert row.success is True
        finally:
            session.close()

    assert result.success is True
    assert task.executions == 1


@pytest.mark.asyncio
async def test_duplicate_async_admission_has_no_second_row_or_body(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
    ):
        admitted = await engine.start_task(task.task_id)
        await asyncio.wait_for(entered.wait(), timeout=1)
        refused = await engine.start_task(task.task_id)

        assert refused.error == "ALREADY_RUNNING"
        session = sessions()
        try:
            assert session.query(TaskExecution).count() == 1
        finally:
            session.close()

        release.set()
        await asyncio.wait_for(asyncio.shield(admitted.completion), timeout=1)

    assert task.executions == 1


@pytest.mark.asyncio
async def test_failure_is_persisted_once_and_releases_admission(test_engine):
    task = _FailingTask()
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
    ):
        admitted = await engine.start_task(task.task_id)
        result = await asyncio.wait_for(asyncio.shield(admitted.completion), timeout=1)

        session = sessions()
        try:
            rows = session.query(TaskExecution).all()
            assert len(rows) == 1
            assert rows[0].status == "failed"
            assert rows[0].success is False
            assert rows[0].error == "controlled task failure"
        finally:
            session.close()

    assert result.success is False
    assert task.task_id not in engine.active_task_ids


@pytest.mark.asyncio
async def test_early_explicit_cancellation_never_enters_task_body(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)
    captured = []

    def hold(coroutine):
        captured.append(coroutine)
        return MagicMock()

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
        patch.object(engine, "_track_job", side_effect=hold),
    ):
        admitted = await engine.start_task(task.task_id)
        cancellation = await engine.cancel_task(task.task_id)
        job = asyncio.create_task(captured.pop())
        result = await asyncio.wait_for(asyncio.shield(admitted.completion), timeout=1)
        await job

    assert cancellation["status"] == "cancelling"
    assert result.error == "CANCELLED"
    assert task.executions == 0


@pytest.mark.asyncio
async def test_cancellation_during_validation_skips_execute(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _ValidatingTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
    ):
        admitted = await engine.start_task(task.task_id)
        await asyncio.wait_for(entered.wait(), timeout=1)
        cancellation = await engine.cancel_task(task.task_id)
        release.set()
        result = await asyncio.wait_for(asyncio.shield(admitted.completion), timeout=1)

    assert cancellation["status"] == "cancelling"
    assert result.error == "CANCELLED"
    assert task.executions == 0


@pytest.mark.asyncio
async def test_invalid_configuration_reaches_terminal_history(test_engine):
    task = _InvalidTask()
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
    ):
        admitted = await engine.start_task(task.task_id)
        result = await asyncio.wait_for(asyncio.shield(admitted.completion), timeout=1)

    session = sessions()
    try:
        row = session.get(TaskExecution, admitted.execution_id)
        assert row.status == "failed"
        assert row.error == "CONFIG_INVALID"
    finally:
        session.close()
    assert result.error == "CONFIG_INVALID"
    assert result.completed_at is not None
    assert task.executions == 0


@pytest.mark.asyncio
async def test_body_cancellation_does_not_leak_into_later_run(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
    ):
        first = await engine.start_task(task.task_id)
        await asyncio.wait_for(entered.wait(), timeout=1)
        cancellation = await engine.cancel_task(task.task_id)
        release.set()
        first_result = await asyncio.wait_for(
            asyncio.shield(first.completion), timeout=1
        )

        second = await engine.start_task(task.task_id)
        second_result = await asyncio.wait_for(
            asyncio.shield(second.completion), timeout=1
        )

    assert cancellation["status"] == "cancelling"
    assert first_result.error == "CANCELLED"
    assert second_result.success is True
    assert second_result.error is None
    assert task.executions == 2


@pytest.mark.asyncio
async def test_admission_insert_failure_starts_no_body(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, _, registry = _engine_harness(test_engine, task)
    session = MagicMock()
    session.commit.side_effect = RuntimeError("controlled insert failure")

    with (
        patch("task_engine.get_session", return_value=session),
        patch("task_engine.get_registry", return_value=registry),
    ):
        with pytest.raises(RuntimeError, match="controlled insert failure"):
            await engine.start_task(task.task_id)

    assert task.executions == 0
    assert not engine.active_task_ids
    assert not engine._jobs


@pytest.mark.asyncio
async def test_launch_failure_marks_row_failed_without_starting_body(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    def fail_launch(coroutine):
        coroutine.close()
        raise RuntimeError("controlled launch failure")

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch.object(engine, "_track_job", side_effect=fail_launch),
    ):
        with pytest.raises(RuntimeError, match="controlled launch failure"):
            await engine.start_task(task.task_id)

    session = sessions()
    try:
        row = session.query(TaskExecution).one()
        assert row.status == "failed"
        assert row.error == "TASK_LAUNCH_FAILED"
    finally:
        session.close()
    assert task.executions == 0
    assert not engine.active_task_ids


@pytest.mark.asyncio
async def test_terminal_connection_failure_retries_persistence_only(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)
    calls = 0

    def session_with_one_terminal_failure():
        nonlocal calls
        calls += 1
        session = sessions()
        if calls == 3:
            session.commit = MagicMock(
                side_effect=OperationalError(
                    "controlled terminal write",
                    {},
                    RuntimeError("connection lost"),
                )
            )
        return session

    sleep = AsyncMock()
    with (
        patch("task_engine.get_session", side_effect=session_with_one_terminal_failure),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
        patch("task_engine.asyncio.sleep", sleep),
    ):
        admitted = await engine.start_task(task.task_id)
        await asyncio.wait_for(entered.wait(), timeout=1)
        release.set()
        result = await asyncio.wait_for(asyncio.shield(admitted.completion), timeout=1)

    session = sessions()
    try:
        row = session.get(TaskExecution, admitted.execution_id)
        assert row.status == "completed"
    finally:
        session.close()
    assert result.success is True
    assert task.executions == 1
    assert (1,) in [entry.args for entry in sleep.await_args_list]


@pytest.mark.asyncio
async def test_missing_accepted_row_fails_completion_without_second_body(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
    ):
        admitted = await engine.start_task(task.task_id)
        await asyncio.wait_for(entered.wait(), timeout=1)
        session = sessions()
        session.query(TaskExecution).delete()
        session.commit()
        session.close()
        release.set()

        with pytest.raises(TaskHistoryError):
            await asyncio.wait_for(asyncio.shield(admitted.completion), timeout=1)

    assert task.executions == 1
    assert not engine.active_task_ids


@pytest.mark.asyncio
async def test_engine_retains_job_after_request_references_are_dropped(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
    ):
        admitted = await engine.start_task(task.task_id)
        await asyncio.wait_for(entered.wait(), timeout=1)
        execution_id = admitted.execution_id
        del admitted
        gc.collect()

        assert engine._jobs
        retained = engine._runs[task.task_id]
        release.set()
        result = await asyncio.wait_for(asyncio.shield(retained.completion), timeout=1)

    assert result.success is True
    session = sessions()
    try:
        assert session.get(TaskExecution, execution_id).status == "completed"
    finally:
        session.close()


@pytest.mark.asyncio
async def test_stop_uses_one_expiry_and_leaves_no_false_success(test_engine):
    entered = asyncio.Event()
    release = asyncio.Event()
    task = _GatedTask(entered, release)
    engine, sessions, registry = _engine_harness(test_engine, task)

    with (
        patch("task_engine.get_session", side_effect=sessions),
        patch("task_engine.get_registry", return_value=registry),
        patch("task_engine.log_entry"),
    ):
        admitted = await engine.start_task(task.task_id)
        await asyncio.wait_for(entered.wait(), timeout=1)
        clock = MagicMock(side_effect=[0, 25, 30, 30])
        with patch(
            "task_engine.asyncio.get_running_loop",
            return_value=SimpleNamespace(time=clock),
        ):
            await engine.stop()
        await asyncio.sleep(0.2)

    assert admitted.completion.cancelled()
    assert engine._stopping is True
    session = sessions()
    try:
        row = session.get(TaskExecution, admitted.execution_id)
        assert row.success is not True
        assert row.status in {"running", "cancelled"}
    finally:
        session.close()


def test_exact_execution_lookup_requires_full_identity(test_engine):
    sessions = sessionmaker(bind=test_engine, expire_on_commit=False)
    started_at = datetime.utcnow() - timedelta(minutes=5)
    session = sessions()
    row = TaskExecution(
        task_id="stream_probe",
        started_at=started_at,
        completed_at=started_at + timedelta(seconds=3),
        duration_seconds=3,
        status="completed",
        success=True,
        triggered_by="manual",
    )
    session.add(row)
    session.commit()
    execution_id = row.id
    session.close()

    engine = TaskEngine()
    with patch("task_engine.get_session", side_effect=sessions):
        exact = engine.get_task_execution("stream_probe", execution_id, started_at)
        wrong_task = engine.get_task_execution("epg_refresh", execution_id, started_at)
        wrong_time = engine.get_task_execution(
            "stream_probe",
            execution_id,
            started_at.replace(tzinfo=timezone.utc) + timedelta(seconds=1),
        )

        session = sessions()
        session.query(TaskExecution).filter(TaskExecution.id == execution_id).delete()
        session.commit()
        session.close()
        deleted = engine.get_task_execution("stream_probe", execution_id, started_at)

        session = sessions()
        session.add(
            TaskExecution(
                id=execution_id,
                task_id="stream_probe",
                started_at=started_at + timedelta(hours=1),
                status="running",
                triggered_by="manual",
            )
        )
        session.commit()
        session.close()
        reused = engine.get_task_execution("stream_probe", execution_id, started_at)

    assert exact["id"] == execution_id
    assert exact["status"] == "completed"
    assert wrong_task is None
    assert wrong_time is None
    assert deleted is None
    assert reused is None


def test_exact_execution_lookup_applies_sync_target_lifetime(test_engine):
    sessions = sessionmaker(bind=test_engine, expire_on_commit=False)
    created_at = datetime.utcnow()
    old_started_at = created_at - timedelta(minutes=1)
    new_started_at = created_at + timedelta(minutes=1)
    session = sessions()
    session.add(
        SyncTarget(
            id=7,
            name="Target B",
            base_url="https://target.example.com",
            credentials="{}",
            enabled=True,
            credential_version=1,
            insecure=False,
            created_at=created_at,
        )
    )
    old_row = TaskExecution(
        task_id="dbas_sync_7",
        started_at=old_started_at,
        status="terminated",
        success=False,
        triggered_by="scheduled",
    )
    new_row = TaskExecution(
        task_id="dbas_sync_7",
        started_at=new_started_at,
        status="running",
        triggered_by="scheduled",
    )
    session.add_all([old_row, new_row])
    session.commit()
    old_id, new_id = old_row.id, new_row.id
    session.close()

    engine = TaskEngine()
    with patch("task_engine.get_session", side_effect=sessions):
        assert engine.get_task_execution("dbas_sync_7", old_id, old_started_at) is None
        current = engine.get_task_execution("dbas_sync_7", new_id, new_started_at)

    assert current["id"] == new_id


def test_restart_repair_terminates_old_identity_even_with_newer_run(test_engine):
    sessions = sessionmaker(bind=test_engine, expire_on_commit=False)
    old_started_at = datetime.utcnow() - timedelta(minutes=5)
    session = sessions()
    old = TaskExecution(
        task_id="stream_probe",
        started_at=old_started_at,
        status="running",
        triggered_by="manual",
    )
    session.add(old)
    session.commit()
    old_id = old.id
    session.close()

    engine = TaskEngine()
    with patch("task_engine.get_session", side_effect=sessions):
        engine._cleanup_stale_executions()
        session = sessions()
        session.add(
            TaskExecution(
                task_id="stream_probe",
                started_at=datetime.utcnow(),
                status="running",
                triggered_by="manual",
            )
        )
        session.commit()
        session.close()
        repaired = engine.get_task_execution(
            "stream_probe",
            old_id,
            old_started_at,
        )

    assert repaired["status"] == "terminated"
    assert repaired["success"] is False
