"""Retention tests for active task execution rows."""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy.orm import sessionmaker

from models import TaskExecution
from task_engine import TaskEngine
from tasks.cleanup import CleanupTask


def _seed_history(sessions):
    old = datetime.utcnow() - timedelta(days=90)
    session = sessions()
    running = TaskExecution(
        task_id="stream_probe",
        started_at=old,
        status="running",
        triggered_by="manual",
    )
    terminal = TaskExecution(
        task_id="epg_refresh",
        started_at=old,
        completed_at=old + timedelta(seconds=5),
        duration_seconds=5,
        status="completed",
        success=True,
        triggered_by="manual",
    )
    session.add_all([running, terminal])
    session.commit()
    identities = running.id, terminal.id
    session.close()
    return identities


def test_engine_purge_keeps_running_rows(test_engine):
    sessions = sessionmaker(bind=test_engine, expire_on_commit=False)
    running_id, terminal_id = _seed_history(sessions)

    with patch("task_engine.get_session", side_effect=sessions):
        deleted = TaskEngine().purge_old_history(days=30)

    session = sessions()
    try:
        assert deleted == 1
        assert session.get(TaskExecution, running_id) is not None
        assert session.get(TaskExecution, terminal_id) is None
    finally:
        session.close()


@pytest.mark.asyncio
async def test_cleanup_task_keeps_running_rows(test_engine):
    sessions = sessionmaker(bind=test_engine, expire_on_commit=False)
    running_id, terminal_id = _seed_history(sessions)
    task = CleanupTask()
    task.task_history_days = 30

    with patch("tasks.cleanup.get_session", side_effect=sessions):
        result = await task.execute()

    session = sessions()
    try:
        assert result.success is True
        assert session.get(TaskExecution, running_id) is not None
        assert session.get(TaskExecution, terminal_id) is None
    finally:
        session.close()
