"""Direct-ASGI adversarial checks for the MCP service-principal boundary."""

import asyncio
import time
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from auth.mcp_service import MCP_CLAIM_HEADER, MCPServiceCredentials, issue_test_claim


MCP_KEY = "<Synthetic-MCP-Service-Key-04c0u4>"


def _auth_on():
    return SimpleNamespace(require_auth=True, setup_complete=True)


def _runtime_settings():
    return SimpleNamespace(mcp_api_key=MCP_KEY)


@pytest.mark.parametrize(
    "method,path,json",
    [
        ("POST", "/api/cloud-targets", {"name": "x"}),
        ("PATCH", "/api/cloud-targets/7", {"enabled": True}),
        ("DELETE", "/api/cloud-targets/7", None),
        ("POST", "/api/sync-targets", {"name": "x"}),
        ("PUT", "/api/sync-targets/7", {"enabled": True}),
        ("DELETE", "/api/sync-targets/7", None),
        ("POST", "/api/m3u/accounts", {"username": "u", "password": "p"}),
        ("POST", "/api/epg/sources", {"username": "u", "password": "p"}),
        ("GET", "/api/auth/admin/users", None),
        ("POST", "/api/settings/mcp-api-key", None),
        ("POST", "/api/tls/upload-cert", {}),
        ("GET", "/api/not-yet-classified", None),
    ],
)
@pytest.mark.asyncio
async def test_direct_backend_call_with_mcp_key_is_refused(
    async_client, method, path, json
):
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
    ):
        response = await async_client.request(
            method,
            path,
            json=json,
            headers={"Authorization": f"Bearer {MCP_KEY}"},
        )
    assert response.status_code == 403, response.text
    assert "MCP service principal" in response.json()["detail"]


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/api/backup/create"),
        ("POST", "/api/backup/save"),
        ("GET", "/api/backup/saved"),
        ("GET", "/api/backup/saved/ecm-backup-2026-08-18_010203.zip"),
        ("DELETE", "/api/backup/saved/ecm-backup-2026-08-18_010203.zip"),
    ],
)
@pytest.mark.asyncio
async def test_direct_mcp_backup_capabilities_are_human_only_before_artifact_access(
    async_client, tmp_path, method, path
):
    """The TLS sentinel must neither be read nor copied by an MCP request."""
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    saved = backup_dir / "ecm-backup-2026-08-18_010203.zip"
    saved.write_bytes(b"TLS-PRIVATE-KEY-SENTINEL-04c0u4")
    create_zip = MagicMock(side_effect=AssertionError("backup body was constructed"))
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
        patch("routers.backup.BACKUPS_DIR", backup_dir),
        patch("routers.backup._create_backup_zip", create_zip),
    ):
        response = await async_client.request(
            method,
            path,
            headers={"Authorization": f"Bearer {MCP_KEY}"},
        )
    assert response.status_code == 403, response.text
    assert b"TLS-PRIVATE-KEY-SENTINEL-04c0u4" not in response.content
    assert saved.read_bytes() == b"TLS-PRIVATE-KEY-SENTINEL-04c0u4"
    assert sorted(item.name for item in backup_dir.iterdir()) == [saved.name]
    create_zip.assert_not_called()


@pytest.mark.parametrize(
    "parameters",
    [
        {"include_credentials": True},
        {"include_credentials": 1},
        {"include_credentials": "true"},
        {"include_credentials": []},
        {"cloud_target_id": 7},
        {"cloud_targets": [7]},
        {"retention_enabled": True},
        {"last_n": 2},
        {"max_age": 30},
    ],
)
@pytest.mark.asyncio
async def test_direct_mcp_dbas_backup_run_is_human_only_before_validation_or_execution(
    async_client, parameters
):
    engine_factory = MagicMock(side_effect=AssertionError("task engine was reached"))
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
        patch("auth.dependencies.get_auth_settings", return_value=_auth_on()),
        patch("auth.dependencies.get_settings", return_value=_runtime_settings()),
        patch("task_engine.get_engine", engine_factory),
    ):
        response = await async_client.post(
            "/api/tasks/dbas_backup/run",
            json={"parameters": parameters},
            headers={"Authorization": f"Bearer {MCP_KEY}"},
        )
    assert response.status_code == 403, response.text
    assert "MCP service principal" in response.json()["detail"]
    engine_factory.assert_not_called()


@pytest.mark.parametrize(
    "path,body",
    [
        ("/api/tasks/dbas_restore/run", {}),
        ("/api/tasks/dbas_restore/runs", {}),
        ("/api/tasks/dbas_sync_7/run", {}),
        ("/api/tasks/dbas_sync_7/runs", {}),
        (
            "/api/tasks/dbas_sync_7/schedules",
            {"schedule_type": "daily", "schedule_time": "03:00"},
        ),
    ],
)
@pytest.mark.asyncio
async def test_direct_mcp_call_cannot_activate_outbound_or_restore_tasks(
    async_client, path, body
):
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
        patch("auth.dependencies.get_auth_settings", return_value=_auth_on()),
        patch("auth.dependencies.get_settings", return_value=_runtime_settings()),
    ):
        response = await async_client.post(
            path,
            json=body,
            headers={"Authorization": f"Bearer {MCP_KEY}"},
        )
    assert response.status_code == 403, response.text
    assert "MCP service principal" in response.json()["detail"]


@pytest.mark.asyncio
async def test_external_mcp_client_key_cannot_reach_even_safe_backend_capability(async_client):
    client = SimpleNamespace(
        get_channels=AsyncMock(return_value={"results": [], "count": 0})
    )
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
        patch("auth.dependencies.get_auth_settings", return_value=_auth_on()),
        patch("auth.dependencies.get_settings", return_value=_runtime_settings()),
        patch("routers.channels.get_client", return_value=client),
    ):
        response = await async_client.get(
            "/api/channels",
            headers={"Authorization": f"Bearer {MCP_KEY}"},
        )
    assert response.status_code == 403, response.text
    assert "valid only at the sidecar" in response.json()["detail"]
    client.get_channels.assert_not_awaited()


@pytest.mark.asyncio
async def test_external_mcp_client_key_cannot_enter_async_task_start(async_client):
    engine_factory = MagicMock(side_effect=AssertionError("task engine was reached"))
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
        patch("auth.dependencies.get_auth_settings", return_value=_auth_on()),
        patch("auth.dependencies.get_settings", return_value=_runtime_settings()),
        patch("task_engine.get_engine", engine_factory),
    ):
        response = await async_client.post(
            "/api/tasks/stream_probe/runs",
            json={},
            headers={"Authorization": f"Bearer {MCP_KEY}"},
        )
    assert response.status_code == 403, response.text
    engine_factory.assert_not_called()


@pytest.mark.asyncio
async def test_private_sidecar_principal_with_bound_claim_reaches_safe_capability(async_client):
    credentials = MCPServiceCredentials("private-backend-key", "private-confirmation-key")
    claim = issue_test_claim(credentials, "GET", "/api/channels", None)
    client = SimpleNamespace(
        get_channels=AsyncMock(return_value={"results": [], "count": 0})
    )
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
        patch("main.load_mcp_service_credentials", return_value=credentials),
        patch("auth.dependencies.load_mcp_service_credentials", return_value=credentials),
        patch("auth.dependencies.get_auth_settings", return_value=_auth_on()),
        patch("routers.channels.get_client", return_value=client),
    ):
        response = await async_client.get(
            "/api/channels",
            headers={
                "Authorization": "Bearer private-backend-key",
                MCP_CLAIM_HEADER: claim,
            },
        )
    assert response.status_code == 200, response.text
    client.get_channels.assert_awaited_once()


@pytest.mark.asyncio
async def test_private_sidecar_principal_can_start_ordinary_task(async_client):
    from task_engine import TaskRun

    credentials = MCPServiceCredentials("private-backend-key", "private-confirmation-key")
    path = "/api/tasks/stream_probe/runs"
    body = {}
    claim = issue_test_claim(credentials, "POST", path, body)
    completion = asyncio.get_running_loop().create_future()
    admitted = TaskRun(
        execution_id=73,
        task_id="stream_probe",
        started_at=datetime(2026, 9, 20, 3, 0, 0),
        completion=completion,
    )
    engine = MagicMock()
    engine.start_task = AsyncMock(return_value=admitted)
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
        patch("main.load_mcp_service_credentials", return_value=credentials),
        patch("auth.dependencies.load_mcp_service_credentials", return_value=credentials),
        patch("auth.dependencies.get_auth_settings", return_value=_auth_on()),
        patch("task_engine.get_engine", return_value=engine),
    ):
        response = await async_client.post(
            path,
            json=body,
            headers={
                "Authorization": "Bearer private-backend-key",
                MCP_CLAIM_HEADER: claim,
            },
        )

    assert response.status_code == 202, response.text
    engine.start_task.assert_awaited_once()
    completion.cancel()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/api/tasks/engine/status", "/api/tasks/stream_probe?details=true"])
async def test_bound_task_reads_succeed_once_and_replay_is_denied(async_client, test_session, path):
    from models import ScheduledTask

    test_session.add(ScheduledTask(task_id="stream_probe", task_name="Stream Probe", schedule_type="manual"))
    test_session.commit()
    credentials = MCPServiceCredentials("private-backend-key", "private-confirmation-key")
    claim = issue_test_claim(credentials, "GET", path, None)
    engine = MagicMock()
    engine.get_status.return_value = {
        "running": True, "check_interval": 5, "max_concurrent": 4,
        "active_tasks": [], "active_task_count": 0, "registered_task_count": 1,
    }
    registry = MagicMock()
    registry.get_task_status.return_value = {"task_id": "stream_probe", "enabled": True}
    headers = {"Authorization": "Bearer private-backend-key", MCP_CLAIM_HEADER: claim}
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
        patch("main.load_mcp_service_credentials", return_value=credentials),
        patch("auth.dependencies.load_mcp_service_credentials", return_value=credentials),
        patch("auth.dependencies.get_auth_settings", return_value=_auth_on()),
        patch("task_engine.get_engine", return_value=engine),
        patch("task_registry.get_registry", return_value=registry),
        patch("routers.tasks.get_client") as get_client,
    ):
        accepted = await async_client.get(path, headers=headers)
        replayed = await async_client.get(path, headers=headers)
    assert accepted.status_code == 200, accepted.text
    assert replayed.status_code == 403
    assert replayed.json()["detail"] == "MCP sidecar claim already used"
    if "engine/status" in path:
        assert accepted.json() == engine.get_status.return_value
        engine.get_status.assert_called_once()
    else:
        assert accepted.json()["stored"]["task_id"] == "stream_probe"
        registry.get_task_status.assert_called_once_with("stream_probe")
    get_client.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/api/tasks/engine/status", "/api/tasks/stream_probe?details=true"])
@pytest.mark.parametrize("failure", ["missing", "expired", "path", "query", "method"])
async def test_task_reads_require_fresh_method_path_and_query_bound_claims(async_client, path, failure):
    credentials = MCPServiceCredentials("private-backend-key", "private-confirmation-key")
    signed_path = path
    signed_method = "GET"
    timestamp = int(time.time())
    if failure == "expired":
        timestamp -= 300
    elif failure == "path":
        signed_path = "/api/tasks/another_task?details=true"
    elif failure == "query":
        signed_path = path.split("?")[0] + "?details=false"
    elif failure == "method":
        signed_method = "POST"
    headers = {"Authorization": "Bearer private-backend-key"}
    if failure != "missing":
        headers[MCP_CLAIM_HEADER] = issue_test_claim(credentials, signed_method, signed_path, None, timestamp=timestamp)
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
        patch("main.load_mcp_service_credentials", return_value=credentials),
        patch("auth.dependencies.load_mcp_service_credentials", return_value=credentials),
        patch("auth.dependencies.get_auth_settings", return_value=_auth_on()),
        patch("task_engine.get_engine") as get_engine,
        patch("task_registry.get_registry") as get_registry,
    ):
        response = await async_client.get(path, headers=headers)
    assert response.status_code == 403, response.text
    get_engine.assert_not_called()
    get_registry.assert_not_called()


@pytest.mark.asyncio
async def test_valid_signed_claim_does_not_admit_human_only_write(async_client):
    credentials = MCPServiceCredentials("private-backend-key", "private-confirmation-key")
    path = "/api/cloud-targets"
    body = {"name": "Selected target"}
    claim = issue_test_claim(credentials, "POST", path, body)
    with (
        patch("main.get_auth_settings", return_value=_auth_on()),
        patch("main.get_settings", return_value=_runtime_settings()),
        patch("main.load_mcp_service_credentials", return_value=credentials),
        patch("auth.dependencies.load_mcp_service_credentials", return_value=credentials),
        patch("auth.dependencies.get_auth_settings", return_value=_auth_on()),
    ):
        response = await async_client.post(path, json=body, headers={
            "Authorization": "Bearer private-backend-key", MCP_CLAIM_HEADER: claim,
        })
    assert response.status_code == 403, response.text
    assert "MCP service principal" in response.json()["detail"]
