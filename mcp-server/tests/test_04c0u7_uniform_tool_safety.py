"""Registry-level safety contract for every ECM MCP tool (04c0u.7)."""

import base64
import hashlib
import hmac
import json
import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from mcp.server.fastmcp import FastMCP

import auth_claim
from auth_claim import SidecarBackendAuth, request_claim_headers
from ecm_client import ECMClient
from tools import register_all_tools
from tools._guardrails import derive_token, token_matches
from tools._safety_policy import (
    SAFETY_INVENTORY,
    ToolSafety,
    confirmation_token,
    install_safety_policy,
)


def _registry() -> FastMCP:
    mcp = FastMCP("safety-test")
    register_all_tools(mcp)
    return mcp


def _text(result) -> str:
    return result[0][0].text


def _token(text: str) -> str:
    match = re.search(r"confirmation_token: ([^\s]+)", text)
    assert match, text
    return match.group(1)


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["reorder_streams", "update_channel"])
@pytest.mark.parametrize("outcome", ["success", "error", "drift", "expiry"])
async def test_preflight_requests_are_signed(tmp_path: Path, tool_name: str, outcome: str):
    projection = tmp_path / "mcp-service.json"
    projection.write_text(json.dumps({
        "backend_key": "b" * 48,
        "confirmation_key": "c" * 48,
    }))
    projection.chmod(0o600)
    mcp = _registry()
    current = {"id": 4, "name": "News", "streams": [7, 8]}
    observed = []
    arguments = {"channel_id": 4}
    arguments["stream_ids" if tool_name == "reorder_streams" else "streams"] = [8, 7]

    async def backend(request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        active = auth_claim._context.get()
        assert active is not None
        assert active.tool_name == tool_name
        assert active.classification == "destructive"
        if request.method == "GET":
            assert active.confirmed is False
        assert request.headers["Authorization"] == f"Bearer {'b' * 48}"
        assert "listener-only-key" not in str(request.headers)
        version, timestamp, nonce, encoded = request.headers["X-ECM-MCP-Claim"].split(".", 3)
        canonical = (
            json.dumps(json.loads(body), sort_keys=True, separators=(",", ":")).encode()
            if body else b"null"
        )
        signed = b"\0".join((
            timestamp.encode(), nonce.encode(), request.method.encode(),
            request.url.raw_path, hashlib.sha256(canonical).hexdigest().encode(),
        ))
        expected = hmac.new(b"c" * 48, signed, hashlib.sha256).digest()
        supplied = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        assert version == "v1"
        assert hmac.compare_digest(supplied, expected)
        observed.append((request.method, request.url.raw_path, body))
        if outcome == "error":
            return httpx.Response(503, json={"detail": "Temporarily unavailable"})
        if request.method == "GET":
            assert request.url.raw_path == b"/api/channels/4"
            assert body == b""
            return httpx.Response(200, json=current)
        expected_path = (
            b"/api/channels/4/reorder-streams" if tool_name == "reorder_streams"
            else b"/api/channels/4"
        )
        assert request.method == ("POST" if tool_name == "reorder_streams" else "PATCH")
        assert request.url.raw_path == expected_path
        field = "stream_ids" if tool_name == "reorder_streams" else "streams"
        assert json.loads(body) == {field: [8, 7]}
        return httpx.Response(200, json={**current, "streams": [8, 7]})

    with (
        patch("config.MCP_SERVICE_FILE", projection),
        patch("config.get_mcp_api_key", return_value="listener-only-key"),
    ):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(backend),
            base_url="http://backend",
            auth=SidecarBackendAuth(),
        ) as http:
            with (
                patch("ecm_client._get_client", return_value=http),
                patch("tools.channels.get_ecm_client", return_value=ECMClient()),
            ):
                assert request_claim_headers("GET", "/api/channels/4") == {}
                if outcome == "error":
                    with pytest.raises(RuntimeError, match="Temporarily unavailable"):
                        await mcp.call_tool(tool_name, arguments)
                    assert request_claim_headers("GET", "/api/channels/4") == {}
                    assert [method for method, _, _ in observed] == ["GET"]
                    return
                preview = await mcp.call_tool(tool_name, arguments)
                assert [method for method, _, _ in observed] == ["GET"]
                assert request_claim_headers("GET", "/api/channels/4") == {}
                token = _token(_text(preview))
                confirmed = {**arguments, "confirmation_token": token}
                if outcome == "drift":
                    current["streams"] = [7, 9]
                if outcome == "expiry":
                    from tools._safety_policy import CONFIRMATION_TTL_SECONDS

                    expired_at = int(token.split(".")[1]) + CONFIRMATION_TTL_SECONDS + 1
                    with patch("tools._safety_policy.time.time", return_value=expired_at):
                        result = await mcp.call_tool(tool_name, confirmed)
                else:
                    result = await mcp.call_tool(tool_name, confirmed)
                assert request_claim_headers("GET", "/api/channels/4") == {}
                if outcome != "success":
                    assert ("drift" if outcome == "drift" else "expired") in _text(result).lower()
                    assert all(method == "GET" for method, _, _ in observed)
                    return
                assert "Error" not in _text(result)
                verb = "POST" if tool_name == "reorder_streams" else "PATCH"
                assert [method for method, _, _ in observed] == ["GET", "GET", "GET", verb]
                result = await mcp.call_tool(tool_name, confirmed)
                assert "already used" in _text(result)
                assert sum(method != "GET" for method, _, _ in observed) == 1
                assert request_claim_headers("GET", "/api/channels/4") == {}


def test_server_plan_cap_uses_authoritative_counts_at_499_and_500():
    from tools._safety_policy import _resolved_count

    assert _resolved_count(
        {"preview": {"rows": list(range(900))}, "write_count": 499, "unique_target_count": 4},
        server_planned=True,
    ) == 499
    assert _resolved_count(
        {"preview": [], "write_count": 500, "unique_target_count": 1},
        server_planned=True,
    ) == 500


def test_live_registry_is_completely_and_only_inventoried():
    mcp = _registry()
    assert set(SAFETY_INVENTORY) == set(mcp._tool_manager._tools)
    assert all(isinstance(value, ToolSafety) for value in SAFETY_INVENTORY.values())


def test_every_live_tool_has_explicit_mcp_annotations():
    mcp = _registry()
    for name, tool in mcp._tool_manager._tools.items():
        classification = SAFETY_INVENTORY[name]
        assert tool.annotations is not None, name
        assert tool.annotations.readOnlyHint is (classification is ToolSafety.READ_ONLY), name
        assert tool.annotations.destructiveHint is (classification is ToolSafety.DESTRUCTIVE), name


def test_registry_fails_closed_for_unclassified_tool():
    mcp = FastMCP("mutant")

    @mcp.tool()
    async def surprise_delete() -> str:
        return "deleted"

    with pytest.raises(RuntimeError, match="unclassified.*surprise_delete"):
        install_safety_policy(mcp)


def test_registry_fails_closed_for_one_call_destructive_mutant():
    mcp = FastMCP("mutant")

    @mcp.tool()
    async def delete_saved_backup(filename: str) -> str:
        return f"deleted {filename}"

    with patch.dict(SAFETY_INVENTORY, {"delete_saved_backup": ToolSafety.DESTRUCTIVE}, clear=True):
        install_safety_policy(mcp)
        tool = mcp._tool_manager._tools["delete_saved_backup"]
        assert "confirmation_token" in tool.parameters["properties"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "tool_name", "arguments"),
    [
        ("system", "delete_saved_backup", {"filename": "backup.yaml"}),
        ("epg", "delete_epg_source", {"source_id": 7}),
        ("tasks", "delete_task_schedule", {"task_id": "refresh", "schedule_id": 3}),
        ("tags", "delete_tag", {"tag_id": 9}),
        ("streams", "cleanup_struck_out_streams", {"delete_empty_channels": True}),
    ],
)
async def test_first_destructive_call_is_a_mutation_free_preview(module, tool_name, arguments):
    mcp = _registry()
    client = AsyncMock()
    with patch(f"tools.{module}.get_ecm_client", return_value=client):
        result = await mcp.call_tool(tool_name, arguments)
    assert "PREVIEW" in _text(result)
    assert "confirmation_token:" in _text(result)
    mutation_calls = [
        call for call in client.call_endpoint.await_args_list
        if getattr(call.args[0], "method", "GET").upper() != "GET"
    ]
    assert not mutation_calls


@pytest.mark.asyncio
async def test_confirmation_is_content_bound_and_drift_invalidates_it():
    mcp = _registry()
    preview = await mcp.call_tool("delete_saved_backup", {"filename": "a.yaml"})
    token = _token(_text(preview))
    client = AsyncMock()
    with patch("tools.system.get_ecm_client", return_value=client):
        result = await mcp.call_tool(
            "delete_saved_backup",
            {"filename": "b.yaml", "confirmation_token": token},
        )
    assert "drift" in _text(result).lower()
    client.call_endpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_confirmation_cannot_mutate():
    mcp = _registry()
    token = confirmation_token("delete_saved_backup", {"filename": "a.yaml"}, issued_at=1)
    client = AsyncMock()
    with patch("tools._safety_policy.time.time", return_value=10_000), patch(
        "tools.system.get_ecm_client", return_value=client
    ):
        result = await mcp.call_tool(
            "delete_saved_backup",
            {"filename": "a.yaml", "confirmation_token": token},
        )
    assert "expired" in _text(result).lower()
    client.call_endpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_valid_uniform_confirmation_executes_exactly_once():
    mcp = _registry()
    preview = await mcp.call_tool("delete_saved_backup", {"filename": "a.yaml"})
    token = _token(_text(preview))
    client = AsyncMock()
    with patch("tools.system.get_ecm_client", return_value=client):
        await mcp.call_tool(
            "delete_saved_backup",
            {"filename": "a.yaml", "confirmation_token": token},
        )
    mutation_calls = [
        call for call in client.call_endpoint.await_args_list
        if getattr(call.args[0], "method", "GET").upper() != "GET"
    ]
    assert len(mutation_calls) == 1


@pytest.mark.asyncio
async def test_uniform_destructive_batch_cap_refuses_without_entering_tool():
    mcp = _registry()
    client = AsyncMock()
    with patch("tools.channels.get_ecm_client", return_value=client):
        result = await mcp.call_tool(
            "bulk_add_streams_to_channel",
            {"channel_id": 1, "stream_ids": list(range(500))},
        )
    assert "hard cap is 500" in _text(result)
    client.call_endpoint.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("count, refused", [(499, False), (500, True)])
async def test_hard_cap_boundary_is_exclusive(count, refused):
    mcp = _registry()
    result = await mcp.call_tool(
        "bulk_add_streams_to_channel",
        {"channel_id": 1, "stream_ids": list(range(count))},
    )
    assert ("hard cap is 500" in _text(result)) is refused


@pytest.mark.asyncio
async def test_state_derived_targets_are_signed_and_drift_rejected():
    mcp = _registry()
    client = AsyncMock()
    client.call_endpoint.return_value = {
        "streams": [{"stream_id": 7, "channels": [{"id": 3}]}],
        "threshold": 3,
    }
    with patch("tools.streams.get_ecm_client", return_value=client):
        preview = await mcp.call_tool(
            "cleanup_struck_out_streams", {"delete_empty_channels": True}
        )
        assert '"stream_ids":[7]' in _text(preview)
        token = _token(_text(preview))
        client.call_endpoint.return_value = {
            "streams": [{"stream_id": 8, "channels": [{"id": 3}]}],
            "threshold": 3,
        }
        result = await mcp.call_tool(
            "cleanup_struck_out_streams",
            {"delete_empty_channels": True, "confirmation_token": token},
        )
    assert "drift" in _text(result).lower()
    assert all(call.args[0].method == "GET" for call in client.call_endpoint.await_args_list)


@pytest.mark.asyncio
async def test_cleanup_executes_the_exact_resolved_stream_set_without_third_recompute():
    mcp = _registry()
    client = AsyncMock()
    resolved = {"streams": [{"stream_id": 7, "channels": []}], "threshold": 3}
    client.call_endpoint.side_effect = [resolved, resolved, {"removed_from_channels": 1}]
    with patch("tools.streams.get_ecm_client", return_value=client):
        preview = await mcp.call_tool("cleanup_struck_out_streams", {})
        await mcp.call_tool(
            "cleanup_struck_out_streams", {"confirmation_token": _token(_text(preview))}
        )
    remove_call = client.call_endpoint.await_args_list[2]
    assert remove_call.kwargs["body"] == {"stream_ids": [7]}


@pytest.mark.asyncio
async def test_reorder_streams_rejects_channel_drift_before_write():
    mcp = _registry()
    client = AsyncMock()
    client.call_endpoint.side_effect = [
        {"id": 4, "streams": [7, 8]},
        {"id": 4, "streams": [7, 8]},
        {"id": 4, "streams": [7, 9]},
    ]
    with patch("tools.channels.get_ecm_client", return_value=client):
        preview = await mcp.call_tool("reorder_streams", {"channel_id": 4, "stream_ids": [8, 7]})
        result = await mcp.call_tool(
            "reorder_streams",
            {"channel_id": 4, "stream_ids": [8, 7], "confirmation_token": _token(_text(preview))},
        )
    assert "drift" in _text(result).lower()
    assert all(call.args[0].method == "GET" for call in client.call_endpoint.await_args_list)


@pytest.mark.asyncio
async def test_update_channel_replacement_rejects_drift_before_write():
    mcp = _registry()
    client = AsyncMock()
    client.call_endpoint.side_effect = [
        {"id": 4, "name": "News", "streams": [7, 8]},
        {"id": 4, "name": "News", "streams": [7, 8]},
        {"id": 4, "name": "News", "streams": [7, 9]},
    ]
    with patch("tools.channels.get_ecm_client", return_value=client):
        preview = await mcp.call_tool("update_channel", {"channel_id": 4, "streams": [8, 7]})
        result = await mcp.call_tool(
            "update_channel",
            {"channel_id": 4, "streams": [8, 7], "confirmation_token": _token(_text(preview))},
        )
    assert "drift" in _text(result).lower()
    assert all(call.args[0].method == "GET" for call in client.call_endpoint.await_args_list)


@pytest.mark.asyncio
async def test_set_logo_from_epg_uses_signed_actions_and_masks_icon_url_preview():
    mcp = _registry()
    client = AsyncMock()
    channel = {"id": 4, "name": "News", "epg_data_id": 11}
    epg = {"id": 11, "icon_url": "https://logos.invalid/a.png?token=secret"}
    client.get.side_effect = [channel, epg, channel, epg, channel, epg]
    client.post.return_value = {"id": 22}
    with patch("tools.channels.get_ecm_client", return_value=client):
        preview = await mcp.call_tool("set_logo_from_epg", {"channel_ids": [4]})
        assert "token=secret" not in _text(preview)
        result = await mcp.call_tool(
            "set_logo_from_epg",
            {"channel_ids": [4], "confirmation_token": _token(_text(preview))},
        )
    assert "1 assigned" in _text(result)
    client.post.assert_awaited_once_with(
        "/api/channels/logos",
        json_data={"name": "News", "url": "https://logos.invalid/a.png?token=secret"},
    )
    client.patch.assert_awaited_once_with("/api/channels/4", json_data={"logo_id": 22})


@pytest.mark.asyncio
@pytest.mark.parametrize("count, refused", [(499, False), (500, True)])
async def test_resolved_set_cap_boundary(count, refused):
    mcp = _registry()
    client = AsyncMock()
    client.call_endpoint.return_value = {
        "results": [{"id": value} for value in range(count)],
        "next": None,
    }
    with patch("tools.streams.get_ecm_client", return_value=client):
        result = await mcp.call_tool("probe_streams", {})
    assert ("hard cap is 500" in _text(result)) is refused
    assert all(call.args[0].method == "GET" for call in client.call_endpoint.await_args_list)


@pytest.mark.asyncio
async def test_confirmation_token_is_single_use():
    mcp = _registry()
    preview = await mcp.call_tool("delete_saved_backup", {"filename": "a.yaml"})
    token = _token(_text(preview))
    client = AsyncMock()
    with patch("tools.system.get_ecm_client", return_value=client):
        args = {"filename": "a.yaml", "confirmation_token": token}
        await mcp.call_tool("delete_saved_backup", args)
        replay = await mcp.call_tool("delete_saved_backup", args)
    assert "used" in _text(replay).lower()
    mutation_calls = [
        call for call in client.call_endpoint.await_args_list
        if getattr(call.args[0], "method", "GET").upper() != "GET"
    ]
    assert len(mutation_calls) == 1


@pytest.mark.asyncio
async def test_normalization_confirmation_commits_backend_owned_plan_not_a_recomputed_preview():
    mcp = _registry()
    client = AsyncMock()
    client.call_endpoint.side_effect = [
        {
            "dry_run": True,
            "diffs": [{"channel_id": 7, "current_name": "OLD", "proposed_name": "New"}],
            "plan_id": "plan-7",
                "plan_hash": "hash-7",
                "expires_at": 9999999999,
                "write_count": 1,
                "unique_target_count": 1,
        },
        {"renamed": [{"channel_id": 7, "old_name": "OLD", "new_name": "New"}]},
    ]
    args = {"dry_run": False, "actions": [{"channel_id": 7, "action": "rename"}]}
    with patch("tools.normalization.get_ecm_client", return_value=client):
        preview = await mcp.call_tool("apply_normalization_to_channels", args)
        token = _token(_text(preview))
        await mcp.call_tool(
            "apply_normalization_to_channels", {**args, "confirmation_token": token}
        )
    commit = client.call_endpoint.await_args_list[1]
    assert commit.kwargs["query"] == {"dry_run": False}
    assert commit.kwargs["body"]["plan_id"] == "plan-7"
    assert commit.kwargs["body"]["plan_hash"] == "hash-7"


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["run_channel_pipeline", "run_auto_creation"])
async def test_pipeline_prerefresh_requires_two_distinct_confirmations(tool_name):
    mcp = _registry()
    client = AsyncMock()
    client.call_endpoint.side_effect = [
        {
            "phase": "refresh", "plan_id": "refresh-plan", "plan_hash": "refresh-hash",
            "preview": {"m3u_account_ids_to_refresh": [7]},
            "write_count": 1, "unique_target_count": 1,
        },
        {
            "requires_confirmation": True, "completed_phase": "refresh",
            "phase": "execute", "plan_id": "write-plan", "plan_hash": "write-hash",
            "preview": {"channels_created": 1},
            "write_count": 1, "unique_target_count": 1,
        },
        {"execution_id": 99, "status": "completed"},
        {"execution_id": 99, "status": "completed", "channels_created": 1},
    ]
    with patch("tools.channel_pipeline.get_ecm_client", return_value=client), patch(
        "tools.channel_pipeline._poll_sleep", AsyncMock()
    ):
        first = await mcp.call_tool(tool_name, {"dry_run": False})
        first_token = _token(_text(first))
        second = await mcp.call_tool(
            tool_name, {"dry_run": False, "confirmation_token": first_token}
        )
        assert "second review is required" in _text(second)
        second_token = _token(_text(second))
        assert second_token != first_token
        final = await mcp.call_tool(
            tool_name, {"dry_run": False, "confirmation_token": second_token}
        )
    assert "complete" in _text(final).lower()
    assert client.call_endpoint.await_args_list[1].kwargs["body"]["phase"] == "refresh"
    assert client.call_endpoint.await_args_list[2].kwargs["body"]["phase"] == "execute"

@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["run_channel_pipeline", "run_auto_creation"])
async def test_pipeline_confirmation_preserves_requested_rule_scope(tool_name):
    mcp = _registry()
    client = AsyncMock()
    client.call_endpoint.return_value = {
        "phase": "execute", "plan_id": "selected-plan", "plan_hash": "selected-hash",
        "preview": {"channels_updated": 1}, "write_count": 1, "unique_target_count": 1,
    }
    args = {"dry_run": False, "rule_ids": [12], "m3u_account_ids": [18]}
    with patch("tools.channel_pipeline.get_ecm_client", return_value=client):
        first = await mcp.call_tool(tool_name, args)
        token = _token(_text(first))
        changed = await mcp.call_tool(
            tool_name, {**args, "rule_ids": [5], "confirmation_token": token},
        )
    assert client.call_endpoint.await_args_list[0].kwargs["body"] == {
        "dry_run": True, "rule_ids": [12], "m3u_account_ids": [18],
    }
    assert "drift" in _text(changed) or "does not match" in _text(changed)
    assert not any(call.args[0].name == "ac_commit_run" for call in client.call_endpoint.await_args_list)

@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["run_channel_pipeline", "run_auto_creation"])
async def test_scoped_pipeline_confirmation_commits_only_prepared_plan(tool_name):
    mcp = _registry()
    client = AsyncMock()
    client.call_endpoint.side_effect = [
        {"phase": "execute", "plan_id": "selected-plan", "plan_hash": "selected-hash",
         "preview": {"channels_updated": 1}, "write_count": 1, "unique_target_count": 1},
        {"execution_id": 99, "status": "completed"},
        {"execution_id": 99, "status": "completed", "channels_updated": 1},
    ]
    arguments = {"dry_run": False, "rule_ids": [12], "m3u_account_ids": [18]}
    with patch("tools.channel_pipeline.get_ecm_client", return_value=client), patch(
        "tools.channel_pipeline._poll_sleep", AsyncMock()
    ):
        preview = await mcp.call_tool(tool_name, arguments)
        result = await mcp.call_tool(tool_name, {**arguments, "confirmation_token": _token(_text(preview))})
    assert "complete" in _text(result).lower()
    assert client.call_endpoint.await_args_list[0].kwargs["body"] == {
        "dry_run": True, "rule_ids": [12], "m3u_account_ids": [18],
    }
    assert client.call_endpoint.await_args_list[1].kwargs["body"] == {
        "plan_id": "selected-plan", "plan_hash": "selected-hash", "phase": "execute",
    }


@pytest.mark.asyncio
async def test_pipeline_policy_refuses_empty_scope_before_preparation():
    from mcp.server.fastmcp.exceptions import ToolError

    client = AsyncMock()
    with patch("tools.channel_pipeline.get_ecm_client", return_value=client):
        with pytest.raises((ToolError, ValueError), match="rule_ids"):
            await _registry().call_tool("run_channel_pipeline", {"dry_run": False, "rule_ids": []})
    client.call_endpoint.assert_not_called()


@pytest.mark.parametrize(
    "name",
    [
        "accept_channel_merge", "dismiss_probe_failures", "probe_streams",
        "run_channel_pipeline", "run_auto_creation",
    ],
)
def test_behaviorally_destructive_inventory(name):
    assert SAFETY_INVENTORY[name] is ToolSafety.DESTRUCTIVE


def test_external_notification_is_not_annotated_read_only_or_idempotent():
    tool = _registry()._tool_manager._tools["test_alert_method"]
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.idempotentHint is False


def test_resolved_target_token_expires_and_rejects_drift():
    with patch("tools._guardrails.time.time", return_value=100):
        token = derive_token([3, 1, 2])
    with patch("tools._guardrails.time.time", return_value=399):
        assert token_matches(token, [1, 2, 3])
        assert not token_matches(token, [1, 2, 3])  # single-use replay refusal
        assert not token_matches(token, [1, 2, 4])
    with patch("tools._guardrails.time.time", return_value=401):
        assert not token_matches(token, [1, 2, 3])
