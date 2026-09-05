"""TDD tests for enhancedchannelmanager-omxy5 — dummy-EPG profile CRUD tools.

Covers get/create/update/delete_dummy_epg_profile and its test companion
preview_dummy_epg. Delete is confirm-gated and previews channel-group
assignments (the blast radius), mirroring delete_logo /
delete_normalization_group.
"""
import pytest
from unittest.mock import AsyncMock, patch


def _mcp():
    from mcp.server.fastmcp import FastMCP
    from tools.epg import register

    mcp = FastMCP("test")
    register(mcp)
    return mcp


def _text(result) -> str:
    return result[0][0].text


class TestGetDummyEpgProfile:
    @pytest.mark.asyncio
    async def test_shows_config_and_group_count(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = {
            "id": 1, "name": "Sports Placeholder", "enabled": True,
            "name_source": "channel", "stream_index": 1,
            "title_pattern": r"^(.*?) - ", "time_pattern": None, "date_pattern": None,
            "title_template": "{title}", "event_timezone": "US/Eastern",
            "program_duration": 180, "channel_group_ids": [5, 7],
            "substitution_pairs": [{"find": "HD", "replace": ""}],
        }

        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("get_dummy_epg_profile", {"profile_id": 1})

        text = _text(result)
        assert "Sports Placeholder" in text
        assert "2 group(s) assigned" in text
        assert "1 configured" in text
        called = [c.args[0].name for c in client.call_endpoint.call_args_list]
        assert called == ["dummy_epg_get_profile"]


class TestCreateDummyEpgProfile:
    @pytest.mark.asyncio
    async def test_creates_with_defaults(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = {"id": 10, "name": "News Placeholder"}

        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("create_dummy_epg_profile", {"name": "News Placeholder"})

        body = client.call_endpoint.call_args.kwargs["body"]
        assert body["name"] == "News Placeholder"
        assert body["enabled"] is True
        assert body["name_source"] == "channel"
        assert body["stream_index"] == 1
        assert body["event_timezone"] == "US/Eastern"
        assert body["program_duration"] == 180
        assert body["tvg_id_template"] == "ecm-{channel_id}"
        for absent in ("title_pattern", "substitution_pairs", "pattern_variants", "channel_group_ids"):
            assert absent not in body, f"{absent} should be omitted when not given"
        assert "id=10" in _text(result)

    @pytest.mark.asyncio
    async def test_creates_with_optional_fields(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = {"id": 11, "name": "Sports"}

        with patch("tools.epg.get_ecm_client", return_value=client):
            await mcp.call_tool("create_dummy_epg_profile", {
                "name": "Sports",
                "title_pattern": r"^(.*?) - ",
                "substitution_pairs": [{"find": "HD", "replace": ""}],
                "channel_group_ids": [5, 7],
            })

        body = client.call_endpoint.call_args.kwargs["body"]
        assert body["title_pattern"] == r"^(.*?) - "
        assert body["substitution_pairs"] == [{"find": "HD", "replace": ""}]
        assert body["channel_group_ids"] == [5, 7]


class TestUpdateDummyEpgProfile:
    @pytest.mark.asyncio
    async def test_forwards_only_provided_fields(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = {"name": "Sports"}

        with patch("tools.epg.get_ecm_client", return_value=client):
            await mcp.call_tool("update_dummy_epg_profile", {"profile_id": 1, "enabled": False})

        body = client.call_endpoint.call_args.kwargs["body"]
        assert body == {"enabled": False}

    @pytest.mark.asyncio
    async def test_no_changes_short_circuits(self):
        mcp = _mcp()
        client = AsyncMock()

        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("update_dummy_epg_profile", {"profile_id": 1})

        assert "No changes specified" in _text(result)
        client.call_endpoint.assert_not_called()


class TestDeleteDummyEpgProfile:
    @pytest.mark.asyncio
    async def test_preview_on_confirm_false_deletes_nothing(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = {"id": 1, "name": "Sports", "channel_group_ids": [5, 7]}

        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("delete_dummy_epg_profile", {"profile_id": 1})

        text = _text(result)
        assert "Sports" in text
        assert "WARNING" in text
        assert "2 channel group" in text
        assert "confirm=True" in text
        called = [c.args[0].name for c in client.call_endpoint.call_args_list]
        assert called == ["dummy_epg_get_profile"]

    @pytest.mark.asyncio
    async def test_preview_no_warning_when_unassigned(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = {"id": 2, "name": "Unused", "channel_group_ids": []}

        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("delete_dummy_epg_profile", {"profile_id": 2})

        text = _text(result)
        assert "WARNING" not in text

    @pytest.mark.asyncio
    async def test_confirm_true_deletes(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = None

        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("delete_dummy_epg_profile", {"profile_id": 1, "confirm": True})

        assert "deleted" in _text(result).lower()
        called = [c.args[0].name for c in client.call_endpoint.call_args_list]
        assert called == ["dummy_epg_delete_profile"]


class TestPreviewDummyEpg:
    @pytest.mark.asyncio
    async def test_matched_preview(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = {
            "original_name": "ESPN - Game Time 8:00PM",
            "substituted_name": "ESPN - Game Time 8:00PM",
            "matched": True,
            "matched_variant": None,
            "rendered": {"title": "Game Time", "description": "Live on ESPN"},
        }

        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("preview_dummy_epg", {
                "sample_name": "ESPN - Game Time 8:00PM",
                "title_pattern": r"^.*? - (.*?) \d",
                "title_template": "{title}",
            })

        text = _text(result)
        assert "MATCHED" in text
        assert "Game Time" in text
        body = client.call_endpoint.call_args.kwargs["body"]
        assert body["sample_name"] == "ESPN - Game Time 8:00PM"
        assert body["title_pattern"] == r"^.*? - (.*?) \d"
        assert "substitution_pairs" not in body

    @pytest.mark.asyncio
    async def test_not_matched_shows_fallback(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = {
            "original_name": "Random Channel",
            "substituted_name": "Random Channel",
            "matched": False,
            "matched_variant": None,
            "rendered": {"title": "", "fallback_title": "Programming"},
        }

        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("preview_dummy_epg", {"sample_name": "Random Channel"})

        text = _text(result)
        assert "NOT MATCHED" in text
        assert "Programming" in text


class TestProgrammeSourceTools:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("tool, arguments", [
        ("create_dummy_epg_profile", {"name": "Universal", "epg_source_ids": [49], "channel_mappings": [{"channel_id": 2950, "source_id": 42, "tvg_id": "32645"}]}),
        ("update_dummy_epg_profile", {"profile_id": 1, "epg_source_ids": []}),
    ])
    async def test_forwards_sources_and_explicit_clear(self, tool, arguments):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = {"id": 1, "name": "Universal"}
        with patch("tools.epg.get_ecm_client", return_value=client):
            await mcp.call_tool(tool, arguments)
        sent = client.call_endpoint.call_args.kwargs["body"]
        assert sent["epg_source_ids"] == arguments["epg_source_ids"]
        if "channel_mappings" in arguments:
            assert sent["channel_mappings"] == arguments["channel_mappings"]
        else:
            assert "channel_mappings" not in sent

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status,error", [
        ("pending", None), ("error", "HTTP status 503."),
        ("error", "Request timed out while reading."),
    ])
    async def test_coverage_only_calls_read_endpoint(self, status, error):
        import json
        mcp = _mcp()
        client = AsyncMock()
        coverage = {"sources": [{"source_id": 51, "status": status, "error": error}], "channels": []}
        client.call_endpoint.return_value = coverage
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("get_dummy_epg_coverage", {"profile_id": 1})
        assert json.loads(_text(result)) == coverage
        client.call_endpoint.assert_awaited_once()
        call = client.call_endpoint.call_args
        assert call.args[0].name == "dummy_epg_coverage"
        assert call.args[0].method == "GET"
        assert call.kwargs == {"path_args": {"profile_id": 1}}

    @pytest.mark.asyncio
    async def test_coverage_errors_do_not_reveal_upstream_credentials(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.side_effect = RuntimeError("https://guide.example/private-key")
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("get_dummy_epg_coverage", {"profile_id": 1})
        assert "private-key" not in _text(result)
        assert "Could not inspect" in _text(result)

class TestGenerateDummyEpg:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(("status", "expected"), [
        ("ok", "Dummy EPG regenerated for 2 enabled profiles."),
        ("pending", "Dummy EPG sources or artwork are still loading."),
        ("error", "Dummy EPG generation is incomplete because programme sources are unavailable."),
    ])
    async def test_reports_generation_readiness(self, status, expected):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = {
            "status": status, "profiles_generated": 2,
            "coverage": {"sources": [], "channels": []},
        }
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("generate_dummy_epg", {"plan_profile_ids": [1, 2]})
        message = _text(result)
        assert expected in message
        if status != "ok":
            assert "get_dummy_epg_coverage" in message
            assert "regenerated for" not in message
        call = client.call_endpoint.call_args
        assert call.args[0].name == "dummy_epg_generate"
        assert call.kwargs == {"body": {"profile_ids": [1, 2]}, "timeout": 60.0}

class TestSearchEpgChannels:
    @pytest.mark.asyncio
    async def test_wrapped_http_error_retains_safe_status(self):
        import httpx

        request = httpx.Request("GET", "https://guide.invalid/?key=private")
        response = httpx.Response(403, request=request, json={"detail": "private credentials"})
        failure = RuntimeError("private credentials")
        failure.__cause__ = httpx.HTTPStatusError("private", request=request, response=response)
        client = AsyncMock()
        client.call_endpoint.side_effect = failure
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await _mcp().call_tool("search_epg_channels", {"search": "Sports"})
        assert _text(result) == "Error searching EPG channels: HTTP status 403."

    @pytest.mark.asyncio
    @pytest.mark.parametrize("detail,expected", [
        ("EPG catalogue request failed: Response exceeded the catalogue size limit.",
         "HTTP status 500. Catalogue error: Response exceeded the catalogue size limit."),
        ("EPG catalogue request failed: Request timed out while reading.",
         "HTTP status 500. Catalogue error: Request timed out while reading."),
        ("EPG catalogue request failed: Invalid JSON response.",
         "HTTP status 500. Catalogue error: Invalid JSON response."),
        ("EPG catalogue request failed: HTTP status 401.",
         "HTTP status 500. Catalogue error: HTTP status 401."),
        ("EPG catalogue request failed: HTTP status 403. https://guide.invalid/?key=private",
         "HTTP status 500."),
        ({"url": "https://guide.invalid/?key=private"}, "HTTP status 500."),
        ("EPG catalogue request failed: " + "private" * 1000, "HTTP status 500."),
    ])
    async def test_backend_reasons_are_allowlisted(self, detail, expected, caplog):
        import httpx

        request = httpx.Request("GET", "https://guide.invalid/?key=private")
        response = httpx.Response(500, request=request, json={"detail": detail})
        failure = RuntimeError("private")
        failure.__cause__ = httpx.HTTPStatusError("private", request=request, response=response)
        client = AsyncMock()
        client.call_endpoint.side_effect = failure
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await _mcp().call_tool("search_epg_channels", {"search": "Sports"})
        assert _text(result) == f"Error searching EPG channels: {expected}"
        assert "private" not in _text(result)
        assert "private" not in caplog.text

    @pytest.mark.asyncio
    async def test_real_client_wrapping_preserves_backend_reason(self):
        import httpx
        from ecm_client import ECMClient

        transport = httpx.MockTransport(lambda request: httpx.Response(
            502, json={"detail": "EPG catalogue request failed: Unsupported catalogue response encoding."},
        ))
        async with httpx.AsyncClient(transport=transport, base_url="https://backend.invalid") as http:
            with patch("ecm_client._get_client", return_value=http), \
                 patch("tools.epg.get_ecm_client", return_value=ECMClient()):
                result = await _mcp().call_tool("search_epg_channels", {"search": "Sports"})
        assert _text(result) == (
            "Error searching EPG channels: HTTP status 502. "
            "Catalogue error: Unsupported catalogue response encoding."
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("rows", [{"detail": "private"}, [None], ["private"]])
    async def test_malformed_catalogue_is_an_error(self, rows):
        client = AsyncMock()
        client.call_endpoint.return_value = rows
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await _mcp().call_tool("search_epg_channels", {"search": "Sports"})
        assert _text(result) == "Error searching EPG channels: Invalid catalogue response."

    @pytest.mark.asyncio
    async def test_timeout_and_unknown_cyclic_causes_are_safe(self, caplog):
        client = AsyncMock()
        client.call_endpoint.side_effect = TimeoutError("private")
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await _mcp().call_tool("search_epg_channels", {"search": "Sports"})
        assert _text(result) == "Error searching EPG channels: Request timed out."
        failure = type("private_credentials", (RuntimeError,), {})("private")
        failure.__cause__ = failure
        client.call_endpoint.side_effect = failure
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await _mcp().call_tool("search_epg_channels", {"search": "Sports"})
        assert _text(result) == "Error searching EPG channels: the catalogue request failed."
        assert "private" not in caplog.text

    @pytest.mark.asyncio
    async def test_default_search_uses_bounded_read_endpoint(self):
        import json
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = []
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("search_epg_channels", {"search": " Sports "})
        assert json.loads(_text(result)) == {
            "search": "Sports", "epg_source_id": None, "limit": 25,
            "returned": 0, "limit_reached": False, "channels": [],
        }
        client.call_endpoint.assert_awaited_once()
        call = client.call_endpoint.call_args
        assert call.args[0].name == "epg_search"
        assert call.args[0].method == "GET"
        assert call.args[0].path == "/api/epg/data"
        assert call.kwargs == {"query": {"search": "Sports", "page": 1, "page_size": 25, "limit": 25}}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("count", [1, 2, 3])
    @pytest.mark.parametrize("source_fields", [{"epg_source": 49}, {"epg_source": {"id": 49}}, {"epg_source_id": 49}, {"epg_source": None, "epg_source_id": 49}, {"epg_source": None, "epg_source_id": {"id": 49}}])
    async def test_preserves_candidate_identity_and_reports_limit(self, count, source_fields):
        import json
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.return_value = [
            {"id": 730 + index, **source_fields,
             "tvg_id": str(32645 + index), "name": f"Sports {index}",
             "icon_url": "https://asset.invalid/?token=hidden"}
            for index in range(count)
        ]
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("search_epg_channels", {"search": "Sports", "epg_source_id": 49, "limit": 2})
        saved = json.loads(_text(result))
        assert saved["channels"] == [
            {"id": 730 + index, "source_id": 49, "tvg_id": str(32645 + index), "name": f"Sports {index}"}
            for index in range(min(count, 2))
        ]
        assert saved["returned"] == min(count, 2)
        assert saved["limit_reached"] == (count >= 2)
        assert ("note" in saved) == (count >= 2)
        if count >= 2:
            assert "more matches may exist" in saved["note"]
        assert "token=hidden" not in _text(result)
        assert client.call_endpoint.call_args.kwargs == {
            "query": {"search": "Sports", "epg_source": 49, "page": 1, "page_size": 2, "limit": 2},
        }
        client.call_endpoint.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("arguments", [
        {"search": ""}, {"search": "x" * 201},
        {"search": "Sports", "epg_source_id": 0},
        {"search": "Sports", "epg_source_id": -1},
        {"search": "Sports", "epg_source_id": True},
        {"search": "Sports", "limit": True},
        {"search": "Sports", "limit": 0},
        {"search": "Sports", "limit": 101},
    ])
    async def test_invalid_arguments_never_request_catalogue(self, arguments):
        from mcp.server.fastmcp.exceptions import ToolError
        mcp = _mcp()
        with patch("tools.epg.get_ecm_client") as get_client:
            with pytest.raises(ToolError, match="validation error"):
                await mcp.call_tool("search_epg_channels", arguments)
        get_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_whitespace_search_never_requests_catalogue(self):
        mcp = _mcp()
        with patch("tools.epg.get_ecm_client") as get_client:
            result = await mcp.call_tool("search_epg_channels", {"search": " \t\n "})
        assert _text(result).startswith("Error searching EPG channels:")
        get_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_schema_publishes_query_and_result_bounds(self):
        tool = next(tool for tool in await _mcp().list_tools() if tool.name == "search_epg_channels")
        fields = tool.inputSchema["properties"]
        assert fields["search"]["minLength"] == 1
        assert fields["search"]["maxLength"] == 200
        assert fields["limit"]["minimum"] == 1
        assert fields["limit"]["maximum"] == 100
        assert fields["limit"]["default"] == 25
        assert {"type": "integer", "exclusiveMinimum": 0} in fields["epg_source_id"]["anyOf"]

    @pytest.mark.asyncio
    async def test_upstream_error_is_not_an_empty_search_result(self):
        mcp = _mcp()
        client = AsyncMock()
        client.call_endpoint.side_effect = RuntimeError("https://guide.invalid/?token=private")
        with patch("tools.epg.get_ecm_client", return_value=client):
            result = await mcp.call_tool("search_epg_channels", {"search": "Sports"})
        assert _text(result) == "Error searching EPG channels: the catalogue request failed."
        assert "private" not in _text(result)
        client.call_endpoint.assert_awaited_once()
