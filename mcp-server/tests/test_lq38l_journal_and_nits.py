"""Regression tests for lq38l.11 (journal envelope) and lq38l.13 (display nits).

lq38l.11 — get_journal unwrapped the wrong envelope key (``entries`` instead of
            the backend's paginated ``results``) so it always reported
            "No journal entries found." Fixed in tools/system.py.

lq38l.13 — a cluster of 12 cosmetic display nits across the MCP tools. Each
            sub-test is annotated with the nit number from the bead.
"""
import json

import pytest
from unittest.mock import AsyncMock, patch

from mcp.server.fastmcp import FastMCP


def _parse_dict(result) -> dict:
    """Parse the JSON text a FastMCP dict-returning tool serialises into a
    single TextContent block: ``result == [TextContent(text='{...}')]``."""
    return json.loads(result[0].text)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _client(side_effect=None, return_value=None):
    """Build an AsyncMock ECM client. Pass ``side_effect`` (list/callable) for
    tools that call call_endpoint more than once, or ``return_value`` for a
    single-call tool."""
    mock = AsyncMock()
    if side_effect is not None:
        mock.call_endpoint.side_effect = side_effect
    else:
        mock.call_endpoint.return_value = return_value
    return mock


def _register(module_name: str) -> FastMCP:
    mcp = FastMCP("test")
    if module_name == "system":
        from tools.system import register
    elif module_name == "channels":
        from tools.channels import register
    elif module_name == "streams":
        from tools.streams import register
    elif module_name == "epg":
        from tools.epg import register
    elif module_name == "auto_creation":
        from tools.channel_pipeline import register
    elif module_name == "stats":
        from tools.stats import register
    elif module_name == "export":
        from tools.export import register
    elif module_name == "dedup":
        from tools.dedup import register
    else:  # pragma: no cover - guard
        raise ValueError(module_name)
    register(mcp)
    return mcp


# ===========================================================================
# lq38l.11 — get_journal unwraps the paginated ``results`` envelope
# ===========================================================================

class TestJournalEnvelope:
    @pytest.mark.asyncio
    async def test_unwraps_results_and_renders_entries(self):
        """Backend returns {count, page, ..., results:[...]} — render its rows."""
        mcp = _register("system")
        envelope = {
            "count": 2,
            "page": 1,
            "page_size": 20,
            "total_pages": 1,
            "results": [
                {
                    "id": 1,
                    "timestamp": "2026-05-22T10:00:00Z",
                    "category": "channel",
                    "action_type": "create",
                    "entity_name": "ESPN",
                    "description": "Created channel ESPN",
                },
                {
                    "id": 2,
                    "timestamp": "2026-05-22T11:00:00Z",
                    "category": "settings",
                    "action_type": "update",
                    "entity_name": "probe",
                    "description": None,  # description may be null — must be None-safe
                },
            ],
        }
        mock_client = _client(return_value=envelope)
        with patch("tools.system.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_journal", {"limit": 20})

        text = result[0][0].text
        assert "No journal entries found." not in text
        # bd-0hjrk.1 reworded the header to "Journal entries (showing N of M
        # total):" (M = envelope count) when pagination landed. The behaviour
        # this test guards (results unwrapped, rows + names + descriptions
        # rendered, None-safe) is unchanged.
        assert "Journal entries (showing 2 of 2 total)" in text
        assert "ESPN" in text
        assert "Created channel ESPN" in text
        # entity_name is appended for usefulness
        assert "channel/create: ESPN" in text
        # None description rendered without crashing (None-safe slice)
        assert "settings/update: probe" in text
        assert "Error" not in text

    @pytest.mark.asyncio
    async def test_empty_results_reports_no_entries(self):
        mcp = _register("system")
        mock_client = _client(return_value={"count": 0, "results": []})
        with patch("tools.system.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_journal", {})
        assert "No journal entries found." in result[0][0].text

    @pytest.mark.asyncio
    async def test_legacy_entries_key_still_supported(self):
        """Fall back to the older ``entries`` key if a draft backend returns it."""
        mcp = _register("system")
        mock_client = _client(return_value={"entries": [
            {"timestamp": "t", "category": "m3u", "action_type": "refresh",
             "entity_name": "Prov", "description": "refreshed"},
        ]})
        with patch("tools.system.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_journal", {})
        assert "m3u/refresh: Prov" in result[0][0].text

    @pytest.mark.asyncio
    async def test_batch_id_passed_through_to_backend_query(self):
        """bd-0emgo.5: get_journal(batch_id=...) filters by the run's
        execution_id so an operator can recover a bad auto-creation merge."""
        mcp = _register("system")
        envelope = {
            "count": 1,
            "page": 1,
            "page_size": 20,
            "total_pages": 1,
            "results": [
                {
                    "id": 9,
                    "timestamp": "2026-05-22T12:00:00Z",
                    "category": "auto_creation",
                    "action_type": "merge_stream",
                    "entity_id": 1,
                    "entity_name": "ESPN",
                    "description": "Merged stream 201 into channel 'ESPN'",
                    "batch_id": "555",
                },
            ],
        }
        mock_client = _client(return_value=envelope)
        with patch("tools.system.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool(
                "get_journal", {"limit": 20, "batch_id": "555"}
            )

        # The batch_id must reach the backend journal_list query.
        _, call_kwargs = mock_client.call_endpoint.call_args
        assert call_kwargs["query"]["batch_id"] == "555"
        # And the merge row renders.
        text = result[0][0].text
        assert "auto_creation/merge_stream: ESPN" in text

    @pytest.mark.asyncio
    async def test_batch_id_omitted_when_not_provided(self):
        """No batch_id arg means no batch_id key in the backend query."""
        mcp = _register("system")
        mock_client = _client(return_value={"count": 0, "results": []})
        with patch("tools.system.get_ecm_client", return_value=mock_client):
            await mcp.call_tool("get_journal", {"limit": 5})

        _, call_kwargs = mock_client.call_endpoint.call_args
        assert "batch_id" not in call_kwargs["query"]


# ===========================================================================
# lq38l.13 #1 — channel numbers render as ints (no trailing .0)
# ===========================================================================

class TestChannelNumberFormatting:
    @pytest.mark.asyncio
    async def test_list_channels_strips_float_suffix(self):
        mcp = _register("channels")
        resp = {"count": 1, "results": [
            {"id": 5, "channel_number": 10440.0, "name": "ESPN", "streams": [1, 2]},
        ]}
        mock_client = _client(return_value=resp)
        with patch("tools.channels.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("list_channels", {})
        text = result[0][0].text
        # bd-0emgo.6: non-compact line now reads "#<num> (id=<cid>): <name>" so
        # the channel number isn't mistaken for the API id. The float-suffix
        # check (the point of this test) still holds on the channel number.
        assert "#10440 (id=5):" in text
        assert "10440.0" not in text

    @pytest.mark.asyncio
    async def test_get_channel_strips_float_suffix(self):
        mcp = _register("channels")
        resp = {"id": 5, "channel_number": 6.0, "name": "ESPN", "streams": []}
        mock_client = _client(return_value=resp)
        with patch("tools.channels.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_channel", {"channel_id": 5})
        text = result[0][0].text
        assert "Number: 6" in text
        assert "6.0" not in text

    @pytest.mark.asyncio
    async def test_create_channel_strips_float_suffix(self):
        mcp = _register("channels")
        resp = {"id": 9, "channel_number": 360.0, "name": "New"}
        mock_client = _client(return_value=resp)
        with patch("tools.channels.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("create_channel", {"name": "New"})
        text = result[0][0].text
        assert "#360:" in text
        assert "360.0" not in text

    @pytest.mark.asyncio
    async def test_fractional_number_preserved(self):
        """A genuine fractional channel number is not truncated."""
        from tools.channels import _fmt_channel_number
        assert _fmt_channel_number(10440.5) == "10440.5"
        assert _fmt_channel_number(10440.0) == "10440"
        assert _fmt_channel_number(7) == "7"
        assert _fmt_channel_number("?") == "?"


# ===========================================================================
# lq38l.13 #2 — get_streams_for_channel / get_streams_by_ids show group + provider
# ===========================================================================

class TestStreamGroupProviderResolution:
    @pytest.mark.asyncio
    async def test_streams_for_channel_resolves_names(self):
        mcp = _register("streams")
        # call 1: channels_streams, call 2: providers, call 3: groups
        streams = [{"id": 5203, "name": "US: ESPN", "m3u_account": 3, "channel_group": 10}]
        providers = [{"id": 3, "name": "Provider X"}]
        groups = [{"id": 10, "name": "Sports"}]
        mock_client = _client(side_effect=[streams, providers, groups])
        with patch("tools.streams.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_streams_for_channel", {"channel_id": 1})
        text = result[0][0].text
        assert "[Sports]" in text
        assert "from Provider X" in text
        # the raw provider id must not leak as the provider label
        assert "from 3" not in text

    @pytest.mark.asyncio
    async def test_streams_by_ids_includes_group_and_provider(self):
        mcp = _register("streams")
        streams = [{"id": 5203, "name": "US: ESPN", "m3u_account": 3, "channel_group": 10}]
        providers = [{"id": 3, "name": "Provider X"}]
        groups = [{"id": 10, "name": "Sports"}]
        mock_client = _client(side_effect=[streams, providers, groups])
        with patch("tools.streams.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_streams_by_ids", {"stream_ids": [5203]})
        text = result[0][0].text
        assert "[Sports]" in text
        assert "from Provider X" in text

    @pytest.mark.asyncio
    async def test_falls_back_to_id_when_name_unknown(self):
        mcp = _register("streams")
        streams = [{"id": 1, "name": "S", "m3u_account": 99, "channel_group": 88}]
        mock_client = _client(side_effect=[streams, [], []])  # empty lookup maps
        with patch("tools.streams.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_streams_for_channel", {"channel_id": 1})
        text = result[0][0].text
        assert "provider 99" in text
        assert "group 88" in text


# ===========================================================================
# bd-8w1ba — get_streams_for_channel surfaces a clean not-found message when the
# backend now returns 404 for a non-existent channel id (the backend fix maps the
# upstream Dispatcharr 404 instead of an opaque 500). The MCP client wraps that
# 404 in a RuntimeError chained from the original httpx.HTTPStatusError.
# ===========================================================================

class TestStreamsForChannelNotFound:
    @staticmethod
    def _not_found_runtime_error() -> RuntimeError:
        """Mirror ecm_client._http_error(...) raise-from for a 404: a
        RuntimeError chained (__cause__) to the httpx.HTTPStatusError."""
        import httpx

        request = httpx.Request(
            "GET", "http://ecm/api/channels/999999/streams"
        )
        response = httpx.Response(
            404, request=request, text='{"detail": "Not found."}'
        )
        status_err = httpx.HTTPStatusError(
            "404 Client Error", request=request, response=response
        )
        err = RuntimeError(
            "GET /api/channels/999999/streams -> HTTP 404 Not Found: Not found."
        )
        err.__cause__ = status_err
        return err

    @pytest.mark.asyncio
    async def test_not_found_returns_clean_message(self):
        mcp = _register("streams")
        mock_client = _client(side_effect=self._not_found_runtime_error())
        with patch("tools.streams.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool(
                "get_streams_for_channel", {"channel_id": 999999}
            )
        text = result[0][0].text
        assert "Channel 999999 not found" in text
        assert "internal channel id" in text
        # the not-found message replaces the opaque "Error getting streams" wrap
        assert "Error getting streams" not in text

    @pytest.mark.asyncio
    async def test_non_404_error_keeps_generic_wrap(self):
        mcp = _register("streams")
        mock_client = _client(side_effect=RuntimeError("upstream exploded"))
        with patch("tools.streams.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool(
                "get_streams_for_channel", {"channel_id": 5}
            )
        text = result[0][0].text
        assert "Error getting streams for channel 5" in text
        assert "not found" not in text


# ===========================================================================
# lq38l.13 #3 — get_epg_grid resolves channel names via channel_uuid
# ===========================================================================

class TestEpgGridChannelResolution:
    @pytest.mark.asyncio
    async def test_resolves_channel_name_from_uuid(self):
        mcp = _register("epg")
        grid = {"data": [
            {"channel_uuid": "uuid-1", "title": "News at 6", "start": "06:00", "stop": "07:00"},
        ]}
        channels = {"count": 1, "results": [
            {"id": 42, "uuid": "uuid-1", "name": "BBC One"},
        ]}
        mock_client = _client(side_effect=[grid, channels])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_epg_grid", {})
        text = result[0][0].text
        assert "[BBC One]" in text
        assert "[Unknown]" not in text
        # end time uses `stop`
        assert "06:00 - 07:00" in text

    @pytest.mark.asyncio
    async def test_channel_id_filter_matches_via_uuid(self):
        mcp = _register("epg")
        grid = {"data": [
            {"channel_uuid": "uuid-1", "title": "A", "start": "1", "stop": "2"},
            {"channel_uuid": "uuid-2", "title": "B", "start": "3", "stop": "4"},
        ]}
        channels = {"count": 2, "results": [
            {"id": 42, "uuid": "uuid-1", "name": "BBC One"},
            {"id": 43, "uuid": "uuid-2", "name": "ITV"},
        ]}
        mock_client = _client(side_effect=[grid, channels])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_epg_grid", {"channel_id": 42})
        text = result[0][0].text
        assert "[BBC One]" in text
        assert "ITV" not in text
        assert "No EPG schedule data available." not in text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("details", [None, False])
    async def test_default_output_and_read_paths_remain_unchanged(self, details):
        from _endpoint_contracts import ENDPOINTS

        mock_client = _client(side_effect=[
            [{"channel_uuid": "uuid-1", "title": "News", "start": "06:00", "stop": "07:00"}],
            [{"id": 42, "uuid": "uuid-1", "name": "BBC One"}],
        ])
        arguments = {} if details is None else {"details": details}
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", arguments)
        assert result[0][0].text == "EPG Schedule (1 programs):\n  [BBC One] News (06:00 - 07:00)"
        calls = mock_client.call_endpoint.call_args_list
        assert len(calls) == 2
        assert calls[0].args == (ENDPOINTS["epg_grid"],)
        assert calls[0].kwargs == {}
        assert calls[1].args == (ENDPOINTS["channels_list"],)
        assert calls[1].kwargs == {"query": {"page": 1, "page_size": 500}}
        assert all(call.args[0].method == "GET" for call in calls)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("association", [
        {"channel_id": 42},
        {"channel_id": "42"},
        {"channel_uuid": "uuid-1"},
        {"epg_data_id": 32645},
        {"epg": 32645},
        {"channel": {"id": 42, "uuid": "uuid-1", "tvg_id": "ESPN.us"}},
        {"epg_data": {"id": 32645, "epg_source": 49, "tvg_id": "32645"}},
    ])
    async def test_details_preserve_original_association_values(self, association):
        row = {**association, "id": 900, "title": "Live game",
               "start_time": "2026-09-05T06:00:00Z", "end_time": "2026-09-05T09:00:00Z"}
        channel = {"id": 42, "uuid": "uuid-1", "epg_data_id": 32645,
                   "epg_data": {"id": 32645, "tvg_id": "32645"},
                   "tvg_id": "ESPN.us", "name": "ESPN"}
        mock_client = _client(side_effect=[[row], {"results": [channel]}])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True})
        parsed = json.loads(result[0][0].text)
        assert parsed["programs"] == [row]
        assert parsed["channels"] == [channel]
        assert set(association) <= set(parsed["association_fields"])
        assert parsed["programs_total"] == 1
        assert parsed["programs_truncated"] is False
        assert parsed["channels_truncated"] is False
        assert parsed["channels_error"] is False

    @pytest.mark.asyncio
    async def test_details_bound_rows_references_and_values(self):
        grid = [{"channel_ids": list(range(40)), "title": "x" * 300} for _ in range(40)]
        channels = [{"id": i, "uuid": f"uuid-{i}", "name": "ESPN"} for i in range(1002)]
        mock_client = _client(side_effect=[grid, {"results": channels}])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True, "limit": 100000})
        parsed = json.loads(result[0][0].text)
        assert len(parsed["programs"]) == 25
        assert parsed["programs_total"] == 40
        assert parsed["programs_truncated"] is True
        assert len(parsed["channels"]) == 1000
        assert parsed["channels_truncated"] is True
        assert parsed["values_truncated"] is True
        assert parsed["programs"][0] == {"channel_ids": list(range(25)), "title": "x" * 256}
        assert len(result[0][0].text) < 100000

    @pytest.mark.asyncio
    async def test_details_project_identity_without_private_or_nested_values(self):
        row = {
            "channel": {"id": 42, "name": "ESPN", "password": "nested-secret",
                        "epg_source": {"id": 49, "token": "nested-token"}},
            "epg_listing": {"secret": "unknown-object"},
            "channel_secret": "association-secret", "title": "Game",
            "description": "private-description", "icon": "https://private/icon",
            "channel_name": "https://private/key", "channel_uuids": ["uuid-1", {"secret": "array-secret"}],
            "uuid": "Bearer private-token",
        }
        channel = {"id": 42, "name": "ESPN", "epg_data": {"id": 3, "secret": "source-secret"},
                   "logo": "https://private/logo", "password": "channel-password"}
        mock_client = _client(side_effect=[[row], [channel]])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True})
        text = result[0][0].text
        parsed = json.loads(text)
        assert parsed["programs"] == [{
            "channel": {"id": 42, "name": "ESPN"}, "title": "Game", "channel_uuids": ["uuid-1"],
        }]
        assert parsed["channels"] == [{"id": 42, "name": "ESPN", "epg_data": {"id": 3}}]
        assert "epg_listing" in parsed["association_fields"]
        assert "epg_listing" not in parsed["programs"][0]
        assert parsed["values_omitted"] is True
        for value in ("secret", "token", "private", "https://", "password", "description", "icon", "logo"):
            assert value not in text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("private", [" \tBearer private-token", " \nBasic private-token", "  //guide.invalid/list?token=private-token", " \twww.guide.invalid/private-token"])
    async def test_details_omit_whitespace_prefixed_private_values(self, private):
        mock_client = _client(side_effect=[
            [{"uuid": private, "title": "  Game  "}],
            [{"id": 42, "tvg_id": private}],
        ])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True})
        text = result[0][0].text
        parsed = json.loads(text)
        assert "private-token" not in text
        assert parsed["programs"] == [{"title": "  Game  "}]
        assert parsed["channels"] == [{"id": 42}]
        assert parsed["values_omitted"] is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("row", [
        {"channel": {"logo": "private-value", "slug": "private-value"}},
        {"epg_listing": {"id": "private-value"}},
        {"channel-uuid": "private-value"},
        {"channel uuid": "private-value"},
        {"channel_" + "x" * 65: "private-value"},
        {"channel_secret": "private-value"},
    ])
    async def test_details_report_allowlist_only_omissions(self, row):
        mock_client = _client(side_effect=[[row], []])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True})
        text = result[0][0].text
        assert "private-value" not in text
        assert json.loads(text)["values_omitted"] is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("group", ["channels", "programs"])
    async def test_details_bound_total_encoded_output(self, group):
        programs, channels = [{"id": 1, "title": "Game"}], []
        if group == "channels":
            values = ["x" * 256] * 25
            channels = [{"id": i, "epg": values, "epg_data": values} for i in range(200)]
        else:
            fields = ("id", "uuid", "channel_id", "epg_data_id", "epg_source", "epg_source_id", "tvg_id", "name")
            nested = {key: ["🏟" * 256] * 25 for key in fields}
            programs = [{"id": i, "epg_data": nested} for i in range(25)]
        mock_client = _client(side_effect=[programs, channels])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True, "limit": 25})
        text = result[0][0].text
        assert len(text.encode("utf-8")) <= 1024 * 1024
        parsed = json.loads(text)
        assert parsed[group + "_truncated"] is True
        assert 0 < len(parsed[group]) < len(channels if group == "channels" else programs)
        assert parsed["programs_returned"] == len(parsed["programs"])
        assert parsed["channels_returned"] == len(parsed["channels"])
        original = channels if group == "channels" else programs
        assert parsed[group] == original[:len(parsed[group])]

    @pytest.mark.asyncio
    async def test_details_keep_all_normal_channel_references(self):
        channels = [{"id": i, "uuid": f"uuid-{i}", "epg_data_id": 1000 + i, "tvg_id": f"channel-{i}", "name": f"Channel {i}"} for i in range(64)]
        mock_client = _client(side_effect=[[{"id": 1, "title": "Game"}], channels])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True})
        parsed = json.loads(result[0][0].text)
        assert parsed["channels"] == channels
        assert parsed["channels_truncated"] is False

    @pytest.mark.asyncio
    async def test_details_preserve_filter_semantics_and_report_empty_selection(self):
        mock_client = _client(side_effect=[
            [{"channel_id": "42", "title": "Game"}],
            [{"id": 42, "uuid": "uuid-1", "name": "ESPN"}],
        ])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True, "channel_id": 42})
        parsed = json.loads(result[0][0].text)
        assert parsed["programs"] == []
        assert parsed["programs_total"] == 0
        assert parsed["channels"][0]["id"] == 42

    @pytest.mark.asyncio
    @pytest.mark.parametrize("limit", [0, -1])
    async def test_details_reject_nonpositive_limit_before_reading(self, limit):
        mock_client = _client()
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True, "limit": limit})
        assert "limit must be positive" in result[0][0].text
        mock_client.call_endpoint.assert_not_called()

    @pytest.mark.asyncio
    async def test_details_report_partial_channel_lookup_failure(self):
        mock_client = _client(side_effect=[
            [{"epg_data_id": 3, "title": "Game"}],
            {"results": [{"id": 42, "uuid": "uuid-1", "name": "ESPN"}], "next": "next-page"},
            RuntimeError("https://private/token"),
        ])
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True})
        parsed = json.loads(result[0][0].text)
        assert parsed["channels_error"] is True
        assert parsed["channels"] == [{"id": 42, "uuid": "uuid-1", "name": "ESPN"}]
        assert parsed["programs"] == [{"epg_data_id": 3, "title": "Game"}]
        assert "private" not in result[0][0].text

    @pytest.mark.asyncio
    async def test_details_keep_upstream_failure_as_redacted_error(self):
        mock_client = _client(side_effect=RuntimeError("https://private/token"))
        with patch("tools.epg.get_ecm_client", return_value=mock_client):
            result = await _register("epg").call_tool("get_epg_grid", {"details": True})
        assert result[0][0].text == "Error getting EPG grid."
        assert mock_client.call_endpoint.call_count == 1


# ===========================================================================
# lq38l.13 #4 — get_auto_creation_rule renders create_channel descriptor
# ===========================================================================

class TestAutoCreationRuleActionDescriptor:
    @pytest.mark.asyncio
    async def test_create_channel_action_shows_name_template(self):
        mcp = _register("auto_creation")
        rule = {
            "id": 1, "name": "R", "enabled": True, "priority": 0,
            "conditions": [],
            "actions": [
                {"type": "create_channel", "name_template": "{stream_name}", "if_exists": "merge"},
                {"type": "create_group", "name_template": "Entertainment", "if_exists": "use_existing"},
            ],
        }
        mock_client = _client(return_value=rule)
        with patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_auto_creation_rule", {"rule_id": 1})
        text = result[0][0].text
        assert "create_channel: {stream_name}" in text
        assert "create_group: Entertainment" in text
        assert "create_channel: ?" not in text


# ===========================================================================
# lq38l.13 #5 — run_auto_creation dry-run sample names + consistent "N more"
# ===========================================================================

class TestRunAutoCreationDryRunSample:
    @pytest.mark.asyncio
    async def test_dry_run_sample_uses_real_names_and_consistent_more(self):
        mcp = _register("auto_creation")
        # 2 would_create rows + 1 non-creating (skip) row.
        dry_rows = [
            {"stream_name": "ESPN", "action": "Created channel 'ESPN'", "would_create": True},
            {"stream_name": "CNN", "action": "Created channel 'CNN'", "would_create": True},
            {"stream_name": "Junk", "action": "Skipped", "would_create": False},
        ]
        kickoff = {"execution_id": 7, "status": "running"}
        final = {
            "id": 7, "status": "completed", "mode": "dry_run",
            "streams_evaluated": 100, "streams_matched": 2,
            "channels_created": 2, "channels_updated": 0, "groups_created": 0,
            "streams_skipped": 1, "duration_seconds": 1.0,
            "dry_run_results": dry_rows,
        }
        mock_client = _client(side_effect=[kickoff, final])
        with (
            patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client),
            patch("tools.channel_pipeline._poll_sleep", new=AsyncMock(return_value=None)),
        ):
            result = await mcp.call_tool("run_auto_creation", {"dry_run": True})
        text = result[0][0].text
        # Real names rendered, not "?"
        assert "ESPN" in text
        assert "CNN" in text
        # The non-creating row is excluded from the sample
        assert "Junk" not in text
        # Summary count is consistent with the would_create sample (2 each)
        assert "Channels would be created: 2" in text

    @pytest.mark.asyncio
    async def test_more_line_counts_only_would_create(self):
        mcp = _register("auto_creation")
        # 25 would_create + 5 non-creating → "... and 5 more" (25 - 20), not 10.
        dry_rows = (
            [{"stream_name": f"C{i}", "action": "Created", "would_create": True} for i in range(25)]
            + [{"stream_name": f"S{i}", "action": "Skipped", "would_create": False} for i in range(5)]
        )
        kickoff = {"execution_id": 8, "status": "running"}
        final = {
            "id": 8, "status": "completed", "mode": "dry_run",
            "streams_evaluated": 30, "streams_matched": 25,
            "channels_created": 25, "duration_seconds": 1.0,
            "dry_run_results": dry_rows,
        }
        mock_client = _client(side_effect=[kickoff, final])
        with (
            patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client),
            patch("tools.channel_pipeline._poll_sleep", new=AsyncMock(return_value=None)),
        ):
            result = await mcp.call_tool("run_auto_creation", {"dry_run": True})
        text = result[0][0].text
        assert "... and 5 more" in text
        assert "and 30 more" not in text


# ===========================================================================
# lq38l.13 #6 — get_top_watched renders the real metric (no fabricated viewers)
# ===========================================================================

class TestTopWatchedViewers:
    @pytest.mark.asyncio
    async def test_renders_watch_count_not_question_mark(self):
        mcp = _register("stats")
        resp = [
            {"channel_id": "abc", "channel_name": "ESPN",
             "watch_count": 12, "total_watch_seconds": 7200},
        ]
        mock_client = _client(return_value=resp)
        with patch("tools.stats.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_top_watched", {"limit": 10})
        text = result[0][0].text
        assert "? unique viewers" not in text
        assert "12 views" in text


# ===========================================================================
# lq38l.13 #7 — create_export_profile validation: removed with the Export tab
# (beads vrrxv / 1w428). The create_export_profile MCP tool no longer exists.
# ===========================================================================


# ===========================================================================
# lq38l.13 #8 — dismiss_channel_merge returns a clean envelope on 404
# ===========================================================================

class TestDismissMergeNotFound:
    @pytest.mark.asyncio
    async def test_404_returns_clean_envelope_not_raised(self):
        mcp = _register("dedup")
        err = RuntimeError("POST /api/channel-merges/99/dismiss -> HTTP 404 Not Found")
        mock_client = AsyncMock()
        mock_client.call_endpoint.side_effect = err
        with patch("tools.dedup.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("dismiss_channel_merge", {"merge_id": 99})
        envelope = _parse_dict(result)
        assert "error" in envelope
        assert envelope["error"]["code"] == "MERGE_NOT_FOUND"

    @pytest.mark.asyncio
    async def test_409_still_invalid_state(self):
        mcp = _register("dedup")
        err = RuntimeError("POST /api/channel-merges/5/dismiss -> HTTP 409 Conflict")
        mock_client = AsyncMock()
        mock_client.call_endpoint.side_effect = err
        with patch("tools.dedup.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("dismiss_channel_merge", {"merge_id": 5})
        envelope = _parse_dict(result)
        assert envelope["error"]["code"] == "INVALID_STATE"


# ===========================================================================
# lq38l.13 #9 — list_pending_channel_merges coerces candidate_channel_id to int
# ===========================================================================

class TestPendingMergesIntCoercion:
    @pytest.mark.asyncio
    async def test_candidate_channel_id_coerced_to_int(self):
        mcp = _register("dedup")
        resp = {
            "merges": [
                {"id": 1, "candidate_channel_id": "5203", "stream_name": "ESPN"},
                {"id": 2, "candidate_channel_id": "42", "stream_name": "CNN"},
            ],
            "total": 2, "page": 1, "page_size": 50, "total_pages": 1,
        }
        mock_client = _client(return_value=resp)
        with patch("tools.dedup.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("list_pending_channel_merges", {})
        envelope = _parse_dict(result)
        assert envelope["merges"][0]["candidate_channel_id"] == 5203
        assert envelope["merges"][1]["candidate_channel_id"] == 42
        assert isinstance(envelope["merges"][0]["candidate_channel_id"], int)


# ===========================================================================
# lq38l.13 #10 — get_probe_results reports "No probe results available."
# ===========================================================================

class TestProbeResultsEmpty:
    @pytest.mark.asyncio
    async def test_all_zero_counts_reports_no_results(self):
        mcp = _register("streams")
        resp = {
            "success_count": 0, "failed_count": 0, "skipped_count": 0,
            "black_screen_count": 0, "low_fps_count": 0,
            "success_streams": [], "failed_streams": [],
        }
        mock_client = _client(return_value=resp)
        with patch("tools.streams.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_probe_results", {})
        text = result[0][0].text
        assert text == "No probe results available."

    @pytest.mark.asyncio
    async def test_nonzero_counts_render_results(self):
        mcp = _register("streams")
        resp = {"success_count": 5, "failed_count": 1, "skipped_count": 0}
        mock_client = _client(return_value=resp)
        with patch("tools.streams.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("get_probe_results", {})
        text = result[0][0].text
        assert "Latest Probe Results:" in text
        assert "Success Count: 5" in text


# ===========================================================================
# lq38l.13 #11 — cancel_probe message when nothing is running
# ===========================================================================

class TestCancelProbeNoRun:
    @pytest.mark.asyncio
    async def test_no_probe_running_message_not_contradictory(self):
        mcp = _register("streams")
        resp = {"status": "no_probe_running", "message": "No probe is currently running"}
        mock_client = _client(return_value=resp)
        with patch("tools.streams.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("cancel_probe", {})
        text = result[0][0].text
        assert "Probe cancelled." not in text
        assert text == "No probe is currently running"

    @pytest.mark.asyncio
    async def test_actual_cancellation_says_cancelled(self):
        mcp = _register("streams")
        resp = {"status": "cancelling", "message": "Probe cancellation requested"}
        mock_client = _client(return_value=resp)
        with patch("tools.streams.get_ecm_client", return_value=mock_client):
            result = await mcp.call_tool("cancel_probe", {})
        text = result[0][0].text
        assert "Probe cancelled." in text
        assert "Probe cancellation requested" in text


# ===========================================================================
# lq38l.13 #12 — create_auto_creation_rule passes the two new params through
# ===========================================================================

class TestCreateRuleNewParams:
    @pytest.mark.asyncio
    async def test_quality_tie_break_and_match_scope_passed_through(self):
        mcp = _register("auto_creation")
        mock_client = _client(return_value={"id": 11, "name": "R"})
        with patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client):
            await mcp.call_tool("create_auto_creation_rule", {
                "name": "R",
                "conditions": [],
                "actions": [],
                "quality_tie_break_order": "asc",
                "match_scope_target_group": False,
            })
        call = mock_client.call_endpoint.call_args
        body = call.kwargs["body"]
        assert body["quality_tie_break_order"] == "asc"
        assert body["match_scope_target_group"] is False

    @pytest.mark.asyncio
    async def test_params_omitted_when_not_set(self):
        """Defaults stay None → fields absent from the body (backend keeps its defaults)."""
        mcp = _register("auto_creation")
        mock_client = _client(return_value={"id": 12, "name": "R"})
        with patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client):
            await mcp.call_tool("create_auto_creation_rule", {
                "name": "R", "conditions": [], "actions": [],
            })
        body = mock_client.call_endpoint.call_args.kwargs["body"]
        assert "quality_tie_break_order" not in body
        assert "match_scope_target_group" not in body

# ===========================================================================
# enhancedchannelmanager-zrte6 — allow_manual_channel_merge (PR #547 / orzck)
# is settable from MCP on both create and update rule tools (canonical names
# AND the deprecated *_auto_creation_rule aliases, which forward explicitly).
# ===========================================================================

class TestAllowManualChannelMergeParam:
    @pytest.mark.asyncio
    async def test_create_passes_flag_through(self):
        mcp = _register("auto_creation")
        mock_client = _client(return_value={"id": 21, "name": "R"})
        with patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client):
            await mcp.call_tool("create_channel_pipeline_rule", {
                "name": "R", "conditions": [], "actions": [],
                "allow_manual_channel_merge": True,
            })
        body = mock_client.call_endpoint.call_args.kwargs["body"]
        assert body["allow_manual_channel_merge"] is True

    @pytest.mark.asyncio
    async def test_create_alias_forwards_flag(self):
        mcp = _register("auto_creation")
        mock_client = _client(return_value={"id": 22, "name": "R"})
        with patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client):
            await mcp.call_tool("create_auto_creation_rule", {
                "name": "R", "conditions": [], "actions": [],
                "allow_manual_channel_merge": True,
            })
        body = mock_client.call_endpoint.call_args.kwargs["body"]
        assert body["allow_manual_channel_merge"] is True

    @pytest.mark.asyncio
    async def test_create_omits_flag_when_not_set(self):
        """None default → field absent so the backend default (False) rules."""
        mcp = _register("auto_creation")
        mock_client = _client(return_value={"id": 23, "name": "R"})
        with patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client):
            await mcp.call_tool("create_channel_pipeline_rule", {
                "name": "R", "conditions": [], "actions": [],
            })
        body = mock_client.call_endpoint.call_args.kwargs["body"]
        assert "allow_manual_channel_merge" not in body

    @pytest.mark.asyncio
    async def test_update_passes_flag_through(self):
        mcp = _register("auto_creation")
        mock_client = _client(return_value={"rule": {"id": 24, "name": "R"}})
        with patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client):
            await mcp.call_tool("update_channel_pipeline_rule", {
                "rule_id": 24,
                "allow_manual_channel_merge": True,
            })
        body = mock_client.call_endpoint.call_args.kwargs["body"]
        assert body == {"allow_manual_channel_merge": True}

    @pytest.mark.asyncio
    async def test_update_alias_forwards_flag(self):
        mcp = _register("auto_creation")
        mock_client = _client(return_value={"rule": {"id": 25, "name": "R"}})
        with patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client):
            await mcp.call_tool("update_auto_creation_rule", {
                "rule_id": 25,
                "allow_manual_channel_merge": False,
            })
        body = mock_client.call_endpoint.call_args.kwargs["body"]
        assert body == {"allow_manual_channel_merge": False}

    @pytest.mark.asyncio
    async def test_update_stringified_bool_coerced(self):
        """Top-level typed params get the same lax coercion as GH #600 nested
        values — a client sending the stringified "true" must not 400."""
        mcp = _register("auto_creation")
        mock_client = _client(return_value={"rule": {"id": 26, "name": "R"}})
        with patch("tools.channel_pipeline.get_ecm_client", return_value=mock_client):
            await mcp.call_tool("update_channel_pipeline_rule", {
                "rule_id": 26,
                "allow_manual_channel_merge": "true",
            })
        body = mock_client.call_endpoint.call_args.kwargs["body"]
        assert body == {"allow_manual_channel_merge": True}
