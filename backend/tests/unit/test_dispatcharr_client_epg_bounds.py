"""Bounded EPG-data response handling for migration callers."""

import asyncio
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from config import DispatcharrSettings
from dispatcharr_client import DispatcharrClient

# Safety bound for the off-loop handshake below. It is a deadlock guard, not an
# assertion threshold: the healthy path clears it in microseconds, and a value
# this generous (10x the worst event-loop stall ever observed on a saturated CI
# runner) cannot be reached by machine load alone.
_LOOP_LIVENESS_TIMEOUT = 10.0


def _client(handler, source_count=None) -> DispatcharrClient:
    def serve(request):
        if source_count is not None and request.url.path == "/api/epg/sources/":
            return httpx.Response(200, json=[{"id": 46, "epg_data_count": source_count}])
        return handler(request)

    client = DispatcharrClient(
        DispatcharrSettings(url="http://dispatcharr", auth_method="api_key", api_key="k")
    )
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(serve))
    return client


class _TrackingStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes, chunk_size: int | None = None):
        self.content = content
        self.chunk_size = chunk_size or max(1, len(content))
        self.iterated = False
        self.closed = False

    async def __aiter__(self):
        self.iterated = True
        for offset in range(0, len(self.content), self.chunk_size):
            yield self.content[offset:offset + self.chunk_size]

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
async def test_bounded_flat_response_rejected_before_json_decode():
    payload = b"[" + (b" " * (1024 * 1024)) + b"]"
    client = _client(lambda request: httpx.Response(200, content=payload))
    try:
        with patch(
            "dispatcharr_client.json.loads",
            side_effect=AssertionError("oversized body must not be decoded"),
        ):
            with pytest.raises(ValueError, match="EPG response exceeds"):
                await client.get_epg_data(max_results=1)
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_bounded_flat_response_returns_normal_rows():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200, json=[{"id": 1, "tvg_id": "101"}, {"id": 2, "tvg_id": "102"}]
        )

    client = _client(handler)
    try:
        assert await client.get_epg_data(max_results=2) == [
            {"id": 1, "tvg_id": "101"},
            {"id": 2, "tvg_id": "102"},
        ]
        assert requests[0].headers["Accept-Encoding"] == "identity"
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("encoding", ["gzip", "br", "deflate"])
async def test_encoded_response_rejected_before_stream_or_json_decode(encoding):
    stream = _TrackingStream(b'[{"id": 1}]')
    client = _client(
        lambda request: httpx.Response(
            200,
            headers={"Content-Encoding": encoding},
            stream=stream,
        )
    )
    try:
        with patch(
            "dispatcharr_client.json.loads",
            side_effect=AssertionError("encoded body must not be decoded"),
        ):
            with pytest.raises(ValueError, match="unexpected Content-Encoding"):
                await client.get_epg_data(max_results=1)
        assert stream.iterated is False
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{}, {"Content-Encoding": "identity"}])
async def test_absent_or_identity_encoding_is_accepted(headers):
    client = _client(
        lambda request: httpx.Response(200, headers=headers, json=[{"id": 1}])
    )
    try:
        assert await client.get_epg_data(max_results=1) == [{"id": 1}]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_large_json_decode_does_not_block_event_loop():
    """A slow EPG decode must run off the event loop thread.

    Asserted as a *mechanism*, not as a latency budget. The previous shape
    measured the wall-clock scheduling latency of a 10ms sleep against a 50ms
    budget, and that measurement cannot separate the two cases it needs to tell
    apart. Measured on this suite (bead enhancedchannelmanager-kcyiq): a
    genuinely broken inline decode costs ~0.10s, while the same *healthy* code
    on a saturated GitHub runner produced 0.79s and 0.97s. Every threshold that
    catches the regression is one the loaded runner blows through, so the old
    assertion reported an identical failure for a real regression and for a busy
    machine. Both assertions below are event-driven rather than clock-driven and
    are therefore immune to machine load.
    """
    client = _client(lambda request: httpx.Response(200, content=b'[{"id": 1}]'))
    real_loads = __import__("json").loads

    loop_thread_ident = threading.get_ident()
    decode_entered = threading.Event()
    loop_advanced = threading.Event()
    observed: dict = {}

    def slow_loads(payload):
        observed["thread_ident"] = threading.get_ident()
        observed["thread_name"] = threading.current_thread().name
        decode_entered.set()
        # Hold the decode open until the event loop proves it is still running.
        # This replaces a fixed sleep: the decode is guaranteed to still be in
        # flight when liveness is observed, instead of racing a timer.
        observed["loop_advanced_during_decode"] = loop_advanced.wait(
            timeout=_LOOP_LIVENESS_TIMEOUT
        )
        return real_loads(payload)

    try:
        with patch("dispatcharr_client.json.loads", side_effect=slow_loads):
            decode_task = asyncio.create_task(client.get_epg_data(max_results=1))
            # Reaching this poll at all means the loop was not blocked by the
            # decode. The deadline is a hang guard, not an assertion.
            poll_deadline = time.monotonic() + _LOOP_LIVENESS_TIMEOUT
            while not decode_entered.is_set() and time.monotonic() < poll_deadline:
                await asyncio.sleep(0.001)
            loop_advanced.set()
            result = await decode_task

        assert decode_entered.is_set(), "the patched json.loads was never called"
        assert observed["thread_ident"] != loop_thread_ident, (
            "json.loads ran on the event loop thread "
            f"({observed['thread_name']}) — the decode is blocking the loop"
        )
        assert observed["loop_advanced_during_decode"] is True, (
            "the event loop did not advance while the decode was in flight — "
            f"decode ran on {observed['thread_name']}"
        )
        assert result == [{"id": 1}]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_bounded_paginated_response_stops_at_exact_limit():
    def handler(request: httpx.Request):
        page = request.url.params.get("page")
        if page == "1":
            return httpx.Response(
                200,
                json={"results": [{"id": 1}], "next": "page-2"},
            )
        return httpx.Response(
            200,
            json={"results": [{"id": 2}, {"id": 3}], "next": None},
        )

    client = _client(handler)
    try:
        assert await client.get_epg_data(max_results=2) == [{"id": 1}, {"id": 2}]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_flat_catalogue_selects_late_rows_without_retaining_unrelated_rows():
    wanted = {"id": 9001, "epg_source": 46, "tvg_id": "ecm-4279", "name": "NRL Warriors"}
    rows = [{"id": n, "epg_source": 42, "tvg_id": str(n), "name": "Other " + "x" * 400}
            for n in range(3000)] + [wanted]

    def handler(request):
        if request.url.path == "/api/epg/sources/":
            return httpx.Response(200, json=[{"id": 42, "epg_data_count": 3000},
                                            {"id": 46, "epg_data_count": 1}])
        return httpx.Response(200, json=rows)

    client = _client(handler)
    try:
        assert await client.get_epg_data(search="ECM-4279", epg_source=46, max_results=1) == [wanted]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_paginated_catalogue_skips_unrelated_pages_before_limit():
    seen = []
    wanted = {"id": 8, "epg_source": 46, "tvg_id": "ecm-8", "name": "Sports"}

    def handler(request):
        if request.url.path == "/api/epg/sources/":
            return httpx.Response(200, json=[{"id": 46, "epg_data_count": 2}])
        seen.append(request.url.params.get("page"))
        if seen[-1] == "1":
            return httpx.Response(200, json={"results": [dict(wanted, epg_source=42)], "next": "page-2"})
        return httpx.Response(200, json={"results": [wanted], "next": None})

    client = _client(handler)
    try:
        assert await client.get_epg_data(search="sports", epg_source=46, max_results=1) == [wanted]
        assert seen == ["1", "2"]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_flat_catalogue_never_returns_unrelated_source_or_name():
    def handler(request):
        if request.url.path == "/api/epg/sources/":
            return httpx.Response(200, json=[{"id": 46, "epg_data_count": 2}])
        return httpx.Response(200, json=[
            {"id": 1, "epg_source": 42, "name": "ESPN", "tvg_id": "ESPN.us"},
            {"id": 2, "epg_source": 46, "name": "Other", "tvg_id": "ecm-2"},
        ])

    client = _client(handler)
    try:
        assert await client.get_epg_data(search="ESPN", epg_source=46, max_results=1) == []
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("paginated", [False, True])
@pytest.mark.parametrize("ensure_ascii", [False, True])
async def test_filtered_catalogue_handles_split_unicode_and_json_delimiters(paginated, ensure_ascii):
    import json
    wanted = {"id": 8, "epg_source": {"id": 46}, "tvg_id": "ecm-8",
              "name": 'Sport 🏆 é \\" } ], : more'}
    rows = [dict(wanted, id=2, epg_source=42), wanted, dict(wanted, id=9)]
    content = {"count": 12345, "results": rows, "next": None} if paginated else rows
    encoded = json.dumps(content, ensure_ascii=ensure_ascii).encode()
    marker = b"\\ud83c" if ensure_ascii else "🏆".encode()
    boundary = encoded.index(marker) + 2
    encoded = b" " * (65536 - boundary) + encoded
    stream = _TrackingStream(encoded, 1)
    client = _client(lambda request: httpx.Response(200, stream=stream), source_count=3)
    try:
        assert await client.get_epg_data(search="SPORT", epg_source=46, max_results=1) == [wanted]
        assert stream.closed
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("tail", [b",]", b"] false", b", {", b", null]", b", [1]]"])
async def test_filtered_catalogue_rejects_invalid_tail_after_matching_limit(tail):
    stream = _TrackingStream(b'[{"id":8,"epg_source":46,"name":"Sport"}' + tail, 7)
    client = _client(lambda request: httpx.Response(200, stream=stream), source_count=3)
    try:
        with pytest.raises(ValueError):
            await client.get_epg_data(search="Sport", epg_source=46, max_results=1)
        assert stream.closed
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("content,reason", [
    (b'{"results":[],"results":[]}', "invalid results"),
    (b'{"count":0}', "incomplete"),
    (b'{"results":[],}', "Expecting value"),
    (b'[]{}', "trailing JSON"),
])
async def test_filtered_catalogue_rejects_invalid_envelopes(content, reason):
    client = _client(lambda request: httpx.Response(200, stream=_TrackingStream(content, 3)), source_count=1)
    try:
        with pytest.raises(ValueError, match=reason):
            await client.get_epg_data(epg_source=46, max_results=1)
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("complete", [False, True])
async def test_filtered_catalogue_bounds_individual_row(complete):
    content = b'[{"id":8,"epg_source":46,"name":"' + b'x' * 17000
    if complete:
        content += b'"}]'
    stream = _TrackingStream(content, 2048)
    client = _client(lambda request: httpx.Response(200, stream=stream), source_count=1)
    try:
        with pytest.raises(ValueError, match="bytes per row"):
            await client.get_epg_data(epg_source=46, max_results=1)
        assert stream.closed
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_catalogue_bounds_scan_when_counts_are_stale():
    stream = _TrackingStream(b'[{"id":1},{"id":2}]')
    client = _client(lambda request: httpx.Response(200, stream=stream), source_count=0)
    try:
        with pytest.raises(ValueError, match="source row counts"):
            await client.get_epg_data(epg_source=46, page_size=1, max_results=1)
        assert stream.closed
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_catalogue_bounds_total_bytes_despite_whitespace():
    stream = _TrackingStream(b'[' + b' ' * (1024 * 1024) + b']', 65536)
    client = _client(lambda request: httpx.Response(200, stream=stream), source_count=0)
    try:
        with pytest.raises(ValueError, match="exceeds 1048576 bytes"):
            await client.get_epg_data(epg_source=46, page_size=1, max_results=1)
        assert stream.closed
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("sources", [[], [{"id": 46}], [{"id": 46, "epg_data_count": -1}],
                                     [{"id": 46, "epg_data_count": True}],
                                     [{"id": 46, "epg_data_count": 200001}], "invalid"])
async def test_filtered_catalogue_requires_bounded_source_counts_before_read(sources):
    seen = []
    def handler(request):
        seen.append(request.url.path)
        return httpx.Response(200, json=sources)
    client = _client(handler)
    try:
        with pytest.raises(ValueError):
            await client.get_epg_data(epg_source=46, max_results=1)
        assert seen == ["/api/epg/sources/"]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_catalogue_releases_unselected_rows_and_bounds_decoder_buffer():
    import json
    wanted = {"id": 9001, "epg_source": 46, "tvg_id": "ecm-9001", "name": "Selected"}
    content = json.dumps([{"id": n, "epg_source": 42, "name": "x" * 400}
                          for n in range(3000)] + [wanted]).encode()
    real_decode = json.JSONDecoder.raw_decode
    seen = {"live": 0, "peak": 0, "buffer": 0}

    class Row(dict):
        def __init__(self, value):
            super().__init__(value)
            seen["live"] += 1
            seen["peak"] = max(seen["peak"], seen["live"])

        def __del__(self):
            seen["live"] -= 1

    def decode(decoder, text, idx=0):
        seen["buffer"] = max(seen["buffer"], len(text.encode()))
        value, end = real_decode(decoder, text, idx)
        return (Row(value) if isinstance(value, dict) else value), end

    client = _client(lambda request: httpx.Response(200, stream=_TrackingStream(content)), source_count=3001)
    try:
        with patch.object(json.JSONDecoder, "raw_decode", decode):
            result = await client.get_epg_data(epg_source=46, max_results=1)
        assert result == [wanted]
        assert seen["peak"] <= 3
        assert seen["buffer"] <= 65536 + 16384
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_catalogue_decode_keeps_event_loop_live():
    import json
    real_decode = json.JSONDecoder.raw_decode
    entered, advanced = threading.Event(), threading.Event()
    loop_thread = threading.get_ident()
    observed = []

    def decode(decoder, text, idx=0):
        if text.startswith('{"id"'):
            observed.append(threading.get_ident())
            entered.set()
            assert advanced.wait(_LOOP_LIVENESS_TIMEOUT)
        return real_decode(decoder, text, idx)

    client = _client(lambda request: httpx.Response(200, content=b'[{"id":1,"epg_source":46}]'), source_count=1)
    try:
        with patch.object(json.JSONDecoder, "raw_decode", decode):
            task = asyncio.create_task(client.get_epg_data(epg_source=46, max_results=1))
            deadline = time.monotonic() + _LOOP_LIVENESS_TIMEOUT
            while not entered.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(0.001)
            advanced.set()
            result = await task
        assert observed and all(thread != loop_thread for thread in observed)
        assert result == [{"id": 1, "epg_source": 46}]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_catalogue_deadline_closes_incomplete_stream():
    class Pending(_TrackingStream):
        async def __aiter__(self):
            yield b"["
            await asyncio.Event().wait()

    stream = Pending(b"")
    client = _client(lambda request: httpx.Response(200, stream=stream))
    try:
        with pytest.raises(TimeoutError):
            await client._get_json_bounded(
                "/api/epg/epgdata/", params={"epg_source": 46}, max_bytes=1048576,
                max_results=1, limits={"rows": 10, "bytes": 1048576},
                deadline=asyncio.get_running_loop().time() + 0.05,
            )
        assert stream.closed
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_catalogue_numeric_envelope_value_crosses_chunk_boundary():
    content = b'{"count":12345,"results":[{"id":1,"epg_source":46}],"next":null}'
    content = b" " * (65536 - content.index(b"12345") - 2) + content
    client = _client(lambda request: httpx.Response(200, stream=_TrackingStream(content, 9)), source_count=1)
    try:
        assert await client.get_epg_data(epg_source=46, max_results=1) == [{"id": 1, "epg_source": 46}]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_catalogue_cancellation_closes_response():
    started = asyncio.Event()
    class Pending(_TrackingStream):
        async def __aiter__(self):
            started.set()
            await asyncio.Event().wait()
            yield b"[]"

    stream = Pending(b"")
    client = _client(lambda request: httpx.Response(200, stream=stream), source_count=1)
    try:
        task = asyncio.create_task(client.get_epg_data(epg_source=46, max_results=1))
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("encoding", ["gzip", "br"])
async def test_filtered_catalogue_rejects_unexpected_encoding_without_consuming(encoding):
    stream = _TrackingStream(b"[]")
    client = _client(lambda request: httpx.Response(200, headers={"Content-Encoding": encoding}, stream=stream),
                     source_count=1)
    try:
        with pytest.raises(ValueError, match="unexpected Content-Encoding"):
            await client.get_epg_data(epg_source=46, max_results=1)
        assert stream.closed and not stream.iterated
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_filtered_catalogue_does_not_treat_boolean_source_as_integer():
    client = _client(lambda request: httpx.Response(200, json=[{"id": 1, "epg_source": True}]), source_count=1)
    try:
        assert await client.get_epg_data(epg_source=1, max_results=1) == []
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_exact_ids_select_flat_rows_without_sending_an_id_filter():
    requests = []
    wanted = [
        {"id": 8, "epg_source": {"id": 46}, "tvg_id": "eight", "name": "Eight"},
        {"id": 13, "epg_source_id": 47, "tvg_id": "thirteen", "name": "Thirteen"},
    ]
    rows = [
        {"id": 1, "epg_source": 99, "tvg_id": "other"},
        *wanted,
        {"id": 21, "epg_source": 99, "tvg_id": "other-21"},
    ]

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json=rows)

    client = _client(handler, source_count=len(rows))
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=30)
    try:
        result = await client.get_epg_data(
            max_results=2,
            ids=frozenset({8, 13}),
            expires_at=expires_at,
        )
        assert result == wanted
        assert len(requests) == 1
        assert set(requests[0].url.params) == {"page", "page_size"}
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_exact_ids_cross_unrelated_pages_and_return_verified_missing_subset():
    pages = []

    def handler(request):
        page = request.url.params["page"]
        pages.append(page)
        if page == "1":
            return httpx.Response(200, json={
                "results": [{"id": 1, "epg_source": 99, "tvg_id": "other"}],
                "next": "page-2",
            })
        if page == "2":
            return httpx.Response(200, json={
                "results": [{"id": 8, "epg_source": 46, "tvg_id": "eight"}],
                "next": None,
            })
        raise AssertionError("an ended catalogue must not request another page")

    client = _client(handler, source_count=2)
    try:
        result = await client.get_epg_data(
            max_results=2,
            ids=frozenset({8, 13}),
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
        assert result == [{"id": 8, "epg_source": 46, "tvg_id": "eight"}]
        assert pages == ["1", "2"]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_exact_ids_read_824_links_in_one_paginated_scan():
    requested = frozenset(range(5001, 5825))
    unrelated = [
        {"id": value, "epg_source": 99, "tvg_id": f"other-{value}"}
        for value in range(1, 1001)
    ]
    selected = [
        {"id": value, "epg_source": 46, "tvg_id": f"selected-{value}"}
        for value in sorted(requested)
    ]
    pages = []

    def handler(request):
        page = request.url.params["page"]
        pages.append(page)
        if page == "1":
            return httpx.Response(200, json={"results": unrelated, "next": "page-2"})
        return httpx.Response(200, json={"results": selected, "next": None})

    client = _client(handler, source_count=len(unrelated) + len(selected))
    try:
        result = await client.get_epg_data(
            max_results=len(requested),
            ids=requested,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
        assert {row["id"] for row in result} == requested
        assert pages == ["1", "2"]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("rows,reason", [
    ([
        {"id": 8, "epg_source": 46, "tvg_id": "eight"},
        {"id": 8, "epg_source": 46, "tvg_id": "duplicate"},
    ], "duplicate requested ID"),
    ([{"id": 8, "epg_source": True, "tvg_id": "eight"}], "malformed requested row"),
    ([{"id": 8, "epg_source": 46, "tvg_id": ""}], "malformed requested row"),
])
async def test_exact_ids_reject_duplicate_and_malformed_requested_rows(rows, reason):
    import json

    stream = _TrackingStream(json.dumps(rows).encode(), 5)
    client = _client(lambda request: httpx.Response(200, stream=stream), source_count=len(rows))
    try:
        with pytest.raises(ValueError, match=reason):
            await client.get_epg_data(
                max_results=1,
                ids=frozenset({8}),
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
            )
        assert stream.closed
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs", [
    {"ids": frozenset(), "max_results": 0},
    {"ids": frozenset({True}), "max_results": 1},
    {"ids": frozenset({0}), "max_results": 1},
    {"ids": frozenset({8}), "max_results": 2},
    {"ids": frozenset({8}), "max_results": 1, "search": "eight"},
    {"ids": frozenset({8}), "max_results": 1, "epg_source": 46},
    {"ids": frozenset({8}), "max_results": 1, "expires_at": datetime.now()},
])
async def test_exact_ids_validate_the_internal_call_contract(kwargs):
    client = _client(lambda request: httpx.Response(200, json=[]), source_count=0)
    kwargs.setdefault("expires_at", datetime.now(timezone.utc) + timedelta(seconds=30))
    try:
        with pytest.raises(ValueError):
            await client.get_epg_data(**kwargs)
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("finite", [False, True])
async def test_exact_ids_pass_one_expiry_to_every_page(finite):
    pages = []
    expiry = datetime.now(timezone.utc) + timedelta(seconds=30) if finite else None

    def handler(request):
        page = request.url.params["page"]
        if page == "1":
            return httpx.Response(200, json={
                "results": [{"id": 1, "epg_source": 99, "tvg_id": "other"}],
                "next": "page-2",
            })
        return httpx.Response(200, json={
            "results": [{"id": 8, "epg_source": 46, "tvg_id": "eight"}],
            "next": None,
        })

    client = _client(handler, source_count=2)
    original = client._get_json_bounded

    async def bounded(*args, **kwargs):
        pages.append(kwargs["expires_at"])
        return await original(*args, **kwargs)

    try:
        with patch.object(client, "_get_json_bounded", side_effect=bounded):
            result = await client.get_epg_data(
                max_results=1,
                ids=frozenset({8}),
                expires_at=expiry,
            )
        assert result == [{"id": 8, "epg_source": 46, "tvg_id": "eight"}]
        assert pages == [expiry, expiry]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_current_programmes_posts_exact_ids_and_keeps_grid_get_behavior():
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path == "/api/epg/grid/":
            return httpx.Response(200, json={"data": [{"title": "Visible"}]})
        assert request.url.path == "/api/epg/current-programs/"
        return httpx.Response(200, json=[{
            "epg_data_id": 8,
            "tvg_id": "hidden-eight",
            "title": "Hidden Event",
            "start_time": "2026-10-04T23:00:00+00:00",
            "end_time": "2026-10-05T01:00:00+00:00",
        }])

    client = _client(handler)
    expiry = datetime.now(timezone.utc) + timedelta(seconds=30)
    try:
        rows = await client.get_epg_programmes(frozenset({13, 8}), expires_at=expiry)
        assert rows[0]["epg_data_id"] == 8
        assert rows[0]["start_time"] == "2026-10-04T23:00:00+00:00"
        assert await client.get_epg_grid() == [{"title": "Visible"}]
        programme_request, grid_request = requests
        assert programme_request.method == "POST"
        assert dict(programme_request.url.params) == {}
        assert json.loads(programme_request.content) == {"epg_data_ids": [8, 13]}
        assert programme_request.headers["X-API-Key"] == "k"
        assert programme_request.headers["Accept-Encoding"] == "identity"
        assert grid_request.method == "GET"
        assert grid_request.url.path == "/api/epg/grid/"
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_current_programmes_refreshes_jwt_once_after_401():
    authorizations = []

    def handler(request):
        authorizations.append(request.headers["Authorization"])
        if len(authorizations) == 1:
            return httpx.Response(401)
        return httpx.Response(200, json=[])

    settings = DispatcharrSettings(
        url="http://dispatcharr", auth_method="password", username="u", password="p",
    )
    with patch("log_utils.register_sensitive_values_from_object"):
        client = DispatcharrClient(settings)
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client.access_token = "old"

    async def refresh():
        client.access_token = "new"

    try:
        with patch.object(client, "_ensure_authenticated", new=AsyncMock()), \
             patch.object(client, "_refresh_access_token", side_effect=refresh) as refreshed:
            assert await client.get_epg_programmes(
                frozenset({8}),
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
            ) == []
        assert authorizations == ["Bearer old", "Bearer new"]
        refreshed.assert_awaited_once()
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("epg_ids", [
    set(),
    frozenset(),
    frozenset({True}),
    frozenset({0}),
    frozenset(range(1, 52)),
])
async def test_current_programmes_rejects_invalid_ids_without_a_request(epg_ids):
    requests = []
    client = _client(lambda request: requests.append(request) or httpx.Response(200, json=[]))
    try:
        with pytest.raises(ValueError):
            await client.get_epg_programmes(
                epg_ids,
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
            )
        assert requests == []
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_current_programmes_rejects_invalid_or_expired_expiry_before_authentication():
    client = _client(lambda request: httpx.Response(200, json=[]))
    ensure = AsyncMock(side_effect=AssertionError("authentication must not run"))
    try:
        with patch.object(client, "_ensure_authenticated", new=ensure):
            with pytest.raises(ValueError):
                await client.get_epg_programmes(
                    frozenset({8}), expires_at=datetime.now(),
                )
            with pytest.raises(TimeoutError):
                await client.get_epg_programmes(
                    frozenset({8}),
                    expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
                )
        ensure.assert_not_awaited()
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("exact", [False, True])
async def test_current_and_exact_reads_have_no_implicit_deadline(monkeypatch, exact):
    deadlines = []
    timeout_at = asyncio.timeout_at

    def record(deadline):
        deadlines.append(deadline)
        return timeout_at(deadline)

    monkeypatch.setattr(asyncio, "timeout_at", record)
    row = ({"id": 8, "epg_source": 46, "tvg_id": "eight"} if exact
           else {"epg_data_id": 8, "title": "Programme"})
    client = _client(lambda request: httpx.Response(200, json=[row]), source_count=1)
    try:
        if exact:
            result = await client.get_epg_data(max_results=1, ids=frozenset({8}), expires_at=None)
        else:
            result = await client.get_epg_programmes(frozenset({8}), expires_at=None)
        assert result == [row]
        assert deadlines == [None]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_nonexact_bounded_read_keeps_explicit_deadline(monkeypatch):
    deadlines = []
    timeout_at = asyncio.timeout_at

    def record(deadline):
        deadlines.append(deadline)
        return timeout_at(deadline)

    monkeypatch.setattr(asyncio, "timeout_at", record)
    client = _client(lambda request: httpx.Response(200, json=[]))
    before = asyncio.get_running_loop().time()
    try:
        assert await client.get_epg_data(max_results=1) == []
        assert len(deadlines) == 1
        assert before + 120 <= deadlines[0] <= asyncio.get_running_loop().time() + 120
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [
    {"results": []},
    [{"epg_data_id": 9}],
    [{"epg_data_id": True}],
    [{"epg_data_id": 8}, {"epg_data_id": 8}],
    [{"epg_data_id": 8}, {"epg_data_id": 9}],
    ["row"],
])
async def test_current_programmes_rejects_untrusted_response_shapes(body):
    client = _client(lambda request: httpx.Response(200, json=body))
    try:
        with pytest.raises(ValueError):
            await client.get_epg_programmes(
                frozenset({8}),
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
            )
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_current_programmes_omits_parsing_rows_and_keeps_native_missing_fields():
    rows = [
        {"epg_data_id": 8, "parsing": True},
        {"epg_data_id": 13, "title": "Missing times"},
    ]
    client = _client(lambda request: httpx.Response(200, json=rows))
    try:
        assert await client.get_epg_programmes(
            frozenset({8, 13}),
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        ) == [{"epg_data_id": 13, "title": "Missing times"}]
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [
    b"[",
    b"[" + (b" " * (1024 * 1024)) + b"]",
])
async def test_current_programmes_rejects_malformed_and_oversized_bodies(content):
    stream = _TrackingStream(content, 4096)
    client = _client(lambda request: httpx.Response(200, stream=stream))
    try:
        with pytest.raises((ValueError, json.JSONDecodeError)):
            await client.get_epg_programmes(
                frozenset({8}),
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
            )
        assert stream.closed
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_current_programmes_rejects_encoded_body_without_consuming_it():
    stream = _TrackingStream(b"[]")
    client = _client(lambda request: httpx.Response(
        200,
        headers={"Content-Encoding": "gzip"},
        stream=stream,
    ))
    try:
        with pytest.raises(ValueError, match="unexpected Content-Encoding"):
            await client.get_epg_programmes(
                frozenset({8}),
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
            )
        assert stream.closed and not stream.iterated
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 404, 500])
async def test_current_programmes_http_errors_close_without_following_a_url(status):
    stream = _TrackingStream(b'{"next":"http://untrusted.invalid/"}')
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, stream=stream)

    client = _client(handler)
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_epg_programmes(
                frozenset({8}),
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
            )
        assert len(requests) == 1
        assert stream.closed and not stream.iterated
    finally:
        await client._client.aclose()


@pytest.mark.asyncio
async def test_current_programmes_cancellation_and_expiry_close_the_response():
    started = asyncio.Event()

    class Pending(_TrackingStream):
        async def __aiter__(self):
            started.set()
            yield b"["
            await asyncio.Event().wait()

    first_stream = Pending(b"")
    client = _client(lambda request: httpx.Response(200, stream=first_stream))
    try:
        task = asyncio.create_task(client.get_epg_programmes(
            frozenset({8}),
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        ))
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert first_stream.closed
    finally:
        await client._client.aclose()

    second_stream = Pending(b"")
    client = _client(lambda request: httpx.Response(200, stream=second_stream))
    try:
        with pytest.raises(TimeoutError):
            await client.get_epg_programmes(
                frozenset({8}),
                expires_at=datetime.now(timezone.utc) + timedelta(milliseconds=50),
            )
        assert second_stream.closed
    finally:
        await client._client.aclose()
