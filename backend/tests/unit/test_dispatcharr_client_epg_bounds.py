"""Bounded EPG-data response handling for migration callers."""

import asyncio
import threading
import time
from unittest.mock import patch

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
