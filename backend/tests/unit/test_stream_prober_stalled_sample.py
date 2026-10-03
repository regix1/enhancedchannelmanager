"""A source that answers and then sends nothing has to measure as nothing.

Channel 907 carried an event the guide showed as on air, and playing it moved
no scrubber and drew no picture. The sampler read zero bytes, hit the read
timeout, and returned None. None means "no sample was taken", which the event
health check reads as no verdict, and no verdict reads as working, so the one
failure the throughput floor exists to catch was the one shape of it that
could never reach the floor.

The distinction the fix rests on already existed: the response headers either
arrived or they did not. Headers and then silence is a stream with nothing to
send. No headers at all is a stream that could not be reached, and that stays
unmeasurable.
"""
import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

import stream_prober
from security.stream_outbound import _MAX_HLS_MANIFEST_BYTES
from stream_prober import StreamProber

STREAM_URL = "http://example.com/907"
SAMPLE_SECONDS = 20
ONE_CHUNK = b"\x00" * 65536


class FrozenClock:
    """``time`` for the prober alone, reading whatever the test hands it.

    Patched in place of the module rather than over ``time.time`` itself, so
    that :mod:`logging` keeps its own real clock and the readings here are
    consumed only by the code under test. The last reading stands for every
    call after it, so a test states the moments it cares about and not the
    number of times the prober happens to look.
    """

    def __init__(self, readings: list[float]) -> None:
        self._readings = readings

    def time(self) -> float:
        if len(self._readings) > 1:
            return self._readings.pop(0)
        return self._readings[0]


class DeadlineClock(datetime):
    current = datetime(2026, 1, 1, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls.current.replace(tzinfo=None)
        return cls.current.astimezone(tz)


class NothingArrives(httpx.AsyncByteStream):
    """A body whose read times out with no chunk ever delivered."""

    def __aiter__(self) -> "NothingArrives":
        return self

    async def __anext__(self) -> bytes:
        raise httpx.ReadTimeout("timed out")


class OneChunkThenSilence(httpx.AsyncByteStream):
    """A body that delivers once and then freezes for good."""

    def __init__(self, chunk: bytes) -> None:
        self._chunk = chunk
        self._sent = False

    def __aiter__(self) -> "OneChunkThenSilence":
        return self

    async def __anext__(self) -> bytes:
        if self._sent:
            raise httpx.ReadTimeout("timed out")
        self._sent = True
        return self._chunk


class OneChunkThenClose(httpx.AsyncByteStream):
    """A body that delivers once and then ends the response cleanly."""

    def __init__(self, chunk: bytes) -> None:
        self._chunk = chunk
        self._sent = False

    def __aiter__(self) -> "OneChunkThenClose":
        return self

    async def __anext__(self) -> bytes:
        if self._sent:
            raise StopAsyncIteration
        self._sent = True
        return self._chunk


class KeepsDelivering(httpx.AsyncByteStream):
    """A body that never stops, the way a live feed behaves."""

    def __init__(self, chunk: bytes) -> None:
        self._chunk = chunk

    def __aiter__(self) -> "KeepsDelivering":
        return self

    async def __anext__(self) -> bytes:
        return self._chunk


class ObservedBody(httpx.AsyncByteStream):
    def __init__(
        self,
        chunks: list[bytes],
        *,
        pause: float = 0.0,
        wait: asyncio.Event | None = None,
        failure: Exception | None = None,
    ) -> None:
        self._chunks = chunks
        self._pause = pause
        self._wait = wait
        self._failure = failure
        self.chunks_read = 0
        self.closed = False
        self.started = asyncio.Event()

    async def __aiter__(self):
        self.started.set()
        for chunk in self._chunks:
            if self._pause:
                await asyncio.sleep(self._pause)
            self.chunks_read += 1
            yield chunk
        if self._wait is not None:
            await self._wait.wait()
        if self._failure is not None:
            raise self._failure

    async def aclose(self) -> None:
        self.closed = True


def client_serving(body: httpx.AsyncByteStream) -> type[httpx.AsyncClient]:
    """A client class that answers 200 and then hands over ``body``.

    Built as a subclass because the prober constructs its own client, so a
    transport can only be reached by standing in for the class itself.
    """

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=body)

    class HeadersThenBody(httpx.AsyncClient):
        def __init__(self, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(respond)
            super().__init__(**kwargs)

    return HeadersThenBody


def client_that_never_answers() -> type[httpx.AsyncClient]:
    """A client class whose request times out before any header comes back."""

    def respond(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    class NoHeaders(httpx.AsyncClient):
        def __init__(self, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(respond)
            super().__init__(**kwargs)

    return NoHeaders


def _hls_client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> type[httpx.AsyncClient]:
    class HlsClient(httpx.AsyncClient):
        def __init__(self, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(handler)
            super().__init__(**kwargs)

    return HlsClient


def _response(
    request: httpx.Request,
    body: bytes | httpx.AsyncByteStream,
    *,
    content_type: str = "application/vnd.apple.mpegurl",
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    response_headers = {"Content-Type": content_type, **(headers or {})}
    if isinstance(body, httpx.AsyncByteStream):
        return httpx.Response(
            200, request=request, headers=response_headers, stream=body,
        )
    return httpx.Response(
        200, request=request, headers=response_headers, content=body,
    )


def create_prober(sample_seconds: int = SAMPLE_SECONDS) -> StreamProber:
    """A prober sampling for ``sample_seconds``, with nothing else stubbed."""
    return StreamProber(
        client=MagicMock(),
        probe_timeout=30,
        bitrate_sample_duration=sample_seconds,
        black_screen_detection_enabled=False,
    )


class TestASourceThatSendsNothingMeasuresZero:
    @pytest.mark.asyncio
    async def test_stall_after_headers_reports_zero_rather_than_no_sample(self):
        """Channel 907's failure, at the one place that can still see it."""
        prober = create_prober()

        with patch("stream_prober.httpx.AsyncClient", client_serving(NothingArrives())), \
                patch("stream_prober.time", FrozenClock([0.0, 25.0])):
            measured = await prober._measure_stream_bitrate(STREAM_URL)

        assert measured == 0

    @pytest.mark.asyncio
    async def test_a_stream_that_froze_measures_zero(self):
        """A burst and then silence did not sustain transport."""
        prober = create_prober()
        body = OneChunkThenSilence(ONE_CHUNK)

        with patch("stream_prober.httpx.AsyncClient", client_serving(body)), \
                patch("stream_prober.time", FrozenClock([0.0, 1.0, 25.0])):
            measured = await prober._measure_stream_bitrate(STREAM_URL)

        assert measured == 0


class TestASourceThatHangsUpSustainedNothing:
    @pytest.mark.asyncio
    async def test_a_burst_then_a_close_measures_zero(self):
        """Channel 907's actual failure, and the costly one.

        The provider hands over a burst and ends the response. Dividing those
        bytes by the fraction of a second the connection lasted reported 60
        Mbps on a stream carrying no event, which clears any floor and reads as
        the healthiest stream in the group. What the division measured was the
        connection's lifetime, not the feed: widening the window from 10s to
        20s moved the same stream to 7.4 Mbps.
        """
        prober = create_prober()
        body = OneChunkThenClose(ONE_CHUNK)

        with patch("stream_prober.httpx.AsyncClient", client_serving(body)), \
                patch("stream_prober.time", FrozenClock([0.0, 1.0, 2.65])):
            measured = await prober._measure_stream_bitrate(STREAM_URL)

        assert measured == 0

    @pytest.mark.asyncio
    async def test_a_feed_that_fills_the_window_keeps_its_rate(self):
        """The other side of the same branch, and the one that must not move.

        A source still delivering when the window closes is measured exactly as
        before. Without this, the check above would be free to call every
        stream dead and nothing here would notice.
        """
        prober = create_prober()
        body = KeepsDelivering(ONE_CHUNK)

        with patch("stream_prober.httpx.AsyncClient", client_serving(body)), \
                patch(
                    "stream_prober.time",
                    FrozenClock([float(value) for value in range(22)] + [21.0]),
                ):
            measured = await prober._measure_stream_bitrate(STREAM_URL)

        assert measured == int(len(ONE_CHUNK) * 21 * 8 / 20)

    @pytest.mark.asyncio
    async def test_startup_delay_is_not_a_delivery_gap(self):
        prober = create_prober(sample_seconds=1)
        body = KeepsDelivering(ONE_CHUNK)

        with patch("stream_prober.httpx.AsyncClient", client_serving(body)), \
                patch(
                    "stream_prober.time",
                    FrozenClock([0.0, 0.75, 1.25, 1.75, 1.75]),
                ):
            measured = await prober._measure_stream_bitrate(STREAM_URL)

        assert measured == len(ONE_CHUNK) * 3 * 8

    @pytest.mark.asyncio
    async def test_two_bursts_separated_by_the_window_measure_zero(self):
        prober = create_prober()
        body = KeepsDelivering(ONE_CHUNK)

        with patch("stream_prober.httpx.AsyncClient", client_serving(body)), \
                patch("stream_prober.time", FrozenClock([0.0, 1.0, 21.0])):
            measured = await prober._measure_stream_bitrate(STREAM_URL)

        assert measured == 0


class TestUnreachableIsStillUnmeasurable:
    @pytest.mark.asyncio
    async def test_a_timeout_before_the_headers_stays_no_sample(self):
        """Nothing answered, so there is nothing to report a rate about. Calling
        that zero would fail a stream on a provider hiccup or a saturated line,
        which is what the None in this branch has always been protecting.
        """
        prober = create_prober()

        with patch("stream_prober.httpx.AsyncClient", client_that_never_answers()), \
                patch("stream_prober.time", FrozenClock([0.0, 25.0])):
            measured = await prober._measure_stream_bitrate(STREAM_URL)

        assert measured is None

    @pytest.mark.asyncio
    async def test_a_window_cut_short_stays_no_sample(self):
        """The sample duration is live: ``PUT /api/settings`` writes it straight
        onto the running prober, so a probe in flight can time out against the
        window it was started with and be judged against a longer one. Whatever
        it read covers less time than the operator now asks for, so it is not a
        measurement of it.
        """
        prober = create_prober(sample_seconds=30)

        with patch("stream_prober.httpx.AsyncClient", client_serving(NothingArrives())), \
                patch("stream_prober.time", FrozenClock([0.0, 15.0])):
            measured = await prober._measure_stream_bitrate(STREAM_URL)

        assert measured is None


class TestHlsManifestTraversal:
    @pytest.mark.asyncio
    async def test_direct_media_playlist_measures_complete_segment_span(self):
        root = "https://media.example/live/index.m3u8"
        requested: list[str] = []
        media = (
            b"#EXTM3U\n#EXTINF:1.0,\none.ts\n"
            b"#EXTINF:1.0,\ntwo.ts\n#EXT-X-ENDLIST\n"
        )

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            if url == root:
                return _response(request, media)
            if url == "https://media.example/live/one.ts":
                return _response(request, b"a" * 100, content_type="video/mp2t")
            if url == "https://media.example/live/two.ts":
                return _response(request, b"b" * 200, content_type="video/mp2t")
            return httpx.Response(404, request=request)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured == 1200
        assert requested == [
            root,
            "https://media.example/live/one.ts",
            "https://media.example/live/two.ts",
        ]

    @pytest.mark.asyncio
    async def test_master_uses_only_first_normal_variant(self):
        root = "https://media.example/master.m3u8"
        requested: list[str] = []
        master = (
            b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000\nfirst/index.m3u8\n"
            b"#EXT-X-STREAM-INF:BANDWIDTH=1600000\nsecond/index.m3u8\n"
        )
        media = b"#EXTM3U\n#EXTINF:2.0,\nsegment.ts\n#EXT-X-ENDLIST\n"

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            if url == root:
                return _response(request, master)
            if url == "https://media.example/first/index.m3u8":
                return _response(request, media)
            if url == "https://media.example/first/segment.ts":
                return _response(request, b"x" * 250, content_type="video/mp2t")
            return httpx.Response(404, request=request)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured == 1000
        assert requested == [
            root,
            "https://media.example/first/index.m3u8",
            "https://media.example/first/segment.ts",
        ]
        assert all("second" not in url for url in requested)

    @pytest.mark.asyncio
    async def test_nested_master_reaches_media_playlist(self):
        root = "https://media.example/root.m3u8"
        first = "https://media.example/level/first.m3u8"
        media_url = "https://media.example/level/media/index.m3u8"
        segment = "https://media.example/level/media/segment.ts"
        requested: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            if url == root:
                return _response(
                    request,
                    b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nlevel/first.m3u8\n",
                )
            if url == first:
                return _response(
                    request,
                    b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nmedia/index.m3u8\n",
                )
            if url == media_url:
                return _response(
                    request,
                    b"#EXTM3U\n#EXTINF:2.0,\nsegment.ts\n#EXT-X-ENDLIST\n",
                )
            if url == segment:
                return _response(request, b"z" * 400, content_type="video/mp2t")
            return httpx.Response(404, request=request)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured == 1600
        assert requested == [root, first, media_url, segment]

    @pytest.mark.asyncio
    async def test_redirected_logical_urls_resolve_queries_bom_and_segments(self):
        start = "https://origin.example/start?channel=7"
        root = "https://cdn.example/root/master.m3u8?token=a"
        child = "https://cdn.example/root/variant/first.m3u8?token=b"
        media_url = "https://cdn.example/root/media/index.m3u8?token=c"
        segment = "https://cdn.example/root/media/segment.ts?part=1"
        requested: list[str] = []
        root_body = ObservedBody([
            b"\xef\xbb\xbf#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\n"
            b"variant/first.m3u8?token=b\n"
        ])
        media_body = ObservedBody([
            b"#EXTM3U\n#EXTINF:2.0,\nsegment.ts?part=1\n#EXT-X-ENDLIST\n"
        ])
        segment_body = ObservedBody([b"q" * 300])

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            if url == start:
                return httpx.Response(302, request=request, headers={"Location": root})
            if url == root:
                return _response(request, root_body)
            if url == child:
                assert root_body.closed is True
                return httpx.Response(
                    302,
                    request=request,
                    headers={"Location": "../media/index.m3u8?token=c"},
                )
            if url == media_url:
                return _response(request, media_body)
            if url == segment:
                assert media_body.closed is True
                return _response(request, segment_body, content_type="video/mp2t")
            return httpx.Response(404, request=request)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(start)

        assert measured == 1200
        assert requested == [start, root, child, media_url, segment]
        assert root_body.closed is True
        assert media_body.closed is True
        assert segment_body.closed is True


class TestHlsManifestBounds:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("oversize_child", [False, True])
    async def test_declared_oversize_reads_no_manifest_chunks(self, oversize_child):
        root = "https://media.example/root.m3u8"
        child = "https://media.example/child.m3u8"
        body = ObservedBody([b"must-not-be-read"])
        requested: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            if oversize_child and url == root:
                return _response(
                    request,
                    b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nchild.m3u8\n",
                )
            if url == (child if oversize_child else root):
                return _response(
                    request,
                    body,
                    headers={"Content-Length": str(_MAX_HLS_MANIFEST_BYTES + 1)},
                )
            return httpx.Response(404, request=request)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured is None
        assert body.chunks_read == 0
        assert body.closed is True
        assert requested == ([root, child] if oversize_child else [root])

    @pytest.mark.asyncio
    @pytest.mark.parametrize("oversize_child", [False, True])
    async def test_streamed_overflow_stops_before_manifest_tail(self, oversize_child):
        root = "https://media.example/root.m3u8"
        child = "https://media.example/child.m3u8"
        body = ObservedBody(
            [b"a" * (_MAX_HLS_MANIFEST_BYTES + 1), b"unread-tail"]
        )
        requested: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            if oversize_child and url == root:
                return _response(
                    request,
                    b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nchild.m3u8\n",
                )
            if url == (child if oversize_child else root):
                return _response(request, body)
            return httpx.Response(404, request=request)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured is None
        assert body.chunks_read == 1
        assert body.closed is True
        assert requested == ([root, child] if oversize_child else [root])

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exact_child", [False, True])
    async def test_exact_limit_manifest_reaches_media_measurement(self, exact_child):
        root = "https://media.example/root.m3u8"
        child = "https://media.example/child.m3u8"
        segment = "https://media.example/segment.ts"
        media = b"#EXTM3U\n#EXTINF:2.0,\nsegment.ts\n#EXT-X-ENDLIST\n#"
        exact = media + b"x" * (_MAX_HLS_MANIFEST_BYTES - len(media))
        requested: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            if exact_child and url == root:
                return _response(
                    request,
                    b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nchild.m3u8\n",
                )
            if url == (child if exact_child else root):
                return _response(request, exact)
            if url == segment:
                return _response(request, b"s" * 250, content_type="video/mp2t")
            return httpx.Response(404, request=request)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured == 1000
        assert requested == (
            [root, child, segment] if exact_child else [root, segment]
        )


class TestHlsUnknownResults:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "playlist",
        [
            b"#EXTINF:2.0,\nsegment.ts\n",
            b"#EXTM3U\nsegment.ts\n",
            b"#EXTM3U\n#EXTINF:2.0,\n#EXTINF:2.0,\nsegment.ts\n",
            b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\n"
            b"#EXT-X-STREAM-INF:BANDWIDTH=2\nchild.m3u8\n",
            b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nchild.m3u8\n"
            b"#EXTINF:2.0,\nsegment.ts\n",
            b"#EXTM3U\n#EXTINF:invalid,\nsegment.ts\n",
            b"#EXTM3U\n#EXTINF:nan,\nsegment.ts\n",
            b"#EXTM3U\n#EXTINF:inf,\nsegment.ts\n",
            b"#EXTM3U\n#EXTINF:0,\nsegment.ts\n",
            b"#EXTM3U\n#EXTINF:-1,\nsegment.ts\n",
            b"#EXTM3U\n#EXTINF:2.0,\n#EXT-X-BYTERANGE:10@0\nsegment.ts\n",
            b"#EXTM3U\n#EXT-X-I-FRAME-STREAM-INF:URI=\"iframe.m3u8\"\n",
            b"#EXTM3U\n#EXT-X-PART:DURATION=0.5,URI=\"part.ts\"\n",
            b"#EXTM3U\n",
            b"\xff#EXTM3U\n",
        ],
    )
    async def test_malformed_or_unsupported_playlist_stays_unknown(self, playlist):
        root = "https://media.example/root.m3u8"
        requested: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            requested.append(str(request.url))
            return _response(request, playlist)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured is None
        assert requested == [root]

    @pytest.mark.asyncio
    async def test_manifest_cycle_stops_before_repeating_request(self):
        root = "https://media.example/root.m3u8"
        requested: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            requested.append(str(request.url))
            return _response(
                request,
                b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nroot.m3u8#again\n",
            )

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured is None
        assert requested == [root]

    @pytest.mark.asyncio
    async def test_redirect_to_ancestor_stops_before_reading_it_again(self):
        root = "https://media.example/root.m3u8"
        child = "https://media.example/child.m3u8"
        requested: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            if url == child:
                return httpx.Response(
                    302, request=request, headers={"Location": root},
                )
            return _response(
                request,
                b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nchild.m3u8\n",
            )

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured is None
        assert requested == [root, child, root]

    @pytest.mark.asyncio
    async def test_sixth_manifest_is_not_requested(self):
        root = "https://media.example/level-0.m3u8"
        requested: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            level = int(url.rsplit("-", 1)[1].removesuffix(".m3u8"))
            return _response(
                request,
                (
                    "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\n"
                    f"level-{level + 1}.m3u8\n"
                ).encode(),
            )

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured is None
        assert requested == [
            f"https://media.example/level-{level}.m3u8" for level in range(5)
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "case", ["short", "empty", "http", "truncated", "timeout"],
    )
    async def test_incomplete_media_stays_unknown(self, case):
        root = "https://media.example/root.m3u8"
        segment = "https://media.example/segment.ts"
        requested: list[str] = []
        truncated = ObservedBody(
            [b"#EXTM3U\n#EXTINF:2.0,\n"],
            failure=httpx.RemoteProtocolError("truncated"),
        )
        timed_out = ObservedBody(
            [b"#EXTM3U\n#EXTINF:2.0,\n"],
            failure=httpx.ReadTimeout("timed out"),
        )

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            if case == "http" and url == root:
                return httpx.Response(503, request=request)
            if case == "truncated" and url == root:
                return _response(request, truncated)
            if case == "timeout" and url == root:
                return _response(request, timed_out)
            if url == root:
                duration = 1.0 if case == "short" else 2.0
                return _response(
                    request,
                    f"#EXTM3U\n#EXTINF:{duration},\nsegment.ts\n".encode(),
                )
            if url == segment:
                body = b"" if case == "empty" else b"complete"
                return _response(request, body, content_type="video/mp2t")
            return httpx.Response(404, request=request)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(root)

        assert measured is None
        if case == "truncated":
            assert truncated.closed is True
        if case == "timeout":
            assert timed_out.closed is True
        assert requested == (
            [root, segment] if case in {"short", "empty"} else [root]
        )


class TestHlsLifetime:
    @pytest.mark.asyncio
    async def test_supplied_expiry_reaches_manifest_and_segment_requests_unchanged(self):
        root = "https://media.example/root.m3u8"
        child = "https://media.example/child.m3u8"
        segment = "https://media.example/segment.ts"
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=30)
        seen: list[datetime] = []
        probe_seconds = stream_prober._probe_seconds

        def remaining(value, cap):
            seen.append(value)
            return probe_seconds(value, cap)

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if url == root:
                return _response(
                    request,
                    b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nchild.m3u8\n",
                )
            if url == child:
                return _response(
                    request,
                    b"#EXTM3U\n#EXTINF:1.0,\nsegment.ts\n",
                )
            if url == segment:
                return _response(request, b"media", content_type="video/mp2t")
            return httpx.Response(404, request=request)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)), \
                patch("stream_prober._probe_seconds", side_effect=remaining):
            measured = await create_prober(sample_seconds=1)._measure_stream_bitrate(
                root, expires_at=expires_at,
            )

        assert measured == len(b"media") * 8
        assert seen
        assert all(value is expires_at for value in seen)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("expires_during", ["root", "child", "segment"])
    async def test_expiry_prevents_the_next_hls_request(self, expires_during):
        root = "https://media.example/root.m3u8"
        child = "https://media.example/child.m3u8"
        first_segment = "https://media.example/one.ts"
        DeadlineClock.current = datetime(2026, 1, 1, tzinfo=timezone.utc)
        expires_at = DeadlineClock.current + timedelta(seconds=30)
        expired = expires_at + timedelta(seconds=1)
        requested: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            requested.append(url)
            if url == root and expires_during == "root":
                DeadlineClock.current = expired
            if url == root:
                if expires_during == "segment":
                    return _response(
                        request,
                        b"#EXTM3U\n#EXTINF:1.0,\none.ts\n"
                        b"#EXTINF:1.0,\ntwo.ts\n",
                    )
                return _response(
                    request,
                    b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nchild.m3u8\n",
                )
            if url == child:
                if expires_during == "child":
                    DeadlineClock.current = expired
                return _response(
                    request,
                    b"#EXTM3U\n#EXTINF:1.0,\none.ts\n",
                )
            if url == first_segment:
                DeadlineClock.current = expired
                return _response(request, b"first", content_type="video/mp2t")
            return _response(request, b"second", content_type="video/mp2t")

        with patch("stream_prober.datetime", DeadlineClock), \
                patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await create_prober(sample_seconds=2)._measure_stream_bitrate(
                root, expires_at=expires_at,
            )

        assert measured is None
        expected = {
            "root": [root],
            "child": [root, child],
            "segment": [root, first_segment],
        }
        assert requested == expected[expires_during]

    @pytest.mark.asyncio
    async def test_outer_lifetime_stops_manifest_body_that_keeps_dripping(self):
        root = "https://media.example/root.m3u8"
        never = asyncio.Event()
        body = ObservedBody(
            [b"#"] * 100,
            pause=0.01,
            wait=never,
        )

        def respond(request: httpx.Request) -> httpx.Response:
            return _response(request, body)

        expires_at = datetime.now(timezone.utc) + timedelta(milliseconds=200)
        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            measured = await asyncio.wait_for(
                create_prober(sample_seconds=1)._measure_stream_bitrate(
                    root, expires_at=expires_at,
                ),
                timeout=0.75,
            )

        assert measured is None
        assert body.chunks_read > 0
        assert body.closed is True

    @pytest.mark.asyncio
    async def test_external_cancellation_closes_manifest_and_propagates(self):
        root = "https://media.example/root.m3u8"
        body = ObservedBody([], wait=asyncio.Event())

        def respond(request: httpx.Request) -> httpx.Response:
            return _response(request, body)

        with patch("stream_prober.httpx.AsyncClient", _hls_client(respond)):
            task = asyncio.create_task(
                create_prober(sample_seconds=1)._measure_stream_bitrate(root)
            )
            await asyncio.wait_for(body.started.wait(), timeout=1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        assert body.closed is True
