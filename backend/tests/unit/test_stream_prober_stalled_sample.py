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
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from stream_prober import StreamProber

STREAM_URL = "http://example.com/907"
SAMPLE_SECONDS = 20
# One read exactly, because the prober re-chunks anything larger and each
# chunk it hands back is another turn of the loop and another clock reading.
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


def client_serving(body: httpx.AsyncByteStream) -> type[httpx.AsyncClient]:
    """A client class that answers 200 and then hands over ``body``.

    Built as a subclass because the prober constructs its own client, so a
    transport can only be reached by standing in for the class itself.
    """

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=body)

    class HeadersThenBody(httpx.AsyncClient):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(transport=httpx.MockTransport(respond), **kwargs)

    return HeadersThenBody


def client_that_never_answers() -> type[httpx.AsyncClient]:
    """A client class whose request times out before any header comes back."""

    def respond(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    class NoHeaders(httpx.AsyncClient):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(transport=httpx.MockTransport(respond), **kwargs)

    return NoHeaders


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
    async def test_a_stream_that_froze_reports_only_what_it_sent(self):
        """A burst and then silence is as dead as silence throughout. The rate
        is what arrived spread over the window that actually ran, so a stream
        that stopped a second in lands under any floor worth setting instead of
        reading as the half-megabit that first second alone would suggest.
        """
        prober = create_prober()
        body = OneChunkThenSilence(ONE_CHUNK)

        with patch("stream_prober.httpx.AsyncClient", client_serving(body)), \
                patch("stream_prober.time", FrozenClock([0.0, 1.0, 25.0])):
            measured = await prober._measure_stream_bitrate(STREAM_URL)

        assert measured == int(len(ONE_CHUNK) * 8 / 25.0)


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
                patch("stream_prober.time", FrozenClock([0.0, 21.0])):
            measured = await prober._measure_stream_bitrate(STREAM_URL)

        assert measured == int(len(ONE_CHUNK) * 8 / 21.0)


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
