"""Tests for black screen detection feature in StreamProber."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from models import StreamStats
from stream_prober import StreamProber, smart_sort_streams


@pytest.fixture(autouse=True)
def _allow_synthetic_subprocess_destination():
    @asynccontextmanager
    async def allowed(url, **_kwargs):
        yield SimpleNamespace(argument=url, response=None, is_http_relay=False)

    with patch("stream_prober.validated_subprocess_input", allowed):
        yield


def create_prober(**kwargs) -> StreamProber:
    """Create a StreamProber with specified settings."""
    mock_client = MagicMock()
    defaults = {
        "probe_timeout": 30,
        "black_screen_detection_enabled": False,
        "black_screen_sample_duration": 5,
    }
    defaults.update(kwargs)
    return StreamProber(client=mock_client, **defaults)


def create_mock_stats(
    stream_id: int,
    probe_status: str = "success",
    resolution: str = "1920x1080",
    bitrate: int = 5000000,
    fps: str = "30",
    audio_channels: int = 2,
    is_black_screen: bool = False,
) -> StreamStats:
    """Create a mock StreamStats object."""
    stats = Mock(spec=StreamStats)
    stats.stream_id = stream_id
    stats.stream_name = f"Stream {stream_id}"
    stats.resolution = resolution
    stats.bitrate = bitrate
    stats.video_bitrate = None
    stats.fps = fps
    stats.audio_channels = audio_channels
    stats.probe_status = probe_status
    stats.is_black_screen = is_black_screen
    return stats


class TestDetectBlackScreen:
    """Tests for _detect_black_screen method (signalstats YAVG-based)."""

    def _make_mock_process(self, stderr_output, *, returncode=0):
        """Create a mock process that works with asyncio.wait_for."""
        mock_process = AsyncMock()

        async def mock_communicate():
            return (b"", stderr_output)

        mock_process.communicate = mock_communicate
        mock_process.kill = Mock()
        mock_process.wait = AsyncMock()
        mock_process.returncode = returncode
        return mock_process

    def _make_yavg_output(self, *values, span=5.0):
        """Build ffmpeg signalstats stderr output from YAVG values."""
        lines = []
        for index, value in enumerate(values):
            frame_time = span * index / max(1, len(values) - 1)
            lines.extend([
                f"frame:{index} pts:{index} pts_time:{frame_time:.3f}\n".encode(),
                f"[Parsed_metadata_1 @ 0x1234] lavfi.signalstats.YAVG={value}\n".encode(),
            ])
        return b"".join(lines)

    @pytest.mark.asyncio
    async def test_detects_dark_screen_below_threshold(self):
        """Returns True when average YAVG is below threshold (pure black = 16)."""
        prober = create_prober(black_screen_detection_enabled=True, black_screen_sample_duration=5)
        stderr_output = self._make_yavg_output(16.0, 16.0, 16.0, 16.0)
        mock_process = self._make_mock_process(stderr_output)

        with patch("stream_prober.asyncio.create_subprocess_exec", return_value=mock_process):
            result = await prober._detect_black_screen("http://example.com/stream")

        assert result is True

    @pytest.mark.asyncio
    async def test_detects_dark_slate_with_logo(self):
        """Returns True for dark slate with small logo (YAVG ~16.5)."""
        prober = create_prober(black_screen_detection_enabled=True, black_screen_sample_duration=5)
        stderr_output = self._make_yavg_output(16.5, 16.5, 16.6, 16.5)
        mock_process = self._make_mock_process(stderr_output)

        with patch("stream_prober.asyncio.create_subprocess_exec", return_value=mock_process):
            result = await prober._detect_black_screen("http://example.com/stream")

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_for_normal_content(self):
        """Returns False when stream has normal content (YAVG ~88)."""
        prober = create_prober(black_screen_detection_enabled=True, black_screen_sample_duration=5)
        stderr_output = self._make_yavg_output(87.5, 88.0, 87.8, 88.2)
        mock_process = self._make_mock_process(stderr_output)

        with patch("stream_prober.asyncio.create_subprocess_exec", return_value=mock_process):
            result = await prober._detect_black_screen("http://example.com/stream")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_for_dim_but_not_dark_content(self):
        """Returns False when brightness is low but above threshold (YAVG ~30)."""
        prober = create_prober(black_screen_detection_enabled=True, black_screen_sample_duration=5)
        stderr_output = self._make_yavg_output(28.0, 30.0, 32.0, 29.0)
        mock_process = self._make_mock_process(stderr_output)

        with patch("stream_prober.asyncio.create_subprocess_exec", return_value=mock_process):
            result = await prober._detect_black_screen("http://example.com/stream")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_none_on_timeout(self):
        """Timeout is indeterminate, not 'clean'. Callers must preserve prior state.

        Regression: pre-fix, timeout returned False and the scan task happily
        wrote is_black_screen=False into StreamStats, silently overwriting
        manual probe findings on every cold-start timeout.
        """
        prober = create_prober(black_screen_detection_enabled=True, black_screen_sample_duration=5)
        mock_process = AsyncMock()

        async def mock_communicate():
            raise asyncio.TimeoutError()

        mock_process.communicate = mock_communicate
        mock_process.kill = Mock()
        mock_process.wait = AsyncMock()

        with patch("stream_prober.asyncio.create_subprocess_exec", return_value=mock_process):
            result = await prober._detect_black_screen("http://example.com/stream")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_yavg_data(self):
        """No YAVG output means signalstats couldn't decode any frames;
        that's indeterminate, not clean."""
        prober = create_prober(black_screen_detection_enabled=True, black_screen_sample_duration=5)
        stderr_output = b"frame=  150 fps= 30 q=-0.0 Lsize=N/A time=00:00:05.00\n"
        mock_process = self._make_mock_process(stderr_output)

        with patch("stream_prober.asyncio.create_subprocess_exec", return_value=mock_process):
            result = await prober._detect_black_screen("http://example.com/stream")

        assert result is None

    @pytest.mark.asyncio
    async def test_borderline_at_threshold(self):
        """Returns False when YAVG is exactly at threshold."""
        prober = create_prober(black_screen_detection_enabled=True, black_screen_sample_duration=5)
        threshold = StreamProber.BLACK_SCREEN_YAVG_THRESHOLD
        stderr_output = self._make_yavg_output(threshold, threshold, threshold)
        mock_process = self._make_mock_process(stderr_output)

        with patch("stream_prober.asyncio.create_subprocess_exec", return_value=mock_process):
            result = await prober._detect_black_screen("http://example.com/stream")

        assert result is False  # < threshold, not <=

    @pytest.mark.asyncio
    async def test_dark_opening_then_normal_content_is_not_persistent(self):
        prober = create_prober(
            black_screen_detection_enabled=True,
            black_screen_sample_duration=10,
        )
        stderr_output = self._make_yavg_output(
            16.0, 16.0, 16.0, 16.0, 16.0, 16.0, 16.0, 16.0, 88.0, 88.0,
            span=10.0,
        )

        with patch(
            "stream_prober.asyncio.create_subprocess_exec",
            return_value=self._make_mock_process(stderr_output),
        ):
            result = await prober._detect_black_screen("http://example.com/stream")

        assert result is False

    @pytest.mark.asyncio
    async def test_truncated_frame_span_is_indeterminate(self):
        prober = create_prober(
            black_screen_detection_enabled=True,
            black_screen_sample_duration=10,
        )
        stderr_output = self._make_yavg_output(16.0, 16.0, span=2.0)

        with patch(
            "stream_prober.asyncio.create_subprocess_exec",
            return_value=self._make_mock_process(stderr_output),
        ):
            result = await prober._detect_black_screen("http://example.com/stream")

        assert result is None

    @pytest.mark.asyncio
    async def test_nonzero_decoder_exit_is_indeterminate(self):
        prober = create_prober(
            black_screen_detection_enabled=True,
            black_screen_sample_duration=5,
        )
        stderr_output = self._make_yavg_output(16.0, 16.0)

        with patch(
            "stream_prober.asyncio.create_subprocess_exec",
            return_value=self._make_mock_process(stderr_output, returncode=1),
        ):
            result = await prober._detect_black_screen("http://example.com/stream")

        assert result is None

    @pytest.mark.asyncio
    async def test_cancellation_kills_and_reaps_decoder(self):
        prober = create_prober(
            black_screen_detection_enabled=True,
            black_screen_sample_duration=5,
        )
        process = AsyncMock()
        process.returncode = None

        async def never_returns():
            await asyncio.Event().wait()

        process.communicate.side_effect = never_returns
        process.kill = Mock()
        process.wait = AsyncMock()

        with patch(
            "stream_prober.asyncio.create_subprocess_exec",
            return_value=process,
        ):
            task = asyncio.create_task(
                prober._detect_black_screen("http://example.com/stream")
            )
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        process.kill.assert_called_once_with()
        process.wait.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_cancellation_kills_and_reaps_ffprobe(self):
        prober = create_prober()
        process = AsyncMock()
        process.returncode = None

        async def never_returns():
            await asyncio.Event().wait()

        process.communicate.side_effect = never_returns
        process.kill = Mock()
        process.wait = AsyncMock()

        with patch(
            "stream_prober.asyncio.create_subprocess_exec",
            return_value=process,
        ):
            task = asyncio.create_task(
                prober._run_ffprobe("http://example.com/stream")
            )
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        process.kill.assert_called_once_with()
        process.wait.assert_awaited_once_with()


class TestSmartSortBlackScreen:
    """Tests for black screen deprioritization in smart sort."""

    def test_black_screen_streams_sort_to_bottom(self):
        """Streams with is_black_screen=True sort to bottom when deprioritize is enabled."""
        stats = {
            1: create_mock_stats(1, is_black_screen=True),
            2: create_mock_stats(2, is_black_screen=False),
        }
        result = smart_sort_streams(
            [1, 2],
            stats,
            deprioritize_failed_streams=True,
            stream_sort_priority=["resolution"],
            stream_sort_enabled={"resolution": True},
        )
        assert result == [2, 1]

    def test_black_screen_not_deprioritized_when_setting_off(self):
        """Black screen streams are not deprioritized when deprioritize_failed_streams is False."""
        stats = {
            1: create_mock_stats(1, is_black_screen=True, resolution="1920x1080"),
            2: create_mock_stats(2, is_black_screen=False, resolution="1280x720"),
        }
        result = smart_sort_streams(
            [1, 2],
            stats,
            deprioritize_failed_streams=False,
            stream_sort_priority=["resolution"],
            stream_sort_enabled={"resolution": True},
        )
        # Stream 1 has higher resolution, should be first even though black
        assert result == [1, 2]


class TestSaveProbeResultBlackScreen:
    """Tests for is_black_screen persistence in _save_probe_result."""

    def test_stores_black_screen_on_success(self):
        """is_black_screen is stored when probe succeeds and detection is enabled."""
        prober = create_prober(black_screen_detection_enabled=True)
        mock_stats = Mock(spec=StreamStats)
        mock_stats.consecutive_failures = 0
        mock_stats.stream_name = "Test"
        mock_stats.black_screen_checked_at = None

        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_stats
        mock_stats.to_dict.return_value = {"is_black_screen": True}

        with patch("stream_prober.get_session", return_value=mock_session):
            prober._save_probe_result(1, "Test", {}, "success", None, is_black_screen=True)

        assert mock_stats.is_black_screen is True
        assert mock_stats.black_screen_checked_at == mock_stats.last_probed

    def test_clears_black_screen_on_failure(self):
        """is_black_screen is cleared when probe fails."""
        prober = create_prober()
        mock_stats = Mock(spec=StreamStats)
        mock_stats.consecutive_failures = 0
        mock_stats.stream_name = "Test"
        mock_stats.is_black_screen = True

        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_stats
        mock_stats.to_dict.return_value = {"is_black_screen": False}

        with patch("stream_prober.get_session", return_value=mock_session):
            prober._save_probe_result(1, "Test", None, "failed", "Connection refused")

        assert mock_stats.is_black_screen is False

    def test_stores_false_when_not_black(self):
        """is_black_screen=False is stored when probe succeeds and detection is enabled."""
        prober = create_prober(black_screen_detection_enabled=True)
        mock_stats = Mock(spec=StreamStats)
        mock_stats.consecutive_failures = 0
        mock_stats.stream_name = "Test"
        mock_stats.black_screen_checked_at = None

        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_stats
        mock_stats.to_dict.return_value = {"is_black_screen": False}

        with patch("stream_prober.get_session", return_value=mock_session):
            prober._save_probe_result(1, "Test", {}, "success", None, is_black_screen=False)

        assert mock_stats.is_black_screen is False
        assert mock_stats.black_screen_checked_at == mock_stats.last_probed

    def test_stores_disabled_probe_result(self):
        """An event-requested result is saved even when the global flag is off."""
        prober = create_prober(black_screen_detection_enabled=False)
        mock_stats = Mock(spec=StreamStats)
        mock_stats.consecutive_failures = 0
        mock_stats.stream_name = "Test"
        mock_stats.is_black_screen = True  # Set by prior black screen scan
        mock_stats.black_screen_checked_at = None

        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_stats
        mock_stats.to_dict.return_value = {"is_black_screen": False}

        with patch("stream_prober.get_session", return_value=mock_session):
            prober._save_probe_result(1, "Test", {}, "success", None, is_black_screen=False)

        assert mock_stats.is_black_screen is False
        assert mock_stats.black_screen_checked_at == mock_stats.last_probed

    def test_preserves_black_screen_when_detection_indeterminate(self):
        """is_black_screen is NOT overwritten when detection returns None.

        Regression: pre-fix, _detect_black_screen's timeout branch returned
        False, and that False got written into StreamStats on the success
        path — silently erasing findings from a prior manual probe or scan.
        The fix routes timeouts through is_black_screen=None, which this
        test pins.
        """
        prober = create_prober(black_screen_detection_enabled=True)
        mock_stats = Mock(spec=StreamStats)
        mock_stats.consecutive_failures = 0
        mock_stats.stream_name = "Test"
        mock_stats.is_black_screen = True  # Earlier detection flagged this stream

        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_stats
        mock_stats.to_dict.return_value = {"is_black_screen": True}

        with patch("stream_prober.get_session", return_value=mock_session):
            prober._save_probe_result(1, "Test", {}, "success", None, is_black_screen=None)

        assert mock_stats.is_black_screen is True

    def test_name_change_resets_prior_content_and_failure_count(self):
        prober = create_prober()
        mock_stats = Mock(spec=StreamStats)
        mock_stats.stream_name = "Old event"
        mock_stats.consecutive_failures = 4
        mock_stats.is_black_screen = True
        mock_stats.black_screen_checked_at = object()

        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_stats
        mock_stats.to_dict.return_value = {"consecutive_failures": 1}

        with patch("stream_prober.get_session", return_value=mock_session):
            prober._save_probe_result(
                1, "New event", None, "failed", "Connection refused"
            )

        assert mock_stats.consecutive_failures == 1
        assert mock_stats.is_black_screen is False
        assert mock_stats.black_screen_checked_at is None


class TestConstructorBlackScreenSettings:
    """Tests for black screen constructor settings."""

    def test_default_values(self):
        """Black screen detection is off by default."""
        prober = create_prober()
        assert prober.black_screen_detection_enabled is False
        assert prober.black_screen_sample_duration == 5

    def test_custom_values(self):
        """Custom black screen settings are accepted."""
        prober = create_prober(
            black_screen_detection_enabled=True,
            black_screen_sample_duration=10,
        )
        assert prober.black_screen_detection_enabled is True
        assert prober.black_screen_sample_duration == 10

    def test_sample_duration_clamped(self):
        """Sample duration is clamped to 3-30 range."""
        prober_low = create_prober(black_screen_sample_duration=1)
        assert prober_low.black_screen_sample_duration == 3

        prober_high = create_prober(black_screen_sample_duration=60)
        assert prober_high.black_screen_sample_duration == 30
