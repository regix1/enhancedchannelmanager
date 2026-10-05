"""Real Linux media controls for event stream playability."""

import asyncio
import hashlib
import re
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from services.event_sync_stream_health import _fresh_flow_state
from stream_prober import StreamProber


_CREATE_PROCESS = asyncio.create_subprocess_exec


class _MediaHandler(BaseHTTPRequestHandler):
    routes = {}

    def do_GET(self):
        body, content_type, mode = self.routes[self.path]
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            if mode == "close":
                self.wfile.write(body[: min(len(body), 65536)])
                return
            if mode == "stall":
                self.wfile.write(body[: min(len(body), 65536)])
                self.wfile.flush()
                time.sleep(7)
                return
            if mode == "pace":
                chunk_size = max(1, len(body) // 20)
                for offset in range(0, len(body), chunk_size):
                    self.wfile.write(body[offset:offset + chunk_size])
                    self.wfile.flush()
                    time.sleep(0.06)
                return
            if mode == "trickle":
                for offset in range(0, len(body), 256):
                    self.wfile.write(body[offset:offset + 256])
                    self.wfile.flush()
                    time.sleep(0.1)
                return
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            return

    def log_message(self, _format, *_args):
        return


class _ObservedProcess:
    def __init__(self, process, samples):
        self._process = process
        self._samples = samples

    @property
    def returncode(self):
        return self._process.returncode

    async def communicate(self):
        stdout, stderr = await self._process.communicate()
        self._samples.append((self._process.returncode, stderr))
        return stdout, stderr

    def kill(self):
        return self._process.kill()

    async def wait(self):
        return await self._process.wait()


def _run_ffmpeg(output: Path, *args: str) -> None:
    result = subprocess.run(
        ["/usr/bin/ffmpeg", "-hide_banner", "-loglevel", "error", "-y", *args, str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.fixture(scope="module")
def media_files(tmp_path_factory):
    assert Path("/usr/bin/ffmpeg").is_file()
    assert Path("/usr/bin/ffprobe").is_file()

    root = tmp_path_factory.mktemp("stream-content")
    card = Path(__file__).parents[1] / "fixtures" / "event_sync" / "offline-card.jpg"
    assert hashlib.sha256(card.read_bytes()).hexdigest() == (
        "cf395881bef12a9f64e043d64bb6a6457c2d6f97ca277e2b17ea1e539ef577a0"
    )

    slate = root / "slate.ts"
    _run_ffmpeg(
        slate,
        "-loop", "1", "-framerate", "10", "-i", str(card),
        "-t", "3", "-vf", "scale=640:360", "-c:v", "libx264",
        "-pix_fmt", "yuv420p", "-f", "mpegts",
    )

    padded_slate = root / "padded-slate.ts"
    _run_ffmpeg(
        padded_slate,
        "-loop", "1", "-framerate", "10", "-i", str(card),
        "-t", "3",
        "-vf", "scale=640:-2,pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,noise=alls=2:allf=t+u",
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-b:v", "4M", "-minrate", "4M", "-maxrate", "4M", "-bufsize", "8M",
        "-f", "mpegts",
    )

    low_rate = root / "low-rate.ts"
    _run_ffmpeg(
        low_rate,
        "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=10:duration=4",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-b:v", "120k",
        "-f", "mpegts",
    )

    mixed = root / "mixed.ts"
    _run_ffmpeg(
        mixed,
        "-f", "lavfi", "-i", "color=c=black:size=320x180:rate=10:duration=8",
        "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=10:duration=2",
        "-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-f", "mpegts",
    )

    hls = root / "hls"
    hls.mkdir()
    playlist = hls / "index.m3u8"
    result = subprocess.run(
        [
            "/usr/bin/ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-i", str(low_rate), "-c", "copy", "-hls_time", "1",
            "-hls_list_size", "0", "-hls_segment_filename", str(hls / "segment-%02d.ts"),
            str(playlist),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    probe = subprocess.run(
        ["/usr/bin/ffprobe", "-v", "error", "-show_streams", str(low_rate)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr

    return {
        "slate": slate,
        "padded_slate": padded_slate,
        "low_rate": low_rate,
        "mixed": mixed,
        "playlist": playlist,
        "segments": sorted(hls.glob("segment-*.ts")),
    }


@pytest.fixture(scope="module")
def media_server(media_files):
    routes = {
        "/slate.ts": (media_files["slate"].read_bytes(), "video/mp2t", "send"),
        "/padded-slate.ts": (
            media_files["padded_slate"].read_bytes(), "video/mp2t", "send",
        ),
        "/low-rate.ts": (media_files["low_rate"].read_bytes(), "video/mp2t", "send"),
        "/mixed.ts": (media_files["mixed"].read_bytes(), "video/mp2t", "send"),
        "/truncated.ts": (
            media_files["low_rate"].read_bytes()[:188], "video/mp2t", "send",
        ),
        "/paced-slate.ts": (
            media_files["padded_slate"].read_bytes(), "video/mp2t", "pace",
        ),
        "/paced-low.ts": (media_files["low_rate"].read_bytes(), "video/mp2t", "pace"),
        "/burst-close.ts": (media_files["low_rate"].read_bytes(), "video/mp2t", "close"),
        "/burst-stall.ts": (media_files["low_rate"].read_bytes(), "video/mp2t", "stall"),
        "/trickle.ts": (
            media_files["low_rate"].read_bytes()[:256] * 100,
            "video/mp2t",
            "trickle",
        ),
        "/hls/index.m3u8": (
            media_files["playlist"].read_bytes(),
            "application/vnd.apple.mpegurl",
            "send",
        ),
        "/hls/master.m3u8": (
            b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=120000\nindex.m3u8\n",
            "application/vnd.apple.mpegurl",
            "send",
        ),
    }
    for segment in media_files["segments"]:
        routes[f"/hls/{segment.name}"] = (segment.read_bytes(), "video/mp2t", "send")
    _MediaHandler.routes = routes
    server = ThreadingHTTPServer(("127.0.0.1", 0), _MediaHandler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@asynccontextmanager
async def _allow_subprocess(url, **_kwargs):
    yield SimpleNamespace(argument=url, response=None, is_http_relay=False)


@asynccontextmanager
async def _allow_stream(url, *, timeout, headers, **_kwargs):
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream("GET", url, headers=headers) as response:
            response.extensions["ssrf_logical_url"] = str(response.url)
            yield response


async def _detect(prober, url, monkeypatch):
    samples = []

    async def observed(*args, **kwargs):
        process = await _CREATE_PROCESS(*args, **kwargs)
        return _ObservedProcess(process, samples)

    monkeypatch.setattr("stream_prober.validated_subprocess_input", _allow_subprocess)
    monkeypatch.setattr("stream_prober.asyncio.create_subprocess_exec", observed)
    verdict = await prober._detect_black_screen(url)
    return verdict, samples[-1]


def _frame_metrics(sample):
    returncode, stderr = sample
    output = stderr.decode(errors="replace")
    frame_times = [float(value) for value in re.findall(r"\bpts_time:([-+\d.eE]+)", output)]
    brightness = [
        float(value)
        for value in re.findall(r"lavfi\.signalstats\.YAVG=([-+\d.eE]+)", output)
    ]
    span = max(frame_times) - min(frame_times) if frame_times else 0.0
    return returncode, len(brightness), span


def _prober(*, content_seconds=3, sample_seconds=1):
    return StreamProber(
        client=None,
        probe_timeout=5,
        bitrate_sample_duration=sample_seconds,
        black_screen_detection_enabled=True,
        black_screen_sample_duration=content_seconds,
        probe_retry_count=0,
    )


@pytest.mark.asyncio
async def test_real_slate_and_low_rate_content_have_separate_polarity(
    media_server, monkeypatch,
):
    prober = _prober()
    slate, slate_sample = await _detect(prober, f"{media_server}/slate.ts", monkeypatch)
    low_rate, low_sample = await _detect(prober, f"{media_server}/low-rate.ts", monkeypatch)
    slate_exit, slate_frames, slate_span = _frame_metrics(slate_sample)
    low_exit, low_frames, low_span = _frame_metrics(low_sample)

    assert slate is True
    assert low_rate is False
    assert slate_exit == low_exit == 0
    assert slate_frames > 1 and low_frames > 1
    assert slate_span >= 2 and low_span >= 2
    print(
        "REAL_MEDIA detector "
        f"slate={slate} exit={slate_exit} frames={slate_frames} span={slate_span:.3f}s "
        f"low_rate={low_rate} exit={low_exit} frames={low_frames} span={low_span:.3f}s"
    )


@pytest.mark.asyncio
async def test_real_padded_slate_is_dark_despite_transport(
    media_server, monkeypatch,
):
    prober = _prober()
    dark, sample = await _detect(
        prober, f"{media_server}/padded-slate.ts", monkeypatch,
    )
    monkeypatch.setattr("stream_prober.stream_request", _allow_stream)
    measured = await prober._measure_stream_bitrate(f"{media_server}/paced-slate.ts")
    checked = datetime.now(timezone.utc)
    stat = {
        "stream_name": "padded slate",
        "probe_status": "success",
        "measured_bitrate": measured,
        "last_probed": checked.isoformat(),
        "is_black_screen": dark,
        "black_screen_checked_at": checked.isoformat(),
    }
    playable = _fresh_flow_state(
        stat,
        checked - timedelta(seconds=1),
        stream_name="padded slate",
        now=checked,
    )
    decoder_exit, frames, span = _frame_metrics(sample)

    assert measured is not None and measured > 0
    assert dark is True
    assert playable is False
    print(
        "REAL_MEDIA padded_slate "
        f"detector={dark} playable={playable} exit={decoder_exit} "
        f"frames={frames} span={span:.3f}s measured_bps={measured}"
    )


@pytest.mark.asyncio
async def test_real_low_rate_video_is_playable(media_server, monkeypatch):
    prober = _prober()
    dark, sample = await _detect(prober, f"{media_server}/low-rate.ts", monkeypatch)
    monkeypatch.setattr("stream_prober.stream_request", _allow_stream)
    measured = await prober._measure_stream_bitrate(f"{media_server}/paced-low.ts")
    checked = datetime.now(timezone.utc)
    stat = {
        "stream_name": "low rate",
        "probe_status": "success",
        "measured_bitrate": measured,
        "last_probed": checked.isoformat(),
        "is_black_screen": dark,
        "black_screen_checked_at": checked.isoformat(),
    }
    playable = _fresh_flow_state(
        stat,
        checked - timedelta(seconds=1),
        stream_name="low rate",
        now=checked,
    )
    decoder_exit, frames, span = _frame_metrics(sample)

    assert measured is not None and measured > 0
    assert dark is False
    assert playable is True
    print(
        "REAL_MEDIA low_rate "
        f"detector={dark} playable={playable} exit={decoder_exit} "
        f"frames={frames} span={span:.3f}s measured_bps={measured}"
    )


@pytest.mark.asyncio
async def test_real_dark_opening_followed_by_normal_frames_is_not_dark(
    media_server, monkeypatch,
):
    prober = _prober(content_seconds=10)
    dark, sample = await _detect(prober, f"{media_server}/mixed.ts", monkeypatch)
    decoder_exit, frames, span = _frame_metrics(sample)

    assert dark is False
    assert decoder_exit == 0
    assert frames > 1
    assert span >= 9
    print(
        "REAL_MEDIA mixed "
        f"detector={dark} exit={decoder_exit} frames={frames} span={span:.3f}s"
    )


@pytest.mark.asyncio
async def test_real_truncated_decoder_is_unknown(media_server, monkeypatch):
    prober = _prober()
    dark, sample = await _detect(prober, f"{media_server}/truncated.ts", monkeypatch)
    decoder_exit, frames, span = _frame_metrics(sample)

    assert dark is None
    assert decoder_exit != 0 or span < 2
    print(
        "REAL_MEDIA truncated "
        f"detector={dark} exit={decoder_exit} frames={frames} span={span:.3f}s"
    )


@pytest.mark.asyncio
async def test_real_transport_distinguishes_sustained_close_and_stall(
    media_server, monkeypatch,
):
    prober = _prober()
    monkeypatch.setattr("stream_prober.stream_request", _allow_stream)

    sustained = await prober._measure_stream_bitrate(f"{media_server}/paced-low.ts")
    trickle = await asyncio.wait_for(
        prober._measure_stream_bitrate(f"{media_server}/trickle.ts"),
        timeout=2,
    )
    closed = await prober._measure_stream_bitrate(f"{media_server}/burst-close.ts")
    stalled = await prober._measure_stream_bitrate(f"{media_server}/burst-stall.ts")

    assert sustained is not None and sustained > 0
    assert trickle is not None and 0 < trickle < 8 * 8192
    assert closed == 0
    assert stalled == 0
    print(
        "REAL_MEDIA transport "
        f"sustained_bps={sustained} trickle_bps={trickle} "
        f"burst_close_bps={closed} burst_stall_bps={stalled}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("playlist", ["index.m3u8", "master.m3u8"])
async def test_real_finite_hls_segments_are_playable(
    media_server, monkeypatch, playlist,
):
    prober = _prober(sample_seconds=2)
    url = f"{media_server}/hls/{playlist}"
    dark, sample = await _detect(prober, url, monkeypatch)
    monkeypatch.setattr("stream_prober.stream_request", _allow_stream)
    measured = await prober._measure_stream_bitrate(url)
    checked = datetime.now(timezone.utc)
    stat = {
        "stream_name": "finite hls",
        "probe_status": "success",
        "measured_bitrate": measured,
        "last_probed": checked.isoformat(),
        "is_black_screen": dark,
        "black_screen_checked_at": checked.isoformat(),
    }
    playable = _fresh_flow_state(
        stat,
        checked - timedelta(seconds=1),
        stream_name="finite hls",
        now=checked,
    )
    decoder_exit, frames, span = _frame_metrics(sample)

    assert measured is not None and measured > 0
    assert dark is False
    assert playable is True
    assert decoder_exit == 0
    assert frames > 1
    assert span >= prober.black_screen_sample_duration - 1
    print(
        f"REAL_MEDIA hls={playlist} "
        f"detector={dark} playable={playable} exit={decoder_exit} "
        f"frames={frames} span={span:.3f}s measured_bps={measured}"
    )


@pytest.mark.asyncio
async def test_probe_process_cleanup_is_bounded(monkeypatch):
    from stream_prober import _stop_probe

    class HungProcess:
        returncode = None

        def __init__(self):
            self.killed = False

        def kill(self):
            self.killed = True

        async def wait(self):
            await asyncio.Event().wait()

    process = HungProcess()
    monkeypatch.setattr("stream_prober._PROBE_STOP_SECONDS", 0.01)
    started = time.monotonic()

    await _stop_probe(process)

    assert process.killed is True
    assert time.monotonic() - started < 0.5


@pytest.mark.asyncio
async def test_ffprobe_cancellation_stops_owned_process_and_preserves_cancellation(
    monkeypatch,
):
    class WaitingProcess:
        returncode = None

        def __init__(self):
            self.started = asyncio.Event()
            self.killed = False
            self.waited = False

        async def communicate(self):
            self.started.set()
            await asyncio.Event().wait()

        def kill(self):
            self.killed = True

        async def wait(self):
            self.returncode = -9
            self.waited = True
            return self.returncode

    process = WaitingProcess()
    monkeypatch.setattr("stream_prober.validated_subprocess_input", _allow_subprocess)
    monkeypatch.setattr(
        "stream_prober.asyncio.create_subprocess_exec",
        AsyncMock(return_value=process),
    )
    prober = _prober()
    task = asyncio.create_task(prober._run_ffprobe("http://media/held"))
    await process.started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.killed is True
    assert process.waited is True
    assert process.returncode == -9


@pytest.mark.asyncio
async def test_real_child_cancellation_leaves_no_owned_process(monkeypatch):
    processes = []

    async def spawn(*_args, **_kwargs):
        process = await _CREATE_PROCESS(
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        processes.append(process)
        return process

    monkeypatch.setattr("stream_prober.validated_subprocess_input", _allow_subprocess)
    monkeypatch.setattr("stream_prober.asyncio.create_subprocess_exec", spawn)
    prober = _prober()
    task = asyncio.create_task(prober._run_ffprobe("http://media/held"))
    for _ in range(100):
        if processes:
            break
        await asyncio.sleep(0)
    assert processes
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert processes[0].returncode is not None


@pytest.mark.asyncio
async def test_probe_stream_threads_expiry_and_keeps_saved_result_when_push_is_cancelled():
    prober = _prober()
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=5)
    media = {
        "streams": [{
            "codec_type": "video",
            "width": 1920,
            "height": 1080,
            "codec_name": "h264",
            "r_frame_rate": "30/1",
        }],
        "format": {"format_name": "mpegts", "bit_rate": "4000000"},
    }
    prober._run_ffprobe = AsyncMock(return_value=media)
    prober._measure_stream_bitrate = AsyncMock(return_value=3_000_000)
    prober._detect_black_screen = AsyncMock(return_value=False)
    saved = {"probe_status": "success", "stream_id": 7}
    prober._save_probe_result = MagicMock(return_value=saved)
    push_started = asyncio.Event()

    async def push(*_args):
        push_started.set()
        await asyncio.Event().wait()

    prober._push_stats_to_dispatcharr = AsyncMock(side_effect=push)
    task = asyncio.create_task(prober.probe_stream(
        7,
        "http://media/7",
        "Seven",
        content=True,
        expires_at=expires_at,
    ))
    await push_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    prober._run_ffprobe.assert_awaited_once_with(
        "http://media/7", expires_at=expires_at,
    )
    prober._measure_stream_bitrate.assert_awaited_once_with(
        "http://media/7", expires_at=expires_at,
    )
    prober._detect_black_screen.assert_awaited_once_with(
        "http://media/7", expires_at=expires_at,
    )
    prober._save_probe_result.assert_called_once()


@pytest.mark.asyncio
async def test_probe_stream_expiry_during_stats_push_keeps_local_result():
    prober = _prober()
    media = {
        "streams": [{
            "codec_type": "video",
            "width": 1920,
            "height": 1080,
            "codec_name": "h264",
            "r_frame_rate": "30/1",
        }],
        "format": {"format_name": "mpegts", "bit_rate": "4000000"},
    }
    prober._run_ffprobe = AsyncMock(return_value=media)
    prober._measure_stream_bitrate = AsyncMock(return_value=3_000_000)
    prober._detect_black_screen = AsyncMock(return_value=False)
    saved = {"probe_status": "success", "stream_id": 9}
    prober._save_probe_result = MagicMock(return_value=saved)

    async def push(*_args):
        await asyncio.Event().wait()

    prober._push_stats_to_dispatcharr = AsyncMock(side_effect=push)

    result = await prober.probe_stream(
        9,
        "http://media/9",
        "Nine",
        content=True,
        expires_at=datetime.now(timezone.utc) + timedelta(milliseconds=30),
    )

    assert result == saved
    prober._save_probe_result.assert_called_once()
    prober._push_stats_to_dispatcharr.assert_awaited_once_with(9, saved)


@pytest.mark.asyncio
async def test_probe_stream_saves_nothing_after_expiry():
    prober = _prober()

    async def held_media(*_args, **_kwargs):
        await asyncio.Event().wait()

    prober._run_ffprobe = AsyncMock(side_effect=held_media)
    prober._save_probe_result = MagicMock()

    result = await prober.probe_stream(
        8,
        "http://media/8",
        "Eight",
        expires_at=datetime.now(timezone.utc) + timedelta(milliseconds=20),
    )

    assert result == {}
    prober._save_probe_result.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, True])
async def test_content_sample_without_loading_expiry(timeout):
    prober = _prober()
    media = {"streams": [], "format": {"format_name": "mpegts"}}
    prober._run_ffprobe = AsyncMock(
        return_value=media, side_effect=asyncio.TimeoutError if timeout else None,
    )
    prober._measure_stream_bitrate = AsyncMock(return_value=None if timeout else 3000000)
    prober._detect_black_screen = AsyncMock(return_value=False)
    prober._save_probe_result = MagicMock(return_value={"stream_id": 7})
    prober._push_stats_to_dispatcharr = AsyncMock()
    result = await prober.probe_stream(7, "http://media/7", "Seven", content=True, expires_at=None)
    assert result == {"stream_id": 7}
    prober._run_ffprobe.assert_awaited_once_with("http://media/7", expires_at=None)
    prober._measure_stream_bitrate.assert_awaited_once_with("http://media/7", expires_at=None)
    if timeout:
        prober._detect_black_screen.assert_not_awaited()
        assert prober._save_probe_result.call_args.args[3] == "timeout"
    else:
        prober._detect_black_screen.assert_awaited_once_with("http://media/7", expires_at=None)
        assert prober._save_probe_result.call_args.args[3] == "success"
