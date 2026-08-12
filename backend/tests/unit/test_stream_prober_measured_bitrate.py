"""A stream ffprobe cannot read may still be carrying content.

The prober used to sample throughput only after ffprobe returned, so a stream
ffprobe choked on was never measured at all, and the number it did produce was
filed in video_bitrate beside ffprobe's own declared bitrate, where no later
reader could tell a measurement from a claim.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine, text

import database
from models import StreamStats
from stream_prober import StreamProber

STREAM_URL = "http://example.com/903"
SAMPLED_BPS = 6_140_000
DECLARED_BPS = 3_000_000


def create_prober(measured: int | None = SAMPLED_BPS) -> StreamProber:
    """A prober whose ffprobe and sampler are both stubbed by the caller."""
    prober = StreamProber(
        client=MagicMock(),
        probe_timeout=30,
        black_screen_detection_enabled=False,
    )
    prober._measure_stream_bitrate = AsyncMock(return_value=measured)
    prober._push_stats_to_dispatcharr = AsyncMock()
    return prober


class TestProbeMeasuresWhateverFfprobeDid:
    @pytest.mark.asyncio
    async def test_measures_throughput_when_ffprobe_raises(self, test_session):
        """ffprobe reads a container header and stops, so a stream it cannot
        parse may still be pushing megabits. Three of the event channels behaved
        exactly this way. [1]
        """
        prober = create_prober()
        prober._run_ffprobe = AsyncMock(side_effect=RuntimeError("ffprobe failed: Invalid data"))

        with patch("stream_prober.get_session", return_value=test_session):
            await prober.probe_stream(903, STREAM_URL, "Stream 903")

        prober._measure_stream_bitrate.assert_awaited_once_with(STREAM_URL)

    @pytest.mark.asyncio
    async def test_measures_throughput_when_ffprobe_times_out(self, test_session):
        """A socket that opens and then delivers nothing is one of the ways the
        provider says there is no event, and it has to be measured to be seen.
        """
        prober = create_prober(measured=0)
        prober._run_ffprobe = AsyncMock(side_effect=asyncio.TimeoutError())

        with patch("stream_prober.get_session", return_value=test_session):
            saved = await prober.probe_stream(924, STREAM_URL, "Stream 924")

        prober._measure_stream_bitrate.assert_awaited_once_with(STREAM_URL)
        assert saved["probe_status"] == "timeout"
        assert saved["measured_bitrate"] == 0

    @pytest.mark.asyncio
    async def test_failed_probe_is_saved_with_its_measurement(self, test_session):
        """The stored row carries both the ffprobe verdict and the number, so a
        later reader can disagree with ffprobe. [2]
        """
        prober = create_prober()
        prober._run_ffprobe = AsyncMock(side_effect=RuntimeError("ffprobe failed: Invalid data"))

        with patch("stream_prober.get_session", return_value=test_session):
            saved = await prober.probe_stream(903, STREAM_URL, "Stream 903")

        assert saved["probe_status"] == "failed"
        assert saved["measured_bitrate"] == SAMPLED_BPS

    @pytest.mark.asyncio
    async def test_measurement_never_lands_in_video_bitrate(self, test_session):
        """ffprobe declared 3 Mbps and the sample read 6.14 Mbps. Both numbers
        survive, in their own columns. [3]
        """
        prober = create_prober()
        prober._run_ffprobe = AsyncMock(return_value={
            "streams": [{
                "codec_type": "video",
                "codec_name": "h264",
                "width": 1920,
                "height": 1080,
                "r_frame_rate": "30/1",
                "bit_rate": str(DECLARED_BPS),
            }],
            "format": {"format_name": "mpegts", "bit_rate": "3500000"},
        })

        with patch("stream_prober.get_session", return_value=test_session):
            saved = await prober.probe_stream(919, STREAM_URL, "Stream 919")

        assert saved["probe_status"] == "success"
        assert saved["measured_bitrate"] == SAMPLED_BPS
        assert saved["video_bitrate"] == DECLARED_BPS

    @pytest.mark.asyncio
    async def test_video_bitrate_stays_empty_when_ffprobe_raised(self, test_session):
        """Nothing was declared, so nothing is claimed. [3]"""
        prober = create_prober()
        prober._run_ffprobe = AsyncMock(side_effect=RuntimeError("ffprobe failed: Invalid data"))

        with patch("stream_prober.get_session", return_value=test_session):
            saved = await prober.probe_stream(926, STREAM_URL, "Stream 926")

        assert saved["video_bitrate"] is None
        assert saved["measured_bitrate"] == SAMPLED_BPS


class TestAFailedProbeDropsAStaleNumber:
    @pytest.mark.asyncio
    async def test_total_failure_clears_the_stored_measurement(self, test_session):
        """One good sample and then nothing answering used to leave 7 Mbps
        standing, so a reader that takes the sample over the probe verdict called
        a stream alive long after it stopped. [34]
        """
        test_session.add(StreamStats(stream_id=931, measured_bitrate=7_000_000))
        test_session.commit()

        prober = create_prober(measured=None)
        prober._run_ffprobe = AsyncMock(side_effect=RuntimeError("ffprobe failed: Invalid data"))

        with patch("stream_prober.get_session", return_value=test_session):
            saved = await prober.probe_stream(931, STREAM_URL, "Stream 931")

        assert saved["probe_status"] == "failed"
        assert saved["measured_bitrate"] is None

    @pytest.mark.asyncio
    async def test_a_readable_stream_keeps_its_number_when_the_sample_fails(self, test_session):
        """ffprobe read the stream and only the sampler came back empty. That is
        not evidence the stream stopped, so the stored number stands. [34]
        """
        test_session.add(StreamStats(stream_id=932, measured_bitrate=7_000_000))
        test_session.commit()

        prober = create_prober(measured=None)
        prober._run_ffprobe = AsyncMock(return_value={
            "streams": [{"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080}],
            "format": {"format_name": "mpegts"},
        })

        with patch("stream_prober.get_session", return_value=test_session):
            saved = await prober.probe_stream(932, STREAM_URL, "Stream 932")

        assert saved["probe_status"] == "success"
        assert saved["measured_bitrate"] == 7_000_000


class TestStreamStatsCarriesTheColumn:
    def test_to_dict_includes_measured_bitrate(self):
        """[4]"""
        stats = StreamStats(stream_id=903, measured_bitrate=SAMPLED_BPS)

        assert stats.to_dict()["measured_bitrate"] == SAMPLED_BPS

    def test_column_is_added_to_a_database_that_predates_it(self, tmp_path):
        """An upgrade has to reach a database that already exists. [5]

        ``init_db`` asserts every model column is physically present BEFORE it
        runs the in-process column additions, so a new column can only arrive
        through a migration: alembic runs ahead of that assertion, and
        ``create_all`` adds tables rather than columns. Building the schema one
        revision back is what makes this a real check, since a fresh database
        gets the column from ``create_all`` and would pass either way.
        """
        from alembic import command
        from alembic.config import Config

        db_file = tmp_path / "predates.db"
        url = f"sqlite:///{db_file}"
        cfg = Config(str(database.ALEMBIC_INI_PATH))
        cfg.set_main_option("sqlalchemy.url", url)

        command.upgrade(cfg, "0039")
        engine = create_engine(url)
        try:
            before = [
                row[1]
                for row in engine.connect().execute(
                    text("PRAGMA table_info(stream_stats)")
                ).fetchall()
            ]
            assert "measured_bitrate" not in before, "0039 is the state this has to upgrade FROM"
            with engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO stream_stats (stream_id, probe_status, created_at,"
                    " consecutive_failures, is_black_screen, is_low_fps)"
                    " VALUES (4711, 'success', '2026-08-12 00:00:00', 0, 0, 0)"
                ))
        finally:
            engine.dispose()

        command.upgrade(cfg, "head")

        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                columns = [
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(stream_stats)")).fetchall()
                ]
                row = conn.execute(text(
                    "SELECT measured_bitrate FROM stream_stats WHERE stream_id = 4711"
                )).fetchone()
        finally:
            engine.dispose()

        assert "measured_bitrate" in columns
        # A row written before the column existed reads as never measured, which
        # the health check treats differently from a measured zero. [34]
        assert row is not None and row[0] is None
