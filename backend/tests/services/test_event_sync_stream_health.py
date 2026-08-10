"""Health check for the streams Event Sync promotion is about to promote.

The rails that matter here are all about what the check REFUSES to do. It
reports a stream dead only on evidence, it probes only on a live run, and
every failure of its own machinery reads as "nothing is dead" so an outage
in ECM can never look like an outage at the provider.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.event_sync_stream_health import (
    MAX_HEALTH_PROBES_PER_RUN,
    find_dead_streams,
)


def _stat(stream_id, *, failures=0, status="success"):
    return {
        "stream_id": stream_id,
        "probe_status": status,
        "consecutive_failures": failures,
    }


def _settings(threshold=3):
    settings = MagicMock()
    settings.strike_threshold = threshold
    return settings


def _stats_returning(stats):
    def _get(stream_ids):
        return {sid: stats[sid] for sid in stream_ids if sid in stats}
    return _get


class TestVerdictFromStoredHealth:
    async def test_a_struck_stream_is_dead(self):
        stats = {7: _stat(7, failures=3), 8: _stat(8)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams([7, 8]) == {7}

    async def test_one_old_failure_is_not_dead(self):
        """Streams fail transiently. The strike threshold is the setting an
        operator already tuned to say how much failure is too much, so a
        single failure below it is not evidence."""
        stats = {7: _stat(7, failures=1, status="failed")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams([7]) == set()

    async def test_a_stream_with_no_health_record_is_not_dead(self):
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams([7, 8]) == set()

    async def test_the_strike_rule_switched_off_reports_nothing(self):
        stats = {7: _stat(7, failures=99, status="failed")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(0)):
            assert await find_dead_streams([7]) == set()

    async def test_no_candidates_asks_nothing(self):
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids") \
                as lookup:
            assert await find_dead_streams([]) == set()
            assert await find_dead_streams([None, None]) == set()
        assert lookup.call_count == 0


class TestFailOpen:
    """Every failure of the check's own machinery reads as "nothing dead"."""

    async def test_an_unreadable_health_table_reports_nothing(self):
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   side_effect=RuntimeError("no database")):
            assert await find_dead_streams([7, 8]) == set()

    async def test_an_unreadable_strike_threshold_reports_nothing(self):
        stats = {7: _stat(7, failures=99, status="failed")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", side_effect=RuntimeError("boom")):
            assert await find_dead_streams([7]) == set()

    async def test_no_prober_reports_nothing(self):
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=None):
            assert await find_dead_streams(
                [7], client=MagicMock(), probe_missing=True) == set()

    async def test_a_url_lookup_failure_reports_nothing(self):
        client = MagicMock()
        client.get_streams_by_ids = AsyncMock(
            side_effect=RuntimeError("provider down"))
        prober = MagicMock()
        prober.max_concurrent_probes = 4
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True) == set()

    async def test_a_probe_that_raises_leaves_the_stream_working(self):
        client = MagicMock()
        client.get_streams_by_ids = AsyncMock(
            return_value=[{"id": 7, "url": "http://x/7", "name": "seven"}])
        prober = MagicMock()
        prober.max_concurrent_probes = 4
        prober.probe_stream = AsyncMock(side_effect=RuntimeError("ffprobe"))
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True) == set()


class TestProbing:
    def _client(self, streams):
        client = MagicMock()
        client.get_streams_by_ids = AsyncMock(return_value=streams)
        return client

    def _prober(self, statuses):
        prober = MagicMock()
        prober.max_concurrent_probes = 4

        async def _probe(stream_id, url, name):
            return {"probe_status": statuses[stream_id]}

        prober.probe_stream = AsyncMock(side_effect=_probe)
        return prober

    async def test_a_stream_that_does_not_answer_is_dead(self):
        client = self._client([
            {"id": 7, "url": "http://x/7", "name": "seven"},
            {"id": 8, "url": "http://x/8", "name": "eight"},
        ])
        prober = self._prober({7: "failed", 8: "success"})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7, 8], client=client, probe_missing=True) == {7}

    async def test_a_timeout_counts_as_not_answering(self):
        client = self._client([{"id": 7, "url": "http://x/7", "name": "s"}])
        prober = self._prober({7: "timeout"})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True) == {7}

    async def test_probe_missing_off_never_probes(self):
        """The preview and every dry run take this path: a probe writes a
        health row, and neither of those may write anything."""
        client = self._client([{"id": 7, "url": "http://x/7", "name": "s"}])
        prober = self._prober({7: "failed"})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams([7], client=client) == set()
        assert prober.probe_stream.call_count == 0
        assert client.get_streams_by_ids.call_count == 0

    async def test_an_already_probed_stream_is_not_probed_again(self):
        client = self._client([{"id": 8, "url": "http://x/8", "name": "s"}])
        prober = self._prober({8: "success"})
        stats = {7: _stat(7)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            await find_dead_streams(
                [7, 8], client=client, probe_missing=True)
        assert client.get_streams_by_ids.call_args[0][0] == [8]

    async def test_a_stream_with_no_url_is_left_alone(self):
        client = self._client([{"id": 7, "url": "", "name": "s"}])
        prober = self._prober({})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True) == set()
        assert prober.probe_stream.call_count == 0

    async def test_probing_is_capped_per_run(self):
        """Probing dials the provider, so a first run on a big rule must
        not hold the pipeline open for the whole playlist. Runs are
        idempotent: the rest gets probed later."""
        candidates = list(range(1, MAX_HEALTH_PROBES_PER_RUN + 51))
        client = self._client([
            {"id": sid, "url": f"http://x/{sid}", "name": str(sid)}
            for sid in candidates
        ])
        prober = self._prober({sid: "success" for sid in candidates})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            await find_dead_streams(
                candidates, client=client, probe_missing=True)
        assert len(client.get_streams_by_ids.call_args[0][0]) \
            == MAX_HEALTH_PROBES_PER_RUN

    async def test_a_struck_stream_and_a_failing_probe_are_both_reported(self):
        client = self._client([{"id": 8, "url": "http://x/8", "name": "s"}])
        prober = self._prober({8: "failed"})
        stats = {7: _stat(7, failures=5, status="failed")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7, 8], client=client, probe_missing=True) == {7, 8}


@pytest.mark.parametrize("probe_missing", [True, False])
async def test_no_client_never_probes(probe_missing):
    with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
               _stats_returning({})), \
         patch("config.get_settings", return_value=_settings(3)), \
         patch("stream_prober.ensure_prober") as ensure:
        assert await find_dead_streams(
            [7], probe_missing=probe_missing) == set()
    assert ensure.call_count == 0
