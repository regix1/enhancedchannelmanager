"""Health check for the streams Event Sync promotion is about to promote.

The rails that matter here are all about what the check REFUSES to do. It
reports a stream dead only on evidence, it probes only on a live run, and
every failure of its own machinery reads as "nothing is dead" so an outage
in ECM can never look like an outage at the provider.

The evidence itself is one rule: a stream is dead when the provider has
stopped listing it, and a probe verdict counts only once the event has
started. Everything in here is a case of that.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.event_sync_stream_health import (
    MAX_HEALTH_PROBES_PER_RUN,
    find_dead_streams,
    find_working_streams,
)


# When the events in these tests began, and when a stored verdict was
# recorded unless a test says otherwise. A probe taken after kickoff is the
# ordinary case; one taken before it is the case in
# test_a_failure_recorded_before_kickoff_stays_out_of_the_verdict.
_KICKOFF = datetime(2026, 7, 11, 23, 0, 0, tzinfo=timezone.utc)


def _stat(stream_id, *, failures=0, status="success", probed_at=None,
          measured=None):
    return {
        "stream_id": stream_id,
        "probe_status": status,
        "consecutive_failures": failures,
        "measured_bitrate": measured,
        "last_probed": (probed_at or _KICKOFF).replace(
            tzinfo=None).isoformat() + "Z",
    }


def _settings(threshold=3, floor_kbps=2000):
    settings = MagicMock()
    settings.strike_threshold = threshold
    settings.min_stream_bitrate_kbps = floor_kbps
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
            assert await find_dead_streams(
                [7, 8], event_start_by_stream={7: _KICKOFF, 8: _KICKOFF}) == {7}

    async def test_a_stored_failure_after_the_event_started_is_dead(self):
        """One recorded failure is below the strike threshold, and once the
        event is on air that is still a stream that did not answer."""
        stats = {7: _stat(7, failures=1, status="failed")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == {7}

    async def test_a_stored_failure_before_the_event_starts_is_not_dead(self):
        """The criterion that keeps upcoming events working: a stream for an
        event that has not begun may fail simply because there is nothing to
        stream yet."""
        stats = {7: _stat(7, failures=99, status="failed")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams([7], event_start_by_stream={}) \
                == set()

    async def test_a_failure_recorded_before_kickoff_stays_out_of_the_verdict(
        self
    ):
        """The same failure as the test above, still stored an hour later
        when the event is on air. It was recorded while the stream had
        nothing to serve, and a stream that already has a record is never
        probed again, so counting it now would decide the event on evidence
        nothing will ever refresh. [59]
        """
        stats = {7: _stat(7, failures=1, status="failed",
                          probed_at=_KICKOFF - timedelta(hours=1))}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == set()

    async def test_a_stream_with_no_health_record_is_not_dead(self):
        """Nothing has looked at this stream. Roughly sixty of thirty-seven
        thousand streams have ever been probed, so an absent verdict has to
        mean nothing, even for an event already on air."""
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7, 8], event_start_by_stream={7: _KICKOFF, 8: _KICKOFF}) == set()

    async def test_the_strike_rule_switched_off_reports_nothing(self):
        stats = {7: _stat(7, failures=99, status="success")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(0)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == set()

    async def test_an_unreadable_strike_threshold_keeps_the_check_on(self):
        """A settings error is not the operator switching the struck-out
        check off, so it must not read as a threshold of 0."""
        stats = {7: _stat(7, failures=99, status="success")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", side_effect=RuntimeError("boom")):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == {7}

    async def test_a_stream_nobody_probed_recently_is_not_dead(self):
        """"Not probed recently" says only that ECM has not looked, so it
        must never reach this gate. The guard exists because there is a
        second endpoint that merges that signal into its stale list, and
        reading the gate off THAT one would block essentially every
        candidate."""
        stat = _stat(7, status="success")
        stat["last_probed"] = "2020-01-01T00:00:00Z"
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({7: stat})), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == set()

    async def test_no_candidates_asks_nothing(self):
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids") \
                as lookup:
            assert await find_dead_streams([]) == set()
            assert await find_dead_streams([None, None]) == set()
        assert lookup.call_count == 0


class TestVerdictFromSampledThroughput:
    """What the stream was actually pushing, once its event was on air.

    ffprobe reads a container header and stops, so it never asks whether
    bytes keep arriving. Measured one at a time against 11 event channels
    it disagreed with the sampled throughput 5 times, in BOTH directions,
    which is worse than a coin flip. So a row carrying a sample taken at or
    after kickoff is judged on the sample, and ffprobe's stored verdict
    decides only a row that has none.

    The provider says "no event" in three shapes and all three land here:
    an offline card looping at 0.45 Mbps, a socket that opens and closes
    with no bytes at all, and a socket that never sends anything and times
    out. Content ran 4.97 Mbps and up, so the 2 Mbps floor sits in empty
    space.
    """

    async def test_a_stream_pushing_less_than_the_floor_is_dead(self):
        """The offline card: 0.45 Mbps, with ffprobe perfectly happy about
        the container it read."""
        stats = {7: _stat(7, status="success", measured=450_000)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == {7}

    async def test_a_stream_sending_no_bytes_at_all_is_dead(self):
        """The same state caught at a different moment: the socket opens,
        nothing arrives and the provider closes it cleanly. Zero is a
        measurement, not a missing one."""
        stats = {7: _stat(7, status="success", measured=0)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == {7}

    async def test_a_stream_that_timed_out_with_nothing_to_sample_is_dead(
        self
    ):
        """The third shape: nothing ever arrives and the socket does not
        even close, so there is no sample to take and the stored verdict is
        all there is."""
        stats = {7: _stat(7, status="timeout", measured=None)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == {7}

    async def test_a_failed_probe_pushing_real_content_is_not_dead(self):
        """The mirror, and the one this whole change exists for: three of
        the channels carrying their event at 5.65 to 7.87 Mbps are stored
        as ``failed``, because ffprobe could not parse what they sent."""
        stats = {7: _stat(7, failures=1, status="failed",
                          measured=6_140_000)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == set()

    async def test_a_struck_stream_pushing_real_content_is_not_dead(self):
        """The strike counter is fed by the same ffprobe verdict, so a
        stream ffprobe cannot parse strikes out while it plays."""
        stats = {7: _stat(7, failures=99, status="failed",
                          measured=7_870_000)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == set()

    async def test_a_sample_taken_before_kickoff_stays_out_of_the_verdict(
        self
    ):
        """A stream dialled while its event was still ahead was sampled
        against the offline card, and a stream that already has a record is
        never probed again, so counting that sample now would condemn the
        event on evidence nothing will ever refresh. [59]"""
        stats = {7: _stat(7, status="success", measured=0,
                          probed_at=_KICKOFF - timedelta(hours=1))}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == set()

    async def test_a_stream_before_its_event_is_not_judged_on_throughput(
        self
    ):
        """Nothing is being broadcast yet, so an empty stream says nothing
        about the stream."""
        stats = {7: _stat(7, status="success", measured=0)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={}) == set()

    async def test_a_stream_nobody_sampled_is_not_dead_on_that_alone(self):
        """Every row written before this column existed reads as ``None``,
        and so does one whose sample could not be taken. An absent number
        is not a low one."""
        stats = {7: _stat(7, status="success", measured=None)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == set()

    async def test_the_floor_is_the_operators_setting_in_kbps(self):
        """6.14 Mbps of real content, against an operator who set the floor
        to 8000 kbps. Pins both the setting and the unit conversion."""
        stats = {7: _stat(7, status="success", measured=6_140_000)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings",
                   return_value=_settings(3, floor_kbps=8000)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == {7}

    async def test_an_unreadable_floor_keeps_the_check_on(self):
        """A settings error is not the operator switching the throughput
        check off, so it must not read as a floor of 0 — nothing is ever
        below that."""
        stats = {7: _stat(7, status="success", measured=0)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", side_effect=RuntimeError("boom")):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == {7}

    async def test_the_floor_switched_off_reports_nothing(self):
        """A stored 0 IS the operator switching the check off, unlike an
        unreadable setting."""
        stats = {7: _stat(7, status="success", measured=0)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings",
                   return_value=_settings(3, floor_kbps=0)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == set()

    async def test_the_floor_switched_off_leaves_the_stored_verdict_alone(
        self
    ):
        """Switching the throughput check off must not switch the stored
        ffprobe verdict off along with it. The sample is what stops being
        consulted, and everything the gate did before it existed carries
        on."""
        stats = {7: _stat(7, failures=1, status="failed",
                          measured=6_140_000)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings",
                   return_value=_settings(3, floor_kbps=0)):
            assert await find_dead_streams(
                [7], event_start_by_stream={7: _KICKOFF}) == {7}


class TestStaleStreamsToDetach:
    """The rule the run applies and the preview reports, in one place. [75]"""

    def test_nothing_goes_without_a_working_stream_on_the_channel(self):
        from services.event_sync_stream_health import stale_streams_to_detach
        assert stale_streams_to_detach(
            unit_stream_ids={1, 2},
            attached=[1, 2],
            stale_stream_ids={1},
            working_stream_ids=set(),
        ) == []

    def test_the_delisted_stream_goes_once_something_works(self):
        from services.event_sync_stream_health import stale_streams_to_detach
        assert stale_streams_to_detach(
            unit_stream_ids={1, 2},
            attached=[1, 2],
            stale_stream_ids={1},
            working_stream_ids={2},
        ) == [1]

    def test_another_events_stream_is_never_taken(self):
        """Two events can derive the same channel name and share a channel.
        Stream 9 is delisted and attached, but it belongs to a different
        event, so this event's passing probe must not remove it."""
        from services.event_sync_stream_health import stale_streams_to_detach
        assert stale_streams_to_detach(
            unit_stream_ids={1, 2},
            attached=[1, 2, 9],
            stale_stream_ids={1, 9},
            working_stream_ids={2},
        ) == [1]

    def test_a_working_stream_on_the_channel_but_not_this_event_is_not_evidence(self):
        """Stream 8 works and is attached, but it is not this event's, so it
        proves nothing about whether this event can lose its delisted one."""
        from services.event_sync_stream_health import stale_streams_to_detach
        assert stale_streams_to_detach(
            unit_stream_ids={1},
            attached=[1, 8],
            stale_stream_ids={1},
            working_stream_ids={8},
        ) == []


class TestDelistedStreams:
    """Dispatcharr's own ``is_stale`` flag: the provider has stopped listing
    the stream. This provider re-issues every event under a new id on each
    refresh, so the superseded id keeps the ``success`` verdict it earned
    while it still worked, and only the listing says otherwise."""

    def _probing_client(self):
        client = MagicMock()
        client.get_streams_by_ids = AsyncMock(
            return_value=[{"id": 7, "url": "http://x/7", "name": "seven"}])
        return client

    def _prober(self):
        prober = MagicMock()
        prober.max_concurrent_probes = 4
        prober.probe_stream = AsyncMock(
            return_value={"probe_status": "success"})
        # Probing is bounded per provider, so the health check asks the prober
        # for that account's gate and refreshes the ceilings first. [76]
        prober.refresh_account_probe_limits = AsyncMock(return_value=None)
        prober.semaphore_for_account = lambda _account: asyncio.Semaphore(4)
        return prober

    async def test_a_delisted_stream_is_dead_despite_a_success_record(self):
        stats = {7: _stat(7, status="success"), 8: _stat(8)}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7, 8], stale_stream_ids={7}) == {7}

    async def test_a_delisted_stream_is_dead_before_its_event_starts(self):
        """Staleness carries the whole load for an event still ahead: a
        delisted stream is delisted whether or not it has aired."""
        stats = {7: _stat(7, status="success")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], stale_stream_ids={7}, event_start_by_stream={}) == {7}

    async def test_a_delisted_stream_is_never_probed(self):
        client = self._probing_client()
        prober = self._prober()
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True,
                stale_stream_ids={7}) == {7}
        assert prober.probe_stream.call_count == 0
        assert client.get_streams_by_ids.call_count == 0

    async def test_a_listed_stream_with_a_success_record_is_not_dead(self):
        stats = {7: _stat(7, status="success")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams(
                [7], stale_stream_ids=set(), event_start_by_stream={7: _KICKOFF}) == set()


class TestFailOpen:
    """A failure of the check's own machinery reads as "nothing dead". The
    one thing that survives it is the provider's own statement that a stream
    is no longer listed, which needed no lookup to begin with."""

    async def test_an_unreadable_health_table_reports_nothing(self):
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   side_effect=RuntimeError("no database")):
            assert await find_dead_streams([7, 8]) == set()

    async def test_an_unreadable_health_table_still_reports_delisted(self):
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   side_effect=RuntimeError("no database")):
            assert await find_dead_streams(
                [7, 8], stale_stream_ids={8}) == {8}

    async def test_no_prober_reports_nothing(self):
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=None):
            assert await find_dead_streams(
                [7], client=MagicMock(), probe_missing=True,
                event_start_by_stream={7: _KICKOFF}) == set()

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
                [7], client=client, probe_missing=True,
                event_start_by_stream={7: _KICKOFF}) == set()

    async def test_a_probe_that_raises_leaves_the_stream_working(self):
        client = MagicMock()
        client.get_streams_by_ids = AsyncMock(
            return_value=[{"id": 7, "url": "http://x/7", "name": "seven"}])
        prober = MagicMock()
        prober.max_concurrent_probes = 4
        prober.probe_stream = AsyncMock(side_effect=RuntimeError("ffprobe"))
        prober.refresh_account_probe_limits = AsyncMock(return_value=None)
        prober.semaphore_for_account = lambda _account: asyncio.Semaphore(4)
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True,
                event_start_by_stream={7: _KICKOFF}) == set()


class TestProbing:
    def _client(self, streams):
        client = MagicMock()
        client.get_streams_by_ids = AsyncMock(return_value=streams)
        return client

    def _prober(self, statuses, measured=None):
        prober = MagicMock()
        prober.max_concurrent_probes = 4
        samples = measured or {}

        async def _probe(stream_id, url, name):
            # probe_stream hands back the saved row, so the sampled
            # throughput is always a key even when nothing was sampled.
            return {
                "probe_status": statuses[stream_id],
                "measured_bitrate": samples.get(stream_id),
            }

        prober.probe_stream = AsyncMock(side_effect=_probe)
        # Probing is bounded per provider now, so the health check refreshes
        # the ceilings and asks for that account's gate. [76]
        prober.refresh_account_probe_limits = AsyncMock(return_value=None)
        prober.semaphore_for_account = lambda _account: asyncio.Semaphore(4)
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
                [7, 8], client=client, probe_missing=True,
                event_start_by_stream={7: _KICKOFF, 8: _KICKOFF}) == {7}

    async def test_a_probe_failure_before_the_event_starts_is_not_dead(self):
        """The measured case that made this rule necessary: every listed
        replacement for an event days away probes failed, and rejecting
        them would stop the channel ever being created."""
        client = self._client([{"id": 7, "url": "http://x/7", "name": "s"}])
        prober = self._prober({7: "failed"})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True,
                event_start_by_stream={}) == set()

    async def test_a_timeout_counts_as_not_answering(self):
        client = self._client([{"id": 7, "url": "http://x/7", "name": "s"}])
        prober = self._prober({7: "timeout"})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True,
                event_start_by_stream={7: _KICKOFF}) == {7}

    async def test_a_fresh_probe_pushing_real_content_is_not_dead(self):
        """This path decides most live runs, not the stored-row one: the
        provider re-issues every event under a new stream id on each
        refresh, so almost nothing a run promotes has a health record yet.
        ffprobe cannot parse this stream and it is carrying its event at
        6.14 Mbps."""
        client = self._client([{"id": 7, "url": "http://x/7", "name": "s"}])
        prober = self._prober({7: "failed"}, measured={7: 6_140_000})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True,
                event_start_by_stream={7: _KICKOFF}) == set()

    async def test_a_fresh_probe_sending_almost_nothing_is_dead(self):
        """The other direction, on the same path: ffprobe reads the
        offline card's container perfectly happily, and 0.45 Mbps against
        a 2 Mbps floor is the provider saying there is no event."""
        client = self._client([{"id": 7, "url": "http://x/7", "name": "s"}])
        prober = self._prober({7: "success"}, measured={7: 450_000})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True,
                event_start_by_stream={7: _KICKOFF}) == {7}

    async def test_a_fresh_probe_before_the_event_is_not_judged_on_it(self):
        """The started-only rule still governs the whole path, sample or
        no sample: the event has not begun, so an empty stream says
        nothing."""
        client = self._client([{"id": 7, "url": "http://x/7", "name": "s"}])
        prober = self._prober({7: "success"}, measured={7: 0})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True,
                event_start_by_stream={}) == set()

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
                [7, 8], client=client, probe_missing=True,
                event_start_by_stream={7: _KICKOFF, 8: _KICKOFF})
        assert client.get_streams_by_ids.call_args[0][0] == [8]

    async def test_a_stream_whose_event_is_still_ahead_is_not_probed(self):
        """Dialling it now writes a row taken while the event still had
        nothing to serve. Nothing re-probes a stream that has a row, and a
        row from before kickoff is not read as a live-event verdict, so that
        one probe would put the stream beyond the gate's reach for good. The
        verdict is discarded this run either way, so nothing is lost by
        waiting until the event is on air.
        """
        client = self._client([
            {"id": 7, "url": "http://x/7", "name": "s"},
            {"id": 8, "url": "http://x/8", "name": "s"},
        ])
        prober = self._prober({7: "failed", 8: "failed"})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7, 8], client=client, probe_missing=True,
                event_start_by_stream={8: _KICKOFF}) == {8}
        assert client.get_streams_by_ids.call_args[0][0] == [8]

    async def test_a_stream_with_no_url_is_left_alone(self):
        client = self._client([{"id": 7, "url": "", "name": "s"}])
        prober = self._prober({})
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)), \
             patch("stream_prober.ensure_prober", return_value=prober):
            assert await find_dead_streams(
                [7], client=client, probe_missing=True,
                event_start_by_stream={7: _KICKOFF}) == set()
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
                candidates, client=client, probe_missing=True,
                event_start_by_stream={sid: _KICKOFF for sid in candidates})
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
                [7, 8], client=client, probe_missing=True,
                event_start_by_stream={7: _KICKOFF, 8: _KICKOFF}) == {7, 8}


class TestProvenWorking:
    """The other half of the gate, and deliberately not its complement.

    "Not dead" is what a stream needs to be ATTACHED, because refusing an
    unprobed candidate would create no channels at all. Taking a stream
    off a channel that is currently serving an event needs more than the
    absence of bad news, so only a passing verdict counts here. [51]
    """

    async def test_only_a_passing_verdict_counts_as_working(self):
        stats = {7: _stat(7), 8: _stat(8, failures=1, status="failed")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)):
            assert await find_working_streams([7, 8]) == {7}

    async def test_a_stream_nobody_probed_is_not_working(self):
        """The gap between the two questions: this stream is not dead, and
        it is not proven to work either."""
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})):
            assert await find_working_streams([7]) == set()
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning({})), \
             patch("config.get_settings", return_value=_settings(3)):
            assert await find_dead_streams([7], event_start_by_stream={7: _KICKOFF}) \
                == set()

    async def test_a_timed_out_probe_is_not_working(self):
        stats = {7: _stat(7, status="timeout"), 8: _stat(8, status="pending")}
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_returning(stats)):
            assert await find_working_streams([7, 8]) == set()

    async def test_an_unreadable_health_table_proves_nothing_working(self):
        """Fail CLOSED here, unlike the dead check. Nothing is proven to
        work, so nothing is taken off a channel."""
        with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   side_effect=RuntimeError("db is gone")):
            assert await find_working_streams([7, 8]) == set()


@pytest.mark.parametrize("probe_missing", [True, False])
async def test_no_client_never_probes(probe_missing):
    with patch("stream_prober.StreamProber.get_stats_by_stream_ids",
               _stats_returning({})), \
         patch("config.get_settings", return_value=_settings(3)), \
         patch("stream_prober.ensure_prober") as ensure:
        assert await find_dead_streams(
            [7], probe_missing=probe_missing) == set()
    assert ensure.call_count == 0
