"""Ordering a channel's fallback streams when nothing has been probed.

A channel built by merging every provider's copy of a network holds a mix of
4K, FHD, HD and SD feeds. Only a probed stream has a measured resolution, and
scoring every unprobed one 0 left that order arbitrary, so an SD copy could
sit above a 4K one. The provider name already declares the tier and costs no
probe and no provider connection to read.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from channel_pipeline_engine import (
    ChannelPipelineEngine,
    _resolution_height_from_stats,
    _sort_streams_by_resolution_height,
)
from channel_pipeline_evaluator import StreamContext
from services.event_sync_resolver import SecondaryStream


class TestHeightFromStats:
    def test_a_measured_resolution_is_used_as_is(self):
        assert _resolution_height_from_stats(
            {"resolution": "1920x1080", "stream_name": "Fox Sports 1 SD"}
        ) == 1080

    def test_a_measurement_outranks_the_name(self):
        """The name is a provider's claim; a probe is the truth. A stream
        labelled SD that measures 1080 sorts as 1080."""
        assert _resolution_height_from_stats(
            {"resolution": "1920x1080", "stream_name": "ESPN SD"}
        ) == 1080

    def test_an_unprobed_stream_falls_back_to_the_tier_in_its_name(self):
        for name, height in (
            ("US: Fox Sports 1 4K", 2160),
            ("US| FOX SPORTS 1 FHD", 1080),
            ("US| FOX SPORTS 1 HD", 720),
            ("Fox Sports 1 SD", 480),
        ):
            assert _resolution_height_from_stats({"stream_name": name}) == height, name

    def test_a_name_declaring_no_tier_is_treated_as_hd(self):
        """ECM reads an unlabelled stream as HD everywhere else too
        (stream_normalization.DEFAULT_QUALITY_PRIORITY, which is what the
        streams API reports as its quality_tier). Sorting picks up that same
        assumption rather than inventing a second one: unlabelled ranks above
        an explicit SD and below an explicit FHD.
        """
        assert _resolution_height_from_stats({"stream_name": "US: ESPN"}) == 720

    def test_no_stats_at_all_scores_zero(self):
        assert _resolution_height_from_stats(None) == 0
        assert _resolution_height_from_stats({}) == 0

    def test_an_unparsable_resolution_still_falls_back_to_the_name(self):
        assert _resolution_height_from_stats(
            {"resolution": "not-a-size", "stream_name": "ESPN 4K"}
        ) == 2160


class TestNameSourcesReachTheSort:
    """A channel's stream list arrives either as dicts carrying names or as
    bare ids. Only probed streams have a name in the stats cache, so when the
    payload is id-only the sort saw no name at all and scored every fallback
    equal — which is how a live channel ended up SD-first with 4K last.
    """

    def _stats_for(self, current_streams, stream_items, stream_name_map):
        """Mirror the engine's seeding step for both payload shapes."""
        payload_names = {
            s["id"]: s["name"]
            for s in stream_items
            if isinstance(s, dict) and s.get("id") and s.get("name")
        }
        names = {
            sid: payload_names.get(sid) or stream_name_map.get(sid)
            for sid in current_streams
        }
        return {sid: {"stream_name": n} for sid, n in names.items() if n}

    def test_names_come_from_the_payload_when_it_carries_dicts(self):
        stats = self._stats_for(
            [1, 2],
            [{"id": 1, "name": "ESPN SD"}, {"id": 2, "name": "ESPN 4K"}],
            {},
        )
        assert _resolution_height_from_stats(stats[2]) == 2160

    def test_names_come_from_the_map_when_the_payload_is_ids_only(self):
        stats = self._stats_for([1, 2], [1, 2], {1: "ESPN SD", 2: "ESPN 4K"})
        assert _resolution_height_from_stats(stats[1]) == 480
        assert _resolution_height_from_stats(stats[2]) == 2160


class TestChannelOrdering:
    def test_unprobed_streams_order_best_definition_first(self):
        """The reported case: FS1 merged 12 copies and put 4K at position 3
        and SD near the bottom of the list purely by accident."""
        stats = {
            1: {"stream_name": "Fox Sports 1 SD"},
            2: {"stream_name": "US| FOX SPORTS 1 HD"},
            3: {"stream_name": "US: Fox Sports 1 4K"},
            4: {"stream_name": "US| FOX SPORTS 1 FHD"},
        }
        order = _sort_streams_by_resolution_height(
            [1, 2, 3, 4], stats, None, "desc", "FS1",
            quality_m3u_tie_break_enabled=False,
        )
        assert order == [3, 4, 2, 1]


EVENT_SYNC_CONFIG = {
    "master_group_id": 10,
    "secondary_group_ids": [20],
    "time_window_minutes": 30,
    "attach_threshold": 0.80,
    "enabled": True,
}


def _quality_rule():
    rule = MagicMock()
    rule.id = 9
    rule.name = "Event Rule"
    rule.priority = 0
    rule.enabled = True
    rule.get_event_sync_config.return_value = dict(EVENT_SYNC_CONFIG)
    rule.is_event_sync.return_value = True
    rule.stream_sort_field = "quality"
    rule.stream_sort_order = "desc"
    rule.quality_tie_break_order = "desc"
    rule.quality_m3u_tie_break_enabled = True
    rule.get_actions.return_value = []
    rule.get_normalization_group_ids.return_value = []
    rule.get_conditions.return_value = []
    return rule


def _attach_summary(rule):
    """What ``execute_event_sync_rule`` hands back for one attached stream."""
    return {
        "rule_id": rule.id,
        "rule_name": rule.name,
        "master_group_id": 10,
        "secondary_group_ids": [20],
        "secondary_streams": 1,
        "master_channels": 1,
        "master_channels_unparsed": 0,
        "attached": 1,
        "already_attached": 0,
        "ambiguous_skipped": 0,
        "contested_skipped": 0,
        "unmatched": 0,
        "parse_failed": 0,
        "attach_errors": 0,
        "cap": 100,
        "capped": False,
        "cap_overage": 0,
        "attach_entries": [],
        "queue_attached": 0,
        "rejected_suppressed": 0,
        "review_candidates": [],
    }


class TestAnEventSyncOnlyRunReachesTheSortWithNames:
    """Drives ``_process_streams`` so the whole chain is under test: the run
    carries only event_sync rules, so Pass 1 fetches no streams and nothing
    seeds the name map up front. The attach phase's own two stream reads are
    the only source of names, and the master channel's payload is bare ids,
    which is the shape Dispatcharr returns for ``/api/channels``.

    Asserting on ``update_channel`` rather than on the map keeps the engine
    the thing under test: with no names every stream scores 0, the order does
    not change, and Pass 3.5 issues no write at all.
    """

    MASTER_CHANNEL_ID = 100
    NATIVE_STREAM_ID = 1001
    ATTACHED_STREAM_ID = 5001

    def _channel(self, stream_order):
        """Master 100 holding both streams as BARE IDS."""
        return {
            "id": self.MASTER_CHANNEL_ID,
            "name": "Fox Sports 1",
            "channel_group_id": 10,
            "auto_created": True,
            "streams": list(stream_order),
        }

    def _run_pipeline_pass(self, secondary_name, native_name, stream_order,
                           catalogue_streams):
        """Run the event_sync attach phase plus Pass 3.5 and hand back the
        client so the caller can read what was written.

        ``catalogue_streams`` is what Pass 1 fetched: empty on an
        event-sync-only run, one entry on a mixed run.
        """
        rule = _quality_rule()
        channel = self._channel(stream_order)

        client = MagicMock()
        client.update_channel = AsyncMock(return_value={})
        client.get_channels = AsyncMock(return_value={"count": 0, "results": []})
        client.get_m3u_accounts = AsyncMock(return_value=[])
        client.get_all_m3u_group_settings = AsyncMock(return_value={})
        # The masters' own streams, resolved by the one bulk read the attach
        # phase already makes for provider coverage.
        client.get_streams_by_ids = AsyncMock(return_value=[
            {
                "id": self.NATIVE_STREAM_ID,
                "name": native_name,
                "m3u_account": 1,
            },
        ])

        engine = ChannelPipelineEngine(client)
        engine._existing_channels = [channel]
        engine._existing_groups = []
        engine._refresh_dummy_epg_and_retry = AsyncMock()
        engine._reconcile_orphans = AsyncMock()
        engine._update_rule_stats = AsyncMock()
        engine._fetch_event_sync_secondary_streams = AsyncMock(return_value=[
            # Same provider as the native stream, so the quality sort's M3U
            # tie-break cannot separate the two and only a name can.
            SecondaryStream(
                name=secondary_name, group_id=20,
                stream_id=self.ATTACHED_STREAM_ID, provider="ProvA",
                provider_id=1,
            ),
        ])

        async def _attach(rule_id, rule_name, config, secondary_streams,
                          exec_ctx, **kwargs):
            exec_ctx.merged_channel_ids.add(self.MASTER_CHANNEL_ID)
            return _attach_summary(rule)

        with patch("channel_pipeline_engine.ActionExecutor") as executor_cls, \
                patch("channel_pipeline_engine.get_session",
                      return_value=MagicMock()):
            executor = MagicMock()
            executor.execute_event_sync_rule = AsyncMock(side_effect=_attach)
            executor.verify_epg_assignments = AsyncMock(return_value=(0, 0, 0))
            executor.prune_merge_streams = AsyncMock()
            executor._channel_by_id = {self.MASTER_CHANNEL_ID: channel}
            executor._created_channels = {}
            executor_cls.return_value = executor

            asyncio.get_event_loop().run_until_complete(
                engine._process_streams(
                    catalogue_streams, [], MagicMock(id=1), dry_run=False,
                    triggered_by="manual", event_sync_rules=[rule],
                )
            )
        return client

    def test_the_master_is_reordered_best_definition_first(self):
        """The 4K stream the run just attached is written ahead of the SD one
        the channel already held. Both names arrive only via the attach
        phase's own reads."""
        client = self._run_pipeline_pass(
            secondary_name="US: Fox Sports 1 4K",
            native_name="Fox Sports 1 SD",
            stream_order=[self.NATIVE_STREAM_ID, self.ATTACHED_STREAM_ID],
            catalogue_streams=[],
        )
        client.update_channel.assert_awaited_once_with(
            self.MASTER_CHANNEL_ID,
            {"streams": [self.ATTACHED_STREAM_ID, self.NATIVE_STREAM_ID]},
        )

    def test_a_mixed_run_keeps_the_name_pass_one_already_fetched(self):
        """When standard rules ran too, Pass 1 named the whole catalogue
        first and the attach phase must not overwrite that. The catalogue
        calls 5001 SD while the secondary group's copy of it says 4K, so the
        order that gets written is the one the catalogue name implies."""
        client = self._run_pipeline_pass(
            secondary_name="US: Fox Sports 1 4K",
            native_name="US| FOX SPORTS 1 FHD",
            stream_order=[self.ATTACHED_STREAM_ID, self.NATIVE_STREAM_ID],
            catalogue_streams=[
                StreamContext.from_dispatcharr_stream(
                    {"id": self.ATTACHED_STREAM_ID,
                     "name": "Fox Sports 1 SD"},
                    m3u_account_id=1,
                ),
            ],
        )
        client.update_channel.assert_awaited_once_with(
            self.MASTER_CHANNEL_ID,
            {"streams": [self.NATIVE_STREAM_ID, self.ATTACHED_STREAM_ID]},
        )


class TestTheNameFillKeysOffItsOwnMap:
    """A caller that wants names and has no provider ids to collect passes
    ``stream_name_map`` and leaves ``stream_m3u_map`` at its default. The
    secondary fetch's names still have to arrive: the two maps answer
    different questions and one's absence says nothing about the other.
    """

    SECONDARY_STREAM_ID = 5001
    SECONDARY_NAME = "US: Fox Sports 1 4K"

    def test_the_secondary_names_arrive_with_no_m3u_map(self):
        rule = _quality_rule()

        client = MagicMock()
        client.get_m3u_accounts = AsyncMock(return_value=[])
        client.get_all_m3u_group_settings = AsyncMock(return_value={})

        engine = ChannelPipelineEngine(client)
        engine._fetch_event_sync_secondary_streams = AsyncMock(return_value=[
            SecondaryStream(
                name=self.SECONDARY_NAME, group_id=20,
                stream_id=self.SECONDARY_STREAM_ID, provider="ProvA",
                provider_id=1,
            ),
        ])

        executor = MagicMock()
        executor.execute_event_sync_rule = AsyncMock(
            return_value=_attach_summary(rule)
        )
        executor._channel_by_id = {}

        results = {
            "execution_log": [],
            "streams_merged": 0,
            "streams_skipped": 0,
            "modified_entities": [],
            "rule_match_counts": {},
        }
        stream_name_map = {}

        with patch("channel_pipeline_engine.get_session",
                   return_value=MagicMock()):
            asyncio.get_event_loop().run_until_complete(
                engine._run_event_sync_rules(
                    [rule], executor, results, False, "manual", set(),
                    rule_channel_order_streams={},
                    stream_name_map=stream_name_map,
                )
            )

        assert stream_name_map == {self.SECONDARY_STREAM_ID:
                                   self.SECONDARY_NAME}
