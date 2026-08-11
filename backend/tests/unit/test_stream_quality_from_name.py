"""Ordering a channel's fallback streams when nothing has been probed.

A channel built by merging every provider's copy of a network holds a mix of
4K, FHD, HD and SD feeds. Only a probed stream has a measured resolution, and
scoring every unprobed one 0 left that order arbitrary, so an SD copy could
sit above a 4K one. The provider name already declares the tier and costs no
probe and no provider connection to read.
"""
from channel_pipeline_engine import (
    _resolution_height_from_stats,
    _sort_streams_by_resolution_height,
)


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
