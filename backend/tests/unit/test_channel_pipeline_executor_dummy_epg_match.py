"""Dummy-guide row matching in ``_match_epg_data`` (bead l76).

Dummy EPG profiles key their guide rows ``ecm-<channel id>``, and a channel id
is never reissued. A recreated event channel therefore carries the same NAME
as the deleted channel whose row is still in the source, and the name tier
linked the new channel to that dead row — no programmes, while the run
reported it assigned.

Field incident (run #40): channel 2408 'Mlb On Tbs Mariners At Yankees @ Aug
11 06:30 PM' was linked to epg_data_id=4956333, whose tvg_id is ecm-2399 —
the channel deleted minutes earlier.
"""
from unittest.mock import MagicMock

from channel_pipeline_executor import ActionExecutor


def make_executor() -> ActionExecutor:
    """Bare executor — _match_epg_data uses no instance state beyond logging."""
    return ActionExecutor(MagicMock())


EVENT_NAME = "Mlb On Tbs Mariners At Yankees @ Aug 11 06:30 PM"

STALE_ROW = {"id": 4956333, "tvg_id": "ecm-2399", "name": EVENT_NAME}
OWN_ROW = {"id": 4956356, "tvg_id": "ecm-2408", "name": EVENT_NAME}

RECREATED_CHANNEL = {"id": 2408, "name": EVENT_NAME, "tvg_id": None}


class TestOwnDummyRow:
    def setup_method(self):
        self.executor = make_executor()

    def test_own_row_wins_over_a_stale_row_with_the_same_name(self):
        result = self.executor._match_epg_data(
            RECREATED_CHANNEL, [STALE_ROW, OWN_ROW]
        )
        assert result is OWN_ROW

    def test_a_stale_row_alone_is_no_match(self):
        """The run-#40 incident: only the deleted channel's row exists.

        No match is what lets the caller defer to Pass 5, which regenerates
        the guide with this channel's own row and retries.
        """
        result = self.executor._match_epg_data(RECREATED_CHANNEL, [STALE_ROW])
        assert result is None

    def test_a_single_stale_row_does_not_win_via_the_single_entry_fallback(self):
        """A one-channel profile reduces the source to exactly one entry,
        which the tail fallback would otherwise return unconditionally."""
        channel = {"id": 51, "name": "###", "tvg_id": None}  # name normalizes empty
        result = self.executor._match_epg_data(
            channel, [{"id": 900, "tvg_id": "ecm-50", "name": "###"}]
        )
        assert result is None

    def test_explicit_channel_tvg_id_stays_authoritative(self):
        """Tier 1 (channel.tvg_id == entry.tvg_id) still outranks the own-row
        tier — an operator-set tvg_id is the requested row."""
        channel = {"id": 2408, "name": EVENT_NAME, "tvg_id": "ecm-2399"}
        result = self.executor._match_epg_data(channel, [STALE_ROW, OWN_ROW])
        assert result is STALE_ROW


class TestNonDummySourcesUnchanged:
    def setup_method(self):
        self.executor = make_executor()

    def test_name_matching_still_works_for_a_channel_with_an_id(self):
        channel = {"id": 7, "name": "ESPN", "tvg_id": None}
        espn = {"id": 21, "tvg_id": "ESPN.us", "name": "ESPN"}
        result = self.executor._match_epg_data(channel, [espn])
        assert result is espn

    def test_a_channel_without_an_id_keeps_the_old_name_match(self):
        """With no channel id there is no own-row key, so ecm rows stay
        ordinary name candidates — the pre-l76 behavior."""
        channel = {"name": EVENT_NAME, "tvg_id": None}
        result = self.executor._match_epg_data(channel, [STALE_ROW])
        assert result is STALE_ROW
