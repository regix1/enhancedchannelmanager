"""
The per-group name sets and the channel-number prefix on a stored name.

``ActionExecutor._apply_channel_number_in_name`` renders a non-integer
channel number verbatim, so an ATSC sub-channel is stored
``"2.1 | ABC: WBAY Green Bay"``. The pipe pass in ``ConditionEvaluator`` is
unconditional because a name may carry a prefix written under an earlier
setting, and it has to see through the decimal form as well, or a name
condition reports an existing channel missing and the rule creates a
duplicate.
"""
from channel_pipeline_evaluator import ConditionEvaluator, StreamContext


class TestConditionEvaluatorDecimalChannelNumberPrefix:
    """A decimal channel number in front of a stored name."""

    GROUP = 65

    def _matches(self, channels, stream_name, separator):
        evaluator = ConditionEvaluator(existing_channels=channels,
                                       channel_number_separator=separator)
        ctx = StreamContext(stream_id=1, stream_name=stream_name)
        return evaluator.evaluate(
            {"type": "normalized_name_in_group", "value": self.GROUP}, ctx
        ).matched

    def test_a_decimal_pipe_prefix_is_seen_through_with_numbering_off(self):
        """The live configuration: nothing writes a prefix today, so the
        evaluator is handed None and only the unconditional pipe pass runs.
        The name still carries the prefix an earlier setting wrote."""
        channels = [{"id": 2462, "name": "2.1 | ABC: WBAY Green Bay",
                     "channel_group_id": self.GROUP}]
        assert self._matches(channels, "ABC: WBAY Green Bay", None) is True

    def test_a_decimal_pipe_prefix_is_seen_through_under_another_separator(self):
        """The configured separator strips the dash form only, so a name
        still carrying the older pipe form depends on the pipe pass."""
        channels = [{"id": 2462, "name": "2.1 | ABC: WBAY Green Bay",
                     "channel_group_id": self.GROUP}]
        assert self._matches(channels, "ABC: WBAY Green Bay", "-") is True

    def test_the_dash_form_matches_when_the_separator_is_set(self):
        """The separator the settings write is what strips the dash."""
        channels = [{"id": 100, "name": "500 - Fury Vs Usyk",
                     "channel_group_id": self.GROUP}]
        assert self._matches(channels, "Fury Vs Usyk", "-") is True

    def test_a_decimal_dash_prefix_matches_when_the_separator_is_set(self):
        channels = [{"id": 100, "name": "4000.1 - USA Network",
                     "channel_group_id": self.GROUP}]
        assert self._matches(channels, "USA Network", "-") is True

    def test_a_name_carrying_no_prefix_is_left_whole(self):
        channels = [{"id": 100, "name": "ABC: WBAY Green Bay",
                     "channel_group_id": self.GROUP}]
        assert self._matches(channels, "ABC: WBAY Green Bay", None) is True

    def test_a_separator_later_in_the_title_is_not_a_prefix(self):
        """The pipe here is punctuation inside the title, and the name does
        not open with a number, so nothing may be stripped off the front."""
        channels = [{"id": 100, "name": "ABC | WBAY Green Bay",
                     "channel_group_id": self.GROUP}]
        assert self._matches(channels, "WBAY Green Bay", None) is False

    def test_a_name_that_is_nothing_but_a_prefix_keeps_its_own_spelling(self):
        """Stripping would leave an empty string, which is never a usable
        lookup key, so the stored name stays the only spelling."""
        channels = [{"id": 100, "name": "2.1 | ",
                     "channel_group_id": self.GROUP}]
        assert self._matches(channels, "2.1 | ", None) is True
