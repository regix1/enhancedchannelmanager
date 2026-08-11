"""Where promoted event channels get numbered.

Promotion created channels with ``channel_number: "auto"``, which starts at 1
and takes the lowest free numbers. On a real lineup that put PPV event
channels at 1, 2 and 5, interleaved among the operator's locals and sports.
``promote_channel_number`` parks them past the real channels instead.
"""
from channel_pipeline_schema import validate_event_sync_config


def _config(**kw) -> dict:
    base = {
        "master_group_id": 1,
        "secondary_group_ids": [2],
        "promote_unmatched": True,
        "promote_target_group_id": 65,
    }
    base.update(kw)
    return base


class TestValidation:
    def test_a_range_is_accepted(self):
        assert validate_event_sync_config(
            _config(promote_channel_number="900-999")) == []

    def test_auto_is_accepted(self):
        assert validate_event_sync_config(
            _config(promote_channel_number="auto")) == []

    def test_a_positive_integer_is_accepted(self):
        assert validate_event_sync_config(
            _config(promote_channel_number=900)) == []

    def test_a_positive_integer_as_a_string_is_accepted(self):
        """The rule editor posts the box contents verbatim, so an operator who
        types 900 sends the string. _get_next_channel_number already parses
        that with int(spec)."""
        assert validate_event_sync_config(
            _config(promote_channel_number="900")) == []

    def test_a_zero_or_zero_padded_string_is_rejected(self):
        """int("007") is 7, so a padded entry would number somewhere the
        operator never typed."""
        for bad in ("0", "007"):
            errs = validate_event_sync_config(_config(promote_channel_number=bad))
            assert any("promote_channel_number" in e for e in errs), bad

    def test_absent_is_accepted_so_existing_rules_are_untouched(self):
        """Every rule saved before this key existed must keep validating, and
        keep its old numbering."""
        assert validate_event_sync_config(_config()) == []

    def test_a_malformed_range_is_rejected(self):
        errs = validate_event_sync_config(_config(promote_channel_number="900-"))
        assert any("promote_channel_number" in e for e in errs)

    def test_a_non_numeric_value_is_rejected(self):
        errs = validate_event_sync_config(_config(promote_channel_number="bottom"))
        assert any("promote_channel_number" in e for e in errs)

    def test_zero_and_negatives_are_rejected(self):
        for bad in (0, -5):
            errs = validate_event_sync_config(_config(promote_channel_number=bad))
            assert any("promote_channel_number" in e for e in errs), bad

    def test_a_bool_is_not_a_channel_number(self):
        """True is an int in Python; it is not a channel number."""
        errs = validate_event_sync_config(_config(promote_channel_number=True))
        assert any("promote_channel_number" in e for e in errs)


class TestRangeResolution:
    """The value feeds a create_channel action, so it resolves through the
    same _get_next_channel_number the action uses."""

    def _executor(self):
        from unittest.mock import MagicMock

        from channel_pipeline_executor import ActionExecutor

        return ActionExecutor(MagicMock())

    def test_a_range_starts_at_its_low_end(self):
        ex = self._executor()
        assert ex._get_next_channel_number("900-999") == 900

    def test_a_range_skips_numbers_already_taken(self):
        ex = self._executor()
        ex._used_channel_numbers = {900, 901}
        assert ex._get_next_channel_number("900-999") == 902

    def test_auto_still_starts_at_one(self):
        """The default must keep its old behavior for rules that do not set
        the key."""
        assert self._executor()._get_next_channel_number("auto") == 1
