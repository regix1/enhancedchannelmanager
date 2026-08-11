"""The channel-number prefix on a stored name, in the executor's adoption
indexes and in the rename it performs.

``ActionExecutor._apply_channel_number_in_name`` renders a non-integer
channel number verbatim, so an ATSC sub-channel is stored
``"2.1 | ABC: WBAY Green Bay"``. ``ConditionEvaluator`` resolves that name
to its unprefixed spelling, so the executor's indexes have to resolve it
the same way: when they disagree the lookup misses, the rule creates a
second channel for one that already exists, and the original leaves the
rule's managed set. The rename path has to see the same prefix, or a
normalization rename writes the name back without the channel number.
"""
from unittest.mock import AsyncMock, MagicMock
import asyncio

from channel_pipeline_evaluator import StreamContext
from channel_pipeline_executor import ActionExecutor, ExecutionContext


class TestAdoptionIndexDecimalChannelNumberPrefix:
    """The base-name and lookup indexes built at construction. [48]"""

    GROUP = 65

    def _executor(self, channels, settings=None):
        return ActionExecutor(MagicMock(), existing_channels=channels,
                              existing_groups=[], settings=settings)

    def _channel(self, name):
        return {"id": 2462, "name": name, "channel_group_id": self.GROUP,
                "streams": [], "auto_created": True}

    def test_a_decimal_pipe_prefix_is_indexed_under_its_base_name(self):
        """The live configuration: numbering is off, so the executor is
        built without settings and only the unconditional pipe pass runs.
        The name still carries the prefix an earlier setting wrote."""
        executor = self._executor([self._channel("2.1 | ABC: WBAY Green Bay")])
        assert executor._base_name_to_channel.get(
            "abc: wbay green bay") is not None

    def test_a_decimal_pipe_prefix_is_found_by_the_name_a_rule_derives(self):
        """The consequence the index exists for: a rule deriving
        "ABC: WBAY Green Bay" adopts channel 2462 instead of creating a
        duplicate beside it."""
        executor = self._executor([self._channel("2.1 | ABC: WBAY Green Bay")])
        found = executor._find_channel_by_name("ABC: WBAY Green Bay",
                                               block_manual=False)
        assert found is not None
        assert found["id"] == 2462

    def test_an_integer_pipe_prefix_still_resolves(self):
        """The form that has always worked keeps working."""
        executor = self._executor([self._channel("500 | USA Network")])
        assert executor._base_name_to_channel.get("usa network") is not None

    def test_a_decimal_dash_prefix_resolves_when_the_separator_is_set(self):
        """The separator the settings write strips the dash form."""
        settings = MagicMock(include_channel_number_in_name=True,
                             channel_number_separator="-")
        executor = self._executor([self._channel("4000.1 - USA Network")],
                                  settings=settings)
        assert executor._base_name_to_channel.get("usa network") is not None

    def test_a_name_carrying_no_prefix_is_left_whole(self):
        """A title opening with a pipe that is punctuation, not a prefix,
        keeps its own spelling and is not indexed under a shortened one."""
        executor = self._executor([self._channel("ABC | WBAY Green Bay")])
        assert executor._base_name_to_channel.get("wbay green bay") is None


class TestRenameKeepsTheChannelNumberPrefix:
    """The normalization rename at the create_channel action. [49]"""

    def _engine(self, mapping):
        """Normalization engine that maps names via ``mapping``."""
        engine = MagicMock()

        def _normalize(name, *args, **kwargs):
            result = MagicMock()
            result.normalized = mapping.get(name, name)
            result.transformations = []
            return result

        engine.normalize.side_effect = _normalize
        engine.extract_core_name.side_effect = lambda n: mapping.get(n, n)
        engine.extract_call_sign.return_value = None
        return engine

    def _rename(self, stored_name):
        """Run create_channel over a stream whose normalized name differs
        from the stored one, and return the name the rename wrote."""
        client = MagicMock()
        client.update_channel = AsyncMock(return_value={})
        client.create_channel = AsyncMock()

        existing = [{"id": 7, "name": stored_name, "streams": [],
                     "channel_group_id": 5, "auto_created": True}]
        engine = self._engine({"RTL ᴿᴬᵂ": "RTL", "RTL": "RTL"})
        executor = ActionExecutor(client, existing_channels=existing,
                                  normalization_engine=engine)

        stream_ctx = StreamContext(
            stream_id=77,
            stream_name="RTL ᴿᴬᵂ",
            m3u_account_id=1,
            m3u_account_name="Provider",
            group_name="DE",
            tvg_id=None,
            resolution_height=None,
            logo_url=None,
        )
        action = {"type": "create_channel", "name_template": "{stream_name}",
                  "if_exists": "merge"}

        result = asyncio.get_event_loop().run_until_complete(
            executor.execute(action, stream_ctx, ExecutionContext(),
                             normalization_group_ids=[1])
        )
        assert result.success is True
        client.create_channel.assert_not_called()
        renames = [c for c in client.update_channel.call_args_list
                   if c[0][0] == 7 and "name" in c[0][1]]
        assert len(renames) >= 1
        return renames[0][0][1]["name"]

    def test_a_decimal_prefix_survives_the_rename(self):
        """The channel number stays in the name. Before, the prefix was
        dropped and the channel was left named "RTL"."""
        assert self._rename("2.1 | RTL ᴿᴬᵂ") == "2.1 | RTL"

    def test_an_integer_prefix_still_survives_the_rename(self):
        assert self._rename("107 | RTL ᴿᴬᵂ") == "107 | RTL"

    def test_a_name_with_no_prefix_is_renamed_whole(self):
        assert self._rename("RTL ᴿᴬᵂ") == "RTL"

    def test_a_trailing_space_leaves_the_prefix_and_core_reconstructing_the_name(self):
        """The invariant the rename rests on: the preserved prefix plus the
        stripped core must spell the stored name exactly. The helper strips
        its own result, so a stored name with trailing whitespace measures
        one character short and the first character of the name is counted
        as part of the prefix, giving "107 | RRTL"."""
        assert self._rename("107 | RTL ᴿᴬᵂ ") == "107 | RTL"

    def test_a_decimal_prefix_survives_a_trailing_space_too(self):
        assert self._rename("2.1 | RTL ᴿᴬᵂ ") == "2.1 | RTL"
