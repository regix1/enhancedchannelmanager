"""The ``stream_is_stale`` condition (dead streams merged onto channels).

Dispatcharr keeps a stream row after the provider stops listing it, flagged
``is_stale``. Nothing in the condition set could see that flag, so a rule
merging every provider's copy of a network kept attaching feeds the provider
had already dropped — they land on the channel as fallbacks that cannot play.
"""
from channel_pipeline_evaluator import ConditionEvaluator, StreamContext
from channel_pipeline_schema import Condition


def _ctx(**kw) -> StreamContext:
    base = {"stream_id": 1, "stream_name": "US| CBS 9 OKLAHOMA CITY OK (KWTV)"}
    return StreamContext(**{**base, **kw})


class TestFlagIsCarried:
    def test_the_flag_is_read_off_the_dispatcharr_stream(self):
        ctx = StreamContext.from_dispatcharr_stream(
            {"id": 5, "name": "US : CBS (KWTV  Oklahoma City OK)", "is_stale": True}
        )
        assert ctx.is_stale is True

    def test_a_stream_the_provider_still_lists_is_not_stale(self):
        ctx = StreamContext.from_dispatcharr_stream(
            {"id": 5, "name": "US| CBS 9 OKLAHOMA CITY OK (KWTV)", "is_stale": False}
        )
        assert ctx.is_stale is False

    def test_a_payload_without_the_key_is_treated_as_current(self):
        """A provider or endpoint that omits is_stale must not make every
        stream look dead."""
        ctx = StreamContext.from_dispatcharr_stream({"id": 5, "name": "ESPN"})
        assert ctx.is_stale is False


class TestCondition:
    def setup_method(self):
        self.ev = ConditionEvaluator()

    def test_it_matches_a_stale_stream(self):
        result = self.ev.evaluate(
            Condition(type="stream_is_stale"), _ctx(is_stale=True)
        )
        assert result.matched is True

    def test_it_does_not_match_a_current_stream(self):
        result = self.ev.evaluate(
            Condition(type="stream_is_stale"), _ctx(is_stale=False)
        )
        assert result.matched is False

    def test_negated_it_admits_only_streams_the_provider_still_carries(self):
        """This is how a rule uses it: a negated guard alongside the name
        match, so a dropped feed is never merged in the first place."""
        cond = Condition(type="stream_is_stale", negate=True)
        assert self.ev.evaluate(cond, _ctx(is_stale=False)).matched is True
        assert self.ev.evaluate(cond, _ctx(is_stale=True)).matched is False

    def test_an_explicit_false_value_reads_as_not_stale(self):
        cond = Condition(type="stream_is_stale", value=False)
        assert self.ev.evaluate(cond, _ctx(is_stale=False)).matched is True
        assert self.ev.evaluate(cond, _ctx(is_stale=True)).matched is False
