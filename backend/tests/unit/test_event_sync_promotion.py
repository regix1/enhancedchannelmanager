"""Event Sync unmatched-stream promotion (bead ti939.4.1, epic ti939 Phase 3).

The ONE sanctioned exception to "ECM never creates channels", exercised
end to end against the stateful fixture Dispatcharr:

* **AC-1 flag-off regression** — a rule without ``promote_unmatched`` is
  byte-identical everywhere: no ``promotion`` key on the summary, no
  channel creation, Pass 4 stays hard-bypassed (``managed_channel_ids``
  never populated).
* **AC-2 live promotion** — each complete-identity unmatched stream gets
  a channel in the target group, its stream attached, and a journal row
  with ``kind="event_sync_promote"`` content-fingerprint provenance.
* **AC-3 idempotence** — an immediate re-run creates zero channels and
  attaches zero duplicates; same-run cross-provider streams sharing an
  event key share ONE channel.
* **Dateless streams never promote** — a synthesized-date parse names a
  recurring slot rather than one broadcast, so it is diverted out of the
  plan whatever its action, counted as ``skipped_dateless``, and any
  channel it already has is kept rather than retired.
* **AC-5 reconciliation lifecycle** — stream gone from the playlist →
  next run's Pass 4 deletes the promoted channel per orphan_action;
  masters are provably never in the managed set; first-run-populate
  protection holds; a fetch-failure run makes NO delete observation.
* **AC-6 self-healing** — the event appearing in the master group
  attaches the stream to the master AND reconciles the promoted
  duplicate away in the SAME run.
* **AC-7 cap overage** — creation stops, WARNs, and a
  ``event_sync_promote_capped`` entry lands in event_sync_warnings.
* **AC-10 rollback** — a promotion run rolls back through the standard
  snapshot path: created channels deleted, master stream lists restored.
* **AC-11 exclusion interaction (pinned)** — an ``excluded_by_operator``
  stream still promotes: exclusions block ATTACH to a specific master,
  not promotion.
* **Renumber no-op** — Pass 4 cleanup for an event_sync rule never
  triggers channel renumbering (no create_channel action → no starting
  number), verified rather than built.

Plan-helper unit tests (clustering, naming, cap determinism) live at the
top; they are the same helper the preview endpoint calls, so dry-run
parity is by construction (and re-checked in
tests/routers/test_event_sync_preview.py).
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytz
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import database
from channel_pipeline_engine import ChannelPipelineEngine
from channel_pipeline_executor import ActionExecutor, ActionResult
from models import ChannelPipelineRule, PendingMerge
from services.event_sync_matcher import (
    ParsedEvent,
    StreamMatchResult,
    parse_event_name,
)
from channel_number_prefix import (
    channel_name_to_id,
    strip_channel_number_prefix,
)
from services.event_sync_promote import (
    DEFAULT_MAX_PROMOTE_PER_RUN,
    MAX_CLUSTER_WINDOW_MINUTES,
    PROMOTE_ACTION_ATTACH_EXISTING,
    PROMOTE_ACTION_CREATE,
    PromotionPlan,
    PromotionUnit,
    build_promotion_plan,
    event_has_started,
    event_is_early,
    event_is_past,
    promoted_channel_name,
)
from services.event_sync_resolver import (
    DISPOSITION_AMBIGUOUS,
    DISPOSITION_EXCLUDED,
    DISPOSITION_PARSE_FAILED,
    DISPOSITION_UNMATCHED,
    ResolvedStream,
    SecondaryStream,
)
from services.event_sync_review import master_event_key
from tests.event_sync_fixtures import (
    FakeDispatcharrState,
    GROUP_NAMES,
    MASTER_GROUP_ID,
    SECONDARY_A,
    SECONDARY_B,
    assert_never_touched_group_settings,
    event_sync_config,
    make_promote_client,
)

EASTERN = pytz.timezone("America/New_York")
FROZEN_NOW = EASTERN.localize(datetime(2026, 7, 11, 12, 0, 0))

PROMOTE_GROUP_ID = 40

MASTER_MERCURY = "Peacock 14: Mercury vs. Aces @ 11 Jul 06:00 PM ET"
STREAM_MERCURY = "WNBA TV 01: Mercury vs. Aces @ 11 Jul 06:00 PM ET"
STREAM_FURY = "DAZN 05: Fury vs. Usyk @ 11 Jul 11:00 PM ET"
STREAM_FURY_ALT = "FightBox 02: Fury vs. Usyk @ 11 Jul 11:00 PM ET"
MASTER_FURY = "Peacock 99: Fury vs. Usyk @ 11 Jul 11:00 PM ET"
STREAM_TYSON = "DAZN 06: Tyson vs. Paul @ 11 Jul 09:00 PM ET"

SECONDARY_A_NAME = GROUP_NAMES[SECONDARY_A]
SECONDARY_B_NAME = GROUP_NAMES[SECONDARY_B]


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _clock(value):
    class ClockType(type):
        def __instancecheck__(cls, instance):
            return isinstance(instance, datetime)

    class Clock(metaclass=ClockType):
        min = datetime.min
        fromisoformat = datetime.fromisoformat

        @classmethod
        def now(cls, tz=None):
            current = value() if callable(value) else value
            if tz is None:
                return current.replace(tzinfo=None) if current.tzinfo else current
            return current.astimezone(tz)

    return Clock


# =========================================================================
# Plan-helper unit tests (pure — the same helper preview + run call).
# =========================================================================


def _parsed(title, start, matched_pattern="slot-title-at-datetime"):
    return ParsedEvent(
        raw_name=f"{title} raw", title=title, start=start, teams=None,
        matched_pattern=matched_pattern,
    )


def _resolved(name, disposition, parsed, provider_id=1, stream_id=1,
              group_id=SECONDARY_A, provider="Prov"):
    return ResolvedStream(
        stream=SecondaryStream(
            name=name, group_id=group_id, stream_id=stream_id,
            provider=provider, provider_id=provider_id,
        ),
        result=StreamMatchResult(stream_name=name, parsed=parsed),
        disposition=disposition,
        best=None,
    )


def _promote_config(**overrides):
    config = event_sync_config(
        secondary_group_ids=[SECONDARY_A, SECONDARY_B],
        promote_unmatched=True,
        promote_target_group_id=PROMOTE_GROUP_ID,
        max_promote_per_run=DEFAULT_MAX_PROMOTE_PER_RUN,
    )
    config.update(overrides)
    return config


START = EASTERN.localize(datetime(2026, 7, 11, 23, 0, 0))


class TestPromotionPlan:
    def test_cross_provider_streams_sharing_event_key_form_one_unit(self):
        """PO decision 2: exact-event-key clustering, any provider. The
        cleaner forgives case/whitespace (LOCALS mode), so two providers'
        spellings of the same title cluster."""
        rows = [
            _resolved(STREAM_FURY, DISPOSITION_UNMATCHED,
                      _parsed("Fury vs. Usyk", START),
                      provider_id=2, stream_id=301),
            _resolved(STREAM_FURY_ALT, DISPOSITION_UNMATCHED,
                      _parsed("FURY  VS.  USYK", START),
                      provider_id=3, stream_id=555),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert len(plan.units) == 1
        unit = plan.units[0]
        assert len(unit.rows) == 2
        assert unit.action == PROMOTE_ACTION_CREATE
        # Deterministic within-unit order: (provider_id, stream_id).
        assert [r.stream.stream_id for r in unit.rows] == [301, 555]

    def test_one_event_under_three_status_prefixes_forms_one_unit(self):
        """The provider re-issues an event under a new stream id whenever
        its status changes and leads the name with that status, so one golf
        show was minting three units and holding channels 900, 902 and 905
        at the same time. The derived channel name drops the status too, so
        the run adopts one channel instead of naming three. [18]"""
        rows = [
            _resolved("NEXT | FURY VS USYK", DISPOSITION_UNMATCHED,
                      _parsed("NEXT | Fury vs. Usyk", START),
                      provider_id=2, stream_id=301),
            _resolved("LIVE | FURY VS USYK", DISPOSITION_UNMATCHED,
                      _parsed("LIVE | Fury vs. Usyk", START),
                      provider_id=2, stream_id=302),
            _resolved("ENDED | FURY VS USYK", DISPOSITION_UNMATCHED,
                      _parsed("ENDED | Fury vs. Usyk", START),
                      provider_id=2, stream_id=303),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert len(plan.units) == 1
        assert [r.stream.stream_id for r in plan.units[0].rows] \
            == [301, 302, 303]
        assert plan.units[0].channel_name == FURY_CHANNEL_NAME

    def test_clustering_is_exact_key_no_fuzzy(self):
        """PO decision 2, the other half: EXACT key only. 'vs.' and 'vs'
        survive the cleaner as distinct titles, so they mint two units —
        promotion never fuzzy-clusters."""
        rows = [
            _resolved(STREAM_FURY, DISPOSITION_UNMATCHED,
                      _parsed("Fury vs. Usyk", START), stream_id=301),
            _resolved(STREAM_FURY_ALT, DISPOSITION_UNMATCHED,
                      _parsed("Fury vs Usyk", START), stream_id=555),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert len(plan.units) == 2

    def test_distinct_event_keys_form_distinct_units_sorted_by_key(self):
        rows = [
            _resolved(STREAM_TYSON, DISPOSITION_UNMATCHED,
                      _parsed("Tyson vs. Paul",
                              EASTERN.localize(datetime(2026, 7, 11, 21, 0))),
                      stream_id=302),
            _resolved(STREAM_FURY, DISPOSITION_UNMATCHED,
                      _parsed("Fury vs. Usyk", START), stream_id=301),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert len(plan.units) == 2
        assert [u.event_key for u in plan.units] == sorted(
            u.event_key for u in plan.units
        )

    def test_only_unmatched_and_excluded_are_promotable(self):
        """AC-11 pin: excluded_by_operator IS promotable (exclusions block
        the attach to one master, not promotion); ambiguous and
        parse_failed are NOT."""
        rows = [
            _resolved("a", DISPOSITION_UNMATCHED,
                      _parsed("Event A", START), stream_id=1),
            _resolved("b", DISPOSITION_EXCLUDED,
                      _parsed("Event B", START), stream_id=2),
            _resolved("c", DISPOSITION_AMBIGUOUS,
                      _parsed("Event C", START), stream_id=3),
            _resolved("d", DISPOSITION_PARSE_FAILED,
                      ParsedEvent(raw_name="d", title=None, start=None,
                                  teams=None, matched_pattern=None),
                      stream_id=4),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        promoted_titles = {
            u.rows[0].result.parsed.title for u in plan.units
        }
        assert promoted_titles == {"Event A", "Event B"}

    def test_incomplete_identity_is_not_promotable(self):
        """A parsed title with no start (or vice versa) has no event key —
        it can neither name a channel nor be recognized next run."""
        rows = [
            _resolved("x", DISPOSITION_UNMATCHED,
                      ParsedEvent(raw_name="x", title="Some Event",
                                  start=None, teams=None,
                                  matched_pattern=None)),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert plan.units == ()

    def test_existing_name_in_target_group_plans_adoption(self):
        rows = [_resolved(STREAM_FURY, DISPOSITION_UNMATCHED,
                          _parsed("Fury vs. Usyk", START), stream_id=301)]
        name = promoted_channel_name(_parsed("Fury vs. Usyk", START))
        plan = build_promotion_plan(
            _promote_config(), rows, {name.lower(): 900}
        )
        assert plan.units[0].action == PROMOTE_ACTION_ATTACH_EXISTING
        assert plan.units[0].existing_channel_id == 900

    @pytest.mark.parametrize("separator", ["|", "-", ":"])
    def test_number_prefixed_existing_channel_is_still_adopted(
        self, separator
    ):
        """Dispatcharr stores the channel with the number prefix
        include_channel_number_in_name writes, and the plan derives the
        name without one. Miss that and the run creates a second channel
        for an event it already has — and with skip_past_events on, the
        first one loses its place in the managed set. The map the caller
        hands the planner is the one the run builds, so this covers the
        keying and the plan together."""
        parsed = _parsed("Fury vs. Usyk", START)
        rows = [_resolved(STREAM_FURY, DISPOSITION_UNMATCHED, parsed,
                          stream_id=301)]
        name = promoted_channel_name(parsed)
        stored = [{"id": 900, "name": f"500 {separator} {name}"}]
        plan = build_promotion_plan(
            _promote_config(), rows, channel_name_to_id(stored, separator),
        )
        assert plan.units[0].action == PROMOTE_ACTION_ATTACH_EXISTING
        assert plan.units[0].existing_channel_id == 900

    @pytest.mark.parametrize("separator", ["|", "-", ":"])
    def test_every_channel_number_separator_is_stripped(self, separator):
        """The no-argument form of the helper, which is what REWRITING a
        prefix uses: the prefix already on a name may have been written
        under a different setting than the one in force now."""
        assert strip_channel_number_prefix(
            f"500 {separator} USA Network"
        ) == "USA Network"

    def test_decimal_channel_number_is_stripped(self):
        assert strip_channel_number_prefix("4000.1 | USA Network") \
            == "USA Network"

    def test_name_without_a_prefix_comes_back_unchanged(self):
        """Including names that merely start with digits, and one that is
        nothing but a prefix (stripping that to empty would key the map on
        an empty string)."""
        assert strip_channel_number_prefix("USA Network") == "USA Network"
        assert strip_channel_number_prefix("500 Miles Of Racing") \
            == "500 Miles Of Racing"
        assert strip_channel_number_prefix("500 - ") == "500 - "

    def test_a_numeric_title_keeps_its_whole_name(self):
        """The map strips only the separator the settings write, and
        strips nothing at all when they write no prefix. A channel
        genuinely named "2024 - Olympics Opening" therefore keeps its
        whole name, instead of also answering to a spelling no channel
        has. [48]"""
        stored = [{"id": 900, "name": "2024 - Olympics Opening"}]
        assert channel_name_to_id(stored, None) == {
            "2024 - olympics opening": 900,
        }
        assert channel_name_to_id(stored, "|") == {
            "2024 - olympics opening": 900,
        }
        # With "-" configured the leading number IS the prefix shape ECM
        # writes, so both spellings key the channel.
        assert channel_name_to_id(stored, "-") == {
            "2024 - olympics opening": 900,
            "olympics opening": 900,
        }

    def test_lowest_id_wins_a_shared_key(self):
        stored = [
            {"id": 900, "name": "12 - Fury Vs Usyk"},
            {"id": 700, "name": "Fury Vs Usyk"},
        ]
        assert channel_name_to_id(stored, "-")["fury vs usyk"] == 700

    def test_cap_applies_to_creations_only_and_is_deterministic(self):
        cfg = _promote_config(max_promote_per_run=1)
        rows = [
            _resolved("a", DISPOSITION_UNMATCHED,
                      _parsed("Alpha Event", START), stream_id=1),
            _resolved("b", DISPOSITION_UNMATCHED,
                      _parsed("Beta Event", START), stream_id=2),
            _resolved("c", DISPOSITION_UNMATCHED,
                      _parsed("Gamma Event", START), stream_id=3),
        ]
        # 'Beta Event' already exists — adoption is cap-exempt.
        beta_name = promoted_channel_name(_parsed("Beta Event", START))
        plan = build_promotion_plan(cfg, rows, {beta_name.lower(): 901})
        assert plan.cap == 1
        assert plan.capped is True
        assert plan.cap_overage == 1
        realized = {u.rows[0].result.parsed.title: u.action
                    for u in plan.units}
        # Key order: alpha < beta < gamma — alpha takes the single create
        # slot, beta adopts (exempt), gamma is deferred.
        assert realized == {
            "Alpha Event": PROMOTE_ACTION_CREATE,
            "Beta Event": PROMOTE_ACTION_ATTACH_EXISTING,
        }
        assert [u.rows[0].result.parsed.title
                for u in plan.capped_units] == ["Gamma Event"]


class TestPromotedChannelName:
    def test_dated_name_carries_local_date_and_clock(self):
        name = promoted_channel_name(_parsed("Fury vs. Usyk", START))
        assert name == "Fury Vs. Usyk @ Jul 11 11:00 PM"

    def test_dateless_name_has_no_date_component(self):
        """AC-4 half 1: a synthesized-date parse must NEVER leak its
        fabricated date into the name."""
        parsed = _parsed(
            "Fury vs Hall", EASTERN.localize(datetime(2026, 7, 11, 18, 0)),
            matched_pattern="dateless-title-time-ampm",
        )
        name = promoted_channel_name(parsed)
        assert name == "Fury Vs Hall @ 06:00 PM"
        assert "11" not in name and "Jul" not in name

    def test_dateless_name_and_key_stable_across_midnight(self):
        """The identity layer is unchanged by the promotion rule: the same
        dateless slot parsed on two different days derives the SAME key and
        the SAME name, so nothing downstream can mint a duplicate."""
        day1 = _parsed(
            "Fury vs Hall", EASTERN.localize(datetime(2026, 7, 11, 18, 0)),
            matched_pattern="dateless-title-time-ampm",
        )
        day2 = _parsed(
            "Fury vs Hall", EASTERN.localize(datetime(2026, 7, 12, 18, 0)),
            matched_pattern="dateless-title-time-ampm",
        )
        assert master_event_key(day1) == master_event_key(day2)
        assert promoted_channel_name(day1) == promoted_channel_name(day2)
        # The plan recognizes the day-1 channel as this unit's own, then
        # diverts the unit because nothing can date it.
        rows = [_resolved("FURY vs HALL 6PM", DISPOSITION_UNMATCHED, day2)]
        plan = build_promotion_plan(
            _promote_config(),
            rows,
            {promoted_channel_name(day1).lower(): 902},
        )
        assert plan.units == ()
        assert plan.skipped_dateless_units[0].action \
            == PROMOTE_ACTION_ATTACH_EXISTING
        assert plan.skipped_dateless_units[0].existing_channel_id == 902


class TestSkipPastEvents:
    """``skip_past_events``: a finished event stops being managed.

    Providers leave a live event in the M3U long after it ends, so without
    this filter every finished game keeps minting a channel nobody can
    watch, and the one it already has never goes away. The filter drops
    the unit whatever its action is: nothing is created, and an event that
    already has a channel leaves the run's managed set, which hands that
    channel to Pass 4's ``orphan_action``. The guards below are what make
    a clock-driven delete safe — see the module docstring.
    """

    # FROZEN_NOW is 2026-07-11 12:00 ET. With the 4-hour default grace:
    # a start before 08:00 is past, 08:00 or later is still current.
    FINISHED = EASTERN.localize(datetime(2026, 7, 8, 20, 0))
    JUST_FINISHED = EASTERN.localize(datetime(2026, 7, 11, 6, 0))
    IN_PROGRESS = EASTERN.localize(datetime(2026, 7, 11, 10, 0))

    def _skip_config(self, **overrides):
        return _promote_config(skip_past_events=True, **overrides)

    def test_finished_event_is_not_created(self):
        rows = [_resolved("old", DISPOSITION_UNMATCHED,
                          _parsed("Fury vs. Usyk", self.FINISHED))]
        plan = build_promotion_plan(
            self._skip_config(), rows, {}, now=FROZEN_NOW
        )
        assert plan.units == ()
        assert plan.skipped_past == 1
        assert plan.skipped_past_units[0].channel_name.startswith("Fury")

    def test_event_inside_the_grace_window_is_still_created(self):
        """An event that started two hours ago is still on air — dropping
        it would kill the channel mid-broadcast."""
        rows = [_resolved("live", DISPOSITION_UNMATCHED,
                          _parsed("Mercury vs. Aces", self.IN_PROGRESS))]
        plan = build_promotion_plan(
            self._skip_config(), rows, {}, now=FROZEN_NOW
        )
        assert plan.would_create == 1
        assert plan.skipped_past == 0

    def test_grace_boundary_is_the_only_thing_separating_the_two(self):
        """Same start time, different grace: 4 hours keeps it, 0 drops it."""
        rows = [_resolved("edge", DISPOSITION_UNMATCHED,
                          _parsed("Edge Event", self.JUST_FINISHED))]
        kept = build_promotion_plan(
            self._skip_config(past_event_grace_hours=8), rows, {},
            now=FROZEN_NOW,
        )
        dropped = build_promotion_plan(
            self._skip_config(past_event_grace_hours=0), rows, {},
            now=FROZEN_NOW,
        )
        assert kept.would_create == 1 and kept.skipped_past == 0
        assert dropped.would_create == 0 and dropped.skipped_past == 1

    def test_future_event_is_created(self):
        rows = [_resolved(STREAM_FURY, DISPOSITION_UNMATCHED,
                          _parsed("Fury vs. Usyk", START))]
        plan = build_promotion_plan(
            self._skip_config(), rows, {}, now=FROZEN_NOW
        )
        assert plan.would_create == 1
        assert plan.skipped_past == 0

    def test_the_past_filter_never_judges_a_dateless_event(self):
        """The date on a synthesized parse was fabricated from "now", so
        past-vs-future says nothing about the event and the verdict would
        flip at midnight. Even a start that reads as long gone is never
        called finished; it leaves the plan as dateless instead."""
        parsed = _parsed(
            "Fury vs Hall", self.FINISHED,
            matched_pattern="dateless-title-time-ampm",
        )
        rows = [_resolved("FURY vs HALL 8PM", DISPOSITION_UNMATCHED, parsed)]
        plan = build_promotion_plan(
            self._skip_config(), rows, {}, now=FROZEN_NOW
        )
        assert plan.skipped_past == 0
        assert plan.skipped_dateless == 1
        assert plan.would_create == 0

    def test_finished_event_releases_its_existing_channel(self):
        """An adopt unit for a finished event is dropped too, so its
        channel is absent from the run's managed set and Pass 4 applies
        the rule's own orphan_action to it. skipped_past_adopted is the
        count of exactly those, because that is the destructive half."""
        parsed = _parsed("Fury vs. Usyk", self.FINISHED)
        rows = [_resolved("old", DISPOSITION_UNMATCHED, parsed)]
        name = promoted_channel_name(parsed)
        plan = build_promotion_plan(
            self._skip_config(), rows, {name.lower(): 900}, now=FROZEN_NOW
        )
        assert plan.units == ()
        assert plan.skipped_past == 1
        assert plan.skipped_past_adopted == 1
        dropped = plan.skipped_past_units[0]
        assert dropped.action == PROMOTE_ACTION_ATTACH_EXISTING
        assert dropped.existing_channel_id == 900

    def test_skipped_past_adopted_counts_only_the_ones_with_a_channel(self):
        """A finished event nobody promoted yet costs nothing to skip; one
        that already has a channel is about to lose it. The two are
        counted apart so the operator sees the second number on its own."""
        never_promoted = _parsed("Alpha Event", self.FINISHED)
        already_promoted = _parsed("Beta Event", self.FINISHED)
        rows = [
            _resolved("a", DISPOSITION_UNMATCHED, never_promoted, stream_id=1),
            _resolved("b", DISPOSITION_UNMATCHED, already_promoted,
                      stream_id=2),
        ]
        plan = build_promotion_plan(
            self._skip_config(), rows,
            {promoted_channel_name(already_promoted).lower(): 901},
            now=FROZEN_NOW,
        )
        assert plan.skipped_past == 2
        assert plan.skipped_past_adopted == 1

    def test_a_skipped_unit_with_no_channel_is_never_counted(self):
        """The count is how many channels the run is about to release, so
        it reads the channel id, not the action. A unit carries
        attach_existing whenever an earlier unit planned the same channel
        name, and such a unit may have no channel at all; counting it
        would warn the operator about a loss that cannot happen. [45]"""
        parsed = _parsed("Fury vs. Usyk", self.FINISHED)
        rows = (_resolved("a", DISPOSITION_UNMATCHED, parsed),)
        name = promoted_channel_name(parsed)
        plan = PromotionPlan(
            units=(),
            capped_units=(),
            cap=DEFAULT_MAX_PROMOTE_PER_RUN,
            target_group_id=40,
            skipped_past_units=(
                PromotionUnit(
                    event_key="fury vs. usyk|a", channel_name=name,
                    dateless=False, rows=rows,
                    action=PROMOTE_ACTION_ATTACH_EXISTING,
                    existing_channel_id=None,
                ),
                PromotionUnit(
                    event_key="fury vs. usyk|b", channel_name=name,
                    dateless=False, rows=rows,
                    action=PROMOTE_ACTION_ATTACH_EXISTING,
                    existing_channel_id=904,
                ),
            ),
        )
        assert plan.skipped_past == 2
        assert plan.skipped_past_adopted == 1

    def test_event_with_no_parsed_start_is_never_past(self):
        """The first guard, at the source: with no start there is nothing
        to compare, so the event is never treated as finished however long
        the grace window is."""
        parsed = ParsedEvent(
            raw_name="no time here", title="Fury vs. Usyk", start=None,
            teams=None, matched_pattern=None,
        )
        assert event_is_past(parsed, 0, FROZEN_NOW) is False

    def test_dateless_event_keeps_its_existing_channel(self):
        """The synthesized-date guard on the destructive path: the date
        came from "now", so a past-vs-future verdict would flip at
        midnight and delete a channel that is still wanted. The dateless
        bucket carries the channel id, which is what the executor holds in
        the managed set."""
        parsed = _parsed(
            "Fury vs Hall", self.FINISHED,
            matched_pattern="dateless-title-time-ampm",
        )
        rows = [_resolved("FURY vs HALL 8PM", DISPOSITION_UNMATCHED, parsed)]
        plan = build_promotion_plan(
            self._skip_config(), rows,
            {promoted_channel_name(parsed).lower(): 903}, now=FROZEN_NOW,
        )
        assert plan.skipped_past == 0
        assert plan.skipped_past_adopted == 0
        assert plan.skipped_dateless == 1
        assert plan.skipped_dateless_units[0].existing_channel_id == 903

    def test_channel_of_an_event_still_on_air_is_kept(self):
        """The grace window on the destructive path: the broadcast started
        two hours ago and has no parsed duration, so its channel must
        survive the run rather than vanish mid-event."""
        parsed = _parsed("Mercury vs. Aces", self.IN_PROGRESS)
        rows = [_resolved("live", DISPOSITION_UNMATCHED, parsed)]
        plan = build_promotion_plan(
            self._skip_config(), rows,
            {promoted_channel_name(parsed).lower(): 904}, now=FROZEN_NOW,
        )
        assert plan.skipped_past == 0
        assert plan.skipped_past_adopted == 0
        assert plan.units[0].existing_channel_id == 904

    def test_past_events_do_not_spend_cap_budget(self):
        """Filtering runs before the cap, so a playlist full of finished
        events cannot starve the live ones of create slots."""
        rows = [
            _resolved("a", DISPOSITION_UNMATCHED,
                      _parsed("Alpha Event", self.FINISHED), stream_id=1),
            _resolved("b", DISPOSITION_UNMATCHED,
                      _parsed("Beta Event", self.FINISHED), stream_id=2),
            _resolved("c", DISPOSITION_UNMATCHED,
                      _parsed("Gamma Event", START), stream_id=3),
        ]
        plan = build_promotion_plan(
            self._skip_config(max_promote_per_run=1), rows, {},
            now=FROZEN_NOW,
        )
        assert plan.skipped_past == 2
        assert plan.capped is False
        assert [u.rows[0].result.parsed.title for u in plan.units] \
            == ["Gamma Event"]

    @pytest.mark.parametrize("flag", [None, False])
    def test_filter_is_inert_when_off(self, flag):
        """A rule that never asked for the filter promotes exactly what it
        promoted before, finished events included."""
        overrides = {} if flag is None else {"skip_past_events": flag}
        rows = [_resolved("old", DISPOSITION_UNMATCHED,
                          _parsed("Fury vs. Usyk", self.FINISHED))]
        plan = build_promotion_plan(
            _promote_config(**overrides), rows, {}, now=FROZEN_NOW
        )
        assert plan.would_create == 1
        assert plan.skipped_past_units == ()

    def test_off_config_never_reads_a_clock(self):
        """No ``now`` passed and none needed — the default clock read is
        gated behind the flag."""
        rows = [_resolved("old", DISPOSITION_UNMATCHED,
                          _parsed("Fury vs. Usyk", self.FINISHED))]
        with patch("services.event_sync_promote.datetime") as fake_clock:
            plan = build_promotion_plan(_promote_config(), rows, {})
            assert fake_clock.now.call_count == 0
        assert plan.would_create == 1


class TestClusteringAcrossProviderClocks:
    """Two providers listing one event at slightly different times.

    One provider publishes the broadcast start and the next the undercard,
    so the same race shows up as 7:15 pm on one and 7:30 pm on the other.
    Each spelling used to mint its own channel. Clustering forgives a
    disagreement up to the rule's own time window and no further than
    ``MAX_CLUSTER_WINDOW_MINUTES``, so two genuinely different events are
    never joined.
    """

    RACE = EASTERN.localize(datetime(2026, 8, 9, 19, 15))

    def _minutes_later(self, minutes):
        return self.RACE + timedelta(minutes=minutes)

    def test_same_title_a_quarter_hour_apart_forms_one_unit(self):
        """The measured case: one race, two providers, 15 minutes apart."""
        rows = [
            _resolved("DIRTVISION 01 : Knoxville Raceway 7:15 pm",
                      DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self.RACE),
                      provider_id=2, stream_id=301),
            _resolved("PPV EVENT 04: Knoxville Raceway 7:30 PM ET",
                      DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self._minutes_later(15)),
                      provider_id=3, stream_id=555),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert len(plan.units) == 1
        assert [r.stream.stream_id for r in plan.units[0].rows] == [301, 555]

    def test_the_earliest_start_names_the_channel(self):
        """The representative is the earliest start, so the channel name
        and the surviving event key do not depend on which provider the
        fetch happened to return first."""
        early = _parsed("Knoxville Raceway", self.RACE)
        late = _parsed("Knoxville Raceway", self._minutes_later(15))
        forward = [
            _resolved("a", DISPOSITION_UNMATCHED, early, stream_id=1),
            _resolved("b", DISPOSITION_UNMATCHED, late, stream_id=2),
        ]
        backward = list(reversed(forward))
        for rows in (forward, backward):
            plan = build_promotion_plan(_promote_config(), rows, {})
            assert len(plan.units) == 1
            assert plan.units[0].channel_name == promoted_channel_name(early)
            assert plan.units[0].event_key == master_event_key(early)

    def test_the_name_follows_the_surviving_key_not_the_stream_order(self):
        """Streams are ordered by provider and id for attaching, which has
        nothing to do with which listing the channel is named after. Here
        the LATER listing sorts first, and the name must still be the
        earlier one's."""
        early = _parsed("Knoxville Raceway", self.RACE)
        rows = [
            _resolved("late but first in stream order",
                      DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self._minutes_later(15)),
                      provider_id=1, stream_id=1),
            _resolved("early but last in stream order",
                      DISPOSITION_UNMATCHED, early,
                      provider_id=9, stream_id=999),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert len(plan.units) == 1
        assert plan.units[0].channel_name == promoted_channel_name(early)
        assert plan.units[0].event_key == master_event_key(early)
        # The surviving key's own rows come first and the folded-in ones
        # follow, each group by provider and stream id. Element zero is
        # therefore the row the unit was named after, which is what lets a
        # caller read the unit's start off rows[0]. [52]
        assert [r.stream.stream_id for r in plan.units[0].rows] == [999, 1]

    def test_a_folded_unit_reads_its_start_off_the_row_it_was_named_after(
        self
    ):
        """Two listings of one event forty minutes apart with the run
        happening between them. The health gate reads the unit's start off
        ``rows[0]`` to decide whether the event has begun, so that row has
        to be the one the unit's identity came from. Sorting the whole
        folded list by provider and stream id used to put the later listing
        first, and a single run then answered "has this started" one way
        for the channel name and the other way for the probe verdicts. [52]
        """
        early = _parsed("Knoxville Raceway", self.RACE)
        late = _parsed("Knoxville Raceway", self._minutes_later(40))
        rows = [
            # The later listing sorts first by (provider_id, stream_id).
            _resolved("later listing", DISPOSITION_UNMATCHED, late,
                      provider_id=1, stream_id=1),
            _resolved("earlier listing", DISPOSITION_UNMATCHED, early,
                      provider_id=9, stream_id=999),
        ]
        between = self._minutes_later(20)

        plan = build_promotion_plan(
            _promote_config(time_window_minutes=60), rows, {}, now=between,
        )

        assert len(plan.units) == 1
        unit = plan.units[0]
        assert unit.channel_name == promoted_channel_name(early)
        assert unit.rows[0].result.parsed.start == early.start
        # The two listings genuinely straddle the run's instant, so reading
        # the wrong one flips the verdict rather than merely shifting it.
        assert event_has_started(unit.rows[0].result.parsed, between) is True
        assert event_has_started(late, between) is False

    def test_an_existing_channel_outranks_the_earliest_start(self):
        """A provider dropping its earlier listing must not rename the
        channel. A key whose channel already exists survives the fold, so
        the run adopts instead of creating a second one and retiring the
        first."""
        late = _parsed("Knoxville Raceway", self._minutes_later(15))
        rows = [
            _resolved("earlier listing", DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self.RACE), stream_id=1),
            _resolved("later listing", DISPOSITION_UNMATCHED, late,
                      stream_id=2),
        ]
        plan = build_promotion_plan(
            _promote_config(), rows,
            {promoted_channel_name(late).lower(): 915},
        )
        assert len(plan.units) == 1
        assert plan.units[0].channel_name == promoted_channel_name(late)
        assert plan.units[0].action == PROMOTE_ACTION_ATTACH_EXISTING
        assert plan.units[0].existing_channel_id == 915

    def test_starts_beyond_the_window_stay_apart(self):
        rows = [
            _resolved("a", DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self.RACE), stream_id=1),
            _resolved("b", DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self._minutes_later(45)),
                      stream_id=2),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert len(plan.units) == 2

    def test_a_chain_of_near_starts_cannot_walk_across_the_window(self):
        """Distance is measured from the cluster's earliest start, not from
        the previous event, so twenty-minute steps cannot drag an hour of
        the schedule onto one channel."""
        rows = [
            _resolved("a", DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self.RACE), stream_id=1),
            _resolved("b", DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self._minutes_later(20)),
                      stream_id=2),
            _resolved("c", DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self._minutes_later(40)),
                      stream_id=3),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert len(plan.units) == 2
        assert [len(u.rows) for u in plan.units] == [2, 1]

    def test_a_weekly_show_never_collapses(self):
        """Seven days apart is outside any window a rule can configure."""
        aug12 = EASTERN.localize(datetime(2026, 8, 12, 20, 0))
        rows = [
            _resolved("AEW Dynamite Aug 12", DISPOSITION_UNMATCHED,
                      _parsed("Aew Dynamite", aug12), stream_id=1),
            _resolved("AEW Dynamite Aug 19", DISPOSITION_UNMATCHED,
                      _parsed("Aew Dynamite", aug12 + timedelta(days=7)),
                      stream_id=2),
        ]
        plan = build_promotion_plan(
            _promote_config(time_window_minutes=1440), rows, {}
        )
        assert len(plan.units) == 2

    def test_practice_and_qualifying_stay_apart(self):
        """Different sessions of one meeting carry different titles, so the
        title test alone keeps them on their own channels even when they
        start minutes apart."""
        rows = [
            _resolved("FP3", DISPOSITION_UNMATCHED,
                      _parsed("Free Practice 3 Fia Wec Lone Star Le Mans",
                              self.RACE),
                      stream_id=1),
            _resolved("Qualifying", DISPOSITION_UNMATCHED,
                      _parsed("Qualifying Fia Wec Lone Star Le Mans",
                              self._minutes_later(10)),
                      stream_id=2),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert len(plan.units) == 2

    def test_dateless_slots_never_fold(self):
        """A dateless start was fabricated from "now", so its clock says
        nothing about which event it is and folding on it would join two
        unrelated recurring slots. Neither is promoted, but they stay two
        separate units on the way out."""
        rows = [
            _resolved("slot a", DISPOSITION_UNMATCHED,
                      _parsed("Ppv Event", self.RACE,
                              matched_pattern="dateless-title-time-ampm"),
                      stream_id=1),
            _resolved("slot b", DISPOSITION_UNMATCHED,
                      _parsed("Ppv Event", self._minutes_later(15),
                              matched_pattern="dateless-title-time-ampm"),
                      stream_id=2),
        ]
        plan = build_promotion_plan(_promote_config(), rows, {})
        assert plan.units == ()
        assert plan.skipped_dateless == 2

    def test_enforce_time_window_off_still_keeps_the_days_apart(self):
        """``enforce_time_window`` governs whether a clock mismatch may
        block an attach to a master. Clustering keeps using the window
        regardless, because a rule that switched it off must not end up
        with a whole season on one channel."""
        aug12 = EASTERN.localize(datetime(2026, 8, 12, 20, 0))
        rows = [
            _resolved("week 1", DISPOSITION_UNMATCHED,
                      _parsed("Aew Dynamite", aug12), stream_id=1),
            _resolved("week 2", DISPOSITION_UNMATCHED,
                      _parsed("Aew Dynamite", aug12 + timedelta(days=7)),
                      stream_id=2),
        ]
        plan = build_promotion_plan(
            _promote_config(enforce_time_window=False), rows, {}
        )
        assert len(plan.units) == 2

    def test_a_folded_unit_adopts_the_channel_of_its_earliest_start(self):
        """Idempotence across the fold: the run after the one that created
        the channel derives the same name and adopts it."""
        early = _parsed("Knoxville Raceway", self.RACE)
        rows = [
            _resolved("a", DISPOSITION_UNMATCHED, early, stream_id=1),
            _resolved("b", DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self._minutes_later(15)),
                      stream_id=2),
        ]
        plan = build_promotion_plan(
            _promote_config(), rows,
            {promoted_channel_name(early).lower(): 910},
        )
        assert len(plan.units) == 1
        assert plan.units[0].action == PROMOTE_ACTION_ATTACH_EXISTING
        assert plan.units[0].existing_channel_id == 910

    def test_the_ceiling_still_folds_at_its_own_boundary(self):
        """A rule asking for more than the ceiling keeps folding right up to
        it. An hour is the coarsest clock disagreement the fold forgives, so
        two providers exactly that far apart are still one event."""
        rows = [
            _resolved("a", DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway", self.RACE), stream_id=1),
            _resolved("b", DISPOSITION_UNMATCHED,
                      _parsed("Knoxville Raceway",
                              self._minutes_later(MAX_CLUSTER_WINDOW_MINUTES)),
                      stream_id=2),
        ]
        plan = build_promotion_plan(
            _promote_config(time_window_minutes=1440), rows, {}
        )
        assert len(plan.units) == 1

    def test_the_ceiling_holds_back_to_back_sessions_apart(self):
        """Measured on live data: two BMX park sessions two hours apart are
        different events. ``time_window_minutes`` is legal all the way to
        1440, so without a ceiling of its own the fold would put them on one
        channel."""
        rows = [
            _resolved("womens park", DISPOSITION_UNMATCHED,
                      _parsed("Park Birmingham Uci Bmx Freestyle",
                              EASTERN.localize(datetime(2026, 8, 9, 16, 45))),
                      stream_id=1),
            _resolved("mens park", DISPOSITION_UNMATCHED,
                      _parsed("Park Birmingham Uci Bmx Freestyle",
                              EASTERN.localize(datetime(2026, 8, 9, 18, 45))),
                      stream_id=2),
        ]
        plan = build_promotion_plan(
            _promote_config(time_window_minutes=1440), rows, {}
        )
        assert len(plan.units) == 2

    def test_no_setting_merges_two_days_of_one_tournament(self):
        """The rule the ceiling exists to keep: nothing folds across a day,
        whatever the operator set. Measured on live data, where the same
        tennis session ran at 12:30 on two consecutive days and the maximum
        window put both on one channel."""
        aug8 = EASTERN.localize(datetime(2026, 8, 8, 12, 30))
        rows = [
            _resolved("day 1", DISPOSITION_UNMATCHED,
                      _parsed("Wta National Bank Open Womens Day Session",
                              aug8),
                      stream_id=1),
            _resolved("day 2", DISPOSITION_UNMATCHED,
                      _parsed("Wta National Bank Open Womens Day Session",
                              aug8 + timedelta(days=1)),
                      stream_id=2),
        ]
        plan = build_promotion_plan(
            _promote_config(time_window_minutes=1440), rows, {}
        )
        assert len(plan.units) == 2


class TestPromoteLeadHours:
    """``promote_lead_hours``: an event still days away waits its turn.

    Providers publish a show days before it airs, so without a lead window
    a channel for next week's card sits in the group all week. This is the
    mirror of ``skip_past_events`` with one deliberate difference: it gates
    CREATES ONLY. Un-promoting a far-off event that already has a channel
    would delete and recreate the same channel every day.
    """

    SOON = EASTERN.localize(datetime(2026, 7, 12, 8, 0))
    FAR = EASTERN.localize(datetime(2026, 7, 26, 20, 0))

    def _lead_config(self, hours=24, **overrides):
        return _promote_config(promote_lead_hours=hours, **overrides)

    def test_event_beyond_the_window_is_not_created(self):
        rows = [
            _resolved("far", DISPOSITION_UNMATCHED,
                      _parsed("Aew All In", self.FAR)),
        ]
        plan = build_promotion_plan(
            self._lead_config(), rows, {}, now=FROZEN_NOW
        )
        assert plan.units == ()
        assert plan.skipped_early == 1
        assert plan.would_create == 0

    def test_event_inside_the_window_is_created(self):
        rows = [
            _resolved("soon", DISPOSITION_UNMATCHED,
                      _parsed("Aew Dynamite", self.SOON)),
        ]
        plan = build_promotion_plan(
            self._lead_config(), rows, {}, now=FROZEN_NOW
        )
        assert plan.skipped_early == 0
        assert plan.would_create == 1

    def test_the_window_boundary_is_the_only_thing_separating_the_two(self):
        """The boundary is INCLUSIVE: an event exactly ``lead_hours`` away
        is created, matching ``event_is_past``, where an event exactly at
        its grace boundary has not finished yet. Bracketing at a minute
        either side alone would let a ``>=`` slip through, so the exact
        hour is pinned too."""
        just_inside = FROZEN_NOW + timedelta(hours=23, minutes=59)
        exactly_at = FROZEN_NOW + timedelta(hours=24)
        just_outside = FROZEN_NOW + timedelta(hours=24, minutes=1)
        assert event_is_early(
            _parsed("x", just_inside), 24, FROZEN_NOW) is False
        assert event_is_early(
            _parsed("x", exactly_at), 24, FROZEN_NOW) is False
        assert event_is_early(
            _parsed("x", just_outside), 24, FROZEN_NOW) is True

    def test_an_existing_channel_is_never_taken_back(self):
        """The create-only rule, stated as the thing it protects: a far-off
        event that already has a channel keeps it, so the channel is not
        deleted tonight and recreated tomorrow. This is what the default
        buys, and the config here does not carry the opt-in key at all."""
        parsed = _parsed("Aew All In", self.FAR)
        rows = [
            _resolved("far", DISPOSITION_UNMATCHED, parsed),
        ]
        plan = build_promotion_plan(
            self._lead_config(), rows,
            {promoted_channel_name(parsed).lower(): 920},
            now=FROZEN_NOW,
        )
        assert plan.skipped_early == 0
        assert len(plan.units) == 1
        assert plan.units[0].action == PROMOTE_ACTION_ATTACH_EXISTING
        assert plan.units[0].existing_channel_id == 920

    def test_an_existing_channel_is_held_back_when_the_rule_opts_in(self):
        """``apply_lead_to_existing``: the same far-off event and the same
        existing channel, with the operator now asking for the window to
        reach it. A provider that lists an event hours ahead and serves an
        offline card until it starts is the case this is for."""
        parsed = _parsed("Aew All In", self.FAR)
        rows = [
            _resolved("far", DISPOSITION_UNMATCHED, parsed),
        ]
        plan = build_promotion_plan(
            self._lead_config(apply_lead_to_existing=True), rows,
            {promoted_channel_name(parsed).lower(): 920},
            now=FROZEN_NOW,
        )
        assert plan.units == ()
        assert plan.skipped_early == 1
        assert (plan.skipped_early_units[0].action
                == PROMOTE_ACTION_ATTACH_EXISTING)

    @pytest.mark.parametrize("opted_in", [False, True])
    def test_a_create_is_held_back_under_either_setting(self, opted_in):
        """The control the two tests above need. Strip the existing-channel
        entry and the very same event is a create, held back either way.
        Without it a passing result proves only that the fixture never
        reached the gate."""
        rows = [
            _resolved("far", DISPOSITION_UNMATCHED,
                      _parsed("Aew All In", self.FAR)),
        ]
        plan = build_promotion_plan(
            self._lead_config(apply_lead_to_existing=opted_in), rows, {},
            now=FROZEN_NOW,
        )
        assert plan.units == ()
        assert plan.skipped_early == 1

    def test_the_key_only_holds_back_what_the_window_calls_early(self):
        """What does the holding back is still the lead window, not the
        key: with the key on, an event close to air keeps its channel."""
        parsed = _parsed("Aew Dynamite", self.SOON)
        rows = [
            _resolved("soon", DISPOSITION_UNMATCHED, parsed),
        ]
        plan = build_promotion_plan(
            self._lead_config(apply_lead_to_existing=True), rows,
            {promoted_channel_name(parsed).lower(): 921},
            now=FROZEN_NOW,
        )
        assert plan.skipped_early == 0
        assert len(plan.units) == 1
        assert plan.units[0].action == PROMOTE_ACTION_ATTACH_EXISTING

    def test_the_lead_window_never_judges_a_dateless_event(self):
        """The date was fabricated from "now", so an early-vs-late verdict
        on it says nothing about the event. It leaves the plan as dateless
        rather than as held back."""
        rows = [
            _resolved("slot", DISPOSITION_UNMATCHED,
                      _parsed("Ppv Event", self.FAR,
                              matched_pattern="dateless-title-time-ampm")),
        ]
        plan = build_promotion_plan(
            self._lead_config(), rows, {}, now=FROZEN_NOW
        )
        assert plan.skipped_early == 0
        assert plan.skipped_dateless == 1
        assert plan.would_create == 0

    def test_event_with_no_parsed_start_is_never_early(self):
        parsed = ParsedEvent(raw_name="no time here", title="Fury vs. Usyk",
                             start=None, teams=None, matched_pattern=None)
        assert event_is_early(parsed, 24, FROZEN_NOW) is False


    def test_early_events_do_not_spend_cap_budget(self):
        """Held-back events must not starve tonight's events of create
        slots, so the lead filter runs before the cap."""
        rows = [
            _resolved("a", DISPOSITION_UNMATCHED,
                      _parsed("Alpha Event", self.FAR), stream_id=1),
            _resolved("b", DISPOSITION_UNMATCHED,
                      _parsed("Beta Event", self.FAR), stream_id=2),
            _resolved("c", DISPOSITION_UNMATCHED,
                      _parsed("Gamma Event", self.SOON), stream_id=3),
        ]
        plan = build_promotion_plan(
            self._lead_config(max_promote_per_run=1), rows, {},
            now=FROZEN_NOW,
        )
        assert plan.skipped_early == 2
        assert plan.capped is False
        assert [u.rows[0].result.parsed.title
                for u in plan.units] == ["Gamma Event"]

    def test_absent_key_promotes_an_event_however_far_away(self):
        rows = [
            _resolved("far", DISPOSITION_UNMATCHED,
                      _parsed("Aew All In", self.FAR)),
        ]
        plan = build_promotion_plan(
            _promote_config(), rows, {}, now=FROZEN_NOW
        )
        assert plan.skipped_early == 0
        assert plan.would_create == 1

    def test_absent_key_never_reads_a_clock(self):
        rows = [
            _resolved("far", DISPOSITION_UNMATCHED,
                      _parsed("Aew All In", self.FAR)),
        ]
        with patch("services.event_sync_promote.datetime") as fake_clock:
            plan = build_promotion_plan(_promote_config(), rows, {})
            assert fake_clock.now.call_count == 0
        assert plan.would_create == 1


class TestEventHasStarted:
    """The question the health gate asks before a probe verdict counts.

    A stream for an event that has not begun may fail simply because there
    is nothing to stream yet, so a failure before kickoff is no evidence at
    all. Only a delisted stream is dead either way.
    """

    BEFORE = EASTERN.localize(datetime(2026, 7, 11, 20, 0))
    AFTER = EASTERN.localize(datetime(2026, 7, 12, 2, 0))

    def test_a_start_already_behind_us_has_started(self):
        parsed = _parsed("Fury vs. Usyk", START)
        assert event_has_started(parsed, self.AFTER) is True

    def test_a_start_still_ahead_has_not_started(self):
        parsed = _parsed("Fury vs. Usyk", START)
        assert event_has_started(parsed, self.BEFORE) is False

    def test_the_exact_start_instant_counts_as_started(self):
        parsed = _parsed("Fury vs. Usyk", START)
        assert event_has_started(parsed, START) is True

    def test_an_event_with_no_parsed_start_never_starts(self):
        parsed = ParsedEvent(raw_name="no time here", title="Fury vs. Usyk",
                             start=None, teams=None, matched_pattern=None)
        assert event_has_started(parsed, self.AFTER) is False

    def test_a_dateless_event_never_starts(self):
        """Its date came from "now", so "has it begun" is unanswerable and
        the safe answer is the one where no probe verdict counts."""
        parsed = _parsed("Ppv Event", START,
                         matched_pattern="dateless-title-time-ampm")
        assert event_has_started(parsed, self.AFTER) is False


class TestDeadStreamsAreNotPromoted:
    """``skip_dead_streams`` at the planning layer.

    The caller checks the health of the streams the plan is about to turn
    into channels and hands the failures back here. A unit keeps promoting
    on its survivors; a unit with no survivor is not realized. The plan
    deletes nothing either way — the unit that loses every stream still
    carries the id of the channel it already has, and the executor decides
    what to do with it.
    """

    def _rows(self):
        return [
            _resolved("dead one", DISPOSITION_UNMATCHED,
                      _parsed("Fury vs. Usyk", START),
                      provider_id=2, stream_id=301),
            _resolved("working one", DISPOSITION_UNMATCHED,
                      _parsed("Fury vs. Usyk", START),
                      provider_id=3, stream_id=555),
        ]

    def test_a_dead_stream_leaves_the_attach_list(self):
        plan = build_promotion_plan(
            _promote_config(skip_dead_streams=True), self._rows(), {},
            dead_stream_ids={301},
        )
        assert len(plan.units) == 1
        assert [r.stream.stream_id for r in plan.units[0].rows] == [555]
        assert plan.dead_streams_skipped == 1
        assert plan.skipped_all_dead == 0

    def test_an_event_with_no_working_stream_is_not_created(self):
        plan = build_promotion_plan(
            _promote_config(skip_dead_streams=True), self._rows(), {},
            dead_stream_ids={301, 555},
        )
        assert plan.units == ()
        assert plan.skipped_all_dead == 1
        assert plan.dead_streams_skipped == 2

    def test_an_all_dead_unit_still_carries_its_existing_channel_id(self):
        """The rail that keeps a provider outage from deleting channels:
        the unit is not realized, but it still names the channel the
        executor has to keep in the managed set."""
        parsed = _parsed("Fury vs. Usyk", START)
        plan = build_promotion_plan(
            _promote_config(skip_dead_streams=True), self._rows(),
            {promoted_channel_name(parsed).lower(): 930},
            dead_stream_ids={301, 555},
        )
        assert plan.units == ()
        assert plan.all_dead_units[0].existing_channel_id == 930

    def test_an_all_dead_unit_has_already_spent_its_cap_slot(self):
        """The health check is the LAST gate, so the cap has already been
        applied when it runs and an all-dead unit's slot is gone for this
        run. That is the deliberate choice: capping afterwards would hand
        the freed slot to a unit nobody probed, and the run would create a
        channel for a stream it never checked. Runs are idempotent, so the
        deferred event comes back next run."""
        rows = [
            _resolved("dead", DISPOSITION_UNMATCHED,
                      _parsed("Alpha Event", START), stream_id=1),
            _resolved("live", DISPOSITION_UNMATCHED,
                      _parsed("Beta Event", START), stream_id=2),
        ]
        plan = build_promotion_plan(
            _promote_config(skip_dead_streams=True, max_promote_per_run=1),
            rows, {}, dead_stream_ids={1},
        )
        assert plan.skipped_all_dead == 1
        assert plan.units == ()
        assert plan.capped is True
        assert [u.rows[0].result.parsed.title
                for u in plan.capped_units] == ["Beta Event"]

    def test_the_cap_decides_before_the_health_check_sees_anything(self):
        """The order the caller depends on: whatever the cap defers is not
        in ``plan.units``, so its streams are never probed."""
        rows = [
            _resolved("a", DISPOSITION_UNMATCHED,
                      _parsed("Alpha Event", START), stream_id=1),
            _resolved("b", DISPOSITION_UNMATCHED,
                      _parsed("Beta Event", START), stream_id=2),
            _resolved("c", DISPOSITION_UNMATCHED,
                      _parsed("Gamma Event", START), stream_id=3),
        ]
        plan = build_promotion_plan(
            _promote_config(skip_dead_streams=True, max_promote_per_run=1),
            rows, {},
        )
        candidates = [r.stream.stream_id
                      for u in plan.units for r in u.rows]
        assert candidates == [1]
        assert plan.cap_overage == 2

    def test_a_finished_or_early_event_is_never_a_probe_candidate(self):
        """Both clocks run before the cap, so neither a finished event nor
        one beyond the lead window reaches the health check."""
        finished = EASTERN.localize(datetime(2026, 7, 8, 20, 0))
        far = EASTERN.localize(datetime(2026, 7, 26, 20, 0))
        soon = EASTERN.localize(datetime(2026, 7, 11, 20, 0))
        rows = [
            _resolved("old", DISPOSITION_UNMATCHED,
                      _parsed("Alpha Event", finished), stream_id=1),
            _resolved("far", DISPOSITION_UNMATCHED,
                      _parsed("Beta Event", far), stream_id=2),
            _resolved("now", DISPOSITION_UNMATCHED,
                      _parsed("Gamma Event", soon), stream_id=3),
        ]
        plan = build_promotion_plan(
            _promote_config(skip_dead_streams=True, skip_past_events=True,
                            promote_lead_hours=24),
            rows, {}, now=FROZEN_NOW,
        )
        assert plan.skipped_past == 1
        assert plan.skipped_early == 1
        candidates = [r.stream.stream_id
                      for u in plan.units for r in u.rows]
        assert candidates == [3]

    def test_no_dead_ids_leaves_the_plan_exactly_as_it_was(self):
        plan = build_promotion_plan(
            _promote_config(skip_dead_streams=True), self._rows(), {},
            dead_stream_ids=set(),
        )
        assert len(plan.units[0].rows) == 2
        assert plan.dead_streams_skipped == 0
        assert plan.skipped_all_dead == 0

    def test_a_finished_event_is_still_retired_when_its_streams_are_dead(self):
        """Ordering rail: the past filter runs BEFORE the health filter, so
        a finished event still leaves the managed set instead of being
        rescued by the health filter's keep-the-channel rule."""
        finished = EASTERN.localize(datetime(2026, 7, 8, 20, 0))
        parsed = _parsed("Fury vs. Usyk", finished)
        rows = [
            _resolved("old", DISPOSITION_UNMATCHED, parsed, stream_id=301),
        ]
        plan = build_promotion_plan(
            _promote_config(skip_past_events=True, skip_dead_streams=True),
            rows, {promoted_channel_name(parsed).lower(): 940},
            now=FROZEN_NOW, dead_stream_ids={301},
        )
        assert plan.skipped_past == 1
        assert plan.skipped_past_adopted == 1
        assert plan.skipped_all_dead == 0
        assert plan.all_dead_units == ()


# =========================================================================
# Engine/executor integration against the stateful fixture Dispatcharr.
# =========================================================================


@pytest.fixture()
def db_session_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False,
    )
    database.Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=engine, expire_on_commit=False
    )
    try:
        yield SessionLocal
    finally:
        database.Base.metadata.drop_all(bind=engine)
        engine.dispose()


def _add_rule(session_factory, config) -> int:
    session = session_factory()
    try:
        rule = ChannelPipelineRule(
            name="Event Rule", enabled=True, priority=0,
            conditions=json.dumps([{"type": "always"}]),
            actions=json.dumps([{"type": "skip"}]),
            event_sync_config=json.dumps(config),
        )
        session.add(rule)
        session.commit()
        session.refresh(rule)
        return rule.id
    finally:
        session.close()


def _manual_run(client, session_factory, dry_run=False, now=FROZEN_NOW):
    engine = ChannelPipelineEngine(client)
    with patch("channel_pipeline_engine.get_session",
               side_effect=session_factory), \
         patch("journal.log_entries") as mock_log_entries, \
         patch("services.event_sync_resolver.datetime") as mock_dt:
        mock_dt.now.return_value = now
        result = _run(engine.run_pipeline(dry_run=dry_run,
                                          triggered_by="manual"))
    entries = [
        e for call in mock_log_entries.call_args_list
        for e in call.kwargs.get("entries", [])
    ]
    return result, entries


def _latest_warnings(session_factory) -> list[dict]:
    """Persisted run warnings — run_pipeline pops event_sync_warnings from
    the API response and persists them on the execution row."""
    from models import ChannelPipelineExecution

    session = session_factory()
    try:
        execution = (
            session.query(ChannelPipelineExecution)
            .order_by(ChannelPipelineExecution.id.desc())
            .first()
        )
        return execution.get_warnings() if execution else []
    finally:
        session.close()


def _managed_ids(session_factory, rule_id) -> list[int]:
    session = session_factory()
    try:
        rule = session.get(ChannelPipelineRule, rule_id)
        return rule.get_managed_channel_ids()
    finally:
        session.close()


def _promote_state() -> FakeDispatcharrState:
    """One attachable master event + one unmatched secondary-only event."""
    return FakeDispatcharrState(
        channels=[{
            "id": 100, "name": MASTER_MERCURY,
            "channel_group_id": MASTER_GROUP_ID,
            "auto_created": True, "streams": [9001],
        }],
        secondary_streams={
            SECONDARY_A_NAME: [
                {"id": 7001, "name": STREAM_MERCURY, "m3u_account": 1},
            ],
            SECONDARY_B_NAME: [
                {"id": 7301, "name": STREAM_FURY, "m3u_account": 2},
            ],
        },
    )


FURY_CHANNEL_NAME = "Fury Vs. Usyk @ Jul 11 11:00 PM"


class TestFlagOffRegression:
    """AC-1: absent flag → byte-identical behavior everywhere."""

    def test_no_promotion_key_no_creation_no_pass4(self, db_session_factory):
        rule_id = _add_rule(db_session_factory, event_sync_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
        ))
        state = _promote_state()
        client = make_promote_client(state)

        result, _ = _manual_run(client, db_session_factory)
        assert result["success"] is True
        summary = result["event_sync"][0]
        assert "promotion" not in summary
        assert "promoted" not in summary["summary_line"]
        client.create_channel.assert_not_awaited()
        client.delete_channel.assert_not_awaited()
        assert result["channels_created"] == 0
        assert result["created_entities"] == []
        # Pass 4 stays hard-bypassed: the managed set is never populated.
        assert _managed_ids(db_session_factory, rule_id) == []
        assert_never_touched_group_settings(client)


class TestLivePromotion:
    """AC-2 + managed-set invariant + journal provenance."""

    def test_unmatched_stream_gets_channel_stream_and_journal_row(
        self, db_session_factory
    ):
        rule_id = _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        client = make_promote_client(state)

        result, journal_entries = _manual_run(client, db_session_factory)
        assert result["success"] is True
        summary = result["event_sync"][0]

        # The attach path is untouched: Mercury still attaches to master.
        assert summary["attached"] == 1
        assert state.stream_ids_of(100) == [9001, 7001]

        # The unmatched Fury event got its OWN channel in the target group.
        promo = summary["promotion"]
        assert promo["promoted_created"] == 1
        assert promo["streams_attached"] == 1
        assert promo["attach_errors"] == 0
        created = [c for cid, c in state.channels.items() if cid >= 900]
        assert len(created) == 1
        channel = created[0]
        assert channel["name"] == FURY_CHANNEL_NAME
        assert channel["channel_group_id"] == PROMOTE_GROUP_ID
        assert channel["streams"] == [7301]
        assert result["channels_created"] == 1
        assert {e["type"]: e["id"] for e in result["created_entities"]} \
            == {"channel": channel["id"]}

        # Journal: category event_sync, fingerprint provenance, IDs
        # display-only alongside names.
        promote_rows = [
            e for e in journal_entries
            if (e.get("after_value") or {}).get("match", {}).get("kind")
            == "event_sync_promote"
        ]
        assert len(promote_rows) == 1
        row = promote_rows[0]
        assert row["category"] == "event_sync"
        assert row["action_type"] == "merge_stream"
        match = row["after_value"]["match"]
        assert match["provider_id"] == 2
        assert len(match["stream_name_hash"]) == 64
        assert "|" in match["event_key"]
        assert match["secondary_stream_name"] == STREAM_FURY
        assert match["promoted_channel_name"] == FURY_CHANNEL_NAME

        # Managed set: promoted channel ONLY — the master is provably
        # absent (dedicated invariant assertion).
        managed = _managed_ids(db_session_factory, rule_id)
        assert managed == [channel["id"]]
        assert 100 not in managed
        assert_never_touched_group_settings(client)

    def test_summary_line_carries_promotion_counts(self, db_session_factory):
        _add_rule(db_session_factory, _promote_config())
        client = make_promote_client(_promote_state())
        result, _ = _manual_run(client, db_session_factory)
        line = result["event_sync"][0]["summary_line"]
        assert "1 promoted, 0 promoted-adopted" in line


def _staged_event(db_session_factory, monkeypatch, *, dedicated=False):
    """Configure one current healthy event with an enabled generated guide."""
    from models import DummyEPGProfile
    from services import event_sync_stream_health
    from tests.unit import test_event_sync_dummy_epg as dummy_epg

    profile_id = dummy_epg.PROFILE_ID
    source_id = dummy_epg.DUMMY_SOURCE_ID
    session = db_session_factory()
    try:
        profile = DummyEPGProfile(
            id=profile_id,
            name="Promoted events",
            enabled=True,
            name_source="channel",
            event_timezone="US/Eastern",
            output_timezone="UTC",
            program_duration=180,
        )
        profile.set_channel_group_ids([PROMOTE_GROUP_ID] if dedicated else [MASTER_GROUP_ID])
        profile.set_epg_source_ids([] if dedicated else [source_id])
        if dedicated:
            profile.set_hide_empty_group_ids([PROMOTE_GROUP_ID])
            profile.set_event_sync_config({
                "secondary": [{"group_id": SECONDARY_A, "m3u_account_id": 1}, {"group_id": SECONDARY_B, "m3u_account_id": 2}],
                "assume_current_date": False, "use_default_patterns": False, "slot_patterns": [],
            })
        session.add(profile)
        session.commit()
    finally:
        session.close()

    config = _promote_config(dummy_epg_profile_id=profile_id)
    if dedicated:
        from tests.event_sync_fixtures import dedicated_event_sync_config

        config = dedicated_event_sync_config(dummy_epg_profile_id=profile_id, promote_target_group_id=PROMOTE_GROUP_ID)
    rule_id = _add_rule(db_session_factory, config)
    state = _promote_state()
    if dedicated:
        state.secondary_streams[SECONDARY_A_NAME] = []
    event_start = datetime.now(EASTERN).replace(second=0, microsecond=0) \
        - timedelta(minutes=1)
    event_name = (
        "DAZN 05: Fury vs. Usyk @ "
        + event_start.strftime("%d %b %I:%M %p ET")
    )
    state.secondary_streams[SECONDARY_B_NAME][0]["name"] = event_name
    event_channel_name = promoted_channel_name(
        _parsed("Fury vs. Usyk", event_start)
    )
    client = make_promote_client(state)
    if dedicated:
        client.get_channel_groups.return_value = [{"id": PROMOTE_GROUP_ID, "name": "Dedicated events"}]
        client.get_m3u_group_settings_by_provider = AsyncMock(return_value={(1, SECONDARY_A): {"auto_channel_sync": False}, (2, SECONDARY_B): {"auto_channel_sync": False}})
    create_channel = client.create_channel.side_effect

    async def create_with_uuid(request):
        channel = await create_channel(request)
        channel["uuid"] = f"event-{channel['id']}"
        state.channels[channel["id"]]["uuid"] = channel["uuid"]
        return copy.deepcopy(channel)

    client.create_channel.side_effect = create_with_uuid
    flow_expires = []
    collect_stream_flow = event_sync_stream_health.collect_stream_flow

    async def collect_and_capture(*args, **kwargs):
        flow_expires.append(kwargs["expires_at"])
        return await collect_stream_flow(*args, **kwargs)

    monkeypatch.setattr(
        event_sync_stream_health,
        "collect_stream_flow",
        collect_and_capture,
    )
    observed_at = datetime.now(timezone.utc)
    health = {
        7301: {
            "stream_name": event_name,
            "probe_status": "success",
            "measured_bitrate": 5_000_000,
            "last_probed": observed_at.isoformat(),
            "is_black_screen": False,
            "black_screen_checked_at": observed_at.isoformat(),
        },
    }
    monkeypatch.setattr(
        event_sync_stream_health,
        "_load_stats",
        AsyncMock(side_effect=lambda ids: {
            stream_id: copy.deepcopy(health[stream_id])
            for stream_id in ids
            if stream_id in health
        }),
    )
    return {
        "profile_id": profile_id,
        "source_id": source_id,
        "rule_id": rule_id,
        "state": state,
        "event_start": event_start,
        "event_name": event_name,
        "event_channel_name": event_channel_name,
        "client": client,
        "dummy_epg": dummy_epg,
        "flow_expires": flow_expires,
        "health": health,
    }


def test_pending_guide_preserves_channel_id(db_session_factory, monkeypatch):
    """A first guide-owned promotion stays staged until exact import proof."""
    from models import DummyEPGProfile
    from services import epg_publication

    setup = _staged_event(db_session_factory, monkeypatch)
    profile_id = setup["profile_id"]
    rule_id = setup["rule_id"]
    state = setup["state"]
    event_start = setup["event_start"]
    event_channel_name = setup["event_channel_name"]
    client = setup["client"]
    dummy_epg = setup["dummy_epg"]
    flow_expires = setup["flow_expires"]
    _, regenerate, wait_refresh = dummy_epg._wire_epg(
        state,
        client,
        db_session_factory,
        regenerated_entries=[
            dummy_epg._dummy_entry(501, 100, MASTER_MERCURY),
            dummy_epg._dummy_entry(502, 900, event_channel_name),
        ],
    )

    publication_snapshots = []
    refresh_snapshots = []
    refresh_expires = []
    first_publish = regenerate.side_effect

    async def publish_before_cancel(**kwargs):
        publication_snapshots.append(copy.deepcopy(
            kwargs["publications"][profile_id]
        ))
        return await first_publish(**kwargs)

    async def cancel_import(*args, **kwargs):
        refresh_expires.append(kwargs["expires_at"])
        raise asyncio.CancelledError

    regenerate.side_effect = publish_before_cancel
    wait_refresh.side_effect = cancel_import

    with patch("services.event_sync_resolver.datetime") as resolver_clock:
        resolver_clock.now.return_value = event_start + timedelta(minutes=1)
        with pytest.raises(asyncio.CancelledError):
            dummy_epg._manual_run(
                client,
                db_session_factory,
                regenerate,
                wait_refresh,
            )

    client.create_channel.assert_awaited_once()
    create_request = client.create_channel.await_args.args[0]
    assert create_request["hidden_from_output"] is True
    assert create_request["streams"] == []
    assert state.channels[900]["uuid"] == "event-900"
    assert state.channels[900]["streams"] == []
    assert state.channels[900]["hidden_from_output"] is True
    assert "epg_data_id" not in state.channels[900]
    with patch(
        "services.epg_publication.get_session",
        side_effect=db_session_factory,
    ):
        cancelled_publication = epg_publication.read_publication(
            f"profile:{profile_id}"
        )
    cancelled_receipt = next(iter(
        cancelled_publication["state"]["delivery"]["pending_channels"].values()
    ))
    assert cancelled_receipt["stage"] == "allocated"
    assert cancelled_receipt["channel_id"] == 900
    assert cancelled_receipt["channel_uuid"] == "event-900"

    resume_client = make_promote_client(state, next_channel_id=901)
    _, regenerate, wait_refresh = dummy_epg._wire_epg(
        state,
        resume_client,
        db_session_factory,
        regenerated_entries=[
            dummy_epg._dummy_entry(501, 100, MASTER_MERCURY),
            dummy_epg._dummy_entry(502, 900, event_channel_name),
        ],
    )
    publish = regenerate.side_effect
    complete_refresh = wait_refresh.complete_refresh

    async def publish_and_capture(**kwargs):
        publication_snapshots.append(copy.deepcopy(
            kwargs["publications"][profile_id]
        ))
        return await publish(**kwargs)

    async def refresh_and_capture(*args, **kwargs):
        refresh_expires.append(kwargs["expires_at"])
        completed = await complete_refresh(*args, **kwargs)
        current = epg_publication.read_publication(
            f"profile:{profile_id}"
        )
        refresh_snapshots.append(copy.deepcopy(current))
        if len(refresh_snapshots) == 2:
            receipt = next(iter(
                current["state"]["delivery"]["pending_channels"].values()
            ))
            state.guide_programmes[:] = [{
                "epg_data_id": 502,
                "tvg_id": "ecm-900",
                "title": receipt["title"],
                "start_time": receipt["start"],
                "end_time": receipt["stop"],
            }]
        return completed

    regenerate.side_effect = publish_and_capture
    wait_refresh.side_effect = refresh_and_capture
    with patch("services.event_sync_resolver.datetime") as resolver_clock:
        resolver_clock.now.return_value = event_start + timedelta(minutes=1)
        result = dummy_epg._manual_run(
            resume_client,
            db_session_factory,
            regenerate,
            wait_refresh,
        )

    assert result["success"] is True
    summary = result["event_sync"][0]["promotion"]
    assert summary["promoted_created"] == 0
    assert summary["promoted_adopted"] == 1
    assert summary["streams_attached"] == 1
    assert summary["guide_pending"] == 0
    resume_client.create_channel.assert_not_awaited()
    channel = state.channels[900]
    assert channel["uuid"] == "event-900"
    assert channel["epg_data_id"] == 502
    assert channel["streams"] == [7301]
    assert channel["hidden_from_output"] is False
    assert _managed_ids(db_session_factory, rule_id) == [900]

    session = db_session_factory()
    try:
        saved_profile = session.get(DummyEPGProfile, profile_id)
        assert saved_profile.get_channel_group_ids() == [
            MASTER_GROUP_ID,
            PROMOTE_GROUP_ID,
        ]
    finally:
        session.close()

    assert len(publication_snapshots) == 2
    assert len(refresh_snapshots) == 2
    with patch(
        "services.epg_publication.get_session",
        side_effect=db_session_factory,
    ):
        final_publication = epg_publication.read_publication(
            f"profile:{profile_id}"
        )
    snapshots = [
        publication_snapshots[0],
        cancelled_publication,
        publication_snapshots[1],
        *refresh_snapshots,
        final_publication,
    ]
    receipts = [
        next(iter(row["state"]["delivery"]["pending_channels"].values()))
        for row in snapshots
    ]
    stable_fields = {
        "attempt_id",
        "attempt_no",
        "input_hash",
        "admitted_at",
        "expires_at",
        "guide_attempt_id",
        "history",
        "channel_id",
        "channel_uuid",
        "event_key",
        "rule_id",
        "rule_hash",
        "config_hash",
        "profile_id",
        "target_group_id",
        "title",
        "start",
        "stop",
        "streams",
        "channel_name",
        "execution_id",
    }
    expected_fields = {
        name: receipts[0][name]
        for name in stable_fields
    }
    assert all(
        {name: receipt[name] for name in stable_fields} == expected_fields
        for receipt in receipts[1:]
    )
    assert [receipt["stage"] for receipt in receipts] == [
        "allocated",
        "allocated",
        "allocated",
        "allocated",
        "importing",
        "complete",
    ]
    final_receipt = receipts[-1]
    assert final_receipt["channel_id"] == 900
    assert final_receipt["channel_uuid"] == "event-900"
    assert final_receipt["reason"] is None
    assert final_receipt["terminal_at"] is not None
    assert final_receipt["retry_at"] is None
    guide_expires = final_publication["state"]["delivery"]["guide_attempt"]["expires_at"]
    assert guide_expires is None
    assert refresh_expires == [guide_expires, guide_expires, guide_expires]
    assert (
        final_publication["state"]["delivery"]["guide_attempt"]["config_hash"]
        == final_publication["state"]["config_hash"]
    )
    assert final_receipt["config_hash"] != final_publication["state"]["config_hash"]
    receipt_expires = datetime.fromisoformat(final_receipt["expires_at"])
    assert receipt_expires in flow_expires


def test_failed_import_keeps_event_hidden(db_session_factory, monkeypatch):
    """A failed programme import retains the owned staged allocation."""
    from services import epg_publication

    setup = _staged_event(db_session_factory, monkeypatch)
    profile_id = setup["profile_id"]
    state = setup["state"]
    event_start = setup["event_start"]
    event_channel_name = setup["event_channel_name"]
    client = setup["client"]
    dummy_epg = setup["dummy_epg"]
    state.channels[50] = {
        "id": 50,
        "uuid": "manual-50",
        "name": "Operator channel",
        "channel_group_id": PROMOTE_GROUP_ID,
        "streams": [888],
        "epg_data_id": 999,
        "logo_id": 88,
    }
    manual_channel = copy.deepcopy(state.channels[50])
    _, regenerate, wait_refresh = dummy_epg._wire_epg(
        state,
        client,
        db_session_factory,
        regenerated_entries=[
            dummy_epg._dummy_entry(501, 100, MASTER_MERCURY),
            dummy_epg._dummy_entry(502, 900, event_channel_name),
        ],
    )
    complete_refresh = wait_refresh.complete_refresh
    imports = {"count": 0}

    async def fail_after_link(*args, **kwargs):
        imports["count"] += 1
        if imports["count"] == 1:
            completed = await complete_refresh(*args, **kwargs)
            state.channels[900]["logo_id"] = 77
            return completed
        # The source completes its import without the newly linked programme.
        state.guide_sources[0].update(status="success", updated_at=str(imports["count"]))
        return True

    client.refresh_epg_source.side_effect = fail_after_link
    with patch("services.event_sync_resolver.datetime") as resolver_clock:
        resolver_clock.now.return_value = event_start + timedelta(minutes=1)
        result = dummy_epg._manual_run(
            client,
            db_session_factory,
            regenerate,
            wait_refresh,
        )

    assert result["success"] is False
    assert imports["count"] == 2
    client.create_channel.assert_awaited_once()
    channel = state.channels[900]
    assert channel["uuid"] == "event-900"
    assert channel["epg_data_id"] == 502
    assert channel["logo_id"] == 77
    assert channel["streams"] == []
    assert channel["hidden_from_output"] is True
    assert state.channels[50] == manual_channel
    with patch(
        "services.epg_publication.get_session",
        side_effect=db_session_factory,
    ):
        publication = epg_publication.read_publication(
            f"profile:{profile_id}"
        )
    receipt = next(iter(
        publication["state"]["delivery"]["pending_channels"].values()
    ))
    assert receipt["stage"] == "failed"
    assert receipt["reason"] == "programme_missing"
    assert receipt["channel_id"] == 900
    assert receipt["channel_uuid"] == "event-900"
    assert receipt["terminal_at"] is not None
    assert receipt["retry_at"] is not None


def test_profile_change_loses_group_claim_without_external_mutation(
    db_session_factory,
    monkeypatch,
):
    """A profile edit wins cleanly over the staged group-add claim."""
    from models import DummyEPGProfile
    from services import epg_publication

    setup = _staged_event(db_session_factory, monkeypatch)
    profile_id = setup["profile_id"]
    state = setup["state"]
    event_start = setup["event_start"]
    event_channel_name = setup["event_channel_name"]
    client = setup["client"]
    dummy_epg = setup["dummy_epg"]
    _, regenerate, wait_refresh = dummy_epg._wire_epg(
        state,
        client,
        db_session_factory,
        regenerated_entries=[
            dummy_epg._dummy_entry(501, 100, MASTER_MERCURY),
            dummy_epg._dummy_entry(502, 900, event_channel_name),
        ],
    )
    add_groups = epg_publication.add_groups
    publications = []

    def change_profile_then_lose(*args, **kwargs):
        publications.append(copy.deepcopy(
            epg_publication.read_publication(f"profile:{profile_id}")
        ))
        session = db_session_factory()
        try:
            profile = session.get(DummyEPGProfile, profile_id)
            profile.name = "Externally changed"
            session.commit()
        finally:
            session.close()
        result = add_groups(*args, **kwargs)
        publications.append(copy.deepcopy(
            epg_publication.read_publication(f"profile:{profile_id}")
        ))
        return result

    monkeypatch.setattr(
        epg_publication,
        "add_groups",
        change_profile_then_lose,
    )
    with patch("services.event_sync_resolver.datetime") as resolver_clock:
        resolver_clock.now.return_value = event_start + timedelta(minutes=1)
        result = dummy_epg._manual_run(
            client,
            db_session_factory,
            regenerate,
            wait_refresh,
        )

    assert result["success"] is False
    assert publications[1] == publications[0]
    assert publications[0]["state"]["delivery"]["pending_channels"]
    client.create_channel.assert_awaited_once()
    regenerate.assert_not_awaited()
    client.refresh_epg_source.assert_not_awaited()
    channel = state.channels[900]
    assert channel["uuid"] == "event-900"
    assert channel["streams"] == []
    assert channel["hidden_from_output"] is True
    assert "epg_data_id" not in channel
    receipt = next(iter(
        publications[1]["state"]["delivery"]["pending_channels"].values()
    ))
    assert receipt["stage"] == "allocated"
    assert receipt["channel_id"] == 900
    assert receipt["channel_uuid"] == "event-900"
    session = db_session_factory()
    try:
        profile = session.get(DummyEPGProfile, profile_id)
        assert profile.name == "Externally changed"
        assert profile.get_channel_group_ids() == [MASTER_GROUP_ID]
    finally:
        session.close()


def _read_event_publication(session_factory, profile_id):
    from services import epg_publication

    with patch(
        "services.epg_publication.get_session",
        side_effect=session_factory,
    ):
        return epg_publication.read_publication(f"profile:{profile_id}")


class _DeadlineLock:
    def __init__(self, current, expires_at, target):
        self.current = current
        self.expires_at = expires_at
        self.target = target
        self.entries = 0

    async def __aenter__(self):
        self.entries += 1
        if self.entries == self.target:
            self.current[0] = self.expires_at
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _CancelledLock:
    async def __aenter__(self):
        raise asyncio.CancelledError

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def _pending_completion(db_session_factory, monkeypatch, *, dedicated=False):
    setup = _staged_event(db_session_factory, monkeypatch, dedicated=dedicated)
    profile_id = setup["profile_id"]
    state = setup["state"]
    client = setup["client"]
    dummy_epg = setup["dummy_epg"]
    _, regenerate, wait_refresh = dummy_epg._wire_epg(
        state,
        client,
        db_session_factory,
        regenerated_entries=([
            dummy_epg._dummy_entry(502, 900, setup["event_channel_name"]),
        ] if dedicated else [
            dummy_epg._dummy_entry(501, 100, MASTER_MERCURY),
            dummy_epg._dummy_entry(502, 900, setup["event_channel_name"]),
        ]),
    )
    if dedicated:
        state.guide_sources[0]["is_active"] = True
    finish = ActionExecutor._finish_event_promotions
    captured = []

    async def pause(executor):
        captured.append(executor)
        raise asyncio.CancelledError

    with patch.object(ActionExecutor, "_finish_event_promotions", new=pause), \
         patch("services.event_sync_resolver.datetime") as resolver_clock:
        resolver_clock.now.return_value = setup["event_start"] + timedelta(minutes=1)
        with pytest.raises(asyncio.CancelledError):
            dummy_epg._manual_run(
                client,
                db_session_factory,
                regenerate,
                wait_refresh,
            )

    assert len(captured) == 1
    executor = captured[0]
    assert len(executor._event_pending) == 1
    publication = _read_event_publication(db_session_factory, profile_id)
    receipt = next(iter(
        publication["state"]["delivery"]["pending_channels"].values()
    ))
    assert receipt["stage"] == "importing"
    assert state.channels[900]["hidden_from_output"] is True
    assert state.channels[900]["streams"] == []
    return setup, executor, finish


def _add_pending_sibling(setup, executor, session_factory):
    from models import DummyEPGProfile
    from services.epg_publication import begin_delivery
    from tasks.event_visibility import _source_refresh_key

    profile_id = setup["profile_id"]
    current = _read_event_publication(session_factory, profile_id)
    first_key = next(iter(executor._event_pending))
    first_receipt = current["state"]["delivery"]["pending_channels"][first_key]
    work = executor._event_pending[first_key]
    source = next(
        row for row in executor._epg_sources
        if row.get("id") == work["source_id"]
    )
    _, endpoint_hash, source_url_hash = _source_refresh_key(
        executor.client,
        source,
        current["scope"],
    )
    sibling_key = f"{first_key}|sibling"
    candidate = {
        "event_key": sibling_key,
        "rule_id": first_receipt["rule_id"],
        "rule_hash": first_receipt["rule_hash"],
        "profile_id": profile_id,
        "target_group_id": first_receipt["target_group_id"],
        "title": "Sibling event",
        "start": first_receipt["start"],
        "stop": first_receipt["stop"],
        "streams": copy.deepcopy(first_receipt["streams"]),
        "channel_name": "Sibling Event",
        "channel_id": 901,
        "channel_uuid": "event-901",
        "execution_id": first_receipt["execution_id"],
        "source_hashes": [{
            "endpoint_hash": endpoint_hash,
            "source_url_hash": source_url_hash,
        }],
        "owner_proven": True,
        "channel_exists": True,
        "health_playable": True,
    }
    setup["state"].channels[901] = {
        "id": 901,
        "uuid": "event-901",
        "name": "Sibling Event",
        "channel_group_id": PROMOTE_GROUP_ID,
        "streams": [],
        "hidden_from_output": True,
    }
    executor._pipeline_managed_channel_ids.add(901)
    session = session_factory()
    try:
        profile = session.get(DummyEPGProfile, profile_id).to_dict()
        rule = session.get(ChannelPipelineRule, setup["rule_id"])
        rule.set_managed_channel_ids([900, 901])
        session.commit()
    finally:
        session.close()
    now = datetime.fromisoformat(first_receipt["admitted_at"]) + timedelta(seconds=1)
    with patch(
        "services.epg_publication.get_session",
        side_effect=session_factory,
    ):
        admitted = begin_delivery(
            current["scope"],
            expected_revision=current["revision"],
            expected_hash=current["state"]["xmltv_hash"],
            profile=profile,
            now=now,
            pending_channels={sibling_key: candidate},
        )
    assert admitted is not None
    return admitted, first_key, sibling_key, now


def test_expiry_during_the_completion_grid_closes_only_its_receipt(
    db_session_factory,
    monkeypatch,
):
    setup, executor, finish = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    profile_id = setup["profile_id"]
    client = setup["client"]
    state = setup["state"]
    before = _read_event_publication(db_session_factory, profile_id)
    event_key, before_receipt = next(iter(
        before["state"]["delivery"]["pending_channels"].items()
    ))
    expires_at = datetime.fromisoformat(before_receipt["expires_at"])
    current = [datetime.fromisoformat(before_receipt["admitted_at"])]
    grid = client.get_epg_programmes.side_effect
    observed = []

    async def expire_during_grid(*args, **kwargs):
        observed.append(_read_event_publication(
            db_session_factory,
            profile_id,
        ))
        current[0] = expires_at
        return await grid(*args, **kwargs)

    client.get_epg_programmes.side_effect = expire_during_grid
    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(lambda: current[0])):
        _run(finish(executor))

    after = _read_event_publication(db_session_factory, profile_id)
    after_receipt = after["state"]["delivery"]["pending_channels"][event_key]
    assert observed == [before]
    immutable = {
        "event_key", "rule_id", "rule_hash", "config_hash", "profile_id",
        "target_group_id", "title", "start", "stop", "streams",
        "channel_name", "channel_id", "channel_uuid", "execution_id",
        "attempt_id", "attempt_no", "input_hash", "admitted_at",
        "expires_at", "guide_attempt_id", "history",
    }
    assert {
        name: after_receipt[name] for name in immutable
    } == {
        name: before_receipt[name] for name in immutable
    }
    assert after_receipt["stage"] == "expired"
    assert after_receipt["reason"] == "guide_expired"
    assert after_receipt["terminal_at"] == expires_at.isoformat()
    assert datetime.fromisoformat(after_receipt["retry_at"]) == (
        expires_at + timedelta(minutes=5)
    )
    assert after["revision"] == before["revision"] + 1
    assert after["state"]["delivery"]["guide_attempt"]["stage"] == "expired"
    assert state.channels[900]["hidden_from_output"] is True
    assert state.channels[900]["streams"] == []
    assert all(
        payload.get("hidden_from_output") is not False
        and "streams" not in payload
        for channel_id, payload in state.update_channel_calls
        if channel_id == 900
    )


def test_fresh_completion_keeps_the_existing_success_path(
    db_session_factory,
    monkeypatch,
):
    setup, executor, finish = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    before = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    event_key, receipt = next(iter(
        before["state"]["delivery"]["pending_channels"].items()
    ))
    current = datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)

    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(current)):
        _run(finish(executor))

    after = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    completed = after["state"]["delivery"]["pending_channels"][event_key]
    assert completed["stage"] == "complete"
    assert completed["reason"] is None
    assert completed["terminal_at"] == current.isoformat()
    assert completed["retry_at"] is None
    assert setup["state"].channels[900]["hidden_from_output"] is False
    assert setup["state"].channels[900]["streams"] == [7301]


@pytest.mark.parametrize("fault", [
    "row_id", "tvg_id", "title", "start", "stop", "missing", "parsing",
])
def test_current_programme_mismatch_keeps_staged_channel_unpublished(
    fault,
    db_session_factory,
    monkeypatch,
):
    setup, executor, finish = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    row = next(
        item for item in setup["state"].guide_programmes
        if item["epg_data_id"] == 502
    )
    if fault == "row_id":
        row["epg_data_id"] = 999
    elif fault == "tvg_id":
        row["tvg_id"] = "foreign"
    elif fault == "title":
        row["title"] = "Different event"
    elif fault == "start":
        row["start_time"] = (
            datetime.fromisoformat(row["start_time"]) + timedelta(minutes=1)
        ).isoformat()
    elif fault == "stop":
        row["end_time"] = (
            datetime.fromisoformat(row["end_time"]) + timedelta(minutes=1)
        ).isoformat()
    elif fault == "missing":
        row.pop("title")
    else:
        row["parsing"] = True
    before = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    event_key, receipt = next(iter(
        before["state"]["delivery"]["pending_channels"].items()
    ))
    current = datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)

    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(current)):
        _run(finish(executor))

    after = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    closed = after["state"]["delivery"]["pending_channels"][event_key]
    assert (closed["stage"], closed["reason"]) == (
        "failed", "programme_missing",
    )
    assert setup["state"].channels[900]["hidden_from_output"] is True
    assert setup["state"].channels[900]["streams"] == []


def test_foreign_link_after_programme_read_loses_authority_without_mutation(
    db_session_factory,
    monkeypatch,
):
    setup, executor, finish = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    before = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    receipt = next(iter(
        before["state"]["delivery"]["pending_channels"].values()
    ))
    current = datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)
    read_programmes = setup["client"].get_epg_programmes.side_effect
    write_count = len(setup["state"].update_channel_calls)

    async def remap_after_read(*args, **kwargs):
        rows = await read_programmes(*args, **kwargs)
        setup["state"].channels[900]["epg_data_id"] = 999
        return rows

    setup["client"].get_epg_programmes.side_effect = remap_after_read
    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(current)):
        _run(finish(executor))

    assert _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    ) == before
    assert len(setup["state"].update_channel_calls) == write_count
    assert setup["state"].channels[900]["epg_data_id"] == 999
    assert setup["state"].channels[900]["hidden_from_output"] is True
    assert setup["state"].channels[900]["streams"] == []


def test_expiry_after_flow_stops_before_link_or_channel_mutation(
    db_session_factory,
    monkeypatch,
):
    from services import event_sync_stream_health

    setup, executor, finish = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    publication = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    event_key, receipt = next(iter(
        publication["state"]["delivery"]["pending_channels"].items()
    ))
    expires_at = datetime.fromisoformat(receipt["expires_at"])
    current = [datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)]
    collect = event_sync_stream_health.collect_stream_flow

    async def expire_after_flow(*args, **kwargs):
        result = await collect(*args, **kwargs)
        current[0] = expires_at
        return result

    monkeypatch.setattr(
        event_sync_stream_health,
        "collect_stream_flow",
        expire_after_flow,
    )
    setup["client"].get_channel.reset_mock()
    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(lambda: current[0])):
        _run(finish(executor))

    after = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    assert after["state"]["delivery"]["pending_channels"][event_key][
        "stage"
    ] == "expired"
    assert setup["client"].get_channel.await_count == 3
    assert setup["state"].channels[900]["streams"] == []
    assert setup["state"].channels[900]["hidden_from_output"] is True


@pytest.mark.parametrize("boundary,target,stale", [
    ("linking", 1, False),
    ("attach", 2, False),
    ("stale_removal", 3, True),
    ("reveal", 3, False),
    ("completion", 4, False),
])
def test_expiry_at_a_publication_guard_closes_outside_the_lock(
    boundary,
    target,
    stale,
    db_session_factory,
    monkeypatch,
):
    from services import epg_publication

    setup, executor, finish = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    publication = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    event_key, receipt = next(iter(
        publication["state"]["delivery"]["pending_channels"].items()
    ))
    expires_at = datetime.fromisoformat(receipt["expires_at"])
    current = [datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)]
    work = executor._event_pending[event_key]
    if stale:
        parsed = work["unit"].rows[0].result.parsed
        stale_row = ResolvedStream(
            stream=SecondaryStream(
                name="Delisted Fury stream",
                group_id=SECONDARY_B,
                stream_id=7399,
                provider="Prov",
                provider_id=2,
                is_stale=True,
            ),
            result=StreamMatchResult(
                stream_name="Delisted Fury stream",
                parsed=parsed,
            ),
            disposition=DISPOSITION_UNMATCHED,
            best=None,
        )
        work["stale_rows"] = {7399: stale_row}
        work["unit_stream_ids"].add(7399)
        setup["state"].channels[900]["streams"] = [7399]

    deadline_lock = _DeadlineLock(current, expires_at, target)
    monkeypatch.setattr(epg_publication, "publication_lock", deadline_lock)
    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(lambda: current[0])):
        _run(finish(executor))

    after = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    closed = after["state"]["delivery"]["pending_channels"][event_key]
    assert deadline_lock.entries > target
    assert closed["stage"] == "expired", boundary
    assert closed["reason"] == "guide_expired"
    assert closed["terminal_at"] == expires_at.isoformat()
    if boundary in {"linking", "attach"}:
        assert setup["state"].channels[900]["streams"] == []
    elif stale:
        assert 7399 in setup["state"].channels[900]["streams"]
    else:
        assert setup["state"].channels[900]["streams"] == [7301]
    if boundary != "completion":
        assert setup["state"].channels[900]["hidden_from_output"] is True


@pytest.mark.parametrize("mutation", ["attach", "reveal"])
def test_an_admitted_mutation_that_outlives_expiry_starts_no_followup(
    mutation,
    db_session_factory,
    monkeypatch,
):
    setup, executor, finish = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    publication = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    event_key, receipt = next(iter(
        publication["state"]["delivery"]["pending_channels"].items()
    ))
    expires_at = datetime.fromisoformat(receipt["expires_at"])
    current = [datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)]
    client = setup["client"]
    client.get_channel.reset_mock()
    if mutation == "attach":
        attach = executor._add_stream_to_channel

        async def attach_then_expire(*args, **kwargs):
            result = await attach(*args, **kwargs)
            current[0] = expires_at
            return result

        monkeypatch.setattr(
            executor,
            "_add_stream_to_channel",
            attach_then_expire,
        )
    else:
        update = client.update_channel.side_effect

        async def reveal_then_expire(channel_id, changes):
            result = await update(channel_id, changes)
            if changes.get("hidden_from_output") is False:
                current[0] = expires_at
            return result

        client.update_channel.side_effect = reveal_then_expire

    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(lambda: current[0])):
        _run(finish(executor))

    after = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    assert after["state"]["delivery"]["pending_channels"][event_key][
        "stage"
    ] == "expired"
    assert setup["state"].channels[900]["streams"] == [7301]
    assert client.get_channel.await_count == (4 if mutation == "attach" else 5)
    assert setup["state"].channels[900]["hidden_from_output"] is (
        mutation == "attach"
    )


@pytest.mark.parametrize("read_number,expire,expected_stage", [
    (1, False, "failed"),
    (1, True, "expired"),
    (2, False, "failed"),
    (2, True, "expired"),
    (3, False, "failed"),
    (3, True, "expired"),
    (4, False, "failed"),
    (4, True, "expired"),
])
def test_channel_read_failure_uses_live_failure_or_expiry(
    read_number,
    expire,
    expected_stage,
    db_session_factory,
    monkeypatch,
):
    setup, executor, finish = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    publication = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    event_key, receipt = next(iter(
        publication["state"]["delivery"]["pending_channels"].items()
    ))
    expires_at = datetime.fromisoformat(receipt["expires_at"])
    current = [datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)]
    client = setup["client"]
    get_channel = client.get_channel.side_effect
    reads = {"count": 0}

    async def fail_selected_read(channel_id):
        reads["count"] += 1
        if reads["count"] == read_number:
            if expire:
                current[0] = expires_at
            raise RuntimeError("channel unavailable")
        return await get_channel(channel_id)

    client.get_channel.side_effect = fail_selected_read
    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(lambda: current[0])):
        _run(finish(executor))

    after = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    closed = after["state"]["delivery"]["pending_channels"][event_key]
    assert reads["count"] == read_number
    assert closed["stage"] == expected_stage
    assert closed["reason"] == (
        "guide_expired" if expire else "channel_missing"
    )
    assert closed["terminal_at"] == current[0].isoformat()
    assert closed["retry_at"] == (
        (current[0] + timedelta(minutes=5)).isoformat() if expire else None
    )
    for field in (
        "admitted_at", "expires_at", "attempt_id", "attempt_no", "history",
        "rule_id", "rule_hash", "profile_id", "target_group_id", "event_key",
        "channel_id", "channel_uuid", "execution_id", "start", "stop",
        "guide_attempt_id", "config_hash", "input_hash", "streams", "channel_name", "title",
    ):
        assert closed[field] == receipt[field]
    guide_before = publication["state"]["delivery"]["guide_attempt"]
    guide_after = after["state"]["delivery"]["guide_attempt"]
    for field in ("admitted_at", "expires_at", "attempt_id", "config_hash"):
        assert guide_after[field] == guide_before[field]


@pytest.mark.parametrize("stages,changes,guide_stage", [
    (
        {"allocated"},
        {
            "stage": "expired",
            "reason": "guide_expired",
            "terminal_at": "2026-01-01T00:00:00+00:00",
            "retry_at": None,
            "detail": "extra",
        },
        None,
    ),
    (
        {"allocated"},
        {
            "stage": "expired",
            "reason": "guide_failed",
            "terminal_at": "2026-01-01T00:00:00+00:00",
            "retry_at": None,
        },
        None,
    ),
    (
        {"allocated"},
        {
            "stage": "expired",
            "reason": "guide_expired",
            "terminal_at": "2026-01-01T00:00:00+00:00",
            "retry_at": None,
        },
        "linking",
    ),
    (
        {"allocated", "complete"},
        {
            "stage": "expired",
            "reason": "guide_expired",
            "terminal_at": "2026-01-01T00:00:00+00:00",
            "retry_at": None,
        },
        None,
    ),
    (
        set(),
        {
            "stage": "expired",
            "reason": "guide_expired",
            "terminal_at": "2026-01-01T00:00:00+00:00",
            "retry_at": None,
        },
        None,
    ),
])
def test_expired_writer_accepts_only_its_terminal_shape(
    stages,
    changes,
    guide_stage,
):
    executor = ActionExecutor(MagicMock(), [])

    with pytest.raises(ValueError, match="Invalid expired event receipt transition"):
        _run(executor._write_event_receipt(
            {},
            "event",
            stages,
            changes,
            guide_stage=guide_stage,
        ))


def test_fresh_receipt_cannot_use_the_expired_writer(
    db_session_factory,
    monkeypatch,
):
    setup, executor, _ = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    before = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    event_key, receipt = next(iter(
        before["state"]["delivery"]["pending_channels"].items()
    ))
    current = datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)
    channel = copy.deepcopy(setup["state"].channels[900])

    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(current)):
        closed = _run(executor._write_event_receipt(
            before,
            event_key,
            {"allocated", "importing", "linking", "ready"},
            {
                "stage": "expired",
                "reason": "guide_expired",
                "terminal_at": current.isoformat(),
                "retry_at": (current + timedelta(minutes=5)).isoformat(),
            },
            channel=channel,
        ))

    assert closed is None
    assert _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    ) == before


def test_receipt_guard_keeps_live_and_expired_authority_separate(
    db_session_factory,
    monkeypatch,
):
    setup, executor, _ = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    publication = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    event_key, receipt = next(iter(
        publication["state"]["delivery"]["pending_channels"].items()
    ))
    channel = copy.deepcopy(setup["state"].channels[900])
    live_at = datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)
    expires_at = datetime.fromisoformat(receipt["expires_at"])

    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(live_at)):
        assert executor._event_receipt_current(
            publication,
            event_key,
            channel=channel,
        ) is not None
        assert executor._event_receipt_current(
            publication,
            event_key,
            channel=channel,
            expired=True,
        ) is None

    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(expires_at)):
        assert executor._event_receipt_current(
            publication,
            event_key,
            channel=channel,
        ) is None
        assert executor._event_receipt_current(
            publication,
            event_key,
            channel=channel,
            expired=True,
        ) is not None
        assert executor._event_receipt_current(
            publication,
            event_key,
            channel_missing=True,
            expired=True,
        ) is not None


@pytest.mark.parametrize("mismatch", [
    "revision",
    "xmltv_hash",
    "config_hash",
    "guide_attempt",
    "receipt_attempt",
    "profile_disabled",
    "profile_changed",
    "rule_disabled",
    "rule_changed",
    "managed_channel",
    "channel_group",
    "channel_uuid",
    "missing_channel",
    "missing_channel_with_evidence",
    "replacement",
])
def test_expired_writer_cannot_close_a_mismatched_owner(
    mismatch,
    db_session_factory,
    monkeypatch,
):
    from models import DummyEPGProfile

    setup, executor, _ = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    profile_id = setup["profile_id"]
    expected = _read_event_publication(db_session_factory, profile_id)
    event_key, receipt = next(iter(
        expected["state"]["delivery"]["pending_channels"].items()
    ))
    expected = copy.deepcopy(expected)
    channel = copy.deepcopy(setup["state"].channels[900])
    channel_missing = False
    durable_before = copy.deepcopy(expected)

    if mismatch == "revision":
        expected["revision"] += 1
    elif mismatch == "xmltv_hash":
        expected["state"]["xmltv_hash"] = "0" * 64
    elif mismatch == "config_hash":
        expected["state"]["config_hash"] = "0" * 64
    elif mismatch == "guide_attempt":
        expected["state"]["delivery"]["guide_attempt"]["attempt_id"] = "0" * 32
    elif mismatch == "receipt_attempt":
        expected["state"]["delivery"]["pending_channels"][event_key][
            "attempt_id"
        ] = "0" * 32
    elif mismatch in {
        "profile_disabled", "profile_changed", "rule_disabled",
        "rule_changed", "managed_channel",
    }:
        session = db_session_factory()
        try:
            profile = session.get(DummyEPGProfile, profile_id)
            rule = session.get(ChannelPipelineRule, setup["rule_id"])
            if mismatch == "profile_disabled":
                profile.enabled = False
            elif mismatch == "profile_changed":
                profile.name = "Changed profile"
            elif mismatch == "rule_disabled":
                rule.enabled = False
            elif mismatch == "rule_changed":
                config = rule.get_event_sync_config()
                config["max_promote_per_run"] = 24
                rule.set_event_sync_config(config)
            else:
                rule.set_managed_channel_ids([])
            session.commit()
        finally:
            session.close()
    elif mismatch == "channel_group":
        channel["channel_group_id"] += 1
    elif mismatch == "channel_uuid":
        channel["uuid"] = "replacement-900"
    elif mismatch == "missing_channel":
        channel = None
    elif mismatch == "missing_channel_with_evidence":
        channel_missing = True
    else:
        live_at = datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)
        with patch("database.get_session", side_effect=db_session_factory), \
             patch(
                 "services.epg_publication.get_session",
                 side_effect=db_session_factory,
             ), \
             patch("channel_pipeline_executor.datetime", _clock(live_at)):
            replacement = _run(executor._write_event_receipt(
                expected,
                event_key,
                {"allocated", "importing", "linking", "ready"},
                {"stage": "linking", "reason": "guide_pending"},
                guide_stage="linking",
                channel=channel,
            ))
        assert replacement is not None
        durable_before = replacement

    expires_at = datetime.fromisoformat(receipt["expires_at"])
    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(expires_at)):
        closed = _run(executor._write_event_receipt(
            expected,
            event_key,
            {"allocated", "importing", "linking", "ready"},
            {
                "stage": "expired",
                "reason": "guide_expired",
                "terminal_at": expires_at.isoformat(),
                "retry_at": (expires_at + timedelta(minutes=5)).isoformat(),
            },
            channel=channel,
            channel_missing=channel_missing,
        ))

    assert closed is None
    assert _read_event_publication(db_session_factory, profile_id) == durable_before


@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("missing", [False, True])
@pytest.mark.parametrize("foreign", [False, True])
def test_completion_rejects_removed_current_rule_ownership(
    expired, missing, foreign, db_session_factory, monkeypatch,
):
    setup, executor, finish = _pending_completion(db_session_factory, monkeypatch)
    before = _read_event_publication(db_session_factory, setup["profile_id"])
    receipt = next(iter(before["state"]["delivery"]["pending_channels"].values()))
    assert receipt["channel_id"] == 900
    assert 900 in executor._pipeline_managed_channel_ids
    session = db_session_factory()
    try:
        rule = session.get(ChannelPipelineRule, receipt["rule_id"])
        config = rule.get_event_sync_config()
        assert rule.get_managed_channel_ids() == [900]
        rule.set_managed_channel_ids([])
        session.commit()
    finally:
        session.close()
    if foreign:
        foreign_id = _add_rule(db_session_factory, config)
        session = db_session_factory()
        try:
            other = session.get(ChannelPipelineRule, foreign_id)
            other.set_managed_channel_ids([900])
            session.commit()
            assert other.id != receipt["rule_id"]
            assert other.get_managed_channel_ids() == [900]
        finally:
            session.close()
    current = (
        datetime.fromisoformat(receipt["expires_at"]) if expired
        else datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)
    )
    channel = copy.deepcopy(setup["state"].channels[900])
    setup["client"].update_channel.reset_mock()
    if missing:
        setup["client"].get_channel.side_effect = RuntimeError("channel unavailable")

    with patch("database.get_session", side_effect=db_session_factory), \
         patch("services.epg_publication.get_session", side_effect=db_session_factory), \
         patch("channel_pipeline_executor.datetime", _clock(current)):
        assert executor._event_receipt_current(
            before, receipt["event_key"], channel=None if missing else channel,
            channel_missing=missing, expired=expired,
        ) is None
        _run(finish(executor))

    assert _read_event_publication(db_session_factory, setup["profile_id"]) == before
    assert setup["state"].channels[900] == channel
    setup["client"].update_channel.assert_not_awaited()


@pytest.mark.parametrize("phase", ["guide", "flow", "lock"])
def test_completion_cancellation_never_writes_expiry(
    phase,
    db_session_factory,
    monkeypatch,
):
    from services import epg_publication, event_sync_stream_health

    setup, executor, finish = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    profile_id = setup["profile_id"]
    before = _read_event_publication(db_session_factory, profile_id)
    receipt = next(iter(
        before["state"]["delivery"]["pending_channels"].values()
    ))
    current = datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)
    if phase == "guide":
        setup["client"].get_epg_programmes.side_effect = asyncio.CancelledError()
    elif phase == "flow":
        monkeypatch.setattr(
            event_sync_stream_health,
            "collect_stream_flow",
            AsyncMock(side_effect=asyncio.CancelledError),
        )
    else:
        monkeypatch.setattr(
            epg_publication,
            "publication_lock",
            _CancelledLock(),
        )

    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(current)):
        with pytest.raises(asyncio.CancelledError):
            _run(finish(executor))

    assert _read_event_publication(db_session_factory, profile_id) == before
    assert setup["state"].channels[900]["hidden_from_output"] is True
    assert setup["state"].channels[900]["streams"] == []


def test_expiring_one_receipt_keeps_an_active_sibling_and_is_idempotent(
    db_session_factory,
    monkeypatch,
):
    setup, executor, _ = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    publication, first_key, sibling_key, _ = _add_pending_sibling(
        setup,
        executor,
        db_session_factory,
    )
    first = publication["state"]["delivery"]["pending_channels"][first_key]
    expires_at = datetime.fromisoformat(first["expires_at"])
    first_channel = copy.deepcopy(setup["state"].channels[900])
    sibling_channel = copy.deepcopy(setup["state"].channels[901])

    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(expires_at)):
        first_closed = _run(executor._write_event_receipt(
            publication,
            first_key,
            {"allocated", "importing", "linking", "ready"},
            {
                "stage": "expired",
                "reason": "guide_expired",
                "terminal_at": expires_at.isoformat(),
                "retry_at": (expires_at + timedelta(minutes=5)).isoformat(),
            },
            channel=first_channel,
        ))
        assert first_closed is not None
        assert first_closed["state"]["delivery"]["pending_channels"][
            sibling_key
        ]["stage"] == "allocated"
        assert first_closed["state"]["delivery"]["guide_attempt"]["stage"] \
            not in {"complete", "expired", "failed"}
        repeated = _run(executor._write_event_receipt(
            first_closed,
            first_key,
            {"allocated", "importing", "linking", "ready"},
            {
                "stage": "expired",
                "reason": "guide_expired",
                "terminal_at": expires_at.isoformat(),
                "retry_at": (expires_at + timedelta(minutes=5)).isoformat(),
            },
            channel=first_channel,
        ))
        assert repeated is None
        both_closed = _run(executor._write_event_receipt(
            first_closed,
            sibling_key,
            {"allocated", "importing", "linking", "ready"},
            {
                "stage": "expired",
                "reason": "guide_expired",
                "terminal_at": expires_at.isoformat(),
                "retry_at": (expires_at + timedelta(minutes=5)).isoformat(),
            },
            channel=sibling_channel,
        ))

    assert both_closed is not None
    assert both_closed["state"]["delivery"]["guide_attempt"]["stage"] == "expired"
    assert _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    ) == both_closed


def test_mixed_terminal_receipts_fail_the_shared_guide_attempt(
    db_session_factory,
    monkeypatch,
):
    setup, executor, _ = _pending_completion(
        db_session_factory,
        monkeypatch,
    )
    publication, first_key, sibling_key, live_at = _add_pending_sibling(
        setup,
        executor,
        db_session_factory,
    )
    sibling_channel = copy.deepcopy(setup["state"].channels[901])
    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(live_at)):
        completed = _run(executor._write_event_receipt(
            publication,
            sibling_key,
            {"allocated", "importing", "linking", "ready"},
            {
                "stage": "complete",
                "reason": None,
                "terminal_at": live_at.isoformat(),
                "retry_at": None,
            },
            channel=sibling_channel,
        ))
    assert completed is not None
    first = completed["state"]["delivery"]["pending_channels"][first_key]
    expires_at = datetime.fromisoformat(first["expires_at"])
    with patch("database.get_session", side_effect=db_session_factory), \
         patch(
             "services.epg_publication.get_session",
             side_effect=db_session_factory,
         ), \
         patch("channel_pipeline_executor.datetime", _clock(expires_at)):
        mixed = _run(executor._write_event_receipt(
            completed,
            first_key,
            {"allocated", "importing", "linking", "ready"},
            {
                "stage": "expired",
                "reason": "guide_expired",
                "terminal_at": expires_at.isoformat(),
                "retry_at": (expires_at + timedelta(minutes=5)).isoformat(),
            },
            channel=copy.deepcopy(setup["state"].channels[900]),
        ))

    assert mixed is not None
    pending = mixed["state"]["delivery"]["pending_channels"]
    assert pending[first_key]["stage"] == "expired"
    assert pending[sibling_key]["stage"] == "complete"
    assert mixed["state"]["delivery"]["guide_attempt"]["stage"] == "failed"


class TestIdempotence:
    """AC-3: re-run creates nothing; cross-provider same-key streams share
    one channel."""

    def test_rerun_creates_zero_and_attaches_zero(self, db_session_factory):
        _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        client = make_promote_client(state)

        first, _ = _manual_run(client, db_session_factory)
        assert first["event_sync"][0]["promotion"]["promoted_created"] == 1
        patch_count_after_first = len(state.update_channel_calls)

        second, second_journal = _manual_run(client, db_session_factory)
        promo2 = second["event_sync"][0]["promotion"]
        assert promo2["promoted_created"] == 0
        assert promo2["promoted_adopted"] == 1
        assert promo2["streams_attached"] == 0
        assert promo2["already_attached"] == 1
        assert client.create_channel.await_count == 1
        # No new PATCHes, no merge-journal rows on the re-run (the manual
        # adoption audit row is the only journal artifact).
        assert len(state.update_channel_calls) == patch_count_after_first
        assert [e for e in second_journal
                if e.get("action_type") == "merge_stream"] == []
        assert second["channels_created"] == 0

    def _numbered_name_run(self, client, session_factory):
        """One run with include_channel_number_in_name on and the default
        "-" separator, so the channel is STORED as "<number> - <name>"
        while the plan keeps deriving the unprefixed name."""
        from config import DispatcharrSettings

        with patch(
            "channel_pipeline_engine.get_settings",
            return_value=DispatcharrSettings(
                include_channel_number_in_name=True,
                channel_number_separator="-",
            ),
        ):
            return _manual_run(client, session_factory)

    def test_rerun_adopts_the_channel_it_stored_under_a_number_prefix(
        self, db_session_factory
    ):
        """The re-run has to find the channel by the name it derives, not
        by the name Dispatcharr stored. Miss it and the run creates a
        duplicate, the first channel drops out of the managed set, and
        Pass 4 deletes it while the event is still ahead. [44]"""
        rule_id = _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        client = make_promote_client(state)

        first, _ = self._numbered_name_run(client, db_session_factory)
        promo1 = first["event_sync"][0]["promotion"]
        assert promo1["promoted_created"] == 1
        created_id = promo1["channel_ids"][0]
        assert state.channels[created_id]["name"].endswith(
            f"- {FURY_CHANNEL_NAME}"
        )

        second, _ = self._numbered_name_run(client, db_session_factory)
        promo2 = second["event_sync"][0]["promotion"]
        assert promo2["promoted_adopted"] == 1
        assert promo2["promoted_created"] == 0
        assert promo2["channel_ids"] == [created_id]
        assert client.create_channel.await_count == 1
        assert state.deleted_channel_ids == []
        assert _managed_ids(db_session_factory, rule_id) == [created_id]

    def test_rerun_adoption_rides_on_the_managed_ledger(
        self, db_session_factory
    ):
        """GH #801 / bead 0ippw: what makes the re-run adopt.

        This path used to force ``allow_manual_channel_merge=True`` because
        the promoted channel reloads from Dispatcharr without the in-run
        ``auto_created`` marker (the fake client reproduces that: it stores
        the create payload, which never carries the key). The override is
        gone; adoption now rides on the persisted ``managed_channel_ids``
        ledger, which ``_is_manual_channel`` consults.

        Pinning the two facts the removal depends on: run 1 registers the
        promoted channel in the ledger, and run 2's executor is handed it.
        Neuter either and the idempotence test above goes red.
        """
        rule_id = _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        client = make_promote_client(state)

        first, _ = _manual_run(client, db_session_factory)
        promoted_id = first["event_sync"][0]["promotion"]["channel_ids"][0]
        assert _managed_ids(db_session_factory, rule_id) == [promoted_id]
        # The reloaded row carries no provenance marker of its own.
        assert "auto_created" not in state.channels[promoted_id]

        with patch(
            "channel_pipeline_engine.ActionExecutor", wraps=ActionExecutor
        ) as executor_cls:
            _manual_run(client, db_session_factory)

        assert promoted_id in executor_cls.call_args.kwargs["managed_channel_ids"]


    def test_cross_provider_streams_share_one_channel(
        self, db_session_factory
    ):
        _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        # Second provider carries the SAME event under its own name shape.
        state.secondary_streams[SECONDARY_A_NAME].append(
            {"id": 7002, "name": STREAM_FURY_ALT, "m3u_account": 1},
        )
        client = make_promote_client(state)

        result, _ = _manual_run(client, db_session_factory)
        promo = result["event_sync"][0]["promotion"]
        assert promo["units"] == 1
        assert promo["promoted_created"] == 1
        assert promo["streams_attached"] == 2
        assert client.create_channel.await_count == 1
        created_id = promo["channel_ids"][0]
        assert sorted(state.stream_ids_of(created_id)) == [7002, 7301]


TYSON_CHANNEL_NAME = "Tyson Vs. Paul @ Jul 11 09:00 PM"
PRIOR_TYSON_CHANNEL = "Tyson Vs. Paul @ Jul 10 09:00 PM"


class TestPromotionExecution:
    """Each unit gets its OWN channel, or that unit alone fails.

    Every other executor-level promotion test runs under
    ``triggered_by="manual"``. The unattended trigger is ``m3u_refresh``,
    which is also the trigger that arms the bulk-M3U dedup hook, so these
    run under that one. The hook scores raw stream names with
    ``token_set_ratio`` and has no notion of time, so in a group full of
    event channels it reads tomorrow's fixture as today's channel:
    ``PRIOR_TYSON_CHANNEL`` scores 0.95 against ``STREAM_TYSON``, well over
    the 0.80 default threshold.
    """

    def _two_event_state(self):
        """Two unmatched events, plus a channel promoted by an earlier run
        whose name is the same fixture on the previous day."""
        state = _promote_state()
        state.channels[800] = {
            "id": 800, "name": PRIOR_TYSON_CHANNEL,
            "channel_group_id": PROMOTE_GROUP_ID,
            "auto_created": True, "streams": [],
        }
        state.secondary_streams[SECONDARY_B_NAME].append(
            {"id": 7501, "name": STREAM_TYSON, "m3u_account": 2},
        )
        return state

    def _refresh_run(self, client, session_factory, monkeypatch):
        """A run under the ONE unattended trigger an event_sync rule can
        take, which is also the trigger that arms the dedup hook."""
        from config import DispatcharrSettings

        monkeypatch.setattr(database, "_SessionLocal", session_factory)
        monkeypatch.setattr(
            "config.get_settings",
            lambda: DispatcharrSettings(dedup_threshold=0.80),
        )
        engine = ChannelPipelineEngine(client)
        with patch("channel_pipeline_engine.get_session",
                   side_effect=session_factory), \
             patch("journal.log_entries"), \
             patch("services.event_sync_resolver.datetime") as mock_dt:
            mock_dt.now.return_value = FROZEN_NOW
            return _run(engine.run_pipeline(dry_run=False,
                                            triggered_by="m3u_refresh"))

    def _pending_merge_count(self, session_factory) -> int:
        session = session_factory()
        try:
            return session.query(PendingMerge).count()
        finally:
            session.close()

    def test_streams_never_land_on_another_units_channel(
        self, db_session_factory, monkeypatch
    ):
        # The premise the whole test rests on: without the opt-out the
        # hook WOULD fire on this pair. Assert it here, or a change to the
        # scorer or to either name leaves every assertion below passing
        # while nothing exercises the opt-out any more. [52]
        from services.dedup_matcher import find_candidate

        armed = find_candidate(
            STREAM_TYSON, [(800, PRIOR_TYSON_CHANNEL)], 0.80
        )
        assert armed is not None
        assert armed.confidence >= 0.80

        _add_rule(db_session_factory, _promote_config(auto_run=True))
        state = self._two_event_state()
        client = make_promote_client(state)

        result = self._refresh_run(client, db_session_factory, monkeypatch)

        promo = result["event_sync"][0]["promotion"]
        assert promo["units"] == 2
        assert promo["promoted_created"] == 2
        assert promo["attach_errors"] == 0
        assert len(set(promo["channel_ids"])) == 2

        by_name = {c["name"]: c for c in state.channels.values()}
        fury_id = by_name[FURY_CHANNEL_NAME]["id"]
        tyson_id = by_name[TYSON_CHANNEL_NAME]["id"]
        assert fury_id != tyson_id
        # Each event's stream is on its own channel and nowhere else.
        assert state.stream_ids_of(fury_id) == [7301]
        assert state.stream_ids_of(tyson_id) == [7501]
        # The near-duplicate channel from the earlier run is left alone.
        assert state.stream_ids_of(800) == []
        # Promotion decides create-vs-adopt itself; it never defers a
        # channel to the operator merge queue.
        assert self._pending_merge_count(db_session_factory) == 0

    def test_create_without_a_channel_id_fails_only_its_own_unit(
        self, db_session_factory, monkeypatch, caplog
    ):
        _add_rule(db_session_factory, _promote_config(auto_run=True))
        state = self._two_event_state()
        client = make_promote_client(state)
        real_create = ActionExecutor._execute_create_channel

        async def _no_channel_for_tyson(self, action, stream_ctx, exec_ctx,
                                        template_ctx, **kwargs):
            if action.params.get("name_template") == TYSON_CHANNEL_NAME:
                return ActionResult(
                    success=True, action_type="create_channel",
                    description="channel creation deferred",
                    entity_type="channel", entity_name=TYSON_CHANNEL_NAME,
                    skipped=True,
                )
            return await real_create(self, action, stream_ctx, exec_ctx,
                                     template_ctx, **kwargs)

        monkeypatch.setattr(ActionExecutor, "_execute_create_channel",
                            _no_channel_for_tyson)
        with caplog.at_level(logging.WARNING,
                             logger="channel_pipeline_executor"):
            result = self._refresh_run(client, db_session_factory, monkeypatch)

        promo = result["event_sync"][0]["promotion"]
        assert promo["units"] == 2
        assert promo["promoted_created"] == 1
        assert promo["promoted_adopted"] == 0
        # A whole unit failed, which is one failed unit and not one failed
        # stream attach — the two are counted apart. [49]
        assert promo["failed_units"] == 1
        assert promo["attach_errors"] == 0

        by_name = {c["name"]: c for c in state.channels.values()}
        assert TYSON_CHANNEL_NAME not in by_name
        fury_id = by_name[FURY_CHANNEL_NAME]["id"]
        # The failed unit contributed nothing: not the previous unit's
        # channel, not the near-duplicate from the earlier run, and no id
        # in the managed set.
        assert state.stream_ids_of(fury_id) == [7301]
        assert state.stream_ids_of(800) == []
        assert promo["channel_ids"] == [fury_id]
        assert any(
            "produced no channel id" in r.getMessage()
            and "Event Rule" in r.getMessage()
            for r in caplog.records if r.levelno == logging.WARNING
        )

    def test_unresolvable_channel_logs_before_counting_the_error(
        self, db_session_factory, monkeypatch, caplog
    ):
        _add_rule(db_session_factory, _promote_config(auto_run=True))
        state = self._two_event_state()
        client = make_promote_client(state)
        real_create = ActionExecutor._execute_create_channel
        unknown_id = 424242

        async def _adopt_unknown_id(self, action, stream_ctx, exec_ctx,
                                    template_ctx, **kwargs):
            if action.params.get("name_template") == TYSON_CHANNEL_NAME:
                return ActionResult(
                    success=True, action_type="create_channel",
                    description="adopted", entity_type="channel",
                    entity_id=unknown_id, entity_name=TYSON_CHANNEL_NAME,
                    skipped=True,
                )
            return await real_create(self, action, stream_ctx, exec_ctx,
                                     template_ctx, **kwargs)

        monkeypatch.setattr(ActionExecutor, "_execute_create_channel",
                            _adopt_unknown_id)
        with caplog.at_level(logging.WARNING,
                             logger="channel_pipeline_executor"):
            result = self._refresh_run(client, db_session_factory, monkeypatch)

        promo = result["event_sync"][0]["promotion"]
        assert promo["promoted_adopted"] == 1
        assert promo["attach_errors"] == 1
        # The channel id is real, so it stays in the managed set — dropping
        # it would make Pass 4 read the channel as an orphan.
        assert unknown_id in promo["channel_ids"]
        fury_id = {c["name"]: c for c in state.channels.values()}[
            FURY_CHANNEL_NAME]["id"]
        assert state.stream_ids_of(fury_id) == [7301]
        assert any(
            "not in this run's channel index" in r.getMessage()
            and "Event Rule" in r.getMessage()
            for r in caplog.records if r.levelno == logging.WARNING
        )


class TestDatelessPromotion:
    """A stream carrying a time but no date never becomes a channel.

    The name identifies a recurring slot rather than one broadcast, so the
    channel it would promote to would carry a different event every day and
    the operator has no way to tell which. The count reaches the run
    summary so the rows are explained rather than silently missing.
    """

    def _dateless_state(self):
        return FakeDispatcharrState(
            channels=[{
                "id": 100, "name": MASTER_MERCURY,
                "channel_group_id": MASTER_GROUP_ID,
                "auto_created": True, "streams": [9001],
            }],
            secondary_streams={SECONDARY_B_NAME: [
                {"id": 7400, "name": "DAZN 07: FURY vs HALL 6PM",
                 "m3u_account": 2},
            ]},
        )

    def test_a_dateless_stream_never_becomes_a_channel(
        self, db_session_factory
    ):
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_B],
            assume_current_date=True,
        ))
        state = self._dateless_state()
        client = make_promote_client(state)

        first, _ = _manual_run(client, db_session_factory, now=FROZEN_NOW)
        promo1 = first["event_sync"][0]["promotion"]
        assert promo1["promoted_created"] == 0
        assert promo1["skipped_dateless"] == 1
        assert promo1["channel_ids"] == []
        client.create_channel.assert_not_awaited()

        # A re-run after simulated midnight still creates nothing, so the
        # rule cannot mint one channel per day for the same slot.
        second, _ = _manual_run(
            client, db_session_factory, now=FROZEN_NOW + timedelta(days=1)
        )
        promo2 = second["event_sync"][0]["promotion"]
        assert promo2["promoted_created"] == 0
        assert promo2["skipped_dateless"] == 1
        assert client.create_channel.await_count == 0
        client.delete_channel.assert_not_awaited()


class TestDelistedStreamSwap:
    """A promoted channel pinned to a stream the provider stopped listing.

    Measured shape from the live instance: the provider re-lists each event
    under a new id on every refresh, so a channel adopted run after run
    keeps the superseded id. The replacement is listed but probes failed,
    because its event is still days away and there is nothing to serve yet.
    Swapping unconditionally would take a channel that plays and leave it
    with a stream that does not, so the delisted stream is dropped only once
    a stream on that channel has a PASSING probe. Attaching asks only that a
    stream is not dead; detaching asks for proof that something works, and
    the gap between those two bars is exactly this class. [51]
    """

    STALE_ID = 7301
    REPLACEMENT_ID = 7302
    OTHER_EVENT_STALE_ID = 7401

    def _swap_state(self) -> FakeDispatcharrState:
        state = _promote_state()
        state.channels[850] = {
            "id": 850, "name": FURY_CHANNEL_NAME,
            "channel_group_id": PROMOTE_GROUP_ID,
            "auto_created": True, "streams": [self.STALE_ID],
        }
        state.secondary_streams[SECONDARY_B_NAME] = [
            {"id": self.STALE_ID, "name": STREAM_FURY, "m3u_account": 2,
             "is_stale": True},
            {"id": self.REPLACEMENT_ID, "name": STREAM_FURY_ALT,
             "m3u_account": 2},
        ]
        return state

    def _run_at(self, client, session_factory, now, dry_run=False,
                replacement_status="failed", probed_at=None):
        """A run whose promotion clock is pinned, with the replacement
        carrying a stored health record.

        ``failed`` is the live instance's own shape: probed twice, failed
        twice, one short of the strike threshold. ``success`` is the shape
        of a replacement that has been proven to play, which is the only
        thing that lets the delisted stream go.

        ``probed_at`` is when that record was written, defaulting to the run
        instant. The health table stores it as naive UTC.
        """
        observed_at = (probed_at or now).astimezone(
            pytz.utc
        ).replace(tzinfo=None).isoformat() + "Z"
        stats = {self.REPLACEMENT_ID: {
            "stream_id": self.REPLACEMENT_ID,
            "stream_name": STREAM_FURY_ALT,
            "probe_status": replacement_status,
            "consecutive_failures": 2 if replacement_status == "failed" else 0,
            "measured_bitrate": 5_000_000 if replacement_status == "success" else 0,
            "last_probed": observed_at,
            "is_black_screen": False if replacement_status == "success" else None,
            "black_screen_checked_at": (
                observed_at if replacement_status == "success" else None
            ),
        }}

        def _stats_for(stream_ids):
            return {sid: stats[sid] for sid in stream_ids if sid in stats}

        with patch("channel_pipeline_executor.datetime") as promote_clock, \
             patch("services.event_sync_stream_health.datetime", _clock(now)), \
             patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_for):
            promote_clock.now.return_value = now
            return _manual_run(client, session_factory, dry_run=dry_run)

    def test_the_delisted_stream_goes_once_a_passing_replacement_arrives(
        self, db_session_factory
    ):
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        client = make_promote_client(state)

        # The replacement has been probed and it answered, so there is
        # positive evidence that this channel keeps playing without the
        # stream the provider dropped.
        result, _ = self._run_at(
            client, db_session_factory, FROZEN_NOW + timedelta(hours=12),
            replacement_status="success",
        )

        promo = result["event_sync"][0]["promotion"]
        assert promo["dead_streams_skipped"] == 1
        assert promo["stale_streams_removed"] == 1
        assert state.stream_ids_of(850) == [self.REPLACEMENT_ID]
        assert 850 in promo["channel_ids"]

    def test_a_stream_added_during_the_run_survives_the_detach(
        self, db_session_factory
    ):
        """The run's channel cache is built once, at the start, and the
        detach writes a whole stream list back. Filtering the cached list
        would drop anything added in between, so the channel is re-read
        first and that list is filtered instead. [70]
        """
        concurrent_id = 7501
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        client = make_promote_client(state)

        # Something outside this run puts a stream on the channel after the
        # cache was built, so only a fresh read can see it.
        cached_get_channel = client.get_channel

        async def _get_channel_after_a_late_addition(channel_id):
            if channel_id == 850:
                streams = state.channels[850]["streams"]
                if concurrent_id not in streams:
                    streams.append(concurrent_id)
            return await cached_get_channel(channel_id)

        client.get_channel = _get_channel_after_a_late_addition

        result, _ = self._run_at(
            client, db_session_factory, FROZEN_NOW + timedelta(hours=12),
            replacement_status="success",
        )

        promo = result["event_sync"][0]["promotion"]
        assert promo["stale_streams_removed"] == 1
        assert self.STALE_ID not in state.stream_ids_of(850)
        assert concurrent_id in state.stream_ids_of(850)

    def test_the_delisted_stream_stays_when_the_replacement_only_probes_failed(
        self, db_session_factory
    ):
        """The shape of the four live channels, and the reason the bar for
        detaching is a passing probe rather than "not dead".

        The event starts at 11 PM and the run is at noon, so the
        replacement's failed probe does not count against it and it is
        attached. It is still not EVIDENCE of anything, so the delisted
        stream that currently plays stays where it is and the channel ends
        the run carrying both. [51]
        """
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        client = make_promote_client(state)

        result, _ = self._run_at(client, db_session_factory, FROZEN_NOW)

        promo = result["event_sync"][0]["promotion"]
        assert promo["stale_streams_removed"] == 0
        assert state.stream_ids_of(850) == [self.STALE_ID]
        assert 850 in promo["channel_ids"]

    def test_the_delisted_stream_stays_when_nobody_has_probed_the_replacement(
        self, db_session_factory
    ):
        """Never probed is not evidence either. A dry run probes nothing,
        so the replacement reaches the detach check with no verdict at all
        and the channel keeps both streams. [51]"""
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        client = make_promote_client(state)

        with patch("channel_pipeline_executor.datetime") as promote_clock, \
             patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   lambda stream_ids: {}):
            promote_clock.now.return_value = FROZEN_NOW
            result, _ = _manual_run(client, db_session_factory, dry_run=True)

        promo = result["event_sync"][0]["promotion"]
        assert promo["stale_streams_removed"] == 0
        assert state.update_channel_calls == []

    def test_the_delisted_stream_stays_when_nothing_else_is_playable(
        self, db_session_factory
    ):
        """The case that would break four live channels if the swap were
        unconditional: once the event is on air the replacement's failed
        probe does count, so there is nothing to attach and the delisted
        stream is the only thing still serving the event."""
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        client = make_promote_client(state)

        result, _ = self._run_at(
            client, db_session_factory, FROZEN_NOW + timedelta(days=1)
        )

        promo = result["event_sync"][0]["promotion"]
        assert promo["skipped_all_dead"] == 1
        assert promo["stale_streams_removed"] == 0
        assert state.stream_ids_of(850) == [self.STALE_ID]
        # The channel stays in the managed set, so Pass 4 leaves it alone.
        assert 850 in promo["channel_ids"]

    def test_a_failure_recorded_before_kickoff_never_turns_fatal(
        self, db_session_factory
    ):
        """A stream probed while its event was still hours away failed
        because there was nothing to serve yet. Nothing re-probes a stream
        that already has a record, so counting that verdict once the event
        starts would make it permanent and leave the event with no
        channel. [59]
        """
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        client = make_promote_client(state)

        result, _ = self._run_at(
            client, db_session_factory, FROZEN_NOW + timedelta(days=1),
            probed_at=FROZEN_NOW,
        )

        promo = result["event_sync"][0]["promotion"]
        assert promo["skipped_all_dead"] == 1
        assert state.stream_ids_of(850) == [self.STALE_ID]

    def test_an_event_whose_only_stream_is_delisted_creates_nothing(
        self, db_session_factory
    ):
        """No replacement is listed at all, so the event has nothing behind
        it. No channel is created, and the channel it already has leaves the
        managed set for the rule's orphan_action: the provider withdrawing
        every stream is its own statement that the event is gone, and this
        provider re-lists a live event under a new id within the hour. [19]
        """
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        state.secondary_streams[SECONDARY_B_NAME] = [
            {"id": self.STALE_ID, "name": STREAM_FURY, "m3u_account": 2,
             "is_stale": True},
        ]
        client = make_promote_client(state)

        result, _ = self._run_at(client, db_session_factory, FROZEN_NOW)

        promo = result["event_sync"][0]["promotion"]
        # The same two numbers the preview reports for this shape, in
        # tests/routers/test_event_sync_preview.py — a delisted stream with
        # a stored success verdict and no replacement. Preview and run read
        # staleness off the same fetch and cannot differ on it.
        assert promo["dead_streams_skipped"] == 1
        assert promo["skipped_all_dead"] == 1
        assert promo["promoted_created"] == 0
        assert promo["stale_streams_removed"] == 0
        assert state.stream_ids_of(850) == [self.STALE_ID]
        assert 850 not in promo["channel_ids"]

    def test_a_channel_nobody_has_probed_keeps_its_place(
        self, db_session_factory
    ):
        """The rail that stops the retirement above widening into a channel
        deleter. About sixty of some thirty-seven thousand streams have ever
        been probed, so an absent verdict can never read as dead: this
        channel's one stream is listed by the provider and has no health
        record at all, and it keeps its place. [19]"""
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        state.secondary_streams[SECONDARY_B_NAME] = [
            {"id": self.STALE_ID, "name": STREAM_FURY, "m3u_account": 2},
        ]
        client = make_promote_client(state)

        result, _ = self._run_at(client, db_session_factory, FROZEN_NOW)

        promo = result["event_sync"][0]["promotion"]
        assert promo["dead_streams_skipped"] == 1
        assert promo["skipped_all_dead"] == 1
        assert 850 in promo["channel_ids"]

    def test_a_dry_run_removes_nothing_and_reports_what_it_would_do(
        self, db_session_factory
    ):
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        client = make_promote_client(state)

        result, _ = self._run_at(
            client, db_session_factory, FROZEN_NOW + timedelta(hours=12),
            dry_run=True,
            replacement_status="success",
        )

        promo = result["event_sync"][0]["promotion"]
        assert promo["stale_streams_removed"] == 1
        assert state.stream_ids_of(850) == [self.STALE_ID]
        assert state.update_channel_calls == []
        would_remove = [
            r for r in result["dry_run_results"]
            if str(r["action"]).startswith("Would remove")
        ]
        assert len(would_remove) == 1
        assert would_remove[0]["stream_id"] == self.STALE_ID

    def test_only_this_event_s_delisted_stream_goes(self, db_session_factory):
        """A channel can carry a delisted stream from a different event, and
        the proof that THIS event has something working says nothing about
        that one. The channel keeps it. [55]
        """
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        state.channels[850]["streams"] = [
            self.STALE_ID, self.OTHER_EVENT_STALE_ID,
        ]
        state.secondary_streams[SECONDARY_B_NAME].append(
            {"id": self.OTHER_EVENT_STALE_ID, "name": STREAM_TYSON,
             "m3u_account": 2, "is_stale": True},
        )

        client = make_promote_client(state)

        result, _ = self._run_at(
            client, db_session_factory, FROZEN_NOW + timedelta(hours=12),
            replacement_status="success",
        )

        promo = result["event_sync"][0]["promotion"]
        assert promo["stale_streams_removed"] == 1
        assert state.stream_ids_of(850) == [
            self.OTHER_EVENT_STALE_ID, self.REPLACEMENT_ID,
        ]

    def test_the_detach_records_the_channel_it_can_be_restored_from(
        self, db_session_factory
    ):
        """Rollback restores a channel from the stream list recorded against
        it, so a detach that records anything else is not reversed. [58]
        """
        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A, SECONDARY_B],
            skip_dead_streams=True,
        ))
        state = self._swap_state()
        client = make_promote_client(state)

        self._run_at(
            client, db_session_factory, FROZEN_NOW + timedelta(hours=12),
            replacement_status="success",
        )

        from models import ChannelPipelineExecution

        session = db_session_factory()
        try:
            execution = (
                session.query(ChannelPipelineExecution)
                .order_by(ChannelPipelineExecution.id.desc())
                .first()
            )
            modified = execution.get_modified_entities()
        finally:
            session.close()

        # The list as it stood after the attach and before the detach is the
        # one only the detach can have recorded.
        detached = [
            e for e in modified
            if e["type"] == "channel" and e["id"] == 850
            and (e.get("previous") or {}).get("streams") == [
                self.STALE_ID, self.REPLACEMENT_ID,
            ]
        ]
        assert len(detached) == 1

    @pytest.mark.asyncio
    async def test_the_preview_reports_the_detach_before_the_run_does_it(
        self, async_client
    ):
        """Detaching is the one destructive thing promotion does, so the
        number the operator reads before approving a run has to be the
        number the run then performs.

        The shape is the one every first refresh produces: the channel
        carries the delisted stream alone, and the replacement is listed
        this run with a passing probe. The run attaches the replacement
        first and detaches the delisted stream after, so a preview that
        reads the channel as it stood before that attach sees nothing
        working on it and can only ever report zero. [1][2]
        """
        state = self._swap_state()
        client = make_promote_client(state)
        preview_now = FROZEN_NOW + timedelta(hours=12)
        stats = {self.REPLACEMENT_ID: {
            "stream_id": self.REPLACEMENT_ID,
            "stream_name": STREAM_FURY_ALT,
            "probe_status": "success",
            "consecutive_failures": 0,
            "measured_bitrate": 5_000_000,
            "last_probed": preview_now.astimezone(pytz.utc).replace(
                tzinfo=None).isoformat() + "Z",
            "is_black_screen": False,
            "black_screen_checked_at": preview_now.astimezone(
                pytz.utc
            ).replace(tzinfo=None).isoformat() + "Z",
        }}

        def _stats_for(stream_ids):
            return {sid: stats[sid] for sid in stream_ids if sid in stats}

        with patch("routers.channel_pipeline.get_client",
                   return_value=client), \
             patch("routers.channel_pipeline.datetime") as preview_clock, \
             patch("services.event_sync_stream_health.datetime", _clock(preview_now)), \
             patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_for):
            preview_clock.now.return_value = preview_now
            resp = await async_client.post(
                "/api/channel-pipeline/event-sync-preview",
                json={"event_sync_config": _promote_config(
                    skip_dead_streams=True,
                )},
            )

        assert resp.status_code == 200
        promo = resp.json()["promotion"]
        assert promo["stale_streams_removed"] == 1
        unit = next(u for u in promo["units"]
                    if u["existing_channel_id"] == 850)
        assert unit["action"] == PROMOTE_ACTION_ATTACH_EXISTING


class TestReconciliationLifecycle:
    """AC-5: reconciliation-driven deletion (PO decision 1) + protections."""

    def test_stream_gone_next_run_deletes_promoted_channel(
        self, db_session_factory
    ):
        rule_id = _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        client = make_promote_client(state)

        first, _ = _manual_run(client, db_session_factory)
        created_id = first["event_sync"][0]["promotion"]["channel_ids"][0]
        # First-run-populate protection: managed set populated, nothing
        # deleted on the run that first saw the channel.
        assert _managed_ids(db_session_factory, rule_id) == [created_id]
        assert state.deleted_channel_ids == []

        # The provider drops the event from the playlist.
        state.secondary_streams[SECONDARY_B_NAME] = []

        second, _ = _manual_run(client, db_session_factory)
        assert second["success"] is True
        # Pass 4 deleted the promoted channel — and ONLY it. The
        # Dispatcharr-owned master survives (managed-set invariant).
        assert state.deleted_channel_ids == [created_id]
        assert 100 in state.channels
        assert _managed_ids(db_session_factory, rule_id) == []
        assert second["channels_removed"] == 1
        # Renumbering after cleanup is a natural no-op for event_sync rules
        # (no create_channel action → no starting number). Verified, not
        # built.
        client.assign_channel_numbers.assert_not_called()

    def test_a_failed_create_does_not_retire_the_channel_it_already_had(
        self, db_session_factory, monkeypatch
    ):
        """Pass 4 cannot tell a channel the planner retired from one whose
        unit hit a transient error on the way to it, so the unit hands its
        known channel back instead of letting the run look like a
        retirement. [46]"""
        rule_id = _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        client = make_promote_client(state)

        first, _ = _manual_run(client, db_session_factory)
        created_id = first["event_sync"][0]["promotion"]["channel_ids"][0]
        assert _managed_ids(db_session_factory, rule_id) == [created_id]

        async def _create_fails(self, action, stream_ctx, exec_ctx,
                                template_ctx, **kwargs):
            return ActionResult(
                success=False, action_type="create_channel",
                description="upstream refused the create",
                entity_type="channel",
                entity_name=action.params.get("name_template"),
                error="503 from Dispatcharr",
            )

        monkeypatch.setattr(ActionExecutor, "_execute_create_channel",
                            _create_fails)
        second, _ = _manual_run(client, db_session_factory)

        promo = second["event_sync"][0]["promotion"]
        assert promo["failed_units"] == 1
        assert promo["promoted_created"] == 0
        assert promo["channel_ids"] == [created_id]
        assert state.deleted_channel_ids == []
        assert created_id in state.channels
        assert _managed_ids(db_session_factory, rule_id) == [created_id]

    def test_failed_promotion_does_not_claim_a_manual_channel(self, db_session_factory):
        rule_id = _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        state.channels[500] = {
            "id": 500,
            "name": FURY_CHANNEL_NAME,
            "channel_group_id": PROMOTE_GROUP_ID,
            "streams": [7301],
        }
        client = make_promote_client(state)
        failed = ActionResult(
            success=False,
            action_type="create_channel",
            description="Channel creation failed",
            error="503 from Dispatcharr",
        )
        with patch.object(
            ActionExecutor, "_execute_create_channel", AsyncMock(return_value=failed)
        ):
            result, _ = _manual_run(client, db_session_factory)

        assert result["event_sync"][0]["promotion"]["failed_units"] == 1
        assert result["event_sync"][0]["promotion"]["channel_ids"] == []
        assert _managed_ids(db_session_factory, rule_id) == []
        assert 500 in state.channels
        assert state.deleted_channel_ids == []

    def _rule_that_skips_finished(self, session_factory, orphan_action):
        """A promotion rule with skip_past_events on and the cleanup
        setting under test. orphan_action lives on the rule row, not in
        event_sync_config, so it is set after the row exists."""
        rule_id = _add_rule(
            session_factory, _promote_config(skip_past_events=True)
        )
        session = session_factory()
        try:
            rule = session.get(ChannelPipelineRule, rule_id)
            rule.orphan_action = orphan_action
            session.commit()
        finally:
            session.close()
        return rule_id

    def _run_at(self, client, session_factory, moment):
        """One manual run with BOTH clocks pinned to ``moment``: the
        resolver's, which dates the parse, and the executor's, which is
        the ONE clock the promotion reads. The planner takes its instant
        from the executor rather than reading the wall clock itself, so
        pinning the executor pins the past filter and the lead window
        too. [53]"""
        with patch("channel_pipeline_executor.datetime") as promote_clock:
            promote_clock.now.return_value = moment
            return _manual_run(client, session_factory, now=moment)

    def _promote_then_finish(self, client, session_factory, rule_id):
        """Run once while the event is still ahead, then again a day
        later with the same playlist, so the only thing that changed is
        the clock. Returns (created channel id, second run result)."""
        first, _ = self._run_at(client, session_factory, FROZEN_NOW)
        promo = first["event_sync"][0]["promotion"]
        assert promo["skipped_past"] == 0
        created_id = promo["channel_ids"][0]
        assert _managed_ids(session_factory, rule_id) == [created_id]

        second, _ = self._run_at(
            client, session_factory, FROZEN_NOW + timedelta(days=1)
        )
        promo2 = second["event_sync"][0]["promotion"]
        # The event has an existing channel and has finished, so its unit
        # is dropped and the channel never reaches the managed set.
        assert promo2["skipped_past"] == 1
        assert promo2["skipped_past_adopted"] == 1
        assert promo2["channel_ids"] == []
        return created_id, second

    def test_finished_event_channel_is_deleted_by_orphan_cleanup(
        self, db_session_factory
    ):
        """The whole mechanism end to end: nothing in the promotion path
        deletes anything, the channel just stops being managed, and Pass 4
        removes it with the rule's own orphan_action."""
        rule_id = self._rule_that_skips_finished(db_session_factory, "delete")
        state = _promote_state()
        client = make_promote_client(state)

        created_id, second = self._promote_then_finish(
            client, db_session_factory, rule_id
        )
        assert state.deleted_channel_ids == [created_id]
        assert second["channels_removed"] == 1
        assert _managed_ids(db_session_factory, rule_id) == []
        # The Dispatcharr-owned master is untouched, as always.
        assert 100 in state.channels

    def test_finished_event_channel_is_moved_when_the_rule_moves_orphans(
        self, db_session_factory
    ):
        rule_id = self._rule_that_skips_finished(
            db_session_factory, "move_uncategorized"
        )
        state = _promote_state()
        client = make_promote_client(state)

        created_id, second = self._promote_then_finish(
            client, db_session_factory, rule_id
        )
        assert state.deleted_channel_ids == []
        assert state.channels[created_id]["channel_group_id"] is None
        assert second["channels_moved"] == 1
        assert second["channels_removed"] == 0

    @pytest.mark.parametrize("orphan_action", ["none", "keep", "disable", "unsupported"])
    def test_finished_event_channel_survives_when_orphan_cleanup_is_off(
        self, db_session_factory, orphan_action
    ):
        """The operator's opt-out still wins: orphan_action 'none' skips
        reconciliation for the rule, so the filter costs the channel
        nothing."""
        rule_id = self._rule_that_skips_finished(db_session_factory, orphan_action)
        state = _promote_state()
        client = make_promote_client(state)

        created_id, second = self._promote_then_finish(
            client, db_session_factory, rule_id
        )
        assert state.deleted_channel_ids == []
        assert second["channels_removed"] == 0
        assert second["channels_moved"] == 0
        assert created_id in state.channels
        assert state.channels[created_id]["channel_group_id"] \
            == PROMOTE_GROUP_ID
        assert _managed_ids(db_session_factory, rule_id) == [created_id]

    def test_fetch_failure_makes_no_delete_observation(
        self, db_session_factory
    ):
        """A transient secondary-fetch failure must never mass-delete
        promoted channels: the rule made no observation this run, so Pass 4
        skips it entirely."""
        rule_id = _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        client = make_promote_client(state)
        first, _ = _manual_run(client, db_session_factory)
        created_id = first["event_sync"][0]["promotion"]["channel_ids"][0]

        client.get_streams = AsyncMock(
            side_effect=RuntimeError("provider 503")
        )
        second, _ = _manual_run(client, db_session_factory)
        assert second["success"] is True
        assert any(
            w["type"] == "event_sync_fetch_failed"
            for w in _latest_warnings(db_session_factory)
        )
        assert state.deleted_channel_ids == []
        assert created_id in state.channels
        # The managed set is untouched — the channel is still owned.
        assert _managed_ids(db_session_factory, rule_id) == [created_id]


class TestSelfHealing:
    """AC-6: master appears → stream attaches to master AND the promoted
    duplicate reconciles away in the SAME run."""

    def test_master_appearing_reattaches_and_deletes_promoted_duplicate(
        self, db_session_factory
    ):
        _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        client = make_promote_client(state)

        first, _ = _manual_run(client, db_session_factory)
        created_id = first["event_sync"][0]["promotion"]["channel_ids"][0]

        # Dispatcharr materializes the event in the master group.
        state.add_master({
            "id": 120, "name": MASTER_FURY,
            "channel_group_id": MASTER_GROUP_ID,
            "auto_created": True, "streams": [9002],
        })

        second, _ = _manual_run(client, db_session_factory)
        summary = second["event_sync"][0]
        # The stream now attaches to the MASTER (promoted channels are not
        # matcher candidates — PO decision 2).
        assert summary["attached"] == 1
        assert 7301 in state.stream_ids_of(120)
        # And the promoted duplicate is gone, same run.
        assert state.deleted_channel_ids == [created_id]
        assert summary["promotion"]["units"] == 0


class TestPromotionCap:
    """AC-7: overage stops creation, WARNs, event_sync_warnings entry."""

    def test_cap_overage_warns_and_defers(self, db_session_factory):
        _add_rule(db_session_factory, _promote_config(max_promote_per_run=1))
        state = _promote_state()
        state.secondary_streams[SECONDARY_B_NAME].append(
            {"id": 7302, "name": STREAM_TYSON, "m3u_account": 2},
        )
        client = make_promote_client(state)

        result, _ = _manual_run(client, db_session_factory)
        promo = result["event_sync"][0]["promotion"]
        assert promo["promoted_created"] == 1
        assert promo["capped"] is True
        assert promo["cap_overage"] == 1
        assert client.create_channel.await_count == 1
        warnings = [w for w in _latest_warnings(db_session_factory)
                    if w["type"] == "event_sync_promote_capped"]
        assert len(warnings) == 1
        assert warnings[0]["cap"] == 1
        assert warnings[0]["overage"] == 1
        assert "promotion cap" in result["event_sync"][0]["summary_line"]


class TestDryRunParity:
    """AC-8 (engine side): a pipeline dry-run computes the same promotion
    counts as the live run and creates NOTHING. (Preview-endpoint parity
    is covered in tests/routers/test_event_sync_preview.py.)"""

    def test_dry_run_creates_nothing_and_predicts_live_counts(
        self, db_session_factory
    ):
        _add_rule(db_session_factory, _promote_config())
        dry_state = _promote_state()
        dry_client = make_promote_client(dry_state)
        dry, _ = _manual_run(dry_client, db_session_factory, dry_run=True)
        dry_promo = dry["event_sync"][0]["promotion"]
        dry_client.create_channel.assert_not_awaited()
        assert dry_state.update_channel_calls == []
        assert dry_state.deleted_channel_ids == []

        live_state = _promote_state()
        live_client = make_promote_client(live_state)
        live, _ = _manual_run(live_client, db_session_factory)
        live_promo = live["event_sync"][0]["promotion"]

        for key in ("units", "promoted_created", "promoted_adopted",
                    "streams_attached", "capped", "cap_overage"):
            assert dry_promo[key] == live_promo[key], key


class TestRollback:
    """AC-10: rollback of a promotion run is defined — the standard
    snapshot path deletes the run-created channels and restores the master
    stream lists."""

    def test_confirmed_rollback_deletes_promoted_and_restores_master(
        self, db_session_factory
    ):
        _add_rule(db_session_factory, _promote_config())
        state = _promote_state()
        client = make_promote_client(state)

        engine = ChannelPipelineEngine(client)
        with patch("channel_pipeline_engine.get_session",
                   side_effect=db_session_factory), \
             patch("journal.log_entries"), \
             patch("services.event_sync_resolver.datetime") as mock_dt:
            mock_dt.now.return_value = FROZEN_NOW
            result = _run(engine.run_pipeline(
                dry_run=False, triggered_by="manual"
            ))
        assert result["success"] is True
        created_id = result["event_sync"][0]["promotion"]["channel_ids"][0]
        execution_id = result["execution_id"]
        assert state.stream_ids_of(100) == [9001, 7001]

        # Without confirm: refused (snapshot present).
        with patch("channel_pipeline_engine.get_session",
                   side_effect=db_session_factory):
            refused = _run(engine.rollback_execution(execution_id))
        assert refused["success"] is False
        assert refused["requires_confirm"] is True

        with patch("channel_pipeline_engine.get_session",
                   side_effect=db_session_factory):
            rolled = _run(engine.rollback_execution(
                execution_id, confirm=True
            ))
        assert rolled["success"] is True
        # The promoted channel is deleted; the master survives with its
        # pre-run stream list.
        assert created_id not in state.channels
        assert created_id in state.deleted_channel_ids
        assert 100 in state.channels
        assert state.stream_ids_of(100) == [9001]


class TestExclusionInteraction:
    """AC-11 integration: an operator exclusion suppresses the ATTACH but
    the stream still promotes (pinned semantics; unit-level pin in
    TestPromotionPlan)."""

    def test_excluded_pairing_stream_is_promoted(self, db_session_factory):
        from services.event_sync_review import (
            pairing_key,
        )
        from services.event_sync_matcher import parse_event_name

        # Exclude the (Mercury stream ↔ Mercury master) pairing, so the
        # stream's ONLY viable pairing is operator-suppressed.
        parsed_master = parse_event_name(MASTER_MERCURY, None, now=FROZEN_NOW)
        fp = pairing_key(1, STREAM_MERCURY, parsed_master)
        assert fp is not None

        _add_rule(db_session_factory, _promote_config(
            secondary_group_ids=[SECONDARY_A],
        ))
        state = FakeDispatcharrState(
            channels=[{
                "id": 100, "name": MASTER_MERCURY,
                "channel_group_id": MASTER_GROUP_ID,
                "auto_created": True, "streams": [9001],
            }],
            secondary_streams={SECONDARY_A_NAME: [
                {"id": 7001, "name": STREAM_MERCURY, "m3u_account": 1},
            ]},
        )
        client = make_promote_client(state)

        engine = ChannelPipelineEngine(client)
        with patch("channel_pipeline_engine.get_session",
                   side_effect=db_session_factory), \
             patch("journal.log_entries"), \
             patch("services.event_sync_exclusion_store.load_exclusion_keys",
                   return_value=frozenset({fp})), \
             patch("services.event_sync_resolver.datetime") as mock_dt:
            mock_dt.now.return_value = FROZEN_NOW
            result = _run(engine.run_pipeline(
                dry_run=False, triggered_by="manual"
            ))

        summary = result["event_sync"][0]
        assert summary["excluded_by_operator"] == 1
        assert summary["attached"] == 0
        # The exclusion blocked the ATTACH — but not the promotion.
        promo = summary["promotion"]
        assert promo["promoted_created"] == 1
        created_id = promo["channel_ids"][0]
        assert state.stream_ids_of(created_id) == [7001]
        # The master was never touched.
        assert state.stream_ids_of(100) == [9001]


class TestDummyEpgCoversPromoted:
    """AC-9 (executor layer): the rule's dummy EPG profile covers the
    promotion target group; foreign EPG is never clobbered; promotion-less
    configs keep the master-only filter."""

    def _executor(self, channels):
        from channel_pipeline_executor import ActionExecutor

        client = AsyncMock()
        executor = ActionExecutor(
            client,
            existing_channels=channels,
            existing_groups=[],
            epg_data=[
                {"id": 501, "epg_source": 61, "tvg_id": "dummy.1"},
            ],
            epg_sources=[
                {"id": 61, "url": "http://ecm/api/dummy-epg/xmltv/9"},
            ],
        )
        return executor, client

    def test_promoted_channel_gets_profile_and_foreign_epg_kept(self, db_session_factory):
        from channel_pipeline_executor import ExecutionContext

        channels = [
            {"id": 100, "name": "Master", "channel_group_id": MASTER_GROUP_ID,
             "epg_data_id": None, "streams": []},
            {"id": 900, "name": FURY_CHANNEL_NAME,
             "channel_group_id": PROMOTE_GROUP_ID,
             "epg_data_id": None, "streams": [7301]},
            {"id": 901, "name": "Promoted With Foreign EPG",
             "channel_group_id": PROMOTE_GROUP_ID,
             "epg_data_id": 777, "streams": [7302]},
            {"id": 902, "name": "Elsewhere", "channel_group_id": 55,
             "epg_data_id": None, "streams": []},
        ]
        executor, _ = self._executor(channels)
        exec_ctx = ExecutionContext(dry_run=True)
        with patch("database.get_session", side_effect=db_session_factory):
            summary = _run(executor.assign_event_sync_dummy_epg(
                1, "Event Rule",
                _promote_config(dummy_epg_profile_id=9),
                exec_ctx,
            ))
        entries = {e["entity_id"] for e in summary["assign_entries"]}
        # Master + bare promoted channel are assigned; the foreign-EPG
        # promoted channel is skipped; the unrelated group is untouched.
        assert entries == {100, 900}
        assert summary["skipped_foreign_epg"] == 1

    def test_promotionless_config_keeps_master_only_filter(self, db_session_factory):
        from channel_pipeline_executor import ExecutionContext

        channels = [
            {"id": 100, "name": "Master", "channel_group_id": MASTER_GROUP_ID,
             "epg_data_id": None, "streams": []},
            {"id": 900, "name": FURY_CHANNEL_NAME,
             "channel_group_id": PROMOTE_GROUP_ID,
             "epg_data_id": None, "streams": []},
        ]
        executor, _ = self._executor(channels)
        exec_ctx = ExecutionContext(dry_run=True)
        config = event_sync_config(
            secondary_group_ids=[SECONDARY_A],
            dummy_epg_profile_id=9,
        )
        with patch("database.get_session", side_effect=db_session_factory):
            summary = _run(executor.assign_event_sync_dummy_epg(
                1, "Event Rule", config, exec_ctx,
            ))
        entries = {e["entity_id"] for e in summary["assign_entries"]}
        assert entries == {100}


class TestEventSyncAssignChannelProfile:
    """GH #720 / y3m6o.1 Finding 4 (0152): a configured assign_channel_profile
    action on an event_sync rule takes effect via the REAL production path
    (run_pipeline -> _run_event_sync_rules -> execute_event_sync_rule ->
    promotion, then _apply_event_sync_profile_action), applying exclusive
    channel-profile membership to the channels the rule touched this run.

    This is the GENUINE production-path regression the 0151 pass faked: it
    invokes execute_event_sync_rule against real secondary streams, not the
    synthetic no-streams path that manually called the generic action.
    """

    def _add_profile_rule(self, session_factory, config, profile_ids):
        session = session_factory()
        try:
            rule = ChannelPipelineRule(
                name="Event Rule", enabled=True, priority=0,
                conditions=json.dumps([{"type": "always"}]),
                actions=json.dumps([
                    {"type": "assign_channel_profile",
                     "channel_profile_ids": profile_ids},
                ]),
                event_sync_config=json.dumps(config),
            )
            session.add(rule)
            session.commit()
            session.refresh(rule)
            return rule.id
        finally:
            session.close()

    def test_promoted_channel_gets_exclusive_profile_membership(
        self, db_session_factory
    ):
        self._add_profile_rule(db_session_factory, _promote_config(), [1])
        state = _promote_state()
        client = make_promote_client(state)
        client.get_channel_profiles = AsyncMock(
            return_value=[{"id": 1}, {"id": 2}, {"id": 3}]
        )
        client.update_profile_channel = AsyncMock()

        result, _ = _manual_run(client, db_session_factory)

        # The production path promoted the unmatched Fury event to a NEW channel.
        promoted = [cid for cid in state.channels if cid >= 900]
        assert len(promoted) == 1
        promoted_id = promoted[0]

        # #720: the promoted (new) channel — which Dispatcharr auto-joins to ALL
        # profiles — is reconciled to EXACTLY profile 1. The diff-aware reconcile
        # (y3m6o.1 review follow-up) DISABLES the unselected profiles 2 and 3 and
        # SKIPS the redundant enable of the already-auto-joined profile 1, so the
        # end state is exactly {1}. This proves the rule's assign_channel_profile
        # action actually executed on the event_sync path, not enable-only/no-op.
        per_channel: dict[int, dict[int, bool]] = {}
        for c in client.update_profile_channel.call_args_list:
            pid, channel_id, body = c.args[0], c.args[1], c.args[2]
            per_channel.setdefault(channel_id, {})[pid] = body["enabled"]
        assert per_channel.get(promoted_id) == {2: False, 3: False}

        # The rule's event_sync summary records the profile step, and the run is
        # a clean success (all profile writes landed).
        summary = result["event_sync"][0]
        assert summary["assign_channel_profile"]["succeeded"] >= 1
        assert result["success"] is True

    def test_no_profile_action_makes_no_profile_writes(self, db_session_factory):
        """Control: an event_sync rule WITHOUT an assign_channel_profile action
        performs no profile writes — byte-identical to pre-feature behavior."""
        _add_rule(db_session_factory, _promote_config())  # actions = [skip]
        client = make_promote_client(_promote_state())
        client.update_profile_channel = AsyncMock()

        _manual_run(client, db_session_factory)

        client.update_profile_channel.assert_not_called()


@pytest.fixture
def retirement(db_session_factory, monkeypatch):
    from datetime import timezone
    from models import DummyEPGProfile
    from services import epg_programmes, epg_publication, event_sync_stream_health

    now = datetime(2026, 7, 12, 4, tzinfo=timezone.utc)
    db = db_session_factory()
    profile = DummyEPGProfile(name="Events", enabled=True, name_source="channel", event_timezone="US/Eastern")
    profile.set_channel_group_ids([PROMOTE_GROUP_ID])
    profile.set_epg_source_ids([50])
    db.add(profile)
    db.commit()
    config = _promote_config(retire_finished_events=True, dummy_epg_profile_id=profile.id, promote_lead_hours=0)
    rule_id = _add_rule(db_session_factory, config)
    rule = db.get(ChannelPipelineRule, rule_id)
    rule.set_managed_channel_ids([900])
    rule.orphan_action = "delete"
    db.commit()
    state = _promote_state()
    state.channels[900] = {"id": 900, "uuid": "event-900", "name": FURY_CHANNEL_NAME,
                           "channel_group_id": PROMOTE_GROUP_ID, "streams": [7301], "auto_created": True}
    client = make_promote_client(state)
    streams = [{"id": 7301, "name": "No EVENT Today", "is_stale": False,
                "updated_at": (now - timedelta(minutes=1)).isoformat()}]
    client.get_streams_by_ids = AsyncMock(side_effect=lambda ids: [row.copy() for row in streams if row["id"] in ids])
    client.get_channel_stats = AsyncMock(return_value={"channels": []})
    stats = {}
    witness = {"source_id": 50, "source_tvg_id": "PPV1", "title": "Fury vs. Usyk",
               "start": (now - timedelta(hours=1)).isoformat(),
               "stop": (now - timedelta(minutes=30)).isoformat()}
    source = {"source_id": 50, "status": "ready", "last_success": now.isoformat()}
    epg_sources = [{
        "id": 50,
        "url": f"https://dispatcharr.invalid/api/dummy-epg/xmltv/{profile.id}",
    }]

    async def prepare(profiles, channels, client, **kwargs):
        return [], {"sources": [source], "channels": [
            {"channel_id": cid, "event": witness.copy() if witness else None} for cid in channels
        ]}

    monkeypatch.setattr(database, "get_session", db_session_factory)
    monkeypatch.setattr(epg_publication, "get_session", db_session_factory)
    monkeypatch.setattr(epg_programmes, "prepare_profiles", AsyncMock(side_effect=prepare))
    monkeypatch.setattr("channel_pipeline_executor.datetime", _clock(now))
    monkeypatch.setattr(event_sync_stream_health, "datetime", _clock(now))
    monkeypatch.setattr(event_sync_stream_health, "_load_stats", AsyncMock(side_effect=lambda ids: stats.copy()))
    executor = ActionExecutor(
        client,
        list(state.channels.values()),
        managed_channel_ids=[900],
        epg_sources=epg_sources,
    )
    yield dict(now=now, rule=rule, config=config, state=state, client=client, streams=streams,
               stats=stats, witness=witness, source=source, executor=executor, db=db,
               session_factory=db_session_factory, epg_sources=epg_sources)
    db.close()


async def _retire(setup, dry_run=False):
    executor, rule = setup["executor"], setup["rule"]
    _, states = await executor._event_lifecycle(
        rule.id,
        setup["config"],
        (),
        setup["now"],
        flow=setup.get("flow", {}),
        expires_at=setup["now"] + timedelta(minutes=5),
    )
    engine = ChannelPipelineEngine(setup["client"])
    result = {"channels_removed": 0, "channels_moved": 0, "dry_run_results": [], "execution_log": []}
    with patch("channel_pipeline_engine.get_session", side_effect=setup["session_factory"]):
        await engine._reconcile_orphans([rule], {rule.id: []}, executor, None, result, dry_run)
    return states, result


@pytest.mark.asyncio
async def test_health_that_consumes_the_lifetime_starts_no_lifecycle_read(
    retirement,
    monkeypatch,
):
    from channel_pipeline_executor import ExecutionContext
    from services import epg_programmes
    from types import SimpleNamespace

    setup = retirement
    executor = setup["executor"]
    current = [setup["now"]]
    expires_at = setup["now"] + timedelta(seconds=60)
    row = _resolved(
        STREAM_FURY,
        DISPOSITION_UNMATCHED,
        _parsed("Fury vs. Usyk", START),
        provider_id=2,
        stream_id=7301,
        group_id=SECONDARY_B,
    )

    async def consume(*args, **kwargs):
        assert kwargs["expires_at"] == expires_at
        current[0] = expires_at
        return {7301: True}, set()

    monkeypatch.setattr(executor, "_event_health", AsyncMock(side_effect=consume))
    setup["client"].get_streams_by_ids.reset_mock()
    epg_programmes.prepare_profiles.reset_mock()
    with patch(
        "channel_pipeline_executor.datetime",
        _clock(lambda: current[0]),
    ):
        flow, dead = await executor._event_health(
            setup["rule"].id, setup["config"], (), [row], setup["now"],
            probe_missing=True, expires_at=expires_at,
        )
        eligible, states = await executor._event_lifecycle(
            setup["rule"].id, setup["config"], (), current[0], dead,
            flow=flow, expires_at=expires_at,
        )
    assert eligible == set()
    assert states == {900: "unknown"}
    setup["client"].get_streams_by_ids.assert_not_awaited()
    epg_programmes.prepare_profiles.assert_not_awaited()
    setup["client"].create_channel.assert_not_awaited()
    setup["client"].update_channel.assert_not_awaited()


@pytest.mark.parametrize("enabled", [True, False])
def test_old_candidates_leave_capacity_for_current_events(enabled):
    now = FROZEN_NOW
    rows = [
        _resolved("old", DISPOSITION_UNMATCHED, _parsed("Alpha", now - timedelta(days=2)), stream_id=1),
        _resolved("older", DISPOSITION_UNMATCHED, _parsed("Beta", now - timedelta(days=3)), stream_id=2),
        _resolved("current", DISPOSITION_UNMATCHED, _parsed("Zeta", now - timedelta(minutes=5)), stream_id=3),
    ]
    plan = build_promotion_plan(
        _promote_config(retire_finished_events=enabled, promote_lead_hours=0, max_promote_per_run=1),
        rows, {}, now=now,
    )
    assert [row.stream.stream_id for unit in plan.units for row in unit.rows] == ([3] if enabled else [1])
    assert plan.cap_overage == (0 if enabled else 2)


@pytest.mark.parametrize("hours, past, early", [(-26, 1, 0), (0.5, 0, 1)])
@pytest.mark.parametrize("eligible", [None, set()])
def test_event_window_preserves_skip_counts(hours, past, early, eligible):
    row = _resolved("event", DISPOSITION_UNMATCHED, _parsed("Event", FROZEN_NOW + timedelta(hours=hours)), stream_id=1)
    plan = build_promotion_plan(
        _promote_config(retire_finished_events=True, promote_lead_hours=1),
        [row], {}, now=FROZEN_NOW, eligible_event_keys=eligible,
    )
    assert plan.units == ()
    assert plan.skipped_past == past
    assert plan.skipped_early == early
    assert plan.cap_overage == 0
    assert plan.skipped_past_adopted == 0


def test_event_window_rejects_missing_start_before_comparison():
    parsed = _parsed("Event", None)
    assert master_event_key(parsed) is None
    row = _resolved("event", DISPOSITION_UNMATCHED, parsed, stream_id=1)
    plan = build_promotion_plan(_promote_config(retire_finished_events=True), [row], {}, now=FROZEN_NOW)
    assert plan.units == ()
    assert plan.capped_units == ()


@pytest.mark.asyncio
async def test_owned_dateless_event_keeps_its_channel(retirement):
    from services.event_sync_matcher import parse_event_name, SYNTHESIZED_DATE_PATTERN_NAMES
    from channel_pipeline_executor import ExecutionContext
    from types import SimpleNamespace

    setup = retirement
    name = "Fury vs. Usyk @ 11:00 PM ET"
    setup["config"]["assume_current_date"] = True
    setup["executor"]._channel_by_id[900]["name"] = name
    parsed = parse_event_name(name, now=setup["now"] - timedelta(hours=1), assume_current_date=True)
    assert parsed.matched_pattern in SYNTHESIZED_DATE_PATTERN_NAMES
    setup["witness"].update(start=parsed.start.isoformat(), stop=(parsed.start + timedelta(minutes=30)).isoformat())
    row = _resolved(name, DISPOSITION_UNMATCHED, parsed, stream_id=7301)
    with patch("channel_pipeline_executor.datetime") as clock:
        clock.now.return_value = setup["now"]
        clock.fromisoformat.side_effect = datetime.fromisoformat
        result = await setup["executor"]._execute_event_sync_promotion(
            setup["rule"].id, setup["rule"].name, setup["config"], SimpleNamespace(resolved=[row]), ExecutionContext(),
        )
    assert result["event_states"] == [{"channel_id": 900, "status": "unknown"}]
    assert 900 in result["channel_ids"]
    _, cleanup = await _retire(setup)
    assert cleanup["channels_removed"] == 0
    setup["client"].delete_channel.assert_not_awaited()


@pytest.mark.asyncio
async def test_event_preview_keeps_the_failed_stream_filter(async_client, retirement):
    setup = retirement
    setup["state"].channels.pop(900)
    setup["db"].get(ChannelPipelineRule, setup["rule"].id).set_managed_channel_ids([])
    setup["db"].commit()
    setup["streams"][:] = [
        {"id": 7301, "name": STREAM_FURY, "is_stale": False, "updated_at": setup["now"].isoformat()},
        {"id": 7302, "name": STREAM_FURY_ALT, "is_stale": False, "updated_at": setup["now"].isoformat()},
    ]
    setup["state"].secondary_streams[SECONDARY_B_NAME] = [
        {"id": 7301, "name": STREAM_FURY, "m3u_account": 2},
        {"id": 7302, "name": STREAM_FURY_ALT, "m3u_account": 2},
    ]
    setup["stats"].update({
        7301: {
            "stream_name": STREAM_FURY,
            "probe_status": "success",
            "measured_bitrate": 5000000,
            "last_probed": setup["now"].isoformat(),
            "is_black_screen": False,
            "black_screen_checked_at": setup["now"].isoformat(),
        },
        7302: {
            "stream_name": STREAM_FURY_ALT,
            "probe_status": "failed",
            "measured_bitrate": 0,
            "last_probed": setup["now"].isoformat(),
        },
    })
    setup["witness"]["stop"] = (setup["now"] + timedelta(hours=1)).isoformat()
    with patch("routers.channel_pipeline.get_client", return_value=setup["client"]), \
         patch("routers.channel_pipeline.get_session", side_effect=setup["session_factory"]), \
         patch("routers.channel_pipeline.datetime") as clock, \
         patch("services.event_sync_stream_health.datetime", _clock(setup["now"])):
        clock.now.return_value = setup["now"]
        response = await async_client.post("/api/channel-pipeline/event-sync-preview", json={"rule_id": setup["rule"].id})
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["promotion"]["would_promote"] == 1
    assert result["promotion"]["dead_streams_skipped"] == 1
    assert [row["stream_id"] for row in result["promotion"]["units"][0]["streams"]] == [7301]
    failed = next(row for row in result["unmatched_streams"] if row["stream_id"] == 7302)
    assert failed["promote_stream_dead"] is True
    assert failed["would_promote"] is False


@pytest.mark.asyncio
async def test_saved_event_preview_uses_unsaved_settings_and_ownership(async_client, retirement):
    setup = retirement
    config = {**setup["config"], "max_promote_per_run": 2}
    with patch("routers.channel_pipeline.get_client", return_value=setup["client"]), \
         patch("routers.channel_pipeline.get_session", side_effect=setup["session_factory"]), \
         patch("channel_pipeline_engine.get_session", side_effect=setup["session_factory"]), \
         patch("routers.channel_pipeline.datetime") as clock:
        clock.now.return_value = setup["now"]
        response = await async_client.post("/api/channel-pipeline/event-sync-preview", json={
            "rule_id": setup["rule"].id, "event_sync_config": config,
        })
    assert response.status_code == 200, response.text
    result = response.json()["promotion"]
    assert result["cap"] == 2
    assert result["event_states"] == [{"channel_id": 900, "status": "idle"}]
    assert result["retirements"][0]["channel_id"] == 900
    setup["db"].expire_all()
    assert setup["db"].get(ChannelPipelineRule, setup["rule"].id).get_event_sync_config() == setup["config"]
    setup["client"].delete_channel.assert_not_awaited()


@pytest.mark.asyncio
async def test_event_preview_explains_ineligible_starts(async_client, retirement):
    setup = retirement
    setup["state"].channels.pop(900)
    setup["db"].get(ChannelPipelineRule, setup["rule"].id).set_managed_channel_ids([])
    setup["db"].commit()
    setup["state"].secondary_streams[SECONDARY_B_NAME] = [
        {"id": 7301, "name": "Old Event @ 10 Jul 10:00 PM ET", "m3u_account": 2},
        {"id": 7302, "name": "Future Event @ 12 Jul 12:30 AM ET", "m3u_account": 2},
    ]
    with patch("routers.channel_pipeline.get_client", return_value=setup["client"]), \
         patch("routers.channel_pipeline.get_session", side_effect=setup["session_factory"]), \
         patch("channel_pipeline_engine.get_session", side_effect=setup["session_factory"]), \
         patch("routers.channel_pipeline.datetime") as clock:
        clock.now.return_value = setup["now"]
        response = await async_client.post("/api/channel-pipeline/event-sync-preview", json={
            "rule_id": setup["rule"].id,
            "event_sync_config": {**setup["config"], "promote_lead_hours": 1},
        })
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["promotion"]["retire_finished_events"] is True
    assert result["promotion"]["skipped_past"] == 1
    assert result["promotion"]["skipped_early"] == 1
    assert result["promotion"]["skipped_past_adopted"] == 0
    assert result["promotion"]["retirements"] == []
    rows = {row["stream_id"]: row for row in result["unmatched_streams"]}
    assert rows[7301]["promote_skipped_past"] is True
    assert rows[7302]["promote_skipped_early"] is True
    assert rows[7301]["would_promote"] is False
    assert rows[7302]["would_promote"] is False
    setup["client"].delete_channel.assert_not_awaited()


@pytest.mark.asyncio
async def test_confirmed_idle_channel_is_removed_without_deleting_stream(retirement):
    states, result = await _retire(retirement)
    assert states[900] == "idle"
    assert result["channels_removed"] == 1
    assert 900 not in retirement["state"].channels
    assert _managed_ids(retirement["session_factory"], retirement["rule"].id) == []
    assert retirement["streams"][0]["id"] == 7301
    retirement["client"].get_channel_stats.assert_awaited_once()


@pytest.mark.asyncio
async def test_event_with_no_guide_listing_retires_when_every_stream_is_dead(retirement):
    """Nothing lists ESPN+ or PPV, so those channels never get a witness."""
    setup = retirement
    setup["witness"].clear()
    _, states = await setup["executor"]._event_lifecycle(
        setup["rule"].id, setup["config"], (), setup["now"], {7301},
        flow={},
        expires_at=setup["now"] + timedelta(minutes=5),
    )
    assert states[900] == "idle"


@pytest.mark.asyncio
async def test_bare_managed_slot_retires_through_the_live_promotion_path(retirement):
    from channel_pipeline_executor import ExecutionContext
    from channel_pipeline_engine import ChannelPipelineEngine
    from services.event_sync_matcher import ParsedEvent
    from types import SimpleNamespace

    setup = retirement
    setup["witness"].clear()
    bare_name = "ESPN PLUS 01:"
    setup["streams"][0].update({
        "name": bare_name,
        "m3u_account": 2,
        "channel_group_id": SECONDARY_B,
    })
    setup["stats"][7301] = {
        "probe_status": "success",
        "measured_bitrate": 1000,
        "last_probed": setup["now"].isoformat(),
    }
    row = _resolved(
        bare_name,
        DISPOSITION_PARSE_FAILED,
        ParsedEvent(
            raw_name=bare_name,
            title=None,
            start=None,
            teams=None,
            matched_pattern=None,
        ),
        stream_id=7301,
        provider_id=2,
        group_id=SECONDARY_B,
    )
    executor = ActionExecutor(
        setup["client"], list(setup["state"].channels.values()),
        managed_channel_ids=[900],
    )
    with patch("channel_pipeline_executor.datetime") as clock:
        clock.now.return_value = setup["now"]
        clock.fromisoformat.side_effect = datetime.fromisoformat
        promotion = await executor._execute_event_sync_promotion(
            setup["rule"].id,
            setup["rule"].name,
            setup["config"],
            SimpleNamespace(resolved=[row]),
            ExecutionContext(),
        )

    result = {
        "channels_removed": 0,
        "channels_moved": 0,
        "dry_run_results": [],
        "execution_log": [],
    }
    with patch("channel_pipeline_engine.get_session",
               side_effect=setup["session_factory"]):
        await ChannelPipelineEngine(setup["client"])._reconcile_orphans(
            [setup["rule"]],
            {setup["rule"].id: promotion["channel_ids"]},
            executor,
            None,
            result,
            False,
        )

    assert promotion["event_states"] == [
        {"channel_id": 900, "status": "unknown"},
    ]
    assert result["channels_removed"] == 0
    assert 900 in setup["state"].channels
    assert setup["streams"][0]["id"] == 7301


@pytest.mark.asyncio
async def test_bare_managed_slot_preview_matches_the_live_retirement(retirement, async_client):
    setup = retirement
    setup["witness"].clear()
    bare_name = "ESPN PLUS 01:"
    setup["state"].secondary_streams[SECONDARY_B_NAME] = [{
        "id": 7301,
        "name": bare_name,
        "m3u_account": 2,
        "is_stale": False,
    }]
    setup["streams"][0].update({
        "name": bare_name,
        "m3u_account": 2,
        "channel_group_id": SECONDARY_B,
    })
    setup["stats"][7301] = {
        "probe_status": "success",
        "measured_bitrate": 1000,
        "last_probed": setup["now"].isoformat(),
    }

    with patch("routers.channel_pipeline.get_client", return_value=setup["client"]), \
         patch("routers.channel_pipeline.get_session",
               side_effect=setup["session_factory"]), \
         patch("channel_pipeline_engine.get_session",
               side_effect=setup["session_factory"]), \
         patch("routers.channel_pipeline.datetime") as clock:
        clock.now.return_value = setup["now"]
        response = await async_client.post(
            "/api/channel-pipeline/event-sync-preview",
            json={"rule_id": setup["rule"].id},
        )

    assert response.status_code == 200, response.text
    promotion = response.json()["promotion"]
    assert promotion["event_states"] == [
        {"channel_id": 900, "status": "unknown"},
    ]
    assert promotion["retirements"] == []
    setup["client"].delete_channel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("dead", [set(), {7301}])
async def test_a_live_stream_keeps_its_channel_whether_or_not_a_sibling_died(retirement, dead):
    setup = retirement
    setup["witness"].clear()
    setup["executor"]._channel_by_id[900]["streams"] = [7301, 7302]
    setup["streams"].append({"id": 7302, "name": STREAM_FURY, "is_stale": False,
                             "updated_at": (setup["now"] - timedelta(minutes=1)).isoformat()})
    _, states = await setup["executor"]._event_lifecycle(
        setup["rule"].id, setup["config"], (), setup["now"], dead,
        flow={7302: True},
        expires_at=setup["now"] + timedelta(minutes=5),
    )
    assert states[900] != "idle"


@pytest.mark.asyncio
@pytest.mark.parametrize("minutes", [15, 45, 60])
async def test_hourly_schedule_retains_fresh_stream_retirement_evidence(retirement, minutes):
    from services import epg_programmes
    retirement["source"]["last_success"] = (retirement["now"] - timedelta(minutes=minutes)).isoformat()
    states, result = await _retire(retirement)
    assert states[900] == "idle" and result["channels_removed"] == 1
    assert epg_programmes.prepare_profiles.await_args.kwargs["wait_for_sources"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["guide_missing", "guide_error", "guide_stale", "guide_active", "old_observation", "missing_stream", "ended_label", "old_stream_active", "mixed_providers", "unknown_identity", "wrong_start"])
async def test_event_retirement_holds_unknown_or_conflicting_evidence(retirement, case):
    setup = retirement
    if case == "guide_missing":
        setup["witness"].clear()
    elif case == "guide_error":
        setup["source"]["status"] = "error"
    elif case == "guide_stale":
        from services.epg_programmes import SOURCE_MAX_AGE
        setup["source"]["last_success"] = (setup["now"] - timedelta(seconds=SOURCE_MAX_AGE + 1)).isoformat()
    elif case == "guide_active":
        setup["witness"]["stop"] = (setup["now"] + timedelta(hours=1)).isoformat()
        setup["streams"][0]["name"] = "Ended"
    elif case == "old_observation":
        setup["streams"][0]["updated_at"] = (setup["now"] - timedelta(hours=2)).isoformat()
    elif case == "missing_stream":
        setup["streams"].clear()
    elif case == "ended_label":
        setup["streams"][0]["name"] = "Ended"
    elif case == "old_stream_active":
        setup["streams"][0].update(name=STREAM_FURY, is_stale=True)
        setup["flow"] = {7301: True}
    elif case == "mixed_providers":
        setup["executor"]._channel_by_id[900]["streams"].append(7302)
        setup["streams"].append({"id": 7302, "name": STREAM_FURY, "updated_at": setup["now"].isoformat()})
    elif case == "unknown_identity":
        setup["executor"]._channel_by_id[900]["name"] = "PPV 05"
    elif case == "wrong_start":
        setup["witness"]["start"] = (setup["now"] - timedelta(hours=5)).isoformat()
    states, result = await _retire(setup)
    if case == "old_stream_active":
        assert states[900] == "idle"
        assert result["channels_removed"] == 1
    else:
        assert states[900] != "idle"
        assert result["channels_removed"] == 0
        setup["client"].delete_channel.assert_not_awaited()
        assert _managed_ids(setup["session_factory"], setup["rule"].id) == [900]


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [{"channels": [{"channel_id": "event-900", "clients": [{"id": "viewer"}]}]}, {}, {"channels": [None]}, {"channels": [{"channel_id": {"unknown": 1}}]}, RuntimeError("unavailable"), "missing_uuid"])
async def test_event_retirement_defers_for_viewers_or_unknown_stats(retirement, response):
    if response == "missing_uuid":
        retirement["executor"]._channel_by_id[900].pop("uuid")
    elif isinstance(response, Exception):
        retirement["client"].get_channel_stats.side_effect = response
    else:
        retirement["client"].get_channel_stats.return_value = response
    _, result = await _retire(retirement)
    assert result["channels_removed"] == 0
    retirement["client"].delete_channel.assert_not_awaited()
    assert _managed_ids(retirement["session_factory"], retirement["rule"].id) == [900]


@pytest.mark.asyncio
async def test_failed_retirement_stays_managed_for_the_next_pass(retirement):
    retirement["client"].delete_channel.side_effect = RuntimeError("unavailable")
    _, result = await _retire(retirement)
    assert result["channels_removed"] == 0
    assert _managed_ids(retirement["session_factory"], retirement["rule"].id) == [900]


@pytest.mark.asyncio
async def test_failed_guide_preparation_preserves_owned_event(retirement, monkeypatch):
    monkeypatch.setattr("services.epg_programmes.prepare_profiles", AsyncMock(side_effect=RuntimeError("unavailable")))
    _, result = await _retire(retirement)
    assert result["channels_removed"] == 0
    assert _managed_ids(retirement["session_factory"], retirement["rule"].id) == [900]


@pytest.mark.asyncio
async def test_stale_managed_id_is_forgotten_without_deletion(retirement):
    retirement["executor"]._channel_by_id.pop(900)
    _, result = await _retire(retirement)
    assert result["channels_removed"] == 0
    assert _managed_ids(retirement["session_factory"], retirement["rule"].id) == []


@pytest.mark.asyncio
async def test_event_preview_reports_guarded_retirement_without_writing(retirement, async_client):
    setup = retirement
    with patch("routers.channel_pipeline.get_client", return_value=setup["client"]), \
         patch("routers.channel_pipeline.get_session", side_effect=setup["session_factory"]), \
         patch("channel_pipeline_engine.get_session", side_effect=setup["session_factory"]), \
         patch("routers.channel_pipeline.datetime") as clock:
        clock.now.return_value = setup["now"]
        response = await async_client.post("/api/channel-pipeline/event-sync-preview", json={"rule_id": setup["rule"].id})
    assert response.status_code == 200, response.text
    promotion = response.json()["promotion"]
    assert promotion["event_states"] == [{"channel_id": 900, "status": "idle"}]
    assert len(promotion["retirements"]) == 1
    assert promotion["retirements"][0]["channel_id"] == 900
    setup["client"].delete_channel.assert_not_awaited()
    assert _managed_ids(setup["session_factory"], setup["rule"].id) == [900]


@pytest.mark.asyncio
@pytest.mark.parametrize("stale_measurement", [False, True])
async def test_later_valid_event_can_use_the_retained_stream(retirement, stale_measurement):
    from channel_pipeline_executor import ExecutionContext
    from types import SimpleNamespace
    setup = retirement
    await _retire(setup)
    now = setup["now"] + timedelta(hours=1)
    name = "DAZN 05: Fury vs. Usyk @ 12 Jul 01:00 AM ET"
    setup["streams"][0].update(name=name, updated_at=now.isoformat())
    setup["stats"][7301] = {
        "stream_name": name,
        "measured_bitrate": 5000000,
        "last_probed": now.isoformat(),
        "is_black_screen": False,
        "black_screen_checked_at": now.isoformat(),
    }
    if stale_measurement:
        setup["config"]["skip_dead_streams"] = True
        setup["stats"][7301].update(measured_bitrate=1000, last_probed=(now - timedelta(minutes=10)).isoformat())
    setup["source"]["last_success"] = now.isoformat()
    start = now - timedelta(minutes=20) if stale_measurement else now
    setup["witness"].update(start=start.isoformat(), stop=(now + timedelta(hours=2)).isoformat())
    parsed = _parsed("Fury vs. Usyk", start)
    row = _resolved(name, DISPOSITION_UNMATCHED, parsed, provider_id=2, stream_id=7301)
    setup["config"]["max_promote_per_run"] = 1
    setup["rule"].set_event_sync_config(setup["config"])
    setup["db"].commit()
    old = [_resolved("old", DISPOSITION_UNMATCHED, _parsed(title, now - timedelta(days=2)), stream_id=sid)
           for sid, title in [(10, "Alpha"), (11, "Beta")]]
    executor = ActionExecutor(
        setup["client"],
        list(setup["state"].channels.values()),
        managed_channel_ids=[],
        epg_sources=setup["epg_sources"],
    )
    after_probe = now + timedelta(seconds=5)
    async def refreshed(client, ids, **kwargs):
        assert ids == [7301]
        setup["stats"][7301].update(
            measured_bitrate=5000000,
            last_probed=after_probe.isoformat(),
            is_black_screen=False,
            black_screen_checked_at=after_probe.isoformat(),
        )
        kwargs["confirmed"].update(ids)
        return set()
    with patch("channel_pipeline_executor.datetime") as clock, \
         patch("services.event_sync_stream_health.datetime", _clock(after_probe)), \
         patch("services.event_sync_stream_health._probe_and_collect_failures", new=AsyncMock(side_effect=refreshed)) as probe:
        times = iter([now])
        clock.now.side_effect = lambda *args: next(times, after_probe)
        clock.fromisoformat.side_effect = datetime.fromisoformat
        result = await executor._execute_event_sync_promotion(
            setup["rule"].id, setup["rule"].name, setup["config"], SimpleNamespace(resolved=[*old, row]), ExecutionContext(),
        )
    if stale_measurement:
        probe.assert_awaited_once()
    assert result["promoted_created"] == 1
    assert result["channel_ids"]
    assert setup["streams"][0]["id"] == 7301
    channel_id = result["channel_ids"][0]
    channel = setup["state"].channels[channel_id]
    assert channel["streams"] == []
    assert channel["hidden_from_output"] is True
    assert result["guide_pending"] == 1
    from services.epg_publication import read_publication
    publication = read_publication(f'profile:{setup["config"]["dummy_epg_profile_id"]}')
    receipt = next(iter(publication["state"]["delivery"]["pending_channels"].values()))
    assert receipt["stage"] == "allocated"
    assert receipt["channel_id"] == channel_id
    assert [stream["id"] for stream in receipt["streams"]] == [7301]


@pytest.mark.asyncio
@pytest.mark.parametrize("age", [26, 1])
async def test_old_attached_streams_leave_probe_capacity_for_current_event(retirement, age):
    from channel_pipeline_executor import ExecutionContext
    from services.event_sync_stream_health import MAX_HEALTH_PROBES_PER_RUN
    from stream_prober import StreamProber
    from types import SimpleNamespace

    setup = retirement
    now = setup["now"]
    old = _parsed("Alpha vs. Beta", (now - timedelta(hours=age)).astimezone(EASTERN))
    current = _parsed("Fury vs. Usyk", now)
    old_ids = list(range(1, MAX_HEALTH_PROBES_PER_RUN + 1))
    setup["state"].channels[900].update(name=promoted_channel_name(old), streams=old_ids)
    setup["streams"][:] = [
        {"id": sid, "name": "Alpha vs. Beta @ " + old.start.astimezone(EASTERN).strftime("%d %b %I:%M %p ET"), "url": "https://example.invalid/stream",
         "m3u_account": 2, "is_stale": False, "updated_at": now.isoformat()}
        for sid in old_ids
    ] + [{"id": 7301, "name": "Fury vs. Usyk @ 12 Jul 12:00 AM ET", "url": "https://example.invalid/stream",
          "m3u_account": 2, "is_stale": False, "updated_at": now.isoformat()}]
    setup["witness"].update(start=now.isoformat(), stop=(now + timedelta(hours=2)).isoformat())
    setup["config"]["max_promote_per_run"] = 1
    setup["rule"].set_event_sync_config(setup["config"])
    setup["db"].commit()
    client = make_promote_client(setup["state"], next_channel_id=901)
    client.get_streams_by_ids = setup["client"].get_streams_by_ids
    client.get_channel_stats = setup["client"].get_channel_stats
    rows = [
        _resolved(
            setup["streams"][index]["name"],
            DISPOSITION_UNMATCHED,
            old,
            stream_id=sid,
        )
        for index, sid in enumerate(old_ids)
    ]
    rows.append(_resolved(
        setup["streams"][-1]["name"],
        DISPOSITION_UNMATCHED,
        current,
        stream_id=7301,
    ))
    prober = StreamProber.__new__(StreamProber)
    prober.max_concurrent_probes = 1
    prober.account_probe_limits = {2: 1}
    prober._probe_condition = asyncio.Condition()
    prober._account_active = {}
    prober._event_probes = 0
    prober.refresh_account_probe_limits = AsyncMock()
    # Simulated clock: every dial costs two seconds, so a batch that also
    # dials the 200 attached streams ends 400 seconds later, past the
    # five-minute window the current event's own reading has to land in.
    clock_now = [now]
    async def probe(sid, url, name, **kwargs):
        clock_now[0] += timedelta(seconds=2)
        stat = {
            "stream_name": name,
            "probe_status": "success",
            "measured_bitrate": 5000000,
            "last_probed": clock_now[0].isoformat(),
            "is_black_screen": False,
            "black_screen_checked_at": clock_now[0].isoformat(),
        }
        setup["stats"][sid] = stat
        return stat
    prober.probe_stream = AsyncMock(side_effect=probe)
    batches, results = [], []
    with patch("stream_prober.ensure_prober", return_value=prober), \
         patch("channel_pipeline_executor.datetime") as clock, \
         patch("services.event_sync_stream_health.datetime", _clock(lambda: clock_now[0])):
        clock.fromisoformat.side_effect = datetime.fromisoformat
        clock.now.side_effect = lambda *args: clock_now[0]
        managed = [900]
        for offset in [0, 310]:
            # A run builds its executor over the channels it just fetched, so
            # the second run adopts the first run's channel instead of
            # planning another create for it.
            executor = ActionExecutor(
                client,
                list(setup["state"].channels.values()),
                managed_channel_ids=managed,
                epg_sources=setup["epg_sources"],
            )
            clock_now[0] = now + timedelta(seconds=offset)
            setup["source"]["last_success"] = clock_now[0].isoformat()
            result = await executor._execute_event_sync_promotion(
                setup["rule"].id, setup["rule"].name, setup["config"], SimpleNamespace(resolved=rows), ExecutionContext(),
            )
            results.append(result)
            batches.append([call.args[0] for call in prober.probe_stream.await_args_list])
            prober.probe_stream.reset_mock()
            managed = result["channel_ids"]
            setup["db"].get(ChannelPipelineRule, setup["rule"].id).set_managed_channel_ids(managed)
            setup["db"].commit()
    assert [result["promoted_created"] for result in results] == [1, 0]
    # Independent retained events get evidence without probing every alternate.
    assert batches[0] == [7301, old_ids[0]], batches
    assert set(batches[1]) == {7301, old_ids[0] if age == 26 else old_ids[1]}, batches
    assert len(batches[1]) == 2
    assert len(batches[1]) <= MAX_HEALTH_PROBES_PER_RUN
    assert 900 in result["channel_ids"]
    assert 901 in result["channel_ids"]
    assert setup["state"].channels[900]["streams"] == old_ids
    client.delete_channel.assert_not_awaited()


@pytest.mark.asyncio
async def test_probe_batch_drops_queued_streams_after_cancel():
    from services.event_sync_stream_health import _probe_and_collect_failures
    from stream_prober import StreamProber

    client = AsyncMock()
    client.base_url = "http://dispatcharr.test"
    client.get_streams_by_ids.return_value = [
        {
            "id": stream_id,
            "name": f"Event {stream_id}",
            "url": f"https://example.invalid/{stream_id}",
            "m3u_account": 7,
        }
        for stream_id in (1, 2, 3)
    ]
    prober = StreamProber.__new__(StreamProber)
    prober.max_concurrent_probes = 1
    prober.account_probe_limits = {7: 1}
    prober._probe_condition = asyncio.Condition()
    prober._account_active = {}
    prober._event_probes = 0
    prober.refresh_account_probe_limits = AsyncMock()
    started = asyncio.Event()
    release = asyncio.Event()
    stopped = {"value": False}

    async def probe(stream_id, url, name, **kwargs):
        started.set()
        await release.wait()
        return {"probe_status": "failed"}

    prober.probe_stream = AsyncMock(side_effect=probe)
    with patch("stream_prober.ensure_prober", return_value=prober):
        run = asyncio.create_task(_probe_and_collect_failures(
            client,
            [1, 2, 3],
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream={
                stream_id: datetime.now(timezone.utc) - timedelta(minutes=1)
                for stream_id in (1, 2, 3)
            },
            stream_names={
                stream_id: f"Event {stream_id}"
                for stream_id in (1, 2, 3)
            },
            cancelled=lambda: stopped["value"],
        ))
        await started.wait()
        stopped["value"] = True
        release.set()
        dead = await run

    assert dead == set()
    prober.probe_stream.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", ["idle", "source_error", "viewer"])
async def test_old_event_retirement_keeps_its_evidence(retirement, guard):
    from channel_pipeline_executor import ExecutionContext
    from types import SimpleNamespace

    setup = retirement
    # The promoted name carries the start's own clock, and the lifecycle
    # reads that name back in the profile's event timezone, so the start
    # has to be Eastern for the name to identify the same programme.
    parsed = _parsed("Fury vs. Usyk", (setup["now"] - timedelta(hours=26)).astimezone(EASTERN))
    setup["state"].channels[900]["name"] = promoted_channel_name(parsed)
    setup["witness"].update(start=parsed.start.isoformat(), stop=(setup["now"] - timedelta(hours=23)).isoformat())
    if guard == "source_error":
        setup["source"]["status"] = "error"
    elif guard == "viewer":
        setup["client"].get_channel_stats.return_value = {"channels": [{"channel_id": "event-900"}]}
    row = _resolved(STREAM_FURY, DISPOSITION_UNMATCHED, parsed, stream_id=7301)
    executor = ActionExecutor(
        setup["client"],
        list(setup["state"].channels.values()),
        managed_channel_ids=[900],
        epg_sources=setup["epg_sources"],
    )
    setup["executor"] = executor
    with patch("channel_pipeline_executor.datetime") as clock, \
         patch("services.event_sync_stream_health._probe_and_collect_failures", new=AsyncMock()) as probe:
        clock.now.return_value = setup["now"]
        clock.fromisoformat.side_effect = datetime.fromisoformat
        await executor._execute_event_sync_promotion(
            setup["rule"].id, setup["rule"].name, setup["config"], SimpleNamespace(resolved=[row]), ExecutionContext(),
        )
    probe.assert_not_awaited()
    states, result = await _retire(setup)
    assert states[900] == ("unknown" if guard == "source_error" else "idle")
    assert result["channels_removed"] == (1 if guard == "idle" else 0)
    if guard != "idle":
        setup["client"].delete_channel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["finished", "future", "placeholder"])
async def test_idle_or_future_event_is_not_recreated(retirement, case):
    from channel_pipeline_executor import ExecutionContext
    from types import SimpleNamespace
    setup = retirement
    await _retire(setup)
    start = setup["now"] - timedelta(hours=1)
    if case == "future":
        start = setup["now"] + timedelta(hours=1)
    elif case == "placeholder":
        start = setup["now"].replace(year=2098)
    parsed = _parsed("Fury vs. Usyk", start)
    row = _resolved(STREAM_FURY, DISPOSITION_UNMATCHED, parsed, provider_id=2, stream_id=7301)
    executor = ActionExecutor(
        setup["client"],
        list(setup["state"].channels.values()),
        managed_channel_ids=[],
        epg_sources=setup["epg_sources"],
    )
    with patch("channel_pipeline_executor.datetime") as clock:
        clock.now.return_value = setup["now"]
        clock.fromisoformat.side_effect = datetime.fromisoformat
        result = await executor._execute_event_sync_promotion(
            setup["rule"].id, setup["rule"].name, setup["config"], SimpleNamespace(resolved=[row]), ExecutionContext(),
        )
    assert result["promoted_created"] == 0
    assert result["channel_ids"] == []
    assert setup["streams"][0]["id"] == 7301


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["espn", "mlb"])
@pytest.mark.parametrize("with_witness", [False, True])
@pytest.mark.parametrize("case", [
    "seconds_zero", "seconds_nonzero", "shared_patterns", "different_group",
    "different_provider", "missing_provider", "ambiguous_scope", "different_date",
    "different_title", "stale_sample", "bare_slot", "invalid_date",
])
async def test_lifecycle_uses_scoped_iso_patterns(retirement, shape, with_witness, case):
    from services.event_sync_matcher import parse_event_name

    setup = retirement
    now = setup["now"]
    start = now - timedelta(minutes=30)
    # The provider writes start: stamps in UK time; the ESPN+ shape is Eastern.
    local = start.astimezone(EASTERN if shape == "espn" else pytz.timezone("Europe/London"))
    seconds = "00" if case == "seconds_zero" else "37"
    stamp = local.strftime("%Y-%m-%d %H:%M:") + seconds
    if shape == "espn":
        group_id = 1557
        name = f"US (ESPN+ 001) | Soccer: Hansa Rostock vs. Wehen Wiesbaden ({stamp})"
        pattern = {
            "name": "espn-iso-date",
            "title_pattern": r"^US\s+\(ESPN\+\s+\d+\)\s*\|\s*(?P<title>.+?)\s+\((?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\s+(?P<hour>\d{2}):(?P<minute>\d{2}):[0-5]\d\)\s*$",
        }
    else:
        group_id = 1525
        name = f"MLB 01 | Giants x Mets start:{stamp} stop:2026-07-12 06:18:20"
        pattern = {
            "name": "mlb-iso-date",
            "title_pattern": r"^MLB\s+\d+\s*\|\s*(?P<title>.+?)\s+start:(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\s+(?P<hour>\d{2}):(?P<minute>\d{2}):[0-5]\d\s+stop:\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:[0-5]\d\s*$",
        }
    parsed = parse_event_name(name, [pattern], event_timezone="US/Eastern", now=now)
    assert parsed.start == start
    config = setup["config"]
    config.update(secondary_group_ids=[group_id],
                  secondary=[{"group_id": group_id, "m3u_account_id": 18}],
                  group_patterns={str(group_id): [pattern]}, max_promote_per_run=1)
    if case == "shared_patterns":
        config.pop("group_patterns")
        config["patterns"] = [pattern]
    row = _resolved(name, DISPOSITION_UNMATCHED, parsed, provider_id=18,
                    stream_id=7301, group_id=group_id)
    rows = [row]
    if case == "ambiguous_scope":
        rows.append(_resolved(name, DISPOSITION_UNMATCHED, parsed, provider_id=18,
                              stream_id=7301, group_id=group_id + 1))
    plan = build_promotion_plan(config, rows, {}, now=now)
    assert len(plan.units) == 1
    setup["rule"].set_managed_channel_ids([])
    setup["db"].commit()
    fresh = setup["streams"][0]
    fresh.update(name=name, m3u_account={"id": 18}, channel_group={"id": group_id})
    if case == "different_group":
        fresh["channel_group"] = {"id": group_id + 1}
    elif case == "different_provider":
        fresh["m3u_account"] = 2
    elif case == "missing_provider":
        fresh.pop("m3u_account")
    elif case == "different_date":
        fresh["name"] = name.replace(local.strftime("%Y-%m-%d"), "2026-07-10", 1)
    elif case == "different_title":
        fresh["name"] = name.replace(parsed.title, "UFC 999", 1)
    elif case == "bare_slot":
        fresh["name"] = "ESPN PLUS 01:"
    elif case == "invalid_date":
        fresh["name"] = name.replace(local.strftime("%Y-%m-%d"), "2026-02-30", 1)
    setup["stats"][7301] = {
        "stream_name": name, "probe_status": "success",
        "measured_bitrate": 5000000, "is_black_screen": False,
        "last_probed": (now - timedelta(minutes=6) if case == "stale_sample" else now).isoformat(),
        "black_screen_checked_at": (now - timedelta(minutes=6) if case == "stale_sample" else now).isoformat(),
    }
    if with_witness:
        setup["witness"].update(title=parsed.title, start=start.isoformat(),
                                stop=(start + timedelta(hours=2)).isoformat())
    else:
        setup["witness"].clear()
    eligible, states = await setup["executor"]._event_lifecycle(
        setup["rule"].id, config, plan.units, now,
        flow={7301: case != "stale_sample"},
        expires_at=now + timedelta(minutes=5),
    )
    active = case in {"seconds_zero", "seconds_nonzero", "shared_patterns"}
    assert eligible == ({plan.units[0].event_key} if active else set())
    assert states.get(-1, "unknown") == ("active" if active else "unknown")
    setup["client"].create_channel.assert_not_awaited()
    setup["client"].delete_channel.assert_not_awaited()

@pytest.fixture
def promotion_candidates(retirement, monkeypatch):
    from cache import Cache
    from stream_prober import StreamProber

    setup = retirement
    setup["config"].update(max_promote_per_run=1, auto_run=False)
    setup["rule"].set_event_sync_config(setup["config"])
    setup["rule"].set_managed_channel_ids([])
    setup["db"].commit()
    setup["state"].channels.clear()
    setup["witness"].clear()
    setup["clock"] = setup["now"]
    setup["first_health"] = "failed"
    setup["batches"] = []
    start = setup["now"] - timedelta(minutes=30)
    names = ["DAZN 01: Alpha Event @ 11 Jul 11:30 PM ET",
             "DAZN 02: Zulu Event @ 11 Jul 11:30 PM ET"]
    setup["rows"] = [
        _resolved(name, DISPOSITION_UNMATCHED, _parsed(title, start),
                  stream_id=sid, provider_id=2, group_id=SECONDARY_A)
        for name, title, sid in zip(names, ["Alpha Event", "Zulu Event"], [7301, 7302])
    ]
    setup["streams"][:] = [
        {"id": sid, "name": name, "url": "https://example.invalid/stream",
         "is_stale": False, "m3u_account": 2, "channel_group_id": SECONDARY_A,
         "updated_at": setup["now"].isoformat()}
        for sid, name in zip([7301, 7302], names)
    ]
    prober = StreamProber.__new__(StreamProber)
    prober.max_concurrent_probes = 1
    prober.account_probe_limits = {2: 1}
    prober._probe_condition = asyncio.Condition()
    prober._account_active = {}
    prober._event_probes = 0
    prober.refresh_account_probe_limits = AsyncMock()

    async def probe(sid, url, name, **kwargs):
        setup["batches"][-1].append(sid)
        if sid != 7302 and setup["first_health"] == "unknown":
            return {}
        working = sid == 7302 or setup["first_health"] == "success"
        stat = {
            "stream_name": name,
            "probe_status": "success" if working else "failed",
            "measured_bitrate": 5000000 if working else 0,
            "last_probed": setup["clock"].isoformat(),
            "is_black_screen": False if working else None,
            "black_screen_checked_at": (
                setup["clock"].isoformat() if working else None
            ),
        }
        setup["stats"][sid] = stat
        return stat

    prober.probe_stream = AsyncMock(side_effect=probe)
    monkeypatch.setattr("cache._cache", Cache())
    monkeypatch.setattr("stream_prober.ensure_prober", lambda: prober)
    setup["prober"] = prober
    return setup


@pytest.mark.asyncio
@pytest.mark.parametrize("first_health", ["failed", "unknown", "missing_url", "success"])
@pytest.mark.parametrize("first_streams", [1, 201])
async def test_event_health_advances_past_unavailable_candidates(promotion_candidates, first_health, first_streams):
    from channel_pipeline_executor import ExecutionContext
    from services.event_sync_stream_health import MAX_HEALTH_PROBES_PER_RUN
    from types import SimpleNamespace

    setup = promotion_candidates
    setup["first_health"] = first_health
    if first_streams > 1:
        first = setup["rows"][0]
        template = setup["streams"][0]
        setup["rows"] = [
            _resolved(first.stream.name, DISPOSITION_UNMATCHED, first.result.parsed,
                      stream_id=sid, provider_id=2, group_id=SECONDARY_A)
            for sid in range(1, first_streams + 1)
        ] + [setup["rows"][1]]
        setup["streams"][:] = [
            {**template, "id": sid} for sid in range(1, first_streams + 1)
        ] + [setup["streams"][1]]
    if first_health == "missing_url":
        for stream in setup["streams"]:
            if stream["id"] != 7302:
                stream.pop("url")
    results = []
    for index in range(1 if first_health == "success" else 2):
        setup["clock"] = setup["now"] + timedelta(minutes=6 * index)
        setup["batches"].append([])
        executor = ActionExecutor(
            setup["client"],
            list(setup["state"].channels.values()),
            managed_channel_ids=[],
            epg_sources=setup["epg_sources"],
        )
        with patch("channel_pipeline_executor.datetime") as clock, \
             patch("services.event_sync_stream_health.datetime", _clock(setup["clock"])):
            clock.now.return_value = setup["clock"]
            clock.fromisoformat.side_effect = datetime.fromisoformat
            result = await executor._execute_event_sync_promotion(
                setup["rule"].id, setup["rule"].name, setup["config"],
                SimpleNamespace(resolved=setup["rows"]), ExecutionContext(),
            )
        results.append(result["promoted_created"])
    assert results == ([1] if first_health == "success" else [0, 1])
    created = next(iter(setup["state"].channels.values()))
    assert ("Alpha Event" if first_health == "success" else "Zulu Event") in created["name"]
    assert all(len(batch) <= MAX_HEALTH_PROBES_PER_RUN for batch in setup["batches"])
    if first_health != "success":
        assert setup["batches"][-1] == [7302]
    assert len(setup["state"].channels) == 1


@pytest.mark.asyncio
async def test_event_preview_does_not_consume_health_progress(promotion_candidates):
    from channel_pipeline_executor import ExecutionContext
    from types import SimpleNamespace

    setup = promotion_candidates
    with patch("channel_pipeline_executor.datetime") as clock, \
         patch("services.event_sync_stream_health.datetime", _clock(setup["clock"])):
        clock.now.return_value = setup["clock"]
        clock.fromisoformat.side_effect = datetime.fromisoformat
        for dry_run in [True, True, False]:
            setup["batches"].append([])
            executor = ActionExecutor(
                setup["client"],
                [],
                managed_channel_ids=[],
                epg_sources=setup["epg_sources"],
            )
            result = await executor._execute_event_sync_promotion(
                setup["rule"].id, setup["rule"].name, setup["config"],
                SimpleNamespace(resolved=setup["rows"]), ExecutionContext(dry_run=dry_run),
            )
            assert result["promoted_created"] == 0
    assert setup["batches"] == [[], [], [7301]]
    setup["client"].create_channel.assert_not_awaited()


def test_staged_event_recovers_after_health_failures(
    db_session_factory,
    monkeypatch,
):
    from services import event_sync_stream_health

    setup = _staged_event(db_session_factory, monkeypatch, dedicated=True)
    state = setup["state"]
    client = setup["client"]
    dummy_epg = setup["dummy_epg"]
    health = setup["health"]
    current = [setup["event_start"] + timedelta(minutes=1)]
    _, regenerate, wait_refresh = dummy_epg._wire_epg(
        state,
        client,
        db_session_factory,
        regenerated_entries=[
            dummy_epg._dummy_entry(502, 900, setup["event_channel_name"]),
        ],
        now=current[0],
    )
    state.guide_sources[0]["is_active"] = True
    finish = ActionExecutor._finish_event_promotions
    final_failures = ["failed", "unknown"]
    active_stages = {"allocated", "importing", "linking", "ready"}
    seen_attempts = set()

    def set_health(kind):
        observed_at = current[0].astimezone(timezone.utc)
        if kind == "failed":
            health[7301] = {
                "stream_name": setup["event_name"],
                "probe_status": "failed",
                "measured_bitrate": 0,
                "last_probed": observed_at.isoformat(),
                "is_black_screen": None,
                "black_screen_checked_at": None,
            }
        elif kind == "dark":
            health[7301] = {
                "stream_name": setup["event_name"],
                "probe_status": "success",
                "measured_bitrate": 5_000_000,
                "last_probed": observed_at.isoformat(),
                "is_black_screen": True,
                "black_screen_checked_at": observed_at.isoformat(),
            }
        elif kind == "off_air":
            health[7301] = {
                "stream_name": setup["event_name"],
                "probe_status": "success",
                "measured_bitrate": 0,
                "last_probed": observed_at.isoformat(),
                "is_black_screen": False,
                "black_screen_checked_at": observed_at.isoformat(),
            }
        elif kind == "stale":
            stale_at = observed_at - timedelta(minutes=6)
            health[7301] = {
                "stream_name": setup["event_name"],
                "probe_status": "success",
                "measured_bitrate": 5_000_000,
                "last_probed": stale_at.isoformat(),
                "is_black_screen": False,
                "black_screen_checked_at": stale_at.isoformat(),
            }
        elif kind == "incomplete":
            health[7301] = {
                "stream_name": setup["event_name"],
                "probe_status": "success",
                "measured_bitrate": 5_000_000,
                "last_probed": observed_at.isoformat(),
                "is_black_screen": None,
                "black_screen_checked_at": None,
            }
        elif kind == "unknown":
            health.clear()
        else:
            health[7301] = {
                "stream_name": setup["event_name"],
                "probe_status": "success",
                "measured_bitrate": 5_000_000,
                "last_probed": observed_at.isoformat(),
                "is_black_screen": False,
                "black_screen_checked_at": observed_at.isoformat(),
            }

    async def fail_after_admission(executor):
        attempt_id = next((
            receipt.get("attempt_id")
            for event_key, work in executor._event_pending.items()
            for receipt in [
                executor._event_publications.get(
                    work["profile_id"], {}
                ).get("state", {}).get("delivery", {}).get(
                    "pending_channels", {}
                ).get(event_key)
            ]
            if receipt is not None
            and receipt.get("stage") in active_stages
        ), None)
        if attempt_id is not None and attempt_id not in seen_attempts:
            seen_attempts.add(attempt_id)
            if final_failures:
                set_health(final_failures.pop(0))
        return await finish(executor)

    def run_at(when, kind):
        current[0] = when
        set_health(kind)
        session = db_session_factory()
        try:
            assert session.get(
                ChannelPipelineRule,
                setup["rule_id"],
            ).get_event_sync_config()["retire_finished_events"] is True
        finally:
            session.close()
        return dummy_epg._manual_run(
            client,
            db_session_factory,
            regenerate,
            wait_refresh,
        )

    monkeypatch.setattr(
        event_sync_stream_health,
        "_probe_and_collect_failures",
        AsyncMock(return_value=None),
    )
    with patch.object(
        ActionExecutor,
        "_finish_event_promotions",
        new=fail_after_admission,
    ), patch(
        "services.event_sync_resolver.datetime",
        _clock(lambda: current[0]),
    ), patch(
        "channel_pipeline_executor.datetime",
        _clock(lambda: current[0]),
    ), patch(
        "services.event_sync_stream_health.datetime",
        _clock(lambda: current[0]),
    ):
        first_result = run_at(current[0], "positive")
        first = _read_event_publication(
            db_session_factory,
            setup["profile_id"],
        )
        event_key, first_receipt = next(iter(
            first["state"]["delivery"]["pending_channels"].items()
        ))
        assert first_receipt["stage"] == "failed"
        assert first_receipt["reason"] == "health_failed"
        identity = (
            first_receipt["input_hash"],
            first_receipt["channel_id"],
            first_receipt["channel_uuid"],
        )
        assert state.channels[900]["hidden_from_output"] is True
        assert state.channels[900]["streams"] == []

        first_retry = datetime.fromisoformat(first_receipt["retry_at"])
        negative_results = []
        for offset, kind in enumerate((
            "failed", "off_air", "dark", "stale", "incomplete", "unknown",
        ), 1):
            negative_results.append(run_at(
                first_retry + timedelta(seconds=offset),
                kind,
            ))
            unchanged = _read_event_publication(
                db_session_factory,
                setup["profile_id"],
            )["state"]["delivery"]["pending_channels"][event_key]
            assert unchanged["attempt_id"] == first_receipt["attempt_id"]
            assert unchanged["stage"] == "failed"
            assert unchanged["reason"] == "health_failed"
            assert state.channels[900]["hidden_from_output"] is True
            assert state.channels[900]["streams"] == []

        second_result = run_at(first_retry + timedelta(seconds=10), "positive")
        second = _read_event_publication(
            db_session_factory,
            setup["profile_id"],
        )
        second_receipt = second["state"]["delivery"]["pending_channels"][event_key]
        assert second_receipt["attempt_no"] == 2
        assert second_receipt["stage"] == "failed"
        assert second_receipt["reason"] == "health_unknown"
        assert (
            second_receipt["input_hash"],
            second_receipt["channel_id"],
            second_receipt["channel_uuid"],
        ) == identity

        second_retry = datetime.fromisoformat(second_receipt["retry_at"])
        for offset, kind in enumerate((
            "failed", "off_air", "dark", "stale", "incomplete", "unknown",
        ), 1):
            negative_results.append(run_at(
                second_retry + timedelta(seconds=offset),
                kind,
            ))
            unchanged = _read_event_publication(
                db_session_factory,
                setup["profile_id"],
            )["state"]["delivery"]["pending_channels"][event_key]
            assert unchanged["attempt_id"] == second_receipt["attempt_id"]
            assert unchanged["stage"] == "failed"
            assert unchanged["reason"] == "health_unknown"
            assert state.channels[900]["hidden_from_output"] is True
            assert state.channels[900]["streams"] == []

        third_result = run_at(second_retry + timedelta(seconds=10), "positive")

    completed = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    completed_receipt = completed["state"]["delivery"]["pending_channels"][event_key]
    assert completed_receipt["attempt_no"] == 3
    assert completed_receipt["stage"] == "complete"
    assert completed_receipt["reason"] is None
    assert (
        completed_receipt["input_hash"],
        completed_receipt["channel_id"],
        completed_receipt["channel_uuid"],
    ) == identity
    assert final_failures == []
    assert len({
        first_receipt["attempt_id"],
        second_receipt["attempt_id"],
        completed_receipt["attempt_id"],
    }) == 3
    assert client.create_channel.await_count == 1
    assert len(state.channels) == 2
    assert state.channels[900]["epg_data_id"] == 502
    assert state.channels[900]["streams"] == [7301]
    assert state.channels[900]["hidden_from_output"] is False
    assert len([
        payload
        for channel_id, payload in state.update_channel_calls
        if channel_id == 900 and "streams" in payload
    ]) == 1
    assert len([
        payload
        for channel_id, payload in state.update_channel_calls
        if channel_id == 900 and payload.get("hidden_from_output") is False
    ]) == 1
    assert [
        first_result["channels_created"],
        second_result["channels_created"],
        third_result["channels_created"],
    ] == [1, 0, 0]
    assert all(result["channels_created"] == 0 for result in negative_results)


@pytest.mark.parametrize("guard", [
    "expired", "nonhealth_failed", "allocation_unknown", "missing_uuid",
    "foreign_uuid", "ownership", "changed_identity", "new_event",
    "missing_stream", "changed_stream", "changed_config", "changed_source",
    "reached_stop", "positive_idle",
])
def test_staged_health_recovery_keeps_retirement_guards(
    db_session_factory,
    monkeypatch,
    guard,
):
    from services import event_sync_stream_health

    setup = _staged_event(db_session_factory, monkeypatch, dedicated=True)
    state = setup["state"]
    client = setup["client"]
    dummy_epg = setup["dummy_epg"]
    health = setup["health"]
    current = [setup["event_start"] + timedelta(minutes=1)]
    _, regenerate, wait_refresh = dummy_epg._wire_epg(
        state,
        client,
        db_session_factory,
        regenerated_entries=[
            dummy_epg._dummy_entry(502, 900, setup["event_channel_name"]),
        ],
        now=current[0],
    )
    state.guide_sources[0]["is_active"] = True
    finish = ActionExecutor._finish_event_promotions

    def set_positive():
        observed_at = current[0].astimezone(timezone.utc).isoformat()
        health[7301] = {
            "stream_name": setup["event_name"],
            "probe_status": "success",
            "measured_bitrate": 5_000_000,
            "last_probed": observed_at,
            "is_black_screen": False,
            "black_screen_checked_at": observed_at,
        }

    async def close_first(executor):
        if not executor._event_pending:
            return await finish(executor)
        publication = _read_event_publication(
            db_session_factory,
            setup["profile_id"],
        )
        receipt = next(iter(
            publication["state"]["delivery"]["pending_channels"].values()
        ))
        if guard == "expired":
            current[0] = datetime.fromisoformat(receipt["expires_at"])
        elif guard == "nonhealth_failed":
            state.guide_programmes.clear()
        else:
            observed_at = current[0].astimezone(timezone.utc).isoformat()
            health[7301] = {
                "stream_name": setup["event_name"],
                "probe_status": "failed",
                "measured_bitrate": 0,
                "last_probed": observed_at,
                "is_black_screen": None,
                "black_screen_checked_at": None,
            }
        return await finish(executor)

    if guard == "allocation_unknown":
        client.create_channel.side_effect = RuntimeError("allocation failed")
    set_positive()
    monkeypatch.setattr(
        event_sync_stream_health,
        "_probe_and_collect_failures",
        AsyncMock(return_value=None),
    )
    with patch.object(
        ActionExecutor,
        "_finish_event_promotions",
        new=close_first,
    ), patch(
        "services.event_sync_resolver.datetime",
        _clock(lambda: current[0]),
    ), patch(
        "channel_pipeline_executor.datetime",
        _clock(lambda: current[0]),
    ), patch(
        "services.event_sync_stream_health.datetime",
        _clock(lambda: current[0]),
    ):
        dummy_epg._manual_run(
            client,
            db_session_factory,
            regenerate,
            wait_refresh,
        )

    before = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    pending_before = before["state"]["delivery"]["pending_channels"]
    pending_keys = set(pending_before)
    event_key, receipt = next(iter(pending_before.items()))
    if guard == "expired":
        assert (receipt["stage"], receipt["reason"]) == (
            "expired", "guide_expired",
        )
    elif guard == "nonhealth_failed":
        assert (receipt["stage"], receipt["reason"]) == (
            "failed", "programme_missing",
        )
    elif guard == "allocation_unknown":
        assert (receipt["stage"], receipt["reason"]) == (
            "allocation_unknown", "allocation_unknown",
        )
    else:
        assert (receipt["stage"], receipt["reason"]) == (
            "failed", "health_failed",
        )

    if receipt["retry_at"] is not None:
        current[0] = datetime.fromisoformat(receipt["retry_at"]) + timedelta(seconds=1)
    else:
        current[0] += timedelta(minutes=6)
    if guard == "reached_stop":
        current[0] = datetime.fromisoformat(receipt["stop"])
    if guard == "missing_uuid":
        state.channels[900].pop("uuid", None)
    elif guard == "foreign_uuid":
        state.channels[900]["uuid"] = "foreign-channel"
    elif guard == "ownership":
        session = db_session_factory()
        try:
            session.get(ChannelPipelineRule, setup["rule_id"]).set_managed_channel_ids([])
            session.commit()
        finally:
            session.close()
    elif guard == "changed_identity":
        state.secondary_streams[SECONDARY_B_NAME][0]["name"] = (
            setup["event_name"].replace("Fury vs. Usyk", "FURY VS. USYK")
        )
    elif guard == "new_event":
        state.secondary_streams[SECONDARY_B_NAME][0]["name"] = (
            setup["event_name"].replace("Fury vs. Usyk", "Fury vs. Joshua")
        )
    elif guard == "missing_stream":
        state.secondary_streams[SECONDARY_B_NAME] = []
    elif guard == "changed_stream":
        state.secondary_streams[SECONDARY_B_NAME][0]["id"] = 7302
        health[7302] = copy.deepcopy(health[7301])
    elif guard == "changed_config":
        session = db_session_factory()
        try:
            rule = session.get(ChannelPipelineRule, setup["rule_id"])
            config = rule.get_event_sync_config()
            config["max_promote_per_run"] = 2
            rule.set_event_sync_config(config)
            session.commit()
        finally:
            session.close()
    elif guard == "changed_source":
        state.guide_sources[0]["url"] = (
            state.guide_sources[0]["url"].rstrip("/") + "/changed"
        )

    set_positive()
    if guard in {"changed_identity", "new_event"}:
        health[7301]["stream_name"] = state.secondary_streams[
            SECONDARY_B_NAME
        ][0]["name"]
    if guard == "changed_stream":
        health[7302] = {
            **health.pop(7301),
            "stream_name": state.secondary_streams[SECONDARY_B_NAME][0]["name"],
        }
    create_count = client.create_channel.await_count
    write_count = len(state.update_channel_calls)
    lifecycle = ActionExecutor._event_lifecycle

    async def mark_idle(executor, *args, **kwargs):
        eligible, states = await lifecycle(executor, *args, **kwargs)
        if 900 in states:
            states[900] = "idle"
        return eligible, states

    session = db_session_factory()
    try:
        rule_config = session.get(
            ChannelPipelineRule,
            setup["rule_id"],
        ).get_event_sync_config()
        assert rule_config["retire_finished_events"] is True
    finally:
        session.close()

    parsed_current = None
    current_event_key = None
    current_channel_name = None
    if guard in {"changed_identity", "new_event"}:
        current_stream = state.secondary_streams[SECONDARY_B_NAME][0]
        parsed_current = parse_event_name(
            current_stream["name"],
            rule_config.get("slot_patterns"),
            now=current[0],
            assume_current_date=rule_config.get("assume_current_date", False),
        )
        current_event_key = master_event_key(parsed_current)
        current_channel_name = promoted_channel_name(parsed_current)
        assert parsed_current.start == datetime.fromisoformat(receipt["start"])
        assert current_stream["id"] == receipt["streams"][0]["id"]
        assert current_event_key is not None
        if guard == "changed_identity":
            assert current_event_key == event_key
            assert current_channel_name == receipt["channel_name"]
            assert parsed_current.title != receipt["title"]
            assert current_stream["name"] != receipt["streams"][0]["name"]
        else:
            assert current_event_key != event_key
            assert current_channel_name != receipt["channel_name"]

    clock_patches = (
        patch("services.event_sync_resolver.datetime", _clock(lambda: current[0])),
        patch("channel_pipeline_executor.datetime", _clock(lambda: current[0])),
        patch("services.event_sync_stream_health.datetime", _clock(lambda: current[0])),
    )
    with clock_patches[0], clock_patches[1], clock_patches[2]:
        if guard == "positive_idle":
            with patch.object(
                ActionExecutor,
                "_event_lifecycle",
                new=mark_idle,
            ):
                dummy_epg._manual_run(
                    client,
                    db_session_factory,
                    regenerate,
                    wait_refresh,
                )
        else:
            dummy_epg._manual_run(
                client,
                db_session_factory,
                regenerate,
                wait_refresh,
            )

    after = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    pending_after = after["state"]["delivery"]["pending_channels"]
    after_receipt = pending_after[event_key]
    if guard == "new_event":
        assert parsed_current is not None
        assert current_event_key is not None
        assert current_channel_name is not None
        assert after_receipt == receipt
        assert set(pending_after) == pending_keys | {current_event_key}
        new_receipt = pending_after[current_event_key]
        assert new_receipt["attempt_no"] == 1
        assert new_receipt["attempt_id"] != receipt["attempt_id"]
        assert new_receipt["channel_id"] == 901
        assert new_receipt["channel_uuid"] == "event-901"
        assert new_receipt["channel_id"] != receipt["channel_id"]
        assert new_receipt["channel_uuid"] != receipt["channel_uuid"]
        assert (
            new_receipt["rule_id"],
            new_receipt["profile_id"],
            new_receipt["target_group_id"],
        ) == (
            receipt["rule_id"],
            receipt["profile_id"],
            receipt["target_group_id"],
        )
        assert new_receipt["start"] == receipt["start"]
        assert new_receipt["channel_name"] == current_channel_name
        assert client.create_channel.await_count == create_count + 1
        assert state.channels[901]["hidden_from_output"] is True
        assert state.channels[901]["streams"] == []
    else:
        assert set(pending_after) == pending_keys
        assert after_receipt["attempt_id"] == receipt["attempt_id"]
        assert after_receipt["stage"] == receipt["stage"]
        assert client.create_channel.await_count == create_count
        assert len(state.update_channel_calls) == write_count
    if 900 in state.channels:
        assert state.channels[900].get("hidden_from_output") is True
        assert state.channels[900].get("streams") == []


def test_staged_health_recovery_outlives_failed_attempt(
    db_session_factory,
    monkeypatch,
):
    from models import DummyEPGProfile
    from services import event_sync_stream_health
    from services.epg_publication import begin_delivery

    setup = _staged_event(db_session_factory, monkeypatch, dedicated=True)
    session = db_session_factory()
    try:
        profile = session.get(DummyEPGProfile, setup["profile_id"]).to_dict()
    finally:
        session.close()
    early = setup["event_start"] - timedelta(hours=23)
    with patch(
        "services.epg_publication.get_session",
        side_effect=db_session_factory,
    ):
        seeded = begin_delivery(
            f"profile:{setup['profile_id']}",
            expected_revision=0,
            expected_hash=None,
            profile=profile,
            now=early,
        )
    first_guide = seeded["state"]["delivery"]["guide_attempt"]
    assert first_guide["expires_at"] is None

    state = setup["state"]
    client = setup["client"]
    dummy_epg = setup["dummy_epg"]
    health = setup["health"]
    current = [setup["event_start"] + timedelta(minutes=1)]
    _, regenerate, wait_refresh = dummy_epg._wire_epg(
        state,
        client,
        db_session_factory,
        regenerated_entries=[
            dummy_epg._dummy_entry(502, 900, setup["event_channel_name"]),
        ],
        now=current[0],
    )
    state.guide_sources[0]["is_active"] = True
    finish = ActionExecutor._finish_event_promotions
    active_stages = {"allocated", "importing", "linking", "ready"}
    seen_attempts = set()

    def set_health(working):
        observed_at = current[0].astimezone(timezone.utc).isoformat()
        health[7301] = {
            "stream_name": setup["event_name"],
            "probe_status": "success" if working else "failed",
            "measured_bitrate": 5_000_000 if working else 0,
            "last_probed": observed_at,
            "is_black_screen": False if working else None,
            "black_screen_checked_at": observed_at if working else None,
        }

    async def fail_then_pause(executor):
        attempt_id = next((
            receipt.get("attempt_id")
            for event_key, work in executor._event_pending.items()
            for receipt in [
                executor._event_publications.get(
                    work["profile_id"], {}
                ).get("state", {}).get("delivery", {}).get(
                    "pending_channels", {}
                ).get(event_key)
            ]
            if receipt is not None
            and receipt.get("stage") in active_stages
        ), None)
        if attempt_id is None or attempt_id in seen_attempts:
            return await finish(executor)
        seen_attempts.add(attempt_id)
        if len(seen_attempts) == 1:
            set_health(False)
            return await finish(executor)
        raise asyncio.CancelledError

    set_health(True)
    monkeypatch.setattr(
        event_sync_stream_health,
        "_probe_and_collect_failures",
        AsyncMock(return_value=None),
    )
    with patch.object(
        ActionExecutor,
        "_finish_event_promotions",
        new=fail_then_pause,
    ), patch(
        "services.event_sync_resolver.datetime",
        _clock(lambda: current[0]),
    ), patch(
        "channel_pipeline_executor.datetime",
        _clock(lambda: current[0]),
    ), patch(
        "services.event_sync_stream_health.datetime",
        _clock(lambda: current[0]),
    ):
        dummy_epg._manual_run(
            client,
            db_session_factory,
            regenerate,
            wait_refresh,
        )
        failed = _read_event_publication(
            db_session_factory,
            setup["profile_id"],
        )
        event_key, failed_receipt = next(iter(
            failed["state"]["delivery"]["pending_channels"].items()
        ))
        assert (failed_receipt["stage"], failed_receipt["reason"]) == (
            "failed", "health_failed",
        )
        assert failed_receipt["expires_at"] == failed_receipt["stop"]
        current[0] = early + timedelta(hours=24, seconds=1)
        assert current[0] < datetime.fromisoformat(failed_receipt["stop"])
        set_health(True)
        with pytest.raises(asyncio.CancelledError):
            dummy_epg._manual_run(
                client,
                db_session_factory,
                regenerate,
                wait_refresh,
            )

    successor = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    next_guide = successor["state"]["delivery"]["guide_attempt"]
    next_receipt = successor["state"]["delivery"]["pending_channels"][event_key]
    assert next_guide["attempt_id"] != first_guide["attempt_id"]
    assert next_receipt["attempt_id"] != failed_receipt["attempt_id"]
    assert next_receipt["attempt_no"] == 2
    assert next_receipt["guide_attempt_id"] == next_guide["attempt_id"]
    assert next_receipt["channel_id"] == failed_receipt["channel_id"] == 900
    assert next_receipt["channel_uuid"] == failed_receipt["channel_uuid"] == "event-900"
    assert next_receipt["history"][-1]["expires_at"] == failed_receipt["stop"]
    assert next_receipt["expires_at"] == failed_receipt["stop"]
    assert state.channels[900]["hidden_from_output"] is True
    assert state.channels[900]["streams"] == []
    assert client.create_channel.await_count == 1


def test_staged_health_recovery_loses_recorded_attempt_race(
    db_session_factory,
    monkeypatch,
):
    from models import DummyEPGProfile
    from services import event_sync_stream_health
    from services.epg_publication import begin_delivery
    from tasks.event_visibility import _source_refresh_key

    setup = _staged_event(db_session_factory, monkeypatch, dedicated=True)
    state = setup["state"]
    client = setup["client"]
    dummy_epg = setup["dummy_epg"]
    health = setup["health"]
    current = [setup["event_start"] + timedelta(minutes=1)]
    _, regenerate, wait_refresh = dummy_epg._wire_epg(
        state,
        client,
        db_session_factory,
        regenerated_entries=[
            dummy_epg._dummy_entry(502, 900, setup["event_channel_name"]),
        ],
        now=current[0],
    )
    state.guide_sources[0]["is_active"] = True
    finish = ActionExecutor._finish_event_promotions

    def set_health(working):
        observed_at = current[0].astimezone(timezone.utc).isoformat()
        health[7301] = {
            "stream_name": setup["event_name"],
            "probe_status": "success" if working else "failed",
            "measured_bitrate": 5_000_000 if working else 0,
            "last_probed": observed_at,
            "is_black_screen": False if working else None,
            "black_screen_checked_at": observed_at if working else None,
        }

    async def fail_first(executor):
        set_health(False)
        return await finish(executor)

    set_health(True)
    monkeypatch.setattr(
        event_sync_stream_health,
        "_probe_and_collect_failures",
        AsyncMock(return_value=None),
    )
    with patch.object(
        ActionExecutor,
        "_finish_event_promotions",
        new=fail_first,
    ), patch(
        "services.event_sync_resolver.datetime",
        _clock(lambda: current[0]),
    ), patch(
        "channel_pipeline_executor.datetime",
        _clock(lambda: current[0]),
    ), patch(
        "services.event_sync_stream_health.datetime",
        _clock(lambda: current[0]),
    ):
        dummy_epg._manual_run(
            client,
            db_session_factory,
            regenerate,
            wait_refresh,
        )

    failed = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    event_key, failed_receipt = next(iter(
        failed["state"]["delivery"]["pending_channels"].items()
    ))
    assert (failed_receipt["stage"], failed_receipt["reason"]) == (
        "failed", "health_failed",
    )
    current[0] = datetime.fromisoformat(
        failed_receipt["retry_at"]
    ) + timedelta(seconds=1)
    set_health(True)
    create_count = client.create_channel.await_count
    write_count = len(state.update_channel_calls)
    current_claim = ActionExecutor._event_receipt_current
    winner = []

    def advance_attempt(executor, publication, key, *args, **kwargs):
        current_value = current_claim(
            executor,
            publication,
            key,
            *args,
            **kwargs,
        )
        if current_value is None or winner:
            return current_value
        claimed_publication, receipt = current_value
        if receipt["stage"] != "failed" or receipt["reason"] != "health_failed":
            return current_value
        session = db_session_factory()
        try:
            profile = session.get(
                DummyEPGProfile,
                setup["profile_id"],
            ).to_dict()
        finally:
            session.close()
        source = next(
            row for row in executor._epg_sources
            if row.get("id") == setup["source_id"]
        )
        _, endpoint_hash, source_url_hash = _source_refresh_key(
            executor.client,
            source,
            claimed_publication["scope"],
        )
        candidate = {
            key: copy.deepcopy(receipt[key])
            for key in (
                "event_key", "rule_id", "rule_hash", "profile_id",
                "target_group_id", "title", "start", "stop", "streams",
                "channel_name", "channel_id", "channel_uuid", "execution_id",
            )
        }
        candidate.update({
            "source_hashes": [{
                "endpoint_hash": endpoint_hash,
                "source_url_hash": source_url_hash,
            }],
            "owner_proven": True,
            "channel_exists": True,
            "health_playable": True,
        })
        admitted = begin_delivery(
            claimed_publication["scope"],
            expected_revision=claimed_publication["revision"],
            expected_hash=claimed_publication["state"]["xmltv_hash"],
            profile=profile,
            now=current[0],
            pending_channels={key: candidate},
        )
        assert admitted is not None
        winner.append(admitted)
        return current_value

    with patch.object(
        ActionExecutor,
        "_event_receipt_current",
        new=advance_attempt,
    ), patch(
        "services.event_sync_resolver.datetime",
        _clock(lambda: current[0]),
    ), patch(
        "channel_pipeline_executor.datetime",
        _clock(lambda: current[0]),
    ), patch(
        "services.event_sync_stream_health.datetime",
        _clock(lambda: current[0]),
    ):
        dummy_epg._manual_run(
            client,
            db_session_factory,
            regenerate,
            wait_refresh,
        )

    assert len(winner) == 1
    stored = _read_event_publication(
        db_session_factory,
        setup["profile_id"],
    )
    receipt = stored["state"]["delivery"]["pending_channels"][event_key]
    winning_receipt = winner[0]["state"]["delivery"]["pending_channels"][event_key]
    assert receipt["attempt_no"] == 2
    assert receipt["attempt_id"] != failed_receipt["attempt_id"]
    assert receipt["attempt_id"] == winning_receipt["attempt_id"]
    assert receipt["stage"] == "allocated"
    assert client.create_channel.await_count == create_count
    assert len(state.update_channel_calls) == write_count
    assert state.channels[900]["hidden_from_output"] is True
    assert state.channels[900]["streams"] == []


class TestDedicatedDelivery:
    def test_imported_target_guide_precedes_attachment_and_reveal(self, db_session_factory, monkeypatch):
        from models import DummyEPGProfile

        setup, executor, finish = _pending_completion(db_session_factory, monkeypatch, dedicated=True)
        before = _read_event_publication(db_session_factory, setup["profile_id"])
        key, receipt = next(iter(before["state"]["delivery"]["pending_channels"].items()))
        assert setup["state"].channels[900]["epg_data_id"] == 502
        assert setup["state"].channels[900]["hidden_from_output"] is True
        assert setup["state"].channels[900]["streams"] == []
        assert _managed_ids(db_session_factory, setup["rule_id"]) == [900]
        current = datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)
        with patch("database.get_session", side_effect=db_session_factory), patch(
            "services.epg_publication.get_session", side_effect=db_session_factory
        ), patch("channel_pipeline_executor.datetime", _clock(current)):
            _run(finish(executor))
        after = _read_event_publication(db_session_factory, setup["profile_id"])
        assert after["state"]["delivery"]["pending_channels"][key]["stage"] == "complete"
        assert after["state"]["delivery"]["pending_channels"][key]["expires_at"] == receipt["expires_at"]
        assert setup["state"].channels[900]["streams"] == [7301]
        assert setup["state"].channels[900]["hidden_from_output"] is False
        assert [call.kwargs.get("m3u_account") for call in setup["client"].get_streams.call_args_list] == [1, 2]
        session = db_session_factory()
        try:
            profile = session.get(DummyEPGProfile, setup["profile_id"])
            assert profile.get_channel_group_ids() == [PROMOTE_GROUP_ID]
            assert profile.get_hide_empty_group_ids() == [PROMOTE_GROUP_ID]
            assert profile.get_epg_source_ids() == []
            assert profile.get_channel_mappings() == []
        finally:
            session.close()
        assert_never_touched_group_settings(setup["client"])

    @pytest.mark.parametrize("fault", [
        "expired", "rule_disabled", "owner_removed", "wrong_uuid", "wrong_group",
        "profile_changed", "guide_missing", "health_unknown", "pending_lost",
    ])
    def test_lost_delivery_proof_never_attaches_or_reveals(self, db_session_factory, monkeypatch, fault):
        from models import DummyEPGProfile
        from services import event_sync_stream_health

        setup, executor, finish = _pending_completion(db_session_factory, monkeypatch, dedicated=True)
        before = _read_event_publication(db_session_factory, setup["profile_id"])
        receipt = next(iter(before["state"]["delivery"]["pending_channels"].values()))
        current = datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)
        session = db_session_factory()
        try:
            rule = session.get(ChannelPipelineRule, setup["rule_id"])
            if fault == "rule_disabled":
                rule.enabled = False
            elif fault == "owner_removed":
                rule.set_managed_channel_ids([])
            elif fault == "profile_changed":
                session.get(DummyEPGProfile, setup["profile_id"]).set_channel_group_ids([MASTER_GROUP_ID])
            session.commit()
        finally:
            session.close()
        if fault == "expired":
            current = datetime.fromisoformat(receipt["expires_at"])
        elif fault == "wrong_uuid":
            setup["state"].channels[900]["uuid"] = "foreign-channel"
        elif fault == "wrong_group":
            setup["state"].channels[900]["channel_group_id"] = MASTER_GROUP_ID
        elif fault == "guide_missing":
            setup["state"].guide_programmes.clear()
        elif fault == "health_unknown":
            monkeypatch.setattr(event_sync_stream_health, "_load_stats", AsyncMock(return_value={}))
        elif fault == "pending_lost":
            executor._event_pending.clear()
        original = copy.deepcopy(setup["state"].channels[900])
        write_count = len(setup["state"].update_channel_calls)
        with patch("database.get_session", side_effect=db_session_factory), patch(
            "services.epg_publication.get_session", side_effect=db_session_factory
        ), patch("channel_pipeline_executor.datetime", _clock(current)):
            _run(finish(executor))
        assert setup["state"].channels[900] == original
        assert len(setup["state"].update_channel_calls) == write_count
        setup["client"].delete_channel.assert_not_awaited()

    @pytest.mark.parametrize("fault", ["manual", "foreign_rule", "wrong_uuid", "wrong_group", "changed_rule", "changed_profile", "unknown_ledger"])
    def test_target_admission_uses_fresh_rule_ownership(self, db_session_factory, monkeypatch, fault):
        from channel_pipeline_executor import validate_event_target
        from channel_pipeline_schema import validate_event_sync_config
        from models import DummyEPGProfile

        setup, executor, _ = _pending_completion(db_session_factory, monkeypatch, dedicated=True)
        session = db_session_factory()
        try:
            rule = session.get(ChannelPipelineRule, setup["rule_id"])
            config = copy.deepcopy(rule.get_event_sync_config())
            with patch("database.get_session", side_effect=db_session_factory):
                assert validate_event_sync_config(config) == []
            if fault == "manual":
                setup["state"].channels[901] = {"id": 901, "uuid": "manual", "channel_group_id": PROMOTE_GROUP_ID, "streams": []}
            elif fault == "foreign_rule":
                rule.set_managed_channel_ids([])
                executor._pipeline_managed_channel_ids.add(900)
            elif fault == "wrong_uuid":
                setup["state"].channels[900]["uuid"] = "foreign"
            elif fault == "wrong_group":
                setup["state"].channels[900]["channel_group_id"] = MASTER_GROUP_ID
            elif fault == "changed_rule":
                value = rule.get_event_sync_config()
                value["max_promote_per_run"] = 1
                rule.set_event_sync_config(value)
            elif fault == "changed_profile":
                session.get(DummyEPGProfile, setup["profile_id"]).set_epg_source_ids([999])
            else:
                rule.managed_channel_ids = "broken"
            session.commit()
        finally:
            session.close()
        original = copy.deepcopy(setup["state"].channels)
        with patch("database.get_session", side_effect=db_session_factory), patch(
            "services.epg_publication.get_session", side_effect=db_session_factory
        ), pytest.raises(ValueError):
            validate_event_target(config, setup["rule_id"], list(original.values()), [{"id": PROMOTE_GROUP_ID}])
        assert setup["state"].channels == original

    def test_equal_start_accounts_form_one_ordered_promotion_unit(self):
        from tests.event_sync_fixtures import dedicated_event_sync_config

        rows = [
            _resolved("Second event", DISPOSITION_UNMATCHED, _parsed("Same Event", START), provider_id=18, stream_id=181, group_id=1514),
            _resolved("First event", DISPOSITION_UNMATCHED, _parsed("Same Event", START), provider_id=2, stream_id=21, group_id=2491),
        ]
        config = dedicated_event_sync_config(secondary=[{"group_id": 2491, "m3u_account_id": 2}, {"group_id": 1514, "m3u_account_id": 18}])
        plan = build_promotion_plan(config, rows, {}, now=START)
        assert len(plan.units) == 1
        assert [row.stream.provider_id for row in plan.units[0].rows] == [2, 18]
        assert [row.stream.stream_id for row in plan.units[0].rows] == [21, 181]
        assert plan.units[0].action == PROMOTE_ACTION_CREATE


    @pytest.mark.parametrize("control,args", [
        ("test_expiry_during_the_completion_grid_closes_only_its_receipt", ()),
        ("test_expiry_after_flow_stops_before_link_or_channel_mutation", ()),
        ("test_expiry_at_a_publication_guard_closes_outside_the_lock", ("linking", 1, False)),
        ("test_expiry_at_a_publication_guard_closes_outside_the_lock", ("attach", 2, False)),
        ("test_expiry_at_a_publication_guard_closes_outside_the_lock", ("stale_removal", 3, True)),
        ("test_expiry_at_a_publication_guard_closes_outside_the_lock", ("reveal", 3, False)),
        ("test_expiry_at_a_publication_guard_closes_outside_the_lock", ("completion", 4, False)),
        ("test_an_admitted_mutation_that_outlives_expiry_starts_no_followup", ("attach",)),
        ("test_an_admitted_mutation_that_outlives_expiry_starts_no_followup", ("reveal",)),
        ("test_channel_read_failure_uses_live_failure_or_expiry", (1, False, "failed")),
        ("test_channel_read_failure_uses_live_failure_or_expiry", (1, True, "expired")),
        ("test_channel_read_failure_uses_live_failure_or_expiry", (2, False, "failed")),
        ("test_channel_read_failure_uses_live_failure_or_expiry", (2, True, "expired")),
        ("test_channel_read_failure_uses_live_failure_or_expiry", (3, False, "failed")),
        ("test_channel_read_failure_uses_live_failure_or_expiry", (3, True, "expired")),
        ("test_channel_read_failure_uses_live_failure_or_expiry", (4, False, "failed")),
        ("test_channel_read_failure_uses_live_failure_or_expiry", (4, True, "expired")),
        ("test_fresh_receipt_cannot_use_the_expired_writer", ()),
        ("test_receipt_guard_keeps_live_and_expired_authority_separate", ()),
        *[("test_expired_writer_cannot_close_a_mismatched_owner", (value,)) for value in [
            "revision", "xmltv_hash", "config_hash", "guide_attempt", "receipt_attempt",
            "profile_disabled", "profile_changed", "rule_disabled", "rule_changed",
            "managed_channel", "channel_group", "channel_uuid", "missing_channel",
            "missing_channel_with_evidence", "replacement",
        ]],
        *[("test_completion_rejects_removed_current_rule_ownership", (expired, missing, foreign))
          for expired in (False, True) for missing in (False, True) for foreign in (False, True)],
        *[("test_completion_cancellation_never_writes_expiry", (phase,)) for phase in ("guide", "flow", "lock")],
    ])
    def test_dedicated_delivery_keeps_existing_expiry_and_owner_controls(self, db_session_factory, monkeypatch, control, args):
        import sys
        from functools import partial

        monkeypatch.setattr(sys.modules[__name__], "_pending_completion", partial(_pending_completion, dedicated=True))
        globals()[control](*args, db_session_factory=db_session_factory, monkeypatch=monkeypatch)

    @pytest.mark.parametrize("fault", ["pending_lost", "owner_deleted", "rule_changed"])
    def test_direct_guide_write_requires_the_current_work_and_owner(self, db_session_factory, monkeypatch, fault):
        from channel_pipeline_evaluator import StreamContext
        from channel_pipeline_schema import Action, ActionType
        from channel_pipeline_executor import ExecutionContext

        setup, executor, _ = _pending_completion(db_session_factory, monkeypatch, dedicated=True)
        executor._event_pending.clear()
        session = db_session_factory()
        try:
            rule = session.get(ChannelPipelineRule, setup["rule_id"])
            if fault == "owner_deleted":
                session.delete(rule)
            elif fault == "rule_changed":
                config = rule.get_event_sync_config()
                config["max_promote_per_run"] = 1
                rule.set_event_sync_config(config)
            session.commit()
        finally:
            session.close()
        original = copy.deepcopy(setup["state"].channels[900])
        count = len(setup["state"].update_channel_calls)
        with patch("database.get_session", side_effect=db_session_factory):
            result = _run(executor._execute_assign_epg(
                Action(type=ActionType.ASSIGN_EPG, params={"epg_id": setup["source_id"]}),
                StreamContext(stream_id=7301, stream_name=setup["event_name"]),
                ExecutionContext(current_channel_id=900),
            ))
        assert result.success is False
        assert setup["state"].channels[900] == original
        assert len(setup["state"].update_channel_calls) == count

    def test_restart_after_guide_completion_keeps_the_same_channel(self, db_session_factory, monkeypatch):
        setup, executor, finish = _pending_completion(db_session_factory, monkeypatch, dedicated=True)
        before = _read_event_publication(db_session_factory, setup["profile_id"])
        receipt = next(iter(before["state"]["delivery"]["pending_channels"].values()))
        current = datetime.fromisoformat(receipt["admitted_at"]) + timedelta(seconds=1)
        with patch("database.get_session", side_effect=db_session_factory), patch(
            "services.epg_publication.get_session", side_effect=db_session_factory
        ), patch("channel_pipeline_executor.datetime", _clock(current)):
            _run(finish(executor))
        client = make_promote_client(setup["state"], next_channel_id=901)
        client.get_channel_groups.return_value = [{"id": PROMOTE_GROUP_ID, "name": "Dedicated events"}]
        client.get_m3u_group_settings_by_provider = AsyncMock(return_value={(1, SECONDARY_A): {"auto_channel_sync": False}, (2, SECONDARY_B): {"auto_channel_sync": False}})
        dummy_epg = setup["dummy_epg"]
        _, regenerate, wait_refresh = dummy_epg._wire_epg(
            setup["state"], client, db_session_factory,
            initial_entries=[dummy_epg._dummy_entry(502, 900, setup["event_channel_name"])],
            regenerated_entries=[dummy_epg._dummy_entry(502, 900, setup["event_channel_name"])],
        )
        setup["state"].guide_sources[0]["is_active"] = True
        with patch("services.event_sync_resolver.datetime") as clock:
            clock.now.return_value = setup["event_start"] + timedelta(minutes=1)
            result = dummy_epg._manual_run(client, db_session_factory, regenerate, wait_refresh)
        assert result["success"] is True
        client.create_channel.assert_not_awaited()
        assert set(setup["state"].channels) == {100, 900}
        assert setup["state"].channels[900]["streams"] == [7301]
        assert setup["state"].channels[900]["epg_data_id"] == 502
        assert _managed_ids(db_session_factory, setup["rule_id"]) == [900]

    def test_dedicated_plan_keeps_dateless_dead_past_and_early_events_hidden(self):
        from tests.event_sync_fixtures import dedicated_event_sync_config

        rows = [
            _resolved("Dateless", DISPOSITION_UNMATCHED, _parsed("Dateless", FROZEN_NOW, matched_pattern="dateless-title-time-ampm"), stream_id=1),
            _resolved("Past", DISPOSITION_UNMATCHED, _parsed("Past", FROZEN_NOW - timedelta(days=2)), stream_id=2),
            _resolved("Early", DISPOSITION_UNMATCHED, _parsed("Early", FROZEN_NOW + timedelta(days=2)), stream_id=3),
            _resolved("Dead", DISPOSITION_UNMATCHED, _parsed("Dead", FROZEN_NOW), stream_id=4),
        ]
        plan = build_promotion_plan(dedicated_event_sync_config(promote_lead_hours=12), rows, {}, now=FROZEN_NOW, dead_stream_ids={4})
        assert plan.units == ()
        assert plan.skipped_dateless == 1
        assert plan.skipped_past == 1
        assert plan.skipped_early == 1
        assert plan.skipped_all_dead == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("count,bad", [(255, None), (256, None), (714, None), (714, "oversized"), (714, "invalid")])
async def test_lifecycle_batches_retained_channels(retirement, count, bad):
    from cache import Cache
    from services import epg_programmes

    setup = retirement
    now = setup["now"]
    name = "DAZN 07: Fresh Match @ 11 Jul 11:30 PM ET"
    row = _resolved(name, DISPOSITION_UNMATCHED, _parsed("Fresh Match", now - timedelta(minutes=30)),
                    provider_id=2, stream_id=7301, group_id=SECONDARY_A)
    plan = build_promotion_plan(setup["config"], [row], {}, now=now)
    assert len(plan.units) == 1
    channels = [{
        "id": cid, "name": FURY_CHANNEL_NAME, "channel_group_id": PROMOTE_GROUP_ID,
        "streams": [10000 + cid],
    } for cid in range(1, count + 1)]
    if bad == "oversized":
        channels[0]["streams"] = list(range(20000, 21001))
    elif bad == "invalid":
        channels[0]["streams"] = [{}]
    channels.append({"id": 9000, "name": "Foreign", "channel_group_id": 99, "streams": [50000]})
    setup["rule"].set_managed_channel_ids(list(range(1, count + 1)))
    setup["db"].commit()
    setup["streams"][:] = [{
        "id": 10000 + cid, "name": STREAM_FURY, "url": f"https://example.invalid/{cid}",
        "m3u_account": 2, "channel_group_id": SECONDARY_A,
    } for cid in range(1, count + 1)] + [{
        "id": 7301, "name": name, "url": "https://example.invalid/new",
        "m3u_account": 2, "channel_group_id": SECONDARY_A,
    }]
    setup["stats"][7301] = {
        "stream_name": name, "probe_status": "success", "measured_bitrate": 5000000,
        "last_probed": now.isoformat(), "is_black_screen": False,
        "black_screen_checked_at": now.isoformat(),
    }
    setup["witness"].clear()
    executor = ActionExecutor(setup["client"], channels)
    cache = Cache()
    with patch("cache.get_cache", return_value=cache), \
         patch("stream_prober.ensure_prober", return_value=None):
        eligible, states = await executor._event_lifecycle(
            setup["rule"].id, setup["config"], plan.units, now, flow={7301: True},
            expires_at=None, advance=True,
        )
        assert eligible == {plan.units[0].event_key}
        assert states[-1] == "active"
        assert all(states[cid] == "unknown" for cid in range(1, count + 1))
        assert 9000 not in states
        assert epg_programmes.prepare_profiles.await_args.kwargs["recover_sources"] is True
        positions = copy.deepcopy(cache.get("event_sync_lifecycle_positions", ttl=86400))
        await executor._event_lifecycle(
            setup["rule"].id, setup["config"], (), now, flow={},
            expires_at=None, advance=False,
        )
        assert cache.get("event_sync_lifecycle_positions", ttl=86400) == positions
        for _ in range(2):
            await executor._event_lifecycle(
                setup["rule"].id, setup["config"], (), now, flow={},
                expires_at=None, advance=True,
            )
        assert next(iter(cache.get("event_sync_lifecycle_positions", ttl=86400).values()))["expires_at"] == (
            next(iter(positions.values()))["expires_at"]
        )
    examined = set()
    for call in epg_programmes.prepare_profiles.await_args_list:
        batch = call.args[1]
        assert len(batch) <= 256
        ids = {stream["id"] for channel in batch.values() for stream in channel["streams"]}
        assert len(ids) <= 1000
        examined.update(cid for cid in batch if cid > 0)
    assert examined == (set(range(1, count + 1)) - ({1} if bad else set()))
    setup["client"].create_channel.assert_not_awaited()
    setup["client"].update_channel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", [
    "fresh", "missing", "ended", "name", "url", "account", "group",
    "rule_disabled", "config", "profile", "ownership",
])
async def test_lifecycle_checks_current_evidence_after_wait(retirement, monkeypatch, case):
    from models import DummyEPGProfile
    from services import epg_programmes
    from stream_prober import StreamProber

    setup = retirement
    current = [setup["now"]]
    name = "DAZN 07: Fresh Match @ 11 Jul 11:30 PM ET"
    start = current[0] - timedelta(minutes=30)
    row = _resolved(name, DISPOSITION_UNMATCHED, _parsed("Fresh Match", start),
                    provider_id=2, stream_id=7301, group_id=SECONDARY_A)
    plan = build_promotion_plan(setup["config"], [row], {}, now=current[0])
    setup["rule"].set_managed_channel_ids([])
    setup["db"].commit()
    setup["streams"][:] = [{
        "id": 7301, "name": name, "url": "https://example.invalid/current",
        "m3u_account": 2, "channel_group_id": SECONDARY_A, "is_stale": False,
    }]
    setup["stats"][7301] = {
        "stream_name": name, "measured_bitrate": 5000000, "probe_status": "success",
        "last_probed": current[0].isoformat(), "is_black_screen": False,
        "black_screen_checked_at": current[0].isoformat(),
    }
    profile_id = setup["config"]["dummy_epg_profile_id"]

    async def prepare(profiles, channels, client, **kwargs):
        assert kwargs["expires_at"] is None
        assert kwargs["recover_sources"] is True
        assert kwargs["wait_for_sources"] is False
        current[0] += timedelta(minutes=6)
        stream = setup["streams"][0]
        if case in {"name", "url"}:
            stream[case] += " changed"
        elif case == "account":
            stream["m3u_account"] = 18
        elif case == "group":
            stream["channel_group_id"] += 1
        elif case == "rule_disabled":
            setup["rule"].enabled = False
        elif case == "config":
            setup["rule"].set_event_sync_config({**setup["config"], "max_promote_per_run": 1})
        elif case == "profile":
            setup["db"].get(DummyEPGProfile, profile_id).event_timezone = "UTC"
        elif case == "ownership":
            setup["rule"].set_managed_channel_ids([900])
        setup["db"].commit()
        witness = {
            "source_id": 50, "source_tvg_id": "event", "title": "Fresh Match",
            "start": start.isoformat(),
            "stop": (current[0] - timedelta(seconds=1) if case == "ended"
                     else current[0] + timedelta(hours=1)).isoformat(),
        }
        return [], {"sources": [{"source_id": 50, "status": "ready",
                                  "last_success": current[0].isoformat()}],
                    "channels": [{"channel_id": cid, "event": witness} for cid in channels]}

    prober = StreamProber.__new__(StreamProber)
    prober.max_concurrent_probes = 1
    prober.account_probe_limits = {2: 1}
    prober._probe_condition = asyncio.Condition()
    prober._account_active = {}
    prober._event_probes = 0
    prober.refresh_account_probe_limits = AsyncMock()

    async def probe(sid, url, stream_name, **kwargs):
        assert kwargs["expires_at"] is None
        if case != "missing":
            setup["stats"][sid].update(
                last_probed=current[0].isoformat(),
                black_screen_checked_at=current[0].isoformat(),
            )

    prober.probe_stream = AsyncMock(side_effect=probe)
    monkeypatch.setattr(epg_programmes, "prepare_profiles", AsyncMock(side_effect=prepare))
    monkeypatch.setattr("stream_prober.ensure_prober", lambda: prober)
    monkeypatch.setattr("stream_prober.get_prober", lambda: prober)
    with patch("channel_pipeline_executor.datetime", _clock(lambda: current[0])), \
         patch("services.event_sync_stream_health.datetime", _clock(lambda: current[0])):
        eligible, states = await setup["executor"]._event_lifecycle(
            setup["rule"].id, setup["config"], plan.units, setup["now"],
            flow={7301: True}, expires_at=None, advance=True,
        )
    assert eligible == ({plan.units[0].event_key} if case == "fresh" else set())
    assert states.get(-1) == ("active" if case == "fresh" else None) or (
        case != "fresh" and states.get(-1) == "unknown"
    )
    if case == "fresh":
        prober.probe_stream.assert_awaited_once()
    assert prober._account_active == {}
    setup["client"].create_channel.assert_not_awaited()
    setup["client"].update_channel.assert_not_awaited()


@pytest.mark.asyncio
async def test_owned_positive_does_not_skip_new_health(retirement):
    from services import event_sync_stream_health

    setup = retirement
    current = setup["now"] - timedelta(minutes=30)
    owned = _resolved(STREAM_FURY, DISPOSITION_UNMATCHED, _parsed("Fury vs. Usyk", current),
                      provider_id=2, stream_id=7301, group_id=SECONDARY_A)
    fresh = _resolved("DAZN 07: Fresh Match @ 11 Jul 11:30 PM ET", DISPOSITION_UNMATCHED,
                      _parsed("Fresh Match", current), provider_id=2, stream_id=7302,
                      group_id=SECONDARY_A)
    plan = build_promotion_plan(
        setup["config"], [owned, fresh], {promoted_channel_name(owned.result.parsed).lower(): 900},
        now=setup["now"],
    )
    collect = AsyncMock(side_effect=[{7302: None}, {7301: True}])
    with patch.object(event_sync_stream_health, "collect_stream_flow", collect):
        flow, _ = await setup["executor"]._event_health(
            setup["rule"].id, setup["config"], plan.units, [owned, fresh], setup["now"],
            probe_missing=True, expires_at=None,
        )
    assert collect.await_args_list[0].args[0] == {7302}
    assert collect.await_args_list[0].kwargs["probe_missing"] is True
    assert flow == {7302: None, 7301: True}


@pytest.mark.asyncio
async def test_complete_sample_stages_event_with_stalled_sibling(promotion_candidates):
    from channel_pipeline_executor import ExecutionContext
    from channel_pipeline_schema import validate_event_sync_config
    from models import DummyEPGProfile
    from services.epg_publication import read_publication
    from tests.event_sync_fixtures import dedicated_event_sync_config
    from types import SimpleNamespace

    setup = promotion_candidates
    config = dedicated_event_sync_config(
        dummy_epg_profile_id=setup["config"]["dummy_epg_profile_id"],
        secondary=[{"group_id": SECONDARY_A, "m3u_account_id": 2}],
    )
    profile = setup["db"].get(DummyEPGProfile, config["dummy_epg_profile_id"])
    profile.set_hide_empty_group_ids([PROMOTE_GROUP_ID])
    profile.set_epg_source_ids([])
    profile.set_event_sync_config({
        "secondary": config["secondary"], "assume_current_date": False,
        "use_default_patterns": False, "slot_patterns": [],
    })
    setup["db"].commit()
    assert validate_event_sync_config(config) == []
    setup["rule"].set_event_sync_config(config)
    setup["db"].commit()
    first = setup["rows"][0]
    setup["rows"][1] = _resolved(
        first.stream.name, DISPOSITION_UNMATCHED, first.result.parsed,
        provider_id=2, stream_id=7302, group_id=SECONDARY_A,
    )
    setup["streams"][1]["name"] = first.stream.name
    setup["prober"].max_concurrent_probes = 2
    setup["prober"].account_probe_limits = {2: 2}
    stalled = asyncio.Event()
    cancelled = asyncio.Event()
    current = [setup["now"]]

    async def probe(sid, url, name, *, content, expires_at):
        assert content is True and expires_at is None
        if sid == 7302:
            stalled.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        else:
            await stalled.wait()
            current[0] += timedelta(seconds=65)
            setup["stats"][sid] = {
                "stream_name": name, "probe_status": "success", "measured_bitrate": 5000000,
                "last_probed": current[0].isoformat(), "is_black_screen": False,
                "black_screen_checked_at": current[0].isoformat(),
            }

    setup["prober"].probe_stream = AsyncMock(side_effect=probe)
    setup["epg_sources"][0]["is_active"] = True
    executor = ActionExecutor(
        setup["client"], [], managed_channel_ids=[], epg_sources=setup["epg_sources"],
        existing_groups=[{"id": PROMOTE_GROUP_ID, "name": "Events"}],
    )
    with patch("channel_pipeline_executor.datetime", _clock(lambda: current[0])), \
         patch("services.event_sync_stream_health.datetime", _clock(lambda: current[0])), \
         patch("stream_prober.get_prober", return_value=setup["prober"]):
        result = await asyncio.wait_for(executor._execute_event_sync_promotion(
            setup["rule"].id, setup["rule"].name, config,
            SimpleNamespace(resolved=setup["rows"]), ExecutionContext(),
        ), 2)
    assert result["promoted_created"] == 1, result
    assert cancelled.is_set()
    assert setup["prober"]._account_active == {}
    channel = next(iter(setup["state"].channels.values()))
    assert channel["hidden_from_output"] is True
    assert channel["streams"] == []
    publication = read_publication(f"profile:{profile.id}")
    receipt = next(iter(publication["state"]["delivery"]["pending_channels"].values()))
    assert receipt["expires_at"] == receipt["stop"]
    assert [row["id"] for row in receipt["streams"]] == [7301]


@pytest.mark.asyncio
async def test_lifecycle_refreshes_each_event_after_wait(promotion_candidates, monkeypatch):
    from services import epg_programmes

    setup = promotion_candidates
    setup["config"]["max_promote_per_run"] = 2
    setup["rule"].set_event_sync_config(setup["config"])
    setup["db"].commit()
    current = [setup["now"]]
    plan = build_promotion_plan(setup["config"], setup["rows"], {}, now=current[0])
    for row in setup["rows"]:
        setup["stats"][row.stream.stream_id] = {
            "stream_name": row.stream.name, "probe_status": "success", "measured_bitrate": 5000000,
            "last_probed": current[0].isoformat(), "is_black_screen": False,
            "black_screen_checked_at": current[0].isoformat(),
        }

    async def prepare(*args, **kwargs):
        current[0] += timedelta(minutes=6)
        return [], {"sources": [], "channels": []}

    async def probe(sid, url, name, **kwargs):
        setup["stats"][sid].update(
            last_probed=current[0].isoformat(), black_screen_checked_at=current[0].isoformat(),
        )

    setup["prober"].probe_stream = AsyncMock(side_effect=probe)
    monkeypatch.setattr(epg_programmes, "prepare_profiles", AsyncMock(side_effect=prepare))
    monkeypatch.setattr("stream_prober.get_prober", lambda: setup["prober"])
    with patch("channel_pipeline_executor.datetime", _clock(lambda: current[0])), \
         patch("services.event_sync_stream_health.datetime", _clock(lambda: current[0])):
        eligible, states = await setup["executor"]._event_lifecycle(
            setup["rule"].id, setup["config"], plan.units, setup["now"],
            flow={7301: True, 7302: True}, expires_at=None, advance=True,
        )
    assert eligible == {unit.event_key for unit in plan.units}
    assert states == {-1: "active", -2: "active"}
    assert [call.args[0] for call in setup["prober"].probe_stream.await_args_list] == [7301, 7302]
    assert setup["prober"]._account_active == {}


@pytest.mark.asyncio
async def test_new_cached_event_does_not_starve_other_event(promotion_candidates, monkeypatch):
    from services import epg_programmes

    setup = promotion_candidates
    setup["config"]["max_promote_per_run"] = 2
    setup["rule"].set_event_sync_config(setup["config"])
    setup["db"].commit()
    plan = build_promotion_plan(setup["config"], setup["rows"], {}, now=setup["now"])
    assert len(plan.units) == 2
    assert all(unit.existing_channel_id is None for unit in plan.units)

    async def probe(sid, url, name, **kwargs):
        assert sid == 7302
        setup["stats"][sid] = {
            "stream_name": name, "probe_status": "success", "measured_bitrate": 5000000,
            "last_probed": setup["now"].isoformat(), "is_black_screen": False,
            "black_screen_checked_at": setup["now"].isoformat(),
        }

    async def prepare(profiles, channels, client, **kwargs):
        witness = {
            "source_id": 50, "source_tvg_id": "pending", "title": "Alpha Event",
            "start": (setup["now"] - timedelta(minutes=30)).isoformat(),
            "stop": (setup["now"] + timedelta(hours=1)).isoformat(),
        }
        return [], {
            "sources": [{"source_id": 50, "status": "error", "last_success": setup["now"].isoformat()}],
            "channels": [{"channel_id": cid, "event": witness if "Alpha" in row["name"] else None}
                         for cid, row in channels.items()],
        }

    setup["prober"].probe_stream = AsyncMock(side_effect=probe)
    monkeypatch.setattr("stream_prober.get_prober", lambda: setup["prober"])
    monkeypatch.setattr(epg_programmes, "prepare_profiles", AsyncMock(side_effect=prepare))
    for _ in range(2):
        setup["stats"].clear()
        setup["stats"][7301] = {
            "stream_name": setup["rows"][0].stream.name, "probe_status": "success",
            "measured_bitrate": 5000000, "last_probed": setup["now"].isoformat(),
            "is_black_screen": False, "black_screen_checked_at": setup["now"].isoformat(),
        }
        flow, dead = await setup["executor"]._event_health(
            setup["rule"].id, setup["config"], plan.units, setup["rows"], setup["now"],
            probe_missing=True, expires_at=None,
        )
        eligible, _ = await setup["executor"]._event_lifecycle(
            setup["rule"].id, setup["config"], plan.units, setup["now"], dead,
            flow=flow, expires_at=None, advance=True,
        )
        assert flow == {7301: True, 7302: True}
        assert eligible == {plan.units[1].event_key}
    assert [call.args[0] for call in setup["prober"].probe_stream.await_args_list] == [7302, 7302]
    assert setup["prober"]._account_active == {}


@pytest.mark.asyncio
async def test_each_new_event_stages_with_shared_capacity(promotion_candidates):
    from channel_pipeline_executor import ExecutionContext
    from types import SimpleNamespace

    setup = promotion_candidates
    setup["config"]["max_promote_per_run"] = 2
    setup["rule"].set_event_sync_config(setup["config"])
    setup["db"].commit()

    async def probe(sid, url, name, **kwargs):
        setup["stats"][sid] = {
            "stream_name": name, "probe_status": "success", "measured_bitrate": 5000000,
            "last_probed": setup["now"].isoformat(), "is_black_screen": False,
            "black_screen_checked_at": setup["now"].isoformat(),
        }

    setup["prober"].probe_stream = AsyncMock(side_effect=probe)
    executor = ActionExecutor(
        setup["client"], [], managed_channel_ids=[], epg_sources=setup["epg_sources"],
    )
    with patch("stream_prober.get_prober", return_value=setup["prober"]):
        result = await executor._execute_event_sync_promotion(
            setup["rule"].id, setup["rule"].name, setup["config"],
            SimpleNamespace(resolved=setup["rows"]), ExecutionContext(),
        )
    assert result["promoted_created"] == 2
    assert [call.args[0] for call in setup["prober"].probe_stream.await_args_list] == [7301, 7302]
    assert len(executor._event_pending) == 2
    assert all(channel["streams"] == [] and channel["hidden_from_output"] is True
               for channel in setup["state"].channels.values())
    assert setup["prober"]._account_active == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["age", "name", "url", "account", "group"])
async def test_lifecycle_rechecks_after_other_event_probe(promotion_candidates, monkeypatch, case):
    from services import epg_programmes

    setup = promotion_candidates
    setup["config"]["max_promote_per_run"] = 2
    setup["rule"].set_event_sync_config(setup["config"])
    setup["db"].commit()
    current = [setup["now"]]
    plan = build_promotion_plan(setup["config"], setup["rows"], {}, now=current[0])
    observed = current[0] - timedelta(minutes=4)
    setup["stats"][7301] = {
        "stream_name": setup["rows"][0].stream.name, "probe_status": "success",
        "measured_bitrate": 5000000, "last_probed": observed.isoformat(),
        "is_black_screen": False, "black_screen_checked_at": observed.isoformat(),
    }

    async def probe(sid, url, name, **kwargs):
        assert sid == 7302
        current[0] += timedelta(minutes=2) if case == "age" else timedelta(seconds=1)
        stream = setup["streams"][0]
        if case in {"name", "url"}:
            stream[case] += " changed"
        elif case == "account":
            stream["m3u_account"] = 18
        elif case == "group":
            stream["channel_group_id"] += 1
        setup["stats"][sid] = {
            "stream_name": name, "probe_status": "success", "measured_bitrate": 5000000,
            "last_probed": current[0].isoformat(), "is_black_screen": False,
            "black_screen_checked_at": current[0].isoformat(),
        }

    setup["prober"].probe_stream = AsyncMock(side_effect=probe)
    monkeypatch.setattr("stream_prober.get_prober", lambda: setup["prober"])
    monkeypatch.setattr(epg_programmes, "prepare_profiles", AsyncMock(return_value=(
        [], {"sources": [], "channels": []},
    )))
    with patch("channel_pipeline_executor.datetime", _clock(lambda: current[0])), \
         patch("services.event_sync_stream_health.datetime", _clock(lambda: current[0])):
        eligible, states = await setup["executor"]._event_lifecycle(
            setup["rule"].id, setup["config"], plan.units, setup["now"],
            flow={7301: True, 7302: True}, expires_at=None, advance=True,
        )
    assert eligible == {plan.units[1].event_key}
    assert states.get(-1, "unknown") == "unknown"
    assert states[-2] == "active"
    setup["prober"].probe_stream.assert_awaited_once()
    assert setup["prober"]._account_active == {}
