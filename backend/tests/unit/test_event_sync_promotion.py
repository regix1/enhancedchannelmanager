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
import json
import logging
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
import pytz
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import database
from channel_pipeline_engine import ChannelPipelineEngine
from channel_pipeline_executor import ActionExecutor, ActionResult
from models import ChannelPipelineRule, PendingMerge
from services.event_sync_matcher import ParsedEvent, StreamMatchResult
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
    on its survivors; a unit with no survivor is not realized. Neither
    outcome deletes anything — the unit that loses every stream still
    carries the id of the channel it already has, which is what the
    executor puts back into the run's managed set.
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
        stats = {self.REPLACEMENT_ID: {
            "stream_id": self.REPLACEMENT_ID,
            "probe_status": replacement_status,
            "consecutive_failures": 2 if replacement_status == "failed" else 0,
            "last_probed": (probed_at or now).astimezone(
                pytz.utc).replace(tzinfo=None).isoformat() + "Z",
        }}

        def _stats_for(stream_ids):
            return {sid: stats[sid] for sid in stream_ids if sid in stats}

        with patch("channel_pipeline_executor.datetime") as promote_clock, \
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
            client, db_session_factory, FROZEN_NOW,
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
            client, db_session_factory, FROZEN_NOW,
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
        assert state.stream_ids_of(850) == [
            self.STALE_ID, self.REPLACEMENT_ID,
        ]
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
        assert promo["skipped_all_dead"] == 0
        assert state.stream_ids_of(850) == [
            self.STALE_ID, self.REPLACEMENT_ID,
        ]

    def test_an_event_whose_only_stream_is_delisted_creates_nothing(
        self, db_session_factory
    ):
        """No replacement is listed at all, so the event has nothing behind
        it. No channel is created, and the one it already has is kept:
        health blocks creates and attaches, it never retires."""
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
            client, db_session_factory, FROZEN_NOW, dry_run=True,
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
            client, db_session_factory, FROZEN_NOW,
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
            client, db_session_factory, FROZEN_NOW,
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
        stats = {self.REPLACEMENT_ID: {
            "stream_id": self.REPLACEMENT_ID,
            "probe_status": "success",
            "consecutive_failures": 0,
            "last_probed": FROZEN_NOW.astimezone(pytz.utc).replace(
                tzinfo=None).isoformat() + "Z",
        }}

        def _stats_for(stream_ids):
            return {sid: stats[sid] for sid in stream_ids if sid in stats}

        with patch("routers.channel_pipeline.get_client",
                   return_value=client), \
             patch("routers.channel_pipeline.datetime") as preview_clock, \
             patch("stream_prober.StreamProber.get_stats_by_stream_ids",
                   _stats_for):
            preview_clock.now.return_value = FROZEN_NOW
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

    def test_finished_event_channel_survives_when_orphan_cleanup_is_off(
        self, db_session_factory
    ):
        """The operator's opt-out still wins: orphan_action 'none' skips
        reconciliation for the rule, so the filter costs the channel
        nothing."""
        rule_id = self._rule_that_skips_finished(db_session_factory, "none")
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

    def test_promoted_channel_gets_profile_and_foreign_epg_kept(self):
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

    def test_promotionless_config_keeps_master_only_filter(self):
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
