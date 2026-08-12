"""
Event Sync unmatched-stream promotion planner (bead ti939.4.1, epic ti939
"Event Sync" Phase 3).

Opt-in promotion of unmatched secondary-only events to ECM-managed channels
— the ONE sanctioned exception to Event Sync's "ECM never creates channels"
principle. This module owns the PURE half of the feature: which resolved
streams are promotable, how they cluster into promotion units, what the
deterministic channel name of a unit is, and how the per-run cap applies.
Execution (channel create/adopt + stream attach) lives in
``channel_pipeline_executor.ActionExecutor``; the preview endpoint calls
THIS SAME planner over the same resolver output, so the promotion PLAN the
preview shows and the plan a live run executes cannot diverge on identical
inputs (dry-run parity by construction — the same argument as
``services.event_sync_resolver``).

**PO-locked design decisions (implement-as-law):**

* **Lifecycle: reconciliation-driven deletion.** A promoted channel is
  current iff its justifying unmatched stream is observed present in the
  provider playlist THIS run; Pass 4 orphan reconciliation deletes (per the
  rule's ``orphan_action``, default delete) when it drops out. Stream
  presence is the ONLY lifecycle signal unless the rule opts into
  ``skip_past_events``; there are no K-run counters either way.
* **``skip_past_events`` also ENDS a promoted channel's life.** The opt-in
  past-event filter is the single place a clock enters this module, and it
  diverts a finished event's unit out of the plan whatever that unit's
  action is. A ``create`` simply never happens. An ``attach_existing``
  leaves its channel out of the run's managed set, so Pass 4 then applies
  the rule's own ``orphan_action`` to it — delete by default, kept
  untouched under ``none``. That IS a delete driven by a clock, and it is
  deliberate: a finished event's channel is unwatchable, and providers
  leave the stream in the playlist indefinitely, so stream presence alone
  would never retire it. Three things keep it safe — the filter is opt-in
  per rule, :func:`event_is_past` refuses to judge an event with no parsed
  start or a synthesized date, and ``past_event_grace_hours`` keeps a
  broadcast still on air from being dropped mid-event. The clock is read
  only when the rule opted in, and ``now`` is injectable.
* **Clustering: same cleaned title, starts within the time window.**
  Same-run unmatched streams (any provider) sharing a
  :func:`services.event_sync_review.master_event_key` form ONE promotion
  unit, and keys that carry the SAME cleaned title with starts no further
  apart than ``time_window_minutes`` are folded together on top of that.
  Two providers list the same event with different clock times (one reads
  the broadcast start, the other the undercard), and without the fold each
  spelling mints its own channel. Titles are still compared EXACTLY — no
  fuzzy clustering — so the fold only ever joins events the cleaner
  already calls the same name. Promoted channels do NOT enter the matcher
  candidate set (they live in the promotion target group, which the
  resolver never reads).
* **A promoted channel is never created for a dead stream.** With
  ``skip_dead_streams`` on, the caller checks the health of exactly the
  streams this plan is about to turn into channels — the survivors of
  every other filter, the cap included, which is a far smaller list than
  the parsed candidates — and passes the failures in as
  ``dead_stream_ids``. Those streams leave their unit's attach list,
  and a unit with nothing left is dropped. Health NEVER retires a channel:
  a unit dropped this way keeps whatever channel it already has in the
  run's managed set, so a provider having a bad hour cannot delete an
  operator's channel.
* **``promote_lead_hours`` gates CREATES ONLY, unless the rule turns on
  ``apply_lead_to_existing``.** An event further ahead than the lead
  window is simply not created yet. By default an event that already has
  a channel is NEVER un-promoted for being far away — that would delete
  and recreate the same channel every day, the deliberate opposite of
  ``skip_past_events`` above, which does divert adopt units. A rule that
  turns ``apply_lead_to_existing`` on takes that churn on purpose, to stop
  a provider holding channels open hours before its events start: the gate
  then diverts adopt units too, and a diverted adopt leaves its channel
  out of the managed set the same way a finished event's does.
* **A DATELESS event is never promoted.** A listing carrying a time and no
  date names a recurring slot rather than one broadcast, so the channel it
  would create would carry a different event every day with nothing to say
  which. Such units are diverted before every other filter, so they never
  spend cap budget, and like the health filter this retires nothing: the
  caller keeps whatever channel such a unit already has. Attaching a
  dateless stream to a MASTER channel is a separate question and is
  unaffected — that is what ``assume_current_date`` governs.

**Which dispositions are promotable (pinned semantics, AC-11):**

* ``unmatched`` — the core case: an event only secondary providers carry.
* ``excluded_by_operator`` — ALSO promotable. An operator exclusion is
  "never attach THIS stream to THAT master" (a pairing-level standing
  order); it says nothing about the stream deserving its own channel.
  Exclusions block ATTACH to a specific master, not promotion.
* ``ambiguous`` is NOT promotable — it is an open operator question that
  may still resolve into an attach; promoting it would race the review
  queue. ``parse_failed`` rows are untouchable (no identity to key on),
  and ``would_attach`` rows have a master.

Only rows with a COMPLETE parsed identity (``master_event_key`` returns a
key) are promotable — an identity-less stream can neither name a channel
deterministically nor be recognized as "the same event" next run.

**Naming: derived from the event key, nothing else.** The channel name is
a pure function of the clustering key (cleaned title + LOCAL clock time;
the date only when genuinely parsed). The name is the adoption identity —
next run's plan finds the existing channel by name in the target group —
so it must be byte-stable across runs, providers and (for dateless
parses, t6bin semantics inherited verbatim) across midnight. A SYNTHESIZED
date NEVER appears in the name or the identity.

**Pure module.** No DB, no Dispatcharr client, no engine imports — the
same isolation contract as ``services.event_sync_matcher``.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

from services.event_sync_matcher import (
    DEFAULT_TIME_WINDOW_MINUTES,
    SYNTHESIZED_DATE_PATTERN_NAMES,
    ParsedEvent,
)
from services.event_sync_resolver import (
    DISPOSITION_EXCLUDED,
    DISPOSITION_UNMATCHED,
)
from services.event_sync_review import master_event_key

__all__ = [
    "DEFAULT_MAX_PROMOTE_PER_RUN",
    "DEFAULT_PAST_EVENT_GRACE_HOURS",
    "MAX_PAST_EVENT_GRACE_HOURS",
    "MAX_PROMOTE_CEILING",
    "MAX_PROMOTE_LEAD_HOURS",
    "MIN_PROMOTE_LEAD_HOURS",
    "PROMOTABLE_DISPOSITIONS",
    "PROMOTE_ACTION_ATTACH_EXISTING",
    "PROMOTE_ACTION_CREATE",
    "PromotionPlan",
    "PromotionUnit",
    "build_promotion_plan",
    "event_has_started",
    "event_is_early",
    "event_is_past",
    "promoted_channel_name",
]

# Default per-run cap on NEW promoted channels (units whose action is
# "create"). Deliberately small — promotion CREATES channels, a strictly
# bigger blast radius than the attach path's merges, so the default cap is
# a fraction of DEFAULT_MAX_ATTACH_PER_RUN (100). Adoption of an existing
# promoted channel and stream attaches to it never consume cap budget
# (idempotent re-runs must not misreport their own prior work as deferred —
# the same rule the attach cap follows).
DEFAULT_MAX_PROMOTE_PER_RUN: int = 25

# Ceiling for event_sync_config.max_promote_per_run (validated in
# channel_pipeline_schema.validate_event_sync_config). The cap is a
# blast-radius control; a config cannot raise it past this.
MAX_PROMOTE_CEILING: int = 200

# How a promotion unit will be realized against the target group.
PROMOTE_ACTION_CREATE = "create"
PROMOTE_ACTION_ATTACH_EXISTING = "attach_existing"

# Pinned promotable dispositions — see the module docstring for the AC-11
# excluded_by_operator rationale.
PROMOTABLE_DISPOSITIONS = frozenset({
    DISPOSITION_UNMATCHED,
    DISPOSITION_EXCLUDED,
})

# Hours after an event's parsed start before skip_past_events treats it as
# finished. Providers leave finished events in the playlist indefinitely, so
# without a filter every one of them mints a channel that nobody can watch.
# The grace exists so a broadcast in progress is never dropped mid-event: a
# 4-hour default covers a long ball game or a full fight card, and the parsed
# start is the only time the name carries (no duration is ever available).
DEFAULT_PAST_EVENT_GRACE_HOURS: int = 4

# Ceiling for event_sync_config.past_event_grace_hours (validated in
# channel_pipeline_schema.validate_event_sync_config). Three days — past that
# the filter no longer filters anything a daily playlist contains.
MAX_PAST_EVENT_GRACE_HOURS: int = 72

# Bounds for event_sync_config.promote_lead_hours (validated in
# channel_pipeline_schema.validate_event_sync_config). There is deliberately
# NO default: an absent key means no lead limit at all, the same
# absent-means-off contract the other promotion keys follow. One hour is the
# tightest useful window (below it a channel appears too late to find), and
# thirty days is past the horizon any provider publishes.
MIN_PROMOTE_LEAD_HOURS: int = 1
MAX_PROMOTE_LEAD_HOURS: int = 720

# Ceiling on how far apart two same-title starts may be and still fold into
# one channel. Deliberately independent of event_sync_config
# .time_window_minutes, which stays legal all the way to 1440: attaching and
# clustering ask different questions, so one setting cannot answer both.
# Attaching asks "is this the master channel for this event", and an
# operator who widens that to a day is making a defensible call about a
# provider with a sloppy clock. Clustering asks "are these two listings the
# same broadcast", and that answer is never yes across a day. At 1440 the
# fold merged six pairs of plainly different events out of 195 real keys
# from one live rule: two days of the same tennis session, two games of a
# baseball series.
#
# 60 minutes, fixed by both edges of that same real data:
#
# * The largest genuine disagreement between two providers listing ONE
#   event is 30 minutes — TREX and IPTorrents carried a single race at
#   7:00, 7:15 and 7:30 PM. Doubling it also absorbs a provider whose clock
#   is a whole hour out, which is what a DST or timezone slip looks like and
#   is the coarsest error of that kind.
# * The closest pair of genuinely DIFFERENT events in the same data is 120
#   minutes apart: back-to-back BMX park sessions at 16:45 and 18:45. The
#   comparison below is inclusive, so a 120 ceiling would merge exactly
#   those two; 60 keeps the same factor of two on this side.
#
# Nothing recurring comes near either edge — the shortest recurring gap
# observed is a weekly show at 7 days. Over those 195 live keys, 30 and 60
# both merge nothing, 120 merges the BMX pair, 1440 merges all six.
MAX_CLUSTER_WINDOW_MINUTES: int = 60


def event_is_past(
    parsed: ParsedEvent, grace_hours: int, now: datetime
) -> bool:
    """Has this event's parsed start plus its grace window already gone by?

    ``False`` — meaning "do not treat this as finished" — for the two cases
    where the question cannot be answered from the name:

    * ``start is None``: no parsed time at all.
    * ``matched_pattern`` in :data:`SYNTHESIZED_DATE_PATTERN_NAMES`: the
      date component was fabricated from "now" (``assume_current_date``),
      not read off the provider string. Judging a fabricated date past or
      future says nothing about the event, and the verdict would flip every
      day at midnight — so a dateless parse is never filtered.

    ``now`` must be tz-aware (``parsed.start`` always is).
    """
    if parsed.start is None:
        return False
    if parsed.matched_pattern in SYNTHESIZED_DATE_PATTERN_NAMES:
        return False
    return parsed.start + timedelta(hours=grace_hours) < now


def event_is_early(
    parsed: ParsedEvent, lead_hours: int, now: datetime
) -> bool:
    """Is this event's parsed start still further ahead than the lead window?

    The mirror image of :func:`event_is_past`, and it refuses the same two
    cases for the same reason — an event with no parsed start, or one whose
    date was fabricated from "now" rather than read off the provider string,
    cannot be judged early any more than it can be judged finished.

    A ``True`` verdict stops a channel from being CREATED yet. By default
    it must not take an existing channel out of a run's managed set: the
    event would lose its channel today and get it back tomorrow, every day
    until the lead window opens. ``apply_lead_to_existing`` is the key an
    operator turns on to accept that churn, for a provider that hands out
    channels hours before its events start. This function does not read
    it — the gate in :func:`build_promotion_plan` does.

    ``now`` must be tz-aware (``parsed.start`` always is).
    """
    if parsed.start is None:
        return False
    if parsed.matched_pattern in SYNTHESIZED_DATE_PATTERN_NAMES:
        return False
    return parsed.start - timedelta(hours=lead_hours) > now


def event_has_started(parsed: ParsedEvent, now: datetime) -> bool:
    """Is this event's parsed start already behind us?

    The health gate asks this before it lets a probe verdict count against a
    stream: a stream for an event that has not begun may fail simply because
    there is nothing to stream yet, and rejecting it would stop the channel
    ever being created. A stream the provider has DELISTED is dead either
    way — that is Dispatcharr's own statement, not a probe's. [7]

    Refuses the same two cases as :func:`event_is_past` and
    :func:`event_is_early`, and for the same reason: an event with no parsed
    start, or one whose date was fabricated from "now" rather than read off
    the provider string, cannot be judged started any more than it can be
    judged finished. Both answer ``False``, which is the safe direction —
    no probe verdict counts.

    ``now`` must be tz-aware (``parsed.start`` always is).
    """
    if parsed.start is None:
        return False
    if parsed.matched_pattern in SYNTHESIZED_DATE_PATTERN_NAMES:
        return False
    return parsed.start <= now


def promoted_channel_name(parsed: ParsedEvent) -> str | None:
    """Deterministic promoted-channel name for one complete parsed identity.

    A pure function of the same components :func:`master_event_key` keys on
    — the LOCALS-cleaned title and the LOCAL clock time — so two streams
    that cluster together derive the SAME name regardless of provider
    spelling, and the name a re-run derives always finds the channel the
    previous run created (name-based adoption in the target group).

    * Dated parse: ``<Title-Cased Cleaned Title> @ <Mon DD HH:MM AM/PM>``
      (local clock of the parse timezone — the time the provider name
      literally carried).
    * Dateless parse (``matched_pattern`` in
      :data:`SYNTHESIZED_DATE_PATTERN_NAMES`): the date component of
      ``start`` was fabricated from "now" (t6bin), so the name carries
      ``<Title-Cased Cleaned Title> @ <HH:MM AM/PM>`` — NO date, ever. A
      recurring dateless slot derives the same name every day, so the
      re-run after midnight adopts the same channel instead of minting a
      dated duplicate.

    Returns ``None`` when the parse is incomplete (no title or no start) —
    mirroring ``master_event_key``'s contract: such a stream is not
    promotable.
    """
    if parsed.title is None or parsed.start is None:
        return None
    # Reuse the key's own cleaning by deriving the display title from the
    # SAME cleaned form the key carries (single identity source). The key
    # format is "<cleaned>|<...>"; recompute the cleaned title through
    # master_event_key's exact code path by splitting the key.
    key = master_event_key(parsed)
    if key is None:  # pragma: no cover — guarded by the title/start check
        return None
    cleaned_title = key.split("|", 1)[0]
    display_title = cleaned_title.title()
    if parsed.matched_pattern in SYNTHESIZED_DATE_PATTERN_NAMES:
        clock = parsed.start.strftime("%I:%M %p")
        return f"{display_title} @ {clock}"
    stamp = parsed.start.strftime("%b %d %I:%M %p")
    return f"{display_title} @ {stamp}"


@dataclass(frozen=True)
class PromotionUnit:
    """One promotion unit: every same-run promotable stream sharing one
    exact event key (any provider), realized as ONE channel.

    ``rows`` are the resolver's ResolvedStream objects (kept whole so the
    executor can thread provenance without re-deriving anything), in
    deterministic (provider_id, stream_id) order within each folded key and
    with the surviving key's own rows first. ``rows[0]`` is therefore the
    row this unit's identity was read from, and its parsed start is the
    unit's start, so a caller asking whether the event has begun may read it
    there. That holds for the plan as this function builds it. A caller that
    replans with ``dead_stream_ids`` gets units whose dead rows have been
    dropped, and the first row is one of them when the stream it names is
    dead, so the start must be read off the plan the caller made BEFORE its
    health check, which is also the plan that decides which events have
    started. [72] ``action`` is the planned
    realization against the CALLER-SUPPLIED existing-name map:
    ``create`` (no channel with the derived name exists in the target
    group) or ``attach_existing`` (adopt + attach only). ``existing_channel_id``
    carries the adopted channel's id when known (display/preview use — the
    executor re-resolves through its own live lookup).
    """

    event_key: str
    channel_name: str
    dateless: bool
    rows: tuple
    action: str
    existing_channel_id: int | None = None


@dataclass(frozen=True)
class PromotionPlan:
    """Full promotion plan for one rule's resolution.

    ``units`` are the units this run will realize; ``capped_units`` are
    create-units beyond the per-run cap — NOT realized this run (runs are
    idempotent: the remainder re-surfaces next run, mirroring the attach
    cap's posture). ``cap`` echoes the effective cap.
    ``skipped_past_units`` are the units ``skip_past_events`` dropped
    because the event already finished, of EITHER action; unlike capped
    units they are not deferred — they will not come back unless the
    provider re-dates them. The ``attach_existing`` ones
    (:attr:`skipped_past_adopted`) each leave a real channel out of the
    run's managed set, which hands it to Pass 4's ``orphan_action``, so
    that count is the destructive half and is surfaced on its own.

    ``skipped_early_units`` are units ``promote_lead_hours`` held back
    because the event is still further ahead than the lead window. They
    come back on their own once the window opens. Create-units only,
    unless the rule turned ``apply_lead_to_existing`` on, in which case a
    unit that already has a channel can be in here too and that channel is
    out of the run's managed set while it waits.

    ``skipped_dateless_units`` are units whose date was fabricated from
    "now" rather than read off the provider string. A name carrying a clock
    time and no date says nothing about WHICH day's event it is, so it is
    never turned into a channel. Dropped whatever their action and before
    every other filter, so a dateless unit never spends cap budget either.
    Like the all-dead bucket this is not a retirement signal: the caller
    keeps any channel such a unit already has in the managed set, because
    a stream the operator promoted yesterday must not disappear on the
    strength of a name the parser cannot date. [30]

    ``all_dead_units`` are units every one of whose streams failed the
    health check, and ``dead_streams_skipped`` counts the individual
    streams that failed. Both are counted among the units that survived
    every earlier filter INCLUDING the cap, because that is the only set
    the health check ever looks at, so neither is a whole-playlist figure.
    Neither is a deletion signal on its own: the caller keeps an all-dead
    unit's existing channel in the managed set, because a stream failing
    right now says nothing about whether the operator still wants the
    channel. The caller reads one exception off the rows itself — a unit
    whose every stream carries the provider's own delisting flag has no
    event left behind it, and the executor retires that channel. [19]
    """

    units: tuple[PromotionUnit, ...]
    capped_units: tuple[PromotionUnit, ...]
    cap: int
    target_group_id: int | None
    skipped_past_units: tuple[PromotionUnit, ...] = ()
    skipped_early_units: tuple[PromotionUnit, ...] = ()
    skipped_dateless_units: tuple[PromotionUnit, ...] = ()
    all_dead_units: tuple[PromotionUnit, ...] = ()
    dead_streams_skipped: int = 0

    @property
    def capped(self) -> bool:
        return bool(self.capped_units)

    @property
    def cap_overage(self) -> int:
        return len(self.capped_units)

    @property
    def skipped_past(self) -> int:
        return len(self.skipped_past_units)

    @property
    def skipped_past_adopted(self) -> int:
        """How many skipped units already have a channel in the group.

        Every one of them drops out of the run's managed set, so Pass 4
        acts on it with the rule's ``orphan_action``. Operator-visible
        before the run, because this is the destructive number.

        Counted by channel id rather than by action: a unit reads as
        ``attach_existing`` when an EARLIER unit in the same run planned
        the same channel name, and that unit has no channel anywhere.
        Counting it would warn the operator about losing a channel that
        nothing is about to lose. [45]
        """
        return sum(
            1 for u in self.skipped_past_units
            if u.existing_channel_id is not None
        )

    @property
    def skipped_early(self) -> int:
        return len(self.skipped_early_units)

    @property
    def skipped_dateless(self) -> int:
        return len(self.skipped_dateless_units)

    @property
    def skipped_all_dead(self) -> int:
        return len(self.all_dead_units)

    @property
    def would_create(self) -> int:
        return sum(1 for u in self.units if u.action == PROMOTE_ACTION_CREATE)

    @property
    def would_attach_existing(self) -> int:
        return sum(
            1 for u in self.units
            if u.action == PROMOTE_ACTION_ATTACH_EXISTING
        )

    @property
    def stream_count(self) -> int:
        return sum(len(u.rows) for u in self.units)


def _row_sort_key(row) -> tuple:
    """Deterministic within-unit stream order: (provider_id, stream_id)."""
    return (
        row.stream.provider_id if row.stream.provider_id is not None else -1,
        row.stream.stream_id if row.stream.stream_id is not None else -1,
        row.stream.name,
    )


def _is_dateless(row) -> bool:
    """Was this row's date fabricated from "now" rather than parsed?"""
    return (
        row.result.parsed.matched_pattern in SYNTHESIZED_DATE_PATTERN_NAMES
    )


def _fold_nearby_starts(
    by_key: dict[str, list],
    window_minutes: int,
    existing_name_to_id: dict[str, int],
) -> dict[str, list]:
    """Fold same-title event keys whose starts are close into one key.

    Two providers carrying the same event rarely agree on its start to the
    minute: one publishes the broadcast time, the next the undercard, and a
    fifteen-minute disagreement used to mint two channels for one event.
    Keys are folded when BOTH hold:

    * the cleaned titles are byte-equal (the fold never guesses that two
      spellings mean the same event, it only forgives the clock); and
    * the starts are no further apart than ``window_minutes``, or than
      :data:`MAX_CLUSTER_WINDOW_MINUTES` when the rule asks for more. The
      caller passes its own ``time_window_minutes`` straight through and
      this is the only place that ceiling applies, so a rule keeps the full
      window it was given for attaching and no setting can make the fold
      reach across a day. See the constant for the numbers behind it.

    A dateless key is never folded. Its date came from "now", so its start
    carries no information about which event it is, and folding on a clock
    alone would join two unrelated recurring slots.

    Distance is measured from the EARLIEST start in a cluster, not from the
    previous key, so a long chain of events each just inside the window
    cannot walk a cluster across hours.

    One key of each cluster survives and names the channel. It is the
    earliest start, EXCEPT that a key whose derived name already has a
    channel in the target group wins over one that does not: a provider
    dropping its earlier listing would otherwise rename the channel, which
    reads downstream as one channel retired and another created. Ties go to
    byte order on the key, so preview and run choose the same survivor for
    identical inputs.

    Returns a new ``key -> rows`` map with each list sorted by
    :func:`_row_sort_key` and the surviving key's own rows first, so the
    caller can read the unit's identity off element zero. A key with
    nothing to fold into keeps exactly the rows it had.
    """
    window = timedelta(
        minutes=min(max(0, window_minutes), MAX_CLUSTER_WINDOW_MINUTES)
    )
    by_title: dict[str, list[str]] = {}
    folded: dict[str, list] = {}
    sorted_rows = {
        key: sorted(rows, key=_row_sort_key)
        for key, rows in by_key.items()
    }
    for key, rows in sorted_rows.items():
        if _is_dateless(rows[0]):
            folded[key] = list(rows)
            continue
        # Everything before the LAST separator is the cleaned title; the
        # tail is the UTC start master_event_key appends.
        by_title.setdefault(key.rsplit("|", 1)[0], []).append(key)

    def _start(key: str) -> datetime:
        return sorted_rows[key][0].result.parsed.start

    def _already_has_a_channel(key: str) -> bool:
        name = promoted_channel_name(sorted_rows[key][0].result.parsed)
        return name is not None and name.lower() in existing_name_to_id

    def _keep(cluster: list[str]) -> None:
        survivor = min(
            cluster,
            key=lambda k: (not _already_has_a_channel(k), _start(k), k),
        )
        rows = list(sorted_rows[survivor])
        for key in cluster:
            if key != survivor:
                rows.extend(sorted_rows[key])
        folded[survivor] = rows

    for title_keys in by_title.values():
        cluster: list[str] = []
        anchor_start: datetime | None = None
        for key in sorted(title_keys, key=lambda k: (_start(k), k)):
            if anchor_start is None or _start(key) - anchor_start > window:
                if cluster:
                    _keep(cluster)
                cluster = [key]
                anchor_start = _start(key)
            else:
                cluster.append(key)
        if cluster:
            _keep(cluster)
    return folded


def build_promotion_plan(
    config: dict,
    resolved,
    existing_name_to_id: dict[str, int],
    *,
    now: datetime | None = None,
    dead_stream_ids: set[int] | None = None,
) -> PromotionPlan:
    """Build the promotion plan for one rule's resolved streams.

    Args:
        config: A VALIDATED event_sync_config with ``promote_unmatched``
            true (the caller gates on the flag; this function trusts it).
        resolved: Iterable of ``services.event_sync_resolver.ResolvedStream``
            — the resolver output shared by preview and run.
        existing_name_to_id: LOWERCASED channel name -> channel id for the
            channels CURRENTLY in the promotion target group. Drives the
            create-vs-adopt decision. The caller supplies it from the same
            channel data its execution path resolves against, so plan and
            execution agree, and the caller owns the keying: a channel
            stored with the ``"<number> <sep> "`` prefix
            ``include_channel_number_in_name`` writes has to appear under
            its unprefixed spelling too, because a missed match plans a
            create for a channel that already exists.
            ``channel_number_prefix.channel_name_to_id`` builds exactly
            that map, and only the caller can see whether the settings
            write a prefix at all.
        now: tz-aware anchor for ``skip_past_events`` and
            ``promote_lead_hours``. Defaults to the current UTC time, and
            is read ONLY when the rule turned one of those on — a config
            with neither never touches a clock. Injectable so tests are
            deterministic.
        dead_stream_ids: Stream ids the caller's health check just failed,
            or ``None`` when the rule did not ask for one. Each such stream
            leaves its unit's attach list; a unit left with none is dropped
            into ``all_dead_units``. The check itself lives with the
            caller because it reads the database and talks to the provider,
            and this module stays pure.

    A DATELESS unit is dropped first of all, whatever its action: its date
    was fabricated from "now", so the name identifies a recurring slot
    rather than one broadcast, and a channel named after it would carry a
    different event every day. The caller keeps any channel such a unit
    already has, so this filter creates nothing and retires nothing. [30]

    ``skip_past_events`` is applied BEFORE the cap so finished events cannot
    spend create budget that a live event needs, and to units of EITHER
    action. Dropping an adopt unit is how a finished event's channel leaves
    the run's managed set and reaches Pass 4's ``orphan_action`` — see the
    module docstring for why that clock-driven delete is deliberate and
    what guards it.

    ``promote_lead_hours`` is applied next, to CREATE units only unless
    ``apply_lead_to_existing`` is on, so by default an event that already
    has a channel keeps it however far away it is. It runs before the cap
    for the same reason the past filter does: an event a fortnight out
    must not spend the budget tonight's event needs.

    **The health filter runs dead last, after the cap**, so the caller only
    ever has to check the handful of streams this run will really turn into
    channels. Probing is by far the most expensive thing on this path, and
    every cheap filter above throws work away before it: on a live rule the
    finished-event filter alone took the candidate list from around 1,100
    streams to around 59. Two things follow, and both are deliberate:

    * ``dead_streams_skipped`` and ``skipped_all_dead`` count only among
      the streams that survived every earlier filter. They are NOT
      whole-playlist health figures and must not be read as any.
    * a unit that turns out to have no working stream **has already spent
      its cap slot**. The alternative, capping after the health check,
      would hand the freed slot to a unit nobody probed, so the run would
      create a channel for a stream it never checked — which is the thing
      the health check exists to prevent. A wasted slot costs one deferred
      event on an idempotent run; an unchecked create costs a dead channel.

    Determinism: units are ordered by event key (byte order), so cap
    trimming selects the same units on preview and run for identical
    inputs. Two distinct keys that derive the SAME channel name (rare —
    e.g. equal cleaned titles + clock across different tz offsets)
    deliberately collapse onto one channel: the first (by key order)
    creates it, later ones adopt — name identity IS the adoption identity,
    so the plan mirrors what execution would do anyway.
    """
    cap = config.get("max_promote_per_run", DEFAULT_MAX_PROMOTE_PER_RUN)
    target_group_id = config.get("promote_target_group_id")
    skip_past = bool(config.get("skip_past_events"))
    grace_hours = config.get(
        "past_event_grace_hours", DEFAULT_PAST_EVENT_GRACE_HOURS
    )
    lead_hours = config.get("promote_lead_hours")
    # Absent means false, like every other promotion key: a stored rule
    # must not start diverting channels it kept yesterday.
    apply_lead_to_existing = bool(config.get("apply_lead_to_existing"))
    if (skip_past or lead_hours is not None) and now is None:
        now = datetime.now(timezone.utc)
    # Clustering forgives a clock disagreement of up to the rule's own
    # matching window, and no further than MAX_CLUSTER_WINDOW_MINUTES —
    # the fold applies that ceiling itself, so what is read here is the
    # operator's setting unaltered and the attach path keeps all of it.
    # enforce_time_window is deliberately NOT consulted: it governs whether
    # a time mismatch may block an ATTACH to a master, and a rule that
    # switched it off still must not end up with every week of a weekly
    # show on one channel.
    window_minutes = config.get(
        "time_window_minutes", DEFAULT_TIME_WINDOW_MINUTES
    )

    by_key: dict[str, list] = {}
    for row in resolved:
        if row.disposition not in PROMOTABLE_DISPOSITIONS:
            continue
        key = master_event_key(row.result.parsed)
        if key is None:
            # Incomplete parsed identity — not promotable (and parse_failed
            # rows never reach here anyway: their disposition is excluded
            # above).
            continue
        by_key.setdefault(key, []).append(row)
    by_key = _fold_nearby_starts(
        by_key, window_minutes, existing_name_to_id
    )

    units: list[PromotionUnit] = []
    capped_units: list[PromotionUnit] = []
    skipped_past_units: list[PromotionUnit] = []
    skipped_early_units: list[PromotionUnit] = []
    skipped_dateless_units: list[PromotionUnit] = []
    all_dead_units: list[PromotionUnit] = []
    dead_streams_skipped = 0
    creates = 0
    planned_names: dict[str, str] = {}  # lowercased name -> first event key
    for key in sorted(by_key):
        clustered = by_key[key]
        # The identity comes from element zero, which is the surviving
        # key's own lowest-sorting row. After a fold the rest of the list
        # carries a different start, so reading the identity off whatever
        # sorts first would name the channel after a listing the key does
        # not belong to. The fold already returns each list in that order,
        # so the rows are kept exactly as it left them: re-sorting here
        # would push a folded-in listing whose start is up to
        # MAX_CLUSTER_WINDOW_MINUTES away into element zero, and every
        # caller that reads a unit's start off ``rows[0]`` would then be
        # reading a different instant than this ``parsed``. [52]
        parsed = clustered[0].result.parsed
        name = promoted_channel_name(parsed)
        if name is None:  # pragma: no cover — key is None first
            continue
        name_lower = name.lower()
        existing_id = existing_name_to_id.get(name_lower)
        if existing_id is not None or name_lower in planned_names:
            action = PROMOTE_ACTION_ATTACH_EXISTING
        else:
            action = PROMOTE_ACTION_CREATE
        unit = PromotionUnit(
            event_key=key,
            channel_name=name,
            dateless=parsed.matched_pattern in SYNTHESIZED_DATE_PATTERN_NAMES,
            rows=tuple(clustered),
            action=action,
            existing_channel_id=existing_id,
        )
        if unit.dateless:
            # Runs before every other filter: an event nobody can date is
            # not a candidate for a channel at all, so it should not spend
            # cap budget on its way to being dropped. [30]
            skipped_dateless_units.append(unit)
            continue
        if (
            skip_past
            and now is not None
            and event_is_past(parsed, grace_hours, now)
        ):
            # Either action. A create never happens; an adopt leaves its
            # channel out of the managed set the caller registers, which
            # is what hands it to Pass 4's orphan_action. [20]
            skipped_past_units.append(unit)
            continue
        if (
            lead_hours is not None
            and now is not None
            and (apply_lead_to_existing
                 or action == PROMOTE_ACTION_CREATE)
            and event_is_early(parsed, lead_hours, now)
        ):
            # CREATE units only, until a rule opts in. An event that
            # already has a channel is not taken back off the operator for
            # being far away, because it would lose the channel today and
            # get it back tomorrow. A provider that lists an event hours
            # ahead and serves an offline card until it starts is the case
            # where an operator wants that trade anyway. [16]
            skipped_early_units.append(unit)
            continue
        if action == PROMOTE_ACTION_CREATE:
            if cap and creates >= cap:
                capped_units.append(unit)
                continue
            creates += 1
        if dead_stream_ids:
            live_rows = [
                r for r in unit.rows
                if r.stream.stream_id not in dead_stream_ids
            ]
            dropped = len(unit.rows) - len(live_rows)
            if dropped:
                dead_streams_skipped += dropped
                if not live_rows:
                    # No working stream behind this event. The unit is not
                    # realized; what happens to any channel it already has
                    # is the CALLER's call, and it keeps the channel unless
                    # the provider has delisted every one of these rows.
                    all_dead_units.append(unit)
                    continue
                unit = replace(unit, rows=tuple(live_rows))
        # Claimed by the unit that is actually realized, so a name whose
        # unit lost every stream is not treated as already planned.
        if action == PROMOTE_ACTION_CREATE:
            planned_names[name_lower] = key
        units.append(unit)

    return PromotionPlan(
        units=tuple(units),
        capped_units=tuple(capped_units),
        cap=cap,
        target_group_id=target_group_id,
        skipped_past_units=tuple(skipped_past_units),
        skipped_early_units=tuple(skipped_early_units),
        skipped_dateless_units=tuple(skipped_dateless_units),
        all_dead_units=tuple(all_dead_units),
        dead_streams_skipped=dead_streams_skipped,
    )
