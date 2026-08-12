/**
 * TypeScript types for Event Sync (epic ti939) — Phase 1A preview-only.
 *
 * Mirrors the backend contract:
 * - config shape: `validate_event_sync_config` in
 *   `backend/channel_pipeline_schema.py` (see docs/event_sync.md)
 * - preview response: `POST /api/channel-pipeline/event-sync-preview` in
 *   `backend/routers/channel_pipeline.py` (see docs/api.md)
 */

// =============================================================================
// Rule configuration (event_sync_config)
// =============================================================================

/**
 * One parse-pattern variant (title/time/date regexes with named capture
 * groups). Same shape as `DEFAULT_EVENT_PATTERNS` in
 * `backend/services/event_sync_matcher.py`.
 */
export interface EventSyncPattern {
  name?: string;
  title_pattern: string;
  time_pattern?: string;
  date_pattern?: string;
}

/**
 * The event_sync rule kind's config. A rule carrying this JSON object IS an
 * event_sync rule; its conditions/actions are placeholders ignored by the
 * engine (docs/event_sync.md).
 */
/** One provider-scoped group: a group id + optional M3U account (provider) id
 *  (null/absent = the whole group / any provider). bead 3p2af. */
export interface EventSyncGroupScope {
  group_id: number;
  m3u_account_id: number | null;
}

export interface EventSyncConfig {
  /**
   * bead 3p2af / 38dzi: canonical provider-scoped shape. The editor (P4)
   * reads and writes these nested scopes; the backend validator derives the
   * flat `master_group_id` / `secondary_group_ids` from them, so a stored
   * config carries both. The flat keys are OPTIONAL on the client: the editor
   * no longer emits them (the backend fills them), but they remain readable on
   * a fetched config and are the migration fallback for a legacy rule authored
   * before the nested shape existed.
   */
  master?: EventSyncGroupScope;
  secondary?: EventSyncGroupScope[];
  master_group_id?: number;
  secondary_group_ids?: number[];
  /** Shared pattern variants; omit to use the matcher's built-in defaults. */
  patterns?: EventSyncPattern[];
  /** Per-group overrides keyed by group ID (JSON object keys are strings). */
  group_patterns?: Record<string, EventSyncPattern[]>;
  /** 1..1440; backend default 30. */
  time_window_minutes?: number;
  /**
   * Enforce the time-window candidacy gate (bead krkm4). Backend default
   * true — parsed start times must be within ±time_window_minutes to become
   * candidate pairs. Set false to disable the gate entirely: streams match
   * on title/team score alone, ignoring time. Safe only for a
   * single-provider, same-day master group — recurring/serial titles risk
   * cross-day matches. The 0.90 no-teams floor and team/numeric rails still
   * route borderline pairs to the review queue.
   */
  enforce_time_window?: boolean;
  /** Hard-clamped >= 0.80 by the backend schema; backend default 0.80. */
  attach_threshold?: number;
  /**
   * Per-run attach cap (1..1000; backend default 100) — Phase 1B blast-radius
   * control (bead ti939.2.1). No editor UI yet; preserved on save so an
   * API-set value survives a UI edit.
   */
  max_attach_per_run?: number;
  enabled?: boolean;
  /**
   * Phase 2 opt-in (bead ti939.3.1): when true, the rule also runs
   * UNATTENDED from the refresh-watermark task. Backend default false —
   * absent means manual-run-only.
   */
  auto_run?: boolean;
  /**
   * bead y8yby: when true, a MANUAL run of this rule (live Run AND dry-run
   * Test) first refreshes the M3U provider accounts backing the rule's
   * master + secondary groups, then runs the match/attach — closing the
   * refresh-ordering staleness window. Never applies to unattended auto-runs
   * (that path already follows a refresh). Backend default false.
   *
   * Consequence surfaced in the UI: with this ON the dry-run Test triggers a
   * real Dispatcharr provider refresh, so Test is no longer zero-write.
   */
  refresh_providers_before_run?: boolean;
  /**
   * Phase 2 (bead ti939.3.3): dummy EPG profile auto-assigned to master
   * event channels on every run. OPTIONAL — absent means off (the backend
   * never default-fills it). Must reference an existing dummy EPG profile.
   */
  dummy_epg_profile_id?: number;
  /**
   * bead 6xxmp: when true, the MASTER group's own streams are also matched
   * to the master channels (the resolver drops those already attached). The
   * sanctioned path for a single channel group (global, unique-by-name)
   * carrying two providers' streams — one auto-synced, one not — so the
   * unsynced provider's streams attach to the synced provider's channels.
   * Backend default false.
   */
  include_master_group_streams?: boolean;
  /**
   * bead assume-current-date: when true, a listing that carries a time but
   * NO date (a dateless "today's schedule") is placed on the CURRENT date so
   * it becomes matchable — deliberately relaxing the never-guess-the-date
   * rail, accepting the cross-day match risk. Backend default false.
   */
  assume_current_date?: boolean;
  /**
   * bead jqwfq: stale-dateless demote rail. When true (the BACKEND DEFAULT
   * — absent reads as true, unlike the other flags here) an
   * assume_current_date match whose dateless stream name already appeared
   * in the provider's previous-day M3U snapshot is routed to the review
   * queue (`ambiguous_reason: "stale_dateless_stream_name"`) instead of
   * auto-attached — guarding against providers that leave yesterday's slot
   * names in the playlist. Set false only for recurring daily events whose
   * names legitimately repeat. Inert unless assume_current_date is on.
   */
  demote_stale_dateless?: boolean;
  /**
   * bead parse-from-stream: when true, each master channel's event identity
   * (title + time) is read from its FIRST attached stream's name instead of
   * the channel name — so master channels can be named freely. Backend
   * default false.
   */
  parse_master_from_stream?: boolean;
  /**
   * bead ti939.4.1: opt-in promotion of unmatched secondary-only events to
   * ECM-managed channels — the ONE sanctioned exception to "ECM never
   * creates channels". Backend default false; ABSENT means the feature is
   * invisible (no preview/summary keys, Pass 4 stays hard-bypassed). When
   * true, ECM CREATES channels in `promote_target_group_id` and DELETES
   * them via orphan reconciliation when the justifying stream leaves the
   * provider playlist.
   */
  promote_unmatched?: boolean;
  /**
   * bead ti939.4.1: the dedicated ECM-owned channel group promoted event
   * channels live in. REQUIRED when `promote_unmatched` is true; the
   * backend refuses the master group and secondary groups (ownership
   * rails). ECM creates AND deletes channels in this group.
   */
  promote_target_group_id?: number;
  /**
   * bead ti939.4.1: per-run cap on NEW promoted channels (1..200; backend
   * default 25, filled on promotion-enabled configs). Adoption of existing
   * promoted channels never consumes the cap. No editor UI; preserved on
   * save so an API-set value survives a UI edit.
   */
  max_promote_per_run?: number;
  /**
   * When true, an event whose parsed start time has already gone by (plus
   * `past_event_grace_hours`) is not promoted, and a channel already
   * promoted for it leaves the set this rule manages, so the rule's orphan
   * cleanup applies to that channel. Providers leave finished events in the
   * playlist for days. Events whose date was synthesized
   * (`assume_current_date`) are never filtered. Absent means the filter is
   * off.
   */
  skip_past_events?: boolean;
  /**
   * How long after its start time an event still counts as current for
   * `skip_past_events` (0..72; backend default 4, filled only when the
   * filter is on). Provider names carry a start time and never a duration,
   * so this is what keeps a broadcast in progress from being dropped.
   */
  past_event_grace_hours?: number;
  /**
   * When true, the run checks that the streams it is about to turn into
   * channels can actually play, and leaves out the ones that fail. An event
   * whose streams all fail gets no channel. A channel that already exists is
   * never removed because a stream failed. The check contacts the provider,
   * so it adds time to every run. Absent means the check is off.
   */
  skip_dead_streams?: boolean;
  /**
   * How far ahead of its start time an event may be promoted (1..720).
   * An event further away than this is left alone and picked up on a later
   * run. Absent means there is no lead limit, so an event promotes as soon
   * as it parses. Gates CREATES only unless `apply_lead_to_existing` says
   * otherwise: a channel that already exists is not removed for being far
   * away, because that would delete and recreate the same channel every day.
   */
  promote_lead_hours?: number;
  /**
   * Whether `promote_lead_hours` also holds back a unit that would attach an
   * event to a channel that already exists. Absent means false, which keeps
   * the create-only behaviour above.
   */
  apply_lead_to_existing?: boolean;
  /**
   * Where promoted event channels are numbered: 'auto', a positive integer,
   * or a 'min-max' range like '900-999'. Absent means 'auto', which starts
   * at 1 and takes the lowest free numbers — that is how event channels end
   * up interleaved with an operator's real lineup. A range parks them past
   * it.
   */
  promote_channel_number?: string | number;
}

// =============================================================================
// Preview response
// =============================================================================

/** Confidence band of one scored candidate (never rendered as color alone). */
export type EventSyncBand = 'attach' | 'ambiguous' | 'reject';

/** Team-token verdict of one scored candidate. */
export type EventSyncTeamVerdict = 'agree' | 'conflict' | 'uncertain' | 'absent';

/** Exactly one disposition per secondary stream; the five sum to the total.
 * `excluded_by_operator` (bead ti939.3.5): the stream's only viable pairing
 * carries an operator "never attach" exclusion. */
export type EventSyncDisposition =
  | 'would_attach'
  | 'ambiguous'
  | 'unmatched'
  | 'parse_failed'
  | 'excluded_by_operator';

export interface EventSyncPreflightFailure {
  group_id: number;
  role: 'master' | 'secondary';
  check: string;
  expected: string;
  got: string;
  message: string;
}

/**
 * bead 2ey2y: rule-level pre-flight WARNING — advisory, never flips `ok`.
 * Same teaching shape as a failure minus the per-group fields. Known
 * backend `check` values: `staleness_rail_snapshots` (the stale-dateless
 * demote rail is enabled but no previous-day M3U snapshot covers any
 * secondary stream, so it silently fails open).
 */
export interface EventSyncPreflightWarning {
  check: string;
  expected: string;
  got: string;
  message: string;
}

export interface EventSyncPreflight {
  ok: boolean;
  failures: EventSyncPreflightFailure[];
  /** bead 2ey2y: advisory warnings. Absent on older payloads → treat as empty. */
  warnings?: EventSyncPreflightWarning[];
}

export interface EventSyncPreviewSummary {
  secondary_streams: number;
  would_attach: number;
  ambiguous_skipped: number;
  unmatched: number;
  parse_failed: number;
  master_channels: number;
  master_channels_unparsed: number;
  /**
   * ti939.3.2: subset of `would_attach` reached via a prior review-queue
   * accept (fingerprint-keyed decision) rather than the score threshold.
   */
  would_attach_via_review: number;
  /** ti939.3.2: rendered candidate pairings currently pending review. */
  candidates_pending_review: number;
  /** bead jqwfq Stage 1: stream names positively present in the provider's
   * previous-day M3U snapshot (stale suspects). Absent on older payloads. */
  stale_suspect_streams?: number;
  /** bead jqwfq Stage 1: streams whose name freshness could not be
   * determined (no qualifying snapshot / unknown provider / uncaptured or
   * capped group — the signal fails open). Absent on older payloads. */
  freshness_unknown_streams?: number;
  /** ti939.3.5: streams whose only viable pairing carries an operator
   * never-attach exclusion (fifth disposition). Absent on older payloads. */
  excluded_by_operator?: number;
  /** bead ti939.4.1: promotion units (= channels) the plan would realize.
   * Present ONLY on promotion-enabled previews. */
  would_promote?: number;
  /** bead ti939.4.1: justifying streams across all promotion units.
   * Present ONLY on promotion-enabled previews. */
  would_promote_streams?: number;
}

/**
 * ti939.3.2: review-queue state of one exact (stream, master) pairing —
 * `'pending'` (awaiting the operator), `'accepted'` / `'rejected'`
 * (durable fingerprint-keyed decision), or `null` (never queued).
 */
export type EventSyncReviewStatus = 'pending' | 'accepted' | 'rejected' | null;

/**
 * ti939.3.2: how a `would_attach` disposition was reached — the matcher's
 * own score admission or a prior operator accept from the review queue.
 */
export type EventSyncAttachSource = 'threshold' | 'review_queue' | null;

export interface EventSyncCandidate {
  master_channel_name: string;
  master_channel_id: number | null;
  master_parsed_title: string | null;
  master_parsed_start: string | null;
  score: number;
  band: EventSyncBand;
  team_verdict: EventSyncTeamVerdict;
  time_delta_minutes: number;
  reject_reason: string | null;
  review_status: EventSyncReviewStatus;
  /** ti939.3.5: true when this exact pairing fingerprint carries an
   * operator never-attach exclusion. Absent on older payloads. */
  excluded?: boolean;
}

/**
 * S5 (bead sf8dj): diagnostic provenance for a `would_attach` row — the
 * optional relaxation (`key`) that admitted it, with a short display `label`.
 * Keys: `assume_current_date` | `time_window_ignored` | `lowered_threshold` |
 * `master_from_stream`. Empty for a plain in-window default-threshold match.
 */
export interface EventSyncMatchedVia {
  key: string;
  label: string;
}

export interface EventSyncStreamRow {
  stream_id: number | null;
  stream_name: string;
  group_id: number;
  provider: string | null;
  parsed_title: string | null;
  parsed_start: string | null;
  matched_pattern: string | null;
  disposition: EventSyncDisposition;
  unmatchable_reason: string | null;
  /**
   * Machine-readable reason when `disposition` is `'ambiguous'`, else null.
   * Opaque string — render verbatim, never enumerate exhaustively. Known
   * backend values: `contested_top_candidates`,
   * `top_candidate_ambiguous_band`, `venue_token_conflict` (bead yjchp),
   * and `stale_dateless_stream_name` (bead jqwfq — an assume_current_date
   * match demoted because its dateless stream name predates today per the
   * previous-day M3U snapshot).
   */
  ambiguous_reason?: string | null;
  attach_source: EventSyncAttachSource;
  /**
   * bead jqwfq Stage 1: tri-state staleness signal. `true` = this exact
   * name was already in the provider's previous-day M3U snapshot (stale
   * suspect under assume_current_date); `false` = captured uncapped and
   * absent (first seen today, advisory); `null`/absent = unknown
   * (no qualifying snapshot / unknown provider / uncaptured or capped
   * group — the signal fails open).
   */
  name_seen_before_today?: boolean | null;
  would_attach_master: { channel_id: number | null; name: string } | null;
  /** ti939.3.5: masters this stream will NEVER attach to (operator
   * exclusion) — non-empty whenever a pairing was suppressed, including on
   * rows that still attach/queue against other masters. Absent on older
   * payloads → treat as empty. */
  excluded_masters?: string[];
  candidates: EventSyncCandidate[];
  /** S5 (bead sf8dj): provenance chips for a would-attach row (may be absent
   * on older payloads → treat as empty). */
  matched_via?: EventSyncMatchedVia[];
}

export interface EventSyncUnmatchedStream {
  stream_id: number | null;
  stream_name: string;
  group_id: number;
  provider: string | null;
  parsed_title: string | null;
  parsed_start: string | null;
  best_candidate: {
    master_channel_name: string;
    score: number;
    band: EventSyncBand;
    reject_reason: string | null;
  } | null;
  /** bead ti939.4.1: true when this unmatched stream is in the promotion
   * plan. Present only on promotion-enabled previews; false on rows with
   * no complete parsed identity (or trimmed by the per-run cap —
   * `promote_capped` marks those). */
  would_promote?: boolean;
  /** bead ti939.4.1: how the plan realizes this stream's unit. */
  promote_action?: 'create' | 'attach_existing' | null;
  /** bead ti939.4.1: the deterministic derived channel name. */
  promote_channel_name?: string;
  /** bead ti939.4.1: true when the unit was trimmed by max_promote_per_run
   * this run (it re-surfaces next run — promotion is idempotent). */
  promote_capped?: boolean;
  /** True when `skip_past_events` dropped this event because it already
   * finished. Unlike `promote_capped` it does not re-surface next run. */
  promote_skipped_past?: boolean;
  /** True when the dropped event already has a channel, so the channel
   * leaves the set this rule manages and the rule's orphan cleanup decides
   * its fate. Only ever set alongside `promote_skipped_past`. */
  promote_skipped_past_adopted?: boolean;
  /** True when `promote_lead_hours` held this event back because its start
   * is further ahead than the lead window. Like `promote_capped` and unlike
   * `promote_skipped_past`, it re-surfaces on a later run. Absent on a
   * backend without the lead window. */
  promote_skipped_early?: boolean;
  /** True when the held-back event already has a channel. The run keeps that
   * channel instead of handing it to the orphan cleanup. Only ever set
   * alongside `promote_skipped_early`. */
  promote_skipped_early_adopted?: boolean;
  /** True when the stream name carries no date, so there is no event day to
   * build a channel around and the stream is left alone. It comes back on a
   * later run only if the provider renames the stream. Absent on a backend
   * that still promotes dateless streams. */
  promote_skipped_dateless?: boolean;
  /** True when this stream failed its health check. On its own it means the
   * event still promotes on the streams that passed; alongside
   * `promote_skipped_all_dead` it means nothing was left to attach. Absent
   * on a backend without the health check. */
  promote_stream_dead?: boolean;
  /** True when EVERY stream behind this event failed its health check, so
   * no channel is created. A channel the event already has is never retired
   * for this. Only ever set alongside `promote_stream_dead`. */
  promote_skipped_all_dead?: boolean;
}

/** bead ti939.4.1: one stream inside a promotion unit. */
export interface EventSyncPromotionUnitStream {
  stream_id: number | null;
  stream_name: string;
  provider: string | null;
  group_id: number;
  disposition: EventSyncDisposition;
}

/** bead ti939.4.1: one promotion unit — every same-run promotable stream
 * sharing one exact event key, realized as ONE channel. */
export interface EventSyncPromotionUnit {
  channel_name: string;
  action: 'create' | 'attach_existing';
  event_key: string;
  dateless: boolean;
  existing_channel_id: number | null;
  streams: EventSyncPromotionUnitStream[];
}

/** bead ti939.4.1: the promotion plan block — present ONLY when the
 * previewed config carries `promote_unmatched: true`. */
export interface EventSyncPromotionPreview {
  enabled: boolean;
  target_group_id: number;
  would_promote: number;
  would_promote_streams: number;
  would_create: number;
  would_attach_existing: number;
  cap: number;
  capped: boolean;
  cap_overage: number;
  /** How many events `skip_past_events` dropped because they had already
   * finished. 0 when the filter is off. Both counts always ride along
   * when this block is present, so neither needs a reader-side default:
   * the backend that renders the block is the one shipped beside this
   * frontend. */
  skipped_past: number;
  /** How many of those dropped events already have a channel. Each one
   * leaves the set this rule manages, so the rule's orphan cleanup decides
   * what happens to the channel. Always a subset of `skipped_past`. */
  skipped_past_adopted: number;
  /**
   * How many events are not promoted yet because their start is further
   * ahead than `promote_lead_hours`. Each one comes back on a later run.
   * 0 when no lead limit is set.
   *
   * This and the two health counts below are OPTIONAL, unlike the two
   * skipped-past counts above: a backend built before the lead window and
   * the stream health check omits them entirely, and the frontend can be
   * deployed on its own (`scripts/deploy-frontend.sh`). Every reader
   * defaults a missing count to 0 rather than rendering `undefined`.
   */
  skipped_early?: number;
  /** How many individual streams were dropped because a health check could
   * not play them. Their events still promote on the streams that passed.
   * Absent on a backend without the health check. */
  dead_streams_skipped?: number;
  /** How many events were dropped because EVERY stream behind them failed
   * the health check, leaving nothing to attach. Failing health blocks a
   * new channel; it never retires one that already exists. Absent on a
   * backend without the health check. */
  skipped_all_dead?: number;
  /** How many streams were left alone because their names carry no date, so
   * there is no event day to build a channel around. Optional and read with
   * a 0 fallback for the same reason as the three counts above: a backend
   * that still promotes dateless streams omits it. */
  skipped_dateless?: number;
  /** How many delisted streams the run would take off promoted channels that
   * already exist. Detaching is the only destructive thing promotion does to
   * a live channel, so the operator sees the number before approving the run.
   * Optional and read with a 0 fallback for the same reason as the counts
   * above: a backend without the health check omits it. */
  stale_streams_removed?: number;
  units: EventSyncPromotionUnit[];
}

export interface EventSyncParseFailureGroup {
  group_id: number;
  group_name: string | null;
  reason: string | null;
  count: number;
  stream_names: string[];
}

export interface EventSyncPreviewResponse {
  preflight: EventSyncPreflight;
  summary: EventSyncPreviewSummary;
  streams: EventSyncStreamRow[];
  unmatched_streams: EventSyncUnmatchedStream[];
  parse_failures: EventSyncParseFailureGroup[];
  unparsed_master_channels: string[];
  truncated: boolean;
  /** bead ti939.4.1: promotion plan — absent unless the previewed config
   * carries `promote_unmatched: true`. */
  promotion?: EventSyncPromotionPreview;
}

/** Request body: exactly one of rule_id / event_sync_config. */
export type EventSyncPreviewRequest =
  | { rule_id: number }
  | { event_sync_config: EventSyncConfig };

// =============================================================================
// Review queue (bead ti939.3.2) — /api/event-sync-reviews
// =============================================================================

/**
 * Display-only evidence snapshot on one review row. Everything the operator
 * needs to judge the pairing without an opaque aggregate score: both raw
 * names, both parsed identities, per-candidate score/band/verdict/delta.
 * The snapshot channel/stream ids are NOT identity — the backend re-verifies
 * them against live Dispatcharr before any use (fingerprints are the key).
 */
export interface EventSyncReviewEvidence {
  rule_name?: string;
  stream_name?: string;
  provider?: string | null;
  secondary_group_id?: number;
  stream_id?: number | null;
  stream_parsed_title?: string | null;
  stream_parsed_start?: string | null;
  master_channel_name?: string;
  master_channel_id?: number | null;
  master_parsed_title?: string | null;
  master_parsed_start?: string | null;
  score?: number;
  band?: EventSyncBand;
  team_verdict?: EventSyncTeamVerdict;
  time_delta_minutes?: number;
  ambiguous_reason?: string | null;
}

/**
 * One event_sync_reviews row, as projected by `GET /api/event-sync-reviews`.
 * Identity is the content fingerprint (`rule_id`, `provider_id`,
 * `stream_name_hash`, `event_key`) — never channel/stream IDs — so decisions
 * survive Dispatcharr refreshes (epic ti939.3 keying constraint).
 */
export interface EventSyncReviewRecord {
  id: number;
  rule_id: number;
  provider_id: number;
  stream_name_hash: string;
  event_key: string;
  status: 'pending' | 'accepted' | 'rejected' | 'superseded';
  created_at: number;
  last_seen_at: number;
  resolved_at: number | null;
  resolution_source: string | null;
  evidence: EventSyncReviewEvidence;
}

/** Paginated envelope for `GET /api/event-sync-reviews`. */
export interface EventSyncReviewsListResponse {
  reviews: EventSyncReviewRecord[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

/** Flat outcome for `POST /api/event-sync-reviews/{id}/accept`. */
export interface AcceptEventSyncReviewOutcome {
  status: 'accepted';
  attached: boolean;
  already_attached: boolean;
  attach_deferred_reason: string | null;
  superseded_siblings: number;
}

/** Flat outcome for `POST /api/event-sync-reviews/{id}/reject`. */
export interface RejectEventSyncReviewOutcome {
  status: 'rejected';
}

// =============================================================================
// Operator exclusions (bead ti939.3.5) — /api/event-sync-exclusions
// =============================================================================

/**
 * One event_sync_exclusions row: a durable "never attach this provider
 * stream to that event" standing order. Identity is the content fingerprint
 * (`rule_id`, `provider_id`, `stream_name_hash`, `event_key`) — never
 * channel/stream IDs — so exclusions survive Dispatcharr refreshes and
 * stream-ID churn (epic ti939.3 keying constraint). `evidence` is the same
 * display-only snapshot shape as review rows.
 */
export interface EventSyncExclusionRecord {
  id: number;
  rule_id: number;
  provider_id: number;
  stream_name_hash: string;
  event_key: string;
  created_at: number;
  note: string | null;
  evidence: EventSyncReviewEvidence;
  /** True on POST responses when the fingerprint was already excluded. */
  already_existed?: boolean;
}

/** Paginated envelope for `GET /api/event-sync-exclusions`. */
export interface EventSyncExclusionsListResponse {
  exclusions: EventSyncExclusionRecord[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

/** Create body for `POST /api/event-sync-exclusions`. */
export interface EventSyncExclusionCreateRequest {
  rule_id: number;
  provider_id: number;
  stream_name_hash: string;
  event_key: string;
  note?: string | null;
  evidence?: EventSyncReviewEvidence;
}
