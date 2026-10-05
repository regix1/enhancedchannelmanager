"""Current stream playability and confirmed retirement evidence for events.

Admission uses one three-state verdict. ``True`` requires sustained measured
transport and complete non-dark content from the exact current stream name.
``False`` requires a current hard failure, zero sustained transport, or
persistent dark content. Missing, stale, future, incomplete, or mismatched
evidence remains ``None``.

Retirement is stronger. Only a scoped delisting or repeated current hard
probe failures can retire a stream. A lone dark frame result or zero-flow
sample can hide a channel reversibly, but cannot destroy event state.

Preview calls are read-only. Callers pass a loading expiry explicitly; None
leaves loading unbounded while each media sample retains its finite limits.
Expired or cancelled work cannot save or consume a late verdict.
"""
from __future__ import annotations

import asyncio
import logging
import math
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from cache import get_cache
from services.mutation_plan_store import canonical_hash

logger = logging.getLogger(__name__)

__all__ = [
    "MAX_HEALTH_PROBES_PER_RUN",
    "collect_stream_flow",
    "find_dead_streams",
    "find_working_streams",
]

# How many never-probed candidate streams one run may probe. Probing runs
# ffprobe against the provider, so an uncapped first run on a large rule
# would hold the pipeline open for as long as it takes to dial every stream
# in the playlist. Runs are idempotent: what this run does not reach keeps
# its "no verdict" reading and gets probed by a later run.
MAX_HEALTH_PROBES_PER_RUN = 200

# Batch size for the stream-by-id lookup that supplies probe URLs. Matches
# the page size the Event Sync fetch already uses against the same API.
_URL_LOOKUP_BATCH = 500

_FAILED_PROBE_STATUSES = frozenset({"failed", "timeout"})
# Probes needed before a failure is a verdict rather than one bad moment.
_CONFIRMED_FAILURES = 2

# Reservations must advance before URL lookup without holding a lock across
# network or probe work. The cached positions remain advisory and bounded.
_selection_lock = threading.Lock()


async def find_dead_streams(
    stream_ids,
    *,
    stream_names: Mapping[int, str],
    client=None,
    probe_missing: bool = False,
    stale_stream_ids: set[int] | None = None,
    event_start_by_stream: Mapping[int, datetime] | None = None,
    probe_before: datetime | None = None,
    probe_first: set[int] | None = None,
    expires_at: datetime | None = None,
) -> set[int]:
    """Return streams with current, confirmed retirement evidence.

    Args:
        stream_ids: The candidate stream ids a promotion plan is about to
            turn into channels, plus bounded attachments already owned by
            that rule when retirement is enabled. Duplicates and ``None``
            entries are tolerated.
        stream_names: Exact names from the caller's validated event scope.
        client: The Dispatcharr client, needed only to look up probe URLs.
            Without it nothing is probed.
        probe_missing: Probe missing, old, or singly failed candidates. True
            for a live run and false for read-only preview.
        stale_stream_ids: Ids the provider has stopped listing, read off the
            ``is_stale`` flag the caller's own stream fetch already carried.
            Only started events with exact scoped names can use this proof.
        event_start_by_stream: Authoritative start time for each event.
        probe_before: Optional earliest usable measurement for an event
            lifecycle check.
        probe_first: Candidates still waiting for a channel, which receive
            the bounded probe budget before existing attachments.
        expires_at: The caller-owned absolute probe expiry. Required when
            ``probe_missing`` is true.

    Operational failures add no retirement evidence. An invalid write call
    without its required expiry raises ``ValueError``.
    """
    ids = sorted({sid for sid in stream_ids if sid is not None})
    if not ids:
        return set()
    if probe_missing and expires_at is None:
        raise ValueError("expires_at is required when probe_missing is true")

    now = datetime.now(timezone.utc)
    started = event_start_by_stream or {}
    cutoff = _utc_time(probe_before)

    def lower_bound(stream_id: int) -> datetime | None:
        event_start = _utc_time(started.get(stream_id))
        if event_start is None or event_start > now:
            return None
        return max(event_start, cutoff) if cutoff is not None else event_start

    eligible = {
        sid for sid in ids
        if isinstance(stream_names.get(sid), str)
        and bool(stream_names[sid])
        and lower_bound(sid) is not None
    }
    delisted = stale_stream_ids or set()
    stale = eligible & delisted

    try:
        stats = await _load_stats(ids)
    except Exception as e:
        logger.warning(
            "[EVENT-SYNC] stream health lookup failed (%s) — only the "
            "stream(s) the provider no longer lists are treated as dead "
            "this run", e,
        )
        return stale

    threshold = _strike_threshold()

    def confirmed(stream_id: int, rows: Mapping[int, dict]) -> bool:
        checked_after = lower_bound(stream_id)
        if checked_after is None:
            return False
        return _dead_once_started(
            rows.get(stream_id),
            checked_after,
            threshold,
            stream_name=stream_names[stream_id],
            now=now,
        )

    dead = set(stale)
    dead.update(sid for sid in eligible if confirmed(sid, stats))

    if probe_missing and client is not None:
        # A stale stream is left out: the provider has already answered
        # the question a probe would ask. So is a stream whose event has not
        # started: its result is discarded below anyway, and the row it would
        # leave behind is one nothing re-probes, so the reading taken while
        # the event had nothing to serve would decide that event forever.
        unprobed = [
            sid for sid in eligible
            if sid not in stale and not confirmed(sid, stats)
            and (
                stats.get(sid) is None
                or (stats[sid].get("probe_status") in _FAILED_PROBE_STATUSES)
                or not _probed_after_kickoff(
                    stats[sid], lower_bound(sid), now=now,
                )
            )
        ]
        unprobed.sort(key=lambda sid: (
            _utc_time((stats.get(sid) or {}).get("last_probed")) is not None,
            _utc_time((stats.get(sid) or {}).get("last_probed"))
            or datetime.min.replace(tzinfo=timezone.utc),
            sid,
        ))
        if probe_first:
            waiting = [sid for sid in unprobed if sid in probe_first]
            if waiting and len(waiting) < len(unprobed):
                logger.info(
                    "[EVENT-SYNC] probing the %d stream(s) still waiting "
                    "for a channel first — %d already-promoted stream(s) "
                    "keep no health verdict until a run has nothing new "
                    "to measure", len(waiting), len(unprobed) - len(waiting),
                )
                unprobed = waiting
        if unprobed:
            await _probe_and_collect_failures(
                client,
                unprobed,
                expires_at=expires_at,
                event_start_by_stream={
                    sid: _utc_time(started.get(sid)) for sid in unprobed
                },
                stream_names={sid: stream_names[sid] for sid in unprobed},
            )
            if _expired(expires_at):
                return dead
            try:
                refreshed = await _load_stats(unprobed)
            except Exception as e:
                logger.warning(
                    "[EVENT-SYNC] refreshed retirement health lookup failed "
                    "(%s) — no new retirement evidence is used", e,
                )
            else:
                stats = {**stats, **refreshed}
                now = datetime.now(timezone.utc)
                dead.update(sid for sid in unprobed if confirmed(sid, stats))

    return dead


def stale_streams_to_detach(
    unit_stream_ids: set[int],
    attached: list[int],
    stale_stream_ids: set[int],
    working_stream_ids: set[int],
) -> list[int]:
    """Which of one event's delisted streams may leave a channel.

    Empty unless that event has a stream on the channel with a passing
    probe. A delisted stream that still plays is the only thing serving the
    event until something has proved it can take over.

    Scoped to the event's own streams, because two events can derive the
    same channel name and share a channel, and an operator can leave a third
    party's stream on it. Without the scope one event's passing probe would
    take away another's only working stream.

    The run detaches exactly this list and the preview reports its length,
    so the rule lives here rather than in either of them. [75]
    """
    on_channel = unit_stream_ids & set(attached)
    if not on_channel & working_stream_ids:
        return []
    return sorted(on_channel & stale_stream_ids)


async def find_working_streams(
    stream_ids,
    *,
    event_start_by_stream: Mapping[int, datetime],
    stream_names: Mapping[int, str],
    checked_after: datetime,
) -> set[int]:
    """Return streams whose current transport and content are playable.

    Deliberately NOT the complement of :func:`find_dead_streams`. "Not
    dead" covers a stream nobody has ever probed and, before its event
    starts, one that is failing right now — neither of which is evidence
    that anything plays. Only the collector's current ``True`` state can
    authorize removing a stream that may still be serving an event.

    A live run's own probes write their rows before this reads, so a
    candidate probed to success moments earlier in the same run answers
    here. The preview and a dry run probe nothing, so they see whatever
    verdicts already existed.

    Never raises. Every failure path returns the empty set, which reads as
    "nothing is proven to work" and leaves every channel exactly as it is.
    """
    states = await collect_stream_flow(
        stream_ids,
        client=None,
        checked_after=checked_after,
        event_start_by_stream=event_start_by_stream,
        stream_names=stream_names,
        expires_at=None,
    )
    return {sid for sid, state in states.items() if state is True}


async def collect_stream_flow(
    stream_ids,
    *,
    client,
    checked_after: datetime,
    event_start_by_stream: Mapping[int, datetime],
    stream_names: Mapping[int, str],
    expires_at: datetime | None,
    probe_missing: bool = False,
    probe_while_busy: bool = False,
    stop_when_playable: bool = False,
    event_streams: Mapping[str, frozenset[int]] | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> dict[int, bool | None]:
    """Return fresh measured-flow verdicts for a bounded stream set.

    ``True`` means the same current observation proved sustained transport and
    complete non-dark content. ``False`` means a complete observation proved
    zero sustained transport, persistent dark content, or a hard probe
    failure. ``None`` means the available evidence is incomplete or stale.
    """
    ids = sorted({sid for sid in stream_ids if sid is not None})
    if not ids:
        return {}
    if stop_when_playable and (
        not isinstance(event_streams, Mapping) or not event_streams
        or any(not isinstance(key, str) or not key
               or not isinstance(members, frozenset) or not members
               for key, members in event_streams.items())
        or set().union(*event_streams.values()) != set(ids)
    ):
        raise ValueError("event_streams must cover the requested streams")
    now = datetime.now(timezone.utc)
    caller_cutoff = _utc_time(checked_after)
    if caller_cutoff is None:
        return {sid: None for sid in ids}
    try:
        stats = await _load_stats(ids)
    except Exception as e:
        logger.warning(
            "[STREAM-HEALTH] stream flow lookup failed (%s) — visibility falls "
            "back to the current guide",
            e,
        )
        stats = {}

    def classify(stream_id: int, rows: Mapping[int, dict], at: datetime) -> bool | None:
        event_start = _utc_time(event_start_by_stream.get(stream_id))
        stream_name = stream_names.get(stream_id)
        if (
            event_start is None
            or event_start > at
            or not isinstance(stream_name, str)
            or not stream_name
        ):
            return None
        lower = max(event_start, caller_cutoff, at - timedelta(minutes=5))
        return _fresh_flow_state(
            rows.get(stream_id),
            lower,
            stream_name=stream_name,
            now=at,
        )

    now = datetime.now(timezone.utc)
    states = {sid: classify(sid, stats, now) for sid in ids}
    missing = [sid for sid, state in states.items() if state is None]
    missing = [
        sid for sid in missing
        if _utc_time(event_start_by_stream.get(sid)) is not None
        and _utc_time(event_start_by_stream.get(sid)) <= now
        and isinstance(stream_names.get(sid), str)
        and bool(stream_names[sid])
    ]
    if stop_when_playable:
        positive = {key for key, members in event_streams.items()
                    if any(states[sid] is True for sid in members)}
        missing = [sid for sid in missing
                   if any(sid in members and key not in positive
                          for key, members in event_streams.items())]
        event_streams = {key: frozenset(members.intersection(missing))
                         for key, members in event_streams.items()
                         if key not in positive and members.intersection(missing)}
    missing.sort(key=lambda sid: (
        _utc_time((stats.get(sid) or {}).get("last_probed")) is not None,
        _utc_time((stats.get(sid) or {}).get("last_probed"))
        or datetime.min.replace(tzinfo=timezone.utc),
        sid,
    ))
    if probe_missing and missing and client is not None:
        try:
            from stream_prober import get_prober

            current_prober = get_prober()
        except Exception:
            current_prober = None
        if not probe_while_busy and current_prober is not None and getattr(
            current_prober, "_probing_in_progress", False,
        ):
            logger.info(
                "[STREAM-HEALTH] A scheduled probe is already running; %d "
                "stream(s) keep their current guide fallback",
                len(missing),
            )
            return states
        confirmed: set[int] = set()
        await _probe_and_collect_failures(
            client,
            missing,
            expires_at=expires_at,
            event_start_by_stream={
                sid: _utc_time(event_start_by_stream[sid]) for sid in missing
            },
            stream_names={sid: stream_names[sid] for sid in missing},
            cancelled=cancelled,
            stop_when_playable=stop_when_playable,
            confirmed=confirmed,
            event_streams=event_streams,
            checked_after=caller_cutoff,
        )
        if _expired(expires_at) or (cancelled is not None and cancelled()):
            return {sid: None for sid in ids}
        refreshed = {}
        try:
            refreshed = await _load_stats(sorted(confirmed))
        except Exception as e:
            logger.warning(
                "[STREAM-HEALTH] refreshed stream flow lookup failed (%s) — "
                "visibility falls back to the current guide",
                e,
            )
        if _expired(expires_at) or (cancelled is not None and cancelled()):
            return {sid: None for sid in ids}
        now = datetime.now(timezone.utc)
        for sid in ids:
            if sid in confirmed:
                states[sid] = classify(sid, refreshed, now)
            elif states[sid] is not None:
                states[sid] = classify(sid, stats, now)
    return states


async def _load_stats(stream_ids: list[int]) -> dict[int, dict]:
    """Health records for these stream ids, keyed by stream id."""
    from fastapi.concurrency import run_in_threadpool
    from stream_prober import StreamProber

    return await run_in_threadpool(
        StreamProber.get_stats_by_stream_ids, stream_ids
    )


def _utc_time(value) -> datetime | None:
    """Return a UTC observation time, or None for an invalid value."""
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.rstrip("Z"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _expired(expires_at: datetime | None) -> bool:
    """Whether the caller's one supplied attempt lifetime is over."""
    expiry = _utc_time(expires_at)
    return expiry is not None and datetime.now(timezone.utc) >= expiry


def _strike_threshold() -> int:
    """How many consecutive failures make a stream struck out, or 0 = off.

    A stored ``0`` is the operator switching the struck-out check off. An
    unreadable setting is not the same statement, so it falls back to the
    shipped default instead of returning ``0`` and turning the check off on
    the operator's behalf without saying so. [10]
    """
    try:
        from config import get_settings

        return int(get_settings().strike_threshold or 0)
    except Exception as e:
        logger.warning(
            "[EVENT-SYNC] strike threshold unreadable (%s) — using the "
            "shipped default of 3, because returning 0 here would switch "
            "the struck-out check off silently", e,
        )
        return 3


def _fresh_flow_state(
    stat: dict | None,
    checked_after: datetime,
    *,
    stream_name: str,
    now: datetime,
) -> bool | None:
    """Interpret current, name-bound transport and content evidence."""
    if stat is None or stat.get("stream_name") != stream_name:
        return None
    lower = _utc_time(checked_after)
    upper = _utc_time(now)
    if lower is None or upper is None or lower > upper:
        return None

    probed_at = _utc_time(stat.get("last_probed"))
    probed_current = (
        probed_at is not None and lower <= probed_at <= upper
    )

    content_at = _utc_time(stat.get("black_screen_checked_at"))
    content_current = (
        content_at is not None
        and lower <= content_at <= upper
        and (probed_at is None or content_at >= probed_at)
        and isinstance(stat.get("is_black_screen"), bool)
    )
    content = stat.get("is_black_screen") if content_current else None

    if probed_current and stat.get("probe_status") in _FAILED_PROBE_STATUSES:
        return False
    if content is True:
        return False

    sampled = _sample_says_dead(stat) if probed_current else None
    if sampled is True:
        return False
    if sampled is False and content is False:
        return True
    return None


def _dead_once_started(
    stat: dict | None,
    started_at: datetime,
    threshold: int,
    *,
    stream_name: str,
    now: datetime,
) -> bool:
    """Whether this exact stream has repeated current hard failures."""
    if stat is None or stat.get("stream_name") != stream_name:
        return False
    if stat.get("probe_status") not in _FAILED_PROBE_STATUSES:
        return False
    if not _probed_after_kickoff(stat, started_at, now=now):
        return False
    return _is_struck(stat, threshold) or _probe_failed(
        stat, started_at, now=now,
    )


def _sample_says_dead(stat: dict | None) -> bool | None:
    """Classify sustained measured flow without using declared bitrate."""
    measured = (stat or {}).get("measured_bitrate")
    if (
        isinstance(measured, bool)
        or not isinstance(measured, (int, float))
        or not math.isfinite(measured)
        or measured < 0
    ):
        return None
    return measured == 0


def _is_struck(stat: dict | None, threshold: int) -> bool:
    """Has this stream failed often enough in a row to count as struck out?"""
    if (
        stat is None
        or threshold <= 0
        or stat.get("probe_status") not in _FAILED_PROBE_STATUSES
    ):
        return False
    return int(stat.get("consecutive_failures") or 0) >= max(
        _CONFIRMED_FAILURES, threshold,
    )


def _probe_failed(
    stat: dict | None,
    started_at: datetime,
    *,
    now: datetime | None = None,
) -> bool:
    """Did this stream's stored probe verdict say it did not answer, twice?

    The sibling of :func:`_is_struck`, asking the other half of the stored
    row: failures recorded as ``failed`` or ``timeout`` count even though the
    strike counter has not reached its operator threshold. [6]

    A single reading does not, though, because one probe is one moment. The
    same slot answered on one probe and failed on the next within the same
    afternoon, in both directions — so a lone failure is as likely to be the
    provider blinking as the event being over, and this verdict now deletes
    the channel. Requiring the next probe to agree costs one cycle and is the
    smallest thing that distinguishes a blink from an ending.

    They count only when the probe itself happened at or after the event
    started, for the reason :func:`_probed_after_kickoff` gives. [59]
    """
    if stat is None:
        return False
    if stat.get("probe_status") not in _FAILED_PROBE_STATUSES:
        return False
    if int(stat.get("consecutive_failures") or 0) < _CONFIRMED_FAILURES:
        return False
    return _probed_after_kickoff(stat, started_at, now=now)


def _probed_after_kickoff(
    stat: dict,
    started_at: datetime,
    *,
    now: datetime | None = None,
) -> bool:
    """Was this stream's stored row written at or after the event started?

    A row written while the event was still ahead was taken when there was
    nothing to serve, and nothing re-probes a stream that already has a
    row, so an earlier reading would decide the event forever. A row with
    no timestamp cannot be shown to be a live-event reading, so it does
    not count either. [59]
    """
    # The health table keeps naive UTC and serializes it with a Z.
    probed_at = _utc_time(stat.get("last_probed"))
    start = _utc_time(started_at)
    upper = _utc_time(now) if now is not None else datetime.now(timezone.utc)
    if probed_at is None or start is None or upper is None:
        return False
    return start <= probed_at <= upper


async def _probe_and_collect_failures(
    client,
    stream_ids: list[int],
    *,
    expires_at: datetime | None,
    event_start_by_stream: Mapping[int, datetime],
    stream_names: Mapping[int, str],
    cancelled: Callable[[], bool] | None = None,
    stop_when_playable: bool = False,
    confirmed: set[int] | None = None,
    event_streams: Mapping[str, frozenset[int]] | None = None,
    checked_after: datetime | None = None,
) -> set[int]:
    """Probe bounded candidates and return current conclusive failures."""
    from stream_prober import ensure_prober

    expiry = _utc_time(expires_at)
    if expires_at is not None and expiry is None:
        raise ValueError("expires_at must be a valid datetime")
    if _expired(expiry) or (cancelled is not None and cancelled()):
        return set()
    if not stream_ids or MAX_HEALTH_PROBES_PER_RUN <= 0:
        return set()
    try:
        prober = ensure_prober()
    except Exception as e:
        logger.warning("[EVENT-SYNC] Stream prober unavailable (%s)", e)
        return set()
    if prober is None:
        return set()

    positive = set()
    memberships = {}
    if stop_when_playable:
        if event_streams is None:
            raise ValueError("event_streams is required for early completion")
        ordered = []
        seen = set()
        groups = {
            key: [sid for sid in stream_ids if sid in members]
            for key, members in sorted(event_streams.items())
        }
        for index in range(max((len(ids) for ids in groups.values()), default=0)):
            for ids in groups.values():
                if index < len(ids) and ids[index] not in seen:
                    ordered.append(ids[index])
                    seen.add(ids[index])
        stream_ids = ordered
        memberships = {
            sid: {key for key, members in event_streams.items() if sid in members}
            for sid in stream_ids
        }
    identity = None
    selection_id = uuid.uuid4().hex
    held_back = max(0, len(stream_ids) - MAX_HEALTH_PROBES_PER_RUN)
    if held_back or (stop_when_playable and len(stream_ids) > 1):
        parsed = urlparse(str(getattr(client, "base_url", "")))
        identity = canonical_hash({
            "endpoint": [
                parsed.scheme.lower(),
                (parsed.hostname or "").lower(),
                parsed.port,
                parsed.path,
            ],
            "streams": [
                [
                    stream_id,
                    stream_names[stream_id],
                    _utc_time(event_start_by_stream[stream_id]).isoformat(),
                ]
                for stream_id in sorted(stream_ids)
            ],
            **({"event_streams": [[key, sorted(members)]
                                 for key, members in sorted(event_streams.items())]}
               if stop_when_playable else {}),
        })
        with _selection_lock:
            if _expired(expiry) or (cancelled is not None and cancelled()):
                return set()
            cache = get_cache()
            cached = cache.get("event_sync_health_positions", ttl=86400)
            positions = dict(cached) if isinstance(cached, dict) else {}
            monotonic_now = time.monotonic()
            for key, position in list(positions.items()):
                if (
                    not isinstance(position, dict)
                    or not isinstance(position.get("expires_at"), (int, float))
                    or position["expires_at"] <= monotonic_now
                    or not isinstance(position.get("stream_id"), int)
                ):
                    positions.pop(key, None)
            position = positions.get(identity)
            ordered = list(stream_ids)
            if position is None:
                retention_expiry = monotonic_now + 86400
            else:
                retention_expiry = position["expires_at"]
                last_stream_id = position["stream_id"]
                if last_stream_id in ordered:
                    start = ordered.index(last_stream_id) + 1
                    ordered = ordered[start:] + ordered[:start]
            stream_ids = ordered[:MAX_HEALTH_PROBES_PER_RUN]
            positions.pop(identity, None)
            positions[identity] = {
                "expires_at": retention_expiry,
                "stream_id": stream_ids[0] if stop_when_playable else stream_ids[-1],
                **({"selection_id": selection_id} if stop_when_playable else {}),
            }
            while len(positions) > 256:
                positions.pop(next(iter(positions)))
            cache.set("event_sync_health_positions", positions)

    def remaining() -> float | None:
        return (
            max(0, (expiry - datetime.now(timezone.utc)).total_seconds())
            if expiry is not None else None
        )

    try:
        urls = await asyncio.wait_for(
            _probe_urls(client, stream_ids, stream_names=stream_names),
            timeout=remaining(),
        )
        if not urls or _expired(expiry) or (cancelled is not None and cancelled()):
            return set()
        await asyncio.wait_for(
            prober.refresh_account_probe_limits(), timeout=remaining(),
        )
    except asyncio.TimeoutError:
        return set()

    async def _probe_one(stream_id: int, url: str, name: str, m3u_account) -> None:
        if _expired(expiry) or (stop_when_playable and memberships[stream_id] <= positive) or (cancelled is not None and cancelled()):
            return
        try:
            async with asyncio.timeout(remaining()):
                async with prober.semaphore_for_account(m3u_account, event=True):
                    # A queued task cannot start media after another task supplies proof.
                    if _expired(expiry) or (stop_when_playable and memberships[stream_id] <= positive) or (cancelled is not None and cancelled()):
                        return
                    if stop_when_playable and identity is not None:
                        with _selection_lock:
                            cache = get_cache()
                            positions = cache.get("event_sync_health_positions", ttl=86400)
                            position = positions.get(identity) if isinstance(positions, dict) else None
                            if (
                                isinstance(position, dict)
                                and position.get("selection_id") == selection_id
                                and not _expired(expiry)
                                and not (stop_when_playable and memberships[stream_id] <= positive)
                                and (cancelled is None or not cancelled())
                            ):
                                position["stream_id"] = stream_id
                                cache.set("event_sync_health_positions", positions)
                    await prober.probe_stream(
                        stream_id, url, name, content=True, expires_at=expiry,
                    )
                    if stop_when_playable and not _expired(expiry):
                        stats = await _load_stats([stream_id])
                        classified_at = datetime.now(timezone.utc)
                        event_start = _utc_time(event_start_by_stream.get(stream_id))
                        if (
                            event_start is not None
                            and event_start <= classified_at
                            and _fresh_flow_state(
                                stats.get(stream_id),
                                max(event_start, _utc_time(checked_after) or event_start,
                                    classified_at - timedelta(minutes=5)),
                                stream_name=stream_names[stream_id],
                                now=classified_at,
                            ) is True
                        ):
                            positive.update(memberships[stream_id])
        except asyncio.TimeoutError:
            return
        except Exception as e:
            logger.warning(
                "[EVENT-SYNC] Health probe of stream %s raised (%s)",
                stream_id, e,
            )

    tasks = {
        asyncio.create_task(_probe_one(sid, url, name, account)): sid
        for sid, (url, name, account, _group) in urls.items()
    }
    pending = set(tasks)
    try:
        while pending:
            if _expired(expiry) or (cancelled is not None and cancelled()):
                break
            if stop_when_playable:
                for task in pending:
                    if memberships[tasks[task]] <= positive and not task.done():
                        task.cancel()
            _, pending = await asyncio.wait(
                pending, timeout=0.05, return_when=asyncio.FIRST_COMPLETED,
            )
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    if _expired(expiry) or (cancelled is not None and cancelled()):
        return set()
    try:
        current = await asyncio.wait_for(
            _probe_urls(client, list(urls), stream_names=stream_names),
            timeout=remaining(),
        )
        unchanged = [
            sid for sid, original in urls.items()
            if sid in current and current[sid] == original
        ]
        stats = await asyncio.wait_for(_load_stats(unchanged), timeout=remaining())
    except asyncio.TimeoutError:
        return set()
    except Exception as e:
        logger.warning("[EVENT-SYNC] Probed stream health reload failed (%s)", e)
        return set()
    classified_at = datetime.now(timezone.utc)
    if _expired(expiry) or (cancelled is not None and cancelled()):
        return set()
    if confirmed is not None:
        confirmed.update(unchanged)
    return {
        sid for sid in unchanged
        if (
            (event_start := _utc_time(event_start_by_stream.get(sid))) is not None
            and event_start <= classified_at
            and _fresh_flow_state(
                stats.get(sid),
                max(event_start, classified_at - timedelta(minutes=5)),
                stream_name=stream_names[sid],
                now=classified_at,
            ) is False
        )
    }


async def _probe_urls(
    client,
    stream_ids: list[int],
    *,
    stream_names: Mapping[int, str],
) -> dict[int, tuple]:
    """Playback url and name per stream id, for the ones that have a url.

    A stream the provider no longer lists, or one with no url at all, is
    left out rather than reported dead: it was never probed, so there is no
    verdict to report.
    """
    from stream_prober import extract_m3u_account_id

    urls: dict[int, tuple] = {}
    for start in range(0, len(stream_ids), _URL_LOOKUP_BATCH):
        batch = stream_ids[start:start + _URL_LOOKUP_BATCH]
        try:
            streams = await client.get_streams_by_ids(batch)
        except Exception as e:
            logger.warning(
                "[EVENT-SYNC] could not look up %d stream url(s) for the "
                "promotion health check (%s) — those streams keep no "
                "health verdict", len(batch), e,
            )
            continue
        for stream in streams or []:
            stream_id = stream.get("id")
            url = stream.get("url")
            name = stream.get("name")
            if (
                stream_id is None
                or not url
                or not isinstance(name, str)
                or name != stream_names.get(stream_id)
                or stream.get("is_stale") is True
            ):
                continue
            group = stream.get("channel_group_id")
            if group is None:
                group = stream.get("channel_group")
            if group is None:
                group = (stream.get("stream_group")
                         or stream.get("stream_group_id")
                         or stream.get("group_id"))
            if isinstance(group, dict):
                group = group.get("id")
            urls[stream_id] = (
                url,
                name,
                extract_m3u_account_id(stream.get("m3u_account")),
                group,
            )
    return urls
