"""Which of an Event Sync promotion's candidate streams do not work.

Promotion turns an unmatched provider stream into a channel, and a provider
that lists an event it cannot actually serve used to get a channel out of it
just the same. This module answers the one question that stops that: of the
streams a plan is about to turn into channels, which ones are dead?

Two call sites — ``channel_pipeline_executor`` for a live run and
``routers.channel_pipeline`` for the preview — so the verdict an operator
reads in the preview is computed by the same code the run obeys.

:func:`find_working_streams` answers the opposite question, and the two are
not each other's complement. "Not dead" is the bar for ATTACHING a stream,
because refusing an unprobed candidate would create no channels at all. A
passing probe is the bar for DETACHING one, because a channel that is
playing must not lose the stream serving it on anything weaker than proof
that something else works. [51]

**It reads health, it does not define it.** The rule is one sentence: a
stream is dead when the provider has stopped listing it, and probe-derived
signals only count once the event has started.

* **Provider-stale** — Dispatcharr's M3U refresh no longer finds the stream
  in the source playlist. Dead ALWAYS, whatever its probe history says and
  however new the event is. This is the provider's own statement rather
  than a verdict of ours, and a delisted stream is delisted whether or not
  it has aired. It is also the signal that matters: this provider re-issues
  every event under a new stream id on each refresh, so the superseded id
  keeps the ``success`` verdict it earned while it still worked.
* **A stored failed or timed-out probe**, and **a struck-out stream**
  (``consecutive_failures`` at or past the configured ``strike_threshold``,
  the same signal auto-creation's ``skip_struck_streams`` uses) — dead only
  once the event's start time has gone by.
* **Never probed, or not probed recently** — never dead. About sixty of
  some thirty-seven thousand streams have ever been probed, so treating an
  absent verdict as a failure would reject essentially every candidate and
  no channel would ever be created.

**Why a probe verdict is ignored before the event starts:** a stream for an
event that has not begun may fail simply because there is nothing to stream
yet. Rejecting it would stop the channel being created at all, which is the
opposite of what an operator wants from a health check. Staleness carries
the whole load for a future event, and it is enough. [4][7][8]

**"The event has started" is the caller's answer**, taken from the same
parsed start ``skip_past_events`` and ``promote_lead_hours`` already read,
so there is exactly one clock on this path and the tests can pin it.

**Probing is for live runs only.** A probe writes a ``StreamStats`` row, and
the preview endpoint promises zero writes, so the preview reads whatever
health data already exists and probes nothing. A dry run is the same. The
consequence is worth stating plainly: on a rule whose streams have never been
probed, the preview shows no dead streams and the run that follows may drop
some. The alternative was a preview that writes to the database, which is
worse.

**Fail open, always.** Every failure path here returns "nothing is dead".
A prober that will not start, a database that will not answer, a provider
timing out on the URL lookup — none of those are evidence that an operator's
event has no working stream, and treating them as evidence would block real
promotions during an outage.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

__all__ = [
    "MAX_HEALTH_PROBES_PER_RUN",
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


async def find_dead_streams(
    stream_ids,
    *,
    client=None,
    probe_missing: bool = False,
    stale_stream_ids: set[int] | None = None,
    event_start_by_stream: dict[int, datetime] | None = None,
) -> set[int]:
    """Return the subset of ``stream_ids`` that has no working stream.

    Args:
        stream_ids: The candidate stream ids — the streams a promotion plan
            is about to turn into channels, and only those. Duplicates and
            ``None`` entries are tolerated.
        client: The Dispatcharr client, needed only to look up probe URLs.
            Without it nothing is probed.
        probe_missing: Probe the candidates that have no health record yet.
            True for a live run, False for the preview and for a dry run,
            because a probe writes a row.
        stale_stream_ids: Ids the provider has stopped listing, read off the
            ``is_stale`` flag the caller's own stream fetch already carried.
            Every candidate in here is dead, and none of them is probed —
            dialling a stream the playlist no longer has is exactly the
            wasted work this saves.
        event_start_by_stream: When the event began, for the candidates
            whose event HAS begun. ONLY these can be marked dead by a probe
            verdict, stored or fresh. A candidate left out is one whose
            event is still ahead, and a stream that has nothing to serve yet
            is not a broken stream. The instant matters as well as the
            membership: a stored verdict recorded before that start was
            taken while the event still had nothing to serve, and nothing
            re-probes a stream that already has a record, so counting it
            would make a pre-kickoff failure permanent. [59]

    Never raises. Every failure path returns the streams the provider
    already disowned and nothing else, because a database that will not
    answer is not evidence that an operator's event has no working stream.
    """
    ids = sorted({sid for sid in stream_ids if sid is not None})
    if not ids:
        return set()

    delisted = stale_stream_ids or set()
    stale = {sid for sid in ids if sid in delisted}

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
    started = event_start_by_stream or {}
    dead = set(stale)
    dead |= {
        sid for sid in ids
        if sid in started
        and (
            _is_struck(stats.get(sid), threshold)
            or _probe_failed(stats.get(sid), started[sid])
        )
    }

    if probe_missing and client is not None:
        # A stale stream is left out: the provider has already answered
        # the question a probe would ask.
        unprobed = [
            sid for sid in ids if sid not in stats and sid not in stale
        ]
        if unprobed:
            fresh_failures = await _probe_and_collect_failures(
                client, unprobed
            )
            dead |= fresh_failures & set(started)

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


async def find_working_streams(stream_ids) -> set[int]:
    """Return the subset of ``stream_ids`` whose last probe passed.

    Deliberately NOT the complement of :func:`find_dead_streams`. "Not
    dead" covers a stream nobody has ever probed and, before its event
    starts, one that is failing right now — neither of which is evidence
    that anything plays. A stored ``success`` verdict is, and that is the
    only thing a caller may act on when it is about to take a stream away
    from a channel that is currently serving an event.

    A live run's own probes write their rows before this reads, so a
    candidate probed to success moments earlier in the same run answers
    here. The preview and a dry run probe nothing, so they see whatever
    verdicts already existed.

    Never raises. Every failure path returns the empty set, which reads as
    "nothing is proven to work" and leaves every channel exactly as it is.
    """
    ids = sorted({sid for sid in stream_ids if sid is not None})
    if not ids:
        return set()

    try:
        stats = await _load_stats(ids)
    except Exception as e:
        logger.warning(
            "[EVENT-SYNC] stream health lookup failed (%s) — no stream is "
            "treated as proven working this run, so nothing that is "
            "currently playing is taken off a channel", e,
        )
        return set()

    return {
        sid for sid in ids
        if (stats.get(sid) or {}).get("probe_status") == "success"
    }


async def _load_stats(stream_ids: list[int]) -> dict[int, dict]:
    """Health records for these stream ids, keyed by stream id."""
    from fastapi.concurrency import run_in_threadpool
    from stream_prober import StreamProber

    return await run_in_threadpool(
        StreamProber.get_stats_by_stream_ids, stream_ids
    )


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


def _is_struck(stat: dict | None, threshold: int) -> bool:
    """Has this stream failed often enough in a row to count as struck out?"""
    if stat is None or threshold <= 0:
        return False
    return int(stat.get("consecutive_failures") or 0) >= threshold


def _probe_failed(stat: dict | None, started_at: datetime) -> bool:
    """Did this stream's stored probe verdict say it did not answer?

    The sibling of :func:`_is_struck`, asking the other half of the stored
    row: one failure recorded as ``failed`` or ``timeout`` counts even
    though the strike counter has not reached its threshold. [6]

    It counts only when the probe itself happened at or after the event
    started. A stream dialled while its event was still ahead had nothing
    to serve, and the record it left is never refreshed, so an earlier
    verdict would decide the event forever. A row with no timestamp cannot
    be shown to be a live-event verdict and does not count either. [59]
    """
    if stat is None:
        return False
    if stat.get("probe_status") not in _FAILED_PROBE_STATUSES:
        return False
    # The health table keeps naive UTC and serializes it with a Z.
    raw = stat.get("last_probed")
    if not raw:
        return False
    probed_at = datetime.fromisoformat(
        raw.rstrip("Z")).replace(tzinfo=timezone.utc)
    return probed_at >= started_at


async def _probe_and_collect_failures(client, stream_ids: list[int]) -> set[int]:
    """Probe candidates with no health record and report the failures.

    Bounded twice over: at most ``MAX_HEALTH_PROBES_PER_RUN`` streams, and
    no more at a time than the prober's own ``max_concurrent_probes``. The
    prober's timeout and retry settings apply because this calls the
    prober's own single-stream probe rather than reimplementing one.
    """
    from stream_prober import ensure_prober

    try:
        prober = ensure_prober()
    except Exception as e:
        logger.warning(
            "[EVENT-SYNC] stream prober unavailable (%s) — promotion "
            "candidates keep their current health verdict", e,
        )
        return set()
    if prober is None:
        logger.info(
            "[EVENT-SYNC] no stream prober configured — %d promotion "
            "candidate(s) with no health record are treated as working",
            len(stream_ids),
        )
        return set()

    held_back = 0
    if len(stream_ids) > MAX_HEALTH_PROBES_PER_RUN:
        held_back = len(stream_ids) - MAX_HEALTH_PROBES_PER_RUN
        stream_ids = stream_ids[:MAX_HEALTH_PROBES_PER_RUN]
        logger.warning(
            "[EVENT-SYNC] promotion health check capped at %d probe(s) this "
            "run — %d candidate stream(s) keep no health verdict and will "
            "be probed by a later run",
            MAX_HEALTH_PROBES_PER_RUN, held_back,
        )

    urls = await _probe_urls(client, stream_ids)
    if not urls:
        return set()

    dead: set[int] = set()
    failures_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(max(1, prober.max_concurrent_probes))

    async def _probe_one(stream_id: int, url: str, name: str) -> None:
        async with semaphore:
            try:
                result = await prober.probe_stream(stream_id, url, name)
            except Exception as e:
                logger.warning(
                    "[EVENT-SYNC] health probe of stream %s raised (%s) — "
                    "treating it as working", stream_id, e,
                )
                return
            status = (result or {}).get("probe_status")
            if status in _FAILED_PROBE_STATUSES:
                async with failures_lock:
                    dead.add(stream_id)

    await asyncio.gather(*[
        _probe_one(sid, url, name) for sid, (url, name) in urls.items()
    ])
    logger.info(
        "[EVENT-SYNC] promotion health check probed %d candidate stream(s), "
        "%d did not answer", len(urls), len(dead),
    )
    return dead


async def _probe_urls(client, stream_ids: list[int]) -> dict[int, tuple]:
    """Playback url and name per stream id, for the ones that have a url.

    A stream the provider no longer lists, or one with no url at all, is
    left out rather than reported dead: it was never probed, so there is no
    verdict to report.
    """
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
            if stream_id is None or not url:
                continue
            urls[stream_id] = (url, stream.get("name") or f"Stream {stream_id}")
    return urls
