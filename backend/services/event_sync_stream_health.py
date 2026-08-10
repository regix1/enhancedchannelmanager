"""Which of an Event Sync promotion's candidate streams do not work.

Promotion turns an unmatched provider stream into a channel, and a provider
that lists an event it cannot actually serve used to get a channel out of it
just the same. This module answers the one question that stops that: of the
streams a plan is about to turn into channels, which ones are dead?

One helper, two call sites — ``channel_pipeline_executor`` for a live run and
``routers.channel_pipeline`` for the preview — so the verdict an operator
reads in the preview is computed by the same code the run obeys.

**It reads health, it does not define it.** A stream counts as dead when

* it has already struck out (``consecutive_failures`` at or past the
  configured ``strike_threshold``) — the same signal auto-creation's
  ``skip_struck_streams`` uses, so ECM has one idea of a broken stream, not
  two; or
* this call probed it just now and the probe did not succeed.

A single old failure is NOT dead. Streams fail transiently all the time, and
the strike threshold is exactly the setting an operator already tuned to say
how much failure is too much.

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

logger = logging.getLogger(__name__)

__all__ = [
    "MAX_HEALTH_PROBES_PER_RUN",
    "find_dead_streams",
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

    Never raises. An empty set means "no stream was shown to be dead",
    which is also what every failure path returns.
    """
    ids = sorted({sid for sid in stream_ids if sid is not None})
    if not ids:
        return set()

    try:
        stats = await _load_stats(ids)
    except Exception as e:
        logger.warning(
            "[EVENT-SYNC] stream health lookup failed (%s) — no stream "
            "treated as dead this run", e,
        )
        return set()

    threshold = _strike_threshold()
    dead = {
        sid for sid in ids
        if _is_struck(stats.get(sid), threshold)
    }

    if probe_missing and client is not None:
        unprobed = [sid for sid in ids if sid not in stats]
        if unprobed:
            dead |= await _probe_and_collect_failures(client, unprobed)

    return dead


async def _load_stats(stream_ids: list[int]) -> dict[int, dict]:
    """Health records for these stream ids, keyed by stream id."""
    from fastapi.concurrency import run_in_threadpool
    from stream_prober import StreamProber

    return await run_in_threadpool(
        StreamProber.get_stats_by_stream_ids, stream_ids
    )


def _strike_threshold() -> int:
    """How many consecutive failures make a stream struck out, or 0 = off."""
    try:
        from config import get_settings

        return int(get_settings().strike_threshold or 0)
    except Exception as e:
        logger.warning(
            "[EVENT-SYNC] strike threshold unreadable (%s) — falling back "
            "to the fresh probe verdict alone", e,
        )
        return 0


def _is_struck(stat: dict | None, threshold: int) -> bool:
    """Has this stream failed often enough in a row to count as struck out?"""
    if stat is None or threshold <= 0:
        return False
    return int(stat.get("consecutive_failures") or 0) >= threshold


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
