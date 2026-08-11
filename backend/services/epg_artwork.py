"""Point Gracenote programme artwork at its portrait variant.

Gracenote publishes one asset in several aspect ratios, and the upstream
XMLTV feeds reference a landscape code (h8/h9). A guide client that renders
programme tiles in portrait center-crops those, cutting the title art in
half. The portrait rendition of the SAME asset is already published under a
different aspect code, so the fix is to repoint the icon rather than to find
different artwork.

Probe order comes from measuring the live guide before this was written: of
30 sampled assets every one had a portrait rendition, all under the SAME
``aa``/``ab`` suffix as the landscape URL, and ``v12`` was the first hit for
27 of them. The probe therefore holds the suffix fixed and walks the codes
in that order, which costs about 1.1 requests per asset.

A rewrite is only emitted for a rendition that answered 200. An asset whose
probe misses, errors, or falls outside the run's budget keeps its landscape
URL, so the failure mode is the cropped image that is showing today rather
than a broken one.
"""
import asyncio
import json
import logging
import re
import time
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

# Ordered by how often each answered first when sampled against the live feed.
VERTICAL_CODES = ("v12", "v13", "v7", "v11", "v8")

# http://dtil.tmsimg.com/assets/p30177490_b_h8_ab.jpg?w=960&h=540
#
# The trailing query is part of the match so the rewrite DROPS it. The feed
# pins the landscape geometry there (w=960&h=540); carrying it onto the
# portrait rendition would hand the client a 16:9 crop of the portrait and
# undo the repoint.
#
# The segment after the asset id is the image KIND (``b`` for programme art,
# ``st`` for station art). A kind's portrait rendition lives under that same
# kind, so it is carried through the rewrite rather than assumed to be ``b``.
_ICON = re.compile(
    r"(?P<base>https?://[\w.-]*tmsimg\.com/assets/)"
    r"(?P<asset>p\d+)_(?P<kind>[a-z]{1,3})_[hv]\d+_(?P<suffix>[a-z]{2})\.jpg"
    r"(?:\?[^\"'\s<>]*)?"
)

_PROBE_CONCURRENCY = 16


class ArtworkCache:
    """asset+suffix -> portrait code, or None when the asset has no portrait.

    Persisted so a restart does not re-probe every asset. A cached None is a
    real answer and is kept: it stops an asset with no portrait rendition
    from being probed again on every refresh.
    """

    def __init__(self, path: Path):
        self.path = path
        self._map: dict[str, str | None] = {}
        self._dirty = False
        try:
            if path.exists():
                self._map = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            # A corrupt cache is a slow run, not a broken one — the probes
            # simply repopulate it.
            logger.warning("[EPG-ART] Could not read cache %s: %s", path, e)

    def get(self, key: str) -> tuple[bool, str | None]:
        """Return (known, code). ``known`` separates a cached None from a miss."""
        if key in self._map:
            return True, self._map[key]
        return False, None

    def put(self, key: str, code: str | None) -> None:
        self._map[key] = code
        self._dirty = True

    def save(self) -> None:
        if not self._dirty:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps(self._map), encoding="utf-8")
            self._dirty = False
        except OSError as e:
            logger.warning("[EPG-ART] Could not write cache %s: %s", self.path, e)


async def _probe(client: httpx.AsyncClient, base: str, asset: str,
                 kind: str, suffix: str) -> str | None:
    """First vertical code whose rendition exists for this asset, else None."""
    for code in VERTICAL_CODES:
        url = f"{base}{asset}_{kind}_{code}_{suffix}.jpg"
        try:
            r = await client.head(url, follow_redirects=True)
            if r.status_code == 405:  # server refuses HEAD, fall back to GET
                r = await client.get(url, follow_redirects=True)
            if r.status_code == 200:
                return code
        except httpx.HTTPError:
            return None
    return None


async def rewrite_artwork(xml: str, cache: ArtworkCache,
                          budget_seconds: float = 60.0) -> tuple[str, dict]:
    """Repoint every landscape Gracenote icon at its portrait rendition.

    Assets already in ``cache`` cost nothing. Unknown ones are probed until
    ``budget_seconds`` is spent; whatever is still unknown after that keeps
    its landscape URL and gets resolved on a later run, so a first pass over
    a very large feed converges instead of blocking for minutes.
    """
    wanted: dict[str, tuple[str, str, str, str]] = {}
    for m in _ICON.finditer(xml):
        key = f"{m.group('asset')}_{m.group('kind')}_{m.group('suffix')}"
        wanted.setdefault(key, (m.group("base"), m.group("asset"),
                                m.group("kind"), m.group("suffix")))

    unknown = [k for k in wanted if not cache.get(k)[0]]
    probed = 0
    if unknown:
        started = time.monotonic()
        sem = asyncio.Semaphore(_PROBE_CONCURRENCY)
        async with httpx.AsyncClient(timeout=10.0) as client:
            async def one(key: str) -> None:
                nonlocal probed
                if time.monotonic() - started > budget_seconds:
                    return
                base, asset, kind, suffix = wanted[key]
                async with sem:
                    cache.put(key, await _probe(client, base, asset, kind, suffix))
                probed += 1

            await asyncio.gather(*(one(k) for k in unknown))
        cache.save()

    rewritten = 0

    def sub(m: re.Match) -> str:
        nonlocal rewritten
        key = f"{m.group('asset')}_{m.group('kind')}_{m.group('suffix')}"
        known, code = cache.get(key)
        if not known or code is None:
            return m.group(0)
        rewritten += 1
        return (f"{m.group('base')}{m.group('asset')}_{m.group('kind')}"
                f"_{code}_{m.group('suffix')}.jpg")

    out = _ICON.sub(sub, xml)
    stats = {
        "assets": len(wanted),
        "probed": probed,
        "rewritten": rewritten,
        "unresolved": len(wanted) - sum(
            1 for k in wanted if cache.get(k)[0] and cache.get(k)[1]
        ),
    }
    logger.info(
        "[EPG-ART] %s assets, %s probed, %s icons repointed to portrait",
        stats["assets"], stats["probed"], stats["rewritten"],
    )
    return out, stats
