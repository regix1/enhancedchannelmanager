"""Point Gracenote programme artwork at its portrait variant.

Gracenote publishes one asset in several aspect ratios, and the upstream
XMLTV feeds reference a landscape code (h8/h9). A guide client that renders
programme tiles in portrait center-crops those, cutting the title art in
half. The portrait rendition of the SAME asset is already published under a
different aspect code, so the fix is to repoint the icon rather than to find
different artwork.

Probe order comes from measuring the live guide: of 30 sampled assets every
one had a portrait rendition, all under the SAME ``aa``/``ab`` suffix as the
landscape URL, and ``v12`` was the first hit for 27 of them. The probe holds
the suffix fixed and walks the codes in that order.

**Rewriting is streamed, and probing never happens inline.** Holding a whole
feed in memory worked for a 53MB locals file and timed out at 900s on a
grace-note feed several times that size, which is also long enough for the
guide client's own fetch to give up. So a response rewrites only what the
cache already knows and streams straight through, while assets it has not
seen are probed afterwards in the background. A first fetch of a new source
therefore serves landscape and the next one serves portrait.

A rewrite is only emitted for a rendition that answered 200, so an asset
with no portrait keeps its landscape URL: the failure mode is the crop that
is showing today, never a broken image.
"""
import asyncio
import codecs
import json
import logging
import re
import zlib

import httpx

logger = logging.getLogger(__name__)

# Ordered by how often each answered first when sampled against the live feed.
VERTICAL_CODES = ("v12", "v13", "v7", "v11", "v8")

# http://dtil.tmsimg.com/assets/p30177490_b_h8_ab.jpg?w=960&h=540
#
# The segment after the asset id is the image KIND (``b`` for programme art,
# ``st`` for station art). A kind's portrait rendition lives under that same
# kind, so it is carried through rather than assumed to be ``b``.
#
# The trailing query is part of the match so the rewrite DROPS it. The feed
# pins the landscape geometry there (w=960&h=540); carrying it onto the
# portrait rendition would hand the client a 16:9 crop of the portrait and
# undo the repoint.
_ICON = re.compile(
    r"(?P<base>https?://[\w.-]*tmsimg\.com/assets/)"
    r"(?P<asset>p\d+)_(?P<kind>[a-z]{1,3})_[hv]\d+_(?P<suffix>[a-z]{2})\.jpg"
    r"(?:\?[^\"'\s<>]*)?"
)

# Longest text _ICON can span. A chunk boundary inside a URL would otherwise
# hide it from both this pass and the next, so this much tail is held back
# until more text arrives.
_MAX_MATCH = 512

_PROBE_CONCURRENCY = 16


class ArtworkCache:
    """asset+kind+suffix -> portrait code, or None when it has no portrait.

    Persisted so a restart does not re-probe. A cached None is a real answer
    and is kept: it stops an asset with no portrait rendition from being
    probed again on every refresh.
    """

    def __init__(self, path):
        self.path = path
        self._map: dict[str, str | None] = {}
        self._dirty = False
        try:
            if path.exists():
                self._map = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            # A corrupt cache is a slow run, not a broken one — probes refill it.
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


class ArtworkRewriter:
    """Repoints icons across a stream of text without holding it all.

    Feed it decoded text in any chunking; it returns the rewritten text and
    withholds a short tail that a match might still be spanning. Call
    ``finish`` to get that tail once the stream ends. Assets the cache cannot
    answer for are recorded in ``unknown`` for the caller to probe later.
    """

    def __init__(self, cache: ArtworkCache):
        self.cache = cache
        self.unknown: dict[str, tuple[str, str, str, str]] = {}
        self.rewritten = 0
        self._tail = ""

    def _sub(self, m: re.Match) -> str:
        key = f"{m.group('asset')}_{m.group('kind')}_{m.group('suffix')}"
        known, code = self.cache.get(key)
        if not known:
            self.unknown.setdefault(key, (
                m.group("base"), m.group("asset"),
                m.group("kind"), m.group("suffix"),
            ))
        if not known or code is None:
            return m.group(0)
        self.rewritten += 1
        return (f"{m.group('base')}{m.group('asset')}_{m.group('kind')}"
                f"_{code}_{m.group('suffix')}.jpg")

    def feed(self, text: str) -> str:
        buf = self._tail + text
        if len(buf) <= _MAX_MATCH:
            self._tail = buf
            return ""
        # Cut where a match cannot straddle. Splitting at a fixed offset put
        # the HEAD of a URL in the emitted half and its tail in the carried
        # half, so the match was gone from both — the icon passed through
        # untouched with nothing to show why. Backing up to the last URL start
        # near the end keeps any partial match whole in the tail.
        cut = buf.rfind("http", max(0, len(buf) - _MAX_MATCH))
        if cut <= 0:
            cut = len(buf) - _MAX_MATCH
        self._tail = buf[cut:]
        return _ICON.sub(self._sub, buf[:cut])

    def finish(self) -> str:
        out = _ICON.sub(self._sub, self._tail)
        self._tail = ""
        return out


async def stream_rewritten(url: str, cache: ArtworkCache, rewriter: ArtworkRewriter):
    """Yield the upstream feed with its artwork repointed, a chunk at a time."""
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    gunzip = None
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=300.0),
                                 follow_redirects=True) as client:
        async with client.stream("GET", url) as upstream:
            upstream.raise_for_status()
            gzipped = (
                url.endswith(".gz")
                or upstream.headers.get("content-encoding") == "gzip"
            )
            async for raw in upstream.aiter_bytes(1 << 20):
                if gzipped:
                    if gunzip is None:
                        gunzip = zlib.decompressobj(zlib.MAX_WBITS | 16)
                    try:
                        raw = gunzip.decompress(raw)
                    except zlib.error:
                        # Not actually gzipped despite the extension/header —
                        # pass the bytes through as they are.
                        gzipped = False
                if raw:
                    out = rewriter.feed(decoder.decode(raw))
                    if out:
                        yield out.encode("utf-8")
    out = rewriter.feed(decoder.decode(b"", True)) + rewriter.finish()
    if out:
        yield out.encode("utf-8")


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


async def probe_unknown(cache: ArtworkCache,
                        unknown: dict[str, tuple[str, str, str, str]]) -> int:
    """Resolve assets the cache could not answer for, then persist.

    Runs after the response has been served, so a slow probe delays nothing —
    it only decides whether the NEXT fetch of this source can repoint them.
    """
    if not unknown:
        return 0
    sem = asyncio.Semaphore(_PROBE_CONCURRENCY)
    async with httpx.AsyncClient(timeout=10.0) as client:
        async def one(key: str) -> None:
            base, asset, kind, suffix = unknown[key]
            async with sem:
                cache.put(key, await _probe(client, base, asset, kind, suffix))

        await asyncio.gather(*(one(k) for k in unknown))
    cache.save()
    resolved = sum(1 for k in unknown if cache.get(k)[1])
    logger.info(
        "[EPG-ART] Probed %s new assets, %s have a portrait rendition",
        len(unknown), resolved,
    )
    return resolved
