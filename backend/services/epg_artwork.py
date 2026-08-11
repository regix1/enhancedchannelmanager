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

**Sports matchups are a second job, because repointing cannot reach them.**
Gracenote publishes no per-game art for a league: measured against the live
feed, 39% of College Football airings carry no icon at all and the rest all
share ONE series image, so a guide shows the same picture for every game.
There is nothing to repoint in either case. Given a game-thumbs base URL,
a programme whose title names a known league and whose sub-title reads
"<away> at <home>" instead gets a banner built from those two teams. Without
that URL the pass does not run and the feed comes through as it always did.
"""
import asyncio
import codecs
import json
import logging
import re
import zlib
from html import unescape
from xml.sax.saxutils import escape

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
_BASE = r"(?P<base>https?://[\w.-]*tmsimg\.com/assets/)"

_ICON = re.compile(
    _BASE +
    r"(?P<asset>p\d+)_(?P<kind>[a-z]{1,3})_[hv]\d+_(?P<suffix>[a-z]{2})\.jpg"
    r"(?:\?[^\"'\s<>]*)?"
)

# Where a match can BEGIN, which is what a chunk boundary has to back onto.
# The literal "http" is not that: it also occurs inside a query carrying a
# percent-encoded URL of its own (?u=http%3A%2F%2F...), and a boundary there
# sits INSIDE the match that query belongs to. The emitted half still matches,
# because the query group is optional, so the URL is rewritten without its
# query and the encoded remainder comes back glued onto it. [54]
_ICON_START = re.compile(_BASE)

# Longest text _ICON can span. A chunk boundary inside a URL would otherwise
# hide it from both this pass and the next, so the tail is backed onto the
# start of any match reaching this far, holding at most twice this much.
_MAX_MATCH = 512

_PROBE_CONCURRENCY = 16

# Programme title -> the league segment game-thumbs knows it by, tried in
# order. The title is matched on a PATTERN rather than in full because a
# postseason airing is titled for the bowl and never for the league ("CFP
# Semifinal at the VRBO Fiesta Bowl"), as are the archive strands ("Hardwood
# Classics", "Super Bowl Classics"). Matching in full left those showing the
# landscape art this module exists to get rid of.
#
# It stays an ALLOWLIST. A title no pattern claims keeps the artwork the feed
# gave it, which is what stops "Divorce Court" — sub-title "No Boundaries and
# Many Betrayals: Michelle vs. Alonzo" — from reading as a matchup and being
# handed a football banner. Under ``fallback=true`` a wrong league still
# renders something plausible, so a false positive here is silent.
#
# WNBA precedes NBA and Minor League precedes MLB so the narrower name wins.
MATCHUP_LEAGUES = (
    (re.compile(r"\bWNBA\b", re.I), "wnba"),
    (re.compile(r"\bNBA\b|Hardwood Classics", re.I), "nba"),
    (re.compile(r"\bNFL\b|Super Bowl", re.I), "nfl"),
    (re.compile(r"\bCFL\b", re.I), "cfl"),
    (re.compile(r"College Football|\bCFP\b", re.I), "ncaaf"),
    (re.compile(r"College Basketball", re.I), "ncaab"),
    (re.compile(r"Minor League Baseball", re.I), "milb"),
    (re.compile(r"\bMLB\b", re.I), "mlb"),
    (re.compile(r"\bNHL\b", re.I), "nhl"),
)

_PROGRAMME_END = "</programme>"
_PROGRAMME = re.compile(r"<programme\b[^>]*>.*?</programme>", re.DOTALL)
_TITLE = re.compile(r"<title\b[^>]*>(.*?)</title>", re.DOTALL)
_SUB_TITLE = re.compile(r"<sub-title\b[^>]*>(.*?)</sub-title>", re.DOTALL)
_ICON_EL = re.compile(r"<icon\b[^>]*/>|<icon\b[^>]*>.*?</icon>", re.DOTALL)

# Gracenote writes the visiting side first, in both the "at" and the "vs."
# spellings, which is the order game-thumbs takes its two team segments in.
_MATCHUP = re.compile(r"^(?P<away>.+?)\s+(?:at|vs\.?)\s+(?P<home>.+)$", re.I)

# Consecutive assets the CDN answered for on NO code before a run stops
# probing. Every one of those already failed on all five codes, so a run of
# them is the host being down rather than assets being unusual, and the ones
# behind them cost five timeouts each for answers the next refresh has to
# fetch anyway. A single answer resets it, so a few unlucky assets partway
# through a healthy run do not end it. [55]
_OUTAGE_STREAK = 3


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


def compile_leagues(rules: list[dict] | None) -> tuple[tuple[re.Pattern, str], ...]:
    """Operator league rules as ordered (pattern, league) pairs.

    ``None`` means they were never configured, so the built-ins apply. An
    empty list is a deliberate "no leagues" and is honoured as one.

    A row whose pattern will not compile is dropped rather than raised: the
    guide is served through this path, so one bad row must cost its own rule
    and not the whole feed.
    """
    if rules is None:
        return MATCHUP_LEAGUES
    compiled = []
    for rule in rules:
        match = str(rule.get("match") or "").strip()
        league = str(rule.get("league") or "").strip()
        if not match or not league:
            continue
        try:
            compiled.append((re.compile(match, re.I), league))
        except re.error as e:
            logger.warning("[EPG-ART] Dropping league rule %r: %s", match, e)
    return tuple(compiled)


def _slug(team: str) -> str:
    """Team name as the path segment game-thumbs matches on.

    The qualifier an archive or tournament airing puts in front of the
    matchup lands on the away side of the split ("2008 NBA Finals: Boston vs.
    Los Angeles", "2025 Big Ten: Indiana at Penn St."). game-thumbs matches on
    the team alone, so anything ahead of the last colon is dropped.
    """
    name = unescape(team).strip()
    name = name.rpartition(":")[2].strip() or name
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def matchup_banner(base: str, title: str, sub_title: str,
                   leagues: tuple[tuple[re.Pattern, str], ...] = MATCHUP_LEAGUES) -> str | None:
    """game-thumbs URL for one matchup, or None when it is not a matchup.

    ``title`` names the league and ``sub_title`` the two teams; both arrive
    still XML-escaped, as they sit in the feed.
    """
    plain_title = unescape(title)
    league = next(
        (code for pattern, code in leagues if pattern.search(plain_title)),
        None,
    )
    if league is None:
        return None
    teams = _MATCHUP.match(unescape(sub_title).strip())
    if teams is None:
        return None
    away, home = _slug(teams.group("away")), _slug(teams.group("home"))
    if not away or not home:
        return None
    # fallback=true is not optional: a team game-thumbs cannot resolve answers
    # 400 with a JSON body without it, and the guide draws that as a broken
    # image. With it, an unresolved side still yields a usable banner.
    return (f"{base}/{league}/{away}/{home}/cover"
            f"?style=4&logo=true&fallback=true")


class ArtworkRewriter:
    """Repoints icons across a stream of text without holding it all.

    Feed it decoded text in any chunking; it returns the rewritten text and
    withholds a short tail that a match might still be spanning. Call
    ``finish`` to get that tail once the stream ends. Assets the cache cannot
    answer for are recorded in ``unknown`` for the caller to probe later.
    """

    def __init__(self, cache: ArtworkCache, banner_base: str = "",
                 leagues: tuple[tuple[re.Pattern, str], ...] = MATCHUP_LEAGUES):
        self.cache = cache
        self.banner_base = banner_base.rstrip("/")
        self.leagues = leagues
        self.unknown: dict[str, tuple[str, str, str, str]] = {}
        self.rewritten = 0
        self.bannered = 0
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

    def _banner(self, m: re.Match) -> str:
        """Give one programme its matchup banner, if it is a matchup."""
        prog = m.group(0)
        title = _TITLE.search(prog)
        sub_title = _SUB_TITLE.search(prog)
        if title is None or sub_title is None:
            return prog
        url = matchup_banner(self.banner_base, title.group(1), sub_title.group(1),
                             self.leagues)
        if url is None:
            return prog
        self.bannered += 1
        # The URL carries no quote to escape, only the & joining its query.
        icon = '<icon src="%s" />' % escape(url)
        # Replacing in place keeps the icon where the feed had it, which is
        # where a reader expecting XMLTV's child order looks for it. Only a
        # programme that carried no icon needs one appended.
        if _ICON_EL.search(prog):
            return _ICON_EL.sub(lambda _: icon, prog, count=1)
        return f"{prog[:-len(_PROGRAMME_END)]}{icon}{_PROGRAMME_END}"

    def _render(self, text: str) -> str:
        """Both passes, in the order that lets the banner win.

        The banner goes first so a matchup's own icon is already gone by the
        time the repoint runs; what the repoint then sees is the artwork of
        everything that is not a matchup.
        """
        if self.banner_base:
            text = _PROGRAMME.sub(self._banner, text)
        return _ICON.sub(self._sub, text)

    def feed(self, text: str) -> str:
        buf = self._tail + text
        if len(buf) <= _MAX_MATCH:
            self._tail = buf
            return ""
        if self.banner_base:
            # Cut just past the last closed programme so the banner pass only
            # ever sees whole ones. Nothing is lost while the header and the
            # channel list stream by with no programme to close: that falls
            # through to the icon boundary below, and a programme cannot have
            # started yet for the banner pass to miss.
            end = buf.rfind(_PROGRAMME_END)
            if end >= 0:
                cut = end + len(_PROGRAMME_END)
                self._tail = buf[cut:]
                return self._render(buf[:cut])
        # Cut where a match cannot straddle. Splitting at a fixed offset put
        # the HEAD of a URL in the emitted half and its tail in the carried
        # half, so the match was gone from both — the icon passed through
        # untouched with nothing to show why. Backing up to the last URL start
        # near the end keeps any partial match whole in the tail.
        #
        # The window reaches back two _MAX_MATCH, not one: a URL that began
        # earlier still spans the boundary, and its start sits outside the
        # last _MAX_MATCH by construction. One search over that window is
        # enough, because the last start in it is also the last start in any
        # suffix of it. [52]
        cut = -1
        for start in _ICON_START.finditer(buf, max(0, len(buf) - 2 * _MAX_MATCH)):
            cut = start.start()
        if cut < 0:
            # No icon can begin within reach, so none can be spanning the
            # boundary: one that began before the window ends before this
            # offset, since _MAX_MATCH bounds how far a match reaches.
            cut = len(buf) - _MAX_MATCH
        self._tail = buf[cut:]
        return _ICON.sub(self._sub, buf[:cut])

    def finish(self) -> str:
        out = self._render(self._tail)
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
    """First vertical code whose rendition exists for this asset, else None.

    Raises the last transport error when NOT ONE code answered, so the caller
    can tell an unreachable CDN from an asset that has no portrait. [35]
    """
    last_error: httpx.HTTPError | None = None
    answered = 0
    for code in VERTICAL_CODES:
        url = f"{base}{asset}_{kind}_{code}_{suffix}.jpg"
        try:
            r = await client.head(url, follow_redirects=True)
            if r.status_code == 405:  # server refuses HEAD, fall back to GET
                r = await client.get(url, follow_redirects=True)
            if r.status_code == 200:
                return code
        except httpx.HTTPError as e:
            # A timeout on one code says nothing about the rest, and the None
            # this would return gets cached as a real "no portrait" forever.
            last_error = e
            continue
        answered += 1
    if not answered:
        raise last_error
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
    unreachable = 0
    async with httpx.AsyncClient(timeout=10.0) as client:
        async def one(key: str) -> bool:
            """False when the CDN answered for no code at all."""
            nonlocal unreachable
            base, asset, kind, suffix = unknown[key]
            async with sem:
                # Read while holding a slot, not before it: every asset is
                # scheduled at once, so a check outside the semaphore would
                # run on all of them before the first probe had answered.
                if unreachable >= _OUTAGE_STREAK:
                    return False
                try:
                    code = await _probe(client, base, asset, kind, suffix)
                except httpx.HTTPError:
                    # An outage lasting this probe window would otherwise
                    # record every asset it touched as having no portrait,
                    # kept forever. Leave the key out and re-probe next
                    # refresh.
                    unreachable += 1
                    return False
                unreachable = 0
            cache.put(key, code)
            return True

        probed = await asyncio.gather(*(one(k) for k in unknown))
    cache.save()
    resolved = sum(1 for k in unknown if cache.get(k)[1])
    logger.info(
        "[EPG-ART] Probed %s new assets, %s have a portrait rendition, "
        "%s unreachable and left for the next refresh",
        len(unknown), resolved, probed.count(False),
    )
    return resolved
