"""Repointing Gracenote programme artwork to its portrait rendition.

The upstream feeds reference landscape codes (h8/h9), which a guide client
rendering portrait tiles center-crops. These pin what keeps the rewrite safe:
only a rendition confirmed to exist is emitted, anything unresolved keeps the
landscape URL it had, and a match split across a chunk boundary is still
found — the rewrite streams, so boundaries land mid-URL routinely.
"""
import httpx

from services.epg_artwork import (
    ArtworkCache,
    ArtworkRewriter,
    MATCHUP_LEAGUES,
    VERTICAL_CODES,
    compile_leagues,
    _MAX_MATCH,
    _OUTAGE_STREAK,
    _PROBE_CONCURRENCY,
    _probe,
    matchup_banner,
    probe_unknown,
)


ICON = "http://dtil.tmsimg.com/assets/p30177490_b_h8_ab.jpg?w=960&h=540"
XML = f'<programme><title>CIA</title><icon src="{ICON}" /></programme>'

BARE = "http://dtil.tmsimg.com/assets/p30177490_b_h8_ab.jpg"
PORTRAIT = "http://dtil.tmsimg.com/assets/p30177490_b_v12_ab.jpg"
SEED = {"p30177490_b_ab": "v12"}

GT = "http://thumbs.example:3100"
MATCHUP = (
    '<programme start="20260830190000 +0000" channel="sec.us">'
    '<title lang="en">College Football</title>'
    '<sub-title lang="en">Alabama at South Carolina</sub-title>'
    '<desc lang="en">The Tide visit the Gamecocks.</desc>'
    "</programme>"
)
BANNER = (f"{GT}/ncaaf/alabama/south-carolina/cover"
          "?style=4&amp;logo=true&amp;fallback=true")

# Bound at import, before any test can patch it. A test that streams twice
# would otherwise build its second stub on top of the first, and the first
# one's transport would win and replay the wrong body.
UPSTREAM_CLIENT = httpx.AsyncClient


def _cache(tmp_path, seed=None):
    c = ArtworkCache(tmp_path / "art.json")
    for k, v in (seed or {}).items():
        c.put(k, v)
    return c


def _run(rewriter, text, chunk=None):
    """Push text through the rewriter, optionally in fixed-size chunks."""
    if chunk is None:
        return rewriter.feed(text) + rewriter.finish()
    out = []
    for i in range(0, len(text), chunk):
        out.append(rewriter.feed(text[i:i + chunk]))
    out.append(rewriter.finish())
    return "".join(out)


async def _streamed(monkeypatch, rewriter, body):
    """Push text through the real streaming path against a stub upstream.

    Driving feed() by hand is a narrower path than production: the streaming
    caller feeds the decoder's flush before finish(), and that extra call
    re-cuts a tail the previous call had already backed onto a URL start.
    """
    from services import epg_artwork

    def _respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body.encode("utf-8"))

    class _Stub(UPSTREAM_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(_respond)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(epg_artwork.httpx, "AsyncClient", _Stub)
    chunks = [
        chunk async for chunk in epg_artwork.stream_rewritten(
            "http://feed/guide.xml", rewriter.cache, rewriter,
        )
    ]
    return b"".join(chunks).decode("utf-8")


class TestRewrite:
    def test_a_cached_asset_is_repointed_to_its_portrait_rendition(self, tmp_path):
        rw = ArtworkRewriter(_cache(tmp_path, {"p30177490_b_ab": "v12"}))
        out = _run(rw, XML)
        assert "p30177490_b_v12_ab.jpg" in out
        assert "h8" not in out
        assert rw.rewritten == 1

    def test_the_query_string_is_dropped(self, tmp_path):
        """The feed pins ?w=960&h=540. Carrying that onto the portrait
        rendition would hand the client a 16:9 crop of it."""
        out = _run(ArtworkRewriter(_cache(tmp_path, {"p30177490_b_ab": "v12"})), XML)
        assert "w=960" not in out and "h=540" not in out

    def test_an_asset_with_no_portrait_keeps_its_landscape_url(self, tmp_path):
        """A cached None is a real answer. Emitting a guessed URL would give
        the client a broken image, worse than the crop it shows today."""
        rw = ArtworkRewriter(_cache(tmp_path, {"p30177490_b_ab": None}))
        assert _run(rw, XML) == XML
        assert rw.rewritten == 0
        assert rw.unknown == {}

    def test_an_unseen_asset_passes_through_and_is_recorded_for_probing(self, tmp_path):
        """Probing never runs inline — that is what timed out on a large
        feed. The response serves landscape and the asset is queued so the
        NEXT fetch can repoint it."""
        rw = ArtworkRewriter(_cache(tmp_path))
        assert _run(rw, XML) == XML
        assert rw.rewritten == 0
        assert "p30177490_b_ab" in rw.unknown

    def test_the_suffix_and_kind_are_carried_not_guessed(self, tmp_path):
        """Portrait renditions live under the same aa/ab suffix and the same
        kind (``b`` programme art, ``st`` station art) as the landscape URL."""
        xml = '<icon src="http://dtil.tmsimg.com/assets/p30660000_st_h10_aa.jpg" />'
        out = _run(ArtworkRewriter(_cache(tmp_path, {"p30660000_st_aa": "v8"})), xml)
        assert "p30660000_st_v8_aa.jpg" in out
        assert "_b_" not in out

    def test_non_gracenote_artwork_is_left_alone(self, tmp_path):
        xml = '<icon src="http://172.16.1.73:3100/mlb/a/b/cover" />'
        rw = ArtworkRewriter(_cache(tmp_path))
        assert _run(rw, xml) == xml
        assert rw.unknown == {}

    def test_one_asset_shared_by_many_airings_is_queued_once(self, tmp_path):
        rw = ArtworkRewriter(_cache(tmp_path))
        _run(rw, XML * 5)
        assert len(rw.unknown) == 1


class TestChunkBoundaries:
    """The rewrite streams, so a URL routinely straddles two chunks. Missing
    those would silently leave icons landscape with nothing to show why.
    """

    def test_output_is_identical_at_every_chunk_size(self, tmp_path):
        whole = _run(ArtworkRewriter(_cache(tmp_path, {"p30177490_b_ab": "v12"})), XML)
        for size in (1, 2, 3, 7, 13, 64, 200):
            out = _run(
                ArtworkRewriter(_cache(tmp_path, {"p30177490_b_ab": "v12"})),
                XML, chunk=size,
            )
            assert out == whole, f"chunk size {size} changed the output"

    def test_a_split_inside_the_url_is_still_repointed(self, tmp_path):
        rw = ArtworkRewriter(_cache(tmp_path, {"p30177490_b_ab": "v12"}))
        cut = XML.index("_b_h8_")
        out = rw.feed(XML[:cut]) + rw.feed(XML[cut:]) + rw.finish()
        assert "p30177490_b_v12_ab.jpg" in out
        assert rw.rewritten == 1

    def test_nothing_is_dropped_or_duplicated_across_chunks(self, tmp_path):
        body = (XML + "<filler/>") * 20
        out = _run(ArtworkRewriter(_cache(tmp_path, {"p30177490_b_ab": "v12"})),
                   body, chunk=11)
        assert out.count("<filler/>") == 20
        assert out.count("p30177490_b_v12_ab.jpg") == 20


class TestWideGapsBetweenIcons:
    """A URL is only safe if its "http" is inside the window the cut searches.

    An icon far enough back from the end of the accumulated buffer has its
    "http" outside that window by construction, so the backup search finds
    nothing and the cut lands inside the URL. Icons placed close together
    hide this completely: any nearby "http" rescues the cut. These inputs put
    one icon on its own with more than _MAX_MATCH of icon-free text after it.
    """

    async def test_a_url_straddling_the_fallback_cut_is_still_repointed(
            self, tmp_path, monkeypatch):
        """The gap is what matters, not the chunk size. Here the URL ends 470
        characters before the buffer end, so nothing in the searched window
        starts a match and the fixed-offset cut splits it."""
        body = "a" * 380 + BARE + "b" * 470
        rw = ArtworkRewriter(_cache(tmp_path, SEED))

        out = await _streamed(monkeypatch, rw, body)

        assert out == "a" * 380 + PORTRAIT + "b" * 470
        assert rw.rewritten == 1

    async def test_a_cut_inside_the_query_does_not_orphan_it(
            self, tmp_path, monkeypatch):
        """The query group is optional, so a cut inside it still leaves a
        COMPLETE match in the emitted half. That half rewrites and drops the
        query, and the leftover comes back glued to the rewritten URL, which
        is the broken image the module promises can never happen."""
        query_url = f"{BARE}?w=960&amp;h=540"
        for offset in range(51, 67):
            # The closing quote ends the query, as it does in the feed. Sized
            # so the fixed-offset cut lands on that offset within the URL.
            after = '" ' + "b" * (offset + _MAX_MATCH - len(query_url) - 2)
            rw = ArtworkRewriter(_cache(tmp_path, SEED))

            out = await _streamed(monkeypatch, rw, "a" * 380 + query_url + after)

            assert out == "a" * 380 + PORTRAIT + after, f"cut at {offset}"
            assert "h=540" not in out, f"query fragment survived, cut at {offset}"

    async def test_a_url_whose_query_carries_another_url_is_not_split(
            self, tmp_path, monkeypatch):
        """A query can hold a percent-encoded URL of its own, so "http"
        occurs INSIDE a match as well as at its start. A boundary backed onto
        that inner one leaves a complete-looking match in the emitted half,
        which rewrites and drops the query, and the encoded remainder comes
        back glued to the rewritten URL."""
        query_url = f"{BARE}?u=http%3A%2F%2Fx.example%2Fa"
        for offset in range(len(query_url) + 1):
            # offset is where a fixed-offset cut lands inside the URL.
            after = '" ' + "b" * (offset + _MAX_MATCH - len(query_url) - 2)
            rw = ArtworkRewriter(_cache(tmp_path, SEED))

            out = await _streamed(monkeypatch, rw, "a" * 380 + query_url + after)

            assert out == "a" * 380 + PORTRAIT + after, f"cut at {offset}"
            assert "x.example" not in out, f"query fragment survived, cut at {offset}"

    def test_the_retained_tail_stays_bounded(self, tmp_path):
        """Backing the cut onto an earlier start reaches back one more
        _MAX_MATCH and no further, so what is held cannot grow with the feed.
        Reads _tail because that IS the held text."""
        body = "".join(f"{'b' * gap}{BARE}"
                       for gap in (0, 100, 461, 462, 511, 512, 700, 1500))
        body += "c" * 900

        worst = 0
        for size in (1, 7, 64, 511, 512, 513, 1024, 4096):
            rw = ArtworkRewriter(_cache(tmp_path, SEED))
            for i in range(0, len(body), size):
                rw.feed(body[i:i + size])
                worst = max(worst, len(rw._tail))
            rw.finish()

        assert worst <= 2 * _MAX_MATCH
        assert worst > _MAX_MATCH, "the wider reach was never exercised"


class TestMatchupBanner:
    """Sports matchups get a banner built from their two teams.

    Gracenote publishes no per-game art for these: every airing of a league
    carries the SAME series image, so a guide shows one picture for every
    game, and 39% of them carry no image at all. game-thumbs renders the
    matchup from the two team names the sub-title already holds.
    """

    def test_a_matchup_with_no_artwork_is_given_a_banner(self, tmp_path):
        rw = ArtworkRewriter(_cache(tmp_path), banner_base=GT)
        out = _run(rw, MATCHUP)
        assert f'<icon src="{BANNER}" />' in out
        assert rw.bannered == 1

    def test_a_recycled_series_image_is_replaced_where_it_stood(self, tmp_path):
        """In place, not appended: a reader that honours XMLTV's child order
        looks for the icon where the feed put it."""
        with_icon = MATCHUP.replace(
            "<desc", f'<icon src="{BARE}" /><desc')
        out = _run(ArtworkRewriter(_cache(tmp_path, SEED), banner_base=GT),
                   with_icon)
        assert out.index(BANNER) < out.index("<desc")
        assert BARE not in out and PORTRAIT not in out
        assert out.count("<icon") == 1

    def test_artwork_outside_a_matchup_is_still_repointed(self, tmp_path):
        """The banner pass must not cost the portrait repoint its job."""
        out = _run(ArtworkRewriter(_cache(tmp_path, SEED), banner_base=GT),
                   MATCHUP + XML)
        assert BANNER in out
        assert PORTRAIT in out

    def test_a_league_game_thumbs_does_not_know_is_left_alone(self, tmp_path):
        """An unreal league segment answers 400 even with fallback=true, and
        a 400 draws as a broken image — worse than the generic art."""
        rw = ArtworkRewriter(_cache(tmp_path), banner_base=GT)
        out = _run(rw, MATCHUP.replace("College Football", "Cycling"))
        assert "<icon" not in out
        assert rw.bannered == 0

    def test_a_programme_with_no_matchup_is_left_alone(self, tmp_path):
        rw = ArtworkRewriter(_cache(tmp_path), banner_base=GT)
        out = _run(rw, MATCHUP.replace(
            "<sub-title lang=\"en\">Alabama at South Carolina</sub-title>",
            "<sub-title lang=\"en\">Week 3 Preview</sub-title>"))
        assert "<icon" not in out
        assert rw.bannered == 0

    def test_no_base_url_leaves_the_feed_as_it_was(self, tmp_path):
        """The default: an operator with no game-thumbs sees today's guide."""
        rw = ArtworkRewriter(_cache(tmp_path))
        assert _run(rw, MATCHUP) == MATCHUP
        assert rw.bannered == 0

    def test_the_query_separator_is_escaped(self, tmp_path):
        """A bare & in an attribute makes the feed not parse at all."""
        out = _run(ArtworkRewriter(_cache(tmp_path), banner_base=GT), MATCHUP)
        assert "&amp;logo=true&amp;fallback=true" in out
        assert "?style=4&l" not in out

    def test_output_is_identical_at_every_chunk_size(self, tmp_path):
        """A programme split across chunks still gets its banner: the cut
        lands past a closed programme, so the pass never sees half of one."""
        body = f"<tv>{MATCHUP}{XML}{MATCHUP}</tv>"
        whole = _run(ArtworkRewriter(_cache(tmp_path, SEED), banner_base=GT),
                     body)
        assert whole.count(BANNER) == 2
        for size in (1, 7, 64, 200, 511, 512, 513, 4096):
            assert _run(
                ArtworkRewriter(_cache(tmp_path, SEED), banner_base=GT),
                body, size,
            ) == whole, f"chunk size {size} diverged"

    async def test_it_survives_the_real_streaming_path(self, monkeypatch,
                                                       tmp_path):
        rw = ArtworkRewriter(_cache(tmp_path, SEED), banner_base=GT)
        out = await _streamed(monkeypatch, rw, f"<tv>{MATCHUP}{XML}</tv>")
        assert BANNER in out and PORTRAIT in out

    def test_a_team_name_is_slugged_and_unescaped(self):
        """Team names arrive XML-escaped and full of punctuation."""
        url = matchup_banner(GT, "College Football",
                             "Texas A&amp;M at Ohio State")
        assert url == (f"{GT}/ncaaf/texas-a-m/ohio-state/cover"
                       "?style=4&logo=true&fallback=true")

    def test_the_vs_spelling_reads_the_same_way_round(self):
        """Gracenote writes the visiting side first in both spellings."""
        assert matchup_banner(GT, "NHL Hockey", "Rangers vs. Bruins") == (
            f"{GT}/nhl/rangers/bruins/cover?style=4&logo=true&fallback=true")

    def test_a_trailing_slash_on_the_base_is_not_doubled(self, tmp_path):
        rw = ArtworkRewriter(_cache(tmp_path), banner_base=GT + "/")
        assert "//ncaaf" not in _run(rw, MATCHUP)


class TestWhichProgrammesCount:
    """Which titles are read as a matchup, and which must never be.

    The title is matched on a pattern because a postseason airing is titled
    for the bowl and an archive strand for itself, never for the league. That
    is also what makes the guard load-bearing: 275 programmes in the live
    guide have a matchup-shaped sub-title and 108 of them are not team sport.
    """

    def test_a_bowl_game_is_recognised_by_its_round_not_its_league(self):
        """"CFP Semifinal at the VRBO Fiesta Bowl" never says "football"."""
        assert matchup_banner(GT, "CFP Semifinal at the VRBO Fiesta Bowl",
                              "Miami vs Ole Miss") == (
            f"{GT}/ncaaf/miami/ole-miss/cover?style=4&logo=true&fallback=true")

    def test_an_archive_strand_is_recognised(self):
        assert matchup_banner(GT, "Hardwood Classics",
                              "2008 NBA Finals: Boston vs. Los Angeles Lakers") == (
            f"{GT}/nba/boston/los-angeles-lakers/cover"
            "?style=4&logo=true&fallback=true")

    def test_a_qualifier_before_the_matchup_is_dropped(self):
        """The teams are what game-thumbs matches on, not the round."""
        url = matchup_banner(GT, "Greatest Games: College Football",
                             "2025 Big Ten: Indiana at Penn St.")
        assert url == (f"{GT}/ncaaf/indiana/penn-st/cover"
                       "?style=4&logo=true&fallback=true")

    def test_a_courtroom_show_is_not_a_matchup(self):
        """Divorce Court is real, airs a "X vs. Y" sub-title, and must not be
        handed a football banner. fallback=true means a wrong league still
        renders something plausible, so this cannot be caught downstream."""
        assert matchup_banner(GT, "Divorce Court",
                              "No Boundaries and Many Betrayals: Shetel vs. Alonzo") is None

    def test_a_narrower_league_name_wins_over_the_one_inside_it(self):
        """WNBA precedes NBA, Minor League precedes MLB."""
        assert "/wnba/" in matchup_banner(GT, "WNBA Basketball", "Aces at Liberty")
        assert "/milb/" in matchup_banner(GT, "Minor League Baseball",
                                          "Iowa Cubs at St. Paul Saints")


class TestLeagueRules:
    """The rules are operator-editable, and one bad row must not cost the feed."""

    def test_unset_rules_fall_back_to_the_built_ins(self):
        assert compile_leagues(None) is MATCHUP_LEAGUES

    def test_an_empty_list_is_a_deliberate_none_not_a_reset(self):
        """Deleting every row has to mean none, or a row can never be removed."""
        assert compile_leagues([]) == ()

    def test_operator_rules_replace_the_built_ins(self, tmp_path):
        leagues = compile_leagues([{"match": "Curling Night", "league": "curling"}])
        rw = ArtworkRewriter(_cache(tmp_path), banner_base=GT, leagues=leagues)
        out = _run(rw, MATCHUP.replace("College Football", "Curling Night"))
        assert f"{GT}/curling/alabama/south-carolina/cover" in out
        # the built-in College Football rule is gone
        assert matchup_banner(GT, "College Football", "A at B", leagues) is None

    def test_the_first_matching_rule_wins(self):
        leagues = compile_leagues([
            {"match": "Bowl", "league": "first"},
            {"match": "Rose Bowl", "league": "second"},
        ])
        assert "/first/" in matchup_banner(GT, "Rose Bowl", "A at B", leagues)

    def test_a_row_that_will_not_compile_is_dropped_and_the_rest_survive(self):
        """A typo'd bracket must cost its own rule, not the whole guide."""
        leagues = compile_leagues([
            {"match": "Foo[", "league": "broken"},
            {"match": "College Football", "league": "ncaaf"},
        ])
        assert len(leagues) == 1
        assert "/ncaaf/" in matchup_banner(GT, "College Football", "A at B", leagues)

    def test_a_row_missing_either_half_is_dropped(self):
        assert compile_leagues([{"match": "", "league": "x"},
                                {"match": "y", "league": ""},
                                {"match": "  ", "league": "  "}]) == ()


class TestProbe:
    async def test_a_network_error_on_one_code_tries_the_rest(self):
        """probe_unknown writes whatever this returns into the cache, where a
        None is a real "no portrait" that is never probed again. Abandoning
        the asset on one timeout records that permanently."""
        tried = []

        class _Cdn:
            async def head(self, url: str,
                           follow_redirects: bool = False) -> httpx.Response:
                tried.append(url)
                if "_v8_" in url:
                    return httpx.Response(200)
                raise httpx.ReadTimeout("upstream stalled")

        code = await _probe(_Cdn(), "http://dtil.tmsimg.com/assets/",
                            "p30177490", "b", "ab")

        assert code == "v8"
        assert len(tried) == len(VERTICAL_CODES)

    async def test_an_asset_the_cdn_never_answered_for_stays_uncached(
        self, tmp_path, monkeypatch
    ):
        """A cached None is kept forever, so writing one for an asset the CDN
        answered for on no code at all turns an outage lasting this probe
        window into a permanent "no portrait" on everything it touched."""
        class _Cdn:
            async def head(self, url: str,
                           follow_redirects: bool = False) -> httpx.Response:
                raise httpx.ConnectError("cdn unreachable")

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc) -> bool:
                return False

        monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _Cdn())
        cache = _cache(tmp_path)

        resolved = await probe_unknown(cache, {"p30177490_b_ab": (
            "http://dtil.tmsimg.com/assets/", "p30177490", "b", "ab",
        )})

        assert resolved == 0
        assert cache.get("p30177490_b_ab") == (False, None)

    async def test_a_host_answering_nothing_ends_the_run_early(
        self, tmp_path, monkeypatch
    ):
        """Every asset here already failed on all five codes, so continuing
        spends five timeouts apiece on a host that is answering nothing, for
        answers the next refresh has to fetch anyway."""
        asked = []

        class _Cdn:
            async def head(self, url: str,
                           follow_redirects: bool = False) -> httpx.Response:
                asked.append(url)
                raise httpx.ConnectError("cdn unreachable")

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc) -> bool:
                return False

        monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _Cdn())
        cache = _cache(tmp_path)
        unknown = {
            f"p{i}_b_ab": ("http://dtil.tmsimg.com/assets/", f"p{i}", "b", "ab")
            for i in range(60)
        }

        resolved = await probe_unknown(cache, unknown)

        assert resolved == 0
        # Everything already in flight when the streak completes still
        # finishes, and nothing behind it is started.
        assert len(asked) <= ((_PROBE_CONCURRENCY + _OUTAGE_STREAK)
                              * len(VERTICAL_CODES))
        assert all(cache.get(k) == (False, None) for k in unknown)

    async def test_scattered_failures_do_not_end_the_run(
        self, tmp_path, monkeypatch
    ):
        """A CDN that keeps answering is not an outage. Ten assets fail here,
        spread through thirty, which is more than enough to end the run if
        those failures were counted cumulatively instead of consecutively."""
        asked = []
        never_answered = {f"p{i}" for i in range(0, 30, 3)}

        class _Cdn:
            async def head(self, url: str,
                           follow_redirects: bool = False) -> httpx.Response:
                asked.append(url)
                if url.split("/assets/")[1].split("_")[0] in never_answered:
                    raise httpx.ConnectError("cdn unreachable")
                return httpx.Response(200 if "_v12_" in url else 404)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc) -> bool:
                return False

        monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _Cdn())
        cache = _cache(tmp_path)
        unknown = {
            f"p{i}_b_ab": ("http://dtil.tmsimg.com/assets/", f"p{i}", "b", "ab")
            for i in range(30)
        }

        resolved = await probe_unknown(cache, unknown)

        assert resolved == 20
        assert all(cache.get(f"{a}_b_ab") == (False, None)
                   for a in never_answered)
        # Twenty answered on the first code, ten walked all five.
        assert len(asked) == 20 + 10 * len(VERTICAL_CODES)


class TestCache:
    def test_it_round_trips_through_its_file(self, tmp_path):
        path = tmp_path / "art.json"
        first = ArtworkCache(path)
        first.put("p1_b_aa", "v12")
        first.put("p2_b_aa", None)
        first.save()

        second = ArtworkCache(path)
        assert second.get("p1_b_aa") == (True, "v12")
        assert second.get("p2_b_aa") == (True, None)   # a known "no portrait"
        assert second.get("p3_b_aa") == (False, None)  # never seen

    def test_a_corrupt_cache_file_is_survivable(self, tmp_path):
        path = tmp_path / "art.json"
        path.write_text("{not json", encoding="utf-8")
        assert ArtworkCache(path).get("p1_b_aa") == (False, None)


async def test_a_source_pointed_at_its_own_proxy_is_refused(monkeypatch):
    """The upstream URL is read off the source itself, so a source whose URL
    is its own proxy would fetch itself forever.

    A coroutine, not asyncio.run: pytest.ini sets asyncio_mode=auto, so
    pytest-asyncio owns the loop and closing one out from under it breaks
    every async test that runs afterwards.
    """
    from unittest.mock import MagicMock

    import pytest
    from fastapi import HTTPException

    import routers.epg as epg

    client = MagicMock()

    async def _source(_id):
        return {"url": "http://ecm:6100/api/epg/artwork-proxy/4"}

    client.get_epg_source = _source
    monkeypatch.setattr(epg, "get_client", lambda: client)

    with pytest.raises(HTTPException) as caught:
        await epg.artwork_proxy(4)
    assert caught.value.status_code == 400
    assert "fetch itself" in caught.value.detail


def test_the_proxy_is_exempt_from_the_request_timeout():
    """It streams a whole upstream guide, which exceeded the 30s budget."""
    from main import _TIMEOUT_EXEMPT_PREFIXES

    assert "/api/epg/artwork-proxy" in _TIMEOUT_EXEMPT_PREFIXES
