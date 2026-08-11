"""Repointing Gracenote programme artwork to its portrait rendition.

The upstream feeds reference landscape codes (h8/h9), which a guide client
rendering portrait tiles center-crops. These pin what keeps the rewrite safe:
only a rendition confirmed to exist is emitted, anything unresolved keeps the
landscape URL it had, and a match split across a chunk boundary is still
found — the rewrite streams, so boundaries land mid-URL routinely.
"""
from services.epg_artwork import ArtworkCache, ArtworkRewriter


ICON = "http://dtil.tmsimg.com/assets/p30177490_b_h8_ab.jpg?w=960&h=540"
XML = f'<programme><title>CIA</title><icon src="{ICON}" /></programme>'


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
