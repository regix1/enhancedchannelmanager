"""Repointing Gracenote programme artwork to its portrait rendition.

The upstream feeds reference landscape codes (h8/h9), which a guide client
rendering portrait tiles center-crops. These pin the two properties that
keep the rewrite safe: only a rendition confirmed to exist is emitted, and
anything unresolved keeps the landscape URL it already had.
"""
from services.epg_artwork import ArtworkCache, rewrite_artwork


ICON = (
    'http://dtil.tmsimg.com/assets/p30177490_b_h8_ab.jpg?w=960&h=540'
)
XML = f'<programme><title>CIA</title><icon src="{ICON}" /></programme>'


def _cache(tmp_path, seed=None):
    c = ArtworkCache(tmp_path / "art.json")
    for k, v in (seed or {}).items():
        c.put(k, v)
    return c


async def test_a_cached_asset_is_repointed_to_its_portrait_rendition(tmp_path):
    cache = _cache(tmp_path, {"p30177490_b_ab": "v12"})
    out, stats = await rewrite_artwork(XML, cache)
    assert "p30177490_b_v12_ab.jpg" in out
    assert "h8" not in out
    assert stats["rewritten"] == 1
    assert stats["probed"] == 0  # cached, so nothing was fetched


async def test_an_asset_with_no_portrait_keeps_its_landscape_url(tmp_path):
    """A cached None is a real answer: this asset has no portrait rendition.

    Emitting a guessed URL would give the client a broken image, which is
    worse than the crop it shows today.
    """
    cache = _cache(tmp_path, {"p30177490_b_ab": None})
    out, stats = await rewrite_artwork(XML, cache)
    assert out == XML
    assert stats["rewritten"] == 0


async def test_the_query_string_is_dropped_so_the_portrait_is_not_resized_to_landscape(tmp_path):
    """The feed pins ?w=960&h=540 on the URL. Carrying that onto the portrait
    rendition would hand the client a 16:9 crop of it, undoing the fix."""
    cache = _cache(tmp_path, {"p30177490_b_ab": "v12"})
    out, _ = await rewrite_artwork(XML, cache)
    assert "w=960" not in out
    assert "h=540" not in out


async def test_the_suffix_is_preserved(tmp_path):
    """Portrait renditions live under the SAME aa/ab suffix as the landscape
    URL — measured across every sampled asset — so the suffix is carried, not
    guessed."""
    xml = XML.replace("_ab.jpg", "_aa.jpg")
    cache = _cache(tmp_path, {"p30177490_b_aa": "v7"})
    out, _ = await rewrite_artwork(xml, cache)
    assert "p30177490_b_v7_aa.jpg" in out


async def test_station_art_keeps_its_kind_segment(tmp_path):
    """Gracenote files programme art under ``_b_`` and station art under
    ``_st_``, and a kind's portrait rendition lives under that same kind.
    Rewriting ``_st_`` to ``_b_`` would point at an asset that does not
    exist."""
    xml = '<icon src="http://dtil.tmsimg.com/assets/p30660000_st_h10_aa.jpg?w=960&h=540" />'
    cache = _cache(tmp_path, {"p30660000_st_aa": "v8"})
    out, stats = await rewrite_artwork(xml, cache)
    assert "p30660000_st_v8_aa.jpg" in out
    assert "_b_" not in out
    assert stats["rewritten"] == 1


async def test_non_gracenote_artwork_is_left_alone(tmp_path):
    xml = '<programme><icon src="http://172.16.1.73:3100/mlb/a/b/cover" /></programme>'
    out, stats = await rewrite_artwork(xml, _cache(tmp_path))
    assert out == xml
    assert stats["assets"] == 0


async def test_one_probe_serves_every_programme_sharing_an_asset(tmp_path):
    """A series repeats its asset across the week's programmes; probing it
    once per airing would multiply the request count for no new answer."""
    xml = XML * 5
    cache = _cache(tmp_path, {"p30177490_b_ab": "v12"})
    out, stats = await rewrite_artwork(xml, cache)
    assert stats["assets"] == 1
    assert stats["rewritten"] == 5
    assert out.count("v12") == 5


async def test_a_spent_budget_leaves_the_feed_untouched_rather_than_blocking(tmp_path):
    """A first pass over a very large feed must not stall the request. With
    no budget left nothing is probed, every icon keeps its landscape URL, and
    later runs resolve the rest."""
    cache = _cache(tmp_path)
    out, stats = await rewrite_artwork(XML, cache, budget_seconds=-1)
    assert out == XML
    assert stats["rewritten"] == 0


def test_the_cache_round_trips_through_its_file(tmp_path):
    path = tmp_path / "art.json"
    first = ArtworkCache(path)
    first.put("p1_aa", "v12")
    first.put("p2_aa", None)
    first.save()

    second = ArtworkCache(path)
    assert second.get("p1_aa") == (True, "v12")
    assert second.get("p2_aa") == (True, None)   # a known "no portrait"
    assert second.get("p3_aa") == (False, None)  # never seen


def test_a_corrupt_cache_file_is_survivable(tmp_path):
    path = tmp_path / "art.json"
    path.write_text("{not json", encoding="utf-8")
    cache = ArtworkCache(path)
    assert cache.get("p1_aa") == (False, None)
