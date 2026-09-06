"""Selected schedules, event identity, bounds and shared guide preparation."""

import asyncio
import copy
import gzip
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from dummy_epg_engine import generate_xmltv
from services import epg_programmes as guides
from services.epg_migration import stream_xmltv

NOW = datetime(2026, 9, 5, 2, tzinfo=timezone.utc)
START = datetime(2026, 9, 4, 4, tzinfo=timezone.utc)
STOP = datetime(2026, 9, 6, 4, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_source_error_retains_wrapped_timeout_reason(monkeypatch):
    from fastapi import HTTPException

    failure = HTTPException(502, "Could not read the configured XMLTV source.")
    failure.__cause__ = httpx.ReadTimeout("https://guide.invalid/?key=private")
    monkeypatch.setattr(guides, "_read_source", AsyncMock(side_effect=failure))
    await guides._load_source("selected", source(), [], START, STOP, NOW)
    assert guides._SOURCE_CACHE["selected"]["error"] == "Request timed out while reading."


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,expected", [
    (httpx.ConnectTimeout("private"), "Request timed out while connecting."),
    (TimeoutError("private"), "Request timed out."),
    (httpx.ConnectError("private"), "Connection failed."),
    (ET.ParseError("private"), "Malformed XML."),
    (ValueError("XMLTV root must be tv."), "XMLTV root must be tv."),
    (ValueError("XMLTV document is empty."), "XMLTV document is empty."),
    (ValueError("XMLTV element exceeds the retained size limit."), "XMLTV element exceeds the retained size limit."),
    (ValueError("Selected XMLTV schedules exceed the retained size limit."), "Selected XMLTV schedules exceed the retained size limit."),
    (ValueError("Dispatcharr EPG catalogue exceeds 200000 rows"), "Response exceeded the catalogue size limit."),
    (ValueError("Dispatcharr EPG response exceeds its source row counts"), "Response exceeded the catalogue size limit."),
    (ValueError("Dispatcharr EPG response exceeds 16384 bytes per row"), "Response exceeded the catalogue size limit."),
    (ValueError("Dispatcharr EPG response contains trailing JSON"), "Invalid JSON response."),
    (ValueError("Dispatcharr EPG response contains a non-object row"), "Invalid JSON response."),
    (ValueError("Dispatcharr EPG source counts are unavailable"), "Catalogue source counts are unavailable."),
    (ValueError("Dispatcharr EPG source counts are unavailable https://guide.invalid/?key=private"), "Request failed."),
    (ValueError("Dispatcharr EPG response is incomplete"), "Invalid JSON response."),
    (ValueError("Dispatcharr EPG response exceeds 16384 bytes per row https://guide.invalid/?key=private"), "Request failed."),
    (ValueError("private https://guide.invalid/?key=private"), "Request failed."),
])
async def test_source_errors_preserve_only_known_reasons(monkeypatch, failure, expected):
    monkeypatch.setattr(guides, "_read_source", AsyncMock(side_effect=failure))
    await guides._load_source("selected", source(), [], START, STOP, NOW)
    assert guides._SOURCE_CACHE["selected"]["error"] == expected
    assert "private" not in guides._SOURCE_CACHE["selected"]["error"]


@pytest.mark.asyncio
@pytest.mark.parametrize("detail", [
    "XMLTV source has no downloadable URL.",
    "XMLTV source returned an invalid redirect.",
    "XMLTV source URL is blocked by the outbound security policy.",
    "XMLTV download exceeds its size limit.",
    "XMLTV decoded content exceeds its size limit.",
    "XMLTV DTDs, entities and non-UTF encodings are not supported.",
    "XMLTV gzip has trailing content.",
    "XMLTV gzip is incomplete.",
])
async def test_source_errors_preserve_fixed_transport_details(monkeypatch, detail):
    from fastapi import HTTPException

    failure = HTTPException(422, detail)
    failure.__cause__ = RuntimeError("https://guide.invalid/?key=private")
    monkeypatch.setattr(guides, "_read_source", AsyncMock(side_effect=failure))
    await guides._load_source("selected", source(), [], START, STOP, NOW)
    assert guides._SOURCE_CACHE["selected"]["error"] == detail


@pytest.mark.asyncio
async def test_source_coverage_reports_safe_status_and_retains_error_state(monkeypatch):
    from fastapi import HTTPException

    request = httpx.Request("GET", "https://guide.invalid/?key=private")
    failure = HTTPException(502, "Could not read the configured XMLTV source.")
    failure.__cause__ = httpx.HTTPStatusError("private", request=request,
                                            response=httpx.Response(503, request=request, text="private"))
    monkeypatch.setattr(guides, "_read_source", AsyncMock(side_effect=failure))
    _, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
    assert coverage["sources"][0]["error"] == "HTTP status 503."
    assert coverage["sources"][0]["status"] == "error"
    assert coverage["sources"][0]["last_success"] is None
    assert coverage["channels"][0]["real_minutes"] == 0
    assert not guides.can_cache(coverage)


@pytest.mark.asyncio
async def test_source_parser_failure_is_visible_in_coverage(monkeypatch):
    install_feed(monkeypatch, b"<tv><channel></tv>")
    _, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
    assert coverage["sources"][0]["error"] == "Malformed XML."
    assert coverage["sources"][0]["status"] == "error"


@pytest.mark.asyncio
async def test_source_error_causes_are_bounded_and_cancellation_is_unchanged(monkeypatch):
    failure = type("private_credentials", (RuntimeError,), {})("private")
    failure.__cause__ = failure
    monkeypatch.setattr(guides, "_read_source", AsyncMock(side_effect=failure))
    await guides._load_source("selected", source(), [], START, STOP, NOW)
    assert guides._SOURCE_CACHE["selected"]["error"] == "Request failed."
    failure = httpx.ReadTimeout("private")
    for _ in range(9):
        outer = RuntimeError("private")
        outer.__cause__ = failure
        failure = outer
    monkeypatch.setattr(guides, "_read_source", AsyncMock(side_effect=failure))
    await guides._load_source("selected", source(), [], START, STOP, NOW)
    assert guides._SOURCE_CACHE["selected"]["error"] == "Request failed."
    monkeypatch.setattr(guides, "_read_source", AsyncMock(side_effect=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await guides._load_source("selected", source(), [], START, STOP, NOW)
    assert guides._SOURCE_CACHE["selected"]["error"] == "XMLTV source loading was cancelled."


@pytest.mark.asyncio
async def test_source_error_ignores_unknown_details_and_classifies_gzip_cause(monkeypatch):
    import zlib
    from fastapi import HTTPException

    failure = HTTPException(502, {"url": "https://guide.invalid/?key=private"})
    monkeypatch.setattr(guides, "_read_source", AsyncMock(side_effect=failure))
    await guides._load_source("selected", source(), [], START, STOP, NOW)
    assert guides._SOURCE_CACHE["selected"]["error"] == "Request failed."
    failure.__cause__ = zlib.error("private")
    await guides._load_source("selected", source(), [], START, STOP, NOW)
    assert guides._SOURCE_CACHE["selected"]["error"] == "Invalid compressed XMLTV content."


@pytest.fixture(autouse=True)
async def clean_sources(monkeypatch):
    guides._SOURCE_CACHE.clear()
    guides._SOURCE_LOADS.clear()
    guides._CATALOGUE_CACHE.clear()
    guides._CATALOGUE_LOADS.clear()
    monkeypatch.setattr(guides, "_CATALOGUE_SLOTS", asyncio.Semaphore(4))
    monkeypatch.setattr(guides, "_ARTWORK_LOAD", None)
    monkeypatch.setattr(guides, "_ARTWORK_CHECKED", float("-inf"))
    monkeypatch.setattr(guides, "_SOURCE_SLOTS", asyncio.Semaphore(2))
    yield
    tasks = list(guides._SOURCE_LOADS.values()) + list(guides._CATALOGUE_LOADS.values())
    if guides._ARTWORK_LOAD is not None:
        tasks.append(guides._ARTWORK_LOAD)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    guides._SOURCE_LOADS.clear()
    guides._SOURCE_CACHE.clear()


def source(source_id=50, **fields):
    return {"id": source_id, "name": f"Guide {source_id}", "source_type": "xmltv",
            "is_active": True, "url": f"https://example.com/{source_id}.xml", "priority": 0, **fields}


def programme(tvg="ESPN.us", title="SportsCenter", start="20260905010000 +0000",
              stop="20260905050000 +0000", children=""):
    return ET.fromstring(f'<programme channel="{tvg}" start="{start}" stop="{stop}">'
                         f'<title>{title}</title>{children}</programme>')


def profile(**fields):
    return {"id": 1, "enabled": True, "epg_source_ids": [50], "channel_group_ids": [65],
            "event_timezone": "US/Eastern", "channel_mappings": [], **fields}


def channel(**fields):
    return {"id": 1, "name": "ESPN", "channel_group_id": 65, "tvg_id": "ESPN.us",
            "streams": [], **fields}


def client(sources=None, rows=None):
    result = AsyncMock()
    result.get_epg_sources.return_value = sources or [source()]
    result.get_epg_data.return_value = rows or []
    result.get_epg_data_by_id.side_effect = lambda link: next(row for row in rows or [] if row["id"] == link)
    return result


def feed(*programmes, headers='<channel id="ESPN.us"><display-name>ESPN</display-name></channel>'):
    return ("<tv>" + headers + "".join(ET.tostring(row, encoding="unicode") for row in programmes) + "</tv>").encode()


def install_feed(monkeypatch, content):
    async def chunks(selected, **_):
        document = content[selected["id"]] if isinstance(content, dict) else content
        for index in range(0, len(document), 73):
            yield document[index:index + 73]
    monkeypatch.setattr(guides, "stream_xmltv", chunks)


SECRET = "hidden-credential"
DIAGNOSTIC_LABELS = {
    "content_type": {"xml", "gzip", "html", "text", "other", "absent"},
    "content_encoding": {"gzip", "identity", "other", "absent"},
    "transfer_encoding": {"chunked", "other", "absent"},
    "http_version": {"HTTP/1.0", "HTTP/1.1", "HTTP/2", "HTTP/3", "other"},
    "compression": {"gzip", "identity"},
    "root": {"tv", "other", "absent"},
    "failure": {"invalid_utf8", "forbidden_character", "incomplete_xml", "incomplete_gzip", "incomplete_body",
                "wrong_root", "malformed_xml", "unknown"},
}
DIAGNOSTIC_COUNTS = {"wire_bytes", "decoded_bytes", "http_status", "parser_code", "parser_line", "parser_column", "attempts",
                     "headers_ms", "content_length", "download_ms", "write_ms", "write_max_ms", "write_calls",
                     "staged_bytes", "validation_ms", "validation_bytes", "selection_ms", "selection_bytes",
                     "total_download_ms", "total_validation_ms", "total_selection_ms"}
DIAGNOSTIC_FLAGS = {"transport_complete", "xml_complete"}
PARSER_KEYS = {"parser_code", "parser_line", "parser_column"}


class Body(httpx.AsyncByteStream):
    def __init__(self, chunks, failure):
        self.chunks, self.failure = chunks, failure

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.failure is not None:
            raise self.failure


def reply(*chunks, status=200, headers=None, failure=None):
    return httpx.Response(status, headers=headers or {}, stream=Body(chunks, failure))


def install_transport(monkeypatch, handler):
    """Run the real streaming loader against an in-memory HTTP transport."""
    transport = httpx.MockTransport(handler)

    async def chunks(selected, **options):
        async for piece in stream_xmltv(selected, transport=transport, **options):
            yield piece
    monkeypatch.setattr(guides, "stream_xmltv", chunks)


def bounded(diagnostics):
    """Every diagnostic value is a count, a flag or a fixed label, so none can carry a secret."""
    assert set(diagnostics) <= set(DIAGNOSTIC_LABELS) | DIAGNOSTIC_COUNTS | DIAGNOSTIC_FLAGS, sorted(diagnostics)
    for key, value in diagnostics.items():
        if key in DIAGNOSTIC_LABELS:
            assert value in DIAGNOSTIC_LABELS[key], (key, value)
        elif key in DIAGNOSTIC_COUNTS:
            assert type(value) is int and value >= 0, (key, value)
        else:
            assert type(value) is bool, (key, value)
    return diagnostics


def test_canonical_sources_reject_recursion_and_disabled_inputs():
    original = source()
    proxy = source(51, url="https://ecm.example/api/epg/artwork-proxy/50")
    assert guides.resolve_sources([51, 50, 51], [original, proxy]) == [original]
    for invalid in (
        source(url="https://ecm.example/api/dummy-epg/xmltv"),
        source(is_active=False), source(source_type="schedules_direct"),
        source(url=""), source(url="https://ecm.example/api/epg/artwork-proxy/50"),
    ):
        with pytest.raises(ValueError):
            guides.resolve_sources([50], [invalid])
    with pytest.raises(ValueError):
        guides.resolve_sources([True], [original])


def test_capture_keeps_numeric_identity_and_remembered_missing_channel():
    profiles = profile(channel_mappings=[{"channel_id": 9, "source_id": 50, "tvg_id": "999"}])
    channels = {1: channel(epg_data_id=44), 2: channel(id=2, channel_group_id=90, epg_data_id=55)}
    rows = [{"id": 44, "epg_source": 50, "tvg_id": "32645"}, {"id": 55, "epg_source": 50, "tvg_id": "9999"}]
    before = copy.deepcopy(profiles)
    assert guides.capture_mappings(profiles, channels, rows, [source()]) == [
        {"channel_id": 1, "source_id": 50, "tvg_id": "32645"},
        {"channel_id": 9, "source_id": 50, "tvg_id": "999"},
    ]
    assert profiles == before


@pytest.mark.asyncio
async def test_shared_fetch_expands_mixed_streams_once_and_both_group_shapes():
    upstream = AsyncMock()
    upstream.get_channels.side_effect = [
        {"results": [channel(streams=[2, {"id": 3, "name": "kept"}])], "next": "next"},
        {"results": [{"id": 4, "name": "Other", "channel_group": 65, "streams": [2]}], "next": None},
    ]
    upstream.get_streams_by_ids.return_value = [{"id": 2, "name": "resolved"}]
    channels = await guides._fetch_all_channels(upstream)
    assert [row["channel_id"] for row in guides._resolve_group_assignments([65], channels)] == [1, 4]
    assert channels[1]["streams"][1]["name"] == "kept"
    assert channels[4]["streams"][0]["name"] == "resolved"
    upstream.get_streams_by_ids.assert_awaited_once_with([2])


@pytest.mark.parametrize("start,stop", [
    ("20260905010000", "20260905050000 +0000"),
    ("20260230010000 +0000", "20260905050000 +0000"),
    ("20260905050000 +0000", "20260905010000 +0000"),
])
def test_source_timestamps_require_valid_aware_positive_intervals(start, stop):
    with pytest.raises(ValueError):
        guides.programme_times(programme(start=start, stop=stop))


@pytest.mark.asyncio
async def test_complete_source_preserves_children_filters_expired_and_unrelated(monkeypatch):
    rows = [
        programme(children='<sub-title>Morning</sub-title><icon src="https://images.example/poster.jpg"/><episode-num system="xmltv_ns">1.2.</episode-num>'),
        programme("unrelated", "Other"),
        programme(start="20240905010000 +0000", stop="20240905050000 +0000"),
    ]
    install_feed(monkeypatch, feed(*rows))
    query = guides._query(profile(), channel(), None, NOW)
    loaded = await guides._read_source(source(), [query], START, STOP, NOW)
    assert list(loaded["rows"]) == ["ESPN.us"]
    saved = loaded["rows"]["ESPN.us"]
    assert len(saved) == 1
    assert saved[0].findtext("episode-num") == "1.2."
    assert saved[0].find("icon").get("src") == "https://images.example/poster.jpg"


@pytest.mark.asyncio
async def test_malformed_tail_never_publishes_partial_success(monkeypatch):
    install_feed(monkeypatch, feed(programme())[:-5])
    query = guides._query(profile(), channel(), None, NOW)
    await guides._load_source("broken", source(), [query], START, STOP, NOW)
    assert guides._SOURCE_CACHE["broken"]["error"]
    assert not guides._SOURCE_CACHE["broken"].get("rows")


@pytest.mark.asyncio
async def test_cancelled_source_is_never_reported_ready(monkeypatch):
    monkeypatch.setattr(guides, "_read_source", AsyncMock(side_effect=asyncio.CancelledError))
    _, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
    assert coverage["sources"][0]["status"] == "error"
    assert not guides.can_cache(coverage)
    assert next(iter(guides._SOURCE_CACHE.values())).get("checked") is not None


@pytest.mark.asyncio
async def test_retained_limit_is_enforced(monkeypatch):
    install_feed(monkeypatch, feed(programme()))
    monkeypatch.setattr(guides, "MAX_RETAINED", 10)
    with pytest.raises(ValueError, match="retained"):
        await guides._read_source(source(), [guides._query(profile(), channel(), None, NOW)], START, STOP, NOW)


@pytest.mark.asyncio
async def test_lower_priority_fills_only_real_gaps_and_placeholders_cannot_win(monkeypatch):
    install_feed(monkeypatch, {
        50: feed(programme(stop="20260905030000 +0000"), programme(title="No EVENT Today", start="20260905030000 +0000")),
        51: feed(programme(title="Backup coverage", start="20260905020000 +0000")),
    })
    profiles, coverage = await guides.prepare_profiles(
        [profile(epg_source_ids=[50, 51])], {1: channel()}, client([source(priority=10), source(51)]),
        now=NOW, wait_for_sources=True,
    )
    rows = profiles[0]["source_programmes"][1]
    assert [row.findtext("title") for row in rows] == ["SportsCenter", "Backup coverage"], [(key, {tvg: [ET.tostring(row) for row in rows] for tvg, rows in value.get("rows", {}).items()}) for key, value in guides._SOURCE_CACHE.items()]
    assert rows[1].get("start") == "20260905030000 +0000"
    assert rows[1].get("stop") == "20260905050000 +0000"
    assert coverage["channels"][0]["current"]["title"] == "SportsCenter"


@pytest.mark.asyncio
async def test_explicit_numeric_source_beats_unrelated_high_priority(monkeypatch):
    install_feed(monkeypatch, {50: feed(programme("32645"), headers='<channel id="32645"><display-name>ESPN</display-name></channel>'),
                              51: feed(programme("32645", "Wrong network"), headers='<channel id="32645"><display-name>Other</display-name></channel>')})
    profiles, coverage = await guides.prepare_profiles(
        [profile(epg_source_ids=[50, 51], channel_mappings=[{"channel_id": 1, "source_id": 50, "tvg_id": "32645"}])],
        {1: channel()}, client([source(), source(51, priority=100)]), now=NOW, wait_for_sources=True,
    )
    assert coverage["channels"][0]["source_id"] == 50, coverage
    assert [row.findtext("title") for row in profiles[0]["source_programmes"][1]] == ["SportsCenter"]


@pytest.mark.asyncio
async def test_one47_uses_same_day_real_end_and_preserves_source_art(monkeypatch):
    event_channel = channel(
        name="ONE Fight Night 47 Stamp Vs. Flores @ Sep 04 09:00 PM", tvg_id="",
        streams=[{"id": 7, "name": "PPV 16 | ONE FIGHT NIGHT 47 STAMP VS. FLORES (9.4 9:00 PM ET) PPV"}],
    )
    install_feed(monkeypatch, feed(
        programme("PPV10.art", "ONE FIGHT NIGHT 47 STAMP V FLORES  ᴸᶦᵛᵉ", children='<icon src="https://art.example/fight.jpg"/>'),
        programme("PPV(1)-05.v", "ONE Fight Night 47 Stamp v Flores  ᴸᶦᵛᵉ",
                  start="20260906010000 +0000", stop="20260906040000 +0000"),
        headers='<channel id="PPV10.art"><display-name>ART - PPV 10</display-name></channel>',
    ))
    profiles, coverage = await guides.prepare_profiles(
        [profile(program_duration=180, ended_title_template="Ended: {title}")], {1: event_channel},
        client(), now=NOW, wait_for_sources=True,
    )
    rows = profiles[0]["source_programmes"][1]
    assert len(rows) == 1
    assert rows[0].get("stop") == "20260905050000 +0000"
    assert coverage["channels"][0]["source_tvg_id"] == "PPV10.art"
    xml = ET.fromstring(generate_xmltv(profiles, {1: event_channel}))
    real = [row for row in xml.findall("programme") if "ONE" in (row.findtext("title") or "")]
    assert len(real) == 1
    assert real[0].get("stop") == "20260905050000 +0000"
    assert real[0].find("icon").get("src") == "https://art.example/fight.jpg"
    assert "Ended:" not in ET.tostring(xml, encoding="unicode")


@pytest.mark.asyncio
@pytest.mark.parametrize("event_name", [
    "Cage Fury 160 @ Sep 04 09:00 PM", "Cage Fury 160", "Cage Fury 161 @ Sep 04 07:15 PM",
])
async def test_event_wrong_time_missing_kickoff_or_edition_stays_unresolved(monkeypatch, event_name):
    install_feed(monkeypatch, feed(programme("PPV07.art", "CAGE FURY FC 160  ᴸᶦᵛᵉ",
                                            start="20260904231500 +0000", stop="20260905031500 +0000")))
    channels = {1: channel(name=event_name, tvg_id="")}
    profiles, coverage = await guides.prepare_profiles([profile()], channels, client(), now=NOW, wait_for_sources=True)
    assert profiles[0]["source_programmes"][1] == []
    assert coverage["channels"][0]["current"] is None
    if event_name == "Cage Fury 160 @ Sep 04 09:00 PM":
        assert "event_start_conflict" in coverage["channels"][0]["warnings"]


@pytest.mark.asyncio
async def test_provider_family_explicit_slot_is_static_but_bare_slot_is_not(monkeypatch):
    install_feed(monkeypatch, feed(programme("ESPN+1.v", "College Football"), headers='<channel id="ESPN+1.v"><display-name>ESPN+ 1</display-name></channel>'))
    for tvg_id, expected in [("ESPN+1.v", 1), ("", 0)]:
        profiles, _ = await guides.prepare_profiles([profile()], {1: channel(name="ESPN+ 1", tvg_id=tvg_id)},
                                                  client(), now=NOW, wait_for_sources=True)
        assert len(profiles[0]["source_programmes"][1]) == expected


@pytest.mark.asyncio
async def test_neutral_gaps_have_no_event_claim_or_art_and_dst_uses_local_midnights(monkeypatch):
    install_feed(monkeypatch, b"<tv/>")
    channels = {1: channel(name="ONE Fight Night 47 @ Sep 07 09:00 PM", tvg_id="")}
    for now, expected_hours in [(datetime(2026, 3, 8, 12, tzinfo=timezone.utc), 47),
                                (datetime(2026, 11, 1, 12, tzinfo=timezone.utc), 49)]:
        profiles, coverage = await guides.prepare_profiles(
            [profile(include_live_tag=True, include_new_tag=True, ended_title_template="Ended: {title}",
                     program_poster_url_template="https://art.example/stale.jpg")], channels, client(),
            now=now, wait_for_sources=True,
        )
        assert (profiles[0]["guide_stop"] - profiles[0]["guide_start"]).total_seconds() / 3600 == expected_hours
        xml = ET.fromstring(generate_xmltv(profiles, channels))
        row = xml.find("programme")
        assert row.findtext("title") == "Programming unavailable"
        assert row.find("live") is None and row.find("new") is None and row.find("icon") is None
        assert coverage["channels"][0]["real_minutes"] == 0


@pytest.mark.asyncio
async def test_cold_budget_deduplicates_load_and_invalidates_output_on_completion(monkeypatch):
    started, release = asyncio.Event(), asyncio.Event()
    calls = 0
    async def read(*_):
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return {"headers": {}, "rows": {"ESPN.us": [programme()]}, "warnings": [], "size": 100}
    monkeypatch.setattr(guides, "_read_source", read)
    monkeypatch.setattr(guides, "HTTP_WAIT", 0.02)
    with patch("services.epg_programmes.get_cache") as cache:
        upstream = client()
        refresh = asyncio.create_task(guides.prepare_profiles(
            [profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True,
        ))
        await started.wait()
        profiles, coverage = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW)
        assert coverage["sources"][0]["status"] == "pending"
        assert profiles[0]["source_programmes"][1] == []
        await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW)
        assert calls == 1
        release.set()
        await refresh
        cache.return_value.invalidate_prefix.assert_called_with("dummy_epg_xmltv")
        profiles, _ = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW)
        assert len(profiles[0]["source_programmes"][1]) == 1


@pytest.mark.asyncio
async def test_slow_catalogue_continues_once_after_public_wait_expires(monkeypatch):
    release = asyncio.Event()
    upstream = client()
    async def sources():
        await release.wait()
        return [source()]
    upstream.get_epg_sources.side_effect = sources
    monkeypatch.setattr(guides, "HTTP_WAIT", 0.01)
    install_feed(monkeypatch, feed(programme()))
    _, first = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW)
    _, second = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW)
    assert first["sources"][0]["status"] == "pending"
    assert second["sources"][0]["status"] == "pending"
    assert upstream.get_epg_sources.await_count == 1
    release.set()
    await asyncio.gather(*list(guides._CATALOGUE_LOADS.values()))
    _, ready = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
    assert ready["sources"][0]["status"] == "ready"
    upstream.get_epg_data.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_catalogue_reports_error_and_retries_after_backoff(monkeypatch):
    upstream = client()
    upstream.get_epg_sources.side_effect = RuntimeError("unreachable")
    for _ in range(2):
        _, coverage = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
        assert coverage["sources"][0]["status"] == "error"
        assert not guides.can_cache(coverage)
    assert upstream.get_epg_sources.await_count == 1


@pytest.mark.asyncio
async def test_current_mapping_lookup_overrides_remembered_identity_and_refreshes(monkeypatch):
    rows = [{"id": 90, "epg_source": 50, "tvg_id": "222"}, {"id": 91, "epg_source": 50, "tvg_id": "333"}]
    upstream = client(rows=rows)
    install_feed(monkeypatch, feed(*(programme(tvg=tvg) for tvg in ("111", "222", "333", "444"))))
    selected = profile(channel_mappings=[{"channel_id": 1, "source_id": 50, "tvg_id": "111"}])
    channels = {1: channel(epg_data_id=90)}
    _, first = await guides.prepare_profiles([selected], channels, upstream, now=NOW, wait_for_sources=True)
    assert first["channels"][0]["source_tvg_id"] == "222"
    channels[1]["epg_data_id"] = 91
    _, pending = await guides.prepare_profiles([selected], channels, upstream, now=NOW)
    assert pending["channels"][0]["source_tvg_id"] is None
    next(iter(guides._SOURCE_CACHE.values()))["checked"] -= guides.SOURCE_TTL + 1
    _, second = await guides.prepare_profiles([selected], channels, upstream, now=NOW, wait_for_sources=True)
    assert second["channels"][0]["source_tvg_id"] == "333"
    rows[1]["tvg_id"] = "444"
    guides._CATALOGUE_CACHE[(upstream, 91)]["checked"] -= guides.SOURCE_RETRY + 1
    next(iter(guides._SOURCE_CACHE.values()))["checked"] -= guides.SOURCE_TTL + 1
    _, third = await guides.prepare_profiles([selected], channels, upstream, now=NOW, wait_for_sources=True)
    assert third["channels"][0]["source_tvg_id"] == "444"
    upstream.get_epg_data.assert_not_awaited()
    assert selected["channel_mappings"][0]["tvg_id"] == "111"


@pytest.mark.asyncio
@pytest.mark.parametrize("link_field", ["epg_data_id", "epg_data"])
async def test_changed_unresolved_mapping_never_uses_the_remembered_source(monkeypatch, link_field):
    upstream = client()
    async def row(link):
        await asyncio.Event().wait()
    upstream.get_epg_data_by_id.side_effect = row
    monkeypatch.setattr(guides, "HTTP_WAIT", 0.01)
    install_feed(monkeypatch, feed(programme(tvg="111")))
    selected = profile(channel_mappings=[{"channel_id": 1, "source_id": 50, "tvg_id": "111"}])
    prepared, coverage = await guides.prepare_profiles([selected], {1: channel(**{"epg_data_id": None, link_field: 91}, tvg_id="111")}, upstream, now=NOW)
    assert prepared[0]["source_programmes"][1] == []
    assert "mapping_unavailable" in coverage["channels"][0]["warnings"]
    assert not guides.can_cache(coverage)


@pytest.mark.asyncio
@pytest.mark.parametrize("link_field", ["epg_data_id", "epg_data"])
async def test_current_unselected_mapping_does_not_restore_an_old_binding(monkeypatch, link_field):
    upstream = client(sources=[source(), source(id=51)], rows=[{"id": 91, "epg_source": 51, "tvg_id": "222"}])
    install_feed(monkeypatch, feed(programme(tvg="111")))
    selected = profile(channel_mappings=[{"channel_id": 1, "source_id": 50, "tvg_id": "111"}])
    prepared, coverage = await guides.prepare_profiles([selected], {1: channel(**{"epg_data_id": None, link_field: 91}, tvg_id="111")}, upstream, now=NOW, wait_for_sources=True)
    assert prepared[0]["source_programmes"][1] == []
    assert "source_not_selected" in coverage["channels"][0]["warnings"]
    assert selected["channel_mappings"][0]["tvg_id"] == "111"


@pytest.mark.asyncio
async def test_known_current_mapping_survives_a_failed_recheck(monkeypatch):
    upstream = client(rows=[{"id": 90, "epg_source": 50, "tvg_id": "111"}])
    install_feed(monkeypatch, feed(programme(tvg="111")))
    selected, channels = profile(), {1: channel(epg_data_id=90, tvg_id="")}
    await guides.prepare_profiles([selected], channels, upstream, now=NOW, wait_for_sources=True)
    guides._CATALOGUE_CACHE[(upstream, 90)]["checked"] -= guides.SOURCE_RETRY + 1
    upstream.get_epg_data_by_id.side_effect = RuntimeError("unavailable")
    prepared, coverage = await guides.prepare_profiles([selected], channels, upstream, now=NOW, wait_for_sources=True)
    assert prepared[0]["source_programmes"][1][0].get("stop") == "20260905050000 +0000"
    assert not guides.can_cache(coverage)


@pytest.mark.asyncio
async def test_unchanged_catalogue_does_not_invalidate_ready_output():
    upstream = client()
    key = (upstream, None)
    with patch.object(guides, "get_cache") as cache:
        await guides._load_catalogue(key, upstream, None)
        cache.return_value.invalidate_prefix.reset_mock()
        await guides._load_catalogue(key, upstream, None)
        cache.return_value.invalidate_prefix.assert_not_called()
        upstream.get_epg_sources.return_value = [source(priority=9)]
        await guides._load_catalogue(key, upstream, None)
        cache.return_value.invalidate_prefix.assert_called_once_with("dummy_epg_xmltv")


@pytest.mark.asyncio
@pytest.mark.parametrize("link", ["primary", "alternate", "embedded"])
async def test_null_primary_aliases_preserve_current_mapping(monkeypatch, link):
    upstream = client(rows=[{"id": 90, "epg_source_id": 50, "tvg_id": "111"}])
    install_feed(monkeypatch, feed(programme(tvg="111")))
    links = {"epg_data_id": 90} if link == "primary" else {"epg_data_id": None, "epg_data": 90 if link == "alternate" else {"id": 90, "epg_source_id": 50, "tvg_id": "111"}}
    prepared, coverage = await guides.prepare_profiles([profile()], {1: channel(**links, tvg_id="", channel_group_id=None, channel_group={"id": 65})}, upstream, now=NOW, wait_for_sources=True)
    assert prepared[0]["source_programmes"][1][0].get("channel") == "111"
    assert coverage["channels"][0]["match"] == "static"


@pytest.mark.asyncio
async def test_custom_dummy_identifier_retains_original_mapping(monkeypatch):
    sources = [source(), {"id": 100, "source_type": "xmltv", "is_active": True, "url": "http://ecm/api/dummy-epg/xmltv/1"}]
    rows = [{"id": 90, "epg_source": 100, "tvg_id": "sports-1"}]
    upstream = client(sources=sources, rows=rows)
    install_feed(monkeypatch, feed(programme(tvg="111")))
    selected = profile(tvg_id_template="sports-{channel_id}", channel_mappings=[{"channel_id": 1, "source_id": 50, "tvg_id": "111"}])
    channels = {1: channel(epg_data_id=90, tvg_id="sports-1")}
    assert guides.capture_mappings(selected, channels, rows, sources) == selected["channel_mappings"]
    prepared, coverage = await guides.prepare_profiles([selected], channels, upstream, now=NOW, wait_for_sources=True)
    assert prepared[0]["source_programmes"][1][0].get("channel") == "111"
    assert coverage["channels"][0]["xmltv_id"] == "sports-1"


@pytest.mark.asyncio
async def test_combined_preparation_keeps_each_profile_schedule(monkeypatch):
    install_feed(monkeypatch, feed(programme()))
    upstream = client()
    prepared, _ = await guides.prepare_profiles([profile(), profile(id=2)], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
    single, _ = await guides.prepare_profiles([profile(id=2)], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
    assert generate_xmltv([prepared[1]], {1: channel()}) == generate_xmltv(single, {1: channel()})
    combined = ET.fromstring(generate_xmltv(prepared, {1: channel()}))
    assert len(combined.findall("channel")) == 1


@pytest.mark.asyncio
async def test_inferred_event_keeps_its_source_channel_icon(monkeypatch):
    icon = "https://images.example/event-logo.png"
    install_feed(monkeypatch, feed(programme("PPV10.art", "ONE FIGHT NIGHT 47 STAMP V FLORES"), headers=f'<channel id="PPV10.art"><display-name>PPV 10</display-name><icon src="{icon}"/></channel>'))
    channels = {1: channel(name="ONE Fight Night 47 Stamp vs. Flores @ Sep 04 09:00 PM", tvg_id="")}
    prepared, _ = await guides.prepare_profiles([profile()], channels, client(), now=NOW, wait_for_sources=True)
    assert ET.fromstring(generate_xmltv(prepared, channels)).find("channel/icon").get("src") == icon


@pytest.mark.asyncio
async def test_catalogue_eviction_does_not_drop_the_current_batch(monkeypatch):
    upstream = client()
    upstream.get_epg_data_by_id.side_effect = lambda link: {"id": link, "epg_source": 50, "tvg_id": "ESPN.us"}
    channels = {i: channel(id=i, epg_data_id=i) for i in range(1, 2049)}
    monkeypatch.setattr(guides, "_read_source", AsyncMock(return_value={"headers": {}, "rows": {}, "warnings": [], "size": 0}))
    _, coverage = await guides.prepare_profiles([profile()], channels, upstream, now=NOW, wait_for_sources=True)
    assert coverage["sources"][0]["status"] == "ready"
    assert len(guides._CATALOGUE_CACHE) <= 2048


@pytest.mark.asyncio
async def test_missing_source_cache_entry_has_a_useful_error(monkeypatch):
    async def disappear(key, *_):
        guides._SOURCE_CACHE.pop(key, None)
        guides._SOURCE_LOADS.pop(key, None)
    monkeypatch.setattr(guides, "_load_source", disappear)
    _, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
    assert coverage["sources"][0]["status"] == "error"
    assert coverage["sources"][0]["error"]


@pytest.mark.asyncio
async def test_profile_subset_reuses_a_complete_source_selection(monkeypatch):
    install_feed(monkeypatch, feed(programme(), programme(tvg="TNT.us")))
    first = profile(channel_group_ids=[], channel_assignments=[{"channel_id": 1}])
    second = profile(id=2, channel_group_ids=[], channel_assignments=[{"channel_id": 2}])
    channels = {1: channel(), 2: channel(id=2, tvg_id="TNT.us")}
    upstream = client()
    with patch.object(guides, "_read_source", wraps=guides._read_source) as read:
        combined, _ = await guides.prepare_profiles([first, second], channels, upstream, now=NOW, wait_for_sources=True)
        subset, _ = await guides.prepare_profiles([second], channels, upstream, now=NOW, wait_for_sources=True)
        assert combined[1]["source_programmes"][2]
        assert subset[0]["source_programmes"][2]
        assert read.await_count == 1


@pytest.mark.asyncio
async def test_cache_is_scoped_to_identity_and_failure_keeps_original_end(monkeypatch):
    install_feed(monkeypatch, feed(programme()))
    upstream = client()
    await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
    key = next(iter(guides._SOURCE_CACHE))
    guides._SOURCE_CACHE[key]["checked"] -= guides.SOURCE_TTL + 1
    async def fail(*_, **__):
        raise ValueError("https://secret.example/key?password=hidden")
        yield b""
    monkeypatch.setattr(guides, "stream_xmltv", fail)
    profiles, coverage = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
    assert profiles[0]["source_programmes"][1], [(key, {tvg: [ET.tostring(row) for row in rows] for tvg, rows in value.get("rows", {}).items()}) for key, value in guides._SOURCE_CACHE.items()]
    assert profiles[0]["source_programmes"][1][0].get("stop") == "20260905050000 +0000"
    assert coverage["sources"][0]["status"] == "error"
    assert "password" not in str(coverage)
    profiles, _ = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW + timedelta(hours=4), wait_for_sources=True)
    assert profiles[0]["source_programmes"][1] == []
    profiles, _ = await guides.prepare_profiles([profile()], {1: channel(tvg_id="Other", name="Other")}, upstream, now=NOW, wait_for_sources=True)
    assert profiles[0]["source_programmes"][1] == []


@pytest.mark.asyncio
async def test_profiles_deduplicate_channels_and_report_custom_id_collision(monkeypatch):
    install_feed(monkeypatch, feed(programme()))
    channels = {1: channel(), 2: channel(id=2)}
    profiles, coverage = await guides.prepare_profiles(
        [profile(id=2), profile(id=1, tvg_id_template="same")], channels, client(), now=NOW, wait_for_sources=True,
    )
    xml = ET.fromstring(generate_xmltv(profiles, channels))
    assert len(xml.findall("channel")) == 1
    assert len({item.get("id") for item in xml.findall("channel")}) == 1
    assert any("xmltv_id_collision" in item["warnings"] for item in coverage["channels"])


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [b"<!DOCTYPE tv><tv/>", b'<!ENTITY x "y"><tv/>', b"<tv/>\x00"])
async def test_transport_rejects_declarations_across_chunk_boundaries(content):
    class Chunks(httpx.AsyncByteStream):
        async def __aiter__(self):
            for value in content:
                yield bytes([value])
    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=Chunks()))
    with pytest.raises(Exception, match="DTDs"):
        async for _ in stream_xmltv(source(), max_download=1024, max_decoded=1024, transport=transport):
            pass


@pytest.mark.asyncio
async def test_transport_gzip_bounds_and_truncation():
    class Chunks(httpx.AsyncByteStream):
        def __init__(self, raw):
            self.raw = raw
        async def __aiter__(self):
            yield self.raw
    compressed = gzip.compress(b"<tv>" + b" " * 5000 + b"</tv>")
    for raw, limit, expected in [(compressed, 100, "size limit"), (compressed[:-3], 10000, "incomplete")]:
        transport = httpx.MockTransport(lambda request: httpx.Response(200, headers={"content-encoding": "gzip"}, stream=Chunks(raw)))
        with pytest.raises(Exception, match=expected):
            async for _ in stream_xmltv(source(), max_download=10000, max_decoded=limit, transport=transport):
                pass


@pytest.mark.asyncio
async def test_multiple_sources_share_one_cold_wait_budget(monkeypatch):
    async def read(*_):
        await asyncio.sleep(0.2)
        return {"headers": {}, "rows": {}, "warnings": [], "size": 0}
    monkeypatch.setattr(guides, "_read_source", read)
    monkeypatch.setattr(guides, "HTTP_WAIT", 0.03)
    loop = asyncio.get_running_loop()
    begin = loop.time()
    _, coverage = await guides.prepare_profiles(
        [profile(epg_source_ids=[50, 51, 52, 53])], {1: channel()},
        client([source(number) for number in [50, 51, 52, 53]]), now=NOW,
    )
    assert loop.time() - begin < 0.1
    assert len(coverage["sources"]) == 4
    assert all(item["status"] == "pending" for item in coverage["sources"])


@pytest.mark.asyncio
async def test_source_download_concurrency_is_two(monkeypatch):
    active = maximum = 0
    async def chunks(*_, **__):
        nonlocal active, maximum
        active += 1
        maximum = max(active, maximum)
        try:
            await asyncio.sleep(0.01)
            yield b"<tv/>"
        finally:
            active -= 1
    monkeypatch.setattr(guides, "stream_xmltv", chunks)
    await asyncio.gather(*(guides._load_source(str(number), source(number), [], START, STOP, NOW)
                           for number in range(4)))
    assert maximum == 2
    assert active == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_ambiguous_same_title_kickoffs_do_not_select_arbitrarily(monkeypatch, explicit):
    install_feed(monkeypatch, feed(
        programme("PPV10.art", "ONE FIGHT NIGHT 47 STAMP V FLORES"),
        programme("PPV11.art", "ONE FIGHT NIGHT 47 STAMP V FLORES",
                  start="20260905011500 +0000", stop="20260905051500 +0000"),
    ))
    profiles, coverage = await guides.prepare_profiles(
        [profile(channel_mappings=[{"channel_id": 1, "source_id": 50, "tvg_id": "PPV10.art"}] if explicit else [])],
        {1: channel(name="ONE Fight Night 47 Stamp vs. Flores @ Sep 04 09:00 PM", tvg_id="")},
        client(), now=NOW, wait_for_sources=True,
    )
    if explicit:
        assert len(profiles[0]["source_programmes"][1]) == 1
        assert profiles[0]["source_programmes"][1][0].get("channel") == "PPV10.art"
        assert "ambiguous_event" not in coverage["channels"][0]["warnings"]
    else:
        assert profiles[0]["source_programmes"][1] == []
        assert "ambiguous_event" in coverage["channels"][0]["warnings"]


@pytest.mark.parametrize("seconds", [-1801, -1800, 0, 1800, 1801])
@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("ended", [False, True])
def test_composition_scores_only_possible_event_times(monkeypatch, seconds, explicit, ended):
    mapping = {"channel_id": 1, "source_id": 50, "tvg_id": "PPV10.art"} if explicit else None
    query = guides._query(profile(), channel(
        name="ONE Fight Night 47 Stamp vs. Flores @ Sep 04 09:00 PM", tvg_id="",
    ), mapping, NOW)
    begin = (query["event"].start + timedelta(seconds=seconds)).astimezone(timezone(timedelta(hours=-4)))
    end = begin + timedelta(minutes=10) if ended else NOW + timedelta(hours=4)
    row = programme("PPV10.art", "ONE FIGHT NIGHT 47 STAMP V FLORES",
                    start=begin.strftime("%Y%m%d%H%M%S %z"), stop=end.strftime("%Y%m%d%H%M%S %z"))
    with patch.object(guides, "_event", wraps=guides._event) as parse, \
            patch.object(guides, "_score_parsed_pair", wraps=guides._score_parsed_pair) as score:
        rows, coverage = guides._compose(query, [source()], {50: {"rows": {"PPV10.art": [row]}}},
                                         START, STOP, NOW)
    if abs(seconds) <= 1800:
        assert score.call_count == parse.call_count == 1
        assert coverage["event"]["start"] == begin.astimezone(timezone.utc).isoformat()
        assert coverage["event"]["stop"] == end.astimezone(timezone.utc).isoformat()
        assert bool(rows) is not ended
        assert (coverage["current"] is None) is ended
    else:
        assert rows == []
        assert coverage["event"] is None
        assert coverage["current"] is coverage["next"] is None
        assert score.call_count == parse.call_count == 0


@pytest.mark.parametrize("title", ["ONE Fight Night 48 Stamp vs. Flores", "ONE Fight Night 47 Stamp vs. Jones"])
def test_composition_keeps_event_conflict_checks_with_explicit_binding(title):
    query = guides._query(profile(), channel(
        name="ONE Fight Night 47 Stamp vs. Flores @ Sep 04 09:00 PM", tvg_id="",
    ), {"channel_id": 1, "source_id": 50, "tvg_id": "PPV10.art"}, NOW)
    with patch.object(guides, "_score_parsed_pair", wraps=guides._score_parsed_pair) as score:
        rows, coverage = guides._compose(query, [source()],
                                         {50: {"rows": {"PPV10.art": [programme("PPV10.art", title)]}}},
                                         START, STOP, NOW)
    assert score.call_count == 1
    assert rows == []
    assert coverage["event"] is None
    assert coverage["match"] == "unresolved"


def test_composition_skips_schedules_without_static_identity():
    query = guides._query(profile(), channel(), None, NOW)
    entry = {"rows": {"Unrelated.us": [programme("Unrelated.us")]},
             "headers": {"Unrelated.us": ET.fromstring('<channel id="Unrelated.us"><display-name>Unrelated</display-name></channel>')},
             "channel_warnings": {query["key"]: ["schedule_pending"]}}
    with patch.object(guides, "programme_times", wraps=guides.programme_times) as times:
        rows, coverage = guides._compose(query, [source()], {50: entry}, START, STOP, NOW)
    assert rows == []
    assert coverage["current"] is coverage["next"] is coverage["event"] is None
    assert coverage["warnings"] == ["schedule_pending"]
    assert times.call_count == 0


@pytest.mark.asyncio
async def test_long_source_duration_is_diagnosed_and_not_rendered(monkeypatch):
    install_feed(monkeypatch, feed(programme(stop="20260907050000 +0000")))
    profiles, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
    assert profiles[0]["source_programmes"][1] == []
    assert "implausible_schedule" in coverage["channels"][0]["warnings"]


@pytest.mark.parametrize("title,expected", [
    ("No EVENT Today", True), ("Signing Off", True), ("SIGN OFF", True),
    ("Next EVENT: Hockey on Saturday", True), ("The Signing Off Story", False),
    ("No Events: A Documentary", False), ("Off-Air with Friends", False),
])
def test_placeholder_filter_is_specific(title, expected):
    assert guides._placeholder(programme(title=title)) is expected


@pytest.mark.asyncio
async def test_source_icons_variants_and_portrait_banner_settings_survive(monkeypatch):
    from types import SimpleNamespace
    install_feed(monkeypatch, feed(
        programme(title="MLB Baseball", children="<sub-title>New York Yankees at Boston Red Sox</sub-title>"),
        headers='<channel id="ESPN.us"><display-name>ESPN</display-name><icon src="https://logos.example/espn.png"/></channel>',
    ))
    with patch("config.get_settings", return_value=SimpleNamespace(
        event_sync_team_aliases=[], sports_banner_base_url="https://game-thumbs.example",
        sports_banner_leagues=None,
    )):
        profiles, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
    xml = ET.fromstring(generate_xmltv(profiles, {1: channel()}))
    assert xml.find("channel/icon").get("src") == "https://logos.example/espn.png"
    actual = next(row for row in xml.findall("programme") if row.findtext("title") == "MLB Baseball")
    assert "/cover?style=4" in actual.find("icon").get("src")
    assert "fallback=true" in actual.find("icon").get("src")
    assert "missing_artwork" not in coverage["channels"][0]["warnings"]
    profiles[0]["pattern_variants"] = [{"name": "ESPN", "title_pattern": "(?<title>ESPN)",
                                       "program_poster_url_template": "https://custom.example/portrait.jpg"}]
    actual = next(row for row in ET.fromstring(generate_xmltv(profiles, {1: channel()})).findall("programme")
                  if row.findtext("title") == "MLB Baseball")
    assert actual.find("icon").get("src") == "https://custom.example/portrait.jpg"


@pytest.mark.asyncio
async def test_uncached_portraits_are_probed_once_without_delaying_guides(monkeypatch, tmp_path):
    from cache import Cache
    from services import epg_artwork
    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    cache = Cache()
    api = client()
    monkeypatch.setattr(guides, "get_cache", lambda: cache)
    landscape = "https://tmsimg.com/assets/p12345_b_h3_aa.jpg"
    install_feed(monkeypatch, feed(programme(children=f'<icon src="{landscape}"/>')))
    started, release = asyncio.Event(), asyncio.Event()

    async def probe(cache, unknown):
        started.set()
        await release.wait()
        for key in unknown:
            cache.put(key, "v12")
        cache.save()
        return len(unknown)

    with patch.object(epg_artwork, "probe_unknown", side_effect=probe) as mock_probe:
        first, first_coverage = await guides.prepare_profiles([profile()], {1: channel()}, api, now=NOW, wait_for_sources=True)
        await asyncio.wait_for(started.wait(), timeout=0.1)
        task = guides._ARTWORK_LOAD
        try:
            assert first[0]["source_programmes"][1][0].find("icon").get("src") == landscape
            assert first_coverage["artwork_pending"] is True
            assert guides.can_cache(first_coverage)
            for key in ("dummy_epg_xmltv_all", "dummy_epg_xmltv_1"):
                cache.set(key, "complete guide")
            second, second_coverage = await guides.prepare_profiles([profile()], {1: channel()}, api, now=NOW, wait_for_sources=True)
            assert second_coverage["artwork_pending"] is True
            assert guides.can_cache(second_coverage)
            assert mock_probe.await_count == 1
            assert cache.get("dummy_epg_xmltv_all") == "complete guide"
            assert cache.get("dummy_epg_xmltv_1") == "complete guide"
        finally:
            release.set()
            await task
        assert cache.get("dummy_epg_xmltv_all") is None
        assert cache.get("dummy_epg_xmltv_1") is None
        third, third_coverage = await guides.prepare_profiles([profile()], {1: channel()}, api, now=NOW, wait_for_sources=True)
        assert third[0]["source_programmes"][1][0].find("icon").get("src") == "https://tmsimg.com/assets/p12345_b_v12_aa.jpg"
        assert guides.can_cache(third_coverage)
        assert third_coverage["artwork_pending"] is False
        assert mock_probe.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["error", "timeout", "cancelled"])
async def test_portrait_probe_failures_release_the_task_and_retry_later(monkeypatch, tmp_path, failure):
    from services import epg_artwork
    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    monkeypatch.setattr(guides, "ARTWORK_WAIT", 0.01)
    install_feed(monkeypatch, feed(programme(children='<icon src="https://tmsimg.com/assets/p12345_b_h3_aa.jpg"/>')))

    async def probe(cache, unknown):
        if failure == "timeout":
            await asyncio.Event().wait()
        elif failure == "cancelled":
            raise asyncio.CancelledError
        else:
            raise RuntimeError("unreachable")

    with patch.object(epg_artwork, "probe_unknown", side_effect=probe) as mock_probe, patch.object(guides, "get_cache") as cache:
        first, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
        task = guides._ARTWORK_LOAD
        await asyncio.gather(task, return_exceptions=True)
        assert guides._ARTWORK_LOAD is None
        assert guides.can_cache(coverage)
        assert first[0]["source_programmes"][1][0].find("icon").get("src").endswith("_h3_aa.jpg")
        await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
        assert mock_probe.await_count == 1
        assert guides._ARTWORK_LOAD is None
        cache.return_value.invalidate_prefix.assert_called_with("dummy_epg_xmltv")


@pytest.mark.asyncio
async def test_portrait_probe_batch_is_bounded(monkeypatch, tmp_path):
    from services import epg_artwork
    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    icons = "".join(f'<icon src="https://tmsimg.com/assets/p{10000 + i}_b_h3_aa.jpg"/>' for i in range(guides.MAX_ARTWORK + 2))
    install_feed(monkeypatch, feed(programme(children=icons)))
    with patch.object(epg_artwork, "probe_unknown", new_callable=AsyncMock) as probe:
        await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
        await guides._ARTWORK_LOAD
        assert len(probe.await_args.args[1]) == guides.MAX_ARTWORK


@pytest.mark.asyncio
async def test_missing_portrait_is_remembered_without_repeated_probes(monkeypatch, tmp_path):
    from services import epg_artwork
    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    original = "https://tmsimg.com/assets/p12345_b_h3_aa.jpg"
    install_feed(monkeypatch, feed(programme(children=f'<icon src="{original}"/>')))
    with patch.object(epg_artwork, "_probe", new=AsyncMock(return_value=None)) as probe:
        await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
        await guides._ARTWORK_LOAD
        prepared, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
        assert prepared[0]["source_programmes"][1][0].find("icon").get("src") == original
        assert guides.can_cache(coverage)
        probe.assert_awaited_once()


@pytest.mark.asyncio
async def test_cache_aggregate_ceiling_evicts_oldest_complete_schedule(monkeypatch):
    async def read(*_):
        return {"headers": {}, "rows": {}, "warnings": [], "size": 60}
    monkeypatch.setattr(guides, "_read_source", read)
    monkeypatch.setattr(guides, "MAX_CACHE", 100)
    await guides._load_source("old", source(), [], START, STOP, NOW)
    await guides._load_source("new", source(51), [], START, STOP, NOW)
    assert list(guides._SOURCE_CACHE) == ["new"]


@pytest.mark.asyncio
async def test_streamed_transport_enforces_wire_limit_and_sanitizes_errors():
    class Chunks(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"<tv>" + b" " * 100 + b"</tv>"
    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=Chunks()))
    with pytest.raises(Exception, match="download exceeds"):
        async for _ in stream_xmltv(source(), max_download=10, max_decoded=1000, transport=transport):
            pass
    def failure(request):
        raise httpx.ConnectError("credential hidden-in-upstream-error", request=request)
    with pytest.raises(Exception) as caught:
        async for _ in stream_xmltv(source(), max_download=100, max_decoded=100, transport=httpx.MockTransport(failure)):
            pass
    assert "hidden-in-upstream-error" not in str(caught.value)


@pytest.mark.asyncio
async def test_streamed_transport_revalidates_redirects():
    transport = httpx.MockTransport(lambda request: httpx.Response(302, headers={"location": "http://127.0.0.1/private"}))
    with pytest.raises(Exception, match="blocked"):
        async for _ in stream_xmltv(source(), max_download=1000, max_decoded=1000, transport=transport):
            pass


@pytest.mark.asyncio
async def test_legacy_profile_precedence_matches_coverage(monkeypatch):
    install_feed(monkeypatch, feed(programme()))
    profiles, coverage = await guides.prepare_profiles(
        [profile(id=2), profile(id=1, epg_source_ids=[], fallback_title_template="Existing guide")],
        {1: channel()}, client(), now=NOW, wait_for_sources=True,
    )
    xml = ET.fromstring(generate_xmltv(profiles, {1: channel()}))
    assert len(xml.findall("channel")) == 1
    assert "SportsCenter" not in ET.tostring(xml, encoding="unicode")
    assert coverage["channels"] == []


@pytest.mark.asyncio
async def test_disabled_source_reports_reason_without_recursion(monkeypatch):
    loader = AsyncMock()
    monkeypatch.setattr(guides, "_read_source", loader)
    profiles, coverage = await guides.prepare_profiles(
        [profile()], {1: channel()}, client([source(is_active=False)]), now=NOW, wait_for_sources=True,
    )
    assert coverage["sources"][0]["status"] == "error"
    assert "disabled" in coverage["sources"][0]["error"]
    assert profiles[0]["source_programmes"][1] == []
    loader.assert_not_awaited()


@pytest.mark.asyncio
async def test_open_xml_element_is_bounded_before_its_closing_tag(monkeypatch):
    install_feed(monkeypatch, b"<tv><programme>" + b"A" * 1000)
    monkeypatch.setattr(guides, "MAX_RETAINED", 200)
    with pytest.raises(ValueError, match="element exceeds"):
        await guides._read_source(source(), [], START, STOP, NOW)


@pytest.mark.asyncio
async def test_failed_cache_entries_have_a_count_limit(monkeypatch):
    async def fail(*_):
        raise ValueError("unavailable")
    monkeypatch.setattr(guides, "_read_source", fail)
    monkeypatch.setattr(guides, "MAX_CACHE_ENTRIES", 1)
    await guides._load_source("old", source(), [], START, STOP, NOW)
    await guides._load_source("new", source(51), [], START, STOP, NOW)
    assert list(guides._SOURCE_CACHE) == ["new"]


@pytest.mark.asyncio
async def test_wrong_day_event_reports_date_conflict(monkeypatch):
    install_feed(monkeypatch, feed(programme("PPV(1)-05.v", "ONE FIGHT NIGHT 47 STAMP V FLORES",
                                            start="20260906010000 +0000", stop="20260906040000 +0000")))
    profiles, coverage = await guides.prepare_profiles(
        [profile()], {1: channel(name="ONE Fight Night 47 Stamp vs. Flores @ Sep 04 09:00 PM", tvg_id="")},
        client(), now=NOW, wait_for_sources=True,
    )
    assert profiles[0]["source_programmes"][1] == []
    assert "event_date_conflict" in coverage["channels"][0]["warnings"]


@pytest.mark.asyncio
async def test_original_ended_event_remains_evidence_without_expired_xml(monkeypatch):
    install_feed(monkeypatch, feed(programme(title="ONE Fight Night 47", stop="20260905013000 +0000")))
    channels = {1: channel(name="ONE Fight Night 47 @ Sep 04 09:00 PM", tvg_id="")}
    profiles, coverage = await guides.prepare_profiles([profile()], channels, client(), now=NOW, wait_for_sources=True)
    event = coverage["channels"][0]["event"]
    assert event is not None
    assert event["start"] == "2026-09-05T01:00:00+00:00"
    assert event["stop"] == "2026-09-05T01:30:00+00:00"
    assert coverage["channels"][0]["current"] is None
    assert profiles[0]["source_programmes"][1] == []
    assert all(row.findtext("title") != "ONE Fight Night 47" for row in ET.fromstring(generate_xmltv(profiles, channels)).findall("programme"))


@pytest.mark.asyncio
async def test_ended_event_evidence_counts_toward_source_limits(monkeypatch):
    install_feed(monkeypatch, feed(programme(title="ONE Fight Night 47", stop="20260905013000 +0000")))
    monkeypatch.setattr(guides, "MAX_PROGRAMMES", 0)
    query = guides._query(profile(), channel(name="ONE Fight Night 47 @ Sep 04 09:00 PM", tvg_id=""), None, NOW)
    with pytest.raises(ValueError, match="retained size limit"):
        await guides._read_source(source(), [query], START, STOP, NOW)


@pytest.mark.asyncio
async def test_cached_original_event_becomes_ended_evidence(monkeypatch):
    install_feed(monkeypatch, feed(programme(title="ONE Fight Night 47", stop="20260905021000 +0000")))
    channels = {1: channel(name="ONE Fight Night 47 @ Sep 04 09:00 PM", tvg_id="")}
    upstream = client()
    _, first = await guides.prepare_profiles([profile()], channels, upstream, now=NOW, wait_for_sources=True)
    assert first["channels"][0]["current"] is not None
    _, later = await guides.prepare_profiles([profile()], channels, upstream, now=NOW + timedelta(minutes=11), wait_for_sources=True)
    assert later["channels"][0]["current"] is None
    assert later["channels"][0]["event"]["stop"] == "2026-09-05T02:10:00+00:00"


GOOD = feed(programme())
PACKED = gzip.compress(GOOD)
CAFE = feed(programme(title="Café"))
SPLIT = CAFE.index(b"\xc3\xa9") + 1
LATIN = (b'<?xml version="1.0" encoding="ISO-8859-1"?><tv><channel id="ESPN.us"><display-name>Caf\xe9</display-name></channel>'
         + ET.tostring(programme()) + b"</tv>")


@pytest.mark.asyncio
@pytest.mark.parametrize("chunks,headers,url,title,expected", [
    ((GOOD,), {"content-type": "application/xml"}, "https://example.com/50.xml", "SportsCenter",
     {"wire_bytes": len(GOOD), "decoded_bytes": len(GOOD), "compression": "identity", "content_encoding": "absent", "content_type": "xml"}),
    ((PACKED,), {"content-encoding": "gzip"}, "https://example.com/50.xml", "SportsCenter",
     {"wire_bytes": len(PACKED), "decoded_bytes": len(GOOD), "compression": "gzip", "content_encoding": "gzip", "content_type": "absent"}),
    ((PACKED,), {}, "https://example.com/50.xml.gz", "SportsCenter",
     {"wire_bytes": len(PACKED), "decoded_bytes": len(GOOD), "compression": "gzip", "content_encoding": "absent"}),
    ((CAFE[:SPLIT], CAFE[SPLIT:]), {}, "https://example.com/50.xml", "Café",
     {"wire_bytes": len(CAFE), "decoded_bytes": len(CAFE), "compression": "identity"}),
    (tuple(LATIN[index:index + 7] for index in range(0, len(LATIN), 7)), {}, "https://example.com/50.xml", "SportsCenter",
     {"decoded_bytes": len(LATIN), "compression": "identity"}),
], ids=["identity", "gzip_header", "gz_suffix", "split_utf8", "latin1_declared"])
async def test_successful_source_load_accounts_wire_and_decoded_bytes(monkeypatch, chunks, headers, url, title, expected):
    install_transport(monkeypatch, lambda request: reply(*chunks, headers=headers))
    _, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client([source(url=url)]), now=NOW, wait_for_sources=True)
    entry = coverage["sources"][0]
    assert entry["status"] == "ready" and entry["error"] is None
    assert coverage["channels"][0]["current"]["title"] == title
    diagnostics = bounded(entry["diagnostics"])
    assert {key: diagnostics.get(key) for key in expected} == expected
    assert {key: diagnostics.get(key) for key in ("http_status", "transport_complete", "xml_complete", "root")} == {
        "http_status": 200, "transport_complete": True, "xml_complete": True, "root": "tv"}
    assert not ({"failure"} | PARSER_KEYS) & set(diagnostics)
    assert "8859" not in json.dumps(diagnostics)


@pytest.mark.asyncio
@pytest.mark.parametrize("chunks,headers,failure,error,expected", [
    ((b"<tv><channel></tv>",), {}, None, "Malformed XML.",
     {"failure": "malformed_xml", "root": "tv", "transport_complete": True, "xml_complete": False}),
    ((b"<html><body>", f"{SECRET}</body>".encode(), b"</html>"), {"content-type": "text/html; charset=utf-8"}, None,
     "XMLTV root must be tv.", {"failure": "wrong_root", "root": "other", "content_type": "html"}),
    ((b'<tv><channel id="ESPN.us">',), {}, None, "Malformed XML.",
     {"failure": "incomplete_xml", "root": "tv", "transport_complete": True, "xml_complete": False}),
    ((b"",), {}, None, "Malformed XML.", {"failure": "incomplete_xml", "root": "absent", "transport_complete": True}),
    ((b"<tv><channel id=",), {}, httpx.RemoteProtocolError("peer closed connection"), "Connection failed.",
     {"failure": "incomplete_body", "transport_complete": False, "xml_complete": False, "wire_bytes": 16}),
    ((PACKED[:-20],), {"content-encoding": "gzip"}, None, "XMLTV gzip is incomplete.",
     {"failure": "incomplete_gzip", "compression": "gzip", "content_encoding": "gzip", "transport_complete": True, "xml_complete": False}),
    ((b'<tv><channel id="a"><display-name>A\x01B</display-name></channel></tv>',), {}, None, "Malformed XML.",
     {"failure": "forbidden_character", "root": "tv"}),
    ((b'<tv><channel id="a"><display-name>Caf\xe9</display-name></channel></tv>',), {}, None, "Malformed XML.",
     {"failure": "invalid_utf8", "root": "tv"}),
    ((b'<tv><channel id="a"><display-name>Caf\xc3', b" latte</display-name></channel></tv>"), {}, None, "Malformed XML.",
     {"failure": "invalid_utf8", "root": "tv"}),
], ids=["malformed", "wrong_root", "incomplete_xml", "empty", "incomplete_body", "incomplete_gzip",
        "forbidden_character", "invalid_utf8", "split_invalid_utf8"])
async def test_failed_source_load_classifies_only_the_proven_cause(monkeypatch, chunks, headers, failure, error, expected):
    install_transport(monkeypatch, lambda request: reply(*chunks, headers=headers, failure=failure))
    _, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
    entry = coverage["sources"][0]
    assert entry["status"] == "error" and entry["error"] == error
    assert set(entry) == {"source_id", "status", "last_success", "error", "diagnostics"}
    diagnostics = bounded(entry["diagnostics"])
    assert {key: diagnostics.get(key) for key in expected} == expected
    if error == "Malformed XML.":
        assert PARSER_KEYS <= set(diagnostics)
    else:
        assert not PARSER_KEYS & set(diagnostics)
    assert SECRET not in json.dumps(coverage, default=str)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["url_and_headers", "status_body", "transport_exception", "declared_encoding"])
async def test_diagnostics_never_serialize_url_header_body_or_exception_text(monkeypatch, case):
    def refuse(request):
        raise httpx.ReadError(SECRET, request=request)
    handler, error, expected = {
        "url_and_headers": (
            lambda request: reply(f"<html>{SECRET}</html>".encode(),
                                  headers={"content-type": f"application/{SECRET}", "content-encoding": SECRET}),
            "XMLTV root must be tv.",
            {"root": "other", "content_type": "other", "content_encoding": "other", "compression": "identity"}),
        "status_body": (lambda request: reply(SECRET.encode(), status=503), "HTTP status 503.", {"http_status": 503}),
        "transport_exception": (refuse, "Connection failed.", {"http_status": None, "transport_complete": False}),
        "declared_encoding": (lambda request: reply(f'<?xml version="1.0" encoding="{SECRET}"?><tv/>'.encode()),
                              "Malformed XML.", {}),
    }[case]
    install_transport(monkeypatch, handler)
    upstream = client([source(url=f"https://example.com/50.xml?key={SECRET}")])
    _, coverage = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
    entry = coverage["sources"][0]
    assert entry["status"] == "error" and entry["error"] == error
    diagnostics = bounded(entry["diagnostics"])
    assert {key: diagnostics.get(key) for key in expected} == expected
    if case == "declared_encoding":
        assert diagnostics["failure"] in {"unknown", "malformed_xml"}
    assert SECRET not in json.dumps(coverage, default=str)


@pytest.mark.asyncio
async def test_retry_success_clears_failure_diagnostics_without_cross_source_leakage(monkeypatch):
    attempts = iter([reply(GOOD[:40], failure=asyncio.CancelledError()), reply(b"<tv><channel></tv>"), reply(GOOD)])
    install_transport(monkeypatch, lambda request: reply(GOOD) if request.url.path == "/51.xml" else next(attempts))
    query = guides._query(profile(), channel(), None, NOW)
    with pytest.raises(asyncio.CancelledError):
        await guides._load_source("retried", source(), [query], START, STOP, NOW)
    assert guides._SOURCE_CACHE["retried"]["error"] == "XMLTV source loading was cancelled."
    await guides._load_source("retried", source(), [query], START, STOP, NOW)
    await guides._load_source("other", source(51), [query], START, STOP, NOW)
    broken = guides._SOURCE_CACHE["retried"]
    assert broken["error"] == "Malformed XML."
    assert bounded(broken["diagnostics"])["failure"] == "malformed_xml"
    assert "failure" not in bounded(guides._SOURCE_CACHE["other"]["diagnostics"])
    await guides._load_source("retried", source(), [query], START, STOP, NOW)
    recovered = guides._SOURCE_CACHE["retried"]
    assert recovered["error"] is None and recovered["rows"]
    diagnostics = bounded(recovered["diagnostics"])
    assert diagnostics["transport_complete"] is True and diagnostics["xml_complete"] is True
    assert not ({"failure"} | PARSER_KEYS) & set(diagnostics)


@pytest.mark.asyncio
async def test_public_reads_queue_selection_without_starting_source_download(monkeypatch):
    install_feed(monkeypatch, feed(programme()))
    upstream = client()
    with patch.object(guides, "_read_source", wraps=guides._read_source) as read:
        for _ in range(3):
            _, coverage = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW)
        assert read.await_count == 0
        assert coverage["channels"][0]["current"] is None
        await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
        assert read.await_count == 1


@pytest.mark.asyncio
async def test_changed_queries_share_source_backoff_and_event_scope(monkeypatch):
    install_feed(monkeypatch, feed(programme()))
    upstream = client()
    with patch.object(guides, "_read_source", wraps=guides._read_source) as read:
        await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
        event_channel = channel(id=2, name="ESPN PLUS 12", tvg_id="")
        _, coverage = await guides.prepare_profiles([profile()], {2: event_channel}, upstream, now=NOW, wait_for_sources=True)
        assert read.await_count == 1
        assert coverage["channels"][0]["event"] is None
        assert coverage["channels"][0]["current"] is None
        entry = next(iter(guides._SOURCE_CACHE.values()))
        entry["checked"] -= guides.SOURCE_TTL + 1
        await guides.prepare_profiles([profile()], {2: event_channel}, upstream, now=NOW, wait_for_sources=True)
        assert read.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("name,headers,expected", [
    ("TNT", '<channel id="us"><display-name>US - TNT</display-name></channel>', "us"),
    ("TNT", '<channel id="us"><display-name>US - TNT</display-name></channel><channel id="ca"><display-name>CA - TNT</display-name></channel>', "us"),
    ("TNT", '<channel id="us"><display-name>US - TNT</display-name></channel><channel id="other"><display-name>US - TNT</display-name></channel>', None),
    ("TNT", '<channel id="us"><display-name>US - TNT East</display-name></channel>', None),
    ("ESPN PLUS 12", '<channel id="us"><display-name>ESPN PLUS 12</display-name></channel>', None),
])
async def test_static_name_fallback_is_unique_and_country_limited(monkeypatch, name, headers, expected):
    install_feed(monkeypatch, feed(programme(tvg="us"), programme(tvg="ca"), programme(tvg="other"), headers=headers))
    _, coverage = await guides.prepare_profiles([profile()], {1: channel(name=name, tvg_id="")}, client(), now=NOW, wait_for_sources=True)
    assert coverage["channels"][0]["source_tvg_id"] == expected


@pytest.mark.asyncio
async def test_unrelated_invalid_schedule_does_not_warn_a_matched_channel(monkeypatch):
    invalid = programme(tvg="Other", start="invalid")
    install_feed(monkeypatch, feed(programme(), invalid))
    _, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), now=NOW, wait_for_sources=True)
    assert "invalid_schedule" not in coverage["channels"][0]["warnings"]
    assert "invalid_schedule" in coverage["sources"][0]["warnings"]


@pytest.mark.parametrize("artwork_pending", [False, True])
def test_complete_partial_output_is_cacheable(artwork_pending):
    assert guides.can_cache({"sources": [{"status": "ready"}, {"status": "error"}], "artwork_pending": artwork_pending})
    assert guides.can_cache({"sources": [{"status": "error", "last_success": NOW.isoformat()}], "artwork_pending": artwork_pending})
    for status in ("pending", "error", "stale"):
        assert not guides.can_cache({"sources": [{"status": status}], "artwork_pending": artwork_pending})


@pytest.mark.asyncio
async def test_dated_event_filler_does_not_become_schedule_evidence(monkeypatch):
    install_feed(monkeypatch, feed())
    channels = {1: channel(name="ONE Fight Night 47 @ Sep 04 09:00 PM", tvg_id="")}
    profiles, coverage = await guides.prepare_profiles([profile()], channels, client(), now=NOW, wait_for_sources=True)
    row = coverage["channels"][0]
    assert row["event"] is None and row["current"] is None and row["next"] is None
    assert row["real_minutes"] == 0
    assert profiles[0]["source_programmes"][1] == []
    xml = generate_xmltv(profiles, channels)
    assert "ONE Fight Night 47" in xml
    assert profiles[0]["source_programmes"][1] == []


@pytest.mark.asyncio
async def test_streaming_parser_discards_completed_unselected_elements(monkeypatch):
    import itertools
    import tracemalloc
    row = ET.tostring(programme(tvg="unselected", children="<desc>" + "x" * 4096 + "</desc>"))
    block = row * 16
    chunks = itertools.chain((b"<tv>",), itertools.repeat(block, 256), (b"</tv>",))
    install_transport(monkeypatch, lambda request: httpx.Response(200, stream=Body(chunks, None)))
    tracemalloc.start()
    try:
        loaded = await guides._read_source(source(), [], START, STOP, NOW)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert loaded["diagnostics"]["decoded_bytes"] > 16 * 1024 * 1024
    assert loaded["diagnostics"]["xml_complete"] is True
    assert loaded["rows"] == {} and loaded["headers"] == {}
    assert loaded["size"] == 0
    assert peak < 8 * 1024 * 1024


@pytest.mark.asyncio
async def test_source_download_finishes_before_slow_programme_selection(monkeypatch):
    clock = 0
    received = 0
    original = guides.programme_times
    row = ET.tostring(programme(children='<icon src="https://example.com/portrait.jpg" />'))

    def select(element):
        nonlocal clock
        clock += 1
        return original(element)

    def chunks():
        nonlocal received
        yield b'<tv><channel id="ESPN.us"><display-name>ESPN</display-name></channel>'
        for _ in range(8):
            # The producer's response expires if selection delays its next read.
            if clock:
                return
            received += 1
            yield row
        yield b'</tv>'

    monkeypatch.setattr(guides, "programme_times", select)
    install_transport(monkeypatch, lambda request: httpx.Response(200, stream=Body(chunks(), None)))
    query = guides._query({}, {"id": 1, "name": "ESPN", "tvg_id": "ESPN.us"}, None, NOW)
    loaded = await guides._read_source(source(), [query], START, STOP, NOW)

    assert received == 8
    assert loaded["diagnostics"]["transport_complete"] is True
    assert loaded["diagnostics"]["xml_complete"] is True
    assert len(loaded["rows"]["ESPN.us"]) == 8
    assert all(item.find("icon").get("src") == "https://example.com/portrait.jpg"
               for item in loaded["rows"]["ESPN.us"])


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["complete", "malformed", "transport", "disk", "cancel"])
async def test_source_temporary_file_is_private_and_closed(monkeypatch, tmp_path, outcome):
    import errno
    import os
    import tempfile

    opened = []
    create = tempfile.TemporaryFile
    to_thread = asyncio.to_thread

    def temporary(*args, **kwargs):
        assert kwargs["dir"] == tmp_path
        item = create(*args, **kwargs)
        assert os.fstat(item.fileno()).st_mode & 0o077 == 0
        opened.append(item)
        return item

    async def run(function, *args, **kwargs):
        if outcome == "disk" and getattr(function, "__name__", None) == "write":
            raise OSError(errno.ENOSPC, "No space left on device")
        return await to_thread(function, *args, **kwargs)

    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    monkeypatch.setattr(tempfile, "TemporaryFile", temporary)
    monkeypatch.setattr(asyncio, "to_thread", run)
    content = b"<tv>" if outcome == "malformed" else b"<tv></tv>"
    failure = (httpx.ReadTimeout("upstream stalled") if outcome == "transport"
               else asyncio.CancelledError() if outcome == "cancel" else None)
    install_transport(monkeypatch, lambda request: reply(content, failure=failure))

    if outcome == "complete":
        loaded = await guides._read_source(source(), [], START, STOP, NOW)
        assert loaded["diagnostics"]["xml_complete"] is True
    else:
        expected = (ET.ParseError if outcome == "malformed" else OSError if outcome == "disk"
                    else asyncio.CancelledError if outcome == "cancel" else Exception)
        with pytest.raises(expected):
            await guides._read_source(source(), [], START, STOP, NOW)
    assert len(opened) == 1 and opened[0].closed
    assert list(tmp_path.iterdir()) == []

    if outcome == "disk":
        previous = {"success": NOW, "rows": {"ESPN.us": [programme()]}, "checked": 0, "size": 0}
        guides._SOURCE_CACHE["existing"] = previous
        await guides._load_source("existing", source(), [], START, STOP, NOW)
        assert guides._SOURCE_CACHE["existing"]["rows"] is previous["rows"]
        assert guides._SOURCE_CACHE["existing"]["success"] == NOW
        assert guides._SOURCE_CACHE["existing"]["error"]
        assert all(item.closed for item in opened)
        assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_cancelled_source_download_closes_temporary_file(monkeypatch, tmp_path):
    import tempfile

    waiting = asyncio.Event()
    opened = []
    create = tempfile.TemporaryFile

    def temporary(*args, **kwargs):
        item = create(*args, **kwargs)
        opened.append(item)
        return item

    async def chunks(*args, **kwargs):
        yield b"<tv>"
        waiting.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    monkeypatch.setattr(tempfile, "TemporaryFile", temporary)
    monkeypatch.setattr(guides, "stream_xmltv", chunks)
    task = asyncio.create_task(guides._read_source(source(), [], START, STOP, NOW))
    await asyncio.wait_for(waiting.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(opened) == 1 and opened[0].closed
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("tail,code", [(b"", 3), (b"<programme", 5), (b"\xc3", 6)])
async def test_incomplete_source_retries_once_with_fresh_selection(monkeypatch, tmp_path, tail, code):
    import tempfile

    attempts = 0
    opened = []
    create = tempfile.TemporaryFile
    broken = feed(programme(title="Discarded partial programme"))[:-5] + tail

    def temporary(*args, **kwargs):
        assert all(item.closed for item in opened)
        item = create(*args, **kwargs)
        opened.append(item)
        return item

    def response(request):
        nonlocal attempts
        attempts += 1
        return reply(broken if attempts == 1 else GOOD)

    with pytest.raises(ET.ParseError) as parsed:
        ET.fromstring(broken)
    assert parsed.value.code == code
    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    monkeypatch.setattr(tempfile, "TemporaryFile", temporary)
    install_transport(monkeypatch, response)
    await guides._load_source("retry", source(), [guides._query(profile(), channel(), None, NOW)], START, STOP, NOW)

    entry = guides._SOURCE_CACHE["retry"]
    assert attempts == 2 and entry["error"] is None
    assert entry["diagnostics"]["attempts"] == 2
    assert entry["diagnostics"]["xml_complete"] is True
    assert PARSER_KEYS.isdisjoint(entry["diagnostics"])
    assert [item.findtext("title") for item in entry["rows"]["ESPN.us"]] == ["SportsCenter"]
    assert len(opened) == 2 and all(item.closed for item in opened)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_repeated_incomplete_source_keeps_last_complete_snapshot(monkeypatch):
    attempts = 0
    previous = {"success": NOW, "rows": {"ESPN.us": [programme(title="Last complete guide")]}, "size": 0}
    guides._SOURCE_CACHE["retry"] = previous

    def response(request):
        nonlocal attempts
        attempts += 1
        return reply(feed(programme(title="Incomplete replacement"))[:-5])

    install_transport(monkeypatch, response)
    await guides._load_source("retry", source(), [guides._query(profile(), channel(), None, NOW)], START, STOP, NOW)
    entry = guides._SOURCE_CACHE["retry"]
    assert attempts == 2
    assert entry["error"] == "Malformed XML."
    assert entry["diagnostics"]["attempts"] == 2
    assert entry["rows"] is previous["rows"] and entry["success"] == NOW


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [
    b"<tv><programme></tv>", b"<html>", b"<tv>\xff</tv>",
    b"<!DOCTYPE tv><tv></tv>", b"<tv>\x00</tv>",
])
async def test_source_content_errors_do_not_retry(monkeypatch, content):
    attempts = 0

    def response(request):
        nonlocal attempts
        attempts += 1
        return reply(content)

    install_transport(monkeypatch, response)
    await guides._load_source("failure", source(), [], START, STOP, NOW)
    assert attempts == 1
    assert guides._SOURCE_CACHE["failure"]["error"]
    assert guides._SOURCE_CACHE["failure"]["diagnostics"]["attempts"] == 1
    assert not guides._SOURCE_CACHE["failure"].get("success")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["size", "disk", "security", "timeout", "cancel"])
async def test_source_resource_and_security_failures_do_not_retry(monkeypatch, failure):
    from fastapi import HTTPException

    attempts = 0
    errors = {"size": HTTPException(413, "XMLTV decoded content exceeds its size limit."),
              "disk": OSError("No space left on device"),
              "security": HTTPException(400, "XMLTV source URL is blocked by the outbound security policy."),
              "timeout": TimeoutError(), "cancel": asyncio.CancelledError()}

    async def read(*args):
        nonlocal attempts
        attempts += 1
        raise errors[failure]

    monkeypatch.setattr(guides, "_read_source", read)
    if failure == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await guides._load_source("failure", source(), [], START, STOP, NOW)
    else:
        await guides._load_source("failure", source(), [], START, STOP, NOW)
    assert attempts == 1
    assert guides._SOURCE_CACHE["failure"]["error"]
    assert guides._SOURCE_CACHE["failure"]["diagnostics"]["attempts"] == 1


@pytest.mark.asyncio
async def test_source_retries_share_one_deadline_and_concurrency_slot(monkeypatch):
    attempts = 0
    slots = asyncio.Semaphore(1)
    deadline = None
    deadlines = []
    original_timeout = asyncio.timeout

    def timeout(delay):
        nonlocal deadline
        deadline = original_timeout(delay)
        deadlines.append(deadline)
        return deadline

    async def read(*args):
        nonlocal attempts
        attempts += 1
        assert slots.locked()
        if attempts == 1:
            try:
                ET.fromstring(b"<tv>")
            except ET.ParseError as exc:
                exc.diagnostics = {"root": "tv", "transport_complete": True,
                                   "xml_complete": False, "failure": "incomplete_xml"}
                raise
        # Move the original deadline to now; a fresh per-attempt budget cannot pass.
        deadline.reschedule(asyncio.get_running_loop().time())
        await asyncio.Event().wait()

    monkeypatch.setattr(guides, "_SOURCE_SLOTS", slots)
    monkeypatch.setattr(guides, "_read_source", read)
    monkeypatch.setattr(asyncio, "timeout", timeout)
    await guides._load_source("timed", source(), [], START, STOP, NOW)
    entry = guides._SOURCE_CACHE["timed"]
    assert attempts == 2 and not slots.locked()
    assert len(deadlines) == 1
    assert entry["error"] == "Request timed out."
    assert entry["diagnostics"] == {"attempts": 2}
    assert "success" not in entry and not entry.get("rows")


@pytest.mark.asyncio
async def test_programme_budget_remains_finite_and_transport_defaults_are_unchanged(monkeypatch):
    observed = []
    def respond(request):
        observed.append(request.extensions["timeout"]["read"])
        return reply(GOOD)
    transport = httpx.MockTransport(respond)
    assert b"".join([piece async for piece in stream_xmltv(
        source(), max_download=len(GOOD), max_decoded=len(GOOD), transport=transport,
    )]) == GOOD
    install_transport(monkeypatch, respond)
    await guides._read_source(source(), [], START, STOP, NOW)
    assert observed == [30.0, 300.0]
    monkeypatch.setattr(guides, "MAX_DECODED", len(GOOD) - 1)
    await guides._load_source("bounded", source(), [], START, STOP, NOW)
    assert guides._SOURCE_CACHE["bounded"]["error"] == "XMLTV decoded content exceeds its size limit."
    assert "success" not in guides._SOURCE_CACHE["bounded"]


@pytest.mark.asyncio
async def test_programme_total_timeout_never_publishes_partial_rows(monkeypatch):
    async def stalled(selected, **options):
        yield GOOD[:-5]
        await asyncio.Event().wait()
    monkeypatch.setattr(guides, "stream_xmltv", stalled)
    monkeypatch.setattr(guides, "SOURCE_TIMEOUT", 0.02)
    await guides._load_source("timed", source(), [guides._query(profile(), channel(), None, NOW)], START, STOP, NOW)
    entry = guides._SOURCE_CACHE["timed"]
    assert entry["error"] == "Request timed out."
    assert "success" not in entry and not entry.get("rows")


@pytest.mark.asyncio
async def test_failed_refresh_does_not_renew_completed_snapshot_age(monkeypatch):
    install_feed(monkeypatch, feed(programme()))
    upstream = client()
    await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
    entry = next(iter(guides._SOURCE_CACHE.values()))
    previous = datetime.now(timezone.utc) - timedelta(seconds=guides.SOURCE_MAX_AGE + 1)
    entry["success"] = previous
    entry["checked"] -= guides.SOURCE_TTL + 1
    monkeypatch.setattr(guides, "_read_source", AsyncMock(side_effect=ValueError("unavailable")))
    prepared, coverage = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
    assert coverage["sources"][0]["status"] == "stale"
    assert coverage["sources"][0]["last_success"] == previous.isoformat()
    assert prepared[0]["source_programmes"][1][0].get("stop") == "20260905050000 +0000"


@pytest.mark.asyncio
async def test_background_refresh_includes_demand_collected_by_public_reads(monkeypatch):
    install_feed(monkeypatch, feed(programme(), programme(tvg="TNT.us")))
    upstream = client()
    selected = profile(channel_group_ids=[], channel_assignments=[{"channel_id": 1}])
    pending = profile(channel_group_ids=[], channel_assignments=[{"channel_id": 2}])
    channels = {1: channel(), 2: channel(id=2, name="TNT", tvg_id="TNT.us")}
    with patch.object(guides, "_read_source", wraps=guides._read_source) as read:
        await guides.prepare_profiles([pending], channels, upstream, now=NOW)
        assert read.await_count == 0
        await guides.prepare_profiles([selected], channels, upstream, now=NOW, wait_for_sources=True)
        prepared, coverage = await guides.prepare_profiles([pending], channels, upstream, now=NOW)
        assert read.await_count == 1
        assert coverage["sources"][0]["status"] == "ready"
        assert prepared[0]["source_programmes"][2]


@pytest.mark.asyncio
async def test_last_completed_intervals_survive_a_changed_guide_window(monkeypatch):
    now = NOW + timedelta(days=1)
    install_feed(monkeypatch, feed(programme(start="20260906010000 +0000", stop="20260906030000 +0000")))
    upstream = client()
    await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=NOW, wait_for_sources=True)
    prepared, coverage = await guides.prepare_profiles([profile()], {1: channel()}, upstream, now=now)
    assert coverage["channels"][0]["current"]["title"] == "SportsCenter"
    assert prepared[0]["source_programmes"][1][0].get("stop") == "20260906030000 +0000"


@pytest.mark.asyncio
@pytest.mark.parametrize("encoding,redirect", [("gzip", False), ("x-gzip", False), (None, True)])
async def test_transport_negotiates_gzip_and_decodes_redirected_files(encoding, redirect):
    requests = []
    def respond(request):
        requests.append(request)
        if redirect and request.url.path == "/50.xml":
            return httpx.Response(302, headers={"location": "/50.xml.gz"})
        return reply(PACKED, headers={"content-encoding": encoding} if encoding else {})
    diagnostics = {}
    content = b"".join([piece async for piece in stream_xmltv(
        source(), max_download=len(PACKED), max_decoded=len(GOOD),
        transport=httpx.MockTransport(respond), diagnostics=diagnostics,
    )])
    assert content == GOOD
    assert all(request.headers["accept-encoding"] == "gzip, identity" for request in requests)
    assert diagnostics["compression"] == "gzip"
    assert diagnostics["wire_bytes"] == len(PACKED)
    assert diagnostics["decoded_bytes"] == len(GOOD)
    assert diagnostics["transport_complete"] is True


@pytest.mark.asyncio
async def test_overlapping_profiles_do_not_replace_the_first_query_demand(monkeypatch):
    install_feed(monkeypatch, feed(programme(title="ONE Fight Night 47")))
    channels = {1: channel(name="ONE Fight Night 47 @ Sep 04 09:00 PM", tvg_id="")}
    prepared, coverage = await guides.prepare_profiles(
        [profile(), profile(id=2, event_timezone="UTC")], channels, client(), now=NOW, wait_for_sources=True,
    )
    assert prepared[0]["source_programmes"][1]
    assert coverage["channels"][0]["current"]["title"] == "ONE Fight Night 47"


@pytest.mark.asyncio
async def test_pending_query_demand_is_bounded_without_claiming_unscanned_rows(monkeypatch):
    monkeypatch.setattr(guides, "MAX_QUERIES", 2)
    install_feed(monkeypatch, feed(programme()))
    channels = {index: channel(id=index, name=f"Station {index}") for index in range(1, 4)}
    prepared, coverage = await guides.prepare_profiles([profile()], channels, client(), now=NOW, wait_for_sources=True)
    entry = next(iter(guides._SOURCE_CACHE.values()))
    assert len(entry["demand"]) == 2
    assert len(entry["selection"]["queries"]) == 2
    assert prepared[0]["source_programmes"][3] == []
    assert coverage["channels"][2]["event"] is None
    assert "schedule_pending" in coverage["channels"][2]["warnings"]


@pytest.mark.asyncio
async def test_distinct_queries_for_one_channel_keep_separate_end_witnesses(monkeypatch):
    install_feed(monkeypatch, feed(
        programme(title="ONE Fight Night 47", stop="20260905013000 +0000"),
        programme(tvg="UFC.us", title="UFC 320", stop="20260905014500 +0000"),
    ))
    channels = {1: channel(
        name="ONE Fight Night 47 @ Sep 04 09:00 PM", tvg_id="",
        streams=[{"id": 5, "name": "UFC 320 @ Sep 04 09:00 PM"}],
    )}
    _, coverage = await guides.prepare_profiles(
        [profile(), profile(id=2, name_source="stream")], channels, client(), now=NOW, wait_for_sources=True,
    )
    witness = coverage["channels"][0]["event"]
    assert witness["title"] == "ONE Fight Night 47"
    assert witness["stop"] == "2026-09-05T01:30:00+00:00"


@pytest.mark.asyncio
async def test_promoted_channel_reuses_scanned_event_identity(monkeypatch):
    install_feed(monkeypatch, feed(programme(title="ONE Fight Night 47", stop="20260905013000 +0000")))
    upstream = client(
        sources=[source(), source(46, url="http://ecm:6100/api/dummy-epg/xmltv/1")],
        rows=[{"id": 900, "epg_source": 46, "tvg_id": "event-slot-99"}],
    )
    event = channel(
        id=-1, name="ONE Fight Night 47 @ Sep 04 09:00 PM", tvg_id="",
        streams=[{"id": 7, "name": "ONE Fight Night 47 @ Sep 04 09:00 PM", "tvg_id": "provider-slot"}],
    )
    with patch.object(guides, "_read_source", wraps=guides._read_source) as read:
        _, before = await guides.prepare_profiles([profile()], {-1: event}, upstream, now=NOW, wait_for_sources=True)
        event.update(id=99, name="ONE Fight Night 47", tvg_id="event-slot-99", epg_data_id=900)
        _, after = await guides.prepare_profiles([profile()], {99: event}, upstream, now=NOW)
        assert read.await_count == 1
        assert after["sources"][0]["status"] == "ready"
        assert after["channels"][0]["event"] == before["channels"][0]["event"]
        assert after["channels"][0]["event"]["title"] == "ONE Fight Night 47"
        event["streams"][0]["name"] = "ONE Fight Night 48 @ Sep 04 09:00 PM"
        _, changed = await guides.prepare_profiles([profile()], {99: event}, upstream, now=NOW)
        assert changed["channels"][0]["event"] is None
        assert read.await_count == 1


@pytest.mark.asyncio
async def test_source_status_changes_preserve_accumulated_query_demand(monkeypatch):
    install_feed(monkeypatch, feed(programme(), programme(tvg="TNT.us")))
    upstream = client()
    channels = {1: channel(), 2: channel(id=2, name="TNT", tvg_id="TNT.us")}
    selected = profile(channel_group_ids=[], channel_assignments=[{"channel_id": 1}])
    pending = profile(channel_group_ids=[], channel_assignments=[{"channel_id": 2}])
    await guides.prepare_profiles([pending], channels, upstream, now=NOW)
    upstream.get_epg_sources.return_value = [source(name="Renamed guide", status="success", updated_at=NOW.isoformat())]
    guides._CATALOGUE_CACHE[(upstream, None)]["checked"] -= guides.SOURCE_RETRY + 1
    await guides.prepare_profiles([selected], channels, upstream, now=NOW, wait_for_sources=True)
    prepared, coverage = await guides.prepare_profiles([pending], channels, upstream, now=NOW)
    assert len(guides._SOURCE_CACHE) == 1
    assert coverage["sources"][0]["status"] == "ready"
    assert prepared[0]["source_programmes"][2]


@pytest.mark.asyncio
async def test_background_completion_rechecks_the_current_programme_time(monkeypatch):
    clock = [NOW]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]
    async def chunks(selected, **options):
        yield GOOD
        clock[0] = NOW + timedelta(hours=4)
    monkeypatch.setattr(guides, "datetime", Clock)
    monkeypatch.setattr(guides, "stream_xmltv", chunks)
    prepared, coverage = await guides.prepare_profiles([profile()], {1: channel()}, client(), wait_for_sources=True)
    assert coverage["generated_at"] == clock[0].isoformat()
    assert coverage["sources"][0]["last_success"] == clock[0].isoformat()
    assert coverage["channels"][0]["current"] is None
    assert prepared[0]["source_programmes"][1] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("compressed", [False, True])
async def test_incomplete_xml_is_rejected_before_event_selection(monkeypatch, compressed):
    row = programme(title="Alpha vs Beta", start="20260905020000 +0000")
    document = feed(row)[:-5]
    content = gzip.compress(document) if compressed else document
    headers = {"content-encoding": "gzip"} if compressed else {}
    install_transport(monkeypatch, lambda request: reply(content, headers=headers))
    query = guides._query({}, channel(name="PPV 1", tvg_id=""), None, NOW)
    query["event"] = guides.ParsedEvent("Alpha vs Beta", "Alpha vs Beta", NOW, ("Alpha", "Beta"), None)
    with patch.object(guides, "programme_times", wraps=guides.programme_times) as times, \
            patch.object(guides, "_score_parsed_pair", wraps=guides._score_parsed_pair) as score:
        with pytest.raises(ET.ParseError) as failed:
            await guides._read_source(source(), [query], START, STOP, NOW)
    assert times.call_count == 0
    assert score.call_count == 0
    diagnostics = failed.value.diagnostics
    assert diagnostics["transport_complete"] is True
    assert diagnostics["xml_complete"] is False
    assert diagnostics["staged_bytes"] == len(document)
    assert diagnostics["validation_bytes"] == len(document)
    assert diagnostics["selection_bytes"] == 0
    assert diagnostics["selection_ms"] == 0


@pytest.mark.asyncio
async def test_complete_gzip_is_validated_then_selected_once(monkeypatch):
    row = programme(children='<sub-title>Highlights</sub-title><category>Sports</category>'
                             '<icon src="https://images.example/portrait.jpg"/>')
    document = feed(row) + b'<!-- trailing </tv> text is legal -->'
    compressed = gzip.compress(document)
    install_transport(monkeypatch, lambda request: httpx.Response(
        200, headers={"content-encoding": "gzip", "content-length": str(len(compressed))},
        stream=Body((compressed,), None), extensions={"http_version": b"HTTP/2"},
    ))
    query = guides._query(profile(), channel(), None, NOW)
    with patch.object(guides, "programme_times", wraps=guides.programme_times) as times:
        loaded = await guides._read_source(source(), [query], START, STOP, NOW)
    assert times.call_count == 1
    assert ET.tostring(loaded["rows"]["ESPN.us"][0]) == ET.tostring(row)
    diagnostics = loaded["diagnostics"]
    assert diagnostics["xml_complete"] is True
    assert diagnostics["staged_bytes"] == diagnostics["validation_bytes"] == diagnostics["selection_bytes"] == len(document)
    assert diagnostics["content_length"] == len(compressed)
    assert diagnostics["transfer_encoding"] == "absent"
    assert diagnostics["http_version"] == "HTTP/2"
    for key in ("headers_ms", "download_ms", "write_ms", "write_max_ms", "validation_ms", "selection_ms"):
        assert type(diagnostics[key]) is int and diagnostics[key] >= 0
    assert diagnostics["write_calls"] > 0


@pytest.mark.asyncio
async def test_validation_uses_source_deadline_and_keeps_previous_snapshot(monkeypatch, tmp_path):
    import tempfile

    opened = []
    temporary = tempfile.TemporaryFile
    to_thread = asyncio.to_thread

    def create(*args, **kwargs):
        result = temporary(*args, **kwargs)
        opened.append(result)
        return result

    async def run(function, *args, **kwargs):
        if getattr(function, "__name__", None) == "consume":
            await asyncio.Event().wait()
        return await to_thread(function, *args, **kwargs)

    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    monkeypatch.setattr(tempfile, "TemporaryFile", create)
    monkeypatch.setattr(asyncio, "to_thread", run)
    monkeypatch.setattr(guides, "SOURCE_TIMEOUT", 0.05)
    install_transport(monkeypatch, lambda request: reply(GOOD))
    previous = {"success": NOW, "rows": {"ESPN.us": [programme()]}, "checked": 0, "size": 0}
    guides._SOURCE_CACHE["previous"] = previous
    with patch.object(guides, "programme_times", wraps=guides.programme_times) as times:
        await guides._load_source("previous", source(), [], START, STOP, NOW)
    entry = guides._SOURCE_CACHE["previous"]
    assert times.call_count == 0
    assert entry["success"] == NOW and entry["rows"] is previous["rows"]
    assert entry["error"] == "Request timed out."
    assert entry["diagnostics"]["attempts"] == 1
    assert entry["diagnostics"]["validation_ms"] > 0
    assert entry["diagnostics"]["selection_ms"] == 0
    assert len(opened) == 1 and opened[0].closed and list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("length,transfer,version,expected", [
    (str(len(GOOD)), "chunked", b"HTTP/1.1", len(GOOD)),
    (SECRET, SECRET, SECRET.encode(), None),
    ("9" * 100, "", b"HTTP/1.0", None),
])
async def test_source_framing_diagnostics_are_bounded(monkeypatch, length, transfer, version, expected):
    install_transport(monkeypatch, lambda request: httpx.Response(
        200, headers={"content-length": length, "transfer-encoding": transfer},
        stream=Body((GOOD,), None), extensions={"http_version": version},
    ))
    loaded = await guides._read_source(source(), [], START, STOP, NOW)
    diagnostics = bounded(loaded["diagnostics"])
    assert diagnostics.get("content_length") == expected
    assert diagnostics["transfer_encoding"] == ("chunked" if transfer == "chunked" else "other" if transfer else "absent")
    assert diagnostics["http_version"] == (version.decode() if version in {b"HTTP/1.0", b"HTTP/1.1"} else "other")
    assert SECRET not in json.dumps(diagnostics)


@pytest.mark.asyncio
async def test_source_phase_totals_include_failed_validation_before_retry(monkeypatch):
    attempts = []
    read = guides._read_source
    to_thread = asyncio.to_thread

    async def run(function, *args, **kwargs):
        if getattr(function, "__name__", None) == "consume" and args[-1] is False:
            await asyncio.sleep(0.01)
        return await to_thread(function, *args, **kwargs)

    async def capture(*args):
        try:
            loaded = await read(*args)
        except ET.ParseError as exc:
            attempts.append(dict(exc.diagnostics))
            raise
        attempts.append(dict(loaded["diagnostics"]))
        return loaded

    install_transport(monkeypatch, lambda request: reply(GOOD[:-5] if not attempts else GOOD))
    monkeypatch.setattr(asyncio, "to_thread", run)
    monkeypatch.setattr(guides, "_read_source", capture)
    await guides._load_source("phases", source(), [guides._query(profile(), channel(), None, NOW)], START, STOP, NOW)
    entry = guides._SOURCE_CACHE["phases"]
    assert entry["error"] is None and len(attempts) == 2
    diagnostics = bounded(entry["diagnostics"])
    for phase in ("download", "validation", "selection"):
        assert diagnostics[f"total_{phase}_ms"] == sum(attempt[f"{phase}_ms"] for attempt in attempts)
    assert attempts[0]["validation_ms"] >= 10 and attempts[0]["selection_ms"] == 0
    assert diagnostics["total_validation_ms"] > diagnostics["validation_ms"]
    assert diagnostics["xml_complete"] is True and "failure" not in diagnostics
    assert [row.findtext("title") for row in entry["rows"]["ESPN.us"]] == ["SportsCenter"]


@pytest.mark.asyncio
async def test_short_staged_write_never_reaches_selection(monkeypatch):
    to_thread = asyncio.to_thread

    async def run(function, *args, **kwargs):
        result = await to_thread(function, *args, **kwargs)
        if getattr(function, "__name__", None) == "write":
            return result - 1
        return result

    install_transport(monkeypatch, lambda request: reply(GOOD))
    monkeypatch.setattr(asyncio, "to_thread", run)
    with patch.object(guides, "programme_times", wraps=guides.programme_times) as times:
        await guides._load_source("short", source(), [], START, STOP, NOW)
    entry = guides._SOURCE_CACHE["short"]
    assert entry["error"] == "XMLTV staged write is incomplete."
    assert "success" not in entry and not entry.get("rows")
    assert entry["diagnostics"]["attempts"] == 1
    assert entry["diagnostics"]["staged_bytes"] == len(GOOD) - 1
    assert entry["diagnostics"]["validation_bytes"] == entry["diagnostics"]["selection_bytes"] == 0
    assert times.call_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [1021, 2 * 1024 * 1024, 2 * 1024 * 1024 + 257])
async def test_source_staging_preserves_uneven_chunks_and_bounds_writes(monkeypatch, length):
    row = programme(title="Café 🚦", children='<sub-title>Résumé</sub-title><icon src="https://example.test/portrait.jpg"/>')
    original = feed(row)
    document = original[:-5] + b"<!--" + b"x" * (length - len(original) - 7) + b"--></tv>"
    assert len(document) == length

    def chunks():
        split = document.index("é".encode()) + 1
        yield document[:split]
        yield document[split:split + 1]
        offset = split + 1
        sizes = (1, 17, 4093, 65535, 3, 12345)
        index = 0
        while offset < len(document):
            size = sizes[index % len(sizes)]
            yield document[offset:offset + size]
            offset += size
            index += 1

    writes, stored = [], []
    to_thread = asyncio.to_thread

    async def run(function, *args, **kwargs):
        name = getattr(function, "__name__", None)
        if name == "write":
            assert type(args[0]) is bytes
            writes.append(args[0])
        result = await to_thread(function, *args, **kwargs)
        if name == "seek" and not stored:
            stored.append(await to_thread(function.__self__.read))
            await to_thread(function.__self__.seek, 0)
        return result

    install_transport(monkeypatch, lambda request: httpx.Response(200, stream=Body(chunks(), None)))
    monkeypatch.setattr(asyncio, "to_thread", run)
    monkeypatch.setattr(guides, "MAX_PROGRAMMES", 1)
    query = guides._query(profile(), channel(), None, NOW)
    loaded = await guides._read_source(source(), [query], START, STOP, NOW)

    assert stored == [document]
    assert b"".join(writes) == document
    assert all(0 < len(block) <= 1024 * 1024 for block in writes)
    assert len(writes) == (len(document) + 1024 * 1024 - 1) // (1024 * 1024)
    assert ET.tostring(loaded["rows"]["ESPN.us"][0]) == ET.tostring(row)
    diagnostics = loaded["diagnostics"]
    assert diagnostics["xml_complete"] is True
    assert diagnostics["write_calls"] == len(writes)
    assert diagnostics["staged_bytes"] == diagnostics["validation_bytes"] == diagnostics["selection_bytes"] == len(document)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["wire", "decoded", "declaration", "gzip", "transport", "cancel"])
async def test_source_staging_discards_residual_bytes_after_upstream_failure(monkeypatch, tmp_path, failure):
    import tempfile

    prefix = b"<tv>" + b" " * (1024 * 1024 + 17 - 4)
    headers = {}
    error = None
    content = (prefix, b"xx")
    if failure == "wire":
        monkeypatch.setattr(guides, "MAX_DOWNLOAD", len(prefix) + 1)
    elif failure == "decoded":
        monkeypatch.setattr(guides, "MAX_DECODED", len(prefix) + 1)
    elif failure == "declaration":
        content = (prefix, b"<!DOCTYPE tv>")
    elif failure == "gzip":
        headers = {"content-encoding": "gzip"}
        content = (gzip.compress(prefix)[:-3],)
    else:
        content = (prefix,)
        error = httpx.ReadTimeout("upstream stalled") if failure == "transport" else asyncio.CancelledError()
    install_transport(monkeypatch, lambda request: httpx.Response(200, headers=headers, stream=Body(content, error)))

    opened, writes = [], []
    create = tempfile.TemporaryFile
    to_thread = asyncio.to_thread

    def temporary(*args, **kwargs):
        item = create(*args, **kwargs)
        opened.append(item)
        return item

    async def run(function, *args, **kwargs):
        if getattr(function, "__name__", None) == "write":
            writes.append(bytes(args[0]))
        return await to_thread(function, *args, **kwargs)

    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    monkeypatch.setattr(tempfile, "TemporaryFile", temporary)
    monkeypatch.setattr(asyncio, "to_thread", run)
    previous = {"success": NOW, "rows": {"ESPN.us": [programme()]}, "checked": 0, "size": 0}
    guides._SOURCE_CACHE["buffered"] = previous
    with patch.object(guides, "programme_times", wraps=guides.programme_times) as times:
        if failure == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await guides._load_source("buffered", source(), [], START, STOP, NOW)
        else:
            await guides._load_source("buffered", source(), [], START, STOP, NOW)
    entry = guides._SOURCE_CACHE["buffered"]
    assert entry["error"]
    assert entry["rows"] is previous["rows"] and entry["success"] == NOW
    assert writes == [prefix[:1024 * 1024]]
    assert entry["diagnostics"]["write_calls"] == 1
    assert entry["diagnostics"]["staged_bytes"] == 1024 * 1024
    assert entry["diagnostics"]["validation_bytes"] == entry["diagnostics"]["selection_bytes"] == 0
    assert entry["diagnostics"]["attempts"] == 1
    assert times.call_count == 0
    assert len(opened) == 1 and opened[0].closed
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["disk", "short", "cancel"])
async def test_source_staging_closes_source_and_file_when_write_fails(monkeypatch, tmp_path, failure):
    import tempfile

    closed = False
    opened = []
    create = tempfile.TemporaryFile
    to_thread = asyncio.to_thread

    async def chunks(*args, **kwargs):
        nonlocal closed
        try:
            yield b"<tv>" + b" " * (1024 * 1024 - 4)
            yield b"</tv>"
        finally:
            closed = True

    def temporary(*args, **kwargs):
        item = create(*args, **kwargs)
        opened.append(item)
        return item

    async def run(function, *args, **kwargs):
        if getattr(function, "__name__", None) == "write":
            if failure == "disk":
                raise OSError("No space left on device")
            if failure == "cancel":
                raise asyncio.CancelledError()
            return await to_thread(function, *args, **kwargs) - 1
        return await to_thread(function, *args, **kwargs)

    monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
    monkeypatch.setattr(tempfile, "TemporaryFile", temporary)
    monkeypatch.setattr(guides, "stream_xmltv", chunks)
    monkeypatch.setattr(asyncio, "to_thread", run)
    expected = OSError if failure == "disk" else ValueError if failure == "short" else asyncio.CancelledError
    with pytest.raises(expected) as caught:
        await guides._read_source(source(), [], START, STOP, NOW)
    diagnostics = caught.value.diagnostics
    assert diagnostics["write_calls"] == 1
    assert diagnostics["staged_bytes"] == (1024 * 1024 - 1 if failure == "short" else 0)
    assert diagnostics["validation_bytes"] == diagnostics["selection_bytes"] == 0
    assert closed
    assert len(opened) == 1 and opened[0].closed
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_reused_event_terms_preserve_edition_date_and_ambiguity_checks(monkeypatch):
    guide = profile()
    queries = [guides._query(guide, channel(id=index, name=f"ONE Fight Night {edition} @ Sep 04 09:00 PM", tvg_id=""), None, NOW)
               for index, edition in ((1, 47), (2, 48))]
    install_feed(monkeypatch, feed(
        programme("PPV(1)-05.v", "ONE Fight Night 47", stop="20260905013000 +0000"),
        programme("PPV(2)-05.v", "ONE Fight Night 47", start="20260905004500 +0000", stop="20260905012000 +0000"),
        programme("PPV(3)-05.v", "ONE Fight Night 47", start="20260906010000 +0000", stop="20260906040000 +0000"),
        programme("PPV(4)-05.v", "ONE Fight Night 48", start="20260906010000 +0000", stop="20260906040000 +0000"),
    ))
    with patch.object(guides, "_event", wraps=guides._event) as event, \
            patch.object(guides, "normalize_alias_term", wraps=guides.normalize_alias_term) as normalize, \
            patch.object(guides, "_score_parsed_pair", wraps=guides._score_parsed_pair) as score:
        loaded = await guides._read_source(source(), queries, START, STOP, NOW)
    assert event.call_count == 4 and normalize.call_count == 4 and score.call_count == 8
    assert set(loaded["ended"]) == {queries[0]["key"]}
    assert loaded["ended"][queries[0]["key"]][3].findtext("title") == "ONE Fight Night 47"
    assert set(loaded["channel_warnings"][queries[0]["key"]]) == {"ambiguous_event", "event_date_conflict"}
    assert loaded["channel_warnings"][queries[1]["key"]] == ["event_date_conflict"]
    for query in queries:
        rows, result = guides._compose(query, [source()], {50: loaded}, START, STOP, NOW)
        assert rows == [] and result["event"] is None
