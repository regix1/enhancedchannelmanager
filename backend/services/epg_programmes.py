"""Selected XMLTV schedules for the existing combined dummy guide."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import pytz

from cache import get_cache
from epg_matching import _epg_source_id, build_source_priority_order
from services.epg_migration import stream_xmltv
from services.event_sync_matcher import (
    BAND_ATTACH, EVENT_ATTACH_FLOOR, ParsedEvent, _score_parsed_pair,
    _split_teams, build_team_alias_index, normalize_alias_term, parse_event_name,
)

SOURCE_TTL = 900
SOURCE_RETRY = 60
HTTP_WAIT = 5.0
MAX_DOWNLOAD = 512 * 1024 * 1024
MAX_DECODED = 2 * 1024 * 1024 * 1024
MAX_RETAINED = 64 * 1024 * 1024
MAX_CACHE = 128 * 1024 * 1024
MAX_CACHE_ENTRIES = 128
MAX_PROGRAMMES = 200000
MAX_ARTWORK = 128
ARTWORK_WAIT = 120
_ARTWORK_LOAD: asyncio.Task | None = None
_ARTWORK_CHECKED = float("-inf")
_CATALOGUE_CACHE: dict = {}
_CATALOGUE_LOADS: dict = {}
_CATALOGUE_SLOTS = asyncio.Semaphore(4)
_SOURCE_CACHE: dict = {}
_SOURCE_LOADS: dict = {}
_SOURCE_SLOTS = asyncio.Semaphore(2)


def resolve_sources(epg_source_ids: list[int], sources: list[dict]) -> list[dict]:
    """Resolve configured portrait sources to their original XMLTV source."""
    by_id = {source["id"]: source for source in sources}
    resolved = {}
    for selected in epg_source_ids:
        if isinstance(selected, bool) or not isinstance(selected, int) or selected <= 0:
            raise ValueError("EPG source IDs must be positive integers.")
        source_id, seen = selected, set()
        while True:
            if source_id in seen:
                raise ValueError("EPG source proxy cycle detected.")
            seen.add(source_id)
            source = by_id.get(source_id)
            if not source or not source.get("is_active", source.get("enabled", True)):
                raise ValueError(f"EPG source {source_id} is unavailable or disabled.")
            if str(source.get("source_type", "")).lower() != "xmltv":
                raise ValueError(f"EPG source {source_id} must be XMLTV.")
            path = urlsplit(source.get("url") or "").path.rstrip("/")
            if "/api/dummy-epg" in path:
                raise ValueError("A dummy guide cannot use itself as an upstream source.")
            proxy = re.fullmatch(r".*/api/epg/artwork-proxy/(\d+)", path)
            if proxy:
                source_id = int(proxy.group(1))
                continue
            if not source.get("url"):
                raise ValueError(f"EPG source {source_id} has no downloadable XMLTV.")
            resolved[source_id] = source
            break
    return list(resolved.values())


def _dummy_source(source_id: int, sources: list[dict]) -> bool:
    return any(
        source.get("id") == source_id
        and re.search(r"/api/dummy-epg/xmltv(?:/|$)", urlsplit(source.get("url") or "").path)
        for source in sources
    )


def capture_mappings(profile: dict, channel_map: dict, epg_rows: list[dict], sources: list[dict]) -> list[dict]:
    """Return original explicit channel identities without persisting anything."""
    selected = resolve_sources(profile.get("epg_source_ids") or [], sources)
    canonical = {source["id"] for source in selected}
    aliases = {}
    for source in sources:
        try:
            aliases[source["id"]] = resolve_sources([source["id"]], sources)[0]["id"]
        except ValueError:
            continue
    mappings = {}
    for item in profile.get("channel_mappings") or []:
        source_id = aliases.get(item.get("source_id"), item.get("source_id"))
        if source_id in canonical and item.get("channel_id") and item.get("tvg_id"):
            mappings[item["channel_id"]] = {
                "channel_id": item["channel_id"], "source_id": source_id, "tvg_id": item["tvg_id"],
            }
    rows = {row.get("id"): row for row in epg_rows}
    assignments = (_resolve_group_assignments(profile["channel_group_ids"], channel_map)
                   if profile.get("channel_group_ids") else profile.get("channel_assignments") or [])
    included = {item["channel_id"] for item in assignments}
    for channel_id, channel in channel_map.items():
        if channel_id not in included:
            continue
        link = channel.get("epg_data_id") or channel.get("epg_data")
        row = link if isinstance(link, dict) else rows.get(link)
        if not row:
            continue
        source_id = aliases.get(_epg_source_id(row.get("epg_source") or row.get("epg_source_id")), _epg_source_id(row.get("epg_source") or row.get("epg_source_id")))
        tvg_id = row.get("tvg_id")
        if _dummy_source(_epg_source_id(row.get("epg_source") or row.get("epg_source_id")), sources):
            continue
        if tvg_id:
            mappings.pop(channel_id, None)
        if source_id in canonical and tvg_id:
            mappings[channel_id] = {"channel_id": channel_id, "source_id": source_id, "tvg_id": tvg_id}
    return [mappings[key] for key in sorted(mappings)]


def _resolve_group_assignments(channel_group_ids: list, channel_map: dict) -> list:
    """Resolve both Dispatcharr channel-group response shapes."""
    groups = set(channel_group_ids or [])
    assignments = []
    for channel_id, channel in channel_map.items():
        group = channel.get("channel_group_id") or channel.get("channel_group")
        if isinstance(group, dict):
            group = group.get("id")
        if group in groups:
            assignments.append({"channel_id": channel_id, "channel_name": channel.get("name", "")})
    return assignments


async def _fetch_all_channels(client=None) -> dict:
    """Fetch channels once and expand their stream IDs in one batch."""
    if client is None:
        from dispatcharr_client import get_client
        client = get_client()
    channels = []
    for page in range(1, 1001):
        response = await client.get_channels(page=page, page_size=500)
        channels.extend(response if isinstance(response, list) else response.get("results", []))
        if isinstance(response, list) or not response.get("next"):
            break
    else:
        raise ValueError("Channel pagination exceeds its limit.")
    channel_map = {channel["id"]: dict(channel) for channel in channels}
    stream_ids = {stream for channel in channels for stream in channel.get("streams", []) if isinstance(stream, int)}
    if stream_ids:
        try:
            streams = {stream["id"]: stream for stream in await client.get_streams_by_ids(sorted(stream_ids))}
        except Exception:
            streams = {}
        for channel in channel_map.values():
            channel["streams"] = [
                streams.get(stream, {"id": stream, "name": ""}) if isinstance(stream, int) else stream
                for stream in channel.get("streams", [])
            ]
    return channel_map


def programme_times(programme: ET.Element) -> tuple[datetime, datetime]:
    """Require complete, timezone-aware source schedule timestamps."""
    values = []
    for field in ("start", "stop"):
        value = programme.get(field, "").strip()
        if not re.fullmatch(r"\d{14}\s[+-]\d{4}", value):
            raise ValueError("Programme timestamp must include an explicit UTC offset.")
        values.append(datetime.strptime(value, "%Y%m%d%H%M%S %z").astimezone(timezone.utc))
    if values[1] <= values[0]:
        raise ValueError("Programme stop must follow its start.")
    return values[0], values[1]


def _event(programme: ET.Element, start: datetime) -> ParsedEvent:
    title = " ".join((programme.findtext("title") or "").replace("ᴸᶦᵛᵉ", "").split())
    subtitle = programme.findtext("sub-title") or ""
    if subtitle and not _split_teams(title):
        title = f"{title}: {subtitle}"
    return ParsedEvent(title, title, start, _split_teams(title), None)


def _placeholder(programme: ET.Element) -> bool:
    title = " ".join((programme.findtext("title") or "").split())
    return bool(re.fullmatch(
        r"(?:no events?(?: today| scheduled)?|signing off|sign off|off[- ]air|"
        r"no programming(?: scheduled)?|next event: .+ on .+)", title, re.IGNORECASE,
    ))


def _query(profile: dict, channel: dict, mapping: dict | None, now: datetime) -> dict:
    from dummy_epg_engine import apply_substitutions

    streams = [stream for stream in channel.get("streams", []) if isinstance(stream, dict)]
    source_name = channel.get("name", "")
    if profile.get("name_source") == "stream" and streams:
        index = max(0, profile.get("stream_index", 1) - 1)
        if index < len(streams):
            source_name = streams[index].get("name", source_name)
    substituted, _ = apply_substitutions(source_name, profile.get("substitution_pairs") or [])
    patterns = profile.get("pattern_variants") or None
    if patterns is None and profile.get("title_pattern"):
        patterns = [profile]
    parsed = parse_event_name(
        substituted, patterns, event_timezone=profile.get("event_timezone") or "US/Eastern", now=now,
    )
    if parsed.start is None:
        for stream in streams:
            candidate = parse_event_name(stream.get("name", ""), now=now,
                                         event_timezone=profile.get("event_timezone") or "US/Eastern")
            if candidate.start is not None:
                parsed = candidate
                break
    identities = {str(channel.get("tvg_id") or "")}
    identities.update(str(stream.get("tvg_id") or "") for stream in streams)
    identities.discard("")
    identities = {value for value in identities if not value.startswith("ecm-")}
    if mapping:
        identities.add(mapping["tvg_id"])
    return {
        "channel_id": channel["id"], "mapping": mapping, "ids": sorted(identities),
        "name": " ".join(channel.get("name", "").casefold().split()),
        "event": parsed,
        "dynamic": parsed.start is not None or bool(re.search(r"\b(?:ppv|espn\s*\+|espnplus)(?:\b|(?=\s|$))", source_name, re.I)),
    }


def _identity(query: dict, source_id: int, tvg_id: str, header: ET.Element | None) -> int | None:
    mapping = query["mapping"]
    if mapping and mapping["source_id"] == source_id and mapping["tvg_id"] == tvg_id:
        return 0
    if tvg_id in query["ids"]:
        # A numeric ID without the explicitly named source is not portable.
        if not tvg_id.isdigit():
            return 1
    if not query["dynamic"] and header is not None:
        for name in header.findall("display-name"):
            if " ".join((name.text or "").casefold().split()) == query["name"] and query["name"]:
                return 2
    return None


async def _read_source(source: dict, queries: list[dict], start: datetime, stop: datetime, now: datetime) -> dict:
    """Keep only useful identities and strictly matched events from a complete XMLTV."""
    from config import get_settings
    alias_index = build_team_alias_index(get_settings().event_sync_team_aliases or [])
    parser = ET.XMLPullParser(events=("start", "end"))
    root = None
    headers, rows, warnings = {}, {}, set()
    channel_warnings = {}
    retained = count = pending_size = 0
    event_headers = any(query["dynamic"] and query["event"].start is not None for query in queries)

    def consume(chunk: bytes | None) -> None:
        nonlocal root, retained, count, pending_size
        if chunk is not None:
            pending_size += len(chunk)
            if pending_size > MAX_RETAINED:
                raise ValueError("XMLTV element exceeds the retained size limit.")
        if chunk is None:
            parser.close()
        else:
            parser.feed(chunk)
        for event, element in parser.read_events():
            if event == "start" and root is None:
                root = element
                if root.tag != "tv":
                    raise ValueError("XMLTV root must be tv.")
            if event != "end":
                continue
            if element.tag == "channel":
                tvg_id = element.get("id", "")
                if event_headers or any(_identity(query, source["id"], tvg_id, element) is not None for query in queries):
                    saved = copy.deepcopy(element)
                    retained += len(ET.tostring(saved))
                    headers[tvg_id] = saved
                if root is not None:
                    root.remove(element)
                    pending_size = 0
            elif element.tag == "programme":
                tvg_id = element.get("channel", "")
                try:
                    begin, end = programme_times(element)
                except ValueError:
                    warnings.add("invalid_schedule")
                else:
                    if end > now and end > start and begin < stop and not _placeholder(element):
                        if end - begin > timedelta(hours=24) or begin < start - timedelta(days=1):
                            warnings.add("implausible_schedule")
                        else:
                            wanted = any(_identity(query, source["id"], tvg_id, headers.get(tvg_id)) is not None
                                         for query in queries)
                            if not wanted:
                                event_title = _event(element, begin)
                                for query in queries:
                                    parsed = query["event"]
                                    if parsed.start is None:
                                        continue
                                    delta = abs((parsed.start - begin).total_seconds())
                                    if delta > 1800:
                                        common = set(normalize_alias_term(parsed.title or "")) & set(normalize_alias_term(event_title.title or ""))
                                        if len(common) >= 2 and _score_parsed_pair(
                                            parsed, event_title, window_minutes=None,
                                            threshold=EVENT_ATTACH_FLOOR, alias_index=alias_index,
                                        ).band == BAND_ATTACH:
                                            reason = "event_date_conflict" if delta >= 43200 else "event_start_conflict"
                                            channel_warnings.setdefault(query["channel_id"], set()).add(reason)
                                        continue
                                    if _score_parsed_pair(parsed, event_title, window_minutes=30,
                                                          threshold=EVENT_ATTACH_FLOOR,
                                                          alias_index=alias_index).band == BAND_ATTACH:
                                        wanted = True
                                        break
                            if wanted:
                                saved = copy.deepcopy(element)
                                retained += len(ET.tostring(saved))
                                count += 1
                                rows.setdefault(tvg_id, []).append(saved)
                if root is not None:
                    root.remove(element)
                    pending_size = 0
            elif root is not None and element in root:
                root.remove(element)
                pending_size = 0
            if retained > MAX_RETAINED or count > MAX_PROGRAMMES:
                raise ValueError("Selected XMLTV schedules exceed the retained size limit.")

    async with _SOURCE_SLOTS:
        async with asyncio.timeout(120):
            async for chunk in stream_xmltv(source, max_download=MAX_DOWNLOAD, max_decoded=MAX_DECODED):
                await asyncio.to_thread(consume, chunk)
            await asyncio.to_thread(consume, None)
    if root is None:
        raise ValueError("XMLTV document is empty.")
    for tvg_id in list(headers):
        if tvg_id not in rows and not any(_identity(query, source["id"], tvg_id, headers[tvg_id]) is not None for query in queries):
            retained -= len(ET.tostring(headers[tvg_id]))
            del headers[tvg_id]
    return {"headers": headers, "rows": rows, "warnings": sorted(warnings), "size": retained,
            "channel_warnings": {channel: sorted(values) for channel, values in channel_warnings.items()}}


async def _load_source(key: str, source: dict, queries: list[dict], start: datetime, stop: datetime, now: datetime) -> None:
    previous = _SOURCE_CACHE.get(key, {})
    try:
        loaded = await _read_source(source, queries, start, stop, now)
        loaded.update({"success": datetime.now(timezone.utc), "checked": time.monotonic(), "error": None,
                       "selection": previous.get("selection")})
        _SOURCE_CACHE[key] = loaded
    except asyncio.CancelledError:
        _SOURCE_CACHE[key] = {**previous, "checked": time.monotonic(), "error": "XMLTV source loading was cancelled."}
        raise
    except Exception:
        _SOURCE_CACHE[key] = {**previous, "checked": time.monotonic(), "error": "Could not load the complete XMLTV source."}
    finally:
        total = sum(entry.get("size", 0) for entry in _SOURCE_CACHE.values())
        for oldest in sorted(_SOURCE_CACHE, key=lambda item: _SOURCE_CACHE[item].get("checked", 0)):
            if total <= MAX_CACHE and len(_SOURCE_CACHE) <= MAX_CACHE_ENTRIES:
                break
            total -= _SOURCE_CACHE[oldest].get("size", 0)
            del _SOURCE_CACHE[oldest]
        _SOURCE_LOADS.pop(key, None)
        get_cache().invalidate_prefix("dummy_epg_xmltv")


async def _probe_artwork(unknown: dict) -> None:
    """Learn a bounded batch of portrait variants without delaying guide output."""
    global _ARTWORK_LOAD, _ARTWORK_CHECKED
    from config import CONFIG_DIR
    from services.epg_artwork import ArtworkCache, probe_unknown
    try:
        cache = ArtworkCache(CONFIG_DIR / "epg_artwork_cache.json")
        unknown = {key: value for key, value in unknown.items() if not cache.get(key)[0]}
        async with asyncio.timeout(ARTWORK_WAIT):
            await probe_unknown(cache, unknown)
    except asyncio.CancelledError:
        raise
    except Exception:
        pass
    finally:
        _ARTWORK_CHECKED = time.monotonic()
        _ARTWORK_LOAD = None
        get_cache().invalidate_prefix("dummy_epg_xmltv")


def can_cache(coverage: dict) -> bool:
    """Only cache output whose source and artwork preparation has settled."""
    return not coverage.get("artwork_pending") and all(
        source.get("status") == "ready" for source in coverage.get("sources", [])
    )


async def _load_catalogue(key: tuple, client, link: int | None) -> dict:
    previous = _CATALOGUE_CACHE.get(key, {})
    try:
        async with _CATALOGUE_SLOTS:
            async with asyncio.timeout(120):
                if link is None:
                    value = await client.get_epg_sources()
                else:
                    row = await client.get_epg_data_by_id(link)
                    value = {name: row.get(name) for name in ("id", "epg_source", "epg_source_id", "tvg_id")}
        _CATALOGUE_CACHE[key] = {"value": value, "checked": time.monotonic(), "error": False}
    except asyncio.CancelledError:
        _CATALOGUE_CACHE[key] = {**previous, "checked": time.monotonic(), "error": True}
        raise
    except Exception:
        _CATALOGUE_CACHE[key] = {**previous, "checked": time.monotonic(), "error": True}
    finally:
        _CATALOGUE_LOADS.pop(key, None)
        for oldest in sorted(_CATALOGUE_CACHE, key=lambda item: _CATALOGUE_CACHE[item].get("checked", 0)):
            if len(_CATALOGUE_CACHE) <= 2048:
                break
            del _CATALOGUE_CACHE[oldest]
        current = _CATALOGUE_CACHE.get(key, {})
        if previous.get("value") != current.get("value") or previous.get("error") != current.get("error"):
            get_cache().invalidate_prefix("dummy_epg_xmltv")
    return current


def _compose(query: dict, sources: list[dict], entries: dict, start: datetime, stop: datetime, now: datetime,
             artwork: dict | None = None, artwork_cache=None) -> tuple[list, dict]:
    from config import get_settings
    alias_index = build_team_alias_index(get_settings().event_sync_team_aliases or [])
    priority = build_source_priority_order(sources)
    candidates, warnings = [], set()
    parsed = query["event"]
    for source in sources:
        entry = entries.get(source["id"], {})
        warnings.update(entry.get("warnings", []))
        warnings.update(entry.get("channel_warnings", {}).get(query["channel_id"], []))
        for tvg_id, programmes in entry.get("rows", {}).items():
            identity = _identity(query, source["id"], tvg_id, entry.get("headers", {}).get(tvg_id))
            for programme in programmes:
                begin, end = programme_times(programme)
                if end <= now or end <= start or begin >= stop:
                    continue
                match = identity
                if query["dynamic"] and parsed.start is not None:
                    pair = _score_parsed_pair(parsed, _event(programme, begin), window_minutes=30,
                                              threshold=EVENT_ATTACH_FLOOR, alias_index=alias_index)
                    if pair.band != BAND_ATTACH:
                        continue
                    match = 0 if identity == 0 else 1
                elif identity is None:
                    continue
                candidates.append((match, priority[source["id"]], begin, end, source["id"], tvg_id, programme))
    if query["dynamic"] and parsed.start is None and not candidates:
        warnings.add("missing_event_identity")
    if query["dynamic"] and parsed.start is not None:
        best_identity = min((candidate[0] for candidate in candidates), default=None)
        candidates = [candidate for candidate in candidates if candidate[0] == best_identity]
        starts = {candidate[2] for candidate in candidates}
        if len(starts) > 1:
            warnings.add("ambiguous_event")
            candidates = []
    accepted = []
    for candidate in sorted(candidates, key=lambda item: (item[0], item[1], item[2], item[5], ET.tostring(item[6]))):
        segments = [(max(start, candidate[2]), min(stop, candidate[3]))]
        for begin, end, *_ in accepted:
            segments = [(left, right) for a, b in segments
                        for left, right in ((a, min(b, begin)), (max(a, end), b)) if right > left]
        for begin, end in segments:
            programme = copy.deepcopy(candidate[6])
            programme.set("start", begin.strftime("%Y%m%d%H%M%S %z"))
            programme.set("stop", end.strftime("%Y%m%d%H%M%S %z"))
            accepted.append((begin, end, candidate[4], candidate[5], programme))
    accepted.sort(key=lambda item: item[0])
    if accepted:
        warnings.difference_update({"event_start_conflict", "event_date_conflict"})
    current = next((row for row in accepted if row[0] <= now < row[1]), None)
    following = next((row for row in accepted if row[0] > now), None)
    chosen = current or following or (accepted[0] if accepted else None)
    real_minutes = int(sum((row[1] - row[0]).total_seconds() for row in accepted) / 60)
    if accepted and any(row[4].find("icon") is None for row in accepted):
        warnings.add("missing_artwork")
    result = {
        "channel_id": query["channel_id"], "source_id": chosen[2] if chosen else None,
        "source_tvg_id": chosen[3] if chosen else None,
        "match": ("event" if query["dynamic"] and parsed.start else "static") if chosen else "unresolved",
        "current": None, "next": None, "real_minutes": real_minutes,
        "gap_minutes": int((stop - start).total_seconds() / 60) - real_minutes,
        "warnings": sorted(warnings),
    }
    for label, row in (("current", current), ("next", following)):
        if row:
            result[label] = {"start": row[0].isoformat(), "stop": row[1].isoformat(), "title": row[4].findtext("title") or ""}
    if accepted:
        from config import CONFIG_DIR
        from services.epg_artwork import ArtworkCache, ArtworkRewriter, compile_leagues
        settings = get_settings()
        rewriter = ArtworkRewriter(
            artwork_cache if artwork_cache is not None else ArtworkCache(CONFIG_DIR / "epg_artwork_cache.json"),
            banner_base=settings.sports_banner_base_url,
            leagues=compile_leagues(settings.sports_banner_leagues),
        )
        rewritten = rewriter.feed("<tv>" + "".join(ET.tostring(row[4], encoding="unicode") for row in accepted) + "</tv>")
        rewritten += rewriter.finish()
        if artwork is not None:
            for key, value in rewriter.unknown.items():
                if len(artwork) >= MAX_ARTWORK:
                    break
                artwork.setdefault(key, value)
        programmes = list(ET.fromstring(rewritten))
        if all(programme.find("icon") is not None for programme in programmes):
            result["warnings"] = [warning for warning in result["warnings"] if warning != "missing_artwork"]
    else:
        programmes = []
    return programmes, result


async def prepare_profiles(profiles: list[dict], channel_map: dict, client, *, now: datetime | None = None,
                           wait_for_sources: bool = False) -> tuple[list[dict], dict]:
    """Prepare the same selected schedules for HTTP, diagnostics and scheduled generation."""
    global _ARTWORK_LOAD
    from dummy_epg_engine import get_xmltv_id
    artwork, artwork_cache = {}, None
    now = now or datetime.now(timezone.utc)
    deadline = time.monotonic() + HTTP_WAIT
    enriched, coverage = [], {"generated_at": now.isoformat(), "window_start": None, "window_stop": None,
                              "sources": [], "channels": []}
    profiles = sorted(profiles, key=lambda profile: (profile.get("id") is None, profile.get("id") or 0))
    selected_ids = {source for profile in profiles if profile.get("enabled", True)
                    for source in profile.get("epg_source_ids") or []}
    sources, epg_rows, source_error = [], [], None
    unresolved_links = set()
    catalogue_status = "pending"
    if selected_ids:
        channel_ids = set()
        for profile in profiles:
            if not profile.get("enabled", True) or not profile.get("epg_source_ids"):
                continue
            assignments = (_resolve_group_assignments(profile["channel_group_ids"], channel_map)
                           if profile.get("channel_group_ids") else profile.get("channel_assignments") or [])
            channel_ids.update(item["channel_id"] for item in assignments)
        links = set()
        for channel_id in channel_ids:
            channel = channel_map.get(channel_id, {})
            link = channel.get("epg_data_id") or channel.get("epg_data")
            if isinstance(link, int) and not isinstance(link, bool):
                links.add(link)
        keys = [(client, link) for link in [None, *sorted(links)]]
        for key in keys:
            entry = _CATALOGUE_CACHE.get(key, {})
            if time.monotonic() - entry.get("checked", float("-inf")) >= SOURCE_RETRY and key not in _CATALOGUE_LOADS:
                _CATALOGUE_LOADS[key] = asyncio.create_task(_load_catalogue(key, client, key[1]))
        catalogue = {key: _CATALOGUE_CACHE.get(key, {}) for key in keys}
        loading = {key: _CATALOGUE_LOADS[key] for key in keys if key in _CATALOGUE_LOADS}
        if loading:
            await asyncio.wait(loading.values(), timeout=130 * max(1, (len(loading) + 3) // 4) if wait_for_sources
                               else max(0, deadline - time.monotonic()))
        for key, task in loading.items():
            if task.done() and not task.cancelled():
                catalogue[key] = task.result()
            else:
                catalogue[key] = _CATALOGUE_CACHE.get(key, catalogue[key])
        sources = catalogue.get((client, None), {}).get("value") or []
        for key in keys:
            entry = catalogue.get(key, {})
            if key in _CATALOGUE_LOADS or entry.get("error") or "value" not in entry:
                source_error = "Configured EPG sources are temporarily unavailable."
                if key not in _CATALOGUE_LOADS:
                    catalogue_status = "error"
                if key[1] is not None and not entry.get("value"):
                    unresolved_links.add(key[1])
            if key[1] is not None and entry.get("value"):
                epg_rows.append(entry["value"])
    jobs, prepared = {}, []
    for original in profiles:
        profile = copy.deepcopy({key: value for key, value in original.items() if key != "channel_map"})
        groups = profile.get("channel_group_ids") or []
        if groups:
            profile["channel_assignments"] = _resolve_group_assignments(groups, channel_map)
        assignments = profile.get("channel_assignments") or []
        enriched.append(profile)
        if not profile.get("enabled", True) or not profile.get("epg_source_ids"):
            continue
        tz = pytz.timezone(profile.get("event_timezone") or "US/Eastern")
        local = now.astimezone(tz)
        midnight = datetime(local.year, local.month, local.day)
        start = tz.localize(midnight).astimezone(timezone.utc)
        stop = tz.localize(midnight + timedelta(days=2)).astimezone(timezone.utc)
        profile.update({"guide_start": start, "guide_stop": stop, "source_programmes": {}, "source_channels": {}})
        coverage["window_start"] = min(coverage["window_start"] or start.isoformat(), start.isoformat())
        coverage["window_stop"] = max(coverage["window_stop"] or stop.isoformat(), stop.isoformat())
        try:
            resolved = resolve_sources(profile["epg_source_ids"], sources)
            mappings = {item["channel_id"]: item for item in capture_mappings(profile, channel_map, epg_rows, sources)}
            for channel_id in list(mappings):
                channel = channel_map.get(channel_id, {})
                link = channel.get("epg_data_id") or channel.get("epg_data")
                if isinstance(link, int) and link in unresolved_links:
                    mappings.pop(channel_id)
        except ValueError as exc:
            resolved, mappings = [], {}
            coverage["sources"].extend(
                {"source_id": source, "status": "error", "last_success": None, "error": str(exc)}
                for source in profile["epg_source_ids"]
            )
        queries = [_query(profile, {**channel_map[item["channel_id"]], "id": item["channel_id"]},
                          mappings.get(item["channel_id"]), now)
                   for item in assignments if item.get("channel_id") in channel_map]
        prepared.append((profile, resolved, queries, start, stop))
        for query in queries:
            channel = channel_map[query["channel_id"]]
            link = channel.get("epg_data_id") or channel.get("epg_data")
            if isinstance(link, int) and link in unresolved_links:
                query["blocked"] = "mapping_unavailable"
            else:
                row = link if isinstance(link, dict) else next((row for row in epg_rows if row.get("id") == link), None)
                if (row and row.get("tvg_id")
                        and not _dummy_source(_epg_source_id(row.get("epg_source") or row.get("epg_source_id")), sources)):
                    source_id = _epg_source_id(row.get("epg_source") or row.get("epg_source_id"))
                    try:
                        canonical = resolve_sources([source_id], sources)[0]["id"]
                    except ValueError:
                        canonical = None
                    if canonical not in {source["id"] for source in resolved}:
                        query["blocked"] = "source_not_selected"
        for source in resolved:
            if not any(not query.get("blocked") for query in queries):
                continue
            job = jobs.setdefault(source["id"], {"source": source, "queries": [], "start": start, "stop": stop})
            job["queries"].extend(query for query in queries if not query.get("blocked"))
            job["start"], job["stop"] = min(start, job["start"]), max(stop, job["stop"])
    for source_id, job in jobs.items():
        fingerprint = json.dumps([job["source"], job["queries"], job["start"], job["stop"]], sort_keys=True, default=str)
        key = hashlib.sha256(fingerprint.encode()).hexdigest()
        selection = {
            "source": hashlib.sha256(json.dumps(job["source"], sort_keys=True, default=str).encode()).hexdigest(),
            "queries": frozenset(json.dumps(query, sort_keys=True, default=str) for query in job["queries"]),
            "start": job["start"], "stop": job["stop"],
        }
        if key not in _SOURCE_CACHE:
            for cached_key, cached in list(_SOURCE_CACHE.items()):
                scope = cached.get("selection") or {}
                fresh = (time.monotonic() - cached.get("checked", float("-inf"))
                         < (SOURCE_RETRY if cached.get("error") else SOURCE_TTL))
                if ((fresh or cached_key in _SOURCE_LOADS) and scope.get("source") == selection["source"]
                        and selection["queries"].issubset(scope.get("queries", ()))
                        and scope["start"] <= selection["start"] and scope["stop"] >= selection["stop"]):
                    key = cached_key
                    break
        job["key"] = key
        entry = _SOURCE_CACHE.get(key, {})
        age = time.monotonic() - entry.get("checked", float("-inf"))
        if age >= (SOURCE_RETRY if entry.get("error") else SOURCE_TTL) and key not in _SOURCE_LOADS:
            _SOURCE_CACHE[key] = {**entry, "selection": selection}
            _SOURCE_LOADS[key] = asyncio.create_task(_load_source(
                key, job["source"], job["queries"], job["start"], job["stop"], now))
    pending = [_SOURCE_LOADS[job["key"]] for job in jobs.values() if job["key"] in _SOURCE_LOADS]
    if pending:
        await asyncio.wait(pending, timeout=130 * max(1, (len(pending) + 1) // 2) if wait_for_sources
                           else max(0, deadline - time.monotonic()))
    entries = {}
    for source_id, job in jobs.items():
        entry = _SOURCE_CACHE.get(job["key"], {})
        entries[source_id] = entry
        success = entry.get("success")
        status = "pending" if job["key"] in _SOURCE_LOADS else "error" if entry.get("error") or not success else "ready"
        if success and (datetime.now(timezone.utc) - success).total_seconds() > 3600:
            status = "stale"
        coverage["sources"].append({"source_id": source_id, "status": status,
                                    "last_success": success.isoformat() if success else None,
                                    "error": entry.get("error") or ("No complete XMLTV schedule is available." if status == "error" else None)})
    if source_error:
        coverage["sources"] = [{"source_id": source, "status": catalogue_status, "last_success": None, "error": source_error}
                               for source in sorted(selected_ids)]
    owners, used_ids, collisions = {}, set(), set()
    for item in enriched:
        if not item.get("enabled", True):
            continue
        for assignment in item.get("channel_assignments") or []:
            channel_id = assignment.get("channel_id")
            if channel_id not in channel_map or channel_id in owners:
                continue
            owners[channel_id] = item
            xmltv_id = get_xmltv_id(assignment, channel_map[channel_id], item)
            if xmltv_id in used_ids:
                collisions.add(channel_id)
            used_ids.add(xmltv_id)
    seen_channels = set()
    if prepared:
        from config import CONFIG_DIR
        from services.epg_artwork import ArtworkCache
        artwork_cache = ArtworkCache(CONFIG_DIR / "epg_artwork_cache.json")
    for profile, resolved, queries, start, stop in prepared:
        for query in queries:
            channel_id = query["channel_id"]
            programmes, result = await asyncio.to_thread(
                _compose, query, [] if query.get("blocked") else resolved, entries, start, stop, now, artwork, artwork_cache,
            )
            if query.get("blocked"):
                result["warnings"].append(query["blocked"])
            assignment = next(item for item in profile["channel_assignments"] if item["channel_id"] == channel_id)
            channel = channel_map[channel_id]
            xmltv_id = get_xmltv_id(assignment, channel, profile)
            result["xmltv_id"] = xmltv_id
            profile["source_programmes"][channel_id] = programmes
            if result["source_id"]:
                header = entries.get(result["source_id"], {}).get("headers", {}).get(result["source_tvg_id"])
                if header is not None:
                    profile["source_channels"][channel_id] = copy.deepcopy(header)
            if channel_id in seen_channels or owners.get(channel_id) is not profile:
                result["warnings"].append("overlapping_profile")
                continue
            seen_channels.add(channel_id)
            if channel_id in collisions:
                result["warnings"].append("xmltv_id_collision")
                result["match"] = "collision"
                coverage["channels"].append(result)
                continue
            if programmes:
                from dummy_epg_engine import generate_channel_xml
                _, rendered = await asyncio.to_thread(
                    generate_channel_xml, channel_id, channel.get("name", ""),
                    channel.get("channel_number"), xmltv_id, profile, channel.get("streams") or [],
                )
                intervals = {(row.get("start"), row.get("stop")) for row in programmes}
                real = [row for row in rendered if (row.get("start"), row.get("stop")) in intervals]
                if real and all(row.find("icon") is not None for row in real):
                    result["warnings"] = [warning for warning in result["warnings"] if warning != "missing_artwork"]
            coverage["channels"].append(result)
    coverage["artwork_pending"] = bool(artwork)
    if artwork and _ARTWORK_LOAD is None and time.monotonic() - _ARTWORK_CHECKED >= SOURCE_RETRY:
        _ARTWORK_LOAD = asyncio.create_task(_probe_artwork(artwork))
    return enriched, coverage
