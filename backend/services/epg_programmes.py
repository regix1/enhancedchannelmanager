"""Selected XMLTV schedules for the existing combined dummy guide."""

from __future__ import annotations

import asyncio
import bisect
import copy
from collections import Counter
from collections.abc import Mapping
from functools import lru_cache
import hashlib
import json
import logging
import math
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
    BAND_ATTACH, ParsedEvent, _score_parsed_pair,
    _split_teams, build_team_alias_index, normalize_alias_term, parse_event_name,
)

logger = logging.getLogger(__name__)

SOURCE_TTL = 900
SOURCE_RETRY = 60
HTTP_WAIT = 5.0
# Large programme feeds are parsed incrementally; selected rows keep their own smaller limit.
MAX_DOWNLOAD = 4 * 1024 * 1024 * 1024
MAX_DECODED = 4 * 1024 * 1024 * 1024
SOURCE_READ_TIMEOUT = 300.0
CATALOGUE_TIMEOUT = 120.0
# Keep guide freshness independent of the time allowed for a replacement scan.
SOURCE_MAX_AGE = 80 * 60
MAX_QUERIES = 4096
MAX_RETAINED = 64 * 1024 * 1024
MAX_CACHE = 128 * 1024 * 1024
MAX_CACHE_ENTRIES = 128
MAX_PROGRAMMES = 200000
MAX_ARTWORK = 128
ARTWORK_WAIT = 120
# Channel warnings that mean "not looked up yet", as opposed to "nothing scheduled".
PROVISIONAL_WARNINGS = frozenset({"schedule_pending", "mapping_unavailable"})
_ARTWORK_LOAD: asyncio.Task | None = None
_ARTWORK_CHECKED = float("-inf")
_CATALOGUE_CACHE: dict = {}
_CATALOGUE_LOADS: dict = {}
_CATALOGUE_EXPIRIES: dict = {}
_CATALOGUE_SLOTS = asyncio.Semaphore(4)
_SOURCE_CACHE: dict = {}
_SOURCE_LOADS: dict = {}
_SOURCE_EXPIRIES: dict = {}
_SOURCE_SLOTS = asyncio.Semaphore(2)


def _expiry(value: datetime | None, field_name: str = "expires_at") -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be a datetime with an offset.")
    return value.astimezone(timezone.utc)


def _remaining(expires_at: datetime | None) -> float | None:
    if expires_at is None:
        return None
    return (_expiry(expires_at) - datetime.now(timezone.utc)).total_seconds()


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
        and re.fullmatch(
            r"/api/dummy-epg/xmltv(?:/[1-9]\d*)?/?",
            urlsplit(source.get("url") or "").path,
        )
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


def _profile_owners(
    profiles: list[dict], channel_map: dict, coverage: dict,
) -> tuple[dict, set[int]]:
    """Apply shared channel and outward guide identity ownership checks."""
    from dummy_epg_engine import get_xmltv_id

    owners, xmltv_owners, collisions, owner_conflicts = {}, {}, set(), set()
    for profile in profiles:
        if not profile.get("enabled", True):
            continue
        for assignment in profile.get("channel_assignments") or []:
            channel_id = assignment.get("channel_id")
            if channel_id not in channel_map:
                continue
            if channel_id in owners:
                owner_conflicts.add(channel_id)
                continue
            owners[channel_id] = profile
            xmltv_id = get_xmltv_id(assignment, channel_map[channel_id], profile)
            if xmltv_id in xmltv_owners:
                collisions.add(channel_id)
                collisions.add(xmltv_owners[xmltv_id])
            else:
                xmltv_owners[xmltv_id] = channel_id
    records = coverage.get("profiles")
    if not isinstance(records, dict):
        raise ValueError("Publication coverage requires profile readiness records.")
    for profile in profiles:
        record = records.get(str(profile.get("id")))
        if record is None:
            continue
        owned = set(record.get("owned_channel_ids") or [])
        reasons = set(record.get("reason_codes") or [])
        if owned & owner_conflicts:
            reasons.add("GUIDE_OWNERSHIP_CONFLICT")
        if owned & collisions:
            reasons.add("GUIDE_XMLTV_ID_COLLISION")
        record["reason_codes"] = sorted(reasons)
        record["can_publish"] = record.get("can_publish") is True and not reasons
    return owners, collisions


async def _fetch_all_channels(client=None) -> dict:
    """Fetch a complete channel list and expand stream IDs in one batch."""
    if client is None:
        from dispatcharr_client import get_client
        client = get_client()

    # Reading all rows in one response avoids overlapping pages when the
    # upstream sort key is tied, including hidden slots without a number.
    response = await client.get_channels(page=None, page_size=None, visibility_filter="all")
    if isinstance(response, list):
        channels = response
    elif isinstance(response, dict) and isinstance(response.get("results"), list):
        channels = response["results"]
        count = response.get("count")
        if response.get("next") or type(count) is not int or count != len(channels):
            raise ValueError("Channel catalogue response is paginated or incomplete.")
    else:
        raise ValueError("Channel catalogue response must contain a list.")
    seen = set()
    for index, channel in enumerate(channels):
        if not isinstance(channel, dict) or type(channel.get("id")) is not int or channel["id"] <= 0:
            raise ValueError(f"Channel catalogue row {index} has an invalid ID.")
        if channel["id"] in seen:
            raise ValueError(f"Channel catalogue contains duplicate ID {channel['id']}.")
        seen.add(channel["id"])
    channel_map = {channel["id"]: dict(channel) for channel in channels}
    stream_ids = {
        stream
        for channel in channels
        for stream in (channel.get("streams") or [])
        if isinstance(stream, int)
    }
    if stream_ids:
        try:
            streams = {stream["id"]: stream for stream in await client.get_streams_by_ids(sorted(stream_ids))}
        except Exception:
            streams = {}
        for channel in channel_map.values():
            channel["streams"] = [
                streams.get(stream, {"id": stream, "name": ""}) if isinstance(stream, int) else stream
                for stream in (channel.get("streams") or [])
            ]
    return channel_map


@lru_cache(maxsize=65536)
def _programme_time(value: str) -> datetime:
    # Guide files repeat a few thousand slot times across every channel, and
    # strptime is slow enough that parsing each occurrence dominated scans.
    if not re.fullmatch(r"\d{14}\s[+-]\d{4}", value):
        raise ValueError("Programme timestamp must include an explicit UTC offset.")
    return datetime.strptime(value, "%Y%m%d%H%M%S %z").astimezone(timezone.utc)


def programme_times(programme: ET.Element) -> tuple[datetime, datetime]:
    """Require complete, timezone-aware source schedule timestamps."""
    values = [_programme_time(programme.get(field, "").strip()) for field in ("start", "stop")]
    if values[1] <= values[0]:
        raise ValueError("Programme stop must follow its start.")
    return values[0], values[1]


def _grid_time(value, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return _expiry(value, field_name)
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO datetime with an offset.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO datetime with an offset.") from exc
    return _expiry(parsed, field_name)


def programme_matches(
    programmes,
    *,
    xmltv_id: str,
    channel_uuid: str | None,
    title: str,
    start: datetime | str,
    stop: datetime | str,
) -> bool:
    """Return whether a grid row proves the exact generated programme."""
    if not isinstance(programmes, list):
        return False
    if not isinstance(xmltv_id, str) or not xmltv_id:
        raise ValueError("Generated XMLTV ID is required.")
    if channel_uuid is not None and (not isinstance(channel_uuid, str) or not channel_uuid):
        raise ValueError("Channel UUID must be a nonempty string when supplied.")
    if not isinstance(title, str) or not title.strip():
        raise ValueError("Programme title is required.")
    expected_start = _grid_time(start, "expected programme start")
    expected_stop = _grid_time(stop, "expected programme stop")
    if expected_stop <= expected_start:
        raise ValueError("Expected programme stop must follow its start.")
    identities = {xmltv_id}
    if channel_uuid is not None:
        identities.add(channel_uuid)
    for programme in programmes:
        if not isinstance(programme, Mapping):
            continue
        if programme.get("tvg_id") not in identities and programme.get("channel_uuid") not in identities:
            continue
        if programme.get("title") != title.strip():
            continue
        raw_start = programme.get("start_time")
        raw_stop = programme.get("end_time")
        if raw_start is None:
            raw_start = programme.get("start")
        if raw_stop is None:
            raw_stop = programme.get("stop")
        try:
            actual_start = _grid_time(raw_start, "grid programme start")
            actual_stop = _grid_time(raw_stop, "grid programme stop")
        except ValueError:
            continue
        if actual_start == expected_start and actual_stop == expected_stop:
            return True
    return False


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
        r"no programming(?: scheduled)?|program(?:ming)? unavailable|"
        r"next event: .+ on .+)", title, re.IGNORECASE,
    ))


def _query(
    profile: dict,
    channel: dict,
    mapping: dict | None,
    now: datetime,
    assignment: dict | None = None,
) -> dict:
    from dummy_epg_engine import apply_substitutions, get_xmltv_id
    from services.event_slots import classify_event_slot, event_config

    streams = [stream for stream in channel.get("streams", []) if isinstance(stream, dict)]
    source_name = channel.get("name", "")
    if profile.get("name_source") == "stream" and streams:
        index = max(0, profile.get("stream_index", 1) - 1)
        if index < len(streams):
            source_name = streams[index].get("name", source_name)
    substituted, _ = apply_substitutions(source_name, profile.get("substitution_pairs") or [])
    config = event_config(profile)
    patterns = profile.get("pattern_variants") or None
    if patterns is None and profile.get("title_pattern"):
        patterns = [profile]
    parsed = parse_event_name(
        substituted, patterns, event_timezone=profile.get("event_timezone") or "US/Eastern", now=now,
        assume_current_date=bool(config.get("assume_current_date", False)),
    )
    if parsed.start is None:
        for stream in streams:
            candidate = parse_event_name(stream.get("name", ""), now=now,
                                         event_timezone=profile.get("event_timezone") or "US/Eastern",
                                         assume_current_date=bool(config.get("assume_current_date", False)))
            if candidate.start is not None:
                parsed = candidate
                break
    slot = classify_event_slot(channel.get("name"), config, role="channel")
    stream_slots = [
        classify_event_slot(stream.get("name"), config, role=role)
        for stream in streams for role in ("fallback", "event")
    ]
    if slot["family"] is None:
        slot = next((candidate for candidate in stream_slots if candidate["family"] is not None), slot)
    identities = {str(channel.get("tvg_id") or "")}
    identities.update(str(stream.get("tvg_id") or "") for stream in streams)
    identities.discard("")
    assignment = assignment or {"channel_id": channel["id"]}
    identities.discard(get_xmltv_id(assignment, channel, profile))
    if mapping:
        identities.add(mapping["tvg_id"])
    query = {
        "channel_id": channel["id"], "mapping": mapping, "ids": sorted(identities),
        "name": " ".join(channel.get("name", "").casefold().split()),
        "event": parsed,
        "dynamic": mapping is None and (parsed.start is not None or slot["family"] is not None),
        "slot": slot,
        "time_window_minutes": config["time_window_minutes"],
        "enforce_time_window": config["enforce_time_window"],
        "attach_threshold": config["attach_threshold"],
    }
    identity = {key: value for key, value in query.items() if key != "channel_id"}
    if mapping:
        identity["mapping"] = {key: value for key, value in mapping.items() if key != "channel_id"}
    if not query["dynamic"]:
        # A static channel is selected by its mapping, IDs and name. The event
        # its current stream carries changes nothing a scan keeps, so it must
        # not make the source look uncovered and force another full scan.
        identity.pop("event")
    elif parsed.start is not None:
        # Dated matches use the parsed event, even after promotion changes its display name and TVG.
        identity.pop("name")
        identity.pop("ids")
        identity["event"] = {
            "title": " ".join((parsed.title or "").casefold().split()),
            "start": parsed.start.astimezone(timezone.utc).isoformat(),
            "teams": [" ".join(team.casefold().split()) for team in parsed.teams] if parsed.teams else None,
        }
    query["key"] = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()
    return query


def _identity(query: dict, source_id: int, tvg_id: str, header: ET.Element | None) -> int | None:
    mapping = query["mapping"]
    if mapping and mapping["source_id"] == source_id and mapping["tvg_id"] == tvg_id:
        return 0
    if tvg_id in query["ids"]:
        # A numeric ID without the explicitly named source is not portable.
        if not tvg_id.isdigit():
            return 1
    if not query["dynamic"] and header is not None:
        from stream_normalization import strip_country_prefix
        names = [" ".join((name.text or "").casefold().split()) for name in header.findall("display-name")]
        if query["name"] and query["name"] in names:
            return 2
        for name in names:
            if re.match(r"^us\s*[-:|/]\s*", name) and strip_country_prefix(name) == query["name"]:
                return 3
    return None


async def _read_source(
    source: dict,
    queries: list[dict],
    start: datetime,
    stop: datetime,
    now: datetime,
    *,
    expires_at: datetime | None,
) -> dict:
    """Keep only useful identities and strictly matched events from a complete XMLTV."""
    import tempfile
    from contextlib import aclosing
    from config import CONFIG_DIR, get_settings
    from stream_normalization import strip_country_prefix
    alias_index = build_team_alias_index(get_settings().event_sync_team_aliases or [])
    dated_queries = [query for query in queries if query["dynamic"] and query["event"].start is not None]
    ended_queries = [query for query in dated_queries if query["dynamic"]]
    query_terms = {id(query): set(normalize_alias_term(query["event"].title or ""))
                   for query in dated_queries}
    # A programme can only affect the dated queries that start within a
    # matching window of it, have no window, or share two title terms with it
    # (the conflict check). Indexing those once keeps each programme from
    # being compared with every dated query in the profile.
    timed = sorted(
        (query["event"].start.timestamp(), index)
        for index, query in enumerate(dated_queries) if query["enforce_time_window"]
    )
    timed_starts = [moment for moment, _ in timed]
    widest = max(
        (query["time_window_minutes"] * 60 for query in dated_queries if query["enforce_time_window"]),
        default=0,
    )
    untimed = {index for index, query in enumerate(dated_queries) if not query["enforce_time_window"]}
    term_index = {}
    for index, query in enumerate(dated_queries):
        for term in query_terms[id(query)]:
            term_index.setdefault(term, []).append(index)

    def near(begin: datetime) -> set[int]:
        moment = begin.timestamp()
        low = bisect.bisect_left(timed_starts, moment - widest)
        high = bisect.bisect_right(timed_starts, moment + widest)
        return untimed | {index for _, index in timed[low:high]}

    mapped_queries = {}
    direct_queries = {}
    named_queries = {}
    for query in queries:
        mapping = query["mapping"]
        if mapping and mapping["source_id"] == source["id"]:
            mapped_queries.setdefault(mapping["tvg_id"], []).append(query)
        for tvg_id in query["ids"]:
            if not tvg_id.isdigit():
                direct_queries.setdefault(tvg_id, []).append(query)
        if not query["dynamic"] and query["name"]:
            named_queries.setdefault(query["name"], []).append(query)
    parser = ET.XMLPullParser(events=("start", "end"))
    root, depth = None, 0
    headers, rows, warnings = {}, {}, set()
    matches = {}
    ended = {}
    channel_warnings = {}
    diagnostics = {"root": "absent", "xml_complete": False, "transport_complete": False,
                   "download_ms": 0, "write_ms": 0, "write_max_ms": 0, "write_calls": 0,
                   "staged_bytes": 0, "validation_ms": 0, "validation_bytes": 0,
                   "selection_ms": 0, "selection_bytes": 0}
    import codecs
    decoder = codecs.getincrementaldecoder("utf-8")()
    prefix = b""
    invalid_utf8 = forbidden = False
    retained = count = pending_size = 0
    event_headers = bool(ended_queries)

    def consume(chunk: bytes | None, select: bool = True) -> None:
        nonlocal root, depth, retained, count, pending_size, prefix, invalid_utf8, forbidden
        if chunk is not None:
            prefix = (prefix + chunk)[:1024]
            forbidden |= re.search(rb"[\x01-\x08\x0b\x0c\x0e-\x1f]", chunk) is not None
            try:
                decoder.decode(chunk)
            except UnicodeDecodeError:
                invalid_utf8 = True
            pending_size += len(chunk)
            if pending_size > MAX_RETAINED:
                raise ValueError("XMLTV element exceeds the retained size limit.")
        if chunk is None:
            parser.close()
        else:
            parser.feed(chunk)
        for event, element in parser.read_events():
            if event == "start":
                depth += 1
                if root is None:
                    root = element
                    diagnostics["root"] = "tv" if root.tag == "tv" else "other"
                    if root.tag != "tv":
                        raise ValueError("XMLTV root must be tv.")
                continue
            depth -= 1
            if depth != 1:
                continue
            if not select:
                if root is not None:
                    root.remove(element)
                    pending_size = 0
                continue
            if element.tag == "channel":
                tvg_id = element.get("id", "")
                candidate_queries = list(mapped_queries.get(tvg_id, ()))
                candidate_queries.extend(direct_queries.get(tvg_id, ()))
                names = [" ".join((name.text or "").casefold().split())
                         for name in element.findall("display-name")]
                for name in names:
                    candidate_queries.extend(named_queries.get(name, ()))
                    if re.match(r"^us\s*[-:|/]\s*", name):
                        candidate_queries.extend(named_queries.get(strip_country_prefix(name), ()))
                candidate_keys = set()
                matched_queries = []
                for query in candidate_queries:
                    if query["key"] in candidate_keys:
                        continue
                    candidate_keys.add(query["key"])
                    if _identity(query, source["id"], tvg_id, element) is not None:
                        matched_queries.append(query)
                if event_headers or matched_queries:
                    saved = copy.deepcopy(element)
                    retained += len(ET.tostring(saved))
                    headers[tvg_id] = saved
                    if matched_queries:
                        matches[tvg_id] = matched_queries
                    else:
                        matches.pop(tvg_id, None)
                if root is not None:
                    root.remove(element)
                    pending_size = 0
            elif element.tag == "programme":
                tvg_id = element.get("channel", "")
                matched_queries = matches.get(tvg_id)
                if matched_queries is None:
                    candidate_queries = list(mapped_queries.get(tvg_id, ()))
                    candidate_queries.extend(direct_queries.get(tvg_id, ()))
                    candidate_keys = set()
                    matched_queries = []
                    for query in candidate_queries:
                        if query["key"] in candidate_keys:
                            continue
                        candidate_keys.add(query["key"])
                        if _identity(query, source["id"], tvg_id, None) is not None:
                            matched_queries.append(query)
                    if matched_queries:
                        matches[tvg_id] = matched_queries
                try:
                    begin, end = programme_times(element)
                except ValueError:
                    warnings.add("invalid_schedule")
                    for query in matched_queries:
                        channel_warnings.setdefault(query["key"], set()).add("invalid_schedule")
                else:
                    if now - timedelta(hours=24) < end <= now and end - begin <= timedelta(hours=24) and not _placeholder(element):
                        ended_event = None
                        for index in sorted(near(begin)):
                            query = ended_queries[index]
                            parsed = query["event"]
                            window = query["time_window_minutes"] if query["enforce_time_window"] else None
                            if window is not None and abs((parsed.start - begin).total_seconds()) > window * 60:
                                continue
                            if ended_event is None:
                                ended_event = _event(element, begin)
                            if _score_parsed_pair(
                                parsed, ended_event, window_minutes=window,
                                threshold=query["attach_threshold"], alias_index=alias_index,
                            ).band != BAND_ATTACH:
                                continue
                            identity = query["key"]
                            previous = ended.get(identity)
                            if previous and previous[1] != begin:
                                channel_warnings.setdefault(query["key"], set()).add("ambiguous_event")
                            if previous and previous[2] >= end:
                                continue
                            saved = copy.deepcopy(element)
                            retained += len(ET.tostring(saved))
                            if previous:
                                retained -= len(ET.tostring(previous[3]))
                            else:
                                count += 1
                            ended[identity] = (tvg_id, begin, end, saved)
                    if end > now and end > start and begin < stop and not _placeholder(element):
                        if end - begin > timedelta(hours=24) or begin < start - timedelta(days=1):
                            warnings.add("implausible_schedule")
                            for query in matched_queries:
                                channel_warnings.setdefault(query["key"], set()).add("implausible_schedule")
                        else:
                            wanted = bool(matched_queries)
                            if not wanted and dated_queries:
                                event_title = _event(element, begin)
                                event_terms = set(normalize_alias_term(event_title.title or ""))
                                shared = Counter(
                                    index for term in event_terms for index in term_index.get(term, ())
                                )
                                candidates = near(begin) | {index for index, hits in shared.items() if hits >= 2}
                                for index in sorted(candidates):
                                    query = dated_queries[index]
                                    parsed = query["event"]
                                    delta = abs((parsed.start - begin).total_seconds())
                                    window = query["time_window_minutes"] if query["enforce_time_window"] else None
                                    if window is not None and delta > window * 60:
                                        reason = "event_date_conflict" if delta >= 43200 else "event_start_conflict"
                                        if reason in channel_warnings.get(query["key"], ()):
                                            continue
                                        common = query_terms[id(query)] & event_terms
                                        if len(common) >= 2 and _score_parsed_pair(
                                            parsed, event_title, window_minutes=None,
                                            threshold=query["attach_threshold"], alias_index=alias_index,
                                        ).band == BAND_ATTACH:
                                            channel_warnings.setdefault(query["key"], set()).add(reason)
                                        continue
                                    if _score_parsed_pair(parsed, event_title, window_minutes=window,
                                                          threshold=query["attach_threshold"],
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
            elif root is not None:
                root.remove(element)
                pending_size = 0
            if retained > MAX_RETAINED or count > MAX_PROGRAMMES:
                raise ValueError("Selected XMLTV schedules exceed the retained size limit.")

    expires_at = _expiry(expires_at)
    remaining = _remaining(expires_at)
    if remaining is not None and remaining <= 0:
        raise TimeoutError("XMLTV source lifetime expired before transport.")
    try:
        async with asyncio.timeout(remaining):
            # Selection must not slow delivery of a time-limited upstream response.
            with tempfile.TemporaryFile(mode="w+b", dir=CONFIG_DIR) as spool:
                download_started = time.monotonic()
                write_elapsed = 0.0
                buffer = bytearray()
                block_size = 1 << 20

                async def write(chunk: bytes) -> None:
                    nonlocal write_elapsed
                    write_started = time.monotonic()
                    diagnostics["write_calls"] += 1
                    try:
                        written = await asyncio.to_thread(spool.write, chunk)
                        diagnostics["staged_bytes"] += written
                        if written != len(chunk):
                            raise ValueError("XMLTV staged write is incomplete.")
                    finally:
                        elapsed = max(0.0, time.monotonic() - write_started)
                        write_elapsed += elapsed
                        diagnostics["write_ms"] = int(write_elapsed * 1000)
                        diagnostics["write_max_ms"] = max(diagnostics["write_max_ms"], int(elapsed * 1000))

                try:
                    transport_time = _remaining(expires_at)
                    if transport_time is not None and transport_time <= 0:
                        raise TimeoutError("XMLTV source lifetime expired before transport.")
                    async with aclosing(stream_xmltv(
                        source, max_download=MAX_DOWNLOAD, max_decoded=MAX_DECODED,
                        timeout=transport_time,
                        read_timeout=SOURCE_READ_TIMEOUT,
                        diagnostics=diagnostics,
                    )) as chunks:
                        async for chunk in chunks:
                            offset = 0
                            while offset < len(chunk):
                                take = min(block_size - len(buffer), len(chunk) - offset)
                                buffer.extend(memoryview(chunk)[offset:offset + take])
                                offset += take
                                if len(buffer) == block_size:
                                    await write(bytes(buffer))
                                    buffer.clear()
                    if buffer:
                        await write(bytes(buffer))
                finally:
                    buffer.clear()
                    diagnostics["download_ms"] = max(0, int((time.monotonic() - download_started) * 1000))
                # Reject incomplete documents before spending time matching their programmes.
                for select, phase in ((False, "validation"), (True, "selection")):
                    parser = ET.XMLPullParser(events=("start", "end"))
                    root, depth, pending_size = None, 0, 0
                    decoder = codecs.getincrementaldecoder("utf-8")()
                    prefix, invalid_utf8, forbidden = b"", False, False
                    phase_started = time.monotonic()
                    try:
                        await asyncio.to_thread(spool.seek, 0)
                        while chunk := await asyncio.to_thread(spool.read, block_size):
                            diagnostics[f"{phase}_bytes"] += len(chunk)
                            await asyncio.to_thread(consume, chunk, select)
                        await asyncio.to_thread(consume, None, select)
                    finally:
                        diagnostics[f"{phase}_ms"] = max(0, int((time.monotonic() - phase_started) * 1000))
                diagnostics["xml_complete"] = True
    except (Exception, asyncio.CancelledError) as exc:
        if isinstance(exc, ET.ParseError):
            diagnostics.update(parser_code=max(0, exc.code), parser_line=max(0, exc.position[0]),
                               parser_column=max(0, exc.position[1]))
            declared = re.search(br'<\?xml[^>]*encoding\s*=\s*["\']([^"\']+)', prefix, re.I)
            utf8 = declared is None or declared[1].lower() in {b"utf-8", b"utf8", b"us-ascii"}
            diagnostics["failure"] = (
                "forbidden_character" if forbidden else "invalid_utf8" if invalid_utf8 and utf8
                else "incomplete_xml" if exc.code in {3, 5, 6} else "malformed_xml"
            )
        elif diagnostics["root"] == "other":
            diagnostics["failure"] = "wrong_root"
        else:
            diagnostics.setdefault("failure", "unknown")
        exc.diagnostics = diagnostics
        raise
    if root is None:
        raise ValueError("XMLTV document is empty.")
    for tvg_id in list(headers):
        if tvg_id not in rows and tvg_id not in matches:
            retained -= len(ET.tostring(headers[tvg_id]))
            del headers[tvg_id]
    return {"headers": headers, "rows": rows, "ended": ended, "warnings": sorted(warnings), "size": retained,
            "diagnostics": diagnostics,
            "channel_warnings": {channel: sorted(values) for channel, values in channel_warnings.items()}}


def _error_reason(exc: Exception) -> str:
    """Describe known read failures without exposing request or response content."""
    import zlib

    import httpx
    from fastapi import HTTPException

    reasons = {
        "XMLTV source has no downloadable URL.": "XMLTV source has no downloadable URL.",
        "XMLTV source returned an invalid redirect.": "XMLTV source returned an invalid redirect.",
        "XMLTV source URL is blocked by the outbound security policy.": "XMLTV source URL is blocked by the outbound security policy.",
        "XMLTV download exceeds its size limit.": "XMLTV download exceeds its size limit.",
        "XMLTV decoded content exceeds its size limit.": "XMLTV decoded content exceeds its size limit.",
        "XMLTV DTDs, entities and non-UTF encodings are not supported.": "XMLTV DTDs, entities and non-UTF encodings are not supported.",
        "XMLTV gzip has trailing content.": "XMLTV gzip has trailing content.",
        "XMLTV gzip is incomplete.": "XMLTV gzip is incomplete.",
        "XMLTV element exceeds the retained size limit.": "XMLTV element exceeds the retained size limit.",
        "Selected XMLTV schedules exceed the retained size limit.": "Selected XMLTV schedules exceed the retained size limit.",
        "XMLTV root must be tv.": "XMLTV root must be tv.",
        "XMLTV document is empty.": "XMLTV document is empty.",
        "XMLTV staged write is incomplete.": "XMLTV staged write is incomplete.",
        "Dispatcharr EPG response used unexpected Content-Encoding": "Unsupported catalogue response encoding.",
        "Dispatcharr EPG source counts are unavailable": "Catalogue source counts are unavailable.",
        "Dispatcharr EPG catalogue exceeds 200000 rows": "Response exceeded the catalogue size limit.",
        "Dispatcharr EPG response exceeds its source row counts": "Response exceeded the catalogue size limit.",
    }
    reason = "Request failed."
    seen = set()
    for _ in range(8):
        if exc is None or id(exc) in seen:
            break
        seen.add(id(exc))
        if isinstance(exc, httpx.ConnectTimeout):
            reason = "Request timed out while connecting."
        elif isinstance(exc, httpx.ReadTimeout):
            reason = "Request timed out while reading."
        elif isinstance(exc, (TimeoutError, httpx.TimeoutException)):
            reason = "Request timed out."
        elif isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            if type(status) is int and 100 <= status <= 599:
                reason = f"HTTP status {status}."
        elif isinstance(exc, httpx.RequestError):
            reason = "Connection failed."
        elif isinstance(exc, ET.ParseError) or type(exc) is LookupError:
            reason = "Malformed XML."
        elif isinstance(exc, (json.JSONDecodeError, UnicodeDecodeError)):
            reason = "Invalid JSON response."
        elif isinstance(exc, zlib.error):
            reason = "Invalid compressed XMLTV content."
        elif isinstance(exc, (HTTPException, ValueError)):
            detail = exc.detail if isinstance(exc, HTTPException) else (exc.args[0] if exc.args else None)
            if type(detail) is str:
                if detail in reasons:
                    reason = reasons[detail]
                elif re.fullmatch(r"Dispatcharr EPG response exceeds [0-9]{1,10} bytes(?: per row)?", detail):
                    reason = "Response exceeded the catalogue size limit."
                elif detail in {
                    "Dispatcharr EPG response " + ending for ending in (
                        "contains trailing JSON", "must be an array or object", "has an invalid object",
                        "has an invalid delimiter", "has invalid results", "has an invalid object key",
                        "has too many fields", "contains a non-object row", "has an invalid structure", "is incomplete",
                    )
                }:
                    reason = "Invalid JSON response."
        exc = exc.__cause__
    return reason


def _scan_folder():
    from config import CONFIG_DIR

    return CONFIG_DIR / "guide_scans"


def _save_scan(key: str, entry: dict) -> None:
    """Keep a finished scan on the config volume so a restart can reuse it.

    A scan that read correctly stays correct whether or not it could be saved,
    so no failure here may reach the caller.
    """
    try:
        _write_scan(key, entry)
    except Exception as exc:
        logger.warning("[EPG-PROGRAMMES] Could not save guide scan %s: %s", key, exc)


def _write_scan(key: str, entry: dict) -> None:
    record = {
        "success": entry["success"].isoformat(),
        "selection": {
            "queries": sorted(entry["selection"]["queries"]),
            "start": entry["selection"]["start"].isoformat(),
            "stop": entry["selection"]["stop"].isoformat(),
        },
        "headers": {tvg_id: ET.tostring(element, encoding="unicode") for tvg_id, element in entry["headers"].items()},
        "rows": {
            tvg_id: [ET.tostring(element, encoding="unicode") for element in elements]
            for tvg_id, elements in entry["rows"].items()
        },
        "ended": {
            identity: [tvg_id, begin.isoformat(), end.isoformat(), ET.tostring(element, encoding="unicode")]
            for identity, (tvg_id, begin, end, element) in entry["ended"].items()
        },
        "warnings": entry["warnings"],
        "size": entry["size"],
        "channel_warnings": entry["channel_warnings"],
    }
    folder = _scan_folder()
    folder.mkdir(exist_ok=True)
    temporary = folder / f"{key}.tmp"
    temporary.write_text(json.dumps(record), encoding="utf-8")
    temporary.replace(folder / f"{key}.json")


def _restore_scans() -> dict:
    """Read saved scans young enough to use, so a restart skips the first full rescan."""
    folder = _scan_folder()
    now = datetime.now(timezone.utc)
    restored = {}
    for path in sorted(folder.glob("*.json")) if folder.is_dir() else ():
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            success = datetime.fromisoformat(record["success"])
            age = (now - success).total_seconds()
            if not 0 <= age <= SOURCE_MAX_AGE:
                continue
            restored[path.stem] = {
                "headers": {tvg_id: ET.fromstring(text) for tvg_id, text in record["headers"].items()},
                "rows": {
                    tvg_id: [ET.fromstring(text) for text in texts]
                    for tvg_id, texts in record["rows"].items()
                },
                "ended": {
                    identity: (tvg_id, datetime.fromisoformat(begin), datetime.fromisoformat(end), ET.fromstring(text))
                    for identity, (tvg_id, begin, end, text) in record["ended"].items()
                },
                "warnings": record["warnings"],
                "size": record["size"],
                "channel_warnings": record["channel_warnings"],
                "diagnostics": {},
                "success": success,
                "checked": time.monotonic() - age,
                "error": None,
                "selection": {
                    "queries": frozenset(record["selection"]["queries"]),
                    "start": datetime.fromisoformat(record["selection"]["start"]),
                    "stop": datetime.fromisoformat(record["selection"]["stop"]),
                },
                "demand": {},
            }
        except (OSError, ValueError, KeyError, TypeError, ET.ParseError) as exc:
            logger.warning("[EPG-PROGRAMMES] Ignoring unreadable guide scan %s: %s", path.name, exc)
    return restored


async def restore_saved_scans() -> None:
    """Load the scans saved before a restart, before any guide work starts."""
    for key, entry in (await asyncio.to_thread(_restore_scans)).items():
        _SOURCE_CACHE.setdefault(key, entry)


async def _load_source(
    key: str,
    source: dict,
    queries: list[dict],
    start: datetime,
    stop: datetime,
    now: datetime,
    *,
    expires_at: datetime | None,
) -> None:
    expires_at = _expiry(expires_at)
    owner = asyncio.current_task()
    _SOURCE_LOADS.setdefault(key, owner)
    if _SOURCE_LOADS.get(key) is owner:
        _SOURCE_EXPIRIES[key] = expires_at
    previous = _SOURCE_CACHE.get(key, {})
    attempt = 0
    diagnostics = {}
    totals = {}
    try:
        remaining = _remaining(expires_at)
        if remaining is not None and remaining <= 0:
            raise TimeoutError("XMLTV source lifetime expired before queue admission.")
        async with asyncio.timeout(remaining):
            async with _SOURCE_SLOTS:
                if expires_at is not None and _remaining(expires_at) <= 0:
                    raise TimeoutError("XMLTV source lifetime expired before transport.")
                for attempt in range(1, 3):
                    diagnostics = {}
                    try:
                        loaded = await _read_source(
                            source,
                            queries,
                            start,
                            stop,
                            now,
                            expires_at=expires_at,
                        )
                        diagnostics = loaded.get("diagnostics", {})
                    except (Exception, asyncio.CancelledError) as exc:
                        diagnostics = getattr(exc, "diagnostics", {})
                        if (isinstance(exc, ET.ParseError) and attempt == 1 and diagnostics.get("root") == "tv"
                                and diagnostics.get("transport_complete") is True
                                and diagnostics.get("failure") == "incomplete_xml"
                                and getattr(exc, "code", None) in {3, 5, 6}):
                            continue
                        raise
                    finally:
                        for phase in ("download", "validation", "selection"):
                            elapsed = diagnostics.get(f"{phase}_ms")
                            if type(elapsed) is int and elapsed >= 0:
                                name = f"total_{phase}_ms"
                                totals[name] = totals.get(name, 0) + elapsed
                    break
        loaded.setdefault("diagnostics", {}).update(totals, attempts=attempt)
        loaded.update({
            "success": datetime.now(timezone.utc), "checked": time.monotonic(), "error": None,
            "selection": {"queries": frozenset(query["key"] for query in queries),
                          "start": start, "stop": stop},
            "demand": _SOURCE_CACHE.get(key, {}).get("demand", {}),
        })
        if _SOURCE_LOADS.get(key) is owner:
            _SOURCE_CACHE[key] = loaded
            await asyncio.to_thread(_save_scan, key, loaded)
    except asyncio.CancelledError as exc:
        diagnostics = getattr(exc, "diagnostics", diagnostics)
        if _SOURCE_LOADS.get(key) is owner:
            _SOURCE_CACHE[key] = {
                **previous, "checked": time.monotonic(), "error": "XMLTV source loading was cancelled.",
                "demand": _SOURCE_CACHE.get(key, {}).get("demand", {}),
                "diagnostics": {**diagnostics, **totals, "attempts": attempt},
            }
        raise
    except Exception as exc:
        diagnostics = getattr(exc, "diagnostics", getattr(exc.__cause__, "diagnostics", diagnostics))
        if _SOURCE_LOADS.get(key) is owner:
            _SOURCE_CACHE[key] = {
                **previous, "checked": time.monotonic(), "error": _error_reason(exc),
                "demand": _SOURCE_CACHE.get(key, {}).get("demand", {}),
                "diagnostics": {**diagnostics, **totals, "attempts": attempt},
            }
    finally:
        if _SOURCE_LOADS.get(key) is owner:
            _SOURCE_LOADS.pop(key, None)
            _SOURCE_EXPIRIES.pop(key, None)
            total = sum(entry.get("size", 0) for entry in _SOURCE_CACHE.values())
            for oldest in sorted(_SOURCE_CACHE, key=lambda item: _SOURCE_CACHE[item].get("checked", 0)):
                if total <= MAX_CACHE and len(_SOURCE_CACHE) <= MAX_CACHE_ENTRIES:
                    break
                if oldest in _SOURCE_LOADS:
                    continue
                total -= _SOURCE_CACHE[oldest].get("size", 0)
                del _SOURCE_CACHE[oldest]
            # A read that failed carries no schedules to recompose from, so dropping the
            # published guide for it trades a working guide for an emptier one.
            if not _SOURCE_CACHE.get(key, {}).get("error"):
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
    """Cache completed composition while source and portrait refreshes run independently.

    A channel warned "schedule_pending" or "mapping_unavailable" is empty because the
    source was never asked for it, not because it has nothing on. Serving that is fine;
    storing it is not, because the entry outlives the scan that would have filled it in.
    """
    profiles = coverage.get("profiles")
    if isinstance(profiles, Mapping):
        return all(
            isinstance(profile, Mapping) and profile.get("can_publish") is True
            for profile in profiles.values()
        )
    sources = coverage.get("sources", [])
    if not sources:
        return True
    if not any(source.get("status") == "ready" or source.get("last_success") for source in sources):
        return False
    return not any(warning in PROVISIONAL_WARNINGS
                   for channel in coverage.get("channels", ())
                   for warning in channel.get("warnings", ()))


async def _load_catalogue(
    key: tuple,
    client,
    *,
    expires_at: datetime | None,
) -> dict:
    expires_at = _expiry(expires_at)
    owner = asyncio.current_task()
    claimed = [key] if key[1] is None else [(client, link) for link in key[1]]
    for item in claimed:
        _CATALOGUE_LOADS.setdefault(item, owner)
        if _CATALOGUE_LOADS.get(item) is owner:
            _CATALOGUE_EXPIRIES[item] = expires_at
    previous = {item: _CATALOGUE_CACHE.get(item, {}) for item in claimed}
    completed = {}
    failed = False
    try:
        remaining = _remaining(expires_at)
        if remaining is not None and remaining <= 0:
            raise TimeoutError("EPG catalogue lifetime expired before queue admission.")
        async with asyncio.timeout(remaining):
            async with _CATALOGUE_SLOTS:
                if expires_at is not None and _remaining(expires_at) <= 0:
                    raise TimeoutError("EPG catalogue lifetime expired before transport.")
                if key[1] is None:
                    value = await client.get_epg_sources()
                    if (not isinstance(value, list)
                            or any(not isinstance(source, dict)
                                   or type(source.get("id")) is not int or source["id"] <= 0
                                   for source in value)
                            or len({source["id"] for source in value}) != len(value)):
                        raise ValueError("Dispatcharr EPG source selection is unavailable")
                    completed[key] = {
                        "value": value,
                        "checked": time.monotonic(),
                        "error": False,
                    }
                else:
                    ids = frozenset(key[1])
                    rows = await client.get_epg_data(
                        max_results=len(ids), ids=ids, expires_at=expires_at,
                    )
                    if not isinstance(rows, list):
                        raise ValueError("Dispatcharr EPG row selection is unavailable")
                    selected = {}
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        link = row.get("id")
                        source_id = _epg_source_id(
                            row.get("epg_source") or row.get("epg_source_id")
                        )
                        tvg_id = row.get("tvg_id")
                        if (type(link) is int and link in ids
                                and type(source_id) is int and source_id > 0
                                and isinstance(tvg_id, str) and tvg_id.strip()):
                            selected[link] = {
                                name: row.get(name)
                                for name in ("id", "epg_source", "epg_source_id", "tvg_id")
                            }
                    checked = time.monotonic()
                    for item in claimed:
                        link = item[1]
                        if link in selected:
                            completed[item] = {
                                "value": selected[link],
                                "checked": checked,
                                "error": False,
                            }
                        else:
                            completed[item] = {
                                **previous[item],
                                "checked": checked,
                                "error": True,
                            }
    except asyncio.CancelledError:
        failed = True
        raise
    except Exception:
        failed = True
    finally:
        if failed:
            checked = time.monotonic()
            completed = {
                item: {**previous[item], "checked": checked, "error": True}
                for item in claimed
            }
        completed = {item: entry for item, entry in completed.items()
                     if _CATALOGUE_LOADS.get(item) is owner}
        for item, entry in completed.items():
            _CATALOGUE_CACHE[item] = entry
        for item in claimed:
            if _CATALOGUE_LOADS.get(item) is owner:
                _CATALOGUE_LOADS.pop(item, None)
                _CATALOGUE_EXPIRIES.pop(item, None)
        for oldest in sorted(_CATALOGUE_CACHE if completed else {},
                             key=lambda item: _CATALOGUE_CACHE[item].get("checked", 0)):
            if len(_CATALOGUE_CACHE) <= 2048:
                break
            if oldest in _CATALOGUE_LOADS:
                continue
            del _CATALOGUE_CACHE[oldest]
        if any(previous[item].get("value") != completed[item].get("value") for item in completed):
            get_cache().invalidate_prefix("dummy_epg_xmltv")
    return completed


def _mapping_checks(
    profile_links: dict,
    entries: dict,
    sources: list[dict],
    client,
    captured_at: str,
    expected: dict | None = None,
) -> tuple[dict, dict]:
    checked_at = time.monotonic()
    checks = {}
    for link in profile_links:
        key = (client, link)
        entry = entries.get(key, {})
        active = key in _CATALOGUE_LOADS
        error = bool(entry.get("error"))
        value_present = "value" in entry
        cached = bool(entry.get("value"))
        row = entry.get("value") if isinstance(entry.get("value"), dict) else None
        row_id = row.get("id") if row else None
        if isinstance(row_id, bool) or not isinstance(row_id, int):
            row_id = None
        source_id = None
        if row:
            try:
                source_id = _epg_source_id(row.get("epg_source") or row.get("epg_source_id"))
            except (TypeError, ValueError):
                pass
        if isinstance(source_id, bool) or not isinstance(source_id, int):
            source_id = None
        source_kind = "unknown"
        source = next(
            (item for item in sources if item.get("id") == source_id),
            None,
        ) if source_id is not None else None
        if source is not None and source_id is not None:
            try:
                source_url = source.get("url")
                urlsplit(source_url or "")
                if _dummy_source(source_id, [source]):
                    source_kind = "generated"
                elif isinstance(source_url, str) and source_url:
                    source_kind = "external"
            except (TypeError, ValueError):
                pass
        checked = entry.get("checked")
        checked_age = None
        if (isinstance(checked, (int, float)) and not isinstance(checked, bool)
                and math.isfinite(checked)):
            checked_age = max(0.0, checked_at - checked)
        load_expires_at = None
        load_expiry = _CATALOGUE_EXPIRIES.get(key)
        if (active and isinstance(load_expiry, datetime) and load_expiry.tzinfo is not None
                and load_expiry.utcoffset() is not None):
            load_expires_at = load_expiry.isoformat()
        pending = (
            active or error or not value_present
            or checked_age is None or checked_age >= SOURCE_RETRY
            or (expected is not None
                and expected.get(key, {}).get("value") != entry.get("value"))
        )
        checks[link] = {
            "active": active,
            "error": error,
            "value_present": value_present,
            "cached": cached,
            "cached_row_id": row_id,
            "cached_row_matches_link": row_id == link if row_id is not None else None,
            "cached_source_id": source_id,
            "cached_source_kind": source_kind,
            "checked_age_seconds": checked_age,
            "load_expires_at": load_expires_at,
            "pending": pending,
            "unresolved": pending and not cached,
        }
    pending = sorted(link for link in profile_links if checks[link]["pending"])
    observation = {
        "captured_at": captured_at,
        "counts": {
            "linked": len(profile_links),
            "pending": len(pending),
            "active_cached": sum(
                check["active"] and check["cached"]
                for check in checks.values()
            ),
            "active_uncached": sum(
                check["active"] and not check["cached"]
                for check in checks.values()
            ),
            "error_cached": sum(
                check["error"] and check["cached"]
                for check in checks.values()
            ),
            "error_uncached": sum(
                check["error"] and not check["cached"]
                for check in checks.values()
            ),
            "ready_value": sum(
                not check["pending"] and check["cached"]
                for check in checks.values()
            ),
            "unresolved": sum(
                check["unresolved"]
                for check in checks.values()
            ),
        },
        "links": [
            {
                "channel_id": min(profile_links[link]),
                "link_id": link,
                **{
                    name: value for name, value in checks[link].items()
                    if name not in {"pending", "unresolved"}
                },
            }
            for link in pending[:5]
        ],
    }
    return observation, checks


def _compose(query: dict, sources: list[dict], entries: dict, start: datetime, stop: datetime, now: datetime,
             artwork: dict | None = None, artwork_cache=None) -> tuple[list, dict]:
    from config import get_settings
    alias_index = build_team_alias_index(get_settings().event_sync_team_aliases or [])
    priority = build_source_priority_order(sources)
    candidates, warnings = [], set()
    witnesses = []
    parsed = query["event"]
    for source in sources:
        entry = entries.get(source["id"], {})
        selection = entry.get("selection")
        if selection is not None and query["key"] not in selection.get("queries", ()):
            warnings.add("schedule_pending")
            continue
        warnings.update(entry.get("channel_warnings", {}).get(query["key"], []))
        identities = {tvg_id: _identity(query, source["id"], tvg_id, entry.get("headers", {}).get(tvg_id))
                      for tvg_id in entry.get("rows", {}).keys() | entry.get("headers", {}).keys()}
        rank = min((value for value in identities.values() if value is not None), default=None)
        ambiguous = rank is not None and rank >= 2 and sum(value == rank for value in identities.values()) > 1
        if ambiguous:
            warnings.add("ambiguous_identity")
        ended = entry.get("ended", {}).get(query["key"])
        if ended:
            witnesses.append((ended[1], ended[2], source["id"], ended[0], ended[3]))
        for tvg_id, programmes in entry.get("rows", {}).items():
            identity = identities[tvg_id]
            if not query["dynamic"] and (rank is None or ambiguous or identity != rank):
                continue
            for programme in programmes:
                begin, end = programme_times(programme)
                match = identity
                if query["dynamic"] and parsed.start is not None:
                    window = query["time_window_minutes"] if query["enforce_time_window"] else None
                    if window is not None and abs((parsed.start - begin).total_seconds()) > window * 60:
                        continue
                    pair = _score_parsed_pair(
                        parsed, _event(programme, begin), window_minutes=window,
                        threshold=query["attach_threshold"], alias_index=alias_index,
                    )
                    if pair.band != BAND_ATTACH:
                        continue
                    match = 0 if identity == 0 else 1
                elif identity is None:
                    continue
                if query["dynamic"] and parsed.start is not None:
                    witnesses.append((begin, end, source["id"], tvg_id, programme))
                if end <= now or end <= start or begin >= stop:
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
    result["event"] = None
    if witnesses and len({row[0] for row in witnesses}) == 1 and "ambiguous_event" not in warnings:
        witness = max(witnesses, key=lambda row: (row[1], -priority[row[2]]))
        result["event"] = {
            "start": witness[0].isoformat(), "stop": witness[1].isoformat(),
            "source_id": witness[2], "source_tvg_id": witness[3],
            "title": witness[4].findtext("title") or "",
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


async def prepare_profiles(profiles: list[dict], channel_map: dict, client, *, expires_at: datetime | None,
                           now: datetime | None = None,
                           wait_for_sources: bool = False,
                           recover_sources: bool = False) -> tuple[list[dict], dict]:
    """Prepare the same selected schedules for HTTP, diagnostics and scheduled generation."""
    global _ARTWORK_LOAD
    from dummy_epg_engine import get_xmltv_id
    artwork, artwork_cache = {}, None
    realtime = now is None
    now = now or datetime.now(timezone.utc)
    expires_at = _expiry(expires_at)
    if expires_at is not None and _remaining(expires_at) <= 0:
        raise TimeoutError("Guide preparation lifetime has expired.")
    enriched, coverage = [], {"generated_at": now.isoformat(), "window_start": None, "window_stop": None,
                              "sources": [], "channels": [], "profiles": {}}
    profiles = sorted(profiles, key=lambda profile: (profile.get("id") is None, profile.get("id") or 0))
    selected_ids = {source for profile in profiles if profile.get("enabled", True)
                    for source in profile.get("epg_source_ids") or []}
    sources, epg_rows, catalogue_error = [], [], None
    unresolved_links = set()
    pending_links = set()
    catalogue_status = "pending"
    mapping_captured_at = ""
    catalogue_checks = {}
    catalogue = {}
    catalogue_tasks = {}
    source_key = None
    catalogue_sources_pending = False
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
        source_key = (client, None)
        source_entry = _CATALOGUE_CACHE.get(source_key, {})
        if (time.monotonic() - source_entry.get("checked", float("-inf")) >= SOURCE_RETRY
                and source_key not in _CATALOGUE_LOADS):
            task = asyncio.create_task(_load_catalogue(source_key, client, expires_at=None))
            _CATALOGUE_LOADS[source_key] = task
            _CATALOGUE_EXPIRIES[source_key] = None
        due = [
            key for key in keys if key[1] is not None
            and time.monotonic() - _CATALOGUE_CACHE.get(key, {}).get(
                "checked", float("-inf")
            ) >= SOURCE_RETRY
            and key not in _CATALOGUE_LOADS
        ]
        if due:
            batch = (client, tuple(key[1] for key in due))
            task = asyncio.create_task(_load_catalogue(batch, client, expires_at=None))
            for key in due:
                _CATALOGUE_LOADS[key] = task
                _CATALOGUE_EXPIRIES[key] = None
        if not wait_for_sources and not recover_sources and any(key in _CATALOGUE_LOADS for key in keys):
            await asyncio.sleep(0)
        catalogue = {key: _CATALOGUE_CACHE.get(key, {}) for key in keys}
        loading = {key: _CATALOGUE_LOADS[key] for key in keys if key in _CATALOGUE_LOADS}
        catalogue_tasks = dict(loading)
        if loading and (wait_for_sources or recover_sources):
            remaining = _remaining(expires_at)
            if not wait_for_sources:
                remaining = CATALOGUE_TIMEOUT if remaining is None else min(CATALOGUE_TIMEOUT, remaining)
            await asyncio.wait(
                set(loading.values()), timeout=None if remaining is None else max(0, remaining)
            )
        for key, task in loading.items():
            if task.done() and not task.cancelled():
                result = task.result()
                catalogue[key] = result.get(key, _CATALOGUE_CACHE.get(key, catalogue[key]))
            else:
                catalogue[key] = _CATALOGUE_CACHE.get(key, catalogue[key])
        mapping_captured_at = datetime.now(timezone.utc).isoformat()
        sources = catalogue.get((client, None), {}).get("value") or []
        _, catalogue_checks = _mapping_checks(
            {link: [link] for link in links},
            catalogue,
            sources,
            client,
            mapping_captured_at,
        )
        source_entry = catalogue.get(source_key, {})
        catalogue_sources_pending = (
            source_key in _CATALOGUE_LOADS
            or bool(source_entry.get("error"))
            or "value" not in source_entry
        )
        for key in keys:
            entry = catalogue.get(key, {})
            if key in _CATALOGUE_LOADS or entry.get("error") or "value" not in entry:
                if key[1] is None:
                    catalogue_error = "Configured EPG sources are temporarily unavailable."
                    if key not in _CATALOGUE_LOADS:
                        catalogue_status = "error"
                if key[1] is not None and not entry.get("value"):
                    unresolved_links.add(key[1])
                if key[1] is not None:
                    pending_links.add(key[1])
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
        if not profile.get("enabled", True):
            continue
        profile_id = profile.get("id")
        profile_coverage = {
            "profile_id": profile_id,
            "source_ids": [],
            "sources": [],
            "owned_channel_ids": sorted({
                item["channel_id"] for item in assignments
                if item.get("channel_id") in channel_map
            }),
            "can_publish": not bool(profile.get("epg_source_ids")),
            "reason_codes": [],
        }
        if any(item.get("channel_id") not in channel_map for item in assignments):
            profile_coverage["can_publish"] = False
            profile_coverage["reason_codes"].append("GUIDE_CHANNEL_UNAVAILABLE")
        coverage["profiles"][str(profile_id)] = profile_coverage
        tz = pytz.timezone(profile.get("event_timezone") or "US/Eastern")
        local = now.astimezone(tz)
        midnight = datetime(local.year, local.month, local.day)
        start = tz.localize(midnight).astimezone(timezone.utc)
        stop = tz.localize(midnight + timedelta(days=2)).astimezone(timezone.utc)
        profile.update({"guide_start": start, "guide_stop": stop, "source_programmes": {}, "source_channels": {}})
        coverage["window_start"] = min(coverage["window_start"] or start.isoformat(), start.isoformat())
        coverage["window_stop"] = max(coverage["window_stop"] or stop.isoformat(), stop.isoformat())
        if not profile.get("epg_source_ids"):
            prepared.append((profile, [], [], start, stop, profile_coverage, {}))
            continue
        try:
            resolved = resolve_sources(profile["epg_source_ids"], sources)
            profile_coverage["source_ids"] = [source["id"] for source in resolved]
            mappings = {item["channel_id"]: item for item in capture_mappings(profile, channel_map, epg_rows, sources)}
        except ValueError as exc:
            resolved, mappings = [], {}
            profile_coverage["reason_codes"].append("GUIDE_SOURCES_PENDING")
            profile_coverage["sources"].extend(
                {"source_id": source, "status": "error", "last_success": None, "error": str(exc)}
                for source in profile["epg_source_ids"]
            )
            coverage["sources"].extend(
                {"source_id": source, "status": "error", "last_success": None, "error": str(exc)}
                for source in profile["epg_source_ids"]
            )
        try:
            queries = [_query(profile, {**channel_map[item["channel_id"]], "id": item["channel_id"]},
                              mappings.get(item["channel_id"]), now, item)
                       for item in assignments if item.get("channel_id") in channel_map]
        except ValueError:
            queries = []
            profile_coverage["reason_codes"].append("GUIDE_CONFIG_INVALID")
        profile_links = {}
        for query in queries:
            if query["dynamic"]:
                continue
            channel_id = query["channel_id"]
            link = channel_map[channel_id].get("epg_data_id") or channel_map[channel_id].get("epg_data")
            if isinstance(link, int) and not isinstance(link, bool):
                profile_links.setdefault(link, []).append(channel_id)
        if any(catalogue_checks[link]["pending"] for link in profile_links):
            profile_coverage["mapping_checks"] = _mapping_checks(
                profile_links, catalogue, sources, client, mapping_captured_at,
            )[0]
        if any(
            not query["dynamic"]
            and isinstance((link := (
                channel_map[query["channel_id"]].get("epg_data_id")
                or channel_map[query["channel_id"]].get("epg_data")
            )), int)
            and not isinstance(link, bool)
            and link in pending_links
            for query in queries
        ):
            profile_coverage["reason_codes"].extend([
                "GUIDE_MAPPING_UNAVAILABLE", "GUIDE_SOURCES_PENDING",
            ])
        prepared.append((profile, resolved, queries, start, stop, profile_coverage, profile_links))
        for query in queries:
            channel = channel_map[query["channel_id"]]
            link = channel.get("epg_data_id") or channel.get("epg_data")
            # Dynamic slots never consume the previous guide binding. They
            # match parsed event evidence or remain unresolved without it.
            if isinstance(link, int) and link in unresolved_links and not query["dynamic"]:
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
            selected_queries = [
                query for query in queries
                if not query.get("blocked")
                or (query.get("blocked") == "mapping_unavailable" and query.get("mapping"))
            ]
            if not selected_queries:
                continue
            job = jobs.setdefault(source["id"], {"source": source, "queries": [], "start": start, "stop": stop})
            job["queries"].extend(selected_queries)
            job["start"], job["stop"] = min(start, job["start"]), max(stop, job["stop"])
    for source_id, job in jobs.items():
        fingerprint = json.dumps({"id": source_id, "url": job["source"].get("url")}, sort_keys=True, default=str)
        key = hashlib.sha256(fingerprint.encode()).hexdigest()
        job["key"] = key
        entry = _SOURCE_CACHE.setdefault(key, {})
        demand = {identity: request for identity, request in entry.get("demand", {}).items() if request["stop"] > now}
        # Query changes join the next permitted scan; they cannot bypass source backoff.
        for query in job["queries"]:
            identity = query["key"]
            if identity in demand or len(demand) < MAX_QUERIES:
                demand[identity] = {"query": query, "start": job["start"], "stop": job["stop"]}
        entry["demand"] = demand
        if not demand:
            continue
        success = entry.get("success")
        selection = entry.get("selection") or {}
        demand_start = min(request["start"] for request in demand.values())
        demand_stop = max(request["stop"] for request in demand.values())
        covered = (
            success is not None
            and all(identity in selection.get("queries", ()) for identity in demand)
            and selection.get("start", demand_stop) <= demand_start
            and selection.get("stop", demand_start) >= demand_stop
        )
        stale = success is not None and (
            datetime.now(timezone.utc) - success
        ).total_seconds() > SOURCE_MAX_AGE
        recovery_needed = bool(entry.get("error")) or not covered or stale
        age = time.monotonic() - entry.get("checked", float("-inf"))
        if ((wait_for_sources or (recover_sources and recovery_needed))
                and age >= (SOURCE_RETRY if recovery_needed else SOURCE_TTL)
                and key not in _SOURCE_LOADS
                and (expires_at is None or _remaining(expires_at) > 0)):
            _SOURCE_EXPIRIES[key] = None
            _SOURCE_LOADS[key] = asyncio.create_task(_load_source(
                key, job["source"], [request["query"] for request in demand.values()],
                min(request["start"] for request in demand.values()),
                max(request["stop"] for request in demand.values()), now,
                expires_at=None,
            ))
    if not wait_for_sources and any(job["key"] in _SOURCE_LOADS for job in jobs.values()):
        await asyncio.sleep(0)
    pending = [_SOURCE_LOADS[job["key"]] for job in jobs.values() if job["key"] in _SOURCE_LOADS]
    if pending and wait_for_sources:
        await asyncio.wait(
            pending,
            timeout=None if expires_at is None else max(0, _remaining(expires_at)),
        )
    for key in list(_SOURCE_CACHE):
        if len(_SOURCE_CACHE) <= MAX_CACHE_ENTRIES:
            break
        if key not in _SOURCE_LOADS and not _SOURCE_CACHE[key].get("success"):
            _SOURCE_CACHE.pop(key, None)
    if realtime:
        now = datetime.now(timezone.utc)
        coverage["generated_at"] = now.isoformat()
    entries = {}
    for source_id, job in jobs.items():
        entry = _SOURCE_CACHE.get(job["key"], {})
        entries[source_id] = entry
        success = entry.get("success")
        selection = entry.get("selection") or {}
        covered = (all(query["key"] in selection.get("queries", ()) for query in job["queries"])
                   and selection.get("start", job["stop"]) <= job["start"]
                   and selection.get("stop", job["start"]) >= job["stop"])
        status = ("pending" if job["key"] in _SOURCE_LOADS else "error" if entry.get("error")
                  else "ready" if success and covered else "pending" if entry else "error")
        if success and (datetime.now(timezone.utc) - success).total_seconds() > SOURCE_MAX_AGE:
            status = "stale"
        coverage["sources"].append({"source_id": source_id, "status": status,
                                    "last_success": success.isoformat() if success else None,
                                    "error": entry.get("error") or ("No complete XMLTV schedule is available." if status == "error" else None),
                                    "diagnostics": entry.get("diagnostics", {})})
        if entry.get("warnings"):
            coverage["sources"][-1]["warnings"] = entry["warnings"]
    if catalogue_error:
        coverage["sources"] = [{"source_id": source, "status": catalogue_status, "last_success": None, "error": catalogue_error}
                               for source in sorted(selected_ids)]
    for profile, resolved, queries, start, stop, profile_coverage, profile_links in prepared:
        reasons = set(profile_coverage["reason_codes"])
        if profile.get("epg_source_ids") and catalogue_sources_pending:
            reasons.add("GUIDE_SOURCES_PENDING")
        for query in queries:
            if query.get("blocked"):
                reasons.add("GUIDE_QUERY_PENDING")
                if query["blocked"] == "mapping_unavailable":
                    reasons.add("GUIDE_MAPPING_UNAVAILABLE")
                elif query["blocked"] == "source_not_selected":
                    reasons.add("GUIDE_SOURCE_NOT_SELECTED")
        diagnostics = []
        for source in resolved:
            entry = entries.get(source["id"], {})
            success = entry.get("success")
            selection = entry.get("selection") or {}
            applicable = [query for query in queries if not query.get("blocked")]
            covered = (
                success is not None
                and all(query["key"] in selection.get("queries", ()) for query in applicable)
                and selection.get("start", stop) <= start
                and selection.get("stop", start) >= stop
            )
            stale = success is not None and (
                datetime.now(timezone.utc) - success
            ).total_seconds() > SOURCE_MAX_AGE
            retained = covered and bool(entry.get("error"))
            status = "stale" if stale else "retained" if retained else "ready" if covered else (
                "pending" if any(job.get("key") in _SOURCE_LOADS for job in jobs.values() if job["source"]["id"] == source["id"])
                else "error"
            )
            diagnostics.append({
                "source_id": source["id"],
                "status": status,
                "last_success": success.isoformat() if success else None,
                "error": entry.get("error"),
                "diagnostics": entry.get("diagnostics", {}),
            })
            if not covered or stale:
                reasons.add("GUIDE_SOURCES_PENDING")
                reasons.add("GUIDE_SOURCE_STALE" if stale else "GUIDE_QUERY_PENDING")
        profile_coverage["sources"] = diagnostics or profile_coverage["sources"]
        profile_coverage["reason_codes"] = sorted(reasons)
        profile_coverage["can_publish"] = not reasons and len(resolved) == len(profile_coverage["source_ids"])

    ownership = _profile_owners(enriched, channel_map, coverage)
    owners = ownership[0]
    collisions = ownership[1]
    seen_channels = set()
    if prepared:
        from config import CONFIG_DIR
        from services.epg_artwork import ArtworkCache
        artwork_cache = ArtworkCache(CONFIG_DIR / "epg_artwork_cache.json")
    for profile, resolved, queries, start, stop, profile_coverage, profile_links in prepared:
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
            result["profile_id"] = profile.get("id")
            profile["source_programmes"][channel_id] = programmes
            if result["source_id"]:
                header = entries.get(result["source_id"], {}).get("headers", {}).get(result["source_tvg_id"])
                if header is not None:
                    profile["source_channels"][channel_id] = copy.deepcopy(header)
            if channel_id in seen_channels or owners.get(channel_id) is not profile:
                result["warnings"].append("overlapping_profile")
                profile_coverage["can_publish"] = False
                profile_coverage["reason_codes"] = sorted(set(profile_coverage["reason_codes"]) | {"GUIDE_OWNERSHIP_CONFLICT"})
                continue
            seen_channels.add(channel_id)
            if channel_id in collisions:
                result["warnings"].append("xmltv_id_collision")
                result["match"] = "collision"
                profile_coverage["can_publish"] = False
                profile_coverage["reason_codes"] = sorted(set(profile_coverage["reason_codes"]) | {"GUIDE_XMLTV_ID_COLLISION"})
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
    due = [key for key, entry in catalogue.items()
           if time.monotonic() - _CATALOGUE_CACHE.get(key, entry).get("checked", float("-inf")) >= SOURCE_RETRY
           and key not in _CATALOGUE_LOADS]
    if expires_at is None or _remaining(expires_at) > 0:
        if source_key in due:
            task = asyncio.create_task(_load_catalogue(source_key, client, expires_at=None))
            _CATALOGUE_LOADS[source_key] = task
            _CATALOGUE_EXPIRIES[source_key] = None
        rows_due = [key for key in due if key[1] is not None]
        if rows_due:
            batch = (client, tuple(key[1] for key in rows_due))
            task = asyncio.create_task(_load_catalogue(batch, client, expires_at=None))
            for key in rows_due:
                _CATALOGUE_LOADS[key] = task
                _CATALOGUE_EXPIRIES[key] = None
    catalogue_tasks = {key: _CATALOGUE_LOADS[key] for key in catalogue if key in _CATALOGUE_LOADS}
    if catalogue_tasks and (wait_for_sources or recover_sources):
        remaining = _remaining(expires_at)
        if not wait_for_sources:
            remaining = CATALOGUE_TIMEOUT if remaining is None else min(CATALOGUE_TIMEOUT, remaining)
        await asyncio.wait(
            set(catalogue_tasks.values()), timeout=None if remaining is None else max(0, remaining)
        )
    final_captured_at = datetime.now(timezone.utc).isoformat()
    final_catalogue = {}
    for key in catalogue:
        active = _CATALOGUE_LOADS.get(key)
        if active is not None:
            final_catalogue[key] = _CATALOGUE_CACHE.get(key, catalogue[key])
            continue
        task = catalogue_tasks.get(key)
        if task is not None and task.done() and not task.cancelled():
            result = task.result()
            final_catalogue[key] = result.get(key, _CATALOGUE_CACHE.get(key, catalogue[key]))
        else:
            final_catalogue[key] = _CATALOGUE_CACHE.get(key, catalogue[key])
    final_sources_pending = False
    final_sources = sources
    if source_key is not None:
        source_entry = final_catalogue.get(source_key, {})
        source_checked = source_entry.get("checked")
        source_age = None
        if (isinstance(source_checked, (int, float)) and not isinstance(source_checked, bool)
                and math.isfinite(source_checked)):
            source_age = max(0.0, time.monotonic() - source_checked)
        final_sources_pending = (
            source_key in _CATALOGUE_LOADS
            or bool(source_entry.get("error"))
            or "value" not in source_entry
            or source_age is None
            or source_age >= SOURCE_RETRY
            or catalogue.get(source_key, {}).get("value") != source_entry.get("value")
        )
        final_sources = source_entry.get("value") or []
    for profile, resolved, queries, start, stop, profile_coverage, profile_links in prepared:
        if profile_links:
            observation, checks = _mapping_checks(
                profile_links,
                final_catalogue,
                final_sources,
                client,
                final_captured_at,
                expected=catalogue,
            )
            if any(check["pending"] for check in checks.values()):
                profile_coverage["mapping_checks"] = observation
                profile_coverage["reason_codes"] = sorted(
                    set(profile_coverage["reason_codes"])
                    | {"GUIDE_MAPPING_UNAVAILABLE", "GUIDE_SOURCES_PENDING"}
                )
                profile_coverage["can_publish"] = False
        if profile.get("epg_source_ids") and final_sources_pending:
            profile_coverage["reason_codes"] = sorted(
                set(profile_coverage["reason_codes"]) | {"GUIDE_SOURCES_PENDING"}
            )
            profile_coverage["can_publish"] = False
    if expires_at is not None and _remaining(expires_at) <= 0:
        for profile_coverage in coverage["profiles"].values():
            profile_coverage["can_publish"] = False
            profile_coverage["reason_codes"] = sorted(
                set(profile_coverage["reason_codes"]) | {"GUIDE_SOURCES_PENDING"}
            )
    coverage["artwork_pending"] = bool(artwork)
    if artwork and _ARTWORK_LOAD is None and time.monotonic() - _ARTWORK_CHECKED >= SOURCE_RETRY:
        _ARTWORK_LOAD = asyncio.create_task(_probe_artwork(artwork))
    return enriched, coverage
