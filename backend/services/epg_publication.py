"""Durable complete XMLTV publication and retained runtime evidence."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
import xml.etree.ElementTree as ET

import pytz
from sqlalchemy.exc import IntegrityError

from database import get_session
from models import GuidePublication
from services.epg_programmes import MAX_CACHE, MAX_RETAINED, programme_times


STATE_VERSION = 1
AGGREGATE_SCOPE = "all"
MAX_OBSERVATIONS = 10000
MAX_CHANNELS = 10000
publication_lock = asyncio.Lock()


@dataclass(frozen=True)
class PublicationResult:
    """Outcome of one complete-publication attempt."""

    published_profile_ids: tuple[int, ...] = ()
    retained_profile_ids: tuple[int, ...] = ()
    unavailable_profile_ids: tuple[int, ...] = ()
    xmltv_by_scope: dict[str, str] = field(default_factory=dict)
    states_by_profile: dict[int, dict] = field(default_factory=dict)
    hashes_by_scope: dict[str, str] = field(default_factory=dict)
    superseded: bool = False
    reason_codes: tuple[str, ...] = ()


def _scope(profile_id: int) -> str:
    return f"profile:{profile_id}"


def _utc(value: datetime | str, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO datetime with an offset.") from exc
    else:
        raise ValueError(f"{field_name} must be an ISO datetime with an offset.")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit UTC offset.")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime | str, field_name: str) -> str:
    return _utc(value, field_name).isoformat()


def _hash(document: str) -> str:
    return hashlib.sha256(document.encode("utf-8")).hexdigest()


def _state_text(state: dict) -> str:
    value = json.dumps(state, sort_keys=True, separators=(",", ":"))
    if len(value.encode("utf-8")) > MAX_RETAINED:
        raise ValueError("Publication state exceeds its size limit.")
    return value


def _document(document: str, limit: int) -> tuple[ET.Element, tuple[str, ...]]:
    if not isinstance(document, str) or not document:
        raise ValueError("Publication XMLTV must be a nonempty string.")
    if len(document.encode("utf-8")) > limit:
        raise ValueError("Publication XMLTV exceeds its size limit.")
    try:
        root = ET.fromstring(document)
    except ET.ParseError as exc:
        raise ValueError("Publication XMLTV is malformed.") from exc
    if root.tag != "tv":
        raise ValueError("Publication XMLTV root must be tv.")
    channel_ids = []
    seen = set()
    for channel in root.findall("channel"):
        xmltv_id = channel.get("id", "")
        if not xmltv_id or xmltv_id in seen:
            raise ValueError("Publication XMLTV channel IDs must be nonempty and unique.")
        seen.add(xmltv_id)
        channel_ids.append(xmltv_id)
    for programme in root.findall("programme"):
        if programme.get("channel") not in seen:
            raise ValueError("Publication XMLTV programme references an unknown channel.")
        programme_times(programme)
    return root, tuple(channel_ids)


def _parse_state(raw: str, *, document: str | None) -> dict:
    try:
        state = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("Stored publication state is malformed.") from exc
    if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
        raise ValueError("Stored publication state version is unsupported.")
    for name in ("published_at", "window_start", "window_stop"):
        _utc(state.get(name), f"state.{name}")
    if _utc(state["window_stop"], "state.window_stop") < _utc(
        state["window_start"], "state.window_start"
    ):
        raise ValueError("Stored publication window is invalid.")
    xmltv_hash = state.get("xmltv_hash")
    config_hash = state.get("config_hash")
    if not isinstance(xmltv_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", xmltv_hash):
        raise ValueError("Stored publication XMLTV hash is invalid.")
    if not isinstance(config_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", config_hash):
        raise ValueError("Stored publication config hash is invalid.")
    if document is None or _hash(document) != xmltv_hash:
        raise ValueError("Stored publication XMLTV does not match its state.")
    _, document_channels = _document(document, MAX_CACHE)
    channels = state.get("channels")
    if not isinstance(channels, list) or len(channels) > MAX_CHANNELS:
        raise ValueError("Stored publication channels are invalid.")
    state_channels = []
    for channel in channels:
        if not isinstance(channel, dict):
            raise ValueError("Stored publication channel is invalid.")
        channel_id = channel.get("channel_id")
        xmltv_id = channel.get("xmltv_id")
        events = channel.get("events")
        if (
            not isinstance(channel_id, int) or isinstance(channel_id, bool)
            or not isinstance(xmltv_id, str) or not xmltv_id
            or not isinstance(events, list)
        ):
            raise ValueError("Stored publication channel is invalid.")
        state_channels.append(xmltv_id)
        for event in events:
            if not isinstance(event, dict):
                raise ValueError("Stored publication event is invalid.")
            start = _utc(event.get("start"), "stored event start")
            stop = _utc(event.get("stop"), "stored event stop")
            title = event.get("title")
            if stop <= start or not isinstance(title, str) or not title.strip() or len(title) > 4096:
                raise ValueError("Stored publication event is invalid.")
    if len(state_channels) != len(set(state_channels)) or set(state_channels) != set(document_channels):
        raise ValueError("Stored publication channel evidence does not match its XMLTV.")
    observations = state.get("observations")
    if not isinstance(observations, dict) or len(observations) > MAX_OBSERVATIONS:
        raise ValueError("Stored publication observations are invalid.")
    normalized_observations = _observations(observations)
    if normalized_observations != observations:
        raise ValueError("Stored publication observation identity is invalid.")
    members = state.get("members")
    if not isinstance(members, dict) or len(members) > MAX_CHANNELS:
        raise ValueError("Stored publication members are invalid.")
    if any(
        not isinstance(key, str) or re.fullmatch(r"[0-9a-f]{64}", value or "") is None
        for key, value in members.items()
    ):
        raise ValueError("Stored publication members are invalid.")
    delivery = state.get("delivery")
    if not isinstance(delivery, dict):
        raise ValueError("Stored publication delivery state is invalid.")
    for name in ("required_dispatcharr_hashes", "confirmed_dispatcharr_hashes"):
        hashes = delivery.get(name)
        if not isinstance(hashes, dict) or len(hashes) > MAX_CHANNELS or any(
            not isinstance(key, str) or re.fullmatch(r"[0-9a-f]{64}", value or "") is None
            for key, value in hashes.items()
        ):
            raise ValueError("Stored publication delivery state is invalid.")
    if not isinstance(delivery.get("pending_emby"), bool):
        raise ValueError("Stored publication delivery state is invalid.")
    return state


def _read_row(row: GuidePublication) -> dict:
    state = _parse_state(row.state, document=row.xmltv)
    if not isinstance(row.revision, int) or row.revision < 1:
        raise ValueError("Stored publication revision is invalid.")
    if row.scope == AGGREGATE_SCOPE:
        expected = hashlib.sha256(
            json.dumps(state["members"], sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        if state["config_hash"] != expected:
            raise ValueError("Stored aggregate membership hash is invalid.")
    else:
        profile_id = row.scope.removeprefix("profile:")
        if state["members"] != {profile_id: state["xmltv_hash"]}:
            raise ValueError("Stored profile membership is invalid.")
    return {
        "scope": row.scope,
        "xmltv": row.xmltv,
        "state": state,
        "revision": row.revision,
    }


def read_publication(scope: str) -> dict | None:
    """Read one complete publication; only a missing row returns ``None``."""
    if scope != AGGREGATE_SCOPE and re.fullmatch(r"profile:[1-9]\d*", scope) is None:
        raise ValueError("Publication scope is invalid.")
    session = get_session()
    try:
        row = session.query(GuidePublication).filter(GuidePublication.scope == scope).one_or_none()
        return None if row is None else _read_row(row)
    finally:
        session.close()


def _plain(value):
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _config_hash(profile: dict) -> str:
    transient = {
        "guide_start", "guide_stop", "source_programmes", "source_channels",
        "event_intervals", "last_generated_at", "created_at", "updated_at",
    }
    value = {key: item for key, item in profile.items() if key not in transient}
    return hashlib.sha256(
        json.dumps(_plain(value), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _intervals(profile: dict, channel_map: dict) -> list[dict]:
    records = []
    prepared = {} if profile.get("epg_source_ids") else profile.get("event_intervals") or {}
    if isinstance(prepared, list):
        by_channel = {}
        for interval in prepared:
            if isinstance(interval, Mapping):
                by_channel.setdefault(interval.get("channel_id"), []).append(interval)
    elif isinstance(prepared, Mapping):
        by_channel = prepared
    else:
        raise ValueError("Prepared event intervals must be a list or object.")

    source_programmes = profile.get("source_programmes") or {}
    for assignment in profile.get("channel_assignments") or []:
        channel_id = assignment.get("channel_id")
        if not isinstance(channel_id, int) or isinstance(channel_id, bool) or channel_id <= 0:
            raise ValueError("Publication channel IDs must be positive integers.")
        channel = channel_map.get(channel_id)
        if channel is None:
            raise ValueError("Publication channel membership is incomplete.")
        from dummy_epg_engine import get_xmltv_id

        xmltv_id = get_xmltv_id(assignment, channel, profile)
        events = []
        for programme in source_programmes.get(channel_id, []):
            start, stop = programme_times(programme)
            title = (programme.findtext("title") or "").strip()
            if title:
                events.append({"start": start.isoformat(), "stop": stop.isoformat(), "title": title})
        values = by_channel.get(channel_id, by_channel.get(str(channel_id), []))
        if values is None:
            values = []
        if not isinstance(values, list):
            raise ValueError("Prepared event intervals for a channel must be a list.")
        for interval in values:
            if not isinstance(interval, Mapping):
                raise ValueError("Prepared event interval must be an object.")
            start = _utc(interval.get("start"), "event interval start")
            stop = _utc(interval.get("stop"), "event interval stop")
            title = interval.get("title")
            if stop <= start or not isinstance(title, str) or not title.strip() or len(title) > 4096:
                raise ValueError("Prepared event interval bounds and title are required.")
            events.append({"start": start.isoformat(), "stop": stop.isoformat(), "title": title.strip()})
        events.sort(key=lambda item: (item["start"], item["stop"], item["title"]))
        records.append({
            "channel_id": channel_id,
            "xmltv_id": xmltv_id,
            "profile_id": profile["id"],
            "events": events,
        })
    if len(records) > MAX_CHANNELS:
        raise ValueError("Publication channel count exceeds its limit.")
    return records


def _observation_key(value: Mapping) -> str:
    parts = (
        value.get("family"), value.get("slot"), value.get("stream_id"),
        value.get("normalized_name"),
    )
    if any(part is None or (isinstance(part, str) and not part.strip()) for part in parts):
        raise ValueError("Observation identity is incomplete.")
    if any("\x1f" in str(part) for part in parts):
        raise ValueError("Observation identity contains an invalid separator.")
    return "\x1f".join(str(part) for part in parts)


def _observations(values) -> dict[str, dict]:
    if values is None:
        return {}
    if isinstance(values, Mapping):
        items = list(values.values())
    elif isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
        items = list(values)
    else:
        raise ValueError("Observations must be a list or object.")
    if len(items) > MAX_OBSERVATIONS:
        raise ValueError("Observation count exceeds its limit.")
    result = {}
    for item in items:
        if not isinstance(item, Mapping):
            raise ValueError("Observation must be an object.")
        stream_id = item.get("stream_id")
        if not isinstance(stream_id, int) or isinstance(stream_id, bool) or stream_id <= 0:
            raise ValueError("Observation stream ID must be a positive integer.")
        provisional = item.get("provisional", True)
        if not isinstance(provisional, bool):
            raise ValueError("Observation provisional state must be a boolean.")
        start = _utc(item.get("start"), "observation start")
        expires_at = _utc(item.get("expires_at"), "observation expiry")
        if expires_at < start:
            raise ValueError("Observation expiry cannot precede its start.")
        title = item.get("title")
        if not isinstance(title, str) or not title.strip() or len(title) > 4096:
            raise ValueError("Observation title is required.")
        for name in ("family", "slot", "normalized_name"):
            if len(str(item[name])) > 512:
                raise ValueError("Observation identity exceeds its size limit.")
        matched_variant = item.get("matched_variant")
        if matched_variant is not None and (
            not isinstance(matched_variant, str) or len(matched_variant) > 512
        ):
            raise ValueError("Observation matched variant is invalid.")
        record = {
            "family": str(item["family"]),
            "slot": str(item["slot"]),
            "stream_id": stream_id,
            "normalized_name": str(item["normalized_name"]),
            "start": start.isoformat(),
            "expires_at": expires_at.isoformat(),
            "title": title.strip(),
            "matched_variant": matched_variant,
            "provisional": provisional,
        }
        result[_observation_key(record)] = record
    return dict(sorted(result.items()))


def _profile_observations(
    observations,
    profile_id: int,
    previous: dict | None,
    now: datetime,
    event_timezone: str,
) -> dict[str, dict]:
    if not isinstance(observations, Mapping):
        raise ValueError("Publication observations must be keyed by profile ID.")
    stored = previous.get("observations", {}) if previous else {}
    if not isinstance(stored, dict):
        raise ValueError("Stored publication observations are invalid.")
    if profile_id in observations:
        submitted = observations[profile_id]
    elif str(profile_id) in observations:
        submitted = observations[str(profile_id)]
    else:
        return copy.deepcopy(stored)
    proposed = _observations(submitted)
    timezone_value = pytz.timezone(event_timezone)
    result = {}
    for key, proposal in proposed.items():
        existing = stored.get(key)
        if isinstance(existing, dict):
            if existing.get("provisional") is True and proposal["provisional"] is False:
                result[key] = proposal
            else:
                result[key] = copy.deepcopy(existing)
            continue
        start = _utc(proposal["start"], "observation start")
        expires_at = _utc(proposal["expires_at"], "observation expiry")
        duration = expires_at - start
        if proposal["provisional"] and duration > timedelta(0) and start > now:
            local = start.astimezone(timezone_value)
            prior_wall = local.replace(tzinfo=None) - timedelta(days=1)
            try:
                prior_start = timezone_value.localize(prior_wall, is_dst=None).astimezone(timezone.utc)
            except (pytz.AmbiguousTimeError, pytz.NonExistentTimeError):
                prior_start = timezone_value.normalize(local - timedelta(days=1)).astimezone(timezone.utc)
            if prior_start <= now < prior_start + duration:
                proposal = {
                    **proposal,
                    "start": prior_start.isoformat(),
                    "expires_at": (prior_start + duration).isoformat(),
                }
        result[key] = proposal
    return dict(sorted(result.items()))


def _bounds(profile: dict, channels: list[dict], now: datetime) -> tuple[datetime, datetime]:
    start = profile.get("guide_start")
    stop = profile.get("guide_stop")
    if start is not None and stop is not None:
        begin = _utc(start, "guide_start")
        end = _utc(stop, "guide_stop")
    else:
        values = [
            _utc(event[name], f"event {name}")
            for channel in channels for event in channel["events"] for name in ("start", "stop")
        ]
        begin = min(values, default=now)
        end = max(values, default=now)
    if end < begin:
        raise ValueError("Publication guide window is invalid.")
    return begin, end


def _delivery(previous: dict | None, xmltv_hash: str) -> dict:
    if previous and previous.get("xmltv_hash") == xmltv_hash:
        stored = previous.get("delivery")
        if isinstance(stored, dict):
            return copy.deepcopy(stored)
    return {
        "required_dispatcharr_hashes": {},
        "confirmed_dispatcharr_hashes": {},
        "pending_emby": True,
    }


def _profile_state(
    profile: dict,
    channel_map: dict,
    document: str,
    observations,
    now: datetime,
    previous: dict | None,
) -> dict:
    channels = _intervals(profile, channel_map)
    begin, end = _bounds(profile, channels, now)
    xmltv_hash = _hash(document)
    config_hash = _config_hash(profile)
    compatible = previous if previous and previous.get("config_hash") == config_hash else None
    return {
        "version": STATE_VERSION,
        "published_at": now.isoformat(),
        "xmltv_hash": xmltv_hash,
        "config_hash": config_hash,
        "window_start": begin.isoformat(),
        "window_stop": end.isoformat(),
        "members": {str(profile["id"]): xmltv_hash},
        "channels": channels,
        "observations": _profile_observations(
            observations, profile["id"], compatible, now,
            profile.get("event_timezone") or "US/Eastern",
        ),
        "delivery": _delivery(compatible, xmltv_hash),
    }


def _combine(publications: Sequence[dict]) -> str:
    root = ET.Element("tv", {
        "generator-info-name": "ECM Enhanced Channel Manager",
        "generator-info-url": "https://github.com/your/ecm",
    })
    seen_channels = set()
    programmes = []
    for publication in publications:
        child, _ = _document(publication["xmltv"], MAX_RETAINED)
        for channel in child.findall("channel"):
            xmltv_id = channel.get("id")
            if xmltv_id in seen_channels:
                raise ValueError("Aggregate publication contains a duplicate outward channel ID.")
            seen_channels.add(xmltv_id)
            root.append(copy.deepcopy(channel))
        programmes.extend(copy.deepcopy(row) for row in child.findall("programme"))
    for programme in programmes:
        root.append(programme)
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode")


def _aggregate_state(publications: Sequence[dict], document: str, now: datetime, previous: dict | None) -> dict:
    channels = [copy.deepcopy(channel) for publication in publications for channel in publication["state"]["channels"]]
    members = {
        str(publication["profile_id"]): publication["state"]["xmltv_hash"]
        for publication in publications
    }
    starts = [_utc(publication["state"]["window_start"], "window_start") for publication in publications]
    stops = [_utc(publication["state"]["window_stop"], "window_stop") for publication in publications]
    xmltv_hash = _hash(document)
    return {
        "version": STATE_VERSION,
        "published_at": now.isoformat(),
        "xmltv_hash": xmltv_hash,
        "config_hash": hashlib.sha256(
            json.dumps(members, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "window_start": min(starts, default=now).isoformat(),
        "window_stop": max(stops, default=now).isoformat(),
        "members": members,
        "channels": channels,
        "observations": {},
        "delivery": _delivery(previous, xmltv_hash),
    }


def _result(
    published: set[int], retained: set[int], unavailable: set[int],
    documents: dict[str, str], states: dict[int, dict], *,
    superseded: bool = False, reasons: set[str] | None = None,
) -> PublicationResult:
    return PublicationResult(
        published_profile_ids=tuple(sorted(published)),
        retained_profile_ids=tuple(sorted(retained)),
        unavailable_profile_ids=tuple(sorted(unavailable)),
        xmltv_by_scope=dict(sorted(documents.items())),
        states_by_profile={key: states[key] for key in sorted(states)},
        hashes_by_scope={scope: _hash(document) for scope, document in sorted(documents.items())},
        superseded=superseded,
        reason_codes=tuple(sorted(reasons or set())),
    )


def publish_profiles(
    profiles: list[dict],
    channel_map: dict,
    coverage: dict,
    *,
    observations,
    now: datetime,
) -> PublicationResult:
    """Render and atomically retain every complete profile and aggregate."""
    from dummy_epg_engine import generate_xmltv

    now = _utc(now, "now")
    enabled = [profile for profile in profiles if profile.get("enabled", True)]
    enabled.sort(key=lambda profile: (profile.get("id") is None, profile.get("id") or 0))
    if any(not isinstance(profile.get("id"), int) or profile["id"] <= 0 for profile in enabled):
        raise ValueError("Publication profiles require positive stored IDs.")
    profile_coverage = coverage.get("profiles")
    if not isinstance(profile_coverage, Mapping):
        raise ValueError("Publication coverage requires profile readiness records.")

    session = get_session()
    try:
        old_rows = {row.scope: row for row in session.query(GuidePublication).all()}
        expected = {scope: row.revision for scope, row in old_rows.items()}
        prior = {}
        corrupt = set()
        for scope, row in old_rows.items():
            try:
                prior[scope] = _read_row(row)
            except ValueError:
                corrupt.add(scope)
    finally:
        session.close()

    candidates = {}
    published, retained, unavailable = set(), set(), set()
    documents, states, reasons = {}, {}, set()
    for profile in enabled:
        profile_id = profile["id"]
        scope = _scope(profile_id)
        readiness = profile_coverage.get(str(profile_id), profile_coverage.get(profile_id))
        ready = isinstance(readiness, Mapping) and readiness.get("can_publish") is True
        if ready:
            try:
                document = generate_xmltv([profile], channel_map)
                _document(document, MAX_RETAINED)
                state = _profile_state(
                    profile, channel_map, document, observations, now,
                    prior.get(scope, {}).get("state"),
                )
                _parse_state(_state_text(state), document=document)
                candidates[scope] = {"xmltv": document, "state": state, "profile_id": profile_id}
                documents[scope] = document
                states[profile_id] = state
                published.add(profile_id)
                continue
            except (TypeError, ValueError):
                reasons.add("GUIDE_PUBLICATION_INVALID")
        elif isinstance(readiness, Mapping):
            reasons.update(str(code) for code in readiness.get("reason_codes", ()) if code)
        if scope in prior:
            documents[scope] = prior[scope]["xmltv"]
            states[profile_id] = prior[scope]["state"]
            retained.add(profile_id)
            if prior[scope]["state"].get("config_hash") != _config_hash(profile):
                reasons.add("GUIDE_CONFIG_CHANGED")
        else:
            unavailable.add(profile_id)
            if scope in corrupt:
                reasons.add("GUIDE_STATE_CORRUPT")

    members = []
    for profile in enabled:
        profile_id = profile["id"]
        scope = _scope(profile_id)
        if scope in candidates:
            members.append(candidates[scope])
            continue
        retained_row = prior.get(scope)
        if retained_row is None or retained_row["state"].get("config_hash") != _config_hash(profile):
            members = []
            break
        members.append({
            "xmltv": retained_row["xmltv"], "state": retained_row["state"],
            "profile_id": profile_id,
        })

    fresh_member = any(_scope(profile["id"]) in candidates for profile in enabled)
    desired_members = {
        str(member["profile_id"]): member["state"]["xmltv_hash"] for member in members
    }
    previous_members = prior.get(AGGREGATE_SCOPE, {}).get("state", {}).get("members")
    membership_changed = desired_members != previous_members
    aggregate_replaced = (
        bool(members) and (fresh_member or membership_changed)
    ) or not enabled
    if aggregate_replaced:
        try:
            aggregate_xml = _combine(members)
            _document(aggregate_xml, MAX_CACHE)
            aggregate_state = _aggregate_state(
                members, aggregate_xml, now, prior.get(AGGREGATE_SCOPE, {}).get("state"),
            )
            _parse_state(_state_text(aggregate_state), document=aggregate_xml)
            candidates[AGGREGATE_SCOPE] = {"xmltv": aggregate_xml, "state": aggregate_state}
            documents[AGGREGATE_SCOPE] = aggregate_xml
        except (TypeError, ValueError):
            aggregate_replaced = False
            reasons.add("GUIDE_AGGREGATE_INVALID")
    if not aggregate_replaced:
        if AGGREGATE_SCOPE in prior:
            documents[AGGREGATE_SCOPE] = prior[AGGREGATE_SCOPE]["xmltv"]
            reasons.add("GUIDE_AGGREGATE_RETAINED")
        elif AGGREGATE_SCOPE in corrupt:
            reasons.add("GUIDE_STATE_CORRUPT")
        else:
            reasons.add("GUIDE_AGGREGATE_UNAVAILABLE")

    if not candidates:
        return _result(published, retained, unavailable, documents, states, reasons=reasons)

    session = get_session()
    try:
        for scope, candidate in sorted(candidates.items()):
            values = {
                "xmltv": candidate["xmltv"],
                "state": _state_text(candidate["state"]),
            }
            if scope in expected:
                updated = (
                    session.query(GuidePublication)
                    .filter(
                        GuidePublication.scope == scope,
                        GuidePublication.revision == expected[scope],
                    )
                    .update({**values, "revision": expected[scope] + 1}, synchronize_session=False)
                )
                if updated != 1:
                    session.rollback()
                    return _result(
                        set(), retained | published, unavailable, documents, states,
                        superseded=True, reasons=reasons | {"GUIDE_PUBLICATION_SUPERSEDED"},
                    )
            else:
                session.add(GuidePublication(scope=scope, revision=1, **values))
        if aggregate_replaced:
            keep = {AGGREGATE_SCOPE, *(_scope(profile["id"]) for profile in enabled)}
            session.query(GuidePublication).filter(~GuidePublication.scope.in_(keep)).delete(
                synchronize_session=False
            )
        session.commit()
    except IntegrityError:
        session.rollback()
        return _result(
            set(), retained | published, unavailable, documents, states,
            superseded=True, reasons=reasons | {"GUIDE_PUBLICATION_SUPERSEDED"},
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return _result(published, retained, unavailable, documents, states, reasons=reasons)


def update_observations(
    profile_id: int,
    observations,
    *,
    expected_revision: int,
) -> int | None:
    """Replace one profile's bounded observations with revision protection."""
    scope = _scope(profile_id)
    values = _observations(observations)
    session = get_session()
    try:
        row = session.query(GuidePublication).filter(GuidePublication.scope == scope).one_or_none()
        if row is None or row.revision != expected_revision:
            return None
        state = _read_row(row)["state"]
        stored = state["observations"]
        state["observations"] = {
            key: (
                value
                if not isinstance(stored.get(key), dict)
                or (
                    stored[key].get("provisional") is True
                    and value["provisional"] is False
                )
                else stored[key]
            )
            for key, value in values.items()
        }
        next_revision = expected_revision + 1
        updated = (
            session.query(GuidePublication)
            .filter(
                GuidePublication.scope == scope,
                GuidePublication.revision == expected_revision,
            )
            .update({
                "state": _state_text(state),
                "revision": next_revision,
            }, synchronize_session=False)
        )
        if updated != 1:
            session.rollback()
            return None
        session.commit()
        return next_revision
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def update_delivery(
    scope: str,
    *,
    expected_revision: int,
    required_dispatcharr_hashes: Mapping | None = None,
    confirmed_dispatcharr_hashes: Mapping | None = None,
    pending_emby: bool | None = None,
) -> int | None:
    """Update bounded delivery progress only when the publication still wins."""
    session = get_session()
    try:
        row = session.query(GuidePublication).filter(GuidePublication.scope == scope).one_or_none()
        if row is None or row.revision != expected_revision:
            return None
        state = _read_row(row)["state"]
        delivery = copy.deepcopy(state["delivery"])
        for name, value in (
            ("required_dispatcharr_hashes", required_dispatcharr_hashes),
            ("confirmed_dispatcharr_hashes", confirmed_dispatcharr_hashes),
        ):
            if value is not None:
                if not isinstance(value, Mapping) or len(value) > MAX_CHANNELS:
                    raise ValueError("Delivery hashes must be a bounded object.")
                hashes = {str(key): str(item) for key, item in value.items()}
                if any(re.fullmatch(r"[0-9a-f]{64}", item) is None for item in hashes.values()):
                    raise ValueError("Delivery hashes must be SHA-256 values.")
                delivery[name] = dict(sorted(hashes.items()))
        if pending_emby is not None:
            delivery["pending_emby"] = bool(pending_emby)
        required = delivery["required_dispatcharr_hashes"]
        confirmed = delivery["confirmed_dispatcharr_hashes"]
        if any(value != state["xmltv_hash"] for value in required.values()):
            raise ValueError("Required delivery hash must match the current publication.")
        if any(required.get(key) != value for key, value in confirmed.items()):
            raise ValueError("Confirmed delivery hashes must match required hashes.")
        state["delivery"] = delivery
        next_revision = expected_revision + 1
        updated = (
            session.query(GuidePublication)
            .filter(
                GuidePublication.scope == scope,
                GuidePublication.revision == expected_revision,
            )
            .update({
                "state": _state_text(state),
                "revision": next_revision,
            }, synchronize_session=False)
        )
        if updated != 1:
            session.rollback()
            return None
        session.commit()
        return next_revision
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
