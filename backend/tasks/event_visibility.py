"""Reconcile configured event profiles with their published guide and channels."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse

import pytz

from database import get_session
from dispatcharr_client import get_client
from models import ChannelPipelineRule, DummyEPGProfile
from task_registry import register_task
from task_scheduler import ScheduleConfig, ScheduleType, TaskResult, TaskScheduler

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 300
MATCH_STREAM_PAGE_SIZE = 500
MAX_MATCH_STREAMS = 10000
EPG_LINK_MAX_RESULTS = 10000


@asynccontextmanager
async def _owned_lock(lock):
    await lock.acquire()
    held = True

    def release() -> None:
        nonlocal held
        if held:
            lock.release()
            held = False

    try:
        yield release
    finally:
        release()


def _stream_id(stream) -> int | None:
    if isinstance(stream, dict):
        return stream.get("id")
    return getattr(stream, "stream_id", stream if isinstance(stream, int) else None)


def _stream_group_id(stream) -> int | None:
    if not isinstance(stream, dict):
        return getattr(stream, "group_id", None)
    group = stream.get("channel_group_id")
    if group is None:
        group = stream.get("channel_group")
    return group.get("id") if isinstance(group, dict) else group


def _stream_account_id(stream) -> int | None:
    if not isinstance(stream, dict):
        return getattr(stream, "provider_id", None)
    from stream_prober import extract_m3u_account_id

    return extract_m3u_account_id(stream.get("m3u_account"))


def _slot_key(
    name: str | None, config: dict, *, role: str = "channel",
) -> tuple[str, str] | None:
    """Keep the established helper name while using configured slot patterns."""
    from services.event_slots import _slot_key as configured_slot_key

    return configured_slot_key(name, config, role=role)


def _guide_name(current: dict | None, event_timezone: str) -> str | None:
    """Render one current guide row in the event matcher's default shape."""
    if not isinstance(current, dict):
        return None
    title = " ".join(
        str(current.get("title") or "")
        .replace("ᴸᶦᵛᵉ", "")
        .replace("ᴺᵉʷ", "")
        .split()
    )
    if not title:
        return None
    from services.epg_programmes import _placeholder

    import xml.etree.ElementTree as ET

    marker = ET.Element("programme")
    ET.SubElement(marker, "title").text = title
    if _placeholder(marker):
        return None
    try:
        start = datetime.fromisoformat(str(current["start"]).replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        local = start.astimezone(pytz.timezone(event_timezone))
    except (KeyError, TypeError, ValueError, pytz.UnknownTimeZoneError):
        return None
    clock = local.strftime("%I:%M %p").lstrip("0")
    return f"{title} @ {local.strftime('%b')} {local.day} {clock}"


def _scope_key(scope: dict) -> tuple[int, int | None]:
    return scope["group_id"], scope.get("m3u_account_id")


async def _fetch_match_streams(client, scopes: list[dict]):
    """Fetch each configured group/account scope once with bounded pagination."""
    from services.event_sync_resolver import SecondaryStream

    normalized = []
    seen_scopes = set()
    for scope in scopes:
        item = (
            {"group_id": scope, "m3u_account_id": None}
            if isinstance(scope, int) else dict(scope)
        )
        key = _scope_key(item)
        if key not in seen_scopes:
            normalized.append(item)
            seen_scopes.add(key)

    streams = []
    complete = set()
    failures = {}
    group_names = {}
    for scope in normalized:
        key = _scope_key(scope)
        try:
            scope_streams = []
            seen_ids = set()
            group_id, account_id = key
            if group_id not in group_names:
                group_names[group_id] = await client._channel_group_name_for_id(group_id)
            group_name = group_names[group_id]
            if not group_name:
                raise ValueError("Configured stream group no longer exists.")
            page = 1
            while True:
                response = await client.get_streams(
                    page=page,
                    page_size=MATCH_STREAM_PAGE_SIZE,
                    channel_group_name=group_name,
                    m3u_account=account_id,
                )
                rows = (
                    response.get("results", [])
                    if isinstance(response, dict) else (response or [])
                )
                for row in rows:
                    stream_id = row.get("id")
                    name = row.get("name")
                    if stream_id is None or not name:
                        continue
                    actual_account = _stream_account_id(row)
                    if account_id is not None and actual_account != account_id:
                        continue
                    if stream_id in seen_ids:
                        continue
                    seen_ids.add(stream_id)
                    scope_streams.append(SecondaryStream(
                        name=name,
                        group_id=group_id,
                        stream_id=stream_id,
                        provider_id=actual_account,
                        is_stale=row.get("is_stale"),
                    ))
                if len(scope_streams) > MAX_MATCH_STREAMS:
                    raise ValueError(
                        f"Guide-match stream scan exceeds {MAX_MATCH_STREAMS} streams."
                    )
                if not isinstance(response, dict) or not response.get("next"):
                    break
                page += 1
            streams.extend(scope_streams)
            complete.add(key)
        except Exception as exc:
            failures[key] = type(exc).__name__
            logger.warning(
                "[EVENT-WORKFLOW] Could not read stream scope group=%s account=%s: %s",
                key[0], key[1], exc,
            )
    return streams, complete, failures


def _generated_scope(source: dict) -> str | None:
    """Return the exact publication scope selected by one generated source URL."""
    try:
        path = urlparse(str(source.get("url") or "")).path.rstrip("/")
    except (TypeError, ValueError):
        return None
    if path == "/api/dummy-epg/xmltv":
        return "all"
    match = re.fullmatch(r"/api/dummy-epg/xmltv/([1-9]\d*)", path)
    return f"profile:{int(match.group(1))}" if match else None


def _url_hash(value) -> str:
    parsed = urlparse(str(value or ""))
    host = parsed.hostname or ""
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    plain = f"{parsed.scheme.lower()}://{host.lower()}{parsed.path}"
    return hashlib.sha256(plain.encode("utf-8")).hexdigest()


def _source_refresh_key(client, source: dict, scope: str) -> tuple[str, str, str]:
    endpoint_hash = _url_hash(getattr(client, "base_url", ""))
    source_url_hash = _url_hash(source.get("url"))
    value = json.dumps(
        {
            "endpoint_hash": endpoint_hash,
            "source_id": source["id"],
            "source_url_hash": source_url_hash,
            "scope": scope,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest(), endpoint_hash, source_url_hash


def _profile_token(profiles: list[dict]) -> str:
    from services.epg_publication import _config_hash

    value = json.dumps(
        [_config_hash(profile) for profile in profiles],
        sort_keys=True, separators=(",", ":"), default=str,
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _rule_token(rules: list) -> str:
    values = [
        {
            "id": getattr(rule, "id", None),
            "enabled": getattr(rule, "enabled", None),
            "event_sync_config": getattr(rule, "event_sync_config", None),
        }
        for rule in rules
    ]
    value = json.dumps(values, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _load_profiles() -> tuple[list[dict], list]:
    session = get_session()
    try:
        profiles = [
            row.to_dict()
            for row in session.query(DummyEPGProfile).filter(
                DummyEPGProfile.enabled == True  # noqa: E712
            ).all()
        ]
        rules = session.query(ChannelPipelineRule).filter(
            ChannelPipelineRule.enabled == True  # noqa: E712
        ).all()
        return profiles, rules
    finally:
        session.close()


def _source_rows(value) -> list[dict]:
    if isinstance(value, dict):
        return value.get("results", value.get("sources", [])) or []
    return value or []


def _stored_active(publication: dict | None, channel_id: int, now: datetime) -> bool:
    if publication is None:
        return False
    for channel in publication["state"].get("channels", []):
        if channel.get("channel_id") != channel_id:
            continue
        for event in channel.get("events", []):
            if _guide_name(event, "UTC") is None:
                continue
            try:
                start = datetime.fromisoformat(event["start"])
                stop = datetime.fromisoformat(event["stop"])
            except (KeyError, TypeError, ValueError):
                continue
            if start <= now < stop:
                return True
    return False


def _first_publication_link_matches(
    profile: dict,
    assignment: dict | None,
    channel: dict,
    generated_source_ids: set[int],
) -> bool:
    """Require exact generated-guide ownership before a first idle transition."""
    if assignment is None or not generated_source_ids:
        return False
    guide_row = channel.get("epg_data")
    if not isinstance(guide_row, dict):
        return False

    from dummy_epg_engine import get_xmltv_id
    from services.epg_programmes import _epg_source_id

    source_id = _epg_source_id(
        guide_row.get("epg_source") or guide_row.get("epg_source_id")
    )
    return (
        source_id in generated_source_ids
        and guide_row.get("tvg_id") == get_xmltv_id(assignment, channel, profile)
    )


def _matches_scope(stream, scope: dict) -> bool:
    if _stream_group_id(stream) != scope["group_id"]:
        return False
    account_id = scope.get("m3u_account_id")
    return account_id is None or _stream_account_id(stream) == account_id


def _ordered_ids(streams, scopes: list[dict]) -> list[int]:
    if not scopes:
        values = []
        for stream in streams:
            stream_id = _stream_id(stream)
            if stream_id is not None and stream_id not in values:
                values.append(stream_id)
        return values
    rank = {_scope_key(scope): index for index, scope in enumerate(scopes)}
    ordered = sorted(
        streams,
        key=lambda stream: (
            rank.get((_stream_group_id(stream), _stream_account_id(stream)),
                     rank.get((_stream_group_id(stream), None), len(rank))),
            _stream_id(stream) or 0,
        ),
    )
    values = []
    for stream in ordered:
        stream_id = _stream_id(stream)
        if stream_id is not None and stream_id not in values:
            values.append(stream_id)
    return values


def _patterns(profile: dict, config: dict) -> list[dict]:
    from services.event_sync_matcher import DEFAULT_EVENT_PATTERNS

    values = list(DEFAULT_EVENT_PATTERNS) if config.get("use_default_patterns") else []
    seen = {
        (row.get("title_pattern"), row.get("time_pattern"), row.get("date_pattern"))
        for row in values
    }
    for row in profile.get("pattern_variants") or []:
        if not isinstance(row, dict) or not row.get("title_pattern"):
            continue
        key = (row.get("title_pattern"), row.get("time_pattern"), row.get("date_pattern"))
        if key not in seen:
            values.append(row)
            seen.add(key)
    return values


def _plan_profile(
    profile: dict,
    config: dict,
    channel_map: dict,
    coverage: dict,
    streams: list,
    complete_scopes: set,
    retained: dict | None,
    now: datetime,
    generated_source_ids: set[int] | None = None,
) -> dict:
    """Prepare profile-local event evidence without external mutations."""
    from services.event_sync_matcher import (
        SYNTHESIZED_DATE_PATTERN_NAMES,
        parse_event_name,
    )
    from services.event_slots import classify_event_slot
    from services.event_sync_resolver import (
        DISPOSITION_AMBIGUOUS,
        DISPOSITION_WOULD_ATTACH,
        resolve_event_sync,
    )

    profile_id = profile["id"]
    generated_source_ids = generated_source_ids or set()
    scopes = config.get("secondary") or []
    scope_keys = {_scope_key(scope) for scope in scopes}
    scan_complete = scope_keys <= complete_scopes
    selected_streams = [
        stream for stream in streams if any(_matches_scope(stream, scope) for scope in scopes)
    ]
    profile_coverage = coverage.get("profiles", {}).get(str(profile_id), {})
    publication_complete = profile_coverage.get("can_publish") is True
    rows = {
        row["channel_id"]: row
        for row in coverage.get("channels", [])
        if row.get("profile_id") in {None, profile_id}
    }
    targets = set(profile.get("hide_empty_group_ids") or [])
    channels = {
        channel_id: channel
        for channel_id, channel in channel_map.items()
        if channel.get("channel_group_id") in targets
    }
    match_patterns = _patterns(profile, config)
    event_timezone = profile.get("event_timezone") or "US/Eastern"
    duration = timedelta(minutes=profile.get("program_duration") or 180)
    observations = []
    intervals = {}
    stored_intervals = profile.get("event_intervals") or {}
    if isinstance(stored_intervals, dict):
        for channel_id, values in stored_intervals.items():
            try:
                parsed_channel_id = int(channel_id)
            except (TypeError, ValueError):
                continue
            if isinstance(values, list):
                real_events = [
                    copy.deepcopy(event)
                    for event in values
                    if _guide_name(event, event_timezone) is not None
                ]
                if real_events:
                    intervals[parsed_channel_id] = real_events
    if retained is not None:
        for item in retained["state"].get("channels", []):
            retained_events = [
                copy.deepcopy(event)
                for event in item.get("events", [])
                if _guide_name(event, event_timezone) is not None
                and datetime.fromisoformat(event["stop"]) > now
            ]
            if retained_events:
                intervals[item["channel_id"]] = retained_events
    desired = {}
    primary_ids = {}
    event_starts = {}
    states = {}
    ambiguous_ids = set()
    assignments = {
        assignment["channel_id"]: assignment
        for assignment in profile.get("channel_assignments") or []
        if assignment.get("channel_id") is not None
    }

    direct_by_slot = {}
    fallback_by_slot = {}
    conflicting_event_slots = False
    for stream in selected_streams:
        if stream.is_stale is True:
            continue
        classification = classify_event_slot(stream.name, config, role="event")
        if classification["validation_issues"]:
            conflicting_event_slots = True
            continue
        event_slot = _slot_key(stream.name, config, role="event")
        fallback_slot = _slot_key(stream.name, config, role="fallback")
        if event_slot is not None:
            direct_by_slot.setdefault(event_slot, []).append(stream)
        elif fallback_slot is not None:
            fallback_by_slot.setdefault(fallback_slot, []).append(stream)

    titled_by_channel = {}
    if scan_complete:
        names = {}
        for channel_id, channel in channels.items():
            guide_name = _guide_name((rows.get(channel_id) or {}).get("current"), event_timezone)
            if guide_name:
                names.setdefault(guide_name, []).append(channel_id)
        titled = [
            stream for stream in selected_streams
            if stream.is_stale is not True
            and _slot_key(stream.name, config, role="fallback") is None
            and _slot_key(stream.name, config, role="event") is None
        ]
        if names and titled:
            resolver_config = {
                "master_group_id": 0,
                "secondary_group_ids": list(dict.fromkeys(scope["group_id"] for scope in scopes)),
                "time_window_minutes": config["time_window_minutes"],
                "enforce_time_window": config["enforce_time_window"],
                "attach_threshold": config["attach_threshold"],
                "assume_current_date": config["assume_current_date"],
                "demote_stale_dateless": config["demote_stale_dateless"],
                "patterns": match_patterns or None,
            }
            resolution = resolve_event_sync(
                resolver_config,
                sorted(names),
                titled,
                now=now,
                event_timezone=event_timezone,
            )
            for resolved in resolution.resolved:
                if resolved.disposition == DISPOSITION_WOULD_ATTACH:
                    for channel_id in names.get(resolved.best.master_name, []):
                        titled_by_channel.setdefault(channel_id, []).append(resolved.stream)
                elif resolved.disposition == DISPOSITION_AMBIGUOUS:
                    for candidate in resolved.result.candidates:
                        ambiguous_ids.update(names.get(candidate.master_name, []))

    for channel_id, channel in channels.items():
        slot = _slot_key(channel.get("name"), config, role="channel")
        direct = list(direct_by_slot.get(slot, [])) if slot is not None else []
        current = (rows.get(channel_id) or {}).get("current")
        if _guide_name(current, event_timezone) is None:
            current = None
        bootstrap = next((
            item.get("bootstrap") is True
            for item in config.get("slot_patterns", [])
            if slot is not None and item.get("name") == slot[0]
        ), False)
        parsed_direct = []
        for stream in direct if current is not None or bootstrap else []:
            parsed = parse_event_name(
                stream.name,
                match_patterns or None,
                event_timezone=event_timezone,
                now=now,
                assume_current_date=config["assume_current_date"],
            )
            if parsed.start is None:
                continue
            stop = parsed.start + duration
            if not parsed.start <= now < stop:
                continue
            parsed_direct.append((stream, parsed, stop))
        active_direct = []
        if parsed_direct:
            latest_start = max(item[1].start for item in parsed_direct)
            latest = [item for item in parsed_direct if item[1].start == latest_start]
            identities = {
                " ".join((item[1].title or item[0].name).casefold().split())
                for item in latest
            }
            if len(identities) == 1:
                active_direct = [item[0] for item in latest]
            else:
                ambiguous_ids.add(channel_id)
        if active_direct:
            selected = next(item for item in parsed_direct if item[0] is active_direct[0])
            title = selected[1].title or selected[0].name
            interval = {
                "channel_id": channel_id,
                "title": title,
                "start": selected[1].start.isoformat(),
                "stop": selected[2].isoformat(),
            }
            intervals[channel_id] = [interval]
        for stream, parsed, stop in parsed_direct:
            if stream not in active_direct:
                continue
            title = parsed.title or stream.name
            observations.append({
                "family": slot[0],
                "slot": slot[1],
                "stream_id": stream.stream_id,
                "normalized_name": " ".join(stream.name.split()).casefold(),
                "start": parsed.start.isoformat(),
                "expires_at": stop.isoformat(),
                "title": title,
                "matched_variant": parsed.matched_pattern,
                "provisional": parsed.matched_pattern in SYNTHESIZED_DATE_PATTERN_NAMES,
            })

        interval_active = False
        for interval in intervals.get(channel_id, []):
            try:
                interval_start = datetime.fromisoformat(
                    str(interval["start"]).replace("Z", "+00:00")
                )
                interval_stop = datetime.fromisoformat(
                    str(interval["stop"]).replace("Z", "+00:00")
                )
                if interval_start.tzinfo is None:
                    interval_start = interval_start.replace(tzinfo=timezone.utc)
                if interval_stop.tzinfo is None:
                    interval_stop = interval_stop.replace(tzinfo=timezone.utc)
            except (KeyError, TypeError, ValueError):
                continue
            if interval_start <= now < interval_stop:
                interval_active = True
                break
        active = (
            current is not None
            or bool(active_direct)
            or interval_active
            or _stored_active(retained, channel_id, now)
        )
        if channel_id in ambiguous_ids or conflicting_event_slots or not scan_complete:
            state = "unknown"
        elif active:
            state = "active"
        elif publication_complete and (
            retained is not None
            or _first_publication_link_matches(
                profile,
                assignments.get(channel_id),
                channel,
                generated_source_ids,
            )
        ):
            state = "idle"
        else:
            state = "unknown"
        states[channel_id] = state

        attached = channel.get("streams") or []
        outside = [
            stream for stream in attached
            if not any(_matches_scope(stream, scope) for scope in scopes)
        ]
        fallback = fallback_by_slot.get(slot, []) if slot is not None else []
        titled_matches = [*active_direct, *titled_by_channel.get(channel_id, [])]
        if state == "active":
            primary_ids[channel_id] = _ordered_ids(titled_matches, scopes)
            desired[channel_id] = list(primary_ids[channel_id])
            current_start = None
            if isinstance(current, dict):
                try:
                    current_start = datetime.fromisoformat(
                        str(current["start"]).replace("Z", "+00:00")
                    )
                    if current_start.tzinfo is None:
                        current_start = current_start.replace(tzinfo=timezone.utc)
                except (KeyError, TypeError, ValueError):
                    current_start = None
            if current_start is None and intervals.get(channel_id):
                try:
                    current_start = datetime.fromisoformat(
                        intervals[channel_id][0]["start"]
                    )
                except (KeyError, TypeError, ValueError):
                    current_start = None
            if current_start is not None:
                for stream_id in primary_ids[channel_id]:
                    event_starts[stream_id] = current_start
        elif state == "idle":
            desired[channel_id] = []
        else:
            continue
        for stream_id in [*_ordered_ids(outside, []), *_ordered_ids(fallback, scopes)]:
            if stream_id not in desired[channel_id]:
                desired[channel_id].append(stream_id)

    profile["event_intervals"] = intervals
    return {
        "profile": profile,
        "observations": observations if scan_complete else None,
        "states": states,
        "desired": desired,
        "primary_ids": primary_ids,
        "event_starts": event_starts,
        "scan_complete": scan_complete,
    }


def _result_details(profile_count: int) -> dict:
    return {
        "configured_profile_count": profile_count,
        "published_profile_ids": [],
        "retained_profile_ids": [],
        "unavailable_profile_ids": [],
        "publication_times": {},
        "source_reason_codes": {},
        "mapping_checks": {},
        "idle_channel_count": 0,
        "active_channel_count": 0,
        "unknown_channel_count": 0,
        "stream_updated_channel_ids": [],
        "epg_linked_channel_ids": [],
        "revealed_channel_ids": [],
        "hidden_channel_ids": [],
        "pending_source_hashes": {},
        "emby_request_outcome": "not_required",
        "pending_emby": False,
        "delivery_pending": False,
        "reason_codes": [],
    }


def _delivery_plan(publication: dict, sources: list[dict]) -> tuple[dict, dict, dict]:
    document_hash = publication["state"]["xmltv_hash"]
    required = {str(source["id"]): document_hash for source in sources}
    confirmed = {
        key: value
        for key, value in publication["state"]["delivery"]["confirmed_dispatcharr_hashes"].items()
        if required.get(key) == value
    }
    pending = {
        int(source_id): document_hash
        for source_id in required
        if confirmed.get(source_id) != document_hash
    }
    return required, confirmed, pending


async def _await_preparation(awaitable, cancelled):
    """Stop waiting promptly without cancelling shared source-load tasks."""
    preparation = asyncio.create_task(awaitable)
    while not preparation.done():
        if cancelled():
            preparation.cancel()
            try:
                await preparation
            except asyncio.CancelledError:
                pass
            return None
        await asyncio.wait({preparation}, timeout=0.05)
    if cancelled():
        return None
    return preparation.result()


def _store_emby_pending(
    publications: dict,
    *,
    pending: bool,
    read_publication,
    update_delivery,
) -> bool:
    """Persist one Emby delivery decision across every published scope."""
    for scope, row in list(publications.items()):
        if row["state"]["delivery"].get("pending_emby") == pending:
            continue
        next_revision = update_delivery(
            scope,
            expected_revision=row["revision"],
            pending_emby=pending,
        )
        if next_revision is None:
            return False
        current = read_publication(scope)
        if current is None:
            return False
        publications[scope] = current
    return True


def _finish(
    started_at: datetime,
    details: dict,
    *,
    success: bool,
    message: str,
    error: str | None = None,
    degraded: bool = False,
) -> TaskResult:
    changed = set(details["stream_updated_channel_ids"])
    changed.update(details["epg_linked_channel_ids"])
    changed.update(details["revealed_channel_ids"])
    changed.update(details["hidden_channel_ids"])
    total = (
        details["idle_channel_count"]
        + details["active_channel_count"]
        + details["unknown_channel_count"]
    )
    return TaskResult(
        success=success,
        completed_degraded=degraded,
        message=message,
        error=error,
        started_at=started_at,
        completed_at=datetime.utcnow(),
        total_items=total,
        success_count=len(changed),
        failed_count=len(details["unavailable_profile_ids"]),
        skipped_count=max(0, total - len(changed)),
        details=details,
    )


def _finish_cancelled(
    started_at: datetime,
    details: dict,
    publications: dict | None = None,
) -> TaskResult:
    if publications:
        details["pending_emby"] = any(
            row["state"]["delivery"].get("pending_emby")
            for row in publications.values()
        )
    details["delivery_pending"] = bool(
        details["delivery_pending"]
        or details["pending_source_hashes"]
        or details["pending_emby"]
    )
    details["reason_codes"] = ["CANCELLED"]
    return _finish(
        started_at,
        details,
        success=False,
        message="Guide reconciliation cancelled",
        error="CANCELLED",
        degraded=bool(
            details["published_profile_ids"]
            or details["retained_profile_ids"]
            or details["hidden_channel_ids"]
            or details["stream_updated_channel_ids"]
            or details["epg_linked_channel_ids"]
            or details["revealed_channel_ids"]
        ),
    )


async def reconcile_profiles(task: TaskScheduler, *, wait_for_sources: bool) -> TaskResult:
    """Publish and deliver every configured event profile through one workflow."""
    from cache import get_cache
    from concurrency import run_cpu_bound
    from dummy_epg_engine import get_xmltv_id
    from services.epg_programmes import (
        _epg_source_id,
        _fetch_all_channels,
        _profile_owners,
        _resolve_group_assignments,
        prepare_profiles,
    )
    from services.epg_publication import (
        begin_delivery,
        publication_lock,
        publish_profiles,
        read_publication,
        update_delivery,
    )
    from services.event_slots import event_config, validate_ownership

    started_at = datetime.utcnow()
    now = datetime.now(timezone.utc)
    client = get_client()

    for attempt in range(2):
        details = _result_details(0)
        stage = "profiles"
        try:
            profiles, rules = _load_profiles()
            details = _result_details(len(profiles))
            if not profiles:
                return _finish(
                    started_at, details, success=True,
                    message="No enabled guide profiles require reconciliation",
                )
            token = (_profile_token(profiles), _rule_token(rules))
            stage = "ownership"
            conflicts = validate_ownership(profiles, rules)
            disputed_groups = {row["group_id"] for row in conflicts}
            stage = "publications"
            retained = {}
            admitted_by_id = {}
            for profile in profiles:
                scope = f"profile:{profile['id']}"
                row = read_publication(scope)
                async with publication_lock:
                    admitted = begin_delivery(
                        scope,
                        expected_revision=row["revision"] if row is not None else 0,
                        expected_hash=(
                            row["state"]["xmltv_hash"] if row is not None else None
                        ),
                        profile=profile,
                        now=now,
                    )
                if admitted is not None:
                    row = admitted
                    admitted_by_id[profile["id"]] = admitted
                retained[profile["id"]] = row

            if task._cancel_requested:
                return _finish_cancelled(started_at, details)
            stage = "channels"
            channel_map = await _fetch_all_channels(client)
            if task._cancel_requested:
                return _finish_cancelled(started_at, details)
            stage = "programmes"
            prepared_by_id = {}
            summary = {
                "generated_at": now.isoformat(),
                "window_start": None,
                "window_stop": None,
                "sources": [],
                "channels": [],
                "profiles": {},
            }
            for profile in profiles:
                profile_id = profile["id"]
                admitted = admitted_by_id.get(profile_id)
                if admitted is None:
                    continue
                guide_attempt = admitted["state"]["delivery"]["guide_attempt"]
                profile_expiry = datetime.fromisoformat(guide_attempt["expires_at"])
                if profile_expiry <= now:
                    continue
                selected = copy.deepcopy(profile)
                if not selected.get("epg_source_ids"):
                    intervals = {}
                    for receipt in admitted["state"]["delivery"]["pending_channels"].values():
                        channel_id = receipt.get("channel_id")
                        if (
                            receipt.get("stage") in {
                                "complete", "failed", "expired", "allocation_unknown",
                            }
                            or receipt.get("guide_attempt_id") != guide_attempt["attempt_id"]
                            or channel_id not in channel_map
                            or datetime.fromisoformat(receipt["expires_at"]) <= now
                        ):
                            continue
                        channel = channel_map[channel_id]
                        group = channel.get("channel_group_id") or channel.get("channel_group")
                        if isinstance(group, dict):
                            group = group.get("id")
                        if (
                            group != receipt["target_group_id"]
                            or (
                                receipt.get("channel_uuid") is not None
                                and channel.get("uuid") != receipt["channel_uuid"]
                            )
                        ):
                            continue
                        intervals.setdefault(channel_id, []).append({
                            "start": receipt["start"],
                            "stop": receipt["stop"],
                            "title": receipt["title"],
                        })
                    if intervals:
                        selected["event_intervals"] = intervals
                preparation = await _await_preparation(
                    prepare_profiles(
                        [selected],
                        channel_map,
                        client,
                        expires_at=profile_expiry,
                        now=now,
                        wait_for_sources=wait_for_sources,
                        recover_sources=True,
                    ),
                    lambda: task._cancel_requested,
                )
                if preparation is None:
                    return _finish_cancelled(started_at, details)
                prepared, coverage = preparation
                if prepared:
                    prepared_profile = next(
                        (item for item in prepared if item.get("id") == profile_id),
                        None,
                    )
                    if prepared_profile is not None:
                        prepared_by_id[profile_id] = prepared_profile
                summary["profiles"].update(coverage.get("profiles") or {})
                summary["sources"].extend(coverage.get("sources") or [])
                summary["channels"].extend(coverage.get("channels") or [])
                for name, choose in (("window_start", min), ("window_stop", max)):
                    value = coverage.get(name)
                    if value is not None:
                        summary[name] = (
                            value if summary[name] is None
                            else choose(summary[name], value)
                        )
                if coverage.get("artwork_pending"):
                    summary["artwork_pending"] = True
            coverage = summary
            prepared = []
            for profile in profiles:
                profile_id = profile["id"]
                current = prepared_by_id.get(profile_id)
                if current is None:
                    current = copy.deepcopy(profile)
                    groups = current.get("channel_group_ids") or []
                    if groups:
                        current["channel_assignments"] = _resolve_group_assignments(
                            groups, channel_map,
                        )
                    assignments = current.get("channel_assignments") or []
                    coverage["profiles"].setdefault(str(profile_id), {
                        "profile_id": profile_id,
                        "source_ids": [],
                        "sources": [],
                        "owned_channel_ids": sorted({
                            item["channel_id"] for item in assignments
                            if item.get("channel_id") in channel_map
                        }),
                        "can_publish": False,
                        "reason_codes": ["GUIDE_UNAVAILABLE"],
                    })
                prepared.append(current)
            _profile_owners(prepared, channel_map, coverage)
            publication_expectations = {
                admitted["scope"]: {
                    "revision": admitted["revision"],
                    "xmltv_hash": admitted["state"]["xmltv_hash"],
                    "config_hash": admitted["state"]["config_hash"],
                    "attempt_id": admitted["state"]["delivery"]["guide_attempt"]["attempt_id"],
                }
                for admitted in admitted_by_id.values()
            }
            if task._cancel_requested:
                return _finish_cancelled(started_at, details)
            stage = "slots"
            configs = {}
            scopes = []
            invalid_profiles = set()
            for profile in prepared:
                profile_id = profile["id"]
                if profile_id not in admitted_by_id:
                    invalid_profiles.add(profile_id)
                    continue
                try:
                    config = event_config(profile)
                except ValueError:
                    invalid_profiles.add(profile_id)
                    continue
                configs[profile_id] = config
                scopes.extend(config.get("secondary") or [])
            if task._cancel_requested:
                return _finish_cancelled(started_at, details)
            stage = "streams"
            streams, complete_scopes, scope_failures = await _fetch_match_streams(client, scopes)
            if task._cancel_requested:
                return _finish_cancelled(started_at, details)
            stage = "sources"
            source_rows = _source_rows(await client.get_epg_sources())
            if task._cancel_requested:
                return _finish_cancelled(started_at, details)

            stage = "plans"
            plans = {}
            observations = {}
            for profile in prepared:
                profile_id = profile["id"]
                record = coverage.get("profiles", {}).setdefault(str(profile_id), {
                    "profile_id": profile_id,
                    "can_publish": False,
                    "reason_codes": [],
                })
                reasons = set(record.get("reason_codes") or [])
                if profile_id in invalid_profiles:
                    reasons.add("GUIDE_CONFIG_INVALID")
                    record["can_publish"] = False
                target_groups = set(profile.get("hide_empty_group_ids") or [])
                if target_groups & disputed_groups:
                    reasons.add("GUIDE_OWNERSHIP_CONFLICT")
                    record["can_publish"] = False
                record["reason_codes"] = sorted(reasons)
                if profile_id not in configs:
                    continue
                plan = _plan_profile(
                    profile,
                    configs[profile_id],
                    channel_map,
                    coverage,
                    streams,
                    complete_scopes,
                    retained.get(profile_id),
                    now,
                    {
                        source["id"]
                        for source in source_rows
                        if isinstance(source, dict)
                        and source.get("id") is not None
                        and source.get("is_active", True)
                        and _generated_scope(source) in {"all", f"profile:{profile_id}"}
                    },
                )
                if target_groups & disputed_groups:
                    plan["states"] = {
                        channel_id: "unknown" for channel_id in plan["states"]
                    }
                    plan["desired"] = {}
                if not plan["scan_complete"]:
                    record["can_publish"] = False
                    record["reason_codes"] = sorted(
                        set(record.get("reason_codes") or []) | {"GUIDE_SOURCES_PENDING"}
                    )
                plans[profile_id] = plan
                if plan["observations"] is not None:
                    observations[profile_id] = plan["observations"]
            for profile in prepared:
                profile_id = profile["id"]
                profile_coverage = coverage.get("profiles", {}).get(str(profile_id), {})
                details["source_reason_codes"][str(profile_id)] = list(profile_coverage.get("reason_codes") or [])
                if "mapping_checks" in profile_coverage:
                    details["mapping_checks"][str(profile_id)] = copy.deepcopy(profile_coverage["mapping_checks"])
            for key in scope_failures:
                for profile in prepared:
                    if key in {_scope_key(scope) for scope in configs.get(profile["id"], {}).get("secondary", [])}:
                        details["source_reason_codes"][str(profile["id"])] = sorted(
                            set(details["source_reason_codes"][str(profile["id"])]) | {"GUIDE_SOURCES_PENDING"}
                        )
            health_admitted_at = datetime.now(timezone.utc)
            flow = {}
            if plans:
                from services.event_sync_stream_health import collect_stream_flow

            for profile_id, plan in plans.items():
                health_ids = {
                    stream_id
                    for values in plan["primary_ids"].values()
                    for stream_id in values
                }
                if not health_ids:
                    continue
                admitted = admitted_by_id[profile_id]
                attempt = admitted["state"]["delivery"]["guide_attempt"]
                health_expires_at = min(
                    datetime.fromisoformat(attempt["expires_at"]),
                    health_admitted_at + timedelta(seconds=60),
                )
                if health_expires_at <= health_admitted_at:
                    continue
                stream_names = {
                    stream.stream_id: stream.name
                    for stream in streams
                    if stream.stream_id in health_ids
                }
                event_start_by_stream = {
                    stream_id: event_start
                    for stream_id, event_start in plan["event_starts"].items()
                    if stream_id in stream_names
                }
                profile_flow = await collect_stream_flow(
                    sorted(health_ids),
                    client=client,
                    checked_after=health_admitted_at - timedelta(minutes=5),
                    event_start_by_stream=event_start_by_stream,
                    stream_names=stream_names,
                    expires_at=health_expires_at,
                    probe_missing=True,
                    probe_while_busy=True,
                    cancelled=lambda: task._cancel_requested,
                )
                flow.update(profile_flow)
            health_states = {}
            for plan in plans.values():
                for channel_id, primary in plan["primary_ids"].items():
                    values = [flow.get(stream_id) for stream_id in primary]
                    if any(value is True for value in values):
                        health_states[channel_id] = True
                    elif values and all(value is False for value in values):
                        health_states[channel_id] = False
                    else:
                        health_states[channel_id] = None
                    desired_ids = plan["desired"].get(channel_id, [])
                    primary_set = set(primary)
                    attached = {
                        stream_id
                        for stream_id in (
                            _stream_id(item)
                            for item in channel_map[channel_id].get("streams") or []
                        )
                        if stream_id is not None
                    }
                    safe_primary = [
                        stream_id for stream_id in primary
                        if flow.get(stream_id) is True
                        or (flow.get(stream_id) is None and stream_id in attached)
                    ]
                    plan["desired"][channel_id] = [
                        *safe_primary,
                        *(
                            stream_id for stream_id in desired_ids
                            if stream_id not in primary_set and stream_id not in safe_primary
                        ),
                    ]
        except Exception as exc:
            logger.exception("[EVENT-WORKFLOW] Could not prepare reconciliation: %s", exc)
            details["reason_codes"] = ["GUIDE_UNAVAILABLE"]
            details["failure_stage"] = stage
            details["failure_type"] = type(exc).__name__
            return _finish(
                started_at, details, success=False,
                message="Guide reconciliation could not prepare complete input",
                error="GUIDE_UNAVAILABLE",
            )

        if task._cancel_requested:
            return _finish_cancelled(started_at, details)

        async with _owned_lock(publication_lock) as release_publication:
            if task._cancel_requested:
                return _finish_cancelled(started_at, details)
            current_profiles, current_rules = _load_profiles()
            if (_profile_token(current_profiles), _rule_token(current_rules)) != token:
                if attempt == 0:
                    continue
                details["reason_codes"] = ["GUIDE_SOURCES_PENDING"]
                return _finish(
                    started_at, details, success=False,
                    message="Guide profile configuration changed during reconciliation",
                    error="GUIDE_SOURCES_PENDING",
                    degraded=any(retained.values()),
                )

            current_time = datetime.now(timezone.utc)
            for profile_id, admitted in admitted_by_id.items():
                attempt = admitted["state"]["delivery"]["guide_attempt"]
                if current_time >= datetime.fromisoformat(attempt["expires_at"]):
                    record = coverage["profiles"][str(profile_id)]
                    record["can_publish"] = False
                    record["reason_codes"] = sorted(
                        set(record.get("reason_codes") or []) | {"GUIDE_UNAVAILABLE"}
                    )

            states = {
                channel_id: state
                for plan in plans.values()
                for channel_id, state in plan["states"].items()
            }
            details["idle_channel_count"] = sum(value == "idle" for value in states.values())
            details["active_channel_count"] = sum(value == "active" for value in states.values())
            details["unknown_channel_count"] = sum(value == "unknown" for value in states.values())

            if task._cancel_requested:
                return _finish_cancelled(started_at, details)
            try:
                publication = await run_cpu_bound(
                    publish_profiles,
                    prepared,
                    channel_map,
                    coverage,
                    observations=observations,
                    now=now,
                    expected=publication_expectations,
                )
            except Exception as exc:
                logger.exception("[EVENT-WORKFLOW] Could not commit publication: %s", exc)
                details["reason_codes"] = ["GUIDE_PUBLICATION_FAILED"]
                return _finish(
                    started_at, details, success=False,
                    message="Guide publication failed",
                    error="GUIDE_PUBLICATION_FAILED",
                )

            details["published_profile_ids"] = list(publication.published_profile_ids)
            details["retained_profile_ids"] = list(publication.retained_profile_ids)
            details["unavailable_profile_ids"] = list(publication.unavailable_profile_ids)
            if publication.superseded:
                details["reason_codes"] = sorted(set(publication.reason_codes) | {"GUIDE_IMPORT_PENDING"})
                details["delivery_pending"] = True
                return _finish(
                    started_at, details, success=False,
                    message="A newer guide publication superseded this run",
                    error="GUIDE_IMPORT_PENDING",
                    degraded=bool(publication.xmltv_by_scope),
                )

            publications = {}
            for scope in publication.xmltv_by_scope:
                row = read_publication(scope)
                if row is not None:
                    if scope.startswith("profile:"):
                        profile_id = int(scope.split(":", 1)[1])
                        profile = next(
                            item for item in prepared if item["id"] == profile_id
                        )
                        admitted = begin_delivery(
                            scope,
                            expected_revision=row["revision"],
                            expected_hash=row["state"]["xmltv_hash"],
                            profile=profile,
                            now=now,
                        )
                        if admitted is None:
                            details["reason_codes"] = ["GUIDE_IMPORT_PENDING"]
                            details["delivery_pending"] = True
                            return _finish(
                                started_at,
                                details,
                                success=False,
                                message="Guide delivery admission changed during reconciliation",
                                error="GUIDE_IMPORT_PENDING",
                                degraded=True,
                            )
                        row = admitted
                    publications[scope] = row
                    if scope.startswith("profile:"):
                        details["publication_times"][scope.split(":", 1)[1]] = row["state"]["published_at"]

            if task._cancel_requested:
                return _finish_cancelled(started_at, details, publications)

            cache = get_cache()
            cache.invalidate_prefix("dummy_epg_xmltv")
            for scope, document in publication.xmltv_by_scope.items():
                key = "dummy_epg_xmltv_all" if scope == "all" else f"dummy_epg_xmltv_{scope.split(':', 1)[1]}"
                cache.set(key, document)

            generated_sources = {}
            for source in source_rows:
                if not isinstance(source, dict) or source.get("id") is None or not source.get("is_active", True):
                    continue
                scope = _generated_scope(source)
                if scope in publications:
                    generated_sources.setdefault(scope, []).append(source)

            pending = {}
            confirmed_sources = set()
            for scope, row in sorted(publications.items()):
                required, confirmed, scope_pending = _delivery_plan(
                    row, generated_sources.get(scope, []),
                )
                delivery = row["state"]["delivery"]
                source_refreshes = copy.deepcopy(delivery["source_refreshes"])
                attempt = delivery["guide_attempt"]
                if scope.startswith("profile:") and attempt is not None:
                    for source in generated_sources.get(scope, []):
                        if source["id"] not in scope_pending:
                            continue
                        source_key, endpoint_hash, source_url_hash = _source_refresh_key(
                            client, source, scope,
                        )
                        if source_key not in source_refreshes:
                            status = str(source.get("status") or "").strip().lower()
                            source_refreshes[source_key] = {
                                "source_id": source["id"],
                                "endpoint_hash": endpoint_hash,
                                "source_url_hash": source_url_hash,
                                "expected_hash": row["state"]["xmltv_hash"],
                                "initial_updated": source.get("updated_at") or source.get("last_updated"),
                                "observed_running": status in {
                                    "fetching", "processing", "parsing", "loading", "pending",
                                    "running", "queued", "refreshing",
                                },
                                "triggered": False,
                                "expires_at": attempt["expires_at"],
                                "attempt_id": attempt["attempt_id"],
                            }
                if (
                    delivery["required_dispatcharr_hashes"] != required
                    or delivery["confirmed_dispatcharr_hashes"] != confirmed
                    or delivery["source_refreshes"] != source_refreshes
                ):
                    next_revision = update_delivery(
                        scope,
                        expected_revision=row["revision"],
                        expected_hash=row["state"]["xmltv_hash"],
                        expected_config_hash=row["state"]["config_hash"],
                        expected_attempt_id=(attempt or {}).get("attempt_id"),
                        required_dispatcharr_hashes=required,
                        confirmed_dispatcharr_hashes=confirmed,
                        source_refreshes=(source_refreshes if attempt is not None else None),
                    )
                    if next_revision is None:
                        details["reason_codes"] = ["GUIDE_IMPORT_PENDING"]
                        details["delivery_pending"] = True
                        return _finish(
                            started_at, details, success=False,
                            message="Guide delivery state changed during reconciliation",
                            error="GUIDE_IMPORT_PENDING",
                            degraded=True,
                        )
                    row = read_publication(scope)
                    publications[scope] = row
                confirmed_sources.update(
                    (scope, int(source_id)) for source_id in confirmed
                )
                pending.update({
                    (scope, source_id): document_hash
                    for source_id, document_hash in scope_pending.items()
                })

            details["pending_source_hashes"] = {
                f"{scope}:{source_id}": document_hash
                for (scope, source_id), document_hash in sorted(pending.items())
                if (scope, source_id) not in confirmed_sources
            }
            if task._cancel_requested:
                return _finish_cancelled(started_at, details, publications)

            release_publication()

            owners = {
                channel_id: profile_id
                for profile_id, plan in plans.items()
                for channel_id in plan["states"]
            }

            def claim_current(channel_id: int) -> bool:
                current_profiles, current_rules = _load_profiles()
                if (_profile_token(current_profiles), _rule_token(current_rules)) != token:
                    return False
                profile_id = owners.get(channel_id)
                if profile_id is None:
                    return False
                scope = f"profile:{profile_id}"
                expected = publications.get(scope)
                current = read_publication(scope)
                if expected is None or current is None:
                    return False
                if (
                    current["revision"] != expected["revision"]
                    or current["state"]["xmltv_hash"] != expected["state"]["xmltv_hash"]
                    or current["state"]["config_hash"] != expected["state"]["config_hash"]
                    or current["state"].get("published", True) is not True
                ):
                    return False
                expected_attempt = expected["state"]["delivery"]["guide_attempt"]
                current_attempt = current["state"]["delivery"]["guide_attempt"]
                if (
                    expected_attempt is None
                    or current_attempt is None
                    or current_attempt["attempt_id"] != expected_attempt["attempt_id"]
                    or datetime.now(timezone.utc) >= datetime.fromisoformat(
                        current_attempt["expires_at"]
                    )
                ):
                    return False
                if any(
                    receipt.get("channel_id") == channel_id
                    and receipt.get("stage") != "complete"
                    for receipt in current["state"]["delivery"]["pending_channels"].values()
                ):
                    return False
                return True

            async def current_channel(channel_id: int) -> dict | None:
                if not claim_current(channel_id):
                    return None
                profile_id = owners[channel_id]
                fresh = await client.get_channel(channel_id)
                group = fresh.get("channel_group_id") or fresh.get("channel_group")
                if isinstance(group, dict):
                    group = group.get("id")
                target_groups = set(
                    plans[profile_id]["profile"].get("hide_empty_group_ids") or []
                )
                if group not in target_groups:
                    return None
                return fresh

            async def update_current_channel(channel_id: int, update: dict) -> bool:
                async with publication_lock:
                    if not claim_current(channel_id):
                        return False
                    await client.update_channel(channel_id, update)
                    return True

            for channel_id, state in sorted(states.items()):
                channel = channel_map[channel_id]
                should_hide = state == "idle" or (
                    state == "active"
                    and (
                        not channel.get("streams")
                        or health_states.get(channel_id) is False
                    )
                )
                if not should_hide or channel.get("hidden_from_output"):
                    continue
                if task._cancel_requested:
                    return _finish_cancelled(started_at, details, publications)
                fresh = await current_channel(channel_id)
                if fresh is None or fresh.get("hidden_from_output"):
                    continue
                try:
                    if not await update_current_channel(
                        channel_id, {"hidden_from_output": True},
                    ):
                        continue
                except Exception:
                    logger.exception("[EVENT-WORKFLOW] Could not hide idle channel %s", channel_id)
                    continue
                channel["hidden_from_output"] = True
                details["hidden_channel_ids"].append(channel_id)
                try:
                    stored_pending = _store_emby_pending(
                        publications,
                        pending=True,
                        read_publication=read_publication,
                        update_delivery=update_delivery,
                    )
                except Exception:
                    logger.exception("[EVENT-WORKFLOW] Could not persist Emby delivery state")
                    stored_pending = False
                if not stored_pending:
                    details["pending_emby"] = True
                    details["delivery_pending"] = True
                    details["reason_codes"] = ["GUIDE_EMBY_PENDING"]
                    return _finish(
                        started_at,
                        details,
                        success=False,
                        degraded=True,
                        message="Guide delivery state changed during reconciliation",
                        error="GUIDE_EMBY_PENDING",
                    )
                if task._cancel_requested:
                    return _finish_cancelled(started_at, details, publications)

            from tasks.dummy_epg_refresh import wait_for_epg_source_refresh

            for (scope, source_id), document_hash in sorted(pending.items()):
                if task._cancel_requested:
                    return _finish_cancelled(started_at, details, publications)
                if not scope.startswith("profile:"):
                    continue
                source = next(
                    item for item in generated_sources[scope] if item["id"] == source_id
                )
                source_key, _, _ = _source_refresh_key(client, source, scope)
                current = read_publication(scope)
                if current is None or current["state"]["xmltv_hash"] != document_hash:
                    continue
                delivery = current["state"]["delivery"]
                attempt = delivery["guide_attempt"]
                progress = copy.deepcopy(delivery["source_refreshes"].get(source_key))
                if attempt is None or progress is None:
                    continue
                source_expires = datetime.fromisoformat(progress["expires_at"])
                if datetime.now(timezone.utc) >= source_expires:
                    continue
                if progress["triggered"] is False:
                    claimed_refreshes = copy.deepcopy(delivery["source_refreshes"])
                    claimed_refreshes[source_key]["triggered"] = True
                    next_revision = update_delivery(
                        scope,
                        expected_revision=current["revision"],
                        expected_hash=document_hash,
                        expected_config_hash=current["state"]["config_hash"],
                        expected_attempt_id=attempt["attempt_id"],
                        source_refreshes=claimed_refreshes,
                    )
                    if next_revision is None:
                        continue
                    current = read_publication(scope)
                    publications[scope] = current
                    if task._cancel_requested:
                        return _finish_cancelled(started_at, details, publications)
                    try:
                        await client.refresh_epg_source(source_id)
                    except Exception:
                        logger.exception(
                            "[EVENT-WORKFLOW] Could not trigger guide source %s", source_id,
                        )
                        continue
                    progress = copy.deepcopy(
                        current["state"]["delivery"]["source_refreshes"][source_key]
                    )
                completed = await wait_for_epg_source_refresh(
                    client,
                    source_id,
                    source.get("name") or f"Source {source_id}",
                    expires_at=source_expires,
                    initial_source=source,
                    trigger=False,
                    cancelled=lambda: task._cancel_requested,
                    progress=progress,
                    wait=wait_for_sources,
                )
                current = read_publication(scope)
                if current is None or current["state"]["xmltv_hash"] != document_hash:
                    continue
                delivery = current["state"]["delivery"]
                attempt = delivery["guide_attempt"]
                current_progress = delivery["source_refreshes"].get(source_key)
                if (
                    attempt is None
                    or current_progress is None
                    or current_progress["attempt_id"] != progress["attempt_id"]
                ):
                    continue
                refreshes = copy.deepcopy(delivery["source_refreshes"])
                refreshes[source_key]["observed_running"] = progress["observed_running"]
                confirmed = dict(delivery["confirmed_dispatcharr_hashes"])
                if completed:
                    confirmed[str(source_id)] = document_hash
                next_revision = update_delivery(
                    scope,
                    expected_revision=current["revision"],
                    expected_hash=document_hash,
                    expected_config_hash=current["state"]["config_hash"],
                    expected_attempt_id=attempt["attempt_id"],
                    confirmed_dispatcharr_hashes=confirmed,
                    source_refreshes=refreshes,
                )
                if next_revision is not None:
                    if completed:
                        confirmed_sources.add((scope, source_id))
                    publications[scope] = read_publication(scope)
                    details["pending_source_hashes"] = {
                        f"{key[0]}:{key[1]}": value
                        for key, value in sorted(pending.items())
                        if key not in confirmed_sources
                    }
                if task._cancel_requested:
                    return _finish_cancelled(started_at, details, publications)

            if task._cancel_requested:
                return _finish_cancelled(started_at, details, publications)

            details["pending_source_hashes"] = {
                f"{scope}:{source_id}": document_hash
                for (scope, source_id), document_hash in sorted(pending.items())
                if (scope, source_id) not in confirmed_sources
            }

            guide_rows = {}
            source_by_id = {
                source["id"]: source
                for scope, values in generated_sources.items() for source in values
                if (scope, source["id"]) in confirmed_sources
            }
            for source_id in sorted(source_by_id):
                if task._cancel_requested:
                    return _finish_cancelled(started_at, details, publications)
                try:
                    resolved_rows = await client.get_epg_data(
                        epg_source=source_id,
                        max_results=EPG_LINK_MAX_RESULTS,
                    )
                    if task._cancel_requested:
                        return _finish_cancelled(started_at, details, publications)
                    for row in resolved_rows:
                        if row.get("id") is not None and row.get("tvg_id"):
                            guide_rows.setdefault(row["tvg_id"], []).append((source_id, row))
                except Exception:
                    logger.exception("[EVENT-WORKFLOW] Could not resolve guide rows for source %s", source_id)

            linked_rows = {}
            for profile_id, plan in sorted(plans.items()):
                profile = plan["profile"]
                available_source_ids = {
                    source["id"]
                    for scope in (f"profile:{profile_id}", "all")
                    for source in generated_sources.get(scope, [])
                    if (scope, source["id"]) in confirmed_sources
                }
                for assignment in profile.get("channel_assignments") or []:
                    channel_id = assignment.get("channel_id")
                    if channel_id not in plan["states"] or plan["states"][channel_id] != "active":
                        continue
                    channel = channel_map[channel_id]
                    xmltv_id = get_xmltv_id(assignment, channel, profile)
                    candidates = [
                        (source_id, row)
                        for source_id, row in guide_rows.get(xmltv_id, [])
                        if source_id in available_source_ids
                    ]
                    if len(candidates) != 1:
                        continue
                    source_id, guide_row = candidates[0]
                    current_link = channel.get("epg_data_id") or channel.get("epg_data")
                    current_source = None
                    if isinstance(channel.get("epg_data"), dict):
                        current_source = _epg_source_id(
                            channel["epg_data"].get("epg_source")
                            or channel["epg_data"].get("epg_source_id")
                        )
                    if current_link is not None and current_link != guide_row["id"]:
                        if current_source not in source_by_id:
                            continue
                    if current_link != guide_row["id"]:
                        if task._cancel_requested:
                            return _finish_cancelled(started_at, details, publications)
                        fresh = await current_channel(channel_id)
                        if fresh is None:
                            continue
                        fresh_link = fresh.get("epg_data_id") or fresh.get("epg_data")
                        if isinstance(fresh_link, dict):
                            fresh_link = fresh_link.get("id")
                        if fresh_link is not None and fresh_link != current_link:
                            continue
                        try:
                            if not await update_current_channel(
                                channel_id, {"epg_data_id": guide_row["id"]},
                            ):
                                continue
                        except Exception:
                            logger.exception("[EVENT-WORKFLOW] Could not link guide row for channel %s", channel_id)
                            continue
                        channel["epg_data_id"] = guide_row["id"]
                        details["epg_linked_channel_ids"].append(channel_id)
                        try:
                            stored_pending = _store_emby_pending(
                                publications,
                                pending=True,
                                read_publication=read_publication,
                                update_delivery=update_delivery,
                            )
                        except Exception:
                            logger.exception("[EVENT-WORKFLOW] Could not persist Emby delivery state")
                            stored_pending = False
                        if not stored_pending:
                            details["pending_emby"] = True
                            details["delivery_pending"] = True
                            details["reason_codes"] = ["GUIDE_EMBY_PENDING"]
                            return _finish(
                                started_at,
                                details,
                                success=False,
                                degraded=True,
                                message="Guide delivery state changed during reconciliation",
                                error="GUIDE_EMBY_PENDING",
                            )
                        if task._cancel_requested:
                            return _finish_cancelled(started_at, details, publications)
                    linked_rows[channel_id] = (
                        profile_id, source_id, guide_row["id"], xmltv_id,
                    )

            from services.epg_programmes import programme_matches

            programme_events = {}
            for channel_id, (profile_id, _, _, _) in linked_rows.items():
                publication_row = publications.get(f"profile:{profile_id}")
                if publication_row is None or not claim_current(channel_id):
                    continue
                evidence = next((
                    item for item in publication_row["state"]["channels"]
                    if item["channel_id"] == channel_id
                ), None)
                if evidence is None:
                    continue
                current_time = datetime.now(timezone.utc)
                current_event = next((
                    item for item in evidence["events"]
                    if datetime.fromisoformat(item["start"]) <= current_time
                    < datetime.fromisoformat(item["stop"])
                ), None)
                if current_event is not None:
                    programme_events[channel_id] = current_event

            programme_rows = {}
            for profile_id in sorted({
                values[0] for channel_id, values in linked_rows.items()
                if channel_id in programme_events
            }):
                publication_row = publications.get(f"profile:{profile_id}")
                attempt_row = (
                    publication_row["state"]["delivery"].get("guide_attempt")
                    if publication_row is not None else None
                )
                if attempt_row is None:
                    continue
                profile_expiry = datetime.fromisoformat(attempt_row["expires_at"])
                profile_channels = [
                    channel_id
                    for channel_id, values in linked_rows.items()
                    if values[0] == profile_id
                    and channel_id in programme_events
                    and claim_current(channel_id)
                ]
                profile_ids = sorted({
                    linked_rows[channel_id][2] for channel_id in profile_channels
                })
                for offset in range(0, len(profile_ids), 50):
                    if task._cancel_requested:
                        return _finish_cancelled(started_at, details, publications)
                    batch = frozenset(profile_ids[offset:offset + 50])
                    if not batch:
                        continue
                    try:
                        rows = await client.get_epg_programmes(
                            batch, expires_at=profile_expiry,
                        )
                    except Exception:
                        logger.exception(
                            "[EVENT-WORKFLOW] Could not read imported programmes for profile %s",
                            profile_id,
                        )
                        continue
                    if task._cancel_requested:
                        return _finish_cancelled(started_at, details, publications)
                    for row in rows:
                        programme_rows[row["epg_data_id"]] = row

            programme_ready = set()
            for channel_id, (profile_id, _, _, xmltv_id) in linked_rows.items():
                channel = channel_map[channel_id]
                current_event = programme_events.get(channel_id)
                programme_row = programme_rows.get(linked_rows[channel_id][2])
                current_time = datetime.now(timezone.utc)
                if (
                    current_event is None
                    or programme_row is None
                    or not claim_current(channel_id)
                    or not (
                        datetime.fromisoformat(current_event["start"])
                        <= current_time
                        < datetime.fromisoformat(current_event["stop"])
                    )
                ):
                    continue
                if programme_matches(
                    [programme_row],
                    xmltv_id=xmltv_id,
                    channel_uuid=channel.get("uuid"),
                    title=current_event["title"],
                    start=current_event["start"],
                    stop=current_event["stop"],
                ):
                    programme_ready.add(channel_id)

            link_pending = False
            for plan in plans.values():
                for channel_id, state in plan["states"].items():
                    if state != "active" or channel_id in programme_ready:
                        continue
                    link_pending = True

            for profile_id, plan in sorted(plans.items()):
                for channel_id, desired_ids in sorted(plan["desired"].items()):
                    state = plan["states"][channel_id]
                    channel = channel_map[channel_id]
                    fresh = await current_channel(channel_id)
                    if fresh is None:
                        continue
                    fresh_link = fresh.get("epg_data_id") or fresh.get("epg_data")
                    if isinstance(fresh_link, dict):
                        fresh_link = fresh_link.get("id")
                    current_event = programme_events.get(channel_id)
                    current_time = datetime.now(timezone.utc)
                    if (
                        state == "active"
                        and (
                            current_event is None
                            or fresh_link != linked_rows.get(channel_id, (None, None, None))[2]
                            or not (
                                datetime.fromisoformat(current_event["start"])
                                <= current_time
                                < datetime.fromisoformat(current_event["stop"])
                            )
                        )
                    ):
                        programme_ready.discard(channel_id)
                        link_pending = True
                    attached_ids = [
                        stream_id for stream_id in (_stream_id(row) for row in fresh.get("streams") or [])
                        if stream_id is not None
                    ]
                    update = {}
                    if desired_ids != attached_ids:
                        update["streams"] = desired_ids
                    if (
                        state == "active"
                        and channel_id in programme_ready
                        and health_states.get(channel_id) is True
                        and fresh.get("hidden_from_output")
                    ):
                        update["hidden_from_output"] = False
                    if state == "active" and desired_ids and channel_id not in programme_ready:
                        update.pop("streams", None)
                    if not update:
                        continue
                    if task._cancel_requested:
                        return _finish_cancelled(started_at, details, publications)
                    try:
                        if not await update_current_channel(channel_id, update):
                            continue
                    except Exception:
                        logger.exception("[EVENT-WORKFLOW] Could not apply channel %s", channel_id)
                        continue
                    if "streams" in update:
                        details["stream_updated_channel_ids"].append(channel_id)
                    if update.get("hidden_from_output") is False:
                        details["revealed_channel_ids"].append(channel_id)
                    try:
                        stored_pending = _store_emby_pending(
                            publications,
                            pending=True,
                            read_publication=read_publication,
                            update_delivery=update_delivery,
                        )
                    except Exception:
                        logger.exception("[EVENT-WORKFLOW] Could not persist Emby delivery state")
                        stored_pending = False
                    if not stored_pending:
                        details["pending_emby"] = True
                        details["delivery_pending"] = True
                        details["reason_codes"] = ["GUIDE_EMBY_PENDING"]
                        return _finish(
                            started_at,
                            details,
                            success=False,
                            degraded=True,
                            message="Guide delivery state changed during reconciliation",
                            error="GUIDE_EMBY_PENDING",
                        )
                    if task._cancel_requested:
                        return _finish_cancelled(started_at, details, publications)

            changed = any(
                details[name]
                for name in (
                    "stream_updated_channel_ids",
                    "epg_linked_channel_ids",
                    "revealed_channel_ids",
                    "hidden_channel_ids",
                )
            )
            pending_emby = any(
                row["state"]["delivery"].get("pending_emby")
                for row in publications.values()
            )
            if changed or pending_emby:
                from emby_client import request_guide_refresh

                if task._cancel_requested:
                    return _finish_cancelled(started_at, details, publications)
                outcome = await request_guide_refresh()
                details["emby_request_outcome"] = (
                    "accepted" if outcome is True else "disabled" if outcome is None else "pending"
                )
                details["pending_emby"] = outcome is False
                if not _store_emby_pending(
                    publications,
                    pending=outcome is False,
                    read_publication=read_publication,
                    update_delivery=update_delivery,
                ):
                    details["pending_emby"] = True
                    details["delivery_pending"] = True
                if task._cancel_requested:
                    return _finish_cancelled(started_at, details, publications)

            reasons = set(publication.reason_codes)
            reasons.update(
                reason
                for values in details["source_reason_codes"].values()
                for reason in values
            )
            if details["unavailable_profile_ids"]:
                reasons.add("GUIDE_UNAVAILABLE")
            if details["pending_source_hashes"]:
                reasons.add("GUIDE_IMPORT_PENDING")
            if link_pending:
                reasons.add("GUIDE_IMPORT_PENDING")
            if details["pending_emby"]:
                reasons.add("GUIDE_EMBY_PENDING")
            details["delivery_pending"] = bool(
                details["pending_source_hashes"] or link_pending or details["pending_emby"]
            )
            details["reason_codes"] = sorted(reasons)

            degraded = bool(
                details["retained_profile_ids"]
                or details["unavailable_profile_ids"]
                or details["delivery_pending"]
                or any("PENDING" in reason or "STALE" in reason for reason in reasons)
            )
            usable = bool(
                details["published_profile_ids"] or details["retained_profile_ids"]
            )
            safe_work = bool(
                details["hidden_channel_ids"]
                or details["stream_updated_channel_ids"]
                or details["epg_linked_channel_ids"]
                or details["revealed_channel_ids"]
            )
            if degraded and (usable or safe_work):
                error = (
                    "GUIDE_EMBY_PENDING" if details["pending_emby"]
                    else "GUIDE_IMPORT_PENDING" if "GUIDE_IMPORT_PENDING" in reasons
                    else "GUIDE_OWNERSHIP_CONFLICT" if "GUIDE_OWNERSHIP_CONFLICT" in reasons
                    else "GUIDE_SOURCES_PENDING" if "GUIDE_SOURCES_PENDING" in reasons
                    else "GUIDE_UNAVAILABLE"
                )
                return _finish(
                    started_at,
                    details,
                    success=False,
                    degraded=True,
                    message="Guide reconciliation completed with retained or pending work",
                    error=error,
                )
            if not usable and not safe_work:
                error = (
                    "GUIDE_OWNERSHIP_CONFLICT" if "GUIDE_OWNERSHIP_CONFLICT" in reasons
                    else "GUIDE_UNAVAILABLE"
                )
                return _finish(
                    started_at, details, success=False,
                    message="No complete guide publication was available",
                    error=error,
                )
            return _finish(
                started_at,
                details,
                success=True,
                message="Guide profiles, channels, and delivery state are reconciled",
            )

    raise RuntimeError("Guide reconciliation did not settle profile revisions.")


@register_task
class EventVisibilityTask(TaskScheduler):
    """Reconcile event profiles on the short recurring schedule."""

    task_id = "event_visibility"
    task_name = "Event Visibility Check"
    task_description = "Keep configured event channels aligned with the published guide"

    def __init__(self, schedule_config: Optional[ScheduleConfig] = None):
        if schedule_config is None:
            schedule_config = ScheduleConfig(
                schedule_type=ScheduleType.INTERVAL,
                interval_seconds=CHECK_INTERVAL_SECONDS,
                timezone="America/Chicago",
            )
        super().__init__(schedule_config)

    async def execute(self) -> TaskResult:
        return await reconcile_profiles(self, wait_for_sources=False)
