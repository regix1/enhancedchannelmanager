"""Reconcile configured event profiles with their published guide and channels."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
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


def _profile_token(profiles: list[dict]) -> str:
    value = json.dumps(profiles, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _rule_token(rules: list) -> str:
    values = [
        {
            "id": getattr(rule, "id", None),
            "enabled": getattr(rule, "enabled", None),
            "event_sync_config": getattr(rule, "event_sync_config", None),
            "updated_at": getattr(rule, "updated_at", None),
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
    desired = {}
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
        bootstrap = next((
            item.get("bootstrap") is True
            for item in config.get("slot_patterns", [])
            if slot is not None and item.get("name") == slot[0]
        ), False)
        active_direct = []
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
            active_direct.append(stream)
            title = parsed.title or stream.name
            interval = {
                "channel_id": channel_id,
                "title": title,
                "start": parsed.start.isoformat(),
                "stop": stop.isoformat(),
            }
            intervals.setdefault(channel_id, []).append(interval)
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

        active = current is not None or bool(active_direct) or _stored_active(retained, channel_id, now)
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
            desired[channel_id] = _ordered_ids(titled_matches, scopes)
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
    from services.epg_programmes import _epg_source_id, _fetch_all_channels, prepare_profiles
    from services.epg_publication import (
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
            for profile in profiles:
                retained[profile["id"]] = read_publication(f"profile:{profile['id']}")

            if task._cancel_requested:
                return _finish_cancelled(started_at, details)
            stage = "channels"
            channel_map = await _fetch_all_channels(client)
            if task._cancel_requested:
                return _finish_cancelled(started_at, details)
            stage = "programmes"
            preparation = await _await_preparation(
                prepare_profiles(
                    profiles,
                    channel_map,
                    client,
                    now=now,
                    wait_for_sources=wait_for_sources,
                    recover_sources=True,
                ),
                lambda: task._cancel_requested,
            )
            if preparation is None:
                return _finish_cancelled(started_at, details)
            prepared, coverage = preparation
            if task._cancel_requested:
                return _finish_cancelled(started_at, details)
            stage = "slots"
            configs = {}
            scopes = []
            invalid_profiles = set()
            for profile in prepared:
                profile_id = profile["id"]
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
                details["source_reason_codes"][str(profile_id)] = list(
                    coverage.get("profiles", {}).get(str(profile_id), {}).get("reason_codes") or []
                )
            for key in scope_failures:
                for profile in prepared:
                    if key in {_scope_key(scope) for scope in configs.get(profile["id"], {}).get("secondary", [])}:
                        details["source_reason_codes"][str(profile["id"])] = sorted(
                            set(details["source_reason_codes"][str(profile["id"])]) | {"GUIDE_SOURCES_PENDING"}
                        )
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

        async with publication_lock:
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
            confirmed_source_ids = set()
            for scope, row in sorted(publications.items()):
                required, confirmed, scope_pending = _delivery_plan(
                    row, generated_sources.get(scope, []),
                )
                delivery = row["state"]["delivery"]
                if (
                    delivery["required_dispatcharr_hashes"] != required
                    or delivery["confirmed_dispatcharr_hashes"] != confirmed
                ):
                    next_revision = update_delivery(
                        scope,
                        expected_revision=row["revision"],
                        required_dispatcharr_hashes=required,
                        confirmed_dispatcharr_hashes=confirmed,
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
                confirmed_source_ids.update(
                    int(source_id) for source_id in confirmed
                )
                pending.update({
                    source_id: (scope, document_hash)
                    for source_id, document_hash in scope_pending.items()
                })

            details["pending_source_hashes"] = {
                str(source_id): document_hash
                for source_id, (_, document_hash) in sorted(pending.items())
                if source_id not in confirmed_source_ids
            }
            if task._cancel_requested:
                return _finish_cancelled(started_at, details, publications)

            for channel_id, state in sorted(states.items()):
                channel = channel_map[channel_id]
                if state != "idle" or channel.get("hidden_from_output"):
                    continue
                if task._cancel_requested:
                    return _finish_cancelled(started_at, details, publications)
                try:
                    await client.update_channel(channel_id, {"hidden_from_output": True})
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

            for source_id, (scope, document_hash) in sorted(pending.items()):
                if task._cancel_requested:
                    return _finish_cancelled(started_at, details, publications)
                source = next(
                    item for item in generated_sources[scope] if item["id"] == source_id
                )
                completed = await wait_for_epg_source_refresh(
                    client,
                    source_id,
                    source.get("name") or f"Source {source_id}",
                    cancelled=lambda: task._cancel_requested,
                )
                if not completed:
                    continue
                current = read_publication(scope)
                if current is None or current["state"]["xmltv_hash"] != document_hash:
                    continue
                confirmed = dict(current["state"]["delivery"]["confirmed_dispatcharr_hashes"])
                confirmed[str(source_id)] = document_hash
                next_revision = update_delivery(
                    scope,
                    expected_revision=current["revision"],
                    confirmed_dispatcharr_hashes=confirmed,
                )
                if next_revision is not None:
                    confirmed_source_ids.add(source_id)
                    publications[scope] = read_publication(scope)
                    details["pending_source_hashes"] = {
                        str(key): value[1]
                        for key, value in sorted(pending.items())
                        if key not in confirmed_source_ids
                    }
                if task._cancel_requested:
                    return _finish_cancelled(started_at, details, publications)

            if task._cancel_requested:
                return _finish_cancelled(started_at, details, publications)

            details["pending_source_hashes"] = {
                str(source_id): document_hash
                for source_id, (_, document_hash) in sorted(pending.items())
                if source_id not in confirmed_source_ids
            }

            guide_rows = {}
            source_by_id = {
                source["id"]: source
                for values in generated_sources.values() for source in values
                if source["id"] in confirmed_source_ids
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
                    if source["id"] in confirmed_source_ids
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
                        try:
                            await client.update_channel(channel_id, {"epg_data_id": guide_row["id"]})
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
                    linked_rows[channel_id] = (source_id, guide_row["id"])

            link_pending = False
            for plan in plans.values():
                for channel_id, state in plan["states"].items():
                    if state != "active" or channel_id in linked_rows:
                        continue
                    channel = channel_map[channel_id]
                    if channel.get("epg_data_id") is None and channel.get("epg_data") is None:
                        link_pending = True

            for profile_id, plan in sorted(plans.items()):
                for channel_id, desired_ids in sorted(plan["desired"].items()):
                    state = plan["states"][channel_id]
                    channel = channel_map[channel_id]
                    attached_ids = [
                        stream_id for stream_id in (_stream_id(row) for row in channel.get("streams") or [])
                        if stream_id is not None
                    ]
                    update = {}
                    if desired_ids != attached_ids:
                        update["streams"] = desired_ids
                    if state == "active" and channel_id in linked_rows and channel.get("hidden_from_output"):
                        update["hidden_from_output"] = False
                    if not update:
                        continue
                    if task._cancel_requested:
                        return _finish_cancelled(started_at, details, publications)
                    try:
                        await client.update_channel(channel_id, update)
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
