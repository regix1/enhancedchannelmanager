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
from uuid import uuid4

import pytz
from sqlalchemy.exc import IntegrityError

from database import get_session
from models import DummyEPGProfile, GuidePublication
from services.epg_programmes import (
    MAX_CACHE,
    MAX_RETAINED,
    _placeholder,
    _resolve_group_assignments,
    programme_times,
)


STATE_VERSION = 1
AGGREGATE_SCOPE = "all"
MAX_OBSERVATIONS = 10000
MAX_CHANNELS = 10000
publication_lock = asyncio.Lock()

GUIDE_STAGES = frozenset({
    "preparing", "importing", "linking", "complete", "expired", "failed",
})
PENDING_STAGES = frozenset({
    "intent", "allocating", "allocation_unknown", "allocated", "importing",
    "linking", "ready", "complete", "failed", "expired",
})
PENDING_REASONS = frozenset({
    "guide_pending", "guide_failed", "guide_expired", "allocation_unknown",
    "channel_missing", "ownership_changed", "health_unknown", "health_failed",
    "programme_missing",
})
TERMINAL_GUIDE_STAGES = frozenset({"complete", "expired", "failed"})
TERMINAL_PENDING_STAGES = frozenset({"complete", "failed", "expired", "allocation_unknown"})
RECOVERABLE_REASONS = frozenset({
    "guide_failed", "guide_expired", "programme_missing", "health_unknown", "health_failed",
})
HEALTH_REASONS = frozenset({"health_unknown", "health_failed"})


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


def _sha(value, field_name: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 value.")
    return value


def _attempt(value, field_name: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{32}", value) is None:
        raise ValueError(f"{field_name} must be a lowercase UUID hex value.")
    return value


def _positive(value, field_name: str, *, optional: bool = False) -> int | None:
    if optional and value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer.")
    return value


def _text(value, field_name: str, *, optional: bool = False, limit: int = 4096) -> str | None:
    if optional and value is None:
        return None
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError(f"{field_name} must be a bounded nonempty string.")
    return value


def _optional_time(value, field_name: str) -> str | None:
    return None if value is None else _iso(value, field_name)


def _guide_attempt(value) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("Stored guide attempt is invalid.")
    required = {"attempt_id", "config_hash", "admitted_at", "expires_at", "stage"}
    if set(value) != required:
        raise ValueError("Stored guide attempt fields are invalid.")
    admitted_at = _utc(value["admitted_at"], "guide attempt admission")
    expires_at = _optional_time(value["expires_at"], "guide attempt expiry")
    if expires_at is not None and _utc(expires_at, "guide attempt expiry") <= admitted_at:
        raise ValueError("Stored guide attempt lifetime is invalid.")
    stage = value["stage"]
    if stage not in GUIDE_STAGES:
        raise ValueError("Stored guide attempt stage is invalid.")
    return {
        "attempt_id": _attempt(value["attempt_id"], "guide attempt ID"),
        "config_hash": _sha(value["config_hash"], "guide attempt config hash"),
        "admitted_at": admitted_at.isoformat(),
        "expires_at": expires_at,
        "stage": stage,
    }


def _streams(values) -> list[dict]:
    if not isinstance(values, list) or len(values) > MAX_CHANNELS:
        raise ValueError("Pending channel streams are invalid.")
    result = []
    seen = set()
    for value in values:
        if not isinstance(value, Mapping):
            raise ValueError("Pending channel stream is invalid.")
        if set(value) != {"id", "name", "account_id", "group_id"}:
            raise ValueError("Pending channel stream fields are invalid.")
        stream_id = _positive(value["id"], "pending stream ID")
        if stream_id in seen:
            raise ValueError("Pending channel streams contain duplicate IDs.")
        seen.add(stream_id)
        result.append({
            "id": stream_id,
            "name": _text(value["name"], "pending stream name", limit=1024),
            "account_id": _positive(value["account_id"], "pending stream account ID", optional=True),
            "group_id": _positive(value["group_id"], "pending stream group ID"),
        })
    return result


def _history_entry(value) -> dict:
    if not isinstance(value, Mapping):
        raise ValueError("Pending channel history entry is invalid.")
    required = {
        "attempt_id", "attempt_no", "input_hash", "admitted_at", "expires_at",
        "terminal_at", "retry_at", "stage", "reason", "execution_id", "channel_id",
        "channel_uuid", "guide_attempt_id", "rule_hash", "config_hash",
        "revision", "xmltv_hash",
    }
    optional = {"detail"}
    if not required <= set(value) or set(value) - required - optional:
        raise ValueError("Pending channel history fields are invalid.")
    stage = value["stage"]
    if stage not in TERMINAL_PENDING_STAGES:
        raise ValueError("Pending channel history stage is invalid.")
    reason = value["reason"]
    failure_reasons = {
        "guide_failed", "channel_missing", "ownership_changed",
        "health_unknown", "health_failed", "programme_missing",
    }
    if (
        (stage == "complete" and reason is not None)
        or (stage == "allocation_unknown" and reason != "allocation_unknown")
        or (stage == "expired" and reason != "guide_expired")
        or (stage == "failed" and reason not in failure_reasons)
    ):
        raise ValueError("Pending channel history reason is invalid.")
    admitted_at = _utc(value["admitted_at"], "pending history admission")
    expires_at = _utc(value["expires_at"], "pending history expiry")
    terminal_at = _utc(value["terminal_at"], "pending history terminal time")
    retry_at = _utc(value["retry_at"], "pending history retry time") if value["retry_at"] is not None else None
    if expires_at <= admitted_at:
        raise ValueError("Pending channel history lifetime is invalid.")
    recoverable = (
        stage in {"failed", "expired"}
        and value["channel_id"] is not None
        and reason in RECOVERABLE_REASONS
    )
    if recoverable:
        if retry_at != terminal_at + timedelta(minutes=5):
            raise ValueError("Pending channel history retry time is invalid.")
    elif retry_at is not None:
        raise ValueError("Pending channel history retry time is invalid.")
    record = {
        "attempt_id": _attempt(value["attempt_id"], "pending history attempt ID"),
        "attempt_no": _positive(value["attempt_no"], "pending history attempt number"),
        "input_hash": _sha(value["input_hash"], "pending history input hash"),
        "admitted_at": admitted_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "terminal_at": terminal_at.isoformat(),
        "retry_at": retry_at.isoformat() if retry_at else None,
        "stage": stage,
        "reason": reason,
        "execution_id": _text(value["execution_id"], "pending history execution ID", limit=512),
        "channel_id": _positive(value["channel_id"], "pending history channel ID", optional=True),
        "channel_uuid": _text(value["channel_uuid"], "pending history channel UUID", optional=True, limit=512),
        "guide_attempt_id": _attempt(value["guide_attempt_id"], "pending history guide attempt ID"),
        "rule_hash": _sha(value["rule_hash"], "pending history rule hash"),
        "config_hash": _sha(value["config_hash"], "pending history config hash"),
        "revision": _positive(value["revision"], "pending history publication revision"),
        "xmltv_hash": _sha(value["xmltv_hash"], "pending history XMLTV hash"),
    }
    if "detail" in value:
        record["detail"] = _text(value["detail"], "pending history detail", optional=True)
    return record


def _pending_channel(event_key: str, value) -> dict:
    if not isinstance(value, Mapping):
        raise ValueError("Pending channel receipt is invalid.")
    required = {
        "event_key", "rule_id", "rule_hash", "config_hash", "profile_id",
        "target_group_id", "title", "start", "stop", "streams", "channel_name",
        "channel_id", "channel_uuid", "stage", "reason", "execution_id", "attempt_id",
        "attempt_no", "input_hash", "admitted_at", "expires_at", "terminal_at",
        "retry_at", "guide_attempt_id", "history",
    }
    optional = {"detail"}
    if not required <= set(value) or set(value) - required - optional:
        raise ValueError("Pending channel receipt fields are invalid.")
    if value["event_key"] != event_key or not event_key or len(event_key) > 1024:
        raise ValueError("Pending channel event identity is invalid.")
    stage = value["stage"]
    if stage not in PENDING_STAGES:
        raise ValueError("Pending channel stage is invalid.")
    reason = value["reason"]
    if reason is not None and reason not in PENDING_REASONS:
        raise ValueError("Pending channel reason is invalid.")
    admitted_at = _utc(value["admitted_at"], "pending channel admission")
    expires_at = _utc(value["expires_at"], "pending channel expiry")
    start = _utc(value["start"], "pending channel start")
    stop = _utc(value["stop"], "pending channel stop")
    terminal_at = _utc(value["terminal_at"], "pending channel terminal time") if value["terminal_at"] is not None else None
    retry_at = _utc(value["retry_at"], "pending channel retry time") if value["retry_at"] is not None else None
    if stop <= start or not admitted_at < expires_at <= stop:
        raise ValueError("Pending channel interval or lifetime is invalid.")
    failure_reasons = {
        "guide_failed", "channel_missing", "ownership_changed",
        "health_unknown", "health_failed", "programme_missing",
    }
    if stage in TERMINAL_PENDING_STAGES:
        if terminal_at is None:
            raise ValueError("Terminal pending channel receipt is incomplete.")
        if (
            (stage == "complete" and reason is not None)
            or (stage == "allocation_unknown" and reason != "allocation_unknown")
            or (stage == "expired" and reason != "guide_expired")
            or (stage == "failed" and reason not in failure_reasons)
        ):
            raise ValueError("Terminal pending channel receipt reason is invalid.")
    elif terminal_at is not None or retry_at is not None:
        raise ValueError("Nonterminal pending channel receipt has terminal state.")
    recoverable = (
        stage in {"failed", "expired"}
        and value["channel_id"] is not None
        and reason in RECOVERABLE_REASONS
    )
    if recoverable:
        if retry_at != terminal_at + timedelta(minutes=5):
            raise ValueError("Pending channel retry time is invalid.")
    elif retry_at is not None:
        raise ValueError("Pending channel retry time is invalid.")
    history = [_history_entry(item) for item in value["history"]] if isinstance(value["history"], list) else None
    if history is None or len(history) > 2:
        raise ValueError("Pending channel history is invalid.")
    attempt_no = _positive(value["attempt_no"], "pending channel attempt number")
    history_length = min(attempt_no - 1, 2)
    if len(history) != history_length:
        raise ValueError("Pending channel attempt history is incomplete.")
    attempt_ids = [item["attempt_id"] for item in history]
    attempt_id = _attempt(value["attempt_id"], "pending channel attempt ID")
    if attempt_id in attempt_ids or len(attempt_ids) != len(set(attempt_ids)):
        raise ValueError("Pending channel attempt IDs are not unique.")
    if [item["attempt_no"] for item in history] != list(
        range(attempt_no - history_length, attempt_no)
    ):
        raise ValueError("Pending channel history order is invalid.")
    if attempt_no > 3:
        latest = history[-1]
        if (
            latest["stage"] != "failed"
            or latest["reason"] not in HEALTH_REASONS
            or latest["input_hash"] != value["input_hash"]
        ):
            raise ValueError("Extended pending channel history is invalid.")
    record = {
        "event_key": event_key,
        "rule_id": _positive(value["rule_id"], "pending channel rule ID"),
        "rule_hash": _sha(value["rule_hash"], "pending channel rule hash"),
        "config_hash": _sha(value["config_hash"], "pending channel config hash"),
        "profile_id": _positive(value["profile_id"], "pending channel profile ID"),
        "target_group_id": _positive(value["target_group_id"], "pending channel target group ID"),
        "title": _text(value["title"], "pending channel title"),
        "start": start.isoformat(),
        "stop": stop.isoformat(),
        "streams": _streams(value["streams"]),
        "channel_name": _text(value["channel_name"], "pending channel name", limit=1024),
        "channel_id": _positive(value["channel_id"], "pending channel ID", optional=True),
        "channel_uuid": _text(value["channel_uuid"], "pending channel UUID", optional=True, limit=512),
        "stage": stage,
        "reason": reason,
        "execution_id": _text(value["execution_id"], "pending channel execution ID", limit=512),
        "attempt_id": attempt_id,
        "attempt_no": attempt_no,
        "input_hash": _sha(value["input_hash"], "pending channel input hash"),
        "admitted_at": admitted_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "terminal_at": terminal_at.isoformat() if terminal_at else None,
        "retry_at": retry_at.isoformat() if retry_at else None,
        "guide_attempt_id": _attempt(value["guide_attempt_id"], "pending channel guide attempt ID"),
        "history": history,
    }
    if "detail" in value:
        record["detail"] = _text(value["detail"], "pending channel detail", optional=True)
    return record


def _links(value) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or len(value) > MAX_CHANNELS:
        raise ValueError("Stored source links are invalid.")
    result = {}
    for key, link in value.items():
        if not isinstance(key, str) or re.fullmatch(r"[1-9]\d*", key) is None:
            raise ValueError("Stored source channel ID is invalid.")
        result[key] = _positive(link, "source programme link")
    return dict(sorted(result.items()))


def _source_refreshes(values, guide_attempt: dict | None, xmltv_hash: str) -> dict:
    if not isinstance(values, Mapping) or len(values) > MAX_CHANNELS:
        raise ValueError("Stored source refreshes are invalid.")
    result = {}
    for key, value in values.items():
        if not isinstance(key, str) or not key or len(key) > 1024 or not isinstance(value, Mapping):
            raise ValueError("Stored source refresh is invalid.")
        required = {
            "source_id", "endpoint_hash", "source_url_hash", "expected_hash",
            "initial_updated", "observed_running", "triggered", "expires_at", "attempt_id",
        }
        extended = {"links", "pending_links", "completed"}
        if set(value) not in (required, required | extended):
            raise ValueError("Stored source refresh fields are invalid.")
        completed = value.get("completed", False)
        if not isinstance(completed, bool) or (completed and value["triggered"] is not True):
            raise ValueError("Stored source refresh completion is invalid.")
        expires_at = _optional_time(value["expires_at"], "source refresh expiry")
        attempt_id = _attempt(value["attempt_id"], "source refresh attempt ID")
        if guide_attempt is None or attempt_id != guide_attempt["attempt_id"]:
            raise ValueError("Stored source refresh has the wrong guide attempt.")
        if expires_at != guide_attempt["expires_at"]:
            raise ValueError("Stored source refresh has the wrong expiry.")
        if value["expected_hash"] != xmltv_hash:
            raise ValueError("Stored source refresh has the wrong XMLTV hash.")
        if not isinstance(value["observed_running"], bool) or not isinstance(value["triggered"], bool):
            raise ValueError("Stored source refresh progress is invalid.")
        result[key] = {
            "links": _links(value.get("links")),
            "pending_links": _links(value.get("pending_links")),
            "completed": completed,
            "source_id": _positive(value["source_id"], "source refresh source ID"),
            "endpoint_hash": _sha(value["endpoint_hash"], "source refresh endpoint hash"),
            "source_url_hash": _sha(value["source_url_hash"], "source refresh URL hash"),
            "expected_hash": xmltv_hash,
            "initial_updated": _text(value["initial_updated"], "source refresh initial timestamp", optional=True, limit=512),
            "observed_running": value["observed_running"],
            "triggered": value["triggered"],
            "expires_at": expires_at,
            "attempt_id": attempt_id,
        }
    return dict(sorted(result.items()))


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
    published = state.get("published", True)
    if not isinstance(published, bool):
        raise ValueError("Stored publication state is invalid.")
    state["published"] = published
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
    root, document_channels = _document(document, MAX_CACHE)
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
    new_fields = {"guide_attempt", "source_refreshes", "pending_channels"}
    present = new_fields & set(delivery)
    if present and present != new_fields:
        raise ValueError("Stored publication delivery attempt fields are incomplete.")
    if not present:
        delivery["guide_attempt"] = None
        delivery["source_refreshes"] = {}
        delivery["pending_channels"] = {}
    else:
        guide_attempt = _guide_attempt(delivery["guide_attempt"])
        delivery["guide_attempt"] = guide_attempt
        legacy_sources = {
            str(value["source_id"])
            for value in delivery["source_refreshes"].values()
            if isinstance(value, Mapping) and "links" not in value
        } if isinstance(delivery["source_refreshes"], Mapping) else set()
        delivery["source_refreshes"] = _source_refreshes(
            delivery["source_refreshes"], guide_attempt, xmltv_hash,
        )
        for source_id in list(delivery["confirmed_dispatcharr_hashes"]):
            phases = [
                value for value in delivery["source_refreshes"].values()
                if str(value["source_id"]) == source_id
            ]
            if source_id in legacy_sources or (guide_attempt is not None and not phases):
                delivery["confirmed_dispatcharr_hashes"].pop(source_id)
            elif phases and not any(
                value["links"] is not None and value["completed"]
                and value["pending_links"] is None
                and value["expected_hash"] == delivery["confirmed_dispatcharr_hashes"][source_id]
                for value in phases
            ):
                raise ValueError("Confirmed source has no completed programme phase.")
        pending = delivery["pending_channels"]
        if not isinstance(pending, Mapping) or len(pending) > MAX_CHANNELS:
            raise ValueError("Stored pending channels are invalid.")
        delivery["pending_channels"] = {
            key: _pending_channel(key, value)
            for key, value in sorted(pending.items())
        }
        if guide_attempt is None and delivery["pending_channels"]:
            raise ValueError("Stored pending channels require a guide attempt.")
        if guide_attempt is not None and any(
            value["guide_attempt_id"] != guide_attempt["attempt_id"]
            and value["stage"] not in TERMINAL_PENDING_STAGES
            for value in delivery["pending_channels"].values()
        ):
            raise ValueError("Stored active pending channel has the wrong guide attempt.")
    if not published and (
        document_channels
        or root.findall("programme")
        or state_channels
        or normalized_observations
        or members
        or _utc(state["window_start"], "state.window_start")
        != _utc(state["window_stop"], "state.window_stop")
        or delivery["required_dispatcharr_hashes"]
        or delivery["confirmed_dispatcharr_hashes"]
        or delivery["source_refreshes"]
        or delivery["pending_emby"] is not False
    ):
        raise ValueError("Unpublished profile state contains publication evidence.")
    return state


def _read_row(row: GuidePublication) -> dict:
    state = _parse_state(row.state, document=row.xmltv)
    if not isinstance(row.revision, int) or row.revision < 1:
        raise ValueError("Stored publication revision is invalid.")
    if row.scope == AGGREGATE_SCOPE:
        if state["published"] is not True:
            raise ValueError("Stored aggregate publication is unpublished.")
        expected = hashlib.sha256(
            json.dumps(state["members"], sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        if state["config_hash"] != expected:
            raise ValueError("Stored aggregate membership hash is invalid.")
    else:
        profile_id = row.scope.removeprefix("profile:")
        expected_members = {profile_id: state["xmltv_hash"]} if state["published"] else {}
        if state["members"] != expected_members:
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
        "event_intervals", "channel_map", "last_generated_at", "created_at", "updated_at",
    }
    value = {key: item for key, item in profile.items() if key not in transient}
    if value.get("channel_group_ids") or not value.get("channel_assignments"):
        value.pop("channel_assignments", None)
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
    stored = previous.get("delivery") if previous else None
    return {
        "required_dispatcharr_hashes": {},
        "confirmed_dispatcharr_hashes": {},
        "pending_emby": True,
        "guide_attempt": copy.deepcopy(stored.get("guide_attempt")) if isinstance(stored, dict) else None,
        "source_refreshes": {},
        "pending_channels": copy.deepcopy(stored.get("pending_channels", {})) if isinstance(stored, dict) else {},
    }


def _profile_state(
    profile: dict,
    channel_map: dict,
    document: str,
    observations,
    now: datetime,
    previous: dict | None,
    *,
    published: bool = True,
) -> dict:
    if not isinstance(published, bool):
        raise ValueError("Publication state requires a published flag.")
    channels = _intervals(profile, channel_map) if published else []
    begin, end = _bounds(profile, channels, now) if published else (now, now)
    xmltv_hash = _hash(document)
    config_hash = _config_hash(profile)
    compatible = previous if previous and previous.get("config_hash") == config_hash else None
    stored_delivery = previous.get("delivery") if previous else None
    preserve_claim = (
        previous
        if isinstance(stored_delivery, dict)
        and (
            stored_delivery.get("guide_attempt") is not None
            or stored_delivery.get("pending_channels")
        )
        else compatible
    )
    delivery = _delivery(preserve_claim, xmltv_hash)
    if preserve_claim is not None and compatible is None:
        attempt = delivery.get("guide_attempt")
        if attempt is not None and attempt["stage"] not in TERMINAL_GUIDE_STAGES:
            attempt["stage"] = "failed"
    if not published:
        delivery["required_dispatcharr_hashes"] = {}
        delivery["confirmed_dispatcharr_hashes"] = {}
        delivery["pending_emby"] = False
        delivery["source_refreshes"] = {}
    return {
        "version": STATE_VERSION,
        "published": published,
        "published_at": now.isoformat(),
        "xmltv_hash": xmltv_hash,
        "config_hash": config_hash,
        "window_start": begin.isoformat(),
        "window_stop": end.isoformat(),
        "members": {str(profile["id"]): xmltv_hash} if published else {},
        "channels": channels,
        "observations": (
            _profile_observations(
                observations, profile["id"], compatible, now,
                profile.get("event_timezone") or "US/Eastern",
            )
            if published else {}
        ),
        "delivery": delivery,
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
        "published": True,
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
    expected: Mapping[str, Mapping] | None = None,
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
    expected = expected or {}
    if not isinstance(expected, Mapping):
        raise ValueError("Publication expectations must be an object.")
    normalized_expected = {}
    for scope, value in expected.items():
        if re.fullmatch(r"profile:[1-9]\d*", scope or "") is None or not isinstance(value, Mapping):
            raise ValueError("Publication expectation is invalid.")
        if set(value) != {"revision", "xmltv_hash", "config_hash", "attempt_id"}:
            raise ValueError("Publication expectation fields are invalid.")
        normalized_expected[scope] = {
            "revision": _positive(value["revision"], "expected publication revision"),
            "xmltv_hash": _sha(value["xmltv_hash"], "expected publication hash"),
            "config_hash": _sha(value["config_hash"], "expected publication config hash"),
            "attempt_id": (
                None if value["attempt_id"] is None
                else _attempt(value["attempt_id"], "expected guide attempt ID")
            ),
        }

    session = get_session()
    try:
        old_rows = {row.scope: row for row in session.query(GuidePublication).all()}
        revisions = {scope: row.revision for scope, row in old_rows.items()}
        prior = {}
        corrupt = set()
        for scope, row in old_rows.items():
            try:
                prior[scope] = _read_row(row)
            except ValueError:
                corrupt.add(scope)
    finally:
        session.close()

    for scope, claim in normalized_expected.items():
        current = prior.get(scope)
        attempt = (
            current["state"]["delivery"].get("guide_attempt")
            if current is not None else None
        )
        if (
            current is None
            or current["revision"] != claim["revision"]
            or current["state"]["xmltv_hash"] != claim["xmltv_hash"]
            or current["state"]["config_hash"] != claim["config_hash"]
            or (attempt or {}).get("attempt_id") != claim["attempt_id"]
        ):
            return _result(
                set(), set(), set(), {}, {}, superseded=True,
                reasons={"GUIDE_PUBLICATION_SUPERSEDED"},
            )

    candidates = {}
    published, retained, unavailable = set(), set(), set()
    documents, states, reasons = {}, {}, set()
    for profile in enabled:
        profile_id = profile["id"]
        scope = _scope(profile_id)
        readiness = profile_coverage.get(str(profile_id), profile_coverage.get(profile_id))
        ready = isinstance(readiness, Mapping) and readiness.get("can_publish") is True
        if isinstance(readiness, Mapping) and any(
            source.get("status") == "retained"
            for source in readiness.get("sources", ())
        ):
            ready = False
            reasons.add("GUIDE_SOURCES_PENDING")
        prior_state = prior.get(scope, {}).get("state")
        active_claim = bool(
            prior_state
            and prior_state.get("config_hash") != _config_hash(profile)
            and any(
                receipt.get("stage") not in TERMINAL_PENDING_STAGES
                for receipt in prior_state.get("delivery", {}).get("pending_channels", {}).values()
            )
        )
        if active_claim:
            ready = False
            reasons.add("GUIDE_CONFIG_CHANGED")
        if ready:
            try:
                without_gaps = set()
                if profile.get("epg_source_ids"):
                    assigned_channels = {
                        assignment.get("channel_id")
                        for assignment in profile.get("channel_assignments", [])
                        if assignment.get("channel_id") is not None
                    }
                    lifecycle_channels = {
                        assignment["channel_id"]
                        for assignment in _resolve_group_assignments(
                            profile.get("hide_empty_group_ids", []), channel_map,
                        )
                    }
                    for channel_id in assigned_channels & lifecycle_channels:
                        channel = channel_map[channel_id]
                        streams = channel.get("streams")
                        current_programme = False
                        for programme in profile.get("source_programmes", {}).get(channel_id, []):
                            try:
                                start, stop = programme_times(programme)
                            except ValueError:
                                continue
                            if start <= now < stop and not _placeholder(programme):
                                current_programme = True
                                break
                        if (
                            channel.get("hidden_from_output") is True
                            or (isinstance(streams, list) and not streams)
                            or not current_programme
                        ):
                            without_gaps.add(channel_id)
                document = generate_xmltv(
                    [profile], channel_map, without_gaps=without_gaps,
                )
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
        if scope in prior and prior[scope]["state"].get("published") is True:
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
        if (
            retained_row is None
            or retained_row["state"].get("published") is not True
            or retained_row["state"].get("config_hash") != _config_hash(profile)
        ):
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
        for scope, claim in normalized_expected.items():
            current_revision = session.query(GuidePublication.revision).filter(
                GuidePublication.scope == scope
            ).scalar()
            if current_revision != claim["revision"]:
                session.rollback()
                return _result(
                    set(), retained | published, unavailable, documents, states,
                    superseded=True,
                    reasons=reasons | {"GUIDE_PUBLICATION_SUPERSEDED"},
                )
        for scope, candidate in sorted(candidates.items()):
            values = {
                "xmltv": candidate["xmltv"],
                "state": _state_text(candidate["state"]),
            }
            if scope in revisions:
                updated = (
                    session.query(GuidePublication)
                    .filter(
                        GuidePublication.scope == scope,
                        GuidePublication.revision == revisions[scope],
                    )
                    .update({**values, "revision": revisions[scope] + 1}, synchronize_session=False)
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
            removable = [
                scope for scope in old_rows
                if scope not in keep
                and scope not in corrupt
                and not prior.get(scope, {}).get("state", {}).get("delivery", {}).get(
                    "pending_channels", {}
                )
            ]
            if removable:
                session.query(GuidePublication).filter(
                    GuidePublication.scope.in_(removable)
                ).delete(synchronize_session=False)
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


def _source_hash_list(values) -> list[dict]:
    if not isinstance(values, list) or len(values) > MAX_CHANNELS:
        raise ValueError("Pending source hashes are invalid.")
    result = []
    for value in values:
        if not isinstance(value, Mapping) or set(value) != {"endpoint_hash", "source_url_hash"}:
            raise ValueError("Pending source hash fields are invalid.")
        result.append({
            "endpoint_hash": _sha(value["endpoint_hash"], "pending endpoint hash"),
            "source_url_hash": _sha(value["source_url_hash"], "pending source URL hash"),
        })
    return result


def _input_hash(config_hash: str, rule_hash: str, streams: list[dict], source_hashes: list[dict]) -> str:
    value = {
        "config_hash": config_hash,
        "rule_hash": rule_hash,
        "streams": sorted(
            (
                stream["id"], stream["name"], stream["account_id"], stream["group_id"],
            )
            for stream in streams
        ),
        "source_hashes": sorted(
            (item["endpoint_hash"], item["source_url_hash"])
            for item in source_hashes
        ),
    }
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _candidate(event_key: str, value, profile_id: int, config_hash: str) -> dict:
    if not isinstance(value, Mapping):
        raise ValueError("Pending channel candidate is invalid.")
    required = {
        "event_key", "rule_id", "rule_hash", "profile_id", "target_group_id",
        "title", "start", "stop", "streams", "channel_name", "channel_id",
        "channel_uuid", "execution_id", "source_hashes", "owner_proven",
        "channel_exists", "health_playable",
    }
    if set(value) != required or value["event_key"] != event_key:
        raise ValueError("Pending channel candidate fields are invalid.")
    if value["owner_proven"] is not True or value["health_playable"] is not True:
        raise ValueError("Pending channel candidate lacks current ownership or health proof.")
    if not isinstance(value["channel_exists"], bool):
        raise ValueError("Pending channel candidate existence proof is invalid.")
    if _positive(value["profile_id"], "candidate profile ID") != profile_id:
        raise ValueError("Pending channel candidate has the wrong profile.")
    start = _utc(value["start"], "candidate event start")
    stop = _utc(value["stop"], "candidate event stop")
    if stop <= start:
        raise ValueError("Pending channel candidate interval is invalid.")
    streams = _streams(value["streams"])
    rule_hash = _sha(value["rule_hash"], "candidate rule hash")
    source_hashes = _source_hash_list(value["source_hashes"])
    return {
        "event_key": event_key,
        "rule_id": _positive(value["rule_id"], "candidate rule ID"),
        "rule_hash": rule_hash,
        "config_hash": config_hash,
        "profile_id": profile_id,
        "target_group_id": _positive(value["target_group_id"], "candidate target group ID"),
        "title": _text(value["title"], "candidate title"),
        "start": start.isoformat(),
        "stop": stop.isoformat(),
        "streams": streams,
        "channel_name": _text(value["channel_name"], "candidate channel name", limit=1024),
        "channel_id": _positive(value["channel_id"], "candidate channel ID", optional=True),
        "channel_uuid": _text(value["channel_uuid"], "candidate channel UUID", optional=True, limit=512),
        "execution_id": _text(value["execution_id"], "candidate execution ID", limit=512),
        "source_hashes": source_hashes,
        "channel_exists": value["channel_exists"],
        "input_hash": _input_hash(config_hash, rule_hash, streams, source_hashes),
    }


def _terminal_snapshot(receipt: dict, revision: int, xmltv_hash: str) -> dict:
    value = {
        key: copy.deepcopy(receipt[key])
        for key in (
            "attempt_id", "attempt_no", "input_hash", "admitted_at", "expires_at",
            "terminal_at", "retry_at", "stage", "reason", "execution_id", "channel_id",
            "channel_uuid", "guide_attempt_id", "rule_hash", "config_hash",
        )
    }
    value["revision"] = revision
    value["xmltv_hash"] = xmltv_hash
    if "detail" in receipt:
        value["detail"] = receipt["detail"]
    return _history_entry(value)


def _close_expired(delivery: dict, now: datetime, revision: int, xmltv_hash: str) -> None:
    guide_attempt = delivery.get("guide_attempt")
    if guide_attempt is None or guide_attempt["stage"] in TERMINAL_GUIDE_STAGES:
        return
    guide_expired = (
        guide_attempt["expires_at"] is not None
        and _utc(guide_attempt["expires_at"], "guide attempt expiry") <= now
    )
    if guide_expired:
        guide_attempt["stage"] = "expired"
    for event_key, receipt in list(delivery["pending_channels"].items()):
        if (
            receipt["stage"] in TERMINAL_PENDING_STAGES
            or (
                not guide_expired
                and _utc(receipt["expires_at"], "pending channel expiry") > now
            )
        ):
            continue
        next_receipt = copy.deepcopy(receipt)
        if next_receipt["stage"] == "allocating" and next_receipt["channel_id"] is None:
            next_receipt["stage"] = "allocation_unknown"
            next_receipt["reason"] = "allocation_unknown"
            next_receipt["retry_at"] = None
        else:
            next_receipt["stage"] = "expired"
            next_receipt["reason"] = "guide_expired"
            next_receipt["retry_at"] = (
                (now + timedelta(minutes=5)).isoformat()
                if next_receipt["channel_id"] is not None else None
            )
        next_receipt["terminal_at"] = now.isoformat()
        delivery["pending_channels"][event_key] = _pending_channel(event_key, next_receipt)


def _admit_pending(
    event_key: str,
    candidate: dict,
    existing: dict | None,
    guide_attempt: dict,
    now: datetime,
    revision: int,
    xmltv_hash: str,
) -> dict | None:
    start = _utc(candidate["start"], "candidate event start")
    stop = _utc(candidate["stop"], "candidate event stop")
    if not start <= now < stop:
        return None
    expires_at = stop
    if expires_at <= now:
        return None
    if existing is None:
        receipt = {
            **{key: copy.deepcopy(candidate[key]) for key in (
                "event_key", "rule_id", "rule_hash", "config_hash", "profile_id",
                "target_group_id", "title", "start", "stop", "streams", "channel_name",
                "channel_id", "channel_uuid", "execution_id", "input_hash",
            )},
            "stage": "allocated" if candidate["channel_id"] is not None else "intent",
            "reason": "guide_pending",
            "attempt_id": uuid4().hex,
            "attempt_no": 1,
            "admitted_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
            "terminal_at": None,
            "retry_at": None,
            "guide_attempt_id": guide_attempt["attempt_id"],
            "history": [],
        }
        return _pending_channel(event_key, receipt)
    if existing["stage"] not in TERMINAL_PENDING_STAGES:
        comparison_hash = _input_hash(
            existing["config_hash"], candidate["rule_hash"],
            candidate["streams"], candidate["source_hashes"],
        )
        if (
            existing["event_key"] != candidate["event_key"]
            or existing["rule_id"] != candidate["rule_id"]
            or existing["profile_id"] != candidate["profile_id"]
            or existing["target_group_id"] != candidate["target_group_id"]
            or existing["title"] != candidate["title"]
            or existing["start"] != candidate["start"]
            or existing["stop"] != candidate["stop"]
            or existing["channel_name"] != candidate["channel_name"]
            or existing["execution_id"] != candidate["execution_id"]
            or existing["guide_attempt_id"] != guide_attempt["attempt_id"]
            or existing["input_hash"] != comparison_hash
            or (
                existing["channel_id"] is not None
                and candidate["channel_id"] != existing["channel_id"]
            )
            or (
                existing["channel_uuid"] is not None
                and candidate["channel_uuid"] != existing["channel_uuid"]
            )
        ):
            return None
        return existing
    if (
        existing["stage"] not in {"failed", "expired"}
        or existing["reason"] not in RECOVERABLE_REASONS
        or existing["channel_id"] is None
        or candidate["channel_exists"] is not True
        or candidate["channel_id"] != existing["channel_id"]
        or candidate["rule_id"] != existing["rule_id"]
        or candidate["profile_id"] != existing["profile_id"]
        or candidate["target_group_id"] != existing["target_group_id"]
        or candidate["event_key"] != existing["event_key"]
        or candidate["start"] != existing["start"]
        or candidate["stop"] != existing["stop"]
        or (existing["channel_uuid"] is not None and candidate["channel_uuid"] != existing["channel_uuid"])
    ):
        return None
    health_failure = (
        existing["stage"] == "failed"
        and existing["reason"] in HEALTH_REASONS
    )
    same_health_input = (
        health_failure
        and existing["input_hash"] == candidate["input_hash"]
    )
    health_recovery = (
        same_health_input
        and existing["title"] == candidate["title"]
        and existing["channel_name"] == candidate["channel_name"]
        and existing["execution_id"] == candidate["execution_id"]
        and existing["channel_uuid"] is not None
        and candidate["channel_uuid"] == existing["channel_uuid"]
    )
    if same_health_input and not health_recovery:
        return None
    retry_at = _utc(existing["retry_at"], "pending channel retry time") if existing["retry_at"] else None
    if health_recovery:
        if retry_at is None or now < retry_at:
            return None
    else:
        if existing["attempt_no"] >= 3:
            return None
        attempted = [item["input_hash"] for item in existing["history"]]
        attempted.append(existing["input_hash"])
        matches = attempted.count(candidate["input_hash"])
        if matches >= 2 or (matches == 1 and (retry_at is None or now < retry_at)):
            return None
    history = [
        *copy.deepcopy(existing["history"]),
        _terminal_snapshot(existing, revision, xmltv_hash),
    ][-2:]
    receipt = {
        **{key: copy.deepcopy(candidate[key]) for key in (
            "event_key", "rule_id", "rule_hash", "config_hash", "profile_id",
            "target_group_id", "title", "streams", "channel_name", "input_hash",
        )},
        "start": existing["start"],
        "stop": existing["stop"],
        "channel_id": existing["channel_id"],
        "channel_uuid": existing["channel_uuid"] or candidate["channel_uuid"],
        "execution_id": existing["execution_id"],
        "stage": "allocated",
        "reason": "guide_pending",
        "attempt_id": uuid4().hex,
        "attempt_no": existing["attempt_no"] + 1,
        "admitted_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
        "terminal_at": None,
        "retry_at": None,
        "guide_attempt_id": guide_attempt["attempt_id"],
        "history": history,
    }
    return _pending_channel(event_key, receipt)


def begin_delivery(
    scope: str,
    *,
    expected_revision: int,
    expected_hash: str | None,
    profile: Mapping,
    now: datetime,
    pending_channels: Mapping[str, Mapping] | None = None,
    plan_only: bool = False,
) -> dict | None:
    """Admit or resume one guide attempt under revision protection."""
    if re.fullmatch(r"profile:[1-9]\d*", scope) is None:
        raise ValueError("Guide delivery requires a profile scope.")
    now = _utc(now, "delivery admission time")
    if (
        not isinstance(expected_revision, int)
        or isinstance(expected_revision, bool)
        or expected_revision < 0
    ):
        raise ValueError("Guide delivery revision is invalid.")
    absent_claim = expected_revision == 0 and expected_hash is None
    if absent_claim:
        normalized_hash = None
    elif expected_revision > 0 and expected_hash is not None:
        normalized_hash = _sha(expected_hash, "expected publication hash")
    else:
        raise ValueError("Guide delivery expectation is invalid.")
    if not isinstance(profile, Mapping):
        raise ValueError("Guide delivery profile is invalid.")
    profile_id = _positive(profile.get("id"), "guide delivery profile ID")
    if scope != _scope(profile_id) or profile.get("enabled", True) is not True:
        return None
    candidates = pending_channels or {}
    if not isinstance(candidates, Mapping) or len(candidates) > MAX_CHANNELS:
        raise ValueError("Pending channel candidates are invalid.")
    config_hash = _config_hash(dict(profile))

    session = get_session()
    try:
        row = session.query(GuidePublication).filter(GuidePublication.scope == scope).one_or_none()
        if absent_claim:
            if row is not None:
                return None
            document = _combine([])
            state = _profile_state(
                dict(profile), {}, document, {}, now, None, published=False,
            )
            revision = 1
        else:
            if row is None or row.revision != expected_revision:
                return None
            document = row.xmltv
            state = _read_row(row)["state"]
            if state["xmltv_hash"] != normalized_hash:
                return None
            revision = expected_revision
        delivery = copy.deepcopy(state["delivery"])
        original_delivery = copy.deepcopy(delivery)
        guide_attempt = delivery.get("guide_attempt")
        if (
            guide_attempt is not None
            and guide_attempt["stage"] not in TERMINAL_GUIDE_STAGES
            and guide_attempt["config_hash"] == config_hash
            and state["config_hash"] == config_hash
        ):
            guide_attempt["expires_at"] = None
            for progress in delivery["source_refreshes"].values():
                if progress["attempt_id"] == guide_attempt["attempt_id"]:
                    progress["expires_at"] = None
            for event_key, receipt in delivery["pending_channels"].items():
                if (
                    receipt["stage"] in TERMINAL_PENDING_STAGES
                    or receipt["guide_attempt_id"] != guide_attempt["attempt_id"]
                    or receipt["config_hash"] != config_hash
                    or receipt["channel_id"] is None
                    or receipt["channel_uuid"] is None
                    or _utc(receipt["stop"], "pending channel stop") <= now
                ):
                    continue
                if event_key in candidates:
                    candidate = _candidate(event_key, candidates[event_key], profile_id, config_hash)
                    if _admit_pending(
                        event_key, candidate, receipt, guide_attempt,
                        now, revision, state["xmltv_hash"],
                    ) is None:
                        continue
                receipt["expires_at"] = receipt["stop"]
        _close_expired(delivery, now, revision, state["xmltv_hash"])
        closed_delivery = copy.deepcopy(delivery)
        closed_state = copy.deepcopy(state)
        closed_state["delivery"] = copy.deepcopy(closed_delivery)
        state_changed = False
        if not absent_claim and state["config_hash"] != config_hash:
            if any(
                receipt["stage"] not in TERMINAL_PENDING_STAGES
                for receipt in delivery["pending_channels"].values()
            ):
                return None
            state["config_hash"] = config_hash
            state["observations"] = {}
            delivery["required_dispatcharr_hashes"] = {}
            delivery["confirmed_dispatcharr_hashes"] = {}
            delivery["source_refreshes"] = {}
            if (
                delivery.get("guide_attempt") is not None
                and delivery["guide_attempt"]["stage"] not in TERMINAL_GUIDE_STAGES
            ):
                delivery["guide_attempt"]["stage"] = "failed"
            state_changed = True
        guide_attempt = delivery.get("guide_attempt")
        if guide_attempt is None or guide_attempt["stage"] in TERMINAL_GUIDE_STAGES:
            guide_attempt = {
                "attempt_id": uuid4().hex,
                "config_hash": config_hash,
                "admitted_at": now.isoformat(),
                "expires_at": None,
                "stage": "preparing",
            }
            delivery["guide_attempt"] = _guide_attempt(guide_attempt)
            delivery["source_refreshes"] = {}
        elif guide_attempt["config_hash"] != config_hash:
            return None
        elif (
            guide_attempt["expires_at"] is not None
            and _utc(guide_attempt["expires_at"], "guide attempt expiry") <= now
        ):
            return None

        for event_key, value in sorted(candidates.items()):
            if not isinstance(event_key, str):
                raise ValueError("Pending channel event key is invalid.")
            candidate = _candidate(event_key, value, profile_id, config_hash)
            admitted = _admit_pending(
                event_key,
                candidate,
                delivery["pending_channels"].get(event_key),
                delivery["guide_attempt"],
                now,
                revision,
                state["xmltv_hash"],
            )
            if admitted is None:
                if (
                    not plan_only
                    and closed_delivery != original_delivery
                    and not absent_claim
                ):
                    _parse_state(_state_text(closed_state), document=document)
                    next_revision = expected_revision + 1
                    updated = (
                        session.query(GuidePublication)
                        .filter(
                            GuidePublication.scope == scope,
                            GuidePublication.revision == expected_revision,
                        )
                        .update({
                            "state": _state_text(closed_state),
                            "revision": next_revision,
                        }, synchronize_session=False)
                    )
                    if updated != 1:
                        session.rollback()
                        return None
                    session.commit()
                return None
            delivery["pending_channels"][event_key] = admitted
        if delivery == original_delivery and not state_changed:
            return _read_row(row)
        state["delivery"] = delivery
        _parse_state(_state_text(state), document=document)
        next_revision = revision if absent_claim else expected_revision + 1
        snapshot = {
            "scope": scope,
            "xmltv": document,
            "state": copy.deepcopy(state),
            "revision": next_revision,
        }
        if plan_only:
            return snapshot
        if absent_claim:
            session.add(GuidePublication(
                scope=scope,
                xmltv=document,
                state=_state_text(state),
                revision=revision,
            ))
        else:
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
    except IntegrityError:
        session.rollback()
        return None
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    return snapshot


def add_groups(
    scope: str,
    *,
    expected_revision: int,
    expected_hash: str,
    expected_config_hash: str,
    expected_attempt_id: str,
    expected_pending: Mapping[str, str],
    profile: Mapping,
    group_ids: Sequence[int],
    now: datetime,
) -> tuple[dict, dict] | None:
    """Append authorized guide groups while preserving admitted receipt evidence."""
    if re.fullmatch(r"profile:[1-9]\d*", scope) is None:
        raise ValueError("Guide group updates require a profile scope.")
    expected_revision = _positive(expected_revision, "expected publication revision")
    expected_hash = _sha(expected_hash, "expected publication hash")
    expected_config_hash = _sha(expected_config_hash, "expected publication config hash")
    expected_attempt_id = _attempt(expected_attempt_id, "expected guide attempt ID")
    now = _utc(now, "guide group update time")
    if not isinstance(profile, Mapping):
        raise ValueError("Guide group update profile is invalid.")
    profile_id = _positive(profile.get("id"), "guide group update profile ID")
    if scope != _scope(profile_id):
        raise ValueError("Guide group update scope does not match its profile.")
    if (
        not isinstance(group_ids, Sequence)
        or isinstance(group_ids, (str, bytes, bytearray))
        or len(group_ids) > MAX_CHANNELS
    ):
        raise ValueError("Guide group IDs are invalid.")
    normalized_groups = [
        _positive(group_id, "guide group ID") for group_id in group_ids
    ]
    if len(normalized_groups) != len(set(normalized_groups)):
        raise ValueError("Guide group IDs must be unique.")
    if not isinstance(expected_pending, Mapping) or len(expected_pending) > MAX_CHANNELS:
        raise ValueError("Expected pending attempts must be an object.")
    normalized_pending = {
        str(event_key): _attempt(attempt_id, "expected pending attempt ID")
        for event_key, attempt_id in expected_pending.items()
    }
    if _config_hash(dict(profile)) != expected_config_hash:
        return None

    session = get_session()
    try:
        profile_row = session.query(DummyEPGProfile).filter(
            DummyEPGProfile.id == profile_id
        ).one_or_none()
        publication_row = session.query(GuidePublication).filter(
            GuidePublication.scope == scope
        ).one_or_none()
        if profile_row is None or publication_row is None or profile_row.enabled is not True:
            return None
        raw_profile = {
            column.name: getattr(profile_row, column.name)
            for column in DummyEPGProfile.__table__.columns
        }
        raw_groups = raw_profile["channel_group_ids"]
        if raw_groups is None:
            stored_groups = []
        else:
            try:
                stored_groups = json.loads(raw_groups)
            except (TypeError, ValueError) as exc:
                raise ValueError("Stored guide group IDs are malformed.") from exc
        if (
            not isinstance(stored_groups, list)
            or len(stored_groups) > MAX_CHANNELS
            or any(
                not isinstance(group_id, int)
                or isinstance(group_id, bool)
                or group_id <= 0
                for group_id in stored_groups
            )
            or len(stored_groups) != len(set(stored_groups))
        ):
            raise ValueError("Stored guide group IDs are invalid.")
        saved_profile = profile_row.to_dict()
        if _config_hash(saved_profile) != expected_config_hash:
            return None
        publication = _read_row(publication_row)
        state = publication["state"]
        delivery = state["delivery"]
        attempt = delivery.get("guide_attempt")
        current_pending = {
            event_key: receipt["attempt_id"]
            for event_key, receipt in delivery["pending_channels"].items()
        }
        if (
            publication["revision"] != expected_revision
            or state["xmltv_hash"] != expected_hash
            or state["config_hash"] != expected_config_hash
            or attempt is None
            or attempt["attempt_id"] != expected_attempt_id
            or attempt["config_hash"] != expected_config_hash
            or attempt["stage"] in TERMINAL_GUIDE_STAGES
            or (
                attempt["expires_at"] is not None
                and _utc(attempt["expires_at"], "guide attempt expiry") <= now
            )
            or current_pending != normalized_pending
        ):
            return None
        missing = sorted(set(normalized_groups) - set(stored_groups))
        active = [
            receipt for receipt in delivery["pending_channels"].values()
            if receipt["stage"] not in TERMINAL_PENDING_STAGES
        ]
        if active:
            authorized = {
                receipt["target_group_id"]
                for receipt in active
                if receipt["channel_id"] is not None
                and receipt["guide_attempt_id"] == expected_attempt_id
                and _utc(receipt["expires_at"], "pending channel expiry") > now
            }
            if any(group_id not in authorized for group_id in missing):
                return None
        if not missing:
            return saved_profile, publication

        next_groups = [*stored_groups, *missing]
        next_profile = copy.deepcopy(saved_profile)
        next_profile["channel_group_ids"] = next_groups
        next_config_hash = _config_hash(next_profile)
        next_state = copy.deepcopy(state)
        next_state["config_hash"] = next_config_hash
        next_attempt = next_state["delivery"]["guide_attempt"]
        next_attempt["config_hash"] = next_config_hash
        next_attempt["stage"] = "preparing"
        next_state["delivery"]["required_dispatcharr_hashes"] = {}
        next_state["delivery"]["confirmed_dispatcharr_hashes"] = {}
        next_state["delivery"]["source_refreshes"] = {}
        _parse_state(_state_text(next_state), document=publication_row.xmltv)

        profile_query = session.query(DummyEPGProfile).filter(
            DummyEPGProfile.id == profile_id
        )
        for column in DummyEPGProfile.__table__.columns:
            value = raw_profile[column.name]
            profile_query = profile_query.filter(
                column.is_(None) if value is None else column == value
            )
        profile_updated = profile_query.update({
            DummyEPGProfile.channel_group_ids: json.dumps(next_groups),
        }, synchronize_session=False)
        publication_updated = (
            session.query(GuidePublication)
            .filter(
                GuidePublication.scope == scope,
                GuidePublication.revision == expected_revision,
            )
            .update({
                "state": _state_text(next_state),
                "revision": expected_revision + 1,
            }, synchronize_session=False)
        )
        if profile_updated != 1 or publication_updated != 1:
            session.rollback()
            return None
        session.flush()
        session.expire(profile_row)
        session.refresh(profile_row)
        committed_profile = profile_row.to_dict()
        session.commit()
        committed_publication = {
            "scope": scope,
            "xmltv": publication_row.xmltv,
            "state": next_state,
            "revision": expected_revision + 1,
        }
        return committed_profile, committed_publication
    except IntegrityError:
        session.rollback()
        return None
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


async def refresh_source(
    client, source, publications, *, expires_at,
    after_link=False, channel_map=None, wait=True, cancelled=None,
) -> bool:
    """Retain each source import phase until its own observed completion."""
    from tasks.dummy_epg_refresh import wait_for_epg_source_refresh
    from tasks.event_visibility import (
        _await_preparation, _generated_scope, _source_refresh_key,
        _stream_group_id,
    )

    def stopped():
        return cancelled is not None and cancelled()

    source_id = _positive(source.get("id"), "refresh source ID")
    source_name = source.get("name", f"Source {source_id}")
    if not publications:
        return bool(await _await_preparation(wait_for_epg_source_refresh(
            client, source_id, source_name, expires_at=expires_at,
            wait=wait, cancelled=cancelled,
        ), stopped))
    scope_kind = _generated_scope(source)
    selected = {
        profile_id: copy.deepcopy(record)
        for profile_id, record in publications.items()
        if scope_kind in {"all", f"profile:{profile_id}"}
    }
    if not selected:
        return False
    keys = {
        profile_id: _source_refresh_key(client, source, f"profile:{profile_id}")
        for profile_id in selected
    }
    running = {"fetching", "processing", "parsing", "loading", "pending", "running", "queued", "refreshing"}
    failed = {"error", "failed", "failure", "cancelled", "canceled"}

    async def read_source():
        current = await _await_preparation(client.get_epg_source(source_id), stopped)
        if stopped() or not isinstance(current, Mapping):
            return None
        if (
            current.get("id") != source_id
            or not isinstance(current.get("url"), str) or not current["url"]
            or current["url"] != source.get("url")
            or any(
                _source_refresh_key(client, source, f"profile:{profile_id}") != key
                for profile_id, key in keys.items()
            )
        ):
            return None
        return current

    def current_claims():
        if stopped():
            return None
        current = {}
        for profile_id, expected in selected.items():
            scope = f"profile:{profile_id}"
            record = read_publication(scope)
            if record is None:
                return None
            state = record["state"]
            attempt = state["delivery"].get("guide_attempt")
            expected_attempt = expected["state"]["delivery"].get("guide_attempt")
            if (
                attempt is None or expected_attempt is None
                or record["revision"] != expected["revision"]
                or state["xmltv_hash"] != expected["state"]["xmltv_hash"]
                or state["config_hash"] != expected["state"]["config_hash"]
                or attempt != expected_attempt
                or state["delivery"]["source_refreshes"] != expected["state"]["delivery"]["source_refreshes"]
                or (attempt["expires_at"] is not None and _utc(attempt["expires_at"], "source expiry") <= datetime.now(timezone.utc))
                or _source_refresh_key(client, source, scope) != keys[profile_id]
            ):
                return None
            if scope_kind == "all":
                aggregate = read_publication("all")
                if (
                    aggregate is None or aggregate["state"].get("published", True) is not True
                    or aggregate["state"]["members"].get(str(profile_id)) != state["xmltv_hash"]
                ):
                    return None
            current[profile_id] = record
        return current

    def bindings(record, channels):
        members = record["state"]["channels"]
        if len(members) > MAX_CHANNELS:
            return None
        result = {}
        for member in members:
            channel_id = member["channel_id"]
            channel = channels.get(channel_id) if channels is not None else None
            if not isinstance(channel, Mapping) or channel.get("id") != channel_id:
                return None
            if "epg_data_id" in channel:
                link = channel["epg_data_id"]
            elif "epg_data" in channel:
                link = channel["epg_data"]
                if isinstance(link, Mapping):
                    link = link.get("id")
                    if link is None:
                        return None
            else:
                return None
            if link is not None:
                try:
                    result[str(channel_id)] = _positive(link, "source programme link")
                except ValueError:
                    return None
        return _links(result)

    async def read_bindings(record):
        fresh = {}
        for member in record["state"]["channels"]:
            channel_id = member["channel_id"]
            admitted = channel_map.get(channel_id) if channel_map is not None else None
            if not isinstance(admitted, Mapping):
                return None
            channel = await _await_preparation(client.get_channel(channel_id), stopped)
            if (
                stopped() or not isinstance(channel, Mapping)
                or channel.get("id") != channel_id
                or channel.get("uuid") != admitted.get("uuid")
                or _stream_group_id(channel) != _stream_group_id(admitted)
            ):
                return None
            fresh[channel_id] = channel
        return bindings(record, fresh)

    async def save(changes):
        if not changes:
            return current_claims() is not None
        async with publication_lock:
            if current_claims() is None:
                return False
            session = get_session() if len(changes) > 1 else None
            revisions = {}
            try:
                for profile_id, (progress, confirmed) in changes.items():
                    record = selected[profile_id]
                    state = record["state"]
                    refreshes = copy.deepcopy(state["delivery"]["source_refreshes"])
                    refreshes[keys[profile_id][0]] = progress
                    required = dict(state["delivery"]["required_dispatcharr_hashes"])
                    required[str(source_id)] = state["xmltv_hash"]
                    claims = {
                        "expected_revision": record["revision"],
                        "expected_hash": state["xmltv_hash"],
                        "expected_config_hash": state["config_hash"],
                        "expected_attempt_id": state["delivery"]["guide_attempt"]["attempt_id"],
                        "required_dispatcharr_hashes": required,
                        "confirmed_dispatcharr_hashes": confirmed,
                        "source_refreshes": refreshes,
                    }
                    if session is not None:
                        claims["session"] = session
                    revision = update_delivery(f"profile:{profile_id}", **claims)
                    if revision is None:
                        if session is not None:
                            session.rollback()
                        return False
                    revisions[profile_id] = revision
                if session is not None:
                    session.commit()
            except BaseException:
                if session is not None:
                    session.rollback()
                raise
            finally:
                if session is not None:
                    session.close()
            for profile_id, revision in revisions.items():
                record = read_publication(f"profile:{profile_id}")
                if record is None or record["revision"] != revision:
                    return False
                selected[profile_id] = copy.deepcopy(record)
                publications[profile_id] = record
            return True

    actual = await read_source()
    if actual is None or current_claims() is None:
        return False
    changes = {}
    for profile_id, record in selected.items():
        state = record["state"]
        delivery = state["delivery"]
        attempt = delivery["guide_attempt"]
        source_key, endpoint_hash, source_url_hash = keys[profile_id]
        if any(
            key != source_key and value["source_id"] == source_id
            for key, value in delivery["source_refreshes"].items()
        ):
            return False
        requested = bindings(record, channel_map) if after_link else None
        if after_link and requested is None:
            progress = delivery["source_refreshes"].get(source_key)
            if progress is not None:
                confirmed = dict(delivery["confirmed_dispatcharr_hashes"])
                confirmed.pop(str(source_id), None)
                await save({profile_id: (copy.deepcopy(progress), confirmed)})
            return False
        progress = copy.deepcopy(delivery["source_refreshes"].get(source_key))
        confirmed = dict(delivery["confirmed_dispatcharr_hashes"])
        if progress is None:
            progress = {
                "source_id": source_id, "endpoint_hash": endpoint_hash,
                "source_url_hash": source_url_hash, "expected_hash": state["xmltv_hash"],
                "attempt_id": attempt["attempt_id"], "expires_at": attempt["expires_at"],
                "initial_updated": actual.get("updated_at") or actual.get("last_updated"),
                "triggered": False, "observed_running": False,
                "links": requested, "pending_links": None, "completed": False,
            }
        elif after_link:
            progress["pending_links"] = requested if requested != progress["links"] else None
        if not progress["completed"] or progress["links"] is None or progress["pending_links"] is not None:
            confirmed.pop(str(source_id), None)
        if progress != delivery["source_refreshes"].get(source_key) or confirmed != delivery["confirmed_dispatcharr_hashes"]:
            changes[profile_id] = (progress, confirmed)
    if not await save(changes):
        return False

    while not stopped():
        if current_claims() is None:
            return False
        phases = {
            profile_id: copy.deepcopy(record["state"]["delivery"]["source_refreshes"][keys[profile_id][0]])
            for profile_id, record in selected.items()
        }
        changes = {}
        for profile_id, progress in phases.items():
            confirmed = dict(selected[profile_id]["state"]["delivery"]["confirmed_dispatcharr_hashes"])
            if progress["completed"] and progress["pending_links"] is not None:
                progress.update(links=progress["pending_links"], pending_links=None,
                                completed=False, triggered=False, observed_running=False)
                confirmed.pop(str(source_id), None)
                changes[profile_id] = (progress, confirmed)
        if changes:
            if not await save(changes):
                return False
            continue

        active = {key: phase for key, phase in phases.items() if phase["triggered"] and not phase["completed"]}
        if active:
            for profile_id, progress in active.items():
                lifetime = min((
                    _utc(phase["expires_at"], "source expiry")
                    for phase in phases.values() if phase["expires_at"] is not None
                ), default=None)
                actual = await read_source()
                if actual is None or current_claims() is None:
                    return False
                observed = copy.deepcopy(progress)
                completed = await _await_preparation(wait_for_epg_source_refresh(
                    client, source_id, source_name, expires_at=lifetime,
                    initial_source=actual, trigger=False, progress=observed,
                    wait=wait, cancelled=cancelled,
                ), stopped)
                if stopped() or current_claims() is None:
                    return False
                actual = await read_source()
                if actual is None or current_claims() is None:
                    return False
                record = selected[profile_id]
                observed["completed"] = bool(completed)
                confirmed = dict(record["state"]["delivery"]["confirmed_dispatcharr_hashes"])
                if completed and observed["links"] is not None:
                    latest = await read_bindings(record)
                    if latest is None or current_claims() is None:
                        return False
                    if latest != observed["links"]:
                        observed["pending_links"] = latest
                terminal = str(actual.get("status") or "").strip().lower() in failed
                if not completed and terminal and observed["pending_links"] is not None:
                    observed.update(links=observed["pending_links"], pending_links=None,
                                    completed=False, triggered=False, observed_running=False)
                if observed["completed"] and observed["links"] is not None and observed["pending_links"] is None:
                    confirmed[str(source_id)] = record["state"]["xmltv_hash"]
                else:
                    confirmed.pop(str(source_id), None)
                if not await save({profile_id: (observed, confirmed)}):
                    return False
                if not completed and not (terminal and not observed["triggered"]):
                    return False
            continue

        untriggered = {key: phase for key, phase in phases.items() if not phase["triggered"]}
        if untriggered:
            actual = await read_source()
            if actual is None or current_claims() is None:
                return False
            if str(actual.get("status") or "").strip().lower() in running:
                finite = [
                    _utc(phase["expires_at"], "source expiry")
                    for phase in phases.values() if phase["expires_at"] is not None
                ]
                await _await_preparation(wait_for_epg_source_refresh(
                    client, source_id, source_name, expires_at=min(finite, default=None),
                    initial_source=actual, trigger=False, progress={}, wait=wait,
                    cancelled=cancelled,
                ), stopped)
                if not wait or stopped() or current_claims() is None:
                    return False
                actual = await read_source()
                if actual is None or str(actual.get("status") or "").strip().lower() in running:
                    return False
            changes = {}
            for profile_id, progress in untriggered.items():
                progress.update(
                    initial_updated=actual.get("updated_at") or actual.get("last_updated"),
                    observed_running=False, triggered=True,
                )
                confirmed = dict(selected[profile_id]["state"]["delivery"]["confirmed_dispatcharr_hashes"])
                confirmed.pop(str(source_id), None)
                changes[profile_id] = (progress, confirmed)
            if not await save(changes) or stopped():
                return False
            await _await_preparation(client.refresh_epg_source(source_id), stopped)
            if stopped():
                return False
            continue

        confirmations = {}
        for profile_id, progress in phases.items():
            if progress["links"] is not None:
                latest = await read_bindings(selected[profile_id])
                if current_claims() is None:
                    return False
                if latest is None:
                    confirmed = dict(selected[profile_id]["state"]["delivery"]["confirmed_dispatcharr_hashes"])
                    confirmed.pop(str(source_id), None)
                    await save({profile_id: (progress, confirmed)})
                    return False
                if latest != progress["links"]:
                    progress["pending_links"] = latest
                    confirmed = dict(selected[profile_id]["state"]["delivery"]["confirmed_dispatcharr_hashes"])
                    confirmed.pop(str(source_id), None)
                    if not await save({profile_id: (progress, confirmed)}):
                        return False
                    break
                confirmed = dict(selected[profile_id]["state"]["delivery"]["confirmed_dispatcharr_hashes"])
                if confirmed.get(str(source_id)) != progress["expected_hash"]:
                    confirmed[str(source_id)] = progress["expected_hash"]
                    confirmations[profile_id] = (progress, confirmed)
            elif after_link:
                return False
        else:
            return await save(confirmations)
    return False


def update_delivery(
    scope: str,
    *,
    expected_revision: int,
    expected_hash: str | None = None,
    expected_config_hash: str | None = None,
    expected_attempt_id: str | None = None,
    expected_pending: Mapping[str, str] | None = None,
    required_dispatcharr_hashes: Mapping | None = None,
    confirmed_dispatcharr_hashes: Mapping | None = None,
    pending_emby: bool | None = None,
    guide_attempt: Mapping | None = None,
    source_refreshes: Mapping | None = None,
    pending_channels: Mapping | None = None,
    session=None,
) -> int | None:
    """Update bounded delivery progress only when the publication still wins."""
    owned_session = session is None
    if owned_session:
        session = get_session()
    try:
        row = session.query(GuidePublication).filter(GuidePublication.scope == scope).one_or_none()
        if row is None or row.revision != expected_revision:
            return None
        state = _read_row(row)["state"]
        if expected_hash is not None and state["xmltv_hash"] != _sha(
            expected_hash, "expected publication hash"
        ):
            return None
        if expected_config_hash is not None and state["config_hash"] != _sha(
            expected_config_hash, "expected publication config hash"
        ):
            return None
        delivery = copy.deepcopy(state["delivery"])
        current_attempt = delivery.get("guide_attempt")
        attempt_write = guide_attempt is not None or source_refreshes is not None
        if expected_attempt_id is not None:
            expected_attempt_id = _attempt(expected_attempt_id, "expected guide attempt ID")
            if current_attempt is None or current_attempt["attempt_id"] != expected_attempt_id:
                return None
        elif attempt_write:
            raise ValueError("Guide attempt writes require the expected attempt ID.")
        expected_pending = expected_pending or {}
        if not isinstance(expected_pending, Mapping):
            raise ValueError("Expected pending attempts must be an object.")
        for event_key, attempt_id in expected_pending.items():
            current_receipt = delivery["pending_channels"].get(event_key)
            if (
                current_receipt is None
                or current_receipt["attempt_id"] != _attempt(
                    attempt_id, "expected pending attempt ID",
                )
            ):
                return None
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
            if not isinstance(pending_emby, bool):
                raise ValueError("Pending Emby state must be a boolean.")
            delivery["pending_emby"] = pending_emby
        if guide_attempt is not None:
            normalized_attempt = _guide_attempt(guide_attempt)
            if normalized_attempt is None or normalized_attempt["attempt_id"] != expected_attempt_id:
                raise ValueError("Guide attempt update cannot replace its identity.")
            if current_attempt is None or any(
                normalized_attempt[name] != current_attempt[name]
                for name in ("config_hash", "admitted_at", "expires_at")
            ):
                raise ValueError("Guide attempt admission is immutable.")
            if (
                current_attempt["stage"] in TERMINAL_GUIDE_STAGES
                and normalized_attempt != current_attempt
            ):
                raise ValueError("Terminal guide attempt is immutable.")
            delivery["guide_attempt"] = normalized_attempt
        if source_refreshes is not None:
            delivery["source_refreshes"] = _source_refreshes(
                source_refreshes,
                delivery["guide_attempt"],
                state["xmltv_hash"],
            )
        if pending_channels is not None:
            if not isinstance(pending_channels, Mapping) or len(pending_channels) > MAX_CHANNELS:
                raise ValueError("Pending channel updates must be a bounded object.")
            if set(pending_channels) != set(delivery["pending_channels"]):
                raise ValueError("Pending channel updates cannot add or remove receipts.")
            if set(expected_pending) != set(pending_channels):
                raise ValueError("Pending channel updates require every current attempt ID.")
            normalized_pending = {
                key: _pending_channel(key, value)
                for key, value in sorted(pending_channels.items())
            }
            for event_key, receipt in normalized_pending.items():
                current_receipt = delivery["pending_channels"][event_key]
                if receipt["attempt_id"] != current_receipt["attempt_id"]:
                    raise ValueError("Pending channel update cannot replace its attempt identity.")
                immutable = (
                    "event_key", "rule_id", "rule_hash", "config_hash", "profile_id",
                    "target_group_id", "title", "start", "stop", "streams",
                    "channel_name", "execution_id", "attempt_id", "attempt_no",
                    "input_hash", "admitted_at", "expires_at", "guide_attempt_id",
                    "history",
                )
                if any(receipt[name] != current_receipt[name] for name in immutable):
                    raise ValueError("Pending channel admission is immutable.")
                for name in ("channel_id", "channel_uuid"):
                    current_value = current_receipt[name]
                    if current_value is not None and receipt[name] != current_value:
                        raise ValueError("Pending channel identity is immutable.")
                    if current_value is None and receipt[name] is None:
                        continue
                if current_receipt["stage"] in TERMINAL_PENDING_STAGES:
                    if receipt != current_receipt:
                        raise ValueError("Terminal pending channel receipt is immutable.")
                    continue
                if receipt["terminal_at"] is not None and current_receipt["terminal_at"] is not None:
                    if receipt["terminal_at"] != current_receipt["terminal_at"]:
                        raise ValueError("Pending channel terminal time is immutable.")
                if receipt["stage"] in {"failed", "expired"}:
                    terminal_at = _utc(receipt["terminal_at"], "pending channel terminal time")
                    expected_retry = terminal_at + timedelta(minutes=5)
                    if receipt["channel_id"] is not None and receipt["reason"] in RECOVERABLE_REASONS:
                        if receipt["retry_at"] != expected_retry.isoformat():
                            raise ValueError("Recoverable pending channel retry time is invalid.")
                    elif receipt["retry_at"] is not None:
                        raise ValueError("Permanently held pending channel cannot have a retry time.")
                elif receipt["retry_at"] is not None:
                    raise ValueError("Nonrecoverable pending channel cannot have a retry time.")
            delivery["pending_channels"] = normalized_pending
        required = delivery["required_dispatcharr_hashes"]
        confirmed = delivery["confirmed_dispatcharr_hashes"]
        if any(value != state["xmltv_hash"] for value in required.values()):
            raise ValueError("Required delivery hash must match the current publication.")
        if any(required.get(key) != value for key, value in confirmed.items()):
            raise ValueError("Confirmed delivery hashes must match required hashes.")
        if delivery["guide_attempt"] is not None and delivery["guide_attempt"]["stage"] in TERMINAL_GUIDE_STAGES:
            if any(
                receipt["stage"] not in TERMINAL_PENDING_STAGES
                for receipt in delivery["pending_channels"].values()
                if receipt["guide_attempt_id"] == delivery["guide_attempt"]["attempt_id"]
            ):
                raise ValueError("Terminal guide attempt cannot retain active pending channels.")
        if delivery == state["delivery"]:
            return expected_revision
        state["delivery"] = delivery
        state = _parse_state(_state_text(state), document=row.xmltv)
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
            if owned_session:
                session.rollback()
            return None
        if owned_session:
            session.commit()
        return next_revision
    except Exception:
        if owned_session:
            session.rollback()
        raise
    finally:
        if owned_session:
            session.close()
