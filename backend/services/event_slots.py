"""Shared profile event-slot configuration and lifecycle ownership checks."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
import re
from typing import Any

import safe_regex


def _value(item: Any, name: str, default=None):
    if isinstance(item, Mapping):
        return item.get(name, default)
    return getattr(item, name, default)


def _legacy_config(group_ids: Iterable[int]) -> dict:
    ordered_ids = []
    for group_id in group_ids:
        if (
            isinstance(group_id, int)
            and not isinstance(group_id, bool)
            and group_id > 0
            and group_id not in ordered_ids
        ):
            ordered_ids.append(group_id)
    return {
        "secondary": [
            {"group_id": group_id, "m3u_account_id": None}
            for group_id in ordered_ids
        ],
        "assume_current_date": True,
        "use_default_patterns": True,
    }


def event_config(profile: Any) -> dict:
    """Return one profile's normalized effective event configuration."""
    if not isinstance(profile, Mapping) and hasattr(profile, "get_event_sync_config"):
        return profile.get_event_sync_config()

    raw = _value(profile, "event_sync_config")
    if raw is None:
        raw = _legacy_config(_value(profile, "stream_match_group_ids", []) or [])
    elif not isinstance(raw, dict):
        raise ValueError("event_sync_config must be an object")
    else:
        raw = {**raw}
        if "secondary" in raw:
            raw["secondary"] = [
                {**scope} if isinstance(scope, dict) else scope
                for scope in raw["secondary"]
            ]
        if "slot_patterns" in raw:
            raw["slot_patterns"] = [
                {**slot} if isinstance(slot, dict) else slot
                for slot in raw["slot_patterns"]
            ]

    from channel_pipeline_schema import validate_event_sync_config

    errors = validate_event_sync_config(
        raw,
        profile_group_ids=_value(profile, "hide_empty_group_ids", []) or [],
    )
    if errors:
        raise ValueError("; ".join(errors))
    return raw


def _normalized_slot(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value or len(value) > 128:
        return None
    if value.isdigit():
        return str(int(value))
    return value.casefold()


def _fullmatch(expression: str, name: str):
    from dummy_epg_engine import _js_to_python_named_groups

    try:
        compiled = safe_regex.compile(
            _js_to_python_named_groups(expression), flags=re.IGNORECASE,
        )
        return compiled.fullmatch(name.strip(), timeout=0.1)
    except (safe_regex.SafeRegexError, TimeoutError):
        return None


def _slot_matches(name: str | None, config: dict, role: str) -> list[tuple[str, str]]:
    if not isinstance(name, str) or role not in {"channel", "fallback", "event"}:
        return []
    matches = []
    for slot in config.get("slot_patterns", []):
        if role == "channel":
            expressions = [slot.get("channel_pattern")]
        elif role == "fallback":
            expressions = [slot.get("fallback_pattern")]
        else:
            expressions = slot.get("event_patterns", [])
        for expression in expressions:
            if not expression:
                continue
            match = _fullmatch(expression, name)
            if match is None:
                continue
            normalized = _normalized_slot(match.groupdict().get("slot"))
            if normalized is not None:
                matches.append((slot["name"], normalized))
    return matches


def _slot_key(
    name: str | None, config: dict, *, role: str = "channel",
) -> tuple[str, str] | None:
    """Return a configured family and normalized slot for one full match."""
    matches = _slot_matches(name, config, role)
    unique = list(dict.fromkeys(matches))
    return unique[0] if len(unique) == 1 else None


def classify_event_slot(
    name: str | None, config: dict, *, role: str,
) -> dict:
    """Classify one preview name without recording runtime observations."""
    matches = list(dict.fromkeys(_slot_matches(name, config, role)))
    issues = []
    if len(matches) > 1:
        issues.append("matching expressions disagree on the captured slot")
    match = matches[0] if len(matches) == 1 else None
    return {
        "family": match[0] if match else None,
        "slot": match[1] if match else None,
        "role": role if match else None,
        "validation_issues": issues,
    }


def _owner(kind: str, item: Any) -> dict:
    owner_id = _value(item, "id")
    name = _value(item, "name")
    return {
        "kind": kind,
        "id": owner_id,
        "name": name,
        "key": f"{kind}:{owner_id}" if owner_id is not None else f"{kind}:new:{name}",
    }


def validate_ownership(profiles: Iterable[Any], rules: Iterable[Any]) -> list[dict]:
    """Return every enabled lifecycle-target ownership conflict."""
    claims: dict[int, dict[str, dict]] = {}
    profile_claims: dict[int, tuple[str, set[int]]] = {}
    linked: dict[int, set[frozenset[str]]] = {}

    for profile in profiles:
        if not bool(_value(profile, "enabled", True)):
            continue
        if isinstance(profile, Mapping):
            group_ids = _value(profile, "hide_empty_group_ids", []) or []
        else:
            group_ids = profile.get_hide_empty_group_ids()
        owner = _owner("profile", profile)
        owned_groups = set()
        for group_id in group_ids:
            if isinstance(group_id, int) and not isinstance(group_id, bool) and group_id > 0:
                claims.setdefault(group_id, {})[owner["key"]] = owner
                owned_groups.add(group_id)
        profile_id = _value(profile, "id")
        if isinstance(profile_id, int) and not isinstance(profile_id, bool):
            profile_claims[profile_id] = (owner["key"], owned_groups)

    for rule in rules:
        if not bool(_value(rule, "enabled", True)):
            continue
        if isinstance(rule, Mapping):
            config = _value(rule, "event_sync_config")
        elif hasattr(rule, "get_event_sync_config"):
            config = rule.get_event_sync_config()
        else:
            config = None
        if not isinstance(config, dict):
            continue
        group_ids = [config.get("master_group_id")]
        if config.get("promote_unmatched"):
            group_ids.append(config.get("promote_target_group_id"))
        owner = _owner("rule", rule)
        profile_claim = profile_claims.get(config.get("dummy_epg_profile_id"))
        for group_id in group_ids:
            if isinstance(group_id, int) and not isinstance(group_id, bool) and group_id > 0:
                claims.setdefault(group_id, {})[owner["key"]] = owner
                if profile_claim and group_id in profile_claim[1]:
                    linked.setdefault(group_id, set()).add(frozenset((
                        profile_claim[0], owner["key"],
                    )))

    conflicts = []
    for group_id in sorted(claims):
        owners = list(claims[group_id].values())
        owner_keys = frozenset(owner["key"] for owner in owners)
        if len(owners) == 2 and owner_keys in linked.get(group_id, set()):
            continue
        if len(owners) > 1:
            conflicts.append({
                "code": "ownership_conflict",
                "group_id": group_id,
                "owners": owners,
            })
    return conflicts
