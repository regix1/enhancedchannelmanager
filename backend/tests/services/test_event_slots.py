from types import SimpleNamespace

from services.event_slots import (
    _slot_key,
    classify_event_slot,
    event_config,
    validate_ownership,
)


def _config(**changes):
    config = {
        "secondary": [],
        "slot_patterns": [{
            "name": "event",
            "channel_pattern": r"Channel (?P<slot>\d+)",
            "fallback_pattern": r"Fallback (?P<slot>\d+)",
            "event_patterns": [r"Event (?P<slot>\d+) .*"],
            "bootstrap": True,
        }],
    }
    config.update(changes)
    return config


def test_event_config_keeps_missing_and_explicit_empty_distinct():
    legacy = event_config({
        "stream_match_group_ids": [7],
        "hide_empty_group_ids": [],
    })
    explicit = event_config({
        "event_sync_config": {},
        "stream_match_group_ids": [7],
        "hide_empty_group_ids": [],
    })
    assert legacy["secondary"] == [{"group_id": 7, "m3u_account_id": None}]
    assert legacy["assume_current_date"] is True
    assert legacy["use_default_patterns"] is True
    assert explicit["secondary"] == []
    assert explicit["assume_current_date"] is False
    assert explicit["use_default_patterns"] is False


def test_slot_key_uses_full_match_and_normalizes_leading_zeroes():
    config = event_config({"event_sync_config": _config()})
    assert _slot_key("Channel 007", config) == ("event", "7")
    assert _slot_key("prefix Channel 007", config) is None
    assert _slot_key("Fallback 007", config, role="fallback") == ("event", "7")


def test_slot_key_preserves_casefolded_text_slot():
    config = event_config({
        "event_sync_config": _config(slot_patterns=[{
            "name": "court",
            "channel_pattern": r"Court (?P<slot>[A-Za-z ]+)",
        }]),
    })
    assert _slot_key("Court Center Stage", config) == ("court", "center stage")


def test_classification_reports_disagreeing_event_expressions():
    config = event_config({
        "event_sync_config": _config(slot_patterns=[{
            "name": "event",
            "channel_pattern": r"Channel (?P<slot>\d+)",
            "event_patterns": [
                r"Event (?P<slot>\d+)-\d+",
                r"Event \d+-(?P<slot>\d+)",
            ],
        }]),
    })
    result = classify_event_slot("Event 1-2", config, role="event")
    assert result["family"] is None
    assert result["validation_issues"] == [
        "matching expressions disagree on the captured slot"
    ]


def test_enabled_profiles_and_rules_conflict_on_lifecycle_targets():
    profile = SimpleNamespace(
        id=1,
        name="Guide",
        enabled=True,
        get_hide_empty_group_ids=lambda: [20],
    )
    rule = SimpleNamespace(
        id=2,
        name="Events",
        enabled=True,
        get_event_sync_config=lambda: {
            "master_group_id": 20,
            "promote_unmatched": True,
            "promote_target_group_id": 30,
        },
    )
    second_profile = SimpleNamespace(
        id=3,
        name="Second Guide",
        enabled=True,
        get_hide_empty_group_ids=lambda: [30],
    )
    conflicts = validate_ownership([profile, second_profile], [rule])
    assert [conflict["group_id"] for conflict in conflicts] == [20, 30]
    assert all(conflict["code"] == "ownership_conflict" for conflict in conflicts)


def test_disabled_owners_and_matching_scopes_do_not_claim_groups():
    profile = {
        "id": 1,
        "name": "Guide",
        "enabled": False,
        "hide_empty_group_ids": [20],
        "event_sync_config": {"secondary": [{"group_id": 30, "m3u_account_id": 7}]},
    }
    rule = {
        "id": 2,
        "name": "Events",
        "enabled": True,
        "event_sync_config": {
            "master_group_id": 40,
            "secondary": [{"group_id": 20, "m3u_account_id": None}],
        },
    }
    assert validate_ownership([profile], [rule]) == []
