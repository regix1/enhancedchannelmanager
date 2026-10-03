"""Durable guide publication, retention, and compare-and-swap checks."""

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json

import pytest
from sqlalchemy.orm import sessionmaker

import database
from models import DummyEPGProfile, GuidePublication
from services.epg_publication import (
    _config_hash,
    add_groups,
    begin_delivery,
    publish_profiles,
    read_publication,
    update_delivery,
    update_observations,
)


NOW = datetime(2026, 9, 20, 18, 0, tzinfo=timezone.utc)
START = "2026-09-20T17:30:00+00:00"
STOP = "2026-09-20T20:30:00+00:00"


@pytest.fixture(autouse=True)
def publication_database(monkeypatch, test_engine):
    sessions = sessionmaker(
        autocommit=False, autoflush=False, bind=test_engine, expire_on_commit=False,
    )
    monkeypatch.setattr(database, "_SessionLocal", sessions)


def profile(profile_id=1):
    return {
        "id": profile_id,
        "name": f"Arena {profile_id}",
        "enabled": True,
        "name_source": "channel",
        "title_pattern": r"(?P<title>.+)",
        "title_template": "{title}",
        "description_template": "Live event",
        "program_duration": 180,
        "event_timezone": "UTC",
        "tvg_id_template": "arena-{channel_id}",
        "channel_assignments": [{"channel_id": profile_id}],
        "event_intervals": {
            profile_id: [{"start": START, "stop": STOP, "title": "Falcons vs Wolves"}],
        },
    }


def channel_map(*profile_ids):
    return {
        profile_id: {
            "id": profile_id,
            "name": "Falcons vs Wolves 5:30 PM",
            "channel_number": 100 + profile_id,
            "streams": [],
        }
        for profile_id in profile_ids
    }


def coverage(*profile_ids, ready=True):
    return {
        "profiles": {
            str(profile_id): {
                "profile_id": profile_id,
                "can_publish": ready,
                "reason_codes": [] if ready else ["GUIDE_SOURCES_PENDING"],
            }
            for profile_id in profile_ids
        },
    }


def pending_candidate(*, channel_id=1, stream_name="Falcons vs Wolves"):
    return {
        "event_key": "arena:falcons-wolves",
        "rule_id": 9,
        "rule_hash": "1" * 64,
        "profile_id": 1,
        "target_group_id": 4,
        "title": "Falcons vs Wolves",
        "start": (NOW - timedelta(minutes=30)).isoformat(),
        "stop": (NOW + timedelta(hours=2)).isoformat(),
        "streams": [{
            "id": 77,
            "name": stream_name,
            "account_id": 3,
            "group_id": 5,
        }],
        "channel_name": "Arena 1",
        "channel_id": channel_id,
        "channel_uuid": "channel-1" if channel_id is not None else None,
        "execution_id": "execution-1",
        "source_hashes": [{
            "endpoint_hash": "2" * 64,
            "source_url_hash": "3" * 64,
        }],
        "owner_proven": True,
        "channel_exists": channel_id is not None,
        "health_playable": True,
    }


def test_complete_profile_and_aggregate_survive_a_fresh_read():
    result = publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={}, now=NOW,
    )

    assert result.published_profile_ids == (1,)
    assert result.retained_profile_ids == ()
    assert result.unavailable_profile_ids == ()
    assert set(result.xmltv_by_scope) == {"all", "profile:1"}
    stored = read_publication("profile:1")
    assert stored is not None
    assert stored["xmltv"] == result.xmltv_by_scope["profile:1"]
    assert stored["state"]["published_at"] == NOW.isoformat()
    assert stored["state"]["channels"][0]["events"] == [{
        "start": START,
        "stop": STOP,
        "title": "Falcons vs Wolves",
    }]
    assert read_publication("all")["state"]["members"] == {
        "1": result.hashes_by_scope["profile:1"],
    }


def test_pending_refresh_retains_profile_and_aggregate_byte_for_byte():
    first = publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={}, now=NOW,
    )
    before = read_publication("all")

    second = publish_profiles(
        [profile()], channel_map(1), coverage(1, ready=False),
        observations={}, now=NOW.replace(hour=19),
    )

    after = read_publication("all")
    assert second.published_profile_ids == ()
    assert second.retained_profile_ids == (1,)
    assert second.xmltv_by_scope == first.xmltv_by_scope
    assert after == before


def test_new_pending_member_cannot_replace_complete_aggregate_with_a_partial_one():
    first = publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={}, now=NOW,
    )
    combined = {
        "profiles": {
            "1": {"profile_id": 1, "can_publish": True, "reason_codes": []},
            "2": {
                "profile_id": 2,
                "can_publish": False,
                "reason_codes": ["GUIDE_SOURCES_PENDING"],
            },
        },
    }

    result = publish_profiles(
        [profile(), profile(2)], channel_map(1, 2), combined,
        observations={}, now=NOW.replace(hour=19),
    )

    assert result.published_profile_ids == (1,)
    assert result.unavailable_profile_ids == (2,)
    assert result.xmltv_by_scope["all"] == first.xmltv_by_scope["all"]
    assert "GUIDE_AGGREGATE_RETAINED" in result.reason_codes


def test_corrupt_state_is_not_reported_as_a_missing_publication(test_session):
    document = '<?xml version="1.0"?><tv></tv>'
    test_session.add(GuidePublication(
        scope="all", xmltv=document, state="{}", revision=1,
    ))
    test_session.commit()

    with pytest.raises(ValueError, match="version"):
        read_publication("all")
    assert read_publication("profile:99") is None


def test_observation_and_delivery_updates_require_the_winning_revision():
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    revision = read_publication("profile:1")["revision"]
    observation = {
        "family": "arena",
        "slot": "1",
        "stream_id": 77,
        "normalized_name": "falcons vs wolves 5:30 pm",
        "start": START,
        "expires_at": STOP,
        "title": "Falcons vs Wolves",
        "matched_variant": "time only",
        "provisional": True,
    }

    next_revision = update_observations(
        1, [observation], expected_revision=revision,
    )
    assert next_revision == revision + 1
    assert update_observations(1, [], expected_revision=revision) is None

    guide_hash = hashlib.sha256(read_publication("profile:1")["xmltv"].encode()).hexdigest()
    final_revision = update_delivery(
        "profile:1",
        expected_revision=next_revision,
        required_dispatcharr_hashes={50: guide_hash},
        confirmed_dispatcharr_hashes={50: guide_hash},
        pending_emby=False,
    )
    assert final_revision == next_revision + 1
    stored = read_publication("profile:1")
    assert stored["state"]["delivery"] == {
        "required_dispatcharr_hashes": {"50": guide_hash},
        "confirmed_dispatcharr_hashes": {"50": guide_hash},
        "pending_emby": False,
        "guide_attempt": None,
        "source_refreshes": {},
        "pending_channels": {},
    }

    stale = copy.deepcopy(stored)
    publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={},
        now=NOW.replace(hour=19),
    )
    winner = read_publication("profile:1")
    before = copy.deepcopy(winner["state"]["delivery"])
    assert winner["revision"] > stale["revision"]
    assert update_delivery(
        "profile:1",
        expected_revision=stale["revision"],
        expected_hash=stale["state"]["xmltv_hash"],
        required_dispatcharr_hashes={51: stale["state"]["xmltv_hash"]},
        confirmed_dispatcharr_hashes={51: stale["state"]["xmltv_hash"]},
    ) is None
    assert read_publication("profile:1")["state"]["delivery"] == before


def test_read_rejects_xml_hash_mismatch(test_session):
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    row = test_session.query(GuidePublication).filter_by(scope="profile:1").one()
    state = json.loads(row.state)
    state["xmltv_hash"] = "0" * 64
    row.state = json.dumps(state)
    test_session.commit()

    with pytest.raises(ValueError, match="does not match"):
        read_publication("profile:1")


def test_first_observation_chooses_previous_date_and_same_name_keeps_it():
    event = {
        "family": "arena",
        "slot": "1",
        "stream_id": 77,
        "normalized_name": "falcons vs wolves 11 pm",
        "start": "2026-09-20T23:00:00+00:00",
        "expires_at": "2026-09-21T02:00:00+00:00",
        "title": "Falcons vs Wolves",
        "matched_variant": "time only",
        "provisional": True,
    }
    first_now = datetime(2026, 9, 20, 0, 30, tzinfo=timezone.utc)

    publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={1: [event]}, now=first_now,
    )
    stored = next(iter(read_publication("profile:1")["state"]["observations"].values()))
    assert stored["start"] == "2026-09-19T23:00:00+00:00"
    assert stored["expires_at"] == "2026-09-20T02:00:00+00:00"

    refreshed = {
        **event,
        "start": "2026-09-21T23:00:00+00:00",
        "expires_at": "2026-09-22T02:00:00+00:00",
    }
    publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={1: [refreshed]},
        now=datetime(2026, 9, 21, 0, 30, tzinfo=timezone.utc),
    )
    retained = next(iter(read_publication("profile:1")["state"]["observations"].values()))
    assert retained["start"] == stored["start"]
    assert retained["expires_at"] == stored["expires_at"]

    authoritative = {
        **refreshed,
        "start": "2026-09-20T00:15:00+00:00",
        "expires_at": "2026-09-20T01:45:00+00:00",
        "provisional": False,
    }
    publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={1: [authoritative]},
        now=datetime(2026, 9, 21, 0, 45, tzinfo=timezone.utc),
    )
    replaced = next(iter(read_publication("profile:1")["state"]["observations"].values()))
    assert replaced["start"] == authoritative["start"]
    assert replaced["expires_at"] == authoritative["expires_at"]
    assert replaced["provisional"] is False


def test_disabled_profile_leaves_rebuilt_aggregate_before_its_row_is_removed():
    publish_profiles(
        [profile(), profile(2)], channel_map(1, 2), coverage(1, 2),
        observations={}, now=NOW,
    )
    disabled = {**profile(2), "enabled": False}

    result = publish_profiles(
        [profile(), disabled], channel_map(1, 2), coverage(1, ready=False),
        observations={}, now=NOW.replace(hour=19),
    )

    assert result.retained_profile_ids == (1,)
    assert read_publication("all")["state"]["members"] == {
        "1": read_publication("profile:1")["state"]["xmltv_hash"],
    }
    assert read_publication("profile:2") is None


def test_reused_profile_id_does_not_inherit_observations_or_delivery():
    observation = {
        "family": "arena",
        "slot": "1",
        "stream_id": 77,
        "normalized_name": "falcons vs wolves 5:30 pm",
        "start": START,
        "expires_at": STOP,
        "title": "Falcons vs Wolves",
        "matched_variant": "time only",
        "provisional": True,
    }
    publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={1: [observation]}, now=NOW,
    )
    stored = read_publication("profile:1")
    guide_hash = stored["state"]["xmltv_hash"]
    update_delivery(
        "profile:1",
        expected_revision=stored["revision"],
        required_dispatcharr_hashes={50: guide_hash},
        confirmed_dispatcharr_hashes={50: guide_hash},
        pending_emby=False,
    )
    replacement = {
        **profile(),
        "name": "Replacement Arena",
        "title_template": "Replacement: {title}",
    }

    publish_profiles(
        [replacement], channel_map(1), coverage(1), observations={},
        now=NOW.replace(hour=19),
    )

    replaced = read_publication("profile:1")["state"]
    assert replaced["observations"] == {}
    assert replaced["delivery"] == {
        "required_dispatcharr_hashes": {},
        "confirmed_dispatcharr_hashes": {},
        "pending_emby": True,
        "guide_attempt": None,
        "source_refreshes": {},
        "pending_channels": {},
    }


def test_stale_publication_candidate_rolls_back_every_candidate(monkeypatch):
    import dummy_epg_engine

    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    aggregate_before = read_publication("all")
    original = dummy_epg_engine.generate_xmltv
    raced = False

    def render(*args, **kwargs):
        nonlocal raced
        document = original(*args, **kwargs)
        if not raced:
            raced = True
            session = database.get_session()
            try:
                row = session.query(GuidePublication).filter_by(scope="profile:1").one()
                row.revision += 1
                session.commit()
            finally:
                session.close()
        return document

    monkeypatch.setattr(dummy_epg_engine, "generate_xmltv", render)
    result = publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={},
        now=NOW.replace(hour=19),
    )

    assert result.superseded is True
    assert result.published_profile_ids == ()
    assert "GUIDE_PUBLICATION_SUPERSEDED" in result.reason_codes
    assert read_publication("all") == aggregate_before


def test_profile_capacity_failure_retains_the_last_complete_documents(monkeypatch):
    from services import epg_publication

    first = publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={}, now=NOW,
    )
    monkeypatch.setattr(epg_publication, "MAX_RETAINED", 32)

    result = publish_profiles(
        [profile()], channel_map(1), coverage(1), observations={},
        now=NOW.replace(hour=19),
    )

    assert result.published_profile_ids == ()
    assert result.retained_profile_ids == (1,)
    assert result.xmltv_by_scope == first.xmltv_by_scope
    assert "GUIDE_PUBLICATION_INVALID" in result.reason_codes


def test_delivery_attempt_reuses_expiry_and_bounds_successor_history():
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    stored = read_publication("profile:1")
    admitted = begin_delivery(
        "profile:1",
        expected_revision=stored["revision"],
        expected_hash=stored["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    )
    assert admitted is not None
    guide_attempt = admitted["state"]["delivery"]["guide_attempt"]
    receipt = admitted["state"]["delivery"]["pending_channels"]["arena:falcons-wolves"]
    assert guide_attempt["expires_at"] == (NOW + timedelta(hours=24)).isoformat()
    assert receipt["expires_at"] == (NOW + timedelta(hours=2)).isoformat()
    assert receipt["attempt_no"] == 1
    assert receipt["stage"] == "allocated"

    resumed = begin_delivery(
        "profile:1",
        expected_revision=admitted["revision"],
        expected_hash=admitted["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW + timedelta(minutes=1),
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    )
    assert resumed["revision"] == admitted["revision"]
    resumed_receipt = resumed["state"]["delivery"]["pending_channels"]["arena:falcons-wolves"]
    assert resumed_receipt["attempt_id"] == receipt["attempt_id"]
    assert resumed_receipt["expires_at"] == receipt["expires_at"]
    assert begin_delivery(
        "profile:1",
        expected_revision=resumed["revision"],
        expected_hash=resumed["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW + timedelta(minutes=1),
        pending_channels={
            "arena:falcons-wolves": pending_candidate(stream_name="Changed stream"),
        },
    ) is None
    assert read_publication("profile:1")["revision"] == resumed["revision"]

    terminal_at = NOW + timedelta(minutes=2)
    terminal = {
        **resumed_receipt,
        "stage": "failed",
        "reason": "guide_failed",
        "terminal_at": terminal_at.isoformat(),
        "retry_at": (terminal_at + timedelta(minutes=5)).isoformat(),
    }
    next_revision = update_delivery(
        "profile:1",
        expected_revision=resumed["revision"],
        expected_hash=resumed["state"]["xmltv_hash"],
        expected_config_hash=resumed["state"]["config_hash"],
        expected_pending={"arena:falcons-wolves": resumed_receipt["attempt_id"]},
        pending_channels={"arena:falcons-wolves": terminal},
    )
    failed = read_publication("profile:1")
    assert failed["revision"] == next_revision

    assert begin_delivery(
        "profile:1",
        expected_revision=failed["revision"],
        expected_hash=failed["state"]["xmltv_hash"],
        profile=profile(),
        now=terminal_at + timedelta(minutes=4),
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    ) is None

    successor = begin_delivery(
        "profile:1",
        expected_revision=failed["revision"],
        expected_hash=failed["state"]["xmltv_hash"],
        profile=profile(),
        now=terminal_at + timedelta(minutes=5),
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    )
    successor_receipt = successor["state"]["delivery"]["pending_channels"]["arena:falcons-wolves"]
    assert begin_delivery(
        "profile:1",
        expected_revision=failed["revision"],
        expected_hash=failed["state"]["xmltv_hash"],
        profile=profile(),
        now=terminal_at + timedelta(minutes=5),
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    ) is None
    assert successor_receipt["attempt_no"] == 2
    assert successor_receipt["channel_id"] == receipt["channel_id"]
    assert successor_receipt["attempt_id"] != receipt["attempt_id"]
    assert [item["attempt_id"] for item in successor_receipt["history"]] == [receipt["attempt_id"]]
    assert update_delivery(
        "profile:1",
        expected_revision=successor["revision"],
        expected_hash=successor["state"]["xmltv_hash"],
        expected_pending={"arena:falcons-wolves": receipt["attempt_id"]},
        pending_channels=successor["state"]["delivery"]["pending_channels"],
    ) is None
    second_terminal_at = terminal_at + timedelta(minutes=6)
    second_failed = {
        **successor_receipt,
        "stage": "failed",
        "reason": "guide_failed",
        "terminal_at": second_terminal_at.isoformat(),
        "retry_at": (second_terminal_at + timedelta(minutes=5)).isoformat(),
    }
    second_revision = update_delivery(
        "profile:1",
        expected_revision=successor["revision"],
        expected_hash=successor["state"]["xmltv_hash"],
        expected_pending={"arena:falcons-wolves": successor_receipt["attempt_id"]},
        pending_channels={"arena:falcons-wolves": second_failed},
    )
    second_stored = read_publication("profile:1")
    assert second_stored["revision"] == second_revision
    assert begin_delivery(
        "profile:1",
        expected_revision=second_revision,
        expected_hash=second_stored["state"]["xmltv_hash"],
        profile=profile(),
        now=second_terminal_at + timedelta(minutes=5),
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    ) is None

    third = begin_delivery(
        "profile:1",
        expected_revision=second_revision,
        expected_hash=second_stored["state"]["xmltv_hash"],
        profile=profile(),
        now=second_terminal_at + timedelta(minutes=5),
        pending_channels={
            "arena:falcons-wolves": pending_candidate(stream_name="Falcons vs Wolves HD"),
        },
    )
    third_receipt = third["state"]["delivery"]["pending_channels"]["arena:falcons-wolves"]
    assert third_receipt["attempt_no"] == 3
    third_terminal_at = second_terminal_at + timedelta(minutes=6)
    third_failed = {
        **third_receipt,
        "stage": "failed",
        "reason": "guide_failed",
        "terminal_at": third_terminal_at.isoformat(),
        "retry_at": (third_terminal_at + timedelta(minutes=5)).isoformat(),
    }
    third_revision = update_delivery(
        "profile:1",
        expected_revision=third["revision"],
        expected_pending={"arena:falcons-wolves": third_receipt["attempt_id"]},
        pending_channels={"arena:falcons-wolves": third_failed},
    )
    third_stored = read_publication("profile:1")
    assert third_stored["revision"] == third_revision
    assert begin_delivery(
        "profile:1",
        expected_revision=third_revision,
        expected_hash=third_stored["state"]["xmltv_hash"],
        profile=profile(),
        now=third_terminal_at + timedelta(minutes=5),
        pending_channels={
            "arena:falcons-wolves": pending_candidate(stream_name="Falcons vs Wolves UHD"),
        },
    ) is None


def test_absent_profile_admission_is_unpublished_and_loses_without_mutation():
    admitted = begin_delivery(
        "profile:1",
        expected_revision=0,
        expected_hash=None,
        profile=profile(),
        now=NOW,
    )

    assert admitted is not None
    assert admitted["revision"] == 1
    assert admitted["state"]["published"] is False
    assert admitted["state"]["members"] == {}
    assert admitted["state"]["channels"] == []
    assert admitted["state"]["observations"] == {}
    assert admitted["state"]["window_start"] == NOW.isoformat()
    assert admitted["state"]["window_stop"] == NOW.isoformat()
    assert admitted["state"]["delivery"]["pending_emby"] is False
    assert read_publication("all") is None

    before = copy.deepcopy(admitted)
    assert begin_delivery(
        "profile:1",
        expected_revision=0,
        expected_hash=None,
        profile=profile(),
        now=NOW + timedelta(minutes=1),
    ) is None
    assert read_publication("profile:1") == before


def test_plan_only_absent_admission_returns_snapshot_without_insert():
    planned = begin_delivery(
        "profile:1",
        expected_revision=0,
        expected_hash=None,
        profile=profile(),
        now=NOW,
        pending_channels={
            "arena:falcons-wolves": pending_candidate(channel_id=None),
        },
        plan_only=True,
    )

    assert planned is not None
    assert planned["revision"] == 1
    assert planned["state"]["delivery"]["pending_channels"][
        "arena:falcons-wolves"
    ]["stage"] == "intent"
    assert read_publication("profile:1") is None


def test_plan_only_current_admission_returns_snapshot_without_update():
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    before = read_publication("profile:1")

    planned = begin_delivery(
        "profile:1",
        expected_revision=before["revision"],
        expected_hash=before["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={"arena:falcons-wolves": pending_candidate()},
        plan_only=True,
    )

    assert planned["revision"] == before["revision"] + 1
    assert planned["state"]["delivery"]["pending_channels"][
        "arena:falcons-wolves"
    ]["stage"] == "allocated"
    assert read_publication("profile:1") == before


def test_plan_only_rejection_does_not_persist_expired_closure():
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    current = read_publication("profile:1")
    admitted = begin_delivery(
        "profile:1",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    )
    before = copy.deepcopy(admitted)
    receipt = before["state"]["delivery"]["pending_channels"][
        "arena:falcons-wolves"
    ]

    assert begin_delivery(
        "profile:1",
        expected_revision=before["revision"],
        expected_hash=before["state"]["xmltv_hash"],
        profile=profile(),
        now=datetime.fromisoformat(receipt["expires_at"]),
        pending_channels={"arena:falcons-wolves": pending_candidate()},
        plan_only=True,
    ) is None
    assert read_publication("profile:1") == before


def test_unpublished_member_cannot_replace_a_complete_aggregate():
    publish_profiles(
        [profile(2)], channel_map(2), coverage(2), observations={}, now=NOW,
    )
    aggregate = read_publication("all")
    begin_delivery(
        "profile:1", expected_revision=0, expected_hash=None,
        profile=profile(), now=NOW,
    )
    readiness = {
        "profiles": {
            "1": {"can_publish": False, "reason_codes": ["GUIDE_SOURCES_PENDING"]},
            "2": {"can_publish": False, "reason_codes": ["GUIDE_SOURCES_PENDING"]},
        },
    }

    result = publish_profiles(
        [profile(), profile(2)], channel_map(1, 2), readiness,
        observations={}, now=NOW + timedelta(minutes=1),
    )

    assert result.unavailable_profile_ids == (1,)
    assert result.retained_profile_ids == (2,)
    assert "profile:1" not in result.xmltv_by_scope
    assert read_publication("all") == aggregate


def test_legacy_state_without_published_flag_remains_complete(test_session):
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    row = test_session.query(GuidePublication).filter_by(scope="profile:1").one()
    state = json.loads(row.state)
    state.pop("published")
    row.state = json.dumps(state)
    test_session.commit()

    assert read_publication("profile:1")["state"]["published"] is True


def test_config_hash_ignores_transient_and_group_derived_assignments():
    grouped = {**profile(), "channel_group_ids": [7], "channel_map": {1: {"id": 1}}}
    reassigned = {
        **grouped,
        "channel_assignments": [{"channel_id": 99}],
        "channel_map": {99: {"id": 99}},
    }
    assignment_free = {**profile(), "channel_group_ids": [], "channel_assignments": []}
    missing_assignment = dict(assignment_free)
    missing_assignment.pop("channel_assignments")

    assert _config_hash(grouped) == _config_hash(reassigned)
    assert _config_hash(assignment_free) == _config_hash(missing_assignment)
    assert _config_hash(assignment_free) != _config_hash({
        **assignment_free,
        "channel_assignments": [{"channel_id": 2}],
    })


def test_config_change_closes_receipt_free_guide_without_erasing_its_identity():
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    current = read_publication("profile:1")
    admitted = begin_delivery(
        "profile:1",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
    )
    attempt = copy.deepcopy(admitted["state"]["delivery"]["guide_attempt"])
    changed = {**profile(), "name": "Changed Arena"}

    result = publish_profiles(
        [changed], channel_map(1), coverage(1),
        observations={}, now=NOW + timedelta(minutes=1),
    )

    assert result.published_profile_ids == (1,)
    stored_attempt = read_publication("profile:1")["state"]["delivery"]["guide_attempt"]
    assert stored_attempt["attempt_id"] == attempt["attempt_id"]
    assert stored_attempt["admitted_at"] == attempt["admitted_at"]
    assert stored_attempt["expires_at"] == attempt["expires_at"]
    assert stored_attempt["config_hash"] == attempt["config_hash"]
    assert stored_attempt["stage"] == "failed"


def test_group_add_updates_current_hash_and_preserves_receipt_admission(
    test_session, monkeypatch,
):
    stored_profile = DummyEPGProfile(
        name="Stored Arena",
        enabled=True,
        event_timezone="UTC",
        title_pattern=r"(?P<title>.+)",
        title_template="{title}",
        program_duration=180,
        tvg_id_template="arena-{channel_id}",
    )
    stored_profile.set_channel_group_ids([3])
    test_session.add(stored_profile)
    test_session.commit()
    test_session.refresh(stored_profile)
    saved = stored_profile.to_dict()
    prepared = {
        **saved,
        "channel_assignments": [{"channel_id": stored_profile.id}],
        "event_intervals": {
            stored_profile.id: [{"start": START, "stop": STOP, "title": "Falcons vs Wolves"}],
        },
    }
    publish_profiles(
        [prepared], channel_map(stored_profile.id), coverage(stored_profile.id),
        observations={}, now=NOW,
    )
    current = read_publication(f"profile:{stored_profile.id}")
    candidate = pending_candidate(channel_id=stored_profile.id)
    candidate["profile_id"] = stored_profile.id
    admitted = begin_delivery(
        f"profile:{stored_profile.id}",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=saved,
        now=NOW,
        pending_channels={candidate["event_key"]: candidate},
    )
    receipt = copy.deepcopy(
        admitted["state"]["delivery"]["pending_channels"][candidate["event_key"]]
    )
    attempt = admitted["state"]["delivery"]["guide_attempt"]
    stored_profile.description_template = "External edit"
    test_session.commit()
    before_publication = copy.deepcopy(read_publication(f"profile:{stored_profile.id}"))
    assert add_groups(
        f"profile:{stored_profile.id}",
        expected_revision=admitted["revision"],
        expected_hash=admitted["state"]["xmltv_hash"],
        expected_config_hash=admitted["state"]["config_hash"],
        expected_attempt_id=attempt["attempt_id"],
        expected_pending={candidate["event_key"]: receipt["attempt_id"]},
        profile=saved,
        group_ids=[4],
        now=NOW + timedelta(seconds=30),
    ) is None
    assert read_publication(f"profile:{stored_profile.id}") == before_publication
    test_session.refresh(stored_profile)
    stored_profile.description_template = saved["description_template"]
    test_session.commit()
    from sqlalchemy.orm import Query

    original_update = Query.update

    def lose_publication(query, *args, **kwargs):
        if query.column_descriptions[0].get("entity") is GuidePublication:
            return 0
        return original_update(query, *args, **kwargs)

    with monkeypatch.context() as patcher:
        patcher.setattr(Query, "update", lose_publication)
        assert add_groups(
            f"profile:{stored_profile.id}",
            expected_revision=admitted["revision"],
            expected_hash=admitted["state"]["xmltv_hash"],
            expected_config_hash=admitted["state"]["config_hash"],
            expected_attempt_id=attempt["attempt_id"],
            expected_pending={candidate["event_key"]: receipt["attempt_id"]},
            profile=saved,
            group_ids=[4],
            now=NOW + timedelta(seconds=45),
        ) is None
    test_session.expire_all()
    assert test_session.query(DummyEPGProfile).filter_by(
        id=stored_profile.id,
    ).one().get_channel_group_ids() == [3]
    assert read_publication(f"profile:{stored_profile.id}") == before_publication

    def lose_profile(query, *args, **kwargs):
        if query.column_descriptions[0].get("entity") is DummyEPGProfile:
            return 0
        return original_update(query, *args, **kwargs)

    with monkeypatch.context() as patcher:
        patcher.setattr(Query, "update", lose_profile)
        assert add_groups(
            f"profile:{stored_profile.id}",
            expected_revision=admitted["revision"],
            expected_hash=admitted["state"]["xmltv_hash"],
            expected_config_hash=admitted["state"]["config_hash"],
            expected_attempt_id=attempt["attempt_id"],
            expected_pending={candidate["event_key"]: receipt["attempt_id"]},
            profile=saved,
            group_ids=[4],
            now=NOW + timedelta(seconds=50),
        ) is None
    test_session.expire_all()
    assert test_session.query(DummyEPGProfile).filter_by(
        id=stored_profile.id,
    ).one().get_channel_group_ids() == [3]
    assert read_publication(f"profile:{stored_profile.id}") == before_publication

    changed = add_groups(
        f"profile:{stored_profile.id}",
        expected_revision=admitted["revision"],
        expected_hash=admitted["state"]["xmltv_hash"],
        expected_config_hash=admitted["state"]["config_hash"],
        expected_attempt_id=attempt["attempt_id"],
        expected_pending={candidate["event_key"]: receipt["attempt_id"]},
        profile=saved,
        group_ids=[4],
        now=NOW + timedelta(minutes=1),
    )

    assert changed is not None
    changed_profile, publication = changed
    assert changed_profile["channel_group_ids"] == [3, 4]
    assert publication["revision"] == admitted["revision"] + 1
    assert publication["state"]["config_hash"] == _config_hash(changed_profile)
    assert publication["state"]["delivery"]["guide_attempt"]["config_hash"] == _config_hash(changed_profile)
    assert publication["state"]["delivery"]["pending_channels"][candidate["event_key"]] == receipt
    assert receipt["config_hash"] == admitted["state"]["config_hash"]


def test_successful_terminal_receipt_has_no_failure_reason_and_is_idempotent():
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    current = read_publication("profile:1")
    admitted = begin_delivery(
        "profile:1",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    )
    receipt = admitted["state"]["delivery"]["pending_channels"]["arena:falcons-wolves"]
    for stage, reason in (
        ("complete", "guide_failed"),
        ("failed", None),
        ("expired", None),
        ("allocation_unknown", None),
    ):
        invalid = {
            **receipt,
            "stage": stage,
            "reason": reason,
            "terminal_at": (NOW + timedelta(seconds=30)).isoformat(),
            "retry_at": None,
        }
        with pytest.raises(ValueError):
            update_delivery(
                "profile:1",
                expected_revision=admitted["revision"],
                expected_pending={"arena:falcons-wolves": receipt["attempt_id"]},
                pending_channels={"arena:falcons-wolves": invalid},
            )
        assert read_publication("profile:1")["revision"] == admitted["revision"]
    completed = {
        **receipt,
        "stage": "complete",
        "reason": None,
        "terminal_at": (NOW + timedelta(minutes=1)).isoformat(),
        "retry_at": None,
    }
    revision = update_delivery(
        "profile:1",
        expected_revision=admitted["revision"],
        expected_hash=admitted["state"]["xmltv_hash"],
        expected_pending={"arena:falcons-wolves": receipt["attempt_id"]},
        pending_channels={"arena:falcons-wolves": completed},
    )
    stored = read_publication("profile:1")
    assert stored["revision"] == revision
    assert stored["state"]["delivery"]["pending_channels"]["arena:falcons-wolves"]["reason"] is None
    assert update_delivery(
        "profile:1",
        expected_revision=revision,
        expected_pending={"arena:falcons-wolves": receipt["attempt_id"]},
        pending_channels={"arena:falcons-wolves": completed},
    ) == revision
    changed = {**completed, "detail": "late rewrite"}
    with pytest.raises(ValueError, match="immutable"):
        update_delivery(
            "profile:1",
            expected_revision=revision,
            expected_pending={"arena:falcons-wolves": receipt["attempt_id"]},
            pending_channels={"arena:falcons-wolves": changed},
        )


def test_receipt_expiry_closes_once_before_the_guide_expiry():
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    current = read_publication("profile:1")
    admitted = begin_delivery(
        "profile:1",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    )
    receipt = admitted["state"]["delivery"]["pending_channels"]["arena:falcons-wolves"]
    receipt_expiry = datetime.fromisoformat(receipt["expires_at"])

    assert begin_delivery(
        "profile:1",
        expected_revision=admitted["revision"],
        expected_hash=admitted["state"]["xmltv_hash"],
        profile=profile(),
        now=receipt_expiry,
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    ) is None
    closed = read_publication("profile:1")
    closed_receipt = closed["state"]["delivery"]["pending_channels"]["arena:falcons-wolves"]
    assert closed_receipt["stage"] == "expired"
    assert closed_receipt["terminal_at"] == receipt_expiry.isoformat()
    assert closed_receipt["retry_at"] == (receipt_expiry + timedelta(minutes=5)).isoformat()
    assert closed["state"]["delivery"]["guide_attempt"]["stage"] == "preparing"

    assert begin_delivery(
        "profile:1",
        expected_revision=closed["revision"],
        expected_hash=closed["state"]["xmltv_hash"],
        profile=profile(),
        now=receipt_expiry + timedelta(minutes=1),
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    ) is None
    assert read_publication("profile:1")["revision"] == closed["revision"]
