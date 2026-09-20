"""Durable guide publication, retention, and compare-and-swap checks."""

from datetime import datetime, timezone
import hashlib
import json

import pytest
from sqlalchemy.orm import sessionmaker

import database
from models import GuidePublication
from services.epg_publication import (
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
    }


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
