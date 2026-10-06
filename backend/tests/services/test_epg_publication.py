"""Durable guide publication, retention, and compare-and-swap checks."""

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import xml.etree.ElementTree as ET

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


def external_profile(*channel_ids):
    return {
        "id": 1,
        "name": "Live Events",
        "enabled": True,
        "name_source": "channel",
        "title_pattern": r"(?P<title>.+)",
        "title_template": "{title}",
        "description_template": "",
        "program_duration": 180,
        "event_timezone": "UTC",
        "tvg_id_template": "ecm-{channel_id}",
        "channel_assignments": [
            {"channel_id": channel_id, "channel_name": f"Event {channel_id}"}
            for channel_id in channel_ids
        ],
        "hide_empty_group_ids": [65],
        "epg_source_ids": [46],
        "guide_start": datetime(2026, 10, 3, 15, tzinfo=timezone.utc),
        "guide_stop": datetime(2026, 10, 5, 4, tzinfo=timezone.utc),
        "source_programmes": {channel_id: [] for channel_id in channel_ids},
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


def _fail_pending(publication, reason, terminal_at):
    event_key, receipt = next(iter(
        publication["state"]["delivery"]["pending_channels"].items()
    ))
    failed = {
        **receipt,
        "stage": "failed",
        "reason": reason,
        "terminal_at": terminal_at.isoformat(),
        "retry_at": (terminal_at + timedelta(minutes=5)).isoformat(),
    }
    revision = update_delivery(
        publication["scope"],
        expected_revision=publication["revision"],
        expected_hash=publication["state"]["xmltv_hash"],
        expected_pending={event_key: receipt["attempt_id"]},
        pending_channels={event_key: failed},
    )
    stored = read_publication(publication["scope"])
    assert stored["revision"] == revision
    return stored


@pytest.mark.parametrize("hidden", [True, False, None], ids=["hidden", "visible", "omitted"])
def test_reported_ended_assignment_keeps_header_without_unconfirmed_programme(hidden):
    now = datetime(2026, 10, 3, 18, 45, tzinfo=timezone.utc)
    reported = external_profile(5040)
    channels = {
        5040: {
            "id": 5040,
            "name": "Atlantic Sun Conference Norfolk St Vs. Bellarmine @ Sep 18 10:00 AM",
            "channel_number": 912,
            "channel_group_id": 65,
            "streams": [],
        },
    }
    if hidden is not None:
        channels[5040]["hidden_from_output"] = hidden

    result = publish_profiles(
        [reported], channels, coverage(1), observations={}, now=now,
    )
    document = ET.fromstring(result.xmltv_by_scope["profile:1"])

    assert document.find("channel").get("id") == "ecm-5040"
    assert document.find("channel/display-name").text == channels[5040]["name"]
    assert document.findall("programme") == []
    assert result.states_by_profile[1]["channels"][0]["channel_id"] == 5040
    assert channels[5040]["channel_number"] == 912
    assert channels[5040]["channel_group_id"] == 65
    assert channels[5040]["streams"] == []


def test_lifecycle_publication_suppresses_only_unconfirmed_gaps():
    now = datetime(2026, 10, 3, 18, 45, tzinfo=timezone.utc)
    start = datetime(2026, 10, 3, 15, tzinfo=timezone.utc)
    stop = datetime(2026, 10, 5, 4, tzinfo=timezone.utc)

    def source_programme(channel_id, begin, end, title, icon=None):
        programme = ET.Element("programme", {
            "channel": f"external-{channel_id}",
            "start": begin.strftime("%Y%m%d%H%M%S %z"),
            "stop": end.strftime("%Y%m%d%H%M%S %z"),
        })
        ET.SubElement(programme, "title").text = title
        if icon is not None:
            ET.SubElement(programme, "icon", src=icon)
        return programme

    prepared = external_profile(1, 2, 3, 4, 5, 6)
    prepared["source_programmes"] = {
        1: [source_programme(1, now - timedelta(minutes=15), now + timedelta(hours=1), "Current event")],
        2: [source_programme(
            2, now - timedelta(minutes=15), now + timedelta(hours=1), "Hidden current",
            "https://example.com/hidden-programme.jpg",
        )],
        3: [source_programme(3, now - timedelta(minutes=15), now + timedelta(hours=1), "Empty current")],
        4: [source_programme(4, start + timedelta(minutes=30), start + timedelta(hours=1), "Ended event")],
        5: [source_programme(5, now + timedelta(hours=8), now + timedelta(hours=10), "Future event")],
        6: [],
    }
    source_channel = ET.Element("channel", id="external-2")
    ET.SubElement(source_channel, "icon", src="https://example.com/hidden-channel.jpg")
    prepared["source_channels"] = {2: source_channel}
    channels = {
        1: {"id": 1, "name": "Current", "channel_number": 101, "channel_group_id": 65,
            "hidden_from_output": False, "streams": [{"id": 11}]},
        2: {"id": 2, "name": "Hidden", "channel_number": 102, "channel_group": {"id": 65},
            "hidden_from_output": True, "streams": [{"id": 12}]},
        3: {"id": 3, "name": "Empty", "channel_number": 103, "channel_group_id": 65,
            "hidden_from_output": False, "streams": []},
        4: {"id": 4, "name": "Ended", "channel_number": 104, "channel_group_id": 65,
            "hidden_from_output": False, "streams": [{"id": 14}]},
        5: {"id": 5, "name": "Future", "channel_number": 105, "channel_group_id": 65,
            "hidden_from_output": False, "streams": [{"id": 15}]},
        6: {"id": 6, "name": "Manual", "channel_number": 106, "channel_group_id": 66,
            "hidden_from_output": False, "streams": []},
    }

    result = publish_profiles(
        [prepared], channels, coverage(1), observations={}, now=now,
    )
    document = ET.fromstring(result.xmltv_by_scope["profile:1"])
    titles = {
        channel_id: [row.findtext("title") for row in document.findall(
            f"programme[@channel='ecm-{channel_id}']"
        )]
        for channel_id in channels
    }

    assert "Current event" in titles[1]
    assert "Programming unavailable" in titles[1]
    assert titles[2] == ["Hidden current"]
    assert titles[3] == ["Empty current"]
    assert titles[4] == ["Ended event"]
    assert titles[5] == ["Future event"]
    assert titles[6] == ["Programming unavailable"]
    assert document.find("channel[@id='ecm-2']/icon").get("src") == (
        "https://example.com/hidden-channel.jpg"
    )
    assert document.find("programme[@channel='ecm-2']/icon").get("src") == (
        "https://example.com/hidden-programme.jpg"
    )
    assert {row.get("id") for row in document.findall("channel")} == {
        f"ecm-{channel_id}" for channel_id in channels
    }
    assert stop == prepared["guide_stop"]


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


@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("elapsed", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
def test_retained_source_keeps_complete_publication_and_delivery(pending, elapsed, mixed):
    prepared = external_profile(1)
    prepared["guide_start"] = NOW - timedelta(hours=1)
    prepared["guide_stop"] = NOW + timedelta(hours=6)
    prepared["source_programmes"] = {1: [ET.fromstring(
        '<programme channel="external-1" start="20260920173000 +0000" '
        'stop="20260920203000 +0000"><title>Falcons vs Wolves</title>'
        '<icon src="https://example.com/event.jpg"/></programme>'
    )]}
    if mixed:
        prepared["epg_source_ids"] = [46, 47]
    channels = channel_map(1)
    channels[1].update(channel_group_id=65, hidden_from_output=True)
    ready = coverage(1)
    ready["profiles"]["1"]["sources"] = [
        {"source_id": source_id, "status": "ready"}
        for source_id in prepared["epg_source_ids"]
    ]
    publish_profiles(
        [prepared], channels, ready, now=NOW,
        observations={1: [{
            "family": "arena", "slot": "1", "stream_id": 77,
            "normalized_name": "falcons vs wolves", "title": "Falcons vs Wolves",
            "start": START, "expires_at": STOP, "provisional": False,
        }]},
    )
    current = read_publication("profile:1")
    update_delivery(
        "profile:1", expected_revision=current["revision"],
        required_dispatcharr_hashes={46: current["state"]["xmltv_hash"]},
        confirmed_dispatcharr_hashes={46: current["state"]["xmltv_hash"]},
        pending_emby=False,
    )
    if pending:
        current = read_publication("profile:1")
        candidate = pending_candidate()
        candidate.update(start=START, stop=STOP)
        admitted = begin_delivery(
            "profile:1", expected_revision=current["revision"],
            expected_hash=current["state"]["xmltv_hash"], profile=prepared,
            now=NOW, pending_channels={candidate["event_key"]: candidate},
        )
        assert admitted is not None
        assert admitted["state"]["delivery"]["pending_channels"][
            candidate["event_key"]
        ]["expires_at"] == STOP
    before = copy.deepcopy(read_publication("profile:1"))
    aggregate = copy.deepcopy(read_publication("all"))
    channels[1].update(hidden_from_output=False, streams=[{"id": 77}])
    ready["profiles"]["1"]["sources"][0]["status"] = "retained"
    refreshed_at = NOW + timedelta(hours=4) if elapsed else NOW + timedelta(minutes=1)

    result = publish_profiles(
        [prepared], channels, ready, observations={1: []}, now=refreshed_at,
    )

    assert result.published_profile_ids == ()
    assert result.retained_profile_ids == (1,)
    assert result.unavailable_profile_ids == ()
    assert "GUIDE_SOURCES_PENDING" in result.reason_codes
    assert result.xmltv_by_scope == {
        "profile:1": before["xmltv"], "all": aggregate["xmltv"],
    }
    assert result.states_by_profile[1] == before["state"]
    assert read_publication("profile:1") == before
    assert read_publication("all") == aggregate


def test_unselected_degraded_source_does_not_block_ready_profile():
    prepared = external_profile(1)
    prepared["guide_start"] = NOW - timedelta(hours=1)
    prepared["guide_stop"] = NOW + timedelta(hours=6)
    readiness = coverage(1)
    readiness["profiles"]["1"]["sources"] = [{"source_id": 46, "status": "ready"}]
    readiness["sources"] = [{"source_id": 99, "status": "retained"}]

    result = publish_profiles(
        [prepared], channel_map(1), readiness, observations={}, now=NOW,
    )

    assert result.published_profile_ids == (1,)
    assert result.retained_profile_ids == ()
    assert result.unavailable_profile_ids == ()
    assert read_publication("all")["state"]["members"] == {
        "1": result.hashes_by_scope["profile:1"],
    }


def test_retained_profile_keeps_its_row_beside_a_fresh_profile():
    prepared = external_profile(1)
    prepared["guide_start"] = NOW - timedelta(hours=1)
    prepared["guide_stop"] = NOW + timedelta(hours=6)
    publish_profiles(
        [prepared, profile(2)], channel_map(1, 2), coverage(1, 2),
        observations={}, now=NOW,
    )
    retained = copy.deepcopy(read_publication("profile:1"))
    aggregate = read_publication("all")
    changed = profile(2)
    changed["event_intervals"][2][0]["title"] = "New sibling event"
    readiness = coverage(1, 2)
    readiness["profiles"]["1"]["sources"] = [{"source_id": 46, "status": "retained"}]
    readiness["profiles"]["2"]["sources"] = []

    result = publish_profiles(
        [prepared, changed], channel_map(1, 2), readiness,
        observations={}, now=NOW + timedelta(minutes=1),
    )

    assert result.published_profile_ids == (2,)
    assert result.retained_profile_ids == (1,)
    assert result.unavailable_profile_ids == ()
    assert result.xmltv_by_scope["profile:1"] == retained["xmltv"]
    assert read_publication("profile:1") == retained
    assert read_publication("all")["revision"] == aggregate["revision"] + 1
    assert read_publication("all")["state"]["members"] == {
        "1": retained["state"]["xmltv_hash"],
        "2": result.hashes_by_scope["profile:2"],
    }
    assert {row.get("id") for row in ET.fromstring(
        result.xmltv_by_scope["all"]
    ).findall("channel")} == {"ecm-1", "arena-2"}


@pytest.mark.parametrize("pending", [False, True])
def test_retained_source_cannot_create_first_publication(pending):
    prepared = external_profile(1)
    prepared["guide_start"] = NOW - timedelta(hours=1)
    prepared["guide_stop"] = NOW + timedelta(hours=6)
    if pending:
        admitted = begin_delivery(
            "profile:1", expected_revision=0, expected_hash=None,
            profile=prepared, now=NOW,
            pending_channels={"arena:falcons-wolves": pending_candidate()},
        )
        assert admitted is not None
        assert admitted["state"]["published"] is False
    before = copy.deepcopy(read_publication("profile:1"))
    readiness = coverage(1)
    readiness["profiles"]["1"]["sources"] = [{"source_id": 46, "status": "retained"}]

    result = publish_profiles(
        [prepared], channel_map(1), readiness,
        observations={}, now=NOW + timedelta(minutes=1),
    )

    assert result.published_profile_ids == ()
    assert result.retained_profile_ids == ()
    assert result.unavailable_profile_ids == (1,)
    assert result.xmltv_by_scope == {}
    assert result.states_by_profile == {}
    assert "GUIDE_SOURCES_PENDING" in result.reason_codes
    assert read_publication("profile:1") == before
    assert read_publication("all") is None


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


def test_slow_run_publishes_profiles_whose_guides_did_not_change():
    publish_profiles(
        [profile(1), profile(2)], channel_map(1, 2), coverage(1, 2), observations={}, now=NOW,
    )
    expected = {}
    for profile_id in (1, 2):
        current = read_publication(f"profile:{profile_id}")
        admitted = begin_delivery(
            f"profile:{profile_id}", expected_revision=current["revision"],
            expected_hash=current["state"]["xmltv_hash"], profile=profile(profile_id), now=NOW,
        )
        expected[f"profile:{profile_id}"] = {
            "revision": admitted["revision"],
            "xmltv_hash": admitted["state"]["xmltv_hash"],
            "config_hash": admitted["state"]["config_hash"],
            "attempt_id": admitted["state"]["delivery"]["guide_attempt"]["attempt_id"],
        }
    # While the slow run works, profile 1 only gets delivery bookkeeping and
    # profile 2 gets a newer guide from another run.
    current = read_publication("profile:1")
    assert update_delivery(
        "profile:1", expected_revision=current["revision"], pending_emby=False,
    ) == current["revision"] + 1
    newer = channel_map(1, 2)
    newer[2]["name"] = "Hawks vs Bears 6:30 PM"
    mixed = coverage(2)
    mixed["profiles"].update(coverage(1, ready=False)["profiles"])
    publish_profiles([profile(1), profile(2)], newer, mixed, observations={}, now=NOW)
    newer_two = read_publication("profile:2")["state"]["xmltv_hash"]
    assert newer_two != expected["profile:2"]["xmltv_hash"]

    final = channel_map(1, 2)
    final[1]["name"] = "Falcons vs Wolves 5:45 PM"
    result = publish_profiles(
        [profile(1), profile(2)], final, coverage(1, 2), observations={},
        now=NOW + timedelta(minutes=30), expected=expected,
    )

    assert result.superseded is False
    assert result.published_profile_ids == (1,)
    assert 2 in result.retained_profile_ids
    assert "GUIDE_PUBLICATION_SUPERSEDED" in result.reason_codes
    assert read_publication("profile:1")["state"]["xmltv_hash"] != expected["profile:1"]["xmltv_hash"]
    assert read_publication("profile:2")["state"]["xmltv_hash"] == newer_two


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
    assert guide_attempt["expires_at"] is None
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


@pytest.mark.parametrize("reason", ["health_failed", "health_unknown", "programme_missing"])
def test_health_recovery_keeps_latest_attempts(reason):
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
    receipt = admitted["state"]["delivery"]["pending_channels"][
        "arena:falcons-wolves"
    ]
    allocation = (
        receipt["channel_id"],
        receipt["channel_uuid"],
        receipt["execution_id"],
        receipt["input_hash"],
    )
    attempt_ids = [receipt["attempt_id"]]
    snapshots = []
    snapshot_fields = (
        "attempt_id", "attempt_no", "input_hash", "admitted_at", "expires_at",
        "terminal_at", "retry_at", "stage", "reason", "execution_id",
        "channel_id", "channel_uuid", "guide_attempt_id", "rule_hash",
        "config_hash",
    )

    for attempt_no in range(2, 5):
        terminal_at = NOW + timedelta(minutes=attempt_no * 10)
        failed = _fail_pending(admitted, reason, terminal_at)
        failed_receipt = failed["state"]["delivery"]["pending_channels"][
            "arena:falcons-wolves"
        ]
        snapshot = {
            key: copy.deepcopy(failed_receipt[key])
            for key in snapshot_fields
        }
        snapshot["revision"] = failed["revision"]
        snapshot["xmltv_hash"] = failed["state"]["xmltv_hash"]
        snapshots.append(snapshot)

        admitted = begin_delivery(
            "profile:1",
            expected_revision=failed["revision"],
            expected_hash=failed["state"]["xmltv_hash"],
            profile=profile(),
            now=terminal_at + timedelta(minutes=5),
            pending_channels={"arena:falcons-wolves": pending_candidate()},
        )
        assert admitted is not None
        fresh = read_publication("profile:1")
        assert fresh == admitted
        receipt = fresh["state"]["delivery"]["pending_channels"][
            "arena:falcons-wolves"
        ]
        attempt_ids.append(receipt["attempt_id"])
        assert receipt["attempt_no"] == attempt_no
        assert receipt["history"] == snapshots[-2:]
        assert (
            receipt["channel_id"],
            receipt["channel_uuid"],
            receipt["execution_id"],
            receipt["input_hash"],
        ) == allocation
        assert len(attempt_ids) == len(set(attempt_ids))


@pytest.mark.parametrize("boundary", ["before", "at", "ended"])
def test_health_recovery_waits_for_retry(boundary):
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    current = read_publication("profile:1")
    candidate = pending_candidate()
    admitted = begin_delivery(
        "profile:1",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={candidate["event_key"]: candidate},
    )
    first = admitted["state"]["delivery"]["pending_channels"][candidate["event_key"]]
    terminal_at = NOW + timedelta(minutes=1)
    failed = _fail_pending(admitted, "health_failed", terminal_at)
    before = copy.deepcopy(failed)
    retry_at = terminal_at + timedelta(minutes=5)
    now = {
        "before": retry_at - timedelta(microseconds=1),
        "at": retry_at,
        "ended": datetime.fromisoformat(candidate["stop"]),
    }[boundary]

    successor = begin_delivery(
        "profile:1",
        expected_revision=failed["revision"],
        expected_hash=failed["state"]["xmltv_hash"],
        profile=profile(),
        now=now,
        pending_channels={candidate["event_key"]: candidate},
    )

    if boundary != "at":
        assert successor is None
        assert read_publication("profile:1") == before
        return
    receipt = successor["state"]["delivery"]["pending_channels"][candidate["event_key"]]
    assert receipt["attempt_no"] == 2
    assert receipt["admitted_at"] == retry_at.isoformat()
    assert receipt["expires_at"] == candidate["stop"]
    assert receipt["history"][0]["expires_at"] == first["expires_at"]
    assert receipt["history"][0]["terminal_at"] == terminal_at.isoformat()


def test_health_recovery_outlives_failed_attempt():
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    current = read_publication("profile:1")
    candidate = pending_candidate()
    candidate["stop"] = (NOW + timedelta(hours=30)).isoformat()
    admitted = begin_delivery(
        "profile:1",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={candidate["event_key"]: candidate},
    )
    first_attempt = admitted["state"]["delivery"]["guide_attempt"]
    first_receipt = admitted["state"]["delivery"]["pending_channels"][candidate["event_key"]]
    failed = _fail_pending(admitted, "health_failed", NOW + timedelta(minutes=1))
    failed_receipt = failed["state"]["delivery"]["pending_channels"][candidate["event_key"]]
    late = NOW + timedelta(hours=24, minutes=1)

    successor = begin_delivery(
        "profile:1",
        expected_revision=failed["revision"],
        expected_hash=failed["state"]["xmltv_hash"],
        profile=profile(),
        now=late,
        pending_channels={candidate["event_key"]: candidate},
    )
    assert successor is not None
    next_attempt = successor["state"]["delivery"]["guide_attempt"]
    next_receipt = successor["state"]["delivery"]["pending_channels"][candidate["event_key"]]
    assert next_attempt["attempt_id"] == first_attempt["attempt_id"]
    assert next_receipt["attempt_id"] != failed_receipt["attempt_id"]
    assert next_receipt["guide_attempt_id"] == next_attempt["attempt_id"]
    assert next_receipt["expires_at"] == candidate["stop"]
    assert next_receipt["history"][-1]["expires_at"] == first_receipt["expires_at"]
    assert next_receipt["history"][-1]["terminal_at"] == failed_receipt["terminal_at"]
    before_stale_write = copy.deepcopy(successor)
    assert update_delivery(
        "profile:1",
        expected_revision=successor["revision"],
        expected_pending={candidate["event_key"]: failed_receipt["attempt_id"]},
        pending_channels=successor["state"]["delivery"]["pending_channels"],
    ) is None
    assert read_publication("profile:1") == before_stale_write

    expired_at = late + timedelta(minutes=1)
    expired = {
        **next_receipt,
        "stage": "expired",
        "reason": "guide_expired",
        "terminal_at": expired_at.isoformat(),
        "retry_at": (expired_at + timedelta(minutes=5)).isoformat(),
    }
    expired_revision = update_delivery(
        "profile:1",
        expected_revision=successor["revision"],
        expected_pending={candidate["event_key"]: next_receipt["attempt_id"]},
        pending_channels={candidate["event_key"]: expired},
    )
    expired_publication = read_publication("profile:1")
    assert expired_publication["revision"] == expired_revision
    assert begin_delivery(
        "profile:1",
        expected_revision=expired_revision,
        expected_hash=expired_publication["state"]["xmltv_hash"],
        profile=profile(),
        now=expired_at + timedelta(minutes=5),
        pending_channels={candidate["event_key"]: candidate},
    ) is None
    assert read_publication("profile:1") == expired_publication

    second_profile = profile(2)
    publish_profiles(
        [second_profile], channel_map(2), coverage(2), observations={}, now=NOW,
    )
    second = read_publication("profile:2")
    ended_candidate = pending_candidate(channel_id=2)
    ended_candidate.update({
        "event_key": "arena:ended-event",
        "profile_id": 2,
        "stop": (NOW + timedelta(minutes=10)).isoformat(),
    })
    ended_admission = begin_delivery(
        "profile:2",
        expected_revision=second["revision"],
        expected_hash=second["state"]["xmltv_hash"],
        profile=second_profile,
        now=NOW,
        pending_channels={ended_candidate["event_key"]: ended_candidate},
    )
    ended_failure = _fail_pending(
        ended_admission, "health_unknown", NOW + timedelta(minutes=1),
    )
    assert begin_delivery(
        "profile:2",
        expected_revision=ended_failure["revision"],
        expected_hash=ended_failure["state"]["xmltv_hash"],
        profile=second_profile,
        now=datetime.fromisoformat(ended_candidate["stop"]),
        pending_channels={ended_candidate["event_key"]: ended_candidate},
    ) is None
    assert read_publication("profile:2") == ended_failure


@pytest.mark.parametrize("reason", ["guide_failed"])
def test_health_recovery_keeps_nonhealth_limits(reason):
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    current = read_publication("profile:1")
    candidate = pending_candidate()
    first = begin_delivery(
        "profile:1",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={candidate["event_key"]: candidate},
    )
    first_failed = _fail_pending(first, "health_failed", NOW + timedelta(minutes=1))
    second = begin_delivery(
        "profile:1",
        expected_revision=first_failed["revision"],
        expected_hash=first_failed["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW + timedelta(minutes=6),
        pending_channels={candidate["event_key"]: candidate},
    )
    second_failed = _fail_pending(second, reason, NOW + timedelta(minutes=7))
    retry = NOW + timedelta(minutes=12)
    assert begin_delivery(
        "profile:1",
        expected_revision=second_failed["revision"],
        expected_hash=second_failed["state"]["xmltv_hash"],
        profile=profile(),
        now=retry,
        pending_channels={candidate["event_key"]: candidate},
    ) is None
    assert read_publication("profile:1") == second_failed

    changed = pending_candidate(stream_name="Falcons vs Wolves HD")
    third = begin_delivery(
        "profile:1",
        expected_revision=second_failed["revision"],
        expected_hash=second_failed["state"]["xmltv_hash"],
        profile=profile(),
        now=retry,
        pending_channels={changed["event_key"]: changed},
    )
    third_receipt = third["state"]["delivery"]["pending_channels"][changed["event_key"]]
    assert third_receipt["attempt_no"] == 3
    assert third_receipt["input_hash"] != second["state"]["delivery"]["pending_channels"][
        candidate["event_key"]
    ]["input_hash"]
    third_failed = _fail_pending(third, reason, NOW + timedelta(minutes=13))
    changed_again = pending_candidate(stream_name="Falcons vs Wolves UHD")
    assert begin_delivery(
        "profile:1",
        expected_revision=third_failed["revision"],
        expected_hash=third_failed["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW + timedelta(minutes=18),
        pending_channels={changed_again["event_key"]: changed_again},
    ) is None
    assert read_publication("profile:1") == third_failed


@pytest.mark.parametrize("case", [
    "missing_uuid", "changed_uuid", "absent_channel", "changed_rule",
    "changed_profile", "changed_group", "changed_event_key", "changed_start",
    "changed_stop", "changed_title", "changed_name", "changed_execution",
    "stale_publication", "stale_attempt", "allocation_unknown",
])
def test_health_recovery_requires_same_owned_event(case):
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    current = read_publication("profile:1")
    candidate = pending_candidate()

    if case == "allocation_unknown":
        candidate["channel_id"] = None
        candidate["channel_uuid"] = None
        candidate["channel_exists"] = False
        admitted = begin_delivery(
            "profile:1",
            expected_revision=current["revision"],
            expected_hash=current["state"]["xmltv_hash"],
            profile=profile(),
            now=NOW,
            pending_channels={candidate["event_key"]: candidate},
        )
        receipt = admitted["state"]["delivery"]["pending_channels"][candidate["event_key"]]
        assert receipt["stage"] == "intent"
        allocating = {**receipt, "stage": "allocating"}
        allocating_revision = update_delivery(
            "profile:1",
            expected_revision=admitted["revision"],
            expected_pending={candidate["event_key"]: receipt["attempt_id"]},
            pending_channels={candidate["event_key"]: allocating},
        )
        admitted = read_publication("profile:1")
        assert admitted["revision"] == allocating_revision
        receipt = admitted["state"]["delivery"]["pending_channels"][candidate["event_key"]]
        assert begin_delivery(
            "profile:1",
            expected_revision=admitted["revision"],
            expected_hash=admitted["state"]["xmltv_hash"],
            profile=profile(),
            now=datetime.fromisoformat(receipt["expires_at"]),
            pending_channels={candidate["event_key"]: candidate},
        ) is None
        closed = read_publication("profile:1")
        assert closed["state"]["delivery"]["pending_channels"][
            candidate["event_key"]
        ]["stage"] == "allocation_unknown"
        assert begin_delivery(
            "profile:1",
            expected_revision=closed["revision"],
            expected_hash=closed["state"]["xmltv_hash"],
            profile=profile(),
            now=datetime.fromisoformat(receipt["expires_at"]) + timedelta(minutes=5),
            pending_channels={candidate["event_key"]: pending_candidate()},
        ) is None
        assert read_publication("profile:1") == closed
        return

    admitted = begin_delivery(
        "profile:1",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={candidate["event_key"]: candidate},
    )
    failed = _fail_pending(admitted, "health_failed", NOW + timedelta(minutes=1))
    before = copy.deepcopy(failed)
    retry = NOW + timedelta(minutes=6)

    if case == "stale_attempt":
        assert update_delivery(
            "profile:1",
            expected_revision=failed["revision"],
            expected_pending={candidate["event_key"]: "f" * 32},
            pending_channels=failed["state"]["delivery"]["pending_channels"],
        ) is None
        assert read_publication("profile:1") == before
        return

    expected_revision = failed["revision"]
    if case == "stale_publication":
        expected_revision -= 1
    elif case == "missing_uuid":
        candidate["channel_uuid"] = None
    elif case == "changed_uuid":
        candidate["channel_uuid"] = "foreign-channel"
    elif case == "absent_channel":
        candidate["channel_exists"] = False
    elif case == "changed_rule":
        candidate["rule_id"] += 1
    elif case == "changed_profile":
        candidate["profile_id"] += 1
    elif case == "changed_group":
        candidate["target_group_id"] += 1
    elif case == "changed_event_key":
        candidate["event_key"] = "arena:other-event"
    elif case == "changed_start":
        candidate["start"] = (NOW - timedelta(minutes=29)).isoformat()
    elif case == "changed_stop":
        candidate["stop"] = (NOW + timedelta(hours=3)).isoformat()
    elif case == "changed_title":
        candidate["title"] = "Falcons vs Bears"
    elif case == "changed_name":
        candidate["channel_name"] = "Arena 2"
    elif case == "changed_execution":
        candidate["execution_id"] = "execution-2"

    call = lambda: begin_delivery(
        "profile:1",
        expected_revision=expected_revision,
        expected_hash=failed["state"]["xmltv_hash"],
        profile=profile(),
        now=retry,
        pending_channels={"arena:falcons-wolves": candidate},
    )
    if case in {"changed_profile", "changed_event_key"}:
        with pytest.raises(ValueError):
            call()
    else:
        assert call() is None
    assert read_publication("profile:1") == before


def test_health_recovery_preserves_terminal_claims():
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    current = read_publication("profile:1")
    candidate = pending_candidate()
    admitted = begin_delivery(
        "profile:1",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={candidate["event_key"]: candidate},
    )
    failed = _fail_pending(admitted, "health_unknown", NOW + timedelta(minutes=1))
    failed_receipt = failed["state"]["delivery"]["pending_channels"][candidate["event_key"]]
    retry = NOW + timedelta(minutes=6)

    planned = begin_delivery(
        "profile:1",
        expected_revision=failed["revision"],
        expected_hash=failed["state"]["xmltv_hash"],
        profile=profile(),
        now=retry,
        pending_channels={candidate["event_key"]: candidate},
        plan_only=True,
    )
    assert planned["state"]["delivery"]["pending_channels"][candidate["event_key"]][
        "attempt_no"
    ] == 2
    assert read_publication("profile:1") == failed

    successor = begin_delivery(
        "profile:1",
        expected_revision=failed["revision"],
        expected_hash=failed["state"]["xmltv_hash"],
        profile=profile(),
        now=retry,
        pending_channels={candidate["event_key"]: candidate},
    )
    successor_receipt = successor["state"]["delivery"]["pending_channels"][candidate["event_key"]]
    before_stale_write = copy.deepcopy(successor)
    assert update_delivery(
        "profile:1",
        expected_revision=successor["revision"],
        expected_pending={candidate["event_key"]: failed_receipt["attempt_id"]},
        pending_channels=successor["state"]["delivery"]["pending_channels"],
    ) is None
    assert read_publication("profile:1") == before_stale_write

    terminal = _fail_pending(successor, "health_failed", NOW + timedelta(minutes=7))
    terminal_receipt = terminal["state"]["delivery"]["pending_channels"][candidate["event_key"]]
    changed = copy.deepcopy(terminal_receipt)
    changed["history"][0]["reason"] = "guide_failed"
    with pytest.raises(ValueError, match="immutable"):
        update_delivery(
            "profile:1",
            expected_revision=terminal["revision"],
            expected_pending={candidate["event_key"]: terminal_receipt["attempt_id"]},
            pending_channels={candidate["event_key"]: changed},
        )
    assert read_publication("profile:1") == terminal


@pytest.mark.parametrize("case", [
    "missing", "duplicate", "out_of_order", "oversized", "non_health",
    "different_input",
])
def test_health_recovery_validates_recent_history(test_session, case):
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    current = read_publication("profile:1")
    candidate = pending_candidate()
    publication = begin_delivery(
        "profile:1",
        expected_revision=current["revision"],
        expected_hash=current["state"]["xmltv_hash"],
        profile=profile(),
        now=NOW,
        pending_channels={candidate["event_key"]: candidate},
    )
    assert publication["state"]["delivery"]["pending_channels"][candidate["event_key"]][
        "attempt_no"
    ] == 1
    for attempt_no in range(2, 5):
        terminal_at = NOW + timedelta(minutes=attempt_no * 10)
        failed = _fail_pending(publication, "health_failed", terminal_at)
        publication = begin_delivery(
            "profile:1",
            expected_revision=failed["revision"],
            expected_hash=failed["state"]["xmltv_hash"],
            profile=profile(),
            now=terminal_at + timedelta(minutes=5),
            pending_channels={candidate["event_key"]: candidate},
        )
        stored = read_publication("profile:1")
        assert stored == publication
        assert stored["state"]["delivery"]["pending_channels"][candidate["event_key"]][
            "attempt_no"
        ] == attempt_no

    receipt = publication["state"]["delivery"]["pending_channels"][candidate["event_key"]]
    assert [item["attempt_no"] for item in receipt["history"]] == [2, 3]
    test_session.expire_all()
    row = test_session.query(GuidePublication).filter_by(scope="profile:1").one()
    state = json.loads(row.state)
    history = state["delivery"]["pending_channels"][candidate["event_key"]]["history"]
    if case == "missing":
        history.pop(0)
    elif case == "duplicate":
        history[1]["attempt_id"] = history[0]["attempt_id"]
    elif case == "out_of_order":
        history.reverse()
    elif case == "oversized":
        older = copy.deepcopy(history[0])
        older["attempt_id"] = "a" * 32
        older["attempt_no"] = 1
        history.insert(0, older)
    elif case == "non_health":
        history[-1]["reason"] = "guide_failed"
    elif case == "different_input":
        history[-1]["input_hash"] = "f" * 64
    row.state = json.dumps(state)
    test_session.commit()
    with pytest.raises(ValueError):
        read_publication("profile:1")


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


@pytest.mark.parametrize("condition", ["active", "ended", "terminal", "foreign", "allocating"])
def test_legacy_delivery_resumes_only_eligible_receipts(condition):
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    stored = read_publication("profile:1")
    candidate = pending_candidate()
    candidate["stop"] = (NOW + timedelta(hours=48)).isoformat()
    admitted = begin_delivery(
        "profile:1", expected_revision=stored["revision"],
        expected_hash=stored["state"]["xmltv_hash"], profile=profile(), now=NOW,
        pending_channels={candidate["event_key"]: candidate},
    )
    legacy = copy.deepcopy(admitted["state"])
    delivery = legacy["delivery"]
    attempt = delivery["guide_attempt"]
    attempt["expires_at"] = (NOW + timedelta(hours=24)).isoformat()
    receipt = delivery["pending_channels"][candidate["event_key"]]
    receipt["expires_at"] = attempt["expires_at"]
    if condition == "ended":
        receipt["stop"] = receipt["expires_at"]
    elif condition == "terminal":
        receipt.update(stage="failed", reason="health_failed",
                       terminal_at=(NOW + timedelta(minutes=1)).isoformat(),
                       retry_at=(NOW + timedelta(minutes=6)).isoformat())
    elif condition == "foreign":
        receipt["config_hash"] = "f" * 64
    elif condition == "allocating":
        receipt.update(stage="allocating", channel_id=None, channel_uuid=None)
    progress = {
        "source_id": 7, "endpoint_hash": "1" * 64, "source_url_hash": "2" * 64,
        "expected_hash": legacy["xmltv_hash"], "initial_updated": "initial",
        "observed_running": True, "triggered": True,
        "expires_at": attempt["expires_at"], "attempt_id": attempt["attempt_id"],
    }
    delivery["source_refreshes"] = {"source": progress}
    with database.get_session() as session:
        row = session.query(GuidePublication).filter_by(scope="profile:1").one()
        row.state = json.dumps(legacy)
        session.commit()
    normalized = copy.deepcopy(legacy)
    normalized["delivery"]["source_refreshes"]["source"].update(
        links=None, pending_links=None, completed=False,
    )
    assert read_publication("profile:1")["state"] == normalized

    resumed = begin_delivery(
        "profile:1", expected_revision=admitted["revision"],
        expected_hash=legacy["xmltv_hash"], profile=profile(),
        now=NOW + timedelta(hours=25),
    )
    assert read_publication("profile:1") == resumed
    result = resumed["state"]["delivery"]
    assert result["guide_attempt"] == {**attempt, "expires_at": None}
    assert result["source_refreshes"]["source"] == {
        **progress, "expires_at": None, "links": None,
        "pending_links": None, "completed": False,
    }
    current = result["pending_channels"][candidate["event_key"]]
    assert current["attempt_id"] == receipt["attempt_id"]
    assert current["channel_id"] == receipt["channel_id"]
    assert current["history"] == receipt["history"]
    if condition == "active":
        assert current == {**receipt, "expires_at": receipt["stop"]}
    elif condition == "terminal":
        assert current == receipt
    else:
        assert current["expires_at"] == receipt["expires_at"]
        assert current["stage"] == ("allocation_unknown" if condition == "allocating" else "expired")


def test_legacy_loading_normalization_requires_current_revision_and_config():
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    stored = read_publication("profile:1")
    admitted = begin_delivery(
        "profile:1", expected_revision=stored["revision"],
        expected_hash=stored["state"]["xmltv_hash"], profile=profile(), now=NOW,
        pending_channels={"arena:falcons-wolves": pending_candidate()},
    )
    with database.get_session() as session:
        row = session.query(GuidePublication).filter_by(scope="profile:1").one()
        state = json.loads(row.state)
        state["delivery"]["guide_attempt"]["expires_at"] = (NOW + timedelta(hours=24)).isoformat()
        row.state = json.dumps(state)
        session.commit()
    before = read_publication("profile:1")
    for revision, selected in (
        (admitted["revision"] - 1, profile()),
        (admitted["revision"], {**profile(), "name": "Changed"}),
    ):
        assert begin_delivery(
            "profile:1", expected_revision=revision,
            expected_hash=before["state"]["xmltv_hash"], profile=selected,
            now=NOW + timedelta(minutes=1),
        ) is None
        assert read_publication("profile:1") == before


@pytest.mark.parametrize("case", ["legacy", "headers", "programmes", "successor", "empty", "finite"])
def test_source_phase_round_trips(case):
    from services.epg_publication import _source_refreshes

    attempt = {"attempt_id": "1" * 32, "expires_at": None}
    progress = {
        "source_id": 46, "endpoint_hash": "a" * 64, "source_url_hash": "b" * 64,
        "expected_hash": "c" * 64, "initial_updated": "initial",
        "observed_running": True, "triggered": True,
        "expires_at": None, "attempt_id": attempt["attempt_id"],
    }
    if case != "legacy":
        progress.update(links=None, pending_links=None, completed=True)
    if case in {"programmes", "successor"}:
        progress["links"] = {"10": 900}
    if case == "successor":
        progress["pending_links"] = {"10": 901}
    elif case == "empty":
        progress["links"] = {}
    elif case == "finite":
        attempt["expires_at"] = progress["expires_at"] = NOW.isoformat()
    result = _source_refreshes({"source": progress}, attempt, "c" * 64)
    assert result == {"source": {
        "links": None, "pending_links": None, "completed": False, **progress,
    }}
    assert _source_refreshes(json.loads(json.dumps(result)), attempt, "c" * 64) == result


@pytest.mark.parametrize("change", [
    {"links": {}},
    {"links": {str(number): 900 for number in range(1, 10002)}, "pending_links": None, "completed": False},
    {"links": {"01": 900}, "pending_links": None, "completed": False},
    {"links": {"10": True}, "pending_links": None, "completed": False},
    {"links": {"10": 0}, "pending_links": None, "completed": False},
    {"links": {True: 900}, "pending_links": None, "completed": False},
    {"links": None, "pending_links": None, "completed": "yes"},
    {"links": None, "pending_links": None, "completed": True, "triggered": False},
    {"links": None, "pending_links": None, "completed": False, "unknown": True},
])
def test_source_phase_rejects_incomplete_or_invalid_fields(change):
    from services.epg_publication import _source_refreshes

    progress = {
        "source_id": 46, "endpoint_hash": "a" * 64, "source_url_hash": "b" * 64,
        "expected_hash": "c" * 64, "initial_updated": "initial",
        "observed_running": False, "triggered": True,
        "expires_at": None, "attempt_id": "1" * 32, **change,
    }
    with pytest.raises(ValueError):
        _source_refreshes({"source": progress}, {"attempt_id": "1" * 32, "expires_at": None}, "c" * 64)


@pytest.mark.asyncio
@pytest.mark.parametrize("lose_second", [False, True])
async def test_aggregate_refresh_admission_commits_all_claims_or_none(monkeypatch, lose_second):
    from unittest.mock import AsyncMock, MagicMock
    from services import epg_publication

    publish_profiles([profile(1), profile(2)], channel_map(1, 2), coverage(1, 2), observations={}, now=NOW)
    publications = {}
    for profile_id in (1, 2):
        row = read_publication(f"profile:{profile_id}")
        publications[profile_id] = begin_delivery(
            row["scope"], expected_revision=row["revision"], expected_hash=row["state"]["xmltv_hash"],
            profile=profile(profile_id), now=NOW,
        )
    before = copy.deepcopy(publications)
    source = {"id": 46, "name": "All", "url": "http://ecm/api/dummy-epg/xmltv", "status": "ready", "updated_at": "old"}
    client = MagicMock(base_url="http://dispatcharr.local")
    client.get_epg_source = AsyncMock(side_effect=lambda source_id: copy.deepcopy(source))

    async def refresh(source_id):
        source.update(status="success", updated_at="new")

    client.refresh_epg_source = AsyncMock(side_effect=refresh)
    original = epg_publication.update_delivery

    def update(scope, **claims):
        assert claims["session"] is not None
        if lose_second and scope == "profile:2":
            return None
        return original(scope, **claims)

    if lose_second:
        monkeypatch.setattr(epg_publication, "update_delivery", update)
    result = await epg_publication.refresh_source(client, source, publications, expires_at=None, wait=False)
    assert result is (not lose_second)
    if lose_second:
        client.refresh_epg_source.assert_not_awaited()
        assert {key: read_publication(f"profile:{key}") for key in (1, 2)} == before
    else:
        client.refresh_epg_source.assert_awaited_once_with(46)
        for row in publications.values():
            progress = next(iter(row["state"]["delivery"]["source_refreshes"].values()))
            assert progress["completed"] is True
            assert progress["links"] is None
            assert row["state"]["delivery"]["confirmed_dispatcharr_hashes"] == {}


@pytest.mark.parametrize("phase", ["missing", "legacy", "headers", "programmes", "successor"])
def test_confirmations_require_current_programme_phase(phase):
    publish_profiles([profile()], channel_map(1), coverage(1), observations={}, now=NOW)
    row = read_publication("profile:1")
    admitted = begin_delivery(row["scope"], expected_revision=row["revision"],
                              expected_hash=row["state"]["xmltv_hash"], profile=profile(), now=NOW)
    state = copy.deepcopy(admitted["state"])
    delivery = state["delivery"]
    delivery["required_dispatcharr_hashes"] = {"46": state["xmltv_hash"]}
    delivery["confirmed_dispatcharr_hashes"] = dict(delivery["required_dispatcharr_hashes"])
    progress = {
        "source_id": 46, "endpoint_hash": "a" * 64, "source_url_hash": "b" * 64,
        "expected_hash": state["xmltv_hash"], "initial_updated": "initial",
        "observed_running": True, "triggered": True, "expires_at": None,
        "attempt_id": delivery["guide_attempt"]["attempt_id"],
    }
    if phase != "missing":
        if phase != "legacy":
            progress.update(links=None if phase == "headers" else {"1": 900},
                            pending_links={"1": 901} if phase == "successor" else None,
                            completed=True)
        delivery["source_refreshes"] = {"source": progress}
    with database.get_session() as session:
        stored = session.query(GuidePublication).filter_by(scope="profile:1").one()
        stored.state = json.dumps(state)
        session.commit()
    if phase in {"headers", "successor"}:
        with pytest.raises(ValueError, match="programme phase"):
            read_publication("profile:1")
    else:
        confirmed = read_publication("profile:1")["state"]["delivery"]["confirmed_dispatcharr_hashes"]
        assert confirmed == ({"46": state["xmltv_hash"]} if phase == "programmes" else {})
