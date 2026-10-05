"""Event Sync dummy EPG auto-assignment (bead enhancedchannelmanager-ti939.3.3).

Full-run behavior of the optional ``dummy_epg_profile_id`` reference in
``event_sync_config``: on every event_sync run (manual AND unattended) the
rule's dummy EPG profile is assigned to master-group channels through the
EXISTING machinery — standard ``assign_epg`` execution against the
Dispatcharr EPG source that serves the profile's XMLTV, with uncovered
channels deferring into the EXISTING Pass 5 refresh-and-retry (which also
auto-adds the master group to the profile's ``channel_group_ids``,
regenerates the XMLTV, refreshes the source, and retries). No parallel
mechanism.

Behavior statements pinned here:

1. **Pass 5 retry path** — a first run against an empty dummy source
   defers every master channel; Pass 5 regenerates + refreshes + retries
   and the channels end the run with the profile's guide data assigned.
2. **Idempotency** — a second run on assigned channels performs ZERO
   further EPG writes (``already_assigned`` no-ops).
3. **Non-clobbering** — a master channel carrying FOREIGN guide data
   (another source / hand-assigned real EPG) is never overwritten.
4. **Steady state** — a channel the source already covers is assigned
   directly, without triggering Pass 5.
5. **Unattended parity** — an ``auto_run`` rule assigns from the
   watermark task exactly like a manual run.
6. **Graceful degradation** — no Dispatcharr source for the profile, or a
   disabled profile, warns and skips ONLY the EPG step; attaches are
   unaffected.

Every scenario rides the standing canaries: event_sync never creates or
deletes channels and never toggles Dispatcharr group settings.
"""
from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import database
# Freeze the real auth session dependency before schema tests patch get_session.
from auth import RequireAdminIfEnabled  # noqa: F401
from channel_pipeline_engine import ChannelPipelineEngine
from models import ChannelPipelineRule, DummyEPGProfile
from tests.event_sync_fixtures import (
    FakeDispatcharrState,
    GROUP_NAMES,
    MASTER_GROUP_ID,
    SECONDARY_A,
    assert_never_created_or_deleted_channels,
    assert_never_touched_group_settings,
    event_sync_config,
    make_stateful_client,
)

MASTER_MERCURY = "Peacock 14: Mercury vs. Aces @ 11 Jul 06:00 PM ET"
STREAM_MERCURY = "WNBA TV 01: Mercury vs. Aces @ 11 Jul 06:00 PM ET"
SECONDARY_GROUP_NAME = GROUP_NAMES[SECONDARY_A]

PROFILE_ID = 7
DUMMY_SOURCE_ID = 42


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@pytest.fixture()
def db_session_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False,
    )
    database.Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=engine, expire_on_commit=False
    )
    try:
        yield SessionLocal
    finally:
        database.Base.metadata.drop_all(bind=engine)
        engine.dispose()


def _add_profile(session_factory, enabled: bool = True) -> None:
    session = session_factory()
    try:
        session.add(DummyEPGProfile(
            id=PROFILE_ID, name="Peacock Events EPG", enabled=enabled,
        ))
        session.commit()
    finally:
        session.close()


def _profile_group_ids(session_factory) -> list:
    session = session_factory()
    try:
        profile = session.get(DummyEPGProfile, PROFILE_ID)
        return profile.get_channel_group_ids()
    finally:
        session.close()


def _add_event_rule(session_factory, config: dict) -> int:
    session = session_factory()
    try:
        rule = ChannelPipelineRule(
            name="Event Rule",
            enabled=True,
            priority=0,
            conditions=json.dumps([{"type": "always"}]),
            actions=json.dumps([{"type": "skip"}]),
            event_sync_config=json.dumps(config),
        )
        session.add(rule)
        session.commit()
        session.refresh(rule)
        return rule.id
    finally:
        session.close()


def _latest_execution_warnings(session_factory) -> list:
    """Run warnings are POPPED from the result and persisted on the
    execution record (ti939.2.1 surface) — read them back from there."""
    from models import ChannelPipelineExecution

    session = session_factory()
    try:
        execution = (
            session.query(ChannelPipelineExecution)
            .order_by(ChannelPipelineExecution.id.desc())
            .first()
        )
        return (execution.get_warnings() or []) if execution else []
    finally:
        session.close()


def _config(**overrides) -> dict:
    return event_sync_config(
        secondary_group_ids=[SECONDARY_A],
        dummy_epg_profile_id=PROFILE_ID,
        **overrides,
    )


def _mercury_state() -> FakeDispatcharrState:
    return FakeDispatcharrState(
        channels=[{
            "id": 100, "name": MASTER_MERCURY,
            "channel_group_id": MASTER_GROUP_ID,
            "auto_created": True, "streams": [9001],
        }],
        secondary_streams={SECONDARY_GROUP_NAME: [
            {"id": 7001, "name": STREAM_MERCURY, "m3u_account": 1},
        ]},
    )


def _dummy_entry(
    entry_id: int,
    channel_id: int,
    channel_name: str,
    source_id: int = DUMMY_SOURCE_ID,
) -> dict:
    """One EPG data entry as Dispatcharr serves it for the dummy source.

    The tvg_id carries the XMLTV channel key, which the dummy engine derives
    from the ECM channel id ("ecm-<channel id>"), not from the EPG row id —
    the two are different id spaces (bead l76).
    """
    return {
        "id": entry_id,
        "tvg_id": f"ecm-{channel_id}",
        "name": channel_name,
        "epg_source": source_id,
    }


def _wire_epg(state, client, session_factory,
              initial_entries: list[dict] | None = None,
              source_url: str | None = None,
              regenerated_entries: list[dict] | None = None,
              *, profile_id: int = PROFILE_ID,
              source_id: int = DUMMY_SOURCE_ID,
              now: datetime | None = None):
    """Wire explicit source, header, programme, and publication evidence.

    Regeneration commits only XMLTV. The first successful source import adds
    headers, and a later import adds programmes after linking. Returns the
    mutable header store plus the regeneration and import mocks.
    """
    if source_url is None:
        source_url = f"http://ecm:8000/api/dummy-epg/xmltv/{profile_id}"
    state.guide_sources[:] = [{
        "id": source_id,
        "name": "ECM Dummy EPG",
        "url": source_url,
        "status": "ready",
        "updated_at": "2026-07-11T15:00:00+00:00",
    }]
    state.guide_rows[:] = copy.deepcopy(initial_entries or [])
    headers = copy.deepcopy(regenerated_entries or [])
    now = (now or datetime.now(timezone.utc)).replace(microsecond=0)

    from services.epg_publication import begin_delivery, publish_profiles, read_publication

    def available_headers():
        return [
            row for row in headers
            if int(row["tvg_id"].split("-", 1)[1]) in state.channels
        ]

    def publication_values(rows, publication=None):
        session = session_factory()
        try:
            profile = session.get(DummyEPGProfile, profile_id).to_dict()
        finally:
            session.close()
        channel_ids = [
            int(row["tvg_id"].split("-", 1)[1])
            for row in rows
        ]
        profile["channel_assignments"] = [
            {
                "channel_id": channel_id,
                "channel_name": state.channels[channel_id]["name"],
            }
            for channel_id in channel_ids
        ]
        receipts = {
            receipt.get("channel_id"): receipt
            for receipt in (
                (publication or {}).get("state", {}).get("delivery", {})
                .get("pending_channels", {}).values()
            )
            if receipt.get("channel_id") is not None
        }
        profile["event_intervals"] = {
            channel_id: ([{
                "start": receipts[channel_id]["start"],
                "stop": receipts[channel_id]["stop"],
                "title": receipts[channel_id]["title"],
            }] if channel_id in receipts else [{
                "start": (now - timedelta(minutes=15)).isoformat(),
                "stop": (now + timedelta(hours=2)).isoformat(),
                "title": state.channels[channel_id]["name"],
            }])
            for channel_id in channel_ids
        }
        channels = {
            channel_id: copy.deepcopy(state.channels[channel_id])
            for channel_id in channel_ids
        }
        coverage = {"profiles": {str(profile_id): {
            "profile_id": profile_id,
            "can_publish": True,
            "reason_codes": [],
        }}}
        return profile, channels, coverage

    async def publish(*, publications, wait_for_sources=True):
        assert wait_for_sources is False
        admitted = publications[profile_id]
        attempt = admitted["state"]["delivery"]["guide_attempt"]
        assert attempt is not None
        profile, channels, coverage = publication_values(
            available_headers(), admitted,
        )
        expected = {
            f"profile:{profile_id}": {
                "revision": admitted["revision"],
                "xmltv_hash": admitted["state"]["xmltv_hash"],
                "config_hash": admitted["state"]["config_hash"],
                "attempt_id": attempt["attempt_id"],
            },
        }
        return publish_profiles(
            [profile],
            channels,
            coverage,
            observations={},
            now=now,
            expected=expected,
        )

    if initial_entries:
        with patch("services.epg_publication.get_session", side_effect=session_factory):
            publication = read_publication(f"profile:{profile_id}")
    if initial_entries and publication is None:
        session = session_factory()
        try:
            saved = session.get(DummyEPGProfile, profile_id)
            if saved.enabled:
                saved.set_channel_group_ids([MASTER_GROUP_ID])
                session.commit()
                admitted_profile = saved.to_dict()
            else:
                admitted_profile = None
        finally:
            session.close()
        if admitted_profile is not None:
            with patch(
                "services.epg_publication.get_session",
                side_effect=session_factory,
            ):
                admitted = begin_delivery(
                    f"profile:{profile_id}",
                    expected_revision=0,
                    expected_hash=None,
                    profile=admitted_profile,
                    now=now,
                )
                profile, channels, coverage = publication_values(initial_entries)
                attempt = admitted["state"]["delivery"]["guide_attempt"]
                publish_profiles(
                    [profile],
                    channels,
                    coverage,
                    observations={},
                    now=now,
                    expected={f"profile:{profile_id}": {
                        "revision": admitted["revision"],
                        "xmltv_hash": admitted["state"]["xmltv_hash"],
                        "config_hash": admitted["state"]["config_hash"],
                        "attempt_id": attempt["attempt_id"],
                    }},
                )

    async def complete_refresh(*args, **kwargs):
        served_headers = available_headers()
        known_tvg_ids = {row["tvg_id"] for row in state.guide_rows}
        missing_headers = [
            row for row in served_headers if row["tvg_id"] not in known_tvg_ids
        ]
        if missing_headers:
            state.guide_rows.extend(copy.deepcopy(missing_headers))
        else:
            from services.epg_publication import read_publication

            publication = read_publication(f"profile:{profile_id}")
            receipts = {
                receipt.get("channel_id"): receipt
                for receipt in (
                    (publication or {}).get("state", {}).get("delivery", {})
                    .get("pending_channels", {}).values()
                )
                if receipt.get("channel_id") is not None
            }
            state.guide_programmes[:] = [
                {
                    "epg_data_id": row["id"],
                    "tvg_id": row["tvg_id"],
                    "title": (
                        receipts.get(int(row["tvg_id"].split("-", 1)[1]), {})
                        .get("title", row["name"])
                    ),
                    "start_time": (
                        receipts.get(int(row["tvg_id"].split("-", 1)[1]), {})
                        .get("start", (now - timedelta(minutes=15)).isoformat())
                    ),
                    "end_time": (
                        receipts.get(int(row["tvg_id"].split("-", 1)[1]), {})
                        .get("stop", (now + timedelta(hours=2)).isoformat())
                    ),
                }
                for row in served_headers
            ]
        return True

    regenerate = AsyncMock(side_effect=publish)
    wait_refresh = AsyncMock(side_effect=complete_refresh)
    wait_refresh.complete_refresh = complete_refresh
    return headers, regenerate, wait_refresh


def _manual_run(client, session_factory, regenerate, wait_refresh,
                dry_run: bool = False):
    """One manual pipeline run with the Pass 5 dummy-EPG surfaces mocked.

    Patches ``database.get_session`` too: the schema validator's
    dummy_epg_profile_id existence check reads the profile table directly.
    """
    engine = ChannelPipelineEngine(client)
    task_cls = MagicMock()
    task_cls.return_value._regenerate_xmltv = regenerate
    with patch("channel_pipeline_engine.get_session",
               side_effect=session_factory), \
         patch("routers.channel_pipeline.get_session",
               side_effect=session_factory), \
         patch("database.get_session", side_effect=session_factory), \
         patch("services.epg_publication.get_session",
               side_effect=session_factory), \
         patch("tasks.dummy_epg_refresh.DummyEPGRefreshTask", task_cls), \
         patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh",
               wait_refresh), \
         patch("journal.log_entries"):
        result = _run(engine.run_pipeline(
            dry_run=dry_run, triggered_by="manual"
        ))
    return result


def _refresh_executor(source_ids=(), publications=None):
    return SimpleNamespace(
        _epg_import_sources=set(source_ids),
        _epg_import_attempts=set(),
        _event_pending={},
        _event_publications=dict(publications or {}),
        _finish_event_promotions=AsyncMock(return_value=set()),
    )


def _refresh_results():
    return {
        "failed_actions": [],
        "streams_merged": 0,
        "streams_skipped": 0,
        "channels_updated": 0,
        "modified_entities": [],
        "channels_touched": 0,
    }


def _seed_publications(session_factory, sources):
    from services.epg_publication import (
        begin_delivery,
        publish_profiles,
        read_publication,
    )
    from tasks.event_visibility import _generated_scope

    now = datetime.now(timezone.utc).replace(microsecond=0)
    session = session_factory()
    try:
        profiles = []
        for source in sources:
            scope = _generated_scope(source)
            assert scope is not None and scope.startswith("profile:")
            profile_id = int(scope.split(":", 1)[1])
            profile = DummyEPGProfile(
                id=profile_id,
                name=f"Guide {profile_id}",
                enabled=True,
                name_source="channel",
                event_timezone="UTC",
                output_timezone="UTC",
                program_duration=180,
            )
            profile.set_channel_group_ids([MASTER_GROUP_ID])
            profile.set_epg_source_ids([source["id"]])
            session.add(profile)
            profiles.append(profile)
        session.commit()
        saved = [profile.to_dict() for profile in profiles]
    finally:
        session.close()

    channels = {}
    coverage = {"profiles": {}}
    expected = {}
    with patch(
        "services.epg_publication.get_session",
        side_effect=session_factory,
    ):
        for profile in saved:
            profile_id = profile["id"]
            channel_id = 900 + profile_id
            admitted = begin_delivery(
                f"profile:{profile_id}",
                expected_revision=0,
                expected_hash=None,
                profile=profile,
                now=now,
            )
            assert admitted is not None
            attempt = admitted["state"]["delivery"]["guide_attempt"]
            expected[f"profile:{profile_id}"] = {
                "revision": admitted["revision"],
                "xmltv_hash": admitted["state"]["xmltv_hash"],
                "config_hash": admitted["state"]["config_hash"],
                "attempt_id": attempt["attempt_id"],
            }
            profile["channel_assignments"] = [{
                "channel_id": channel_id,
                "channel_name": f"Arena {profile_id}",
            }]
            profile["event_intervals"] = {channel_id: [{
                "start": (now - timedelta(minutes=15)).isoformat(),
                "stop": (now + timedelta(hours=2)).isoformat(),
                "title": f"Event {profile_id}",
            }]}
            channels[channel_id] = {
                "id": channel_id,
                "name": f"Arena {profile_id}",
                "channel_number": channel_id,
                "channel_group_id": MASTER_GROUP_ID,
                "streams": [],
            }
            coverage["profiles"][str(profile_id)] = {
                "profile_id": profile_id,
                "can_publish": True,
                "reason_codes": [],
            }
        publish_profiles(
            saved,
            channels,
            coverage,
            observations={},
            now=now,
            expected=expected,
        )
        return {
            profile["id"]: read_publication(f'profile:{profile["id"]}')
            for profile in saved
        }


class TestPass5RetryPath:
    """Acceptance: the Pass 5 deferred assign_epg retry path is exercised
    end to end — no parallel mechanism."""

    def test_first_run_defers_then_pass5_assigns_the_profile(
        self, db_session_factory
    ):
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        # Empty source: the profile has never generated XMLTV covering the
        # master group. Pass 5's regeneration produces the entry.
        _epg_store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory,
            initial_entries=[],
            regenerated_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )

        result = _manual_run(
            client, db_session_factory, regenerate, wait_refresh
        )

        assert result["success"] is True
        # The attach path ran alongside (same run, same rule).
        assert result["event_sync"][0]["attached"] == 1

        # The EPG step deferred (source empty at execution time) ...
        epg_summary = result["event_sync"][0]["dummy_epg"]
        assert epg_summary["profile_id"] == PROFILE_ID
        assert epg_summary["source_id"] == DUMMY_SOURCE_ID
        assert epg_summary["deferred"] == 1
        assert epg_summary["assigned"] == 0

        # ... and Pass 5 did the real work: regenerated the XMLTV,
        # refreshed the source, re-fetched entries, retried the assign.
        regenerate.assert_awaited_once()
        assert wait_refresh.await_count == 2
        assert state.channels[100]["epg_data_id"] == 501
        # Pass 5 step 1 auto-added the master group to the profile so the
        # regenerated XMLTV covers it (the existing mechanism, reused).
        assert MASTER_GROUP_ID in _profile_group_ids(db_session_factory)
        # The retry rode the EXISTING Pass 5 execution-log surface.
        pass5_entries = [
            e for e in result["execution_log"]
            if str(e.get("stream_name", "")).startswith("[Pass 5")
        ]
        assert pass5_entries, "Pass 5 must appear in the execution log"

        assert_never_created_or_deleted_channels(client)
        assert_never_touched_group_settings(client)

    def test_second_run_is_an_idempotent_noop(self, db_session_factory):
        """Already-assigned channels are no-ops: zero further EPG writes."""
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        _epg_store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory,
            initial_entries=[],
            regenerated_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )

        _manual_run(client, db_session_factory, regenerate, wait_refresh)
        assert state.channels[100]["epg_data_id"] == 501
        writes_after_first = len(state.update_channel_calls)

        second = _manual_run(
            client, db_session_factory, regenerate, wait_refresh
        )

        assert second["success"] is True
        epg_summary = second["event_sync"][0]["dummy_epg"]
        assert epg_summary["already_assigned"] == 1
        assert epg_summary["assigned"] == 0
        assert epg_summary["deferred"] == 0
        # ZERO further writes and no second Pass 5 cycle.
        assert len(state.update_channel_calls) == writes_after_first
        regenerate.assert_awaited_once()  # run 1 only

        assert_never_touched_group_settings(client)

    def test_new_links_import_programmes_in_the_same_run(self, db_session_factory):
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        _store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory, initial_entries=[],
            regenerated_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )
        programmes = []

        async def import_linked(*args, **kwargs):
            await wait_refresh.complete_refresh(*args, **kwargs)
            if state.channels[100].get("epg_data_id") == 501:
                programmes.append({"channel_id": 100, "title": MASTER_MERCURY})
            return True

        wait_refresh.side_effect = import_linked
        result = _manual_run(client, db_session_factory, regenerate, wait_refresh)
        assert result["success"] is True
        assert programmes == [{"channel_id": 100, "title": MASTER_MERCURY}]
        assert wait_refresh.await_count == 2

    def test_existing_header_imports_programmes_after_first_link(self, db_session_factory):
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        _store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory,
            initial_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )
        programmes = []

        async def import_linked(*args, **kwargs):
            await wait_refresh.complete_refresh(*args, **kwargs)
            if state.channels[100].get("epg_data_id") == 501:
                programmes.append({"channel_id": 100, "title": MASTER_MERCURY})
            return True

        wait_refresh.side_effect = import_linked
        result = _manual_run(client, db_session_factory, regenerate, wait_refresh)
        assert result["success"] is True
        assert programmes == [{"channel_id": 100, "title": MASTER_MERCURY}]
        wait_refresh.assert_awaited_once()
        regenerate.assert_not_awaited()

    def test_failed_refresh_does_not_link_stale_rows(self, db_session_factory):
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        _store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory, initial_entries=[],
            regenerated_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )
        wait_refresh.side_effect = None
        wait_refresh.return_value = False
        result = _manual_run(client, db_session_factory, regenerate, wait_refresh)
        assert state.channels[100].get("epg_data_id") is None
        assert result["failed_actions"]
        wait_refresh.assert_awaited_once()

    def test_failed_programme_import_is_reported_after_linking(self, db_session_factory):
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        _store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory, initial_entries=[],
            regenerated_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )
        async def fail_programme(*args, **kwargs):
            if not state.guide_rows:
                return await wait_refresh.complete_refresh(*args, **kwargs)
            return False

        wait_refresh.side_effect = fail_programme
        result = _manual_run(client, db_session_factory, regenerate, wait_refresh)
        assert state.channels[100].get("epg_data_id") == 501
        assert result["failed_actions"]
        assert wait_refresh.await_count == 2


    @pytest.mark.parametrize("cancelled", [False, True])
    def test_failed_programme_import_retries_on_identical_pipeline_run(self, db_session_factory, cancelled):
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        _store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory,
            initial_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )
        programmes = []
        wait_refresh.side_effect = None
        wait_refresh.return_value = False
        if cancelled:
            wait_refresh.side_effect = asyncio.CancelledError
            with pytest.raises(asyncio.CancelledError):
                _manual_run(client, db_session_factory, regenerate, wait_refresh)
        else:
            first = _manual_run(client, db_session_factory, regenerate, wait_refresh)
            assert first["failed_actions"]
        assert state.channels[100].get("epg_data_id") == 501
        async def import_linked(*args, **kwargs):
            await wait_refresh.complete_refresh(*args, **kwargs)
            programmes.append({"channel_id": 100, "title": MASTER_MERCURY})
            return True
        wait_refresh.side_effect = import_linked
        second = _manual_run(client, db_session_factory, regenerate, wait_refresh)
        assert second["success"] is True
        assert programmes == [{"channel_id": 100, "title": MASTER_MERCURY}]
        _manual_run(client, db_session_factory, regenerate, wait_refresh)
        assert len(programmes) == 1


    def test_cancelled_import_retains_sources_not_yet_attempted(
        self, db_session_factory
    ):
        sources = [
            {
                "id": 100,
                "name": "Guide one",
                "url": "http://ecm/api/dummy-epg/xmltv/1",
                "status": "ready",
                "updated_at": "2026-07-11T15:00:00+00:00",
            },
            {
                "id": 101,
                "name": "Guide two",
                "url": "http://ecm/api/dummy-epg/xmltv/2",
                "status": "ready",
                "updated_at": "2026-07-11T15:00:00+00:00",
            },
        ]
        publications = _seed_publications(db_session_factory, sources)
        state = FakeDispatcharrState(guide_sources=sources)
        client = make_stateful_client(state)
        engine = ChannelPipelineEngine(client)
        with patch(
            "services.epg_publication.get_session",
            side_effect=db_session_factory,
        ), patch(
            "tasks.dummy_epg_refresh.wait_for_epg_source_refresh",
            new_callable=AsyncMock,
        ) as wait:
            wait.return_value = True
            for profile_id, source in zip((1, 2), sources):
                expires_at = datetime.fromisoformat(
                    publications[profile_id]["state"]["delivery"]
                    ["guide_attempt"]["expires_at"]
                )
                assert _run(engine._refresh_epg_source(
                    source,
                    {profile_id: publications[profile_id]},
                    expires_at=expires_at,
                )) is True

            wait.reset_mock()
            wait.side_effect = asyncio.CancelledError
            with pytest.raises(asyncio.CancelledError):
                _run(engine._refresh_linked_epg(
                    _refresh_executor(), _refresh_results(), sources,
                ))

            wait.side_effect = None
            wait.return_value = True
            wait.reset_mock()
            restarted = ChannelPipelineEngine(make_stateful_client(state))
            _run(restarted._refresh_linked_epg(
                _refresh_executor(), _refresh_results(), sources,
            ))
            assert [call.args[1] for call in wait.await_args_list] == [100, 101]

    @pytest.mark.parametrize("change", ["client", "source_id", "source_url"])
    def test_failed_import_retry_stays_with_its_client_and_source(
        self, db_session_factory, change
    ):
        source = {
            "id": 100,
            "name": "Guide one",
            "url": "http://ecm/api/dummy-epg/xmltv/1",
            "status": "ready",
            "updated_at": "2026-07-11T15:00:00+00:00",
        }
        _seed_publications(db_session_factory, [source])
        state = FakeDispatcharrState(guide_sources=[source])
        client = make_stateful_client(state)
        engine = ChannelPipelineEngine(client)
        executor = _refresh_executor({100})
        with patch(
            "services.epg_publication.get_session",
            side_effect=db_session_factory,
        ), patch(
            "tasks.dummy_epg_refresh.wait_for_epg_source_refresh",
            new_callable=AsyncMock,
        ) as wait:
            wait.return_value = False
            _run(engine._refresh_linked_epg(
                executor, _refresh_results(), [source],
            ))
            _run(engine._refresh_linked_epg(
                executor, _refresh_results(), [source],
            ))
            assert wait.await_count == 1

            other = dict(source)
            other_engine = engine
            if change == "client":
                other_client = make_stateful_client(state)
                other_client.base_url = "http://other.dispatcharr.test"
                other_engine = ChannelPipelineEngine(other_client)
            elif change == "source_id":
                other["id"] = 101
            else:
                other["url"] = "http://other-ecm/api/dummy-epg/xmltv/1"
            wait.return_value = True
            _run(other_engine._refresh_linked_epg(
                _refresh_executor(), _refresh_results(), [other],
            ))
            assert wait.await_count == 1
            restarted = ChannelPipelineEngine(make_stateful_client(state))
            _run(restarted._refresh_linked_epg(
                _refresh_executor(), _refresh_results(), [source],
            ))
            assert wait.await_count == 2
            from services.epg_publication import read_publication
            current = read_publication("profile:1")
            assert current["state"]["delivery"][
                "confirmed_dispatcharr_hashes"
            ] == {"100": current["state"]["xmltv_hash"]}

    @pytest.mark.parametrize("late_success", [True, False])
    def test_concurrent_imports_preserve_each_source_retry(
        self, db_session_factory, late_success
    ):
        sources = [
            {
                "id": 100,
                "name": "Guide one",
                "url": "http://ecm/api/dummy-epg/xmltv/1",
                "status": "ready",
                "updated_at": "2026-07-11T15:00:00+00:00",
            },
            {
                "id": 101,
                "name": "Guide two",
                "url": "http://ecm/api/dummy-epg/xmltv/2",
                "status": "ready",
                "updated_at": "2026-07-11T15:00:00+00:00",
            },
        ]
        _seed_publications(db_session_factory, sources)
        state = FakeDispatcharrState(guide_sources=sources)
        first_engine = ChannelPipelineEngine(make_stateful_client(state))
        second_engine = ChannelPipelineEngine(make_stateful_client(state))

        async def run():
            with patch(
                "services.epg_publication.get_session",
                side_effect=db_session_factory,
            ), patch(
                "tasks.dummy_epg_refresh.wait_for_epg_source_refresh",
                new_callable=AsyncMock,
            ) as wait:
                started, release = asyncio.Event(), asyncio.Event()

                async def finish(_client, source_id, *_args, **_kwargs):
                    if source_id == 100:
                        started.set()
                        await release.wait()
                        return late_success
                    return not late_success

                wait.side_effect = finish
                task = asyncio.create_task(first_engine._refresh_linked_epg(
                    _refresh_executor({100}),
                    _refresh_results(),
                    [sources[0]],
                ))
                await asyncio.wait_for(started.wait(), timeout=2)
                await second_engine._refresh_linked_epg(
                    _refresh_executor({101}),
                    _refresh_results(),
                    [sources[1]],
                )
                release.set()
                await task
                from services.epg_publication import read_publication
                pending = set()
                for profile_id, source_id in ((1, 100), (2, 101)):
                    current = read_publication(f"profile:{profile_id}")
                    delivery = current["state"]["delivery"]
                    if delivery["confirmed_dispatcharr_hashes"].get(
                        str(source_id)
                    ) != current["state"]["xmltv_hash"]:
                        pending.add(source_id)
                assert pending == ({101} if late_success else {100})
        _run(run())


class TestDirectAssignment:
    def test_channel_covered_by_the_source_is_assigned_without_pass5(
        self, db_session_factory
    ):
        """Steady state: the source already carries the channel's entry —
        a direct standard assign_epg write, no deferral, no Pass 5."""
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        _epg_store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory,
            initial_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )

        result = _manual_run(
            client, db_session_factory, regenerate, wait_refresh
        )

        assert result["success"] is True
        epg_summary = result["event_sync"][0]["dummy_epg"]
        assert epg_summary["assigned"] == 1
        assert epg_summary["deferred"] == 0
        assert state.channels[100]["epg_data_id"] == 501
        assert result["channels_updated"] >= 1
        regenerate.assert_not_awaited()

        assert_never_touched_group_settings(client)

    def test_combined_all_profiles_source_is_a_valid_fallback(
        self, db_session_factory
    ):
        """A Dispatcharr source on the combined /api/dummy-epg/xmltv URL
        (no profile id) serves every profile — accepted as fallback."""
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        _epg_store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory,
            initial_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
            source_url="http://ecm:8000/api/dummy-epg/xmltv",
        )

        result = _manual_run(
            client, db_session_factory, regenerate, wait_refresh
        )

        epg_summary = result["event_sync"][0]["dummy_epg"]
        assert epg_summary["source_id"] == DUMMY_SOURCE_ID
        assert epg_summary["assigned"] == 1
        assert state.channels[100]["epg_data_id"] == 501


class TestNonClobbering:
    def test_foreign_guide_data_is_never_overwritten(
        self, db_session_factory
    ):
        """A master channel carrying guide data from ANY other source
        (e.g. a hand-assigned real EPG) keeps it."""
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = FakeDispatcharrState(
            channels=[{
                "id": 100, "name": MASTER_MERCURY,
                "channel_group_id": MASTER_GROUP_ID,
                "auto_created": True, "streams": [9001],
                "epg_data_id": 999,  # foreign — not the dummy source's
            }],
            secondary_streams={SECONDARY_GROUP_NAME: [
                {"id": 7001, "name": STREAM_MERCURY, "m3u_account": 1},
            ]},
        )
        client = make_stateful_client(state)
        _epg_store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory,
            initial_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )

        result = _manual_run(
            client, db_session_factory, regenerate, wait_refresh
        )

        epg_summary = result["event_sync"][0]["dummy_epg"]
        assert epg_summary["skipped_foreign_epg"] == 1
        assert epg_summary["assigned"] == 0
        assert state.channels[100]["epg_data_id"] == 999
        # The only write this run is the attach — never an EPG overwrite.
        epg_writes = [
            payload for _, payload in state.update_channel_calls
            if "epg_data_id" in payload
        ]
        assert epg_writes == []


class TestUnattendedRuns:
    def test_auto_run_watermark_task_assigns_like_a_manual_run(
        self, db_session_factory
    ):
        """The bead's 'manual AND unattended' requirement: an opted-in
        (auto_run) rule assigns the profile from the watermark task."""
        import os
        from tasks.channel_pipeline import ChannelPipelineTask

        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config(auto_run=True))
        state = _mercury_state()
        client = make_stateful_client(state)
        _epg_store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory,
            initial_entries=[],
            regenerated_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )

        engine = ChannelPipelineEngine(client)
        settings = MagicMock(
            auto_creation_run_on_refresh_disabled=False,
            last_m3u_refresh_completed_at="2026-01-02T00:00:00+00:00",
            last_auto_creation_consumed_refresh_at="2026-01-01T00:00:00+00:00",
        )
        task_cls = MagicMock()
        task_cls.return_value._regenerate_xmltv = regenerate

        os.environ.pop("ECM_DISABLE_RUN_ON_REFRESH", None)
        with patch("tasks.channel_pipeline.get_settings", return_value=settings), \
             patch("tasks.channel_pipeline.save_settings"), \
             patch("services.notification_service.create_notification_internal",
                   new=AsyncMock()), \
             patch("channel_pipeline_engine.get_channel_pipeline_engine",
                   return_value=engine), \
             patch("channel_pipeline_engine.get_session",
                   side_effect=db_session_factory), \
             patch("database.get_session", side_effect=db_session_factory), \
             patch("services.epg_publication.get_session",
                   side_effect=db_session_factory), \
             patch("tasks.channel_pipeline.get_client", return_value=client), \
             patch("tasks.dummy_epg_refresh.DummyEPGRefreshTask", task_cls), \
             patch("tasks.dummy_epg_refresh.wait_for_epg_source_refresh",
                   wait_refresh), \
             patch("journal.log_entry"), \
             patch("journal.log_entries"):
            task = ChannelPipelineTask()
            task._enabled = True
            result = _run(task.execute())

        assert result.success is True
        # Both the attach AND the guide data landed unattended.
        assert state.stream_ids_of(100) == [9001, 7001]
        assert state.channels[100]["epg_data_id"] == 501
        regenerate.assert_awaited_once()

        assert_never_created_or_deleted_channels(client)
        assert_never_touched_group_settings(client)


class TestGracefulDegradation:
    """EPG-step problems warn and skip ONLY the EPG step — the attach path
    is a safety-relevant feature and must be unaffected."""

    def test_no_dispatcharr_source_for_the_profile_warns_and_still_attaches(
        self, db_session_factory
    ):
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        # A dummy source exists — but for a DIFFERENT profile, and there is
        # no combined source.
        _epg_store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory,
            initial_entries=[],
            source_url="http://ecm:8000/api/dummy-epg/xmltv/99",
        )

        result = _manual_run(
            client, db_session_factory, regenerate, wait_refresh
        )

        assert result["success"] is True
        assert result["event_sync"][0]["attached"] == 1
        epg_summary = result["event_sync"][0]["dummy_epg"]
        assert epg_summary["source_id"] is None
        warnings = [
            w for w in _latest_execution_warnings(db_session_factory)
            if w["type"] == "event_sync_dummy_epg_no_source"
        ]
        assert len(warnings) == 1
        assert f"/api/dummy-epg/xmltv/{PROFILE_ID}" in warnings[0]["message"]
        assert "epg_data_id" not in state.channels[100]

    def test_disabled_profile_warns_skips_epg_step_and_still_attaches(
        self, db_session_factory
    ):
        """A disabled profile is a run-time warning, never a rule-killing
        validation error — attaches are unaffected."""
        _add_profile(db_session_factory, enabled=False)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        _epg_store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory,
            initial_entries=[_dummy_entry(501, 100, MASTER_MERCURY)],
        )

        result = _manual_run(
            client, db_session_factory, regenerate, wait_refresh
        )

        assert result["success"] is True
        assert result["event_sync"][0]["attached"] == 1
        assert "dummy_epg" not in result["event_sync"][0]
        warnings = [
            w for w in _latest_execution_warnings(db_session_factory)
            if w["type"] == "event_sync_dummy_epg_profile_disabled"
        ]
        assert len(warnings) == 1
        assert "epg_data_id" not in state.channels[100]

    def test_dry_run_defers_reports_and_writes_nothing(
        self, db_session_factory
    ):
        _add_profile(db_session_factory)
        _add_event_rule(db_session_factory, _config())
        state = _mercury_state()
        client = make_stateful_client(state)
        _epg_store, regenerate, wait_refresh = _wire_epg(
            state, client, db_session_factory, initial_entries=[],
        )

        result = _manual_run(
            client, db_session_factory, regenerate, wait_refresh,
            dry_run=True,
        )

        assert result["success"] is True
        epg_summary = result["event_sync"][0]["dummy_epg"]
        assert epg_summary["deferred"] == 1
        # Dry run: no writes, no XMLTV regeneration, no profile edit.
        assert state.update_channel_calls == []
        regenerate.assert_not_awaited()
        assert _profile_group_ids(db_session_factory) == []
        # Pass 5 reported its would-do steps on the dry-run surface.
        assert any(
            str(r.get("stream_name", "")).startswith("[Pass 5")
            for r in result["dry_run_results"]
        )


class TestDedicatedGuide:
    def test_deferred_assignment_keeps_target_only_profile(self, db_session_factory, monkeypatch):
        from services import epg_publication
        from tests.unit.test_event_sync_promotion import _pending_completion

        with patch("services.epg_publication.add_groups", wraps=epg_publication.add_groups) as add_groups:
            setup, executor, _ = _pending_completion(db_session_factory, monkeypatch, dedicated=True)
        add_groups.assert_not_called()
        assert executor._combined_dummy_source_ids == []
        assert executor._dummy_source_by_profile[setup["profile_id"]] == setup["source_id"]
        assert setup["state"].channels[900]["epg_data_id"] == 502
        assert "epg_data_id" not in setup["state"].channels[100]
        assert setup["state"].source_refresh_ids
        assert set(setup["state"].source_refresh_ids) == {setup["source_id"]}

    @pytest.mark.parametrize("fault", ["pending_lost", "extra_group", "wrong_source", "combined_source", "outside_target"])
    def test_deferred_scope_drift_fails_without_profile_repair(self, db_session_factory, monkeypatch, fault):
        from channel_pipeline_executor import ExecutionContext
        from channel_pipeline_evaluator import StreamContext
        from channel_pipeline_schema import Action, ActionType
        from tests.unit.test_event_sync_promotion import _pending_completion

        setup, executor, _ = _pending_completion(db_session_factory, monkeypatch, dedicated=True)
        channel_id = 900
        source_id = setup["source_id"]
        action = Action(type=ActionType.ASSIGN_EPG, params={"epg_id": source_id})
        context = ExecutionContext(current_channel_id=channel_id)
        executor._deferred_epg_assignments = [(channel_id, action, StreamContext(stream_id=7301, stream_name=setup["event_name"]), context)]
        executor._deferred_epg_profiles[channel_id] = setup["profile_id"]
        if fault == "pending_lost":
            executor._event_pending.clear()
        elif fault == "extra_group":
            session = db_session_factory()
            try:
                session.get(DummyEPGProfile, setup["profile_id"]).set_channel_group_ids([40, MASTER_GROUP_ID])
                session.commit()
            finally:
                session.close()
        elif fault == "wrong_source":
            executor._dummy_source_by_profile[setup["profile_id"]] = 999
        elif fault == "combined_source":
            executor._epg_sources[0]["url"] = "http://ecm.test/api/dummy-epg/xmltv"
        else:
            setup["state"].channels[channel_id]["channel_group_id"] = MASTER_GROUP_ID
        engine = ChannelPipelineEngine(setup["client"])
        results = _refresh_results()
        results.update({"execution_log": [], "dry_run_results": []})
        before = copy.deepcopy(setup["state"].channels)
        with patch("database.get_session", side_effect=db_session_factory), patch(
            "channel_pipeline_engine.get_session", side_effect=db_session_factory
        ), patch("services.epg_publication.get_session", side_effect=db_session_factory), patch(
            "services.epg_publication.add_groups"
        ) as add_groups:
            if fault == "extra_group":
                with pytest.raises(ValueError, match="Dedicated deferred guide configuration"):
                    _run(engine._refresh_dummy_epg_and_retry(executor, results, executor._epg_sources, False))
            else:
                _run(engine._refresh_dummy_epg_and_retry(executor, results, executor._epg_sources, False))
        add_groups.assert_not_called()
        assert setup["state"].channels == before
        if fault != "extra_group":
            assert results["failed_actions"]
