"""Refresh polling and task entry points share confirmed workflow outcomes."""
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.epg_publication import PublicationResult, _config_hash
from task_scheduler import TaskResult
from tasks.dummy_epg_refresh import DummyEPGRefreshTask, wait_for_epg_source_refresh
from tasks.event_visibility import EventVisibilityTask


EXPIRES_AT = datetime.now(timezone.utc) + timedelta(hours=1)


@pytest.fixture
def phase_source(monkeypatch):
    import copy
    from types import SimpleNamespace
    from tests.tasks.test_event_visibility import _publication

    source = {"id": 46, "name": "Guide", "url": "http://ecm/api/dummy-epg/xmltv/1",
              "status": "ready", "updated_at": "initial"}
    channels = {10: {"id": 10, "uuid": "channel-10", "channel_group_id": 7, "epg_data_id": None}}
    stored = _publication("profile:1", pending=False, channels=[{"channel_id": 10, "events": []}])
    client = MagicMock(base_url="http://dispatcharr.local")
    client.get_epg_source = AsyncMock(side_effect=lambda source_id: copy.deepcopy(source))
    client.get_channels = AsyncMock(side_effect=lambda **_kwargs: copy.deepcopy(list(channels.values())))
    imports = []

    async def dispatch(source_id):
        imports.append(channels[10]["epg_data_id"])
        source["status"] = "processing"

    client.refresh_epg_source = AsyncMock(side_effect=dispatch)

    def update(scope, **claims):
        if claims["expected_revision"] != stored["revision"]:
            return None
        for name in ("source_refreshes", "required_dispatcharr_hashes", "confirmed_dispatcharr_hashes"):
            if name in claims:
                stored["state"]["delivery"][name] = copy.deepcopy(claims[name])
        stored["revision"] += 1
        return stored["revision"]

    monkeypatch.setattr("services.epg_publication.read_publication", lambda scope: copy.deepcopy(stored))
    monkeypatch.setattr("services.epg_publication.update_delivery", update)
    return SimpleNamespace(source=source, channels=channels, stored=stored,
                           client=client, imports=imports)


@pytest.mark.asyncio
async def test_programme_phase_survives_reconstructed_callers(phase_source):
    import copy
    from services.epg_publication import refresh_source

    setup = phase_source

    async def call(after_link=False):
        return await refresh_source(
            setup.client, setup.source, {1: copy.deepcopy(setup.stored)},
            expires_at=None, after_link=after_link, channel_map=setup.channels, wait=False,
        )

    assert await call() is False
    assert await call() is False
    assert setup.imports == [None]
    setup.source.update(status="success", updated_at="headers")
    assert await call() is True
    assert setup.stored["state"]["delivery"]["confirmed_dispatcharr_hashes"] == {}
    setup.channels[10]["epg_data_id"] = 900
    assert await call(True) is False
    assert await call(True) is False
    assert setup.imports == [None, 900]
    setup.source.update(status="success", updated_at="programmes")
    assert await call(True) is True
    setup.source["status"] = "ready"
    assert await call(True) is True
    assert setup.imports == [None, 900]
    progress = next(iter(setup.stored["state"]["delivery"]["source_refreshes"].values()))
    assert progress["links"] == {"10": 900}
    assert progress["completed"] is True
    assert progress["pending_links"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("predecessor", ["headers", "programmes"])
async def test_successor_keeps_latest_bindings_and_its_own_proof(phase_source, predecessor):
    import copy
    from services.epg_publication import refresh_source

    setup = phase_source

    async def call(after_link):
        return await refresh_source(setup.client, setup.source, {1: copy.deepcopy(setup.stored)},
                                    expires_at=None, after_link=after_link,
                                    channel_map=setup.channels, wait=False)

    if predecessor == "programmes":
        setup.channels[10]["epg_data_id"] = 800
    assert await call(predecessor == "programmes") is False
    for link in (900, 901, 902):
        setup.channels[10]["epg_data_id"] = link
        assert await call(True) is False
        assert await call(True) is False
    assert len(setup.imports) == 1
    progress = next(iter(setup.stored["state"]["delivery"]["source_refreshes"].values()))
    assert progress["pending_links"] == {"10": 902}
    setup.source.update(status="success", updated_at="predecessor")
    assert await call(True) is False
    assert setup.imports[-1] == 902
    progress = next(iter(setup.stored["state"]["delivery"]["source_refreshes"].values()))
    assert progress["initial_updated"] == "predecessor"
    assert progress["completed"] is False
    assert setup.stored["state"]["delivery"]["confirmed_dispatcharr_hashes"] == {}
    setup.source.update(status="success", updated_at="successor")
    assert await call(True) is True
    assert len(setup.imports) == 2


@pytest.mark.asyncio
async def test_unrelated_import_cannot_complete_an_untriggered_phase(phase_source):
    import copy
    from services.epg_publication import refresh_source

    setup = phase_source
    setup.source["status"] = "processing"

    async def call():
        return await refresh_source(setup.client, setup.source, {1: copy.deepcopy(setup.stored)},
                                    expires_at=None, channel_map=setup.channels, wait=False)

    assert await call() is False
    assert setup.imports == []
    progress = next(iter(setup.stored["state"]["delivery"]["source_refreshes"].values()))
    assert progress["triggered"] is False
    assert progress["observed_running"] is False
    setup.source.update(status="success", updated_at="unrelated")
    assert await call() is False
    assert setup.imports == [None]
    progress = next(iter(setup.stored["state"]["delivery"]["source_refreshes"].values()))
    assert progress["initial_updated"] == "unrelated"
    assert progress["completed"] is False


@pytest.mark.asyncio
async def test_incomplete_bindings_remove_confirmation_without_retrigger(phase_source):
    import copy
    from services.epg_publication import refresh_source

    setup = phase_source
    setup.channels[10]["epg_data_id"] = 900

    async def call():
        return await refresh_source(setup.client, setup.source, {1: copy.deepcopy(setup.stored)},
                                    expires_at=None, after_link=True,
                                    channel_map=setup.channels, wait=False)

    assert await call() is False
    setup.source.update(status="success", updated_at="programmes")
    assert await call() is True
    setup.channels[10].pop("epg_data_id")
    assert await call() is False
    assert setup.stored["state"]["delivery"]["confirmed_dispatcharr_hashes"] == {}
    setup.channels[10]["epg_data_id"] = 900
    assert await call() is True
    assert setup.imports == [900]
    setup.channels[10]["epg_data_id"] = None
    assert await call() is False
    setup.source.update(status="success", updated_at="empty-programmes")
    assert await call() is True
    progress = next(iter(setup.stored["state"]["delivery"]["source_refreshes"].values()))
    assert progress["links"] == {}
    assert setup.imports == [900, None]


@pytest.mark.asyncio
async def test_failed_predecessor_releases_only_a_distinct_successor(phase_source):
    import copy
    from services.epg_publication import refresh_source

    setup = phase_source

    async def call(after_link):
        return await refresh_source(setup.client, setup.source, {1: copy.deepcopy(setup.stored)},
                                    expires_at=None, after_link=after_link,
                                    channel_map=setup.channels, wait=False)

    assert await call(False) is False
    setup.channels[10]["epg_data_id"] = 900
    assert await call(True) is False
    setup.source.update(status="failed", updated_at="failed-header")
    assert await call(True) is False
    assert setup.imports == [None, 900]
    setup.source.update(status="failed", updated_at="failed-programmes")
    for _ in range(2):
        assert await call(True) is False
    assert setup.imports == [None, 900]


@pytest.mark.asyncio
async def test_cancelled_phase_resumes_accepted_work_without_retrigger(phase_source):
    import copy
    from services.epg_publication import refresh_source

    setup = phase_source
    assert await refresh_source(setup.client, setup.source, {1: copy.deepcopy(setup.stored)},
                                expires_at=None, channel_map=setup.channels, wait=False) is False
    cancelled = {"value": False}
    observed = asyncio.Event()
    read = setup.client.get_epg_source.side_effect

    async def source(source_id):
        observed.set()
        return read(source_id)

    setup.client.get_epg_source.side_effect = source
    pending = asyncio.create_task(refresh_source(
        setup.client, setup.source, {1: copy.deepcopy(setup.stored)}, expires_at=None,
        channel_map=setup.channels, wait=True, cancelled=lambda: cancelled["value"],
    ))
    await asyncio.wait_for(observed.wait(), timeout=1)
    cancelled["value"] = True
    assert await asyncio.wait_for(pending, timeout=1) is False
    assert setup.imports == [None]
    assert setup.stored["state"]["delivery"]["confirmed_dispatcharr_hashes"] == {}
    setup.source.update(status="success", updated_at="accepted")
    assert await refresh_source(setup.client, setup.source, {1: copy.deepcopy(setup.stored)},
                                expires_at=None, channel_map=setup.channels, wait=False) is True
    assert setup.imports == [None]


def admitted_publication(profile, *, expires_at=None):
    return {
        "scope": f"profile:{profile['id']}",
        "xmltv": '<?xml version="1.0"?><tv></tv>',
        "revision": 3,
        "state": {
            "xmltv_hash": "a" * 64,
            "config_hash": _config_hash(profile),
            "delivery": {
                "guide_attempt": {
                    "attempt_id": "1" * 32,
                    "config_hash": _config_hash(profile),
                    "admitted_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
                    "expires_at": expires_at.isoformat() if expires_at is not None else None,
                    "stage": "preparing",
                },
                "pending_channels": {},
            },
        },
    }


@pytest.mark.asyncio
async def test_unchanged_timestamp_never_means_finished():
    client = MagicMock()
    client.get_epg_source = AsyncMock(return_value={"updated_at": "old", "status": "success"})
    client.refresh_epg_source = AsyncMock()
    assert not await wait_for_epg_source_refresh(
        client, 1, "Guide", expires_at=EXPIRES_AT, wait=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["processing", "error", "failed"])
async def test_changed_timestamp_requires_successful_state(status):
    client = MagicMock()
    client.get_epg_source = AsyncMock(side_effect=[
        {"updated_at": "old", "status": "success"},
        {"updated_at": "new", "status": status},
    ])
    client.refresh_epg_source = AsyncMock()
    assert not await wait_for_epg_source_refresh(
        client, 1, "Guide", expires_at=EXPIRES_AT, wait=False,
    )


@pytest.mark.asyncio
async def test_successful_transition_completes_without_timestamp():
    client = MagicMock()
    client.get_epg_source = AsyncMock(side_effect=[
        {"status": "success"}, {"status": "processing"}, {"status": "success"},
    ])
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()):
        assert await wait_for_epg_source_refresh(
            client, 1, "Guide", poll_interval=0, expires_at=EXPIRES_AT,
        )
    client.refresh_epg_source.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_cancelled_wait_does_not_trigger_refresh():
    client = MagicMock()
    client.refresh_epg_source = AsyncMock()
    assert not await wait_for_epg_source_refresh(
        client, 1, "Guide", expires_at=EXPIRES_AT, cancelled=lambda: True,
    )
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["expired", "cancelled", "live"])
@pytest.mark.parametrize("wait", [False, True])
async def test_source_observation_respects_expiry_and_cancellation(outcome, wait):
    from tests.unit.test_event_sync_promotion import _clock

    admitted_at = datetime.now(timezone.utc)
    expires_at = admitted_at + timedelta(seconds=30)
    clock = {"now": admitted_at, "cancelled": False}
    client = MagicMock()

    async def read(source_id):
        if client.get_epg_source.await_count == 1:
            clock["now"] += timedelta(seconds=10)
            return {"updated_at": "old", "status": "success"}
        if outcome == "expired":
            clock["now"] = expires_at + timedelta(seconds=1)
        elif outcome == "cancelled":
            clock["cancelled"] = True
        return {"updated_at": "new", "status": "success"}

    client.get_epg_source = AsyncMock(side_effect=read)
    client.refresh_epg_source = AsyncMock()
    progress = {}
    read_wait = AsyncMock(wraps=asyncio.wait_for)

    with patch("tasks.dummy_epg_refresh.datetime", new=_clock(lambda: clock["now"])), \
         patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()), \
         patch("tasks.dummy_epg_refresh.asyncio.wait_for", new=read_wait):
        completed = await wait_for_epg_source_refresh(
            client,
            1,
            "Guide",
            poll_interval=0,
            expires_at=expires_at,
            cancelled=lambda: clock["cancelled"],
            progress=progress,
            wait=wait,
        )

    if outcome == "live":
        assert completed is True
    else:
        assert completed is False
    assert client.get_epg_source.await_count == 2
    client.refresh_epg_source.assert_awaited_once_with(1)
    assert progress["triggered"] is True
    assert [call.kwargs["timeout"] for call in read_wait.await_args_list] == [30, 20]


@pytest.mark.asyncio
async def test_waiting_for_existing_refresh_does_not_trigger_it_again():
    client = MagicMock()
    client.get_epg_source = AsyncMock(return_value={"updated_at": "new", "status": "success"})
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()):
        assert await wait_for_epg_source_refresh(
            client, 1, "Guide", expires_at=EXPIRES_AT,
            initial_source={"updated_at": "old"}, trigger=False,
        )
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_processing_state_can_complete_without_timestamps():
    client = MagicMock()
    client.get_epg_source = AsyncMock(return_value={"status": "success"})
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.asyncio.sleep", new=AsyncMock()):
        assert await wait_for_epg_source_refresh(
            client, 1, "Guide", expires_at=EXPIRES_AT,
            initial_source={"status": "processing"}, trigger=False,
        )
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_source_lifetime_never_triggers_refresh():
    client = MagicMock()
    client.get_epg_source = AsyncMock()
    client.refresh_epg_source = AsyncMock()

    completed = await wait_for_epg_source_refresh(
        client,
        1,
        "Guide",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    assert completed is False
    client.get_epg_source.assert_not_awaited()
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_unbounded_import_retains_progress_across_restart():
    import json

    client = MagicMock(base_url="http://dispatcharr.local")
    initial = {"id": 1, "url": "http://guide.local/events.xml", "status": "ready"}
    client.get_epg_source = AsyncMock(return_value={**initial, "status": "processing"})
    client.refresh_epg_source = AsyncMock()
    progress = {}
    assert await wait_for_epg_source_refresh(
        client, 1, "Guide", expires_at=None, initial_source=initial,
        progress=progress, wait=False,
    ) is False
    progress = json.loads(json.dumps(progress))
    client.get_epg_source.return_value = {**initial, "status": "success"}
    assert await wait_for_epg_source_refresh(
        client, 1, "Guide", expires_at=None, initial_source=initial,
        progress=progress, wait=False,
    ) is True
    client.refresh_epg_source.assert_awaited_once_with(1)
    assert progress["observed_running"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["none", "endpoint", "url", "id"])
async def test_unbounded_import_rechecks_identity_after_long_wait(change):
    from tests.unit.test_event_sync_promotion import _clock

    clock = {"now": datetime.now(timezone.utc)}
    client = MagicMock(base_url="http://dispatcharr.local")
    initial = {"id": 1, "url": "http://guide.local/events.xml", "status": "processing"}

    async def read(source_id):
        clock["now"] += timedelta(days=2)
        current = {**initial, "status": "success"}
        if change == "endpoint":
            client.base_url = "http://other.local"
        elif change == "url":
            current["url"] = "http://guide.local/other.xml"
        elif change == "id":
            current["id"] = 2
        return current

    client.get_epg_source = AsyncMock(side_effect=read)
    client.refresh_epg_source = AsyncMock()
    with patch("tasks.dummy_epg_refresh.datetime", new=_clock(lambda: clock["now"])):
        completed = await wait_for_epg_source_refresh(
            client, 1, "Guide", expires_at=None, initial_source=initial,
            trigger=False, poll_interval=0,
        )
    assert completed is (change == "none")
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_unbounded_import_cancels_owned_observation():
    started = asyncio.Event()
    stopped = asyncio.Event()
    cancel = {"requested": False}

    async def read(source_id):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    client = MagicMock()
    client.get_epg_source = AsyncMock(side_effect=read)
    client.refresh_epg_source = AsyncMock()
    waiting = asyncio.create_task(wait_for_epg_source_refresh(
        client, 1, "Guide", expires_at=None, initial_source={"status": "processing"},
        trigger=False, poll_interval=0, cancelled=lambda: cancel["requested"],
    ))
    await asyncio.wait_for(started.wait(), timeout=1)
    cancel["requested"] = True
    assert await asyncio.wait_for(waiting, timeout=1) is False
    assert stopped.is_set()
    client.refresh_epg_source.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task, wait_for_sources",
    [(EventVisibilityTask(), False), (DummyEPGRefreshTask(), True)],
)
async def test_both_tasks_use_the_shared_reconciliation(task, wait_for_sources):
    expected = TaskResult(success=True, message="done")
    shared = AsyncMock(return_value=expected)

    with patch("tasks.event_visibility.reconcile_profiles", new=shared):
        result = await task.execute()

    assert result is expected
    shared.assert_awaited_once_with(task, wait_for_sources=wait_for_sources)


@pytest.mark.asyncio
async def test_publication_only_entry_returns_explicit_result_and_updates_cache_after_commit():
    profile = MagicMock()
    saved_profile = {"id": 7, "enabled": True, "epg_source_ids": []}
    profile.to_dict.return_value = saved_profile
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [profile]
    client = MagicMock()
    channels = {9: {"id": 9, "name": "Station", "channel_group_id": 4}}
    prepared = [{**saved_profile, "channel_assignments": []}]
    coverage = {"profiles": {"7": {
        "owned_channel_ids": [], "can_publish": True, "reason_codes": [],
    }}}
    publication = PublicationResult(
        published_profile_ids=(7,),
        xmltv_by_scope={"all": "<tv/>", "profile:7": "<tv/>"},
    )
    cache = MagicMock()

    with patch("database.get_session", return_value=session), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=client), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=(prepared, coverage))), \
         patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=publication)) as publish, \
         patch("cache.get_cache", return_value=cache):
        result = await DummyEPGRefreshTask()._regenerate_xmltv(
            publications={7: admitted_publication(saved_profile)},
        )

    assert result is publication
    publish.assert_awaited_once()
    cache.invalidate_prefix.assert_called_once_with("dummy_epg_xmltv")
    cache.set.assert_any_call("dummy_epg_xmltv_all", "<tv/>")
    cache.set.assert_any_call("dummy_epg_xmltv_7", "<tv/>")


@pytest.mark.asyncio
async def test_superseded_publication_does_not_update_cache():
    profile = MagicMock()
    saved_profile = {"id": 7, "enabled": True, "epg_source_ids": []}
    profile.to_dict.return_value = saved_profile
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [profile]
    publication = PublicationResult(superseded=True, reason_codes=("GUIDE_PUBLICATION_SUPERSEDED",))
    cache = MagicMock()

    with patch("database.get_session", return_value=session), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=MagicMock()), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=([saved_profile], {
             "profiles": {"7": {
                 "owned_channel_ids": [], "can_publish": True, "reason_codes": [],
             }},
         }))), patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=publication)), \
         patch("cache.get_cache", return_value=cache):
        result = await DummyEPGRefreshTask()._regenerate_xmltv(
            publications={7: admitted_publication(saved_profile)},
        )

    assert result.superseded is True
    cache.invalidate_prefix.assert_not_called()
    cache.set.assert_not_called()


def test_dummy_task_has_no_duplicate_visibility_writer():
    assert not hasattr(DummyEPGRefreshTask, "_apply_empty_channel_visibility")


@pytest.mark.asyncio
async def test_regeneration_passes_each_stored_profile_expiry():
    saved = [
        {"id": 7, "enabled": True, "epg_source_ids": []},
        {"id": 8, "enabled": True, "epg_source_ids": []},
    ]
    rows = [MagicMock(to_dict=MagicMock(return_value=profile)) for profile in saved]
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = rows
    prepare = AsyncMock(side_effect=lambda profiles, *args, **kwargs: (
        profiles,
        {"profiles": {str(profiles[0]["id"]): {
            "owned_channel_ids": [], "can_publish": True, "reason_codes": [],
        }}},
    ))
    first_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    second_expiry = datetime.now(timezone.utc) + timedelta(hours=2)
    publish = AsyncMock(return_value=PublicationResult())

    with patch("database.get_session", return_value=session), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=MagicMock()), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=prepare), \
         patch("concurrency.run_cpu_bound", new=publish), \
         patch("cache.get_cache"):
        await DummyEPGRefreshTask()._regenerate_xmltv(
            publications={
                7: admitted_publication(saved[0], expires_at=first_expiry),
                8: admitted_publication(saved[1], expires_at=second_expiry),
            },
            wait_for_sources=False,
        )

    assert [call.kwargs["expires_at"] for call in prepare.await_args_list] == [
        first_expiry,
        second_expiry,
    ]
    assert all(
        call.kwargs["wait_for_sources"] is False
        and call.kwargs["recover_sources"] is True
        for call in prepare.await_args_list
    )
    expected = publish.await_args.kwargs["expected"]
    assert expected["profile:7"]["attempt_id"] == "1" * 32
    assert expected["profile:8"]["attempt_id"] == "1" * 32
    assert expected["profile:7"]["revision"] == 3
    assert expected["profile:8"]["revision"] == 3


@pytest.mark.asyncio
async def test_expired_profile_starts_no_preparation_while_eligible_profile_progresses():
    saved = [
        {"id": 7, "enabled": True, "epg_source_ids": [70]},
        {"id": 8, "enabled": True, "epg_source_ids": [80]},
    ]
    rows = [MagicMock(to_dict=MagicMock(return_value=profile)) for profile in saved]
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = rows
    prepare = AsyncMock(return_value=(
        [saved[1]],
        {"profiles": {"8": {
            "owned_channel_ids": [], "can_publish": True, "reason_codes": [],
        }}},
    ))
    expired = datetime.now(timezone.utc) - timedelta(seconds=1)
    eligible = datetime.now(timezone.utc) + timedelta(hours=1)

    with patch("database.get_session", return_value=session), \
         patch("tasks.dummy_epg_refresh.get_client", return_value=MagicMock()), \
         patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value={})), \
         patch("services.epg_programmes.prepare_profiles", new=prepare), \
         patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=PublicationResult(
             published_profile_ids=(8,), unavailable_profile_ids=(7,),
         ))), patch("cache.get_cache"):
        result = await DummyEPGRefreshTask()._regenerate_xmltv(
            publications={
                7: admitted_publication(saved[0], expires_at=expired),
                8: admitted_publication(saved[1], expires_at=eligible),
            },
            wait_for_sources=False,
        )

    assert result.published_profile_ids == (8,)
    prepare.assert_awaited_once()
    assert prepare.await_args.args[0] == [saved[1]]
    assert prepare.await_args.kwargs["expires_at"] == eligible


@pytest.mark.asyncio
async def test_blocking_drain_keeps_requested_phase_separate(phase_source, monkeypatch):
    import copy
    from services.epg_publication import refresh_source

    setup = phase_source
    setup.source["status"] = "processing"
    reads = 0

    async def read(source_id):
        nonlocal reads
        reads += 1
        if setup.imports:
            setup.source.update(status="success", updated_at="requested")
        elif reads >= 3:
            setup.source.update(status="success", updated_at="unrelated")
        return copy.deepcopy(setup.source)

    async def observe(*args, **kwargs):
        return await wait_for_epg_source_refresh(*args, **kwargs, poll_interval=0)

    setup.client.get_epg_source.side_effect = read
    monkeypatch.setattr("tasks.dummy_epg_refresh.wait_for_epg_source_refresh", observe)
    assert await refresh_source(
        setup.client, setup.source, {1: copy.deepcopy(setup.stored)},
        expires_at=None, channel_map=setup.channels, wait=True,
    ) is True
    assert setup.imports == [None]
    progress = next(iter(setup.stored["state"]["delivery"]["source_refreshes"].values()))
    assert progress["initial_updated"] == "unrelated"
    assert progress["completed"] is True
    assert setup.stored["state"]["delivery"]["confirmed_dispatcharr_hashes"] == {}
