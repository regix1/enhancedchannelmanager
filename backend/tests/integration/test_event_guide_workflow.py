"""Immutable-base control for retained guide reconciliation."""
import copy
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _config():
    return {
        "secondary": [],
        "time_window_minutes": 30,
        "enforce_time_window": True,
        "attach_threshold": 0.8,
        "assume_current_date": True,
        "demote_stale_dateless": True,
        "use_default_patterns": True,
        "slot_patterns": [],
    }


def _profile(profile_id, group_id, source_ids):
    values = {
        "id": profile_id,
        "name": f"Guide {profile_id}",
        "enabled": True,
        "channel_group_ids": [group_id],
        "hide_empty_group_ids": [group_id],
        "stream_match_group_ids": [],
        "event_sync_config": _config(),
        "event_timezone": "UTC",
        "output_timezone": "UTC",
        "program_duration": 180,
        "pattern_variants": [],
        "epg_source_ids": source_ids,
        "tvg_id_template": "event-{channel_id}",
        "channel_assignments": [{
            "channel_id": profile_id * 10,
            "channel_name": f"Arena {profile_id}",
        }],
    }
    return values


def _stored(scope, document, *, channels=None):
    return {
        "scope": scope,
        "xmltv": document,
        "revision": 1,
        "state": {
            "published_at": "2026-09-19T10:00:00+00:00",
            "xmltv_hash": "b" * 64,
            "channels": list(channels or []),
            "delivery": {
                "required_dispatcharr_hashes": {},
                "confirmed_dispatcharr_hashes": {},
                "pending_emby": True,
            },
        },
    }


@pytest.mark.asyncio
async def test_retained_placeholder_continues_safe_profile_work():
    """The immutable base fails this fixed-behavior assertion at its early return."""
    from tasks import dummy_epg_refresh
    from tasks import event_visibility

    retained_xml = "<tv><channel id='event-10'/></tv>"
    profiles = [
        _profile(1, 7, [100]),
        _profile(2, 8, []),
    ]
    rows = [SimpleNamespace(to_dict=lambda value=value: dict(value), enabled=True)
            for value in profiles]
    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = rows
    channels = {
        10: {
            "id": 10,
            "name": "Arena 1",
            "channel_group_id": 7,
            "hidden_from_output": False,
            "epg_data_id": 500,
            "streams": [],
        },
        20: {
            "id": 20,
            "name": "Arena 2",
            "channel_group_id": 8,
            "hidden_from_output": False,
            "epg_data_id": None,
            "streams": [],
        },
    }
    coverage = {
        "sources": [{
            "source_id": 100,
            "status": "error",
            "last_success": None,
        }],
        "profiles": {
            "1": {"profile_id": 1, "can_publish": False, "reason_codes": ["GUIDE_SOURCES_PENDING"]},
            "2": {"profile_id": 2, "can_publish": True, "reason_codes": []},
        },
        "channels": [
            {"profile_id": 1, "channel_id": 10, "current": None},
            {"profile_id": 2, "channel_id": 20, "current": None},
        ],
    }
    client = MagicMock()
    client.get_epg_sources = AsyncMock(return_value=[])
    client.update_channel = AsyncMock()
    cache = MagicMock()
    emby = AsyncMock(return_value=True)

    with ExitStack() as stack:
        stack.enter_context(patch("database.get_session", return_value=session))
        stack.enter_context(patch("tasks.dummy_epg_refresh.get_client", return_value=client))
        stack.enter_context(patch("services.epg_programmes._fetch_all_channels", new=AsyncMock(return_value=channels)))
        stack.enter_context(patch("services.epg_programmes.prepare_profiles", new=AsyncMock(return_value=(copy.deepcopy(profiles), coverage))))
        stack.enter_context(patch("cache.get_cache", return_value=cache))
        stack.enter_context(patch("emby_client.request_guide_refresh", new=emby))

        if hasattr(event_visibility, "reconcile_profiles"):
            from services.epg_publication import PublicationResult

            result = PublicationResult(
                published_profile_ids=(2,),
                retained_profile_ids=(1,),
                xmltv_by_scope={
                    "all": "<tv><channel id='event-10'/><channel id='event-20'/></tv>",
                    "profile:1": retained_xml,
                    "profile:2": "<tv><channel id='event-20'/></tv>",
                },
                reason_codes=("GUIDE_SOURCES_PENDING",),
            )
            publications = {
                "all": _stored("all", result.xmltv_by_scope["all"]),
                "profile:1": _stored("profile:1", retained_xml, channels=[{
                    "channel_id": 10,
                    "events": [{
                        "start": "2020-01-01T00:00:00+00:00",
                        "stop": "2030-01-01T00:00:00+00:00",
                    }],
                }]),
                "profile:2": _stored("profile:2", result.xmltv_by_scope["profile:2"]),
            }

            def read(scope):
                return publications.get(scope)

            def update(scope, *, expected_revision, required_dispatcharr_hashes=None,
                       confirmed_dispatcharr_hashes=None, pending_emby=None):
                row = publications[scope]
                if row["revision"] != expected_revision:
                    return None
                if required_dispatcharr_hashes is not None:
                    row["state"]["delivery"]["required_dispatcharr_hashes"] = dict(required_dispatcharr_hashes)
                if confirmed_dispatcharr_hashes is not None:
                    row["state"]["delivery"]["confirmed_dispatcharr_hashes"] = dict(confirmed_dispatcharr_hashes)
                if pending_emby is not None:
                    row["state"]["delivery"]["pending_emby"] = pending_emby
                row["revision"] += 1
                return row["revision"]

            stack.enter_context(patch("tasks.event_visibility._load_profiles", side_effect=[
                (copy.deepcopy(profiles), []), (copy.deepcopy(profiles), []),
            ]))
            stack.enter_context(patch("tasks.event_visibility.get_client", return_value=client))
            stack.enter_context(patch("tasks.event_visibility._fetch_match_streams", new=AsyncMock(return_value=([], set(), {}))))
            stack.enter_context(patch("concurrency.run_cpu_bound", new=AsyncMock(return_value=result)))
            stack.enter_context(patch("services.epg_publication.read_publication", side_effect=read))
            stack.enter_context(patch("services.epg_publication.update_delivery", side_effect=update))

        outcome = await dummy_epg_refresh.DummyEPGRefreshTask().execute()

    assert outcome.success is False
    assert outcome.completed_degraded is True
    assert outcome.details["retained_profile_ids"] == [1]
    assert outcome.details["published_profile_ids"] == [2]
    assert outcome.details["hidden_channel_ids"] == [20]
    assert client.update_channel.await_args_list == [
        ((20, {"hidden_from_output": True}),),
    ]
    cache.set.assert_any_call("dummy_epg_xmltv_1", retained_xml)
    emby.assert_awaited_once_with()
