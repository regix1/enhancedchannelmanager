"""Publish and deliver generated guide documents through the shared workflow."""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Mapping, Optional

from dispatcharr_client import get_client
from task_registry import register_task
from task_scheduler import ScheduleConfig, ScheduleType, TaskResult, TaskScheduler

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 5
MAX_WAIT_SECONDS = 300


async def wait_for_epg_source_refresh(
    client,
    source_id: int,
    source_name: str,
    poll_interval: int = POLL_INTERVAL_SECONDS,
    *,
    expires_at: datetime,
    initial_source: dict | None = None,
    trigger: bool = True,
    cancelled: Callable[[], bool] | None = None,
    progress: dict | None = None,
    wait: bool = True,
) -> bool:
    """Trigger or observe a source refresh, returning only confirmed completion."""
    if not isinstance(expires_at, datetime) or expires_at.tzinfo is None or expires_at.utcoffset() is None:
        raise ValueError("expires_at must be a datetime with an offset.")
    expires_at = expires_at.astimezone(timezone.utc)
    progress = progress if progress is not None else {}
    if cancelled is not None and cancelled():
        return False
    if datetime.now(timezone.utc) >= expires_at:
        return False
    if initial_source is None:
        initial_source = await client.get_epg_source(source_id)
    if "initial_updated" not in progress:
        progress["initial_updated"] = (
            initial_source.get("updated_at") or initial_source.get("last_updated")
        )
    if cancelled is not None and cancelled():
        return False
    if trigger and progress.get("triggered") is not True:
        if datetime.now(timezone.utc) >= expires_at:
            return False
        await client.refresh_epg_source(source_id)
        progress["triggered"] = True

    running_states = {
        "fetching", "processing", "parsing", "loading", "pending",
        "running", "queued", "refreshing",
    }
    if str(initial_source.get("status") or "").strip().lower() in running_states:
        progress["observed_running"] = True
    while True:
        if cancelled is not None and cancelled():
            return False
        remaining = (expires_at - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            logger.warning("[EPG-REFRESH] Timeout waiting for source %s", source_id)
            return False
        if wait:
            await asyncio.sleep(min(max(0, poll_interval), remaining))
        if cancelled is not None and cancelled():
            return False
        if datetime.now(timezone.utc) >= expires_at:
            return False
        current_source = await client.get_epg_source(source_id)
        status = str(current_source.get("status") or "").strip().lower()
        current_updated = current_source.get("updated_at") or current_source.get("last_updated")
        if status in {"error", "failed", "failure", "cancelled", "canceled"}:
            logger.warning("[EPG-REFRESH] Source %s ended with status %s", source_id, status)
            return False
        if status in running_states:
            progress["observed_running"] = True
            if not wait:
                return False
            continue
        succeeded = status in {"success", "completed", "complete", "ok", "done"}
        changed = bool(
            current_updated and current_updated != progress.get("initial_updated")
        )
        if (succeeded and (changed or progress.get("observed_running") is True)) or (not status and changed):
            logger.info("[EPG-REFRESH] Source %s refresh complete", source_id)
            return True
        if not wait:
            return False


@register_task
class DummyEPGRefreshTask(TaskScheduler):
    """Run the full guide reconciliation on the manual refresh schedule."""

    task_id = "dummy_epg_refresh"
    task_name = "Dummy EPG Refresh"
    task_description = "Publish generated guide data and reconcile its event channels"

    def __init__(self, schedule_config: Optional[ScheduleConfig] = None):
        if schedule_config is None:
            schedule_config = ScheduleConfig(schedule_type=ScheduleType.MANUAL)
        super().__init__(schedule_config)

    async def _regenerate_xmltv(
        self,
        *,
        publications: Mapping[int, Mapping],
        wait_for_sources: bool = True,
    ):
        """Publish complete documents without owning visibility or delivery."""
        import copy

        from cache import get_cache
        from concurrency import run_cpu_bound
        from database import get_session
        from models import DummyEPGProfile
        from services.epg_programmes import (
            _fetch_all_channels,
            _profile_owners,
            _resolve_group_assignments,
            prepare_profiles,
        )
        from services.epg_publication import (
            PublicationResult,
            _config_hash,
            publication_lock,
            publish_profiles,
        )

        if not isinstance(publications, Mapping):
            raise ValueError("Guide regeneration publications must be an object.")

        session = get_session()
        try:
            profiles = [
                row.to_dict()
                for row in session.query(DummyEPGProfile).filter(
                    DummyEPGProfile.enabled == True  # noqa: E712
                ).all()
            ]
        finally:
            session.close()

        now = datetime.now(timezone.utc)
        saved = {profile["id"]: profile for profile in profiles}
        admitted = {}
        unavailable = set()
        for profile_id, publication in publications.items():
            if (
                not isinstance(profile_id, int)
                or isinstance(profile_id, bool)
                or profile_id <= 0
                or not isinstance(publication, Mapping)
                or publication.get("scope") != f"profile:{profile_id}"
            ):
                raise ValueError("Guide regeneration publication is invalid.")
            profile = saved.get(profile_id)
            state = publication.get("state")
            delivery = state.get("delivery") if isinstance(state, Mapping) else None
            attempt = delivery.get("guide_attempt") if isinstance(delivery, Mapping) else None
            if (
                profile is None
                or not isinstance(publication.get("revision"), int)
                or publication["revision"] <= 0
                or not isinstance(attempt, Mapping)
                or state.get("config_hash") != _config_hash(profile)
                or attempt.get("config_hash") != state.get("config_hash")
            ):
                raise ValueError("Guide regeneration publication does not match its profile.")
            expires_at = datetime.fromisoformat(attempt["expires_at"])
            if expires_at.tzinfo is None or expires_at.utcoffset() is None:
                raise ValueError("Guide regeneration expiry requires an offset.")
            expires_at = expires_at.astimezone(timezone.utc)
            if expires_at <= now:
                unavailable.add(profile_id)
                continue
            admitted[profile_id] = (copy.deepcopy(profile), publication, expires_at)

        if not admitted:
            return PublicationResult(
                unavailable_profile_ids=tuple(sorted(unavailable)),
                reason_codes=("GUIDE_UNAVAILABLE",),
            )

        client = get_client()
        channel_map = await _fetch_all_channels(client)
        prepared_by_id = {}
        summary = {
            "generated_at": now.isoformat(),
            "window_start": None,
            "window_stop": None,
            "sources": [],
            "channels": [],
            "profiles": {},
        }
        for profile_id, (profile, publication, expires_at) in admitted.items():
            if not profile.get("epg_source_ids"):
                intervals = {}
                attempt = publication["state"]["delivery"]["guide_attempt"]
                for receipt in publication["state"]["delivery"]["pending_channels"].values():
                    channel_id = receipt.get("channel_id")
                    if (
                        receipt.get("stage") in {"complete", "failed", "expired", "allocation_unknown"}
                        or receipt.get("guide_attempt_id") != attempt["attempt_id"]
                        or channel_id not in channel_map
                        or datetime.fromisoformat(receipt["expires_at"]) <= now
                    ):
                        continue
                    channel = channel_map[channel_id]
                    group = channel.get("channel_group_id") or channel.get("channel_group")
                    if isinstance(group, dict):
                        group = group.get("id")
                    if (
                        group != receipt["target_group_id"]
                        or (
                            receipt.get("channel_uuid") is not None
                            and channel.get("uuid") != receipt["channel_uuid"]
                        )
                    ):
                        continue
                    intervals.setdefault(channel_id, []).append({
                        "start": receipt["start"],
                        "stop": receipt["stop"],
                        "title": receipt["title"],
                    })
                if intervals:
                    profile["event_intervals"] = intervals
            prepared, coverage = await prepare_profiles(
                [profile],
                channel_map,
                client,
                expires_at=expires_at,
                now=now,
                wait_for_sources=wait_for_sources,
                recover_sources=True,
            )
            if prepared:
                prepared_profile = next(
                    (item for item in prepared if item.get("id") == profile_id),
                    None,
                )
                if prepared_profile is not None:
                    prepared_by_id[profile_id] = prepared_profile
            summary["profiles"].update(coverage.get("profiles") or {})
            summary["sources"].extend(coverage.get("sources") or [])
            summary["channels"].extend(coverage.get("channels") or [])
            for name, choose in (("window_start", min), ("window_stop", max)):
                value = coverage.get(name)
                if value is not None:
                    summary[name] = value if summary[name] is None else choose(summary[name], value)
            if coverage.get("artwork_pending"):
                summary["artwork_pending"] = True

        coverage = summary
        complete_profiles = []
        for profile in profiles:
            profile_id = profile["id"]
            prepared = prepared_by_id.get(profile_id)
            if prepared is None:
                prepared = copy.deepcopy(profile)
                groups = prepared.get("channel_group_ids") or []
                if groups:
                    prepared["channel_assignments"] = _resolve_group_assignments(
                        groups, channel_map,
                    )
                assignments = prepared.get("channel_assignments") or []
                coverage["profiles"].setdefault(str(profile_id), {
                    "profile_id": profile_id,
                    "source_ids": [],
                    "sources": [],
                    "owned_channel_ids": sorted({
                        item["channel_id"] for item in assignments
                        if item.get("channel_id") in channel_map
                    }),
                    "can_publish": False,
                    "reason_codes": ["GUIDE_UNAVAILABLE"],
                })
            complete_profiles.append(prepared)
        _profile_owners(complete_profiles, channel_map, coverage)
        for profile_id, (_, _, expires_at) in admitted.items():
            if datetime.now(timezone.utc) >= expires_at:
                record = coverage["profiles"][str(profile_id)]
                record["can_publish"] = False
                record["reason_codes"] = sorted(
                    set(record.get("reason_codes") or []) | {"GUIDE_UNAVAILABLE"}
                )
        expectations = {
            publication["scope"]: {
                "revision": publication["revision"],
                "xmltv_hash": publication["state"]["xmltv_hash"],
                "config_hash": publication["state"]["config_hash"],
                "attempt_id": publication["state"]["delivery"]["guide_attempt"]["attempt_id"],
            }
            for _, publication, _ in admitted.values()
        }
        async with publication_lock:
            session = get_session()
            try:
                current = [
                    row.to_dict()
                    for row in session.query(DummyEPGProfile).filter(
                        DummyEPGProfile.enabled == True  # noqa: E712
                    ).all()
                ]
            finally:
                session.close()
            if (
                {profile["id"] for profile in current} != set(saved)
                or any(
                    _config_hash(profile) != _config_hash(saved[profile["id"]])
                    for profile in current
                )
            ):
                return PublicationResult(
                    retained_profile_ids=tuple(sorted(saved)),
                    superseded=True,
                    reason_codes=("GUIDE_PUBLICATION_SUPERSEDED",),
                )
            result = await run_cpu_bound(
                publish_profiles,
                complete_profiles,
                channel_map,
                coverage,
                observations={},
                now=now,
                expected=expectations,
            )
            if result.superseded:
                return result
            cache = get_cache()
            cache.invalidate_prefix("dummy_epg_xmltv")
            for scope, document in result.xmltv_by_scope.items():
                key = (
                    "dummy_epg_xmltv_all" if scope == "all"
                    else f"dummy_epg_xmltv_{scope.split(':', 1)[1]}"
                )
                cache.set(key, document)
            return result

    async def execute(self) -> TaskResult:
        from tasks.event_visibility import reconcile_profiles

        return await reconcile_profiles(self, wait_for_sources=True)
