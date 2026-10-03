"""Record and replay exact Dispatcharr writes for channel-pipeline plans."""
from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from services.mutation_plan_store import canonical_hash


PIPELINE_WRITE_METHODS = frozenset({
    "assign_channel_numbers", "create_channel", "create_channel_group", "create_logo",
    "delete_channel", "delete_channel_group", "update_channel", "update_profile_channel",
})
EVENT_PROMOTE_METHOD = "event_sync_promote"

# Every normal successful API run side effect suppressed during plan_only must
# be recreated after replay. This inventory is asserted by tests and reviewed
# alongside the external write chokepoint inventory above.
PIPELINE_INTERNAL_SIDE_EFFECTS = frozenset({
    "execution_record", "rollback_snapshot", "journal_entries",
    "event_review_candidates", "rule_statistics", "conflict_records",
    "database_commit", "managed_channel_ledger", "stream_probe",
    "provider_refresh", "dummy_epg_refresh", "xmltv_cache",
    "notification", "live_data_refresh",
})


@dataclass
class PlannedWrite:
    method: str
    args: list[Any]
    kwargs: dict[str, Any]
    result_id: int | None = None


_EVENT_FIELDS = frozenset({
    "rule_id", "rule_name", "config", "profile_id", "profile_hash",
    "source_id", "source_hashes", "expected_revision", "expected_hash",
    "unit", "allocation_writes", "default_profile_ids", "stale_streams",
    "working_stream_ids", "channel_uuid",
})


def _positive(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _sha(value: Any, name: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 hash")
    return value


def _event_row(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "stream", "disposition", "parsed",
    }:
        raise ValueError("event row fields are invalid")
    stream = value["stream"]
    parsed = value["parsed"]
    if not isinstance(stream, dict) or set(stream) != {
        "name", "group_id", "stream_id", "provider", "provider_id",
        "name_seen_before_today", "is_stale",
    }:
        raise ValueError("event stream fields are invalid")
    if not isinstance(stream["name"], str) or not stream["name"]:
        raise ValueError("event stream name is invalid")
    _positive(stream["group_id"], "event stream group ID")
    _positive(stream["stream_id"], "event stream ID")
    if stream["provider_id"] is not None:
        _positive(stream["provider_id"], "event stream provider ID")
    if stream["provider"] is not None and not isinstance(stream["provider"], str):
        raise ValueError("event stream provider is invalid")
    for name in ("name_seen_before_today", "is_stale"):
        if stream[name] is not None and not isinstance(stream[name], bool):
            raise ValueError(f"event stream {name} is invalid")
    if value["disposition"] not in {"unmatched", "excluded"}:
        raise ValueError("event stream disposition is invalid")
    if not isinstance(parsed, dict) or set(parsed) != {
        "raw_name", "title", "start", "teams", "matched_pattern",
    }:
        raise ValueError("parsed event fields are invalid")
    if (
        not isinstance(parsed["raw_name"], str)
        or not isinstance(parsed["title"], str)
        or not parsed["title"].strip()
        or not isinstance(parsed["start"], str)
    ):
        raise ValueError("parsed event identity is invalid")
    try:
        start = datetime.fromisoformat(parsed["start"])
    except ValueError as exc:
        raise ValueError("parsed event start is invalid") from exc
    if start.tzinfo is None or start.utcoffset() != timezone.utc.utcoffset(start):
        raise ValueError("parsed event start must be UTC")
    teams = parsed["teams"]
    if teams is not None and (
        not isinstance(teams, list)
        or len(teams) != 2
        or any(not isinstance(team, str) for team in teams)
    ):
        raise ValueError("parsed event teams are invalid")
    if parsed["matched_pattern"] is not None and not isinstance(
        parsed["matched_pattern"], str
    ):
        raise ValueError("parsed event pattern is invalid")
    return copy.deepcopy(value)


def _event_operation(value: Any, result_id: Any) -> dict[str, Any]:
    """Validate the exact bounded event operation stored by the server."""
    if not isinstance(value, dict) or set(value) != _EVENT_FIELDS:
        raise ValueError("event promotion fields are invalid")
    if not isinstance(result_id, int) or isinstance(result_id, bool) or result_id == 0:
        raise ValueError("event promotion result ID is invalid")
    _positive(value["rule_id"], "event rule ID")
    _positive(value["profile_id"], "event profile ID")
    _positive(value["source_id"], "event source ID")
    if not isinstance(value["rule_name"], str) or not value["rule_name"].strip():
        raise ValueError("event rule name is invalid")
    if not isinstance(value["config"], dict):
        raise ValueError("event rule configuration is invalid")
    try:
        json.dumps(value["config"], sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise ValueError("event rule configuration is not JSON-safe") from exc
    _sha(value["profile_hash"], "event profile hash")
    hashes = value["source_hashes"]
    if (
        not isinstance(hashes, list)
        or len(hashes) != 1
        or not isinstance(hashes[0], dict)
        or set(hashes[0]) != {"endpoint_hash", "source_url_hash"}
    ):
        raise ValueError("event source hashes are invalid")
    _sha(hashes[0]["endpoint_hash"], "event source endpoint hash")
    _sha(hashes[0]["source_url_hash"], "event source URL hash")
    revision = value["expected_revision"]
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise ValueError("event publication revision is invalid")
    if revision == 0:
        if value["expected_hash"] is not None:
            raise ValueError("absent event publication cannot have a hash")
    else:
        _sha(value["expected_hash"], "event publication hash")

    unit = value["unit"]
    if not isinstance(unit, dict) or set(unit) != {
        "event_key", "channel_name", "dateless", "action",
        "existing_channel_id", "rows",
    }:
        raise ValueError("event promotion unit fields are invalid")
    if (
        not isinstance(unit["event_key"], str)
        or not unit["event_key"]
        or not isinstance(unit["channel_name"], str)
        or not unit["channel_name"].strip()
        or not isinstance(unit["dateless"], bool)
        or unit["action"] not in {"create", "attach_existing"}
        or not isinstance(unit["rows"], list)
        or not unit["rows"]
    ):
        raise ValueError("event promotion unit is invalid")
    rows = [_event_row(row) for row in unit["rows"]]
    selected_ids = [row["stream"]["stream_id"] for row in rows]
    if len(selected_ids) != len(set(selected_ids)):
        raise ValueError("event promotion streams must be unique")
    existing_id = unit["existing_channel_id"]
    if unit["action"] == "create":
        if existing_id is not None or result_id >= 0 or value["channel_uuid"] is not None:
            raise ValueError("event creation result is invalid")
    else:
        if _positive(existing_id, "event existing channel ID") != result_id:
            raise ValueError("event adoption result is invalid")
        if not isinstance(value["channel_uuid"], str) or not value["channel_uuid"]:
            raise ValueError("event adoption channel UUID is invalid")

    default_ids = value["default_profile_ids"]
    if not isinstance(default_ids, list):
        raise ValueError("event default profile IDs are invalid")
    normalized_defaults = [
        _positive(item, "event default profile ID") for item in default_ids
    ]
    if len(normalized_defaults) != len(set(normalized_defaults)):
        raise ValueError("event default profile IDs must be unique")

    writes = value["allocation_writes"]
    if not isinstance(writes, list):
        raise ValueError("event allocation writes are invalid")
    creates = 0
    seen_profiles: set[int] = set()
    for nested in writes:
        if not isinstance(nested, dict) or set(nested) != {
            "method", "args", "kwargs", "result_id",
        }:
            raise ValueError("event allocation write fields are invalid")
        method = nested["method"]
        args = nested["args"]
        kwargs = nested["kwargs"]
        if not isinstance(args, list) or not isinstance(kwargs, dict) or kwargs:
            raise ValueError("event allocation write arguments are invalid")
        if method == "create_channel":
            creates += 1
            if (
                len(args) != 1
                or not isinstance(args[0], dict)
                or args[0].get("streams") != []
                or args[0].get("hidden_from_output") is not True
                or args[0].get("channel_group_id") != value["config"].get(
                    "promote_target_group_id"
                )
                or nested["result_id"] != result_id
            ):
                raise ValueError("event allocation channel create is invalid")
        elif method == "update_profile_channel":
            if (
                len(args) != 3
                or _positive(args[0], "event allocation profile ID") in seen_profiles
                or args[1] != result_id
                or not isinstance(args[2], dict)
                or set(args[2]) != {"enabled"}
                or not isinstance(args[2]["enabled"], bool)
                or nested["result_id"] is not None
            ):
                raise ValueError("event allocation profile write is invalid")
            seen_profiles.add(args[0])
            if args[2]["enabled"] is not (args[0] in normalized_defaults):
                raise ValueError("event allocation profile target is invalid")
        else:
            raise ValueError("event allocation contains an unsupported write")
    if creates != (1 if unit["action"] == "create" else 0):
        raise ValueError("event allocation create count is invalid")
    if unit["action"] == "attach_existing" and writes:
        raise ValueError("event adoption cannot contain allocation writes")

    stale = value["stale_streams"]
    if not isinstance(stale, list):
        raise ValueError("event stale streams are invalid")
    stale_rows = [_event_row(row) for row in stale]
    stale_ids = [row["stream"]["stream_id"] for row in stale_rows]
    if len(stale_ids) != len(set(stale_ids)):
        raise ValueError("event stale streams must be unique")
    if set(stale_ids) & set(selected_ids):
        raise ValueError("event selected and stale streams must not overlap")
    working = value["working_stream_ids"]
    if not isinstance(working, list):
        raise ValueError("event working streams are invalid")
    working_ids = [_positive(item, "event working stream ID") for item in working]
    if len(working_ids) != len(set(working_ids)):
        raise ValueError("event working streams must be unique")
    if not set(working_ids) <= set(selected_ids) | set(stale_ids):
        raise ValueError("event working streams are outside the approved unit")
    return copy.deepcopy(value)


def _event_write_bounds(value: dict[str, Any]) -> tuple[int, set[tuple[str, Any]]]:
    writes = value["allocation_writes"]
    stream_count = len(value["unit"]["rows"])
    stale_count = len(value["stale_streams"])
    result_id = next(
        nested["result_id"]
        for nested in writes
        if nested["method"] == "create_channel"
    ) if value["unit"]["action"] == "create" else value["unit"]["existing_channel_id"]
    targets = {("channel", result_id)}
    for nested in writes:
        if nested["method"] == "create_channel":
            targets.add(("channel", nested["result_id"]))
        elif nested["method"] == "update_profile_channel":
            targets.add(("channel", nested["args"][1]))
    return len(writes) + 2 + stream_count + stale_count, targets


@dataclass(frozen=True)
class PlanningContext:
    """Explicit capability boundary for a mutation-free planning traversal.

    This is intentionally distinct from ``dry_run``: dry-run produces a user
    simulation and therefore skips writes, while planning executes normal
    decision logic against a shadow client so it can record the exact writes.
    Internal sinks must key off this context rather than inferring safety from
    the Dispatcharr client type.
    """

    enabled: bool = False

    @property
    def allow_internal_side_effects(self) -> bool:
        return not self.enabled


@dataclass
class PipelineWritePlan:
    writes: list[PlannedWrite] = field(default_factory=list)
    channel_preconditions: dict[str, dict[str, Any]] = field(default_factory=dict)
    group_preconditions: dict[str, dict[str, Any]] = field(default_factory=dict)
    profile_preconditions: dict[str, bool] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "writes": [vars(write) for write in self.writes],
            "channel_preconditions": self.channel_preconditions,
            "group_preconditions": self.group_preconditions,
            "profile_preconditions": self.profile_preconditions,
        }

    def accounting(self) -> dict[str, int]:
        """Authoritative counts derived from exact operations, never previews."""
        targets: set[tuple[str, Any]] = set()
        write_count = 0
        for index, write in enumerate(self.writes):
            if write.method == EVENT_PROMOTE_METHOD:
                if len(write.args) != 1 or write.kwargs:
                    raise ValueError("event promotion write shape is invalid")
                operation = _event_operation(write.args[0], write.result_id)
                bounded_writes, bounded_targets = _event_write_bounds(operation)
                write_count += bounded_writes
                targets.update(bounded_targets)
            elif write.method == "assign_channel_numbers":
                write_count += 1
                targets.update(("channel", value) for value in write.args[0])
            elif write.method == "update_profile_channel":
                write_count += 1
                targets.add(("channel", write.args[1]))
            elif write.method in {"update_channel", "delete_channel"}:
                write_count += 1
                targets.add(("channel", write.args[0]))
            elif write.method == "delete_channel_group":
                write_count += 1
                targets.add(("group", write.args[0]))
            else:
                write_count += 1
                # Each create is a distinct future entity even when payloads match.
                targets.add((write.method, write.result_id if write.result_id is not None else index))
        return {"write_count": write_count, "unique_target_count": len(targets)}


class PartialReplayError(RuntimeError):
    """Upstream has no transaction; exposes exactly how far replay reached."""

    def __init__(self, failed_index: int, completed: list[str], compensation_errors: list[str]):
        super().__init__(f"pipeline replay failed at write {failed_index}")
        self.failed_index = failed_index
        self.completed = completed
        self.compensation_errors = compensation_errors


class PlanningDispatcharrClient:
    """Delegate reads while replacing every supported write with a recording."""

    def __init__(self, client) -> None:
        self._client = client
        self.plan = PipelineWritePlan()
        self._next_temp_id = -1
        self._shadow_channels: dict[int, dict[str, Any]] = {}

    def __getattr__(self, name: str):
        return getattr(self._client, name)

    async def get_channel(self, channel_id: int) -> dict[str, Any]:
        if channel_id in self._shadow_channels:
            return copy.deepcopy(self._shadow_channels[channel_id])
        return await self._client.get_channel(channel_id)

    async def _channel_before(self, channel_id: int) -> dict[str, Any]:
        if channel_id in self._shadow_channels:
            return copy.deepcopy(self._shadow_channels[channel_id])
        channel = await self._client.get_channel(channel_id)
        relevant = {
            "id": channel_id,
            "name": channel.get("name"),
            "streams": list(channel.get("streams", []) or []),
            "channel_group_id": channel.get("channel_group_id"),
            "logo_id": channel.get("logo_id"),
            "tvg_id": channel.get("tvg_id"),
            "channel_number": channel.get("channel_number"),
            "stream_profile_id": channel.get("stream_profile_id"),
            "epg_data_id": channel.get("epg_data_id"),
        }
        self.plan.channel_preconditions.setdefault(str(channel_id), copy.deepcopy(relevant))
        self._shadow_channels[channel_id] = relevant
        return copy.deepcopy(relevant)

    def _record(self, method: str, *args, result_id: int | None = None, **kwargs) -> None:
        self.plan.writes.append(PlannedWrite(
            method,
            copy.deepcopy(list(args)),
            copy.deepcopy(kwargs),
            result_id=result_id,
        ))

    async def create_channel(self, data: dict) -> dict:
        temp_id = self._next_temp_id
        self._next_temp_id -= 1
        created = {"id": temp_id, **copy.deepcopy(data)}
        self._shadow_channels[temp_id] = created
        self._record("create_channel", data, result_id=temp_id)
        return copy.deepcopy(created)

    async def create_channel_group(self, name: str) -> dict:
        temp_id = self._next_temp_id
        self._next_temp_id -= 1
        self._record("create_channel_group", name, result_id=temp_id)
        return {"id": temp_id, "name": name}

    async def create_logo(self, data: dict) -> dict:
        temp_id = self._next_temp_id
        self._next_temp_id -= 1
        self._record("create_logo", data, result_id=temp_id)
        return {"id": temp_id, **copy.deepcopy(data)}

    async def update_channel(self, channel_id: int, data: dict) -> dict:
        current = await self._channel_before(channel_id)
        current.update(copy.deepcopy(data))
        self._shadow_channels[channel_id] = current
        self._record("update_channel", channel_id, data)
        return copy.deepcopy(current)

    async def delete_channel(self, channel_id: int) -> None:
        await self._channel_before(channel_id)
        self._record("delete_channel", channel_id)
        self._shadow_channels.pop(channel_id, None)

    async def delete_channel_group(self, group_id: int) -> None:
        groups = await self._client.get_channel_groups()
        group = next((item for item in groups if item.get("id") == group_id), None)
        if group is None:
            raise ValueError(f"channel group {group_id} not found during planning")
        self.plan.group_preconditions[str(group_id)] = {
            "id": group_id, "name": group.get("name")
        }
        self._record("delete_channel_group", group_id)

    async def assign_channel_numbers(self, channel_ids: list[int], starting_number=None) -> dict:
        for channel_id in channel_ids:
            await self._channel_before(channel_id)
        self._record("assign_channel_numbers", channel_ids, starting_number)
        return {"status": "planned"}

    async def update_profile_channel(self, profile_id: int, channel_id: int, data: dict) -> dict:
        await self._channel_before(channel_id)
        profiles = await self._client.get_channel_profiles()
        profile = next((item for item in profiles if item.get("id") == profile_id), None)
        if profile is None:
            raise ValueError(f"channel profile {profile_id} not found during planning")
        members = {
            item.get("id") if isinstance(item, dict) else item
            for item in (profile.get("channels") or [])
        }
        self.plan.profile_preconditions[f"{profile_id}:{channel_id}"] = channel_id in members
        self._record("update_profile_channel", profile_id, channel_id, data)
        return copy.deepcopy(data)

    async def record_event_promotion(
        self, operation: dict, realize=None, *, result_id: int | None = None
    ) -> Any:
        """Replace one staged allocation segment with its semantic event write."""
        start = len(self.plan.writes)
        result = None
        if result_id is None:
            if realize is None:
                raise ValueError("event promotion allocation is required")
            result = await realize()
            result_id = getattr(result, "entity_id", None)
            if not getattr(result, "success", False) or result_id is None:
                del self.plan.writes[start:]
                raise ValueError("event promotion allocation could not be planned")
        elif result_id < 1 or realize is not None:
            raise ValueError("event promotion adoption is invalid")
        else:
            await self._channel_before(result_id)
        allocation = [vars(write) for write in self.plan.writes[start:]]
        del self.plan.writes[start:]
        stored = copy.deepcopy(operation)
        stored["allocation_writes"] = allocation
        stored = _event_operation(stored, result_id)
        self.plan.writes.append(PlannedWrite(
            EVENT_PROMOTE_METHOD,
            [stored],
            {},
            result_id=result_id,
        ))
        return result


class EventAllocationClient:
    """Forward only the approved staged allocation calls in their exact order."""

    def __init__(
        self,
        client,
        writes: list[dict],
        result_id: int,
        *,
        forward: bool = True,
    ) -> None:
        self._client = client
        self._writes = copy.deepcopy(writes)
        self._result_id = result_id
        self._forward = forward
        self._index = 0
        self._real_id: int | None = None

    def __getattr__(self, name: str):
        if name in PIPELINE_WRITE_METHODS:
            async def refuse(*args, **kwargs):
                raise ValueError(f"unexpected event allocation write {name}")
            return refuse
        return getattr(self._client, name)

    def _expected(self, method: str, args: list[Any], kwargs: dict[str, Any]) -> None:
        if self._index >= len(self._writes):
            raise ValueError("event allocation issued an extra write")
        expected = copy.deepcopy(self._writes[self._index])
        if self._real_id is not None:
            def remap(value):
                if value == self._result_id:
                    return self._real_id
                if isinstance(value, list):
                    return [remap(item) for item in value]
                if isinstance(value, dict):
                    return {key: remap(item) for key, item in value.items()}
                return value
            expected["args"] = remap(expected["args"])
            expected["kwargs"] = remap(expected["kwargs"])
        actual = {"method": method, "args": args, "kwargs": kwargs}
        approved = {
            "method": expected["method"],
            "args": expected["args"],
            "kwargs": expected["kwargs"],
        }
        if canonical_hash(actual) != canonical_hash(approved):
            raise ValueError("event allocation write drifted")
        self._index += 1

    async def create_channel(self, value: dict) -> dict:
        self._expected("create_channel", [value], {})
        if not self._forward:
            return {"id": self._result_id, **copy.deepcopy(value)}
        result = await self._client.create_channel(value)
        real_id = result.get("id") if isinstance(result, dict) else None
        if real_id is None:
            raise RuntimeError("event allocation create did not return an id")
        self._real_id = int(real_id)
        return result

    async def update_profile_channel(
        self, profile_id: int, channel_id: int, value: dict
    ) -> dict:
        self._expected(
            "update_profile_channel", [profile_id, channel_id, value], {}
        )
        if not self._forward:
            return copy.deepcopy(value)
        return await self._client.update_profile_channel(
            profile_id, channel_id, value
        )

    def finish(self) -> None:
        if self._index != len(self._writes):
            raise ValueError("event allocation omitted an approved write")


async def validate_read_set(client, plan: PipelineWritePlan) -> None:
    """Validate every existing channel before replay performs its first write."""
    for raw_id, expected in plan.channel_preconditions.items():
        current = await client.get_channel(int(raw_id))
        actual = {key: current.get(key) for key in expected if key != "id"}
        actual["id"] = int(raw_id)
        if canonical_hash(actual) != canonical_hash(expected):
            raise ValueError(f"channel {raw_id} drifted")
    if plan.group_preconditions:
        groups = {str(item.get("id")): item for item in await client.get_channel_groups()}
        for raw_id, expected in plan.group_preconditions.items():
            current = groups.get(raw_id)
            if current is None or current.get("name") != expected.get("name"):
                raise ValueError(f"channel group {raw_id} drifted")
    if plan.profile_preconditions:
        profiles = {item.get("id"): item for item in await client.get_channel_profiles()}
        for key, expected in plan.profile_preconditions.items():
            profile_id, channel_id = map(int, key.split(":"))
            profile = profiles.get(profile_id)
            members = {
                item.get("id") if isinstance(item, dict) else item
                for item in ((profile or {}).get("channels") or [])
            }
            if (channel_id in members) is not expected:
                raise ValueError(f"channel profile membership {key} drifted")


async def replay_write_plan(
    client,
    plan: PipelineWritePlan,
    *,
    read_set_validated: bool = False,
    event_operation=None,
) -> tuple[list[Any], dict[int, int]]:
    """Validate first, then replay only recorded writes with temp-ID remapping."""
    if any(write.method == EVENT_PROMOTE_METHOD for write in plan.writes):
        if event_operation is None:
            raise ValueError("event promotion replay support is required")
        for write in plan.writes:
            if write.method == EVENT_PROMOTE_METHOD:
                if write.args is None or len(write.args) != 1 or write.kwargs:
                    raise ValueError("event promotion write shape is invalid")
                _event_operation(write.args[0], write.result_id)
    if not read_set_validated:
        await validate_read_set(client, plan)
    remap: dict[int, int] = {}
    results: list[Any] = []

    def mapped(value: Any) -> Any:
        if isinstance(value, int) and value < 0:
            if value not in remap:
                raise ValueError(f"unresolved temporary id {value}")
            return remap[value]
        if isinstance(value, list):
            return [mapped(item) for item in value]
        if isinstance(value, dict):
            return {key: mapped(item) for key, item in value.items()}
        return value

    next_temp = -1
    completed: list[tuple[PlannedWrite, list[Any], Any]] = []
    try:
        for write in plan.writes:
            if write.method == EVENT_PROMOTE_METHOD:
                operation = _event_operation(write.args[0], write.result_id)
                result = await event_operation(operation, write.result_id)
                real_id = result.get("id") if isinstance(result, dict) else None
                if real_id is None:
                    raise RuntimeError("event promotion did not return an id")
                real_id = int(real_id)
                if write.result_id < 0:
                    remap[write.result_id] = real_id
                elif real_id != write.result_id:
                    raise RuntimeError("event promotion returned the wrong channel id")
                args = [operation]
                kwargs = {}
            else:
                args = mapped(write.args)
                kwargs = mapped(write.kwargs)
                result = await getattr(client, write.method)(*args, **kwargs)
            if write.method.startswith("create_"):
                real_id = result.get("id") if isinstance(result, dict) else None
                if real_id is None:
                    raise RuntimeError(f"{write.method} did not return an id")
                temp_id = write.result_id if write.result_id is not None else next_temp
                if temp_id != next_temp:
                    raise ValueError("planned temporary id order is invalid")
                remap[temp_id] = int(real_id)
                next_temp -= 1
            elif write.method == EVENT_PROMOTE_METHOD and write.result_id < 0:
                if write.result_id != next_temp:
                    raise ValueError("planned temporary id order is invalid")
                next_temp -= 1
            results.append(result)
            completed.append((write, args, result))
    except Exception as exc:
        compensation_errors: list[str] = []
        # Best effort for reversible writes. Deletes and profile membership are
        # explicitly not recreated because upstream cannot preserve their IDs.
        for done, args, result in reversed(completed):
            try:
                if done.method == "create_channel":
                    await client.delete_channel(result["id"])
                elif done.method == "create_channel_group":
                    await client.delete_channel_group(result["id"])
                elif done.method == "update_channel" and args[0] > 0:
                    before = plan.channel_preconditions.get(str(args[0]))
                    if before:
                        await client.update_channel(args[0], {
                            key: value for key, value in before.items() if key != "id"
                        })
            except Exception as compensation_exc:  # noqa: BLE001
                compensation_errors.append(f"{done.method}: {compensation_exc}")
        completed_targets = [
            (
                f"{item[0].method}:{item[0].result_id}:recovery-retained"
                if item[0].method == EVENT_PROMOTE_METHOD
                else f"{item[0].method}:{item[1][0] if item[1] else '<no-arg>'}"
            )
            for item in completed
        ]
        if (
            write.method == EVENT_PROMOTE_METHOD
            and getattr(exc, "event_recovery_retained", False) is True
        ):
            completed_targets.append(
                f"{EVENT_PROMOTE_METHOD}:{write.result_id}:recovery-retained"
            )
        raise PartialReplayError(
            len(completed), completed_targets, compensation_errors
        ) from exc
    return results, remap


def journal_entries_for_plan(
    plan: PipelineWritePlan, remap: dict[int, int], execution_id: int
) -> list[dict[str, Any]]:
    """Build target-specific audit rows for every replayed mutation semantic."""
    entries: list[dict[str, Any]] = []
    def append(action: str, entity_id: Any, name: Any, before: Any, after: Any, description: str):
        entries.append({
            "category": "auto_creation", "action_type": action,
            "entity_id": entity_id, "entity_name": name,
            "description": description, "before_value": before,
            "after_value": after, "user_initiated": False,
            "mutation_source": "auto_creation", "batch_id": str(execution_id),
        })

    next_temp = -1
    # A channel can be updated several times in one replay. Provenance must
    # describe each transition, not repeatedly compare every write with the
    # pre-run snapshot.
    shadow = {
        int(channel_id): copy.deepcopy(value)
        for channel_id, value in plan.channel_preconditions.items()
    }
    for write in plan.writes:
        method = write.method
        if method == EVENT_PROMOTE_METHOD:
            continue
        if method.startswith("create_"):
            temp_id = write.result_id if write.result_id is not None else next_temp
            entity_id = remap.get(temp_id, temp_id)
            if temp_id == next_temp:
                next_temp -= 1
            payload = write.args[0] if write.args else {}
            append(method, entity_id, payload.get("name") if isinstance(payload, dict) else str(payload),
                   None, payload, f"Planned pipeline executed {method} for {entity_id}")
            if method == "create_channel" and isinstance(payload, dict):
                shadow[entity_id] = {"id": entity_id, **copy.deepcopy(payload)}
            continue
        if method == "assign_channel_numbers":
            starting_number = write.args[1]
            for index, channel_id in enumerate(write.args[0]):
                resolved_id = remap.get(channel_id, channel_id)
                before_channel = shadow.setdefault(resolved_id, {})
                old_number = before_channel.get("channel_number")
                new_number = (
                    starting_number + index if starting_number is not None else None
                )
                append("assign_channel_number", resolved_id, before_channel.get("name"),
                       {"channel_number": old_number}, {"channel_number": new_number},
                       "Planned pipeline assigned channel number")
                before_channel["channel_number"] = new_number
            continue
        if method == "update_profile_channel":
            profile_id, raw_id, payload = write.args
            channel_id = remap.get(raw_id, raw_id)
            append("assign_channel_profile", channel_id, None,
                   {"profile_id": profile_id}, payload,
                   f"Planned pipeline updated profile {profile_id} membership")
            continue
        raw_id = write.args[0] if write.args else None
        entity_id = remap.get(raw_id, raw_id)
        before = copy.deepcopy(shadow.get(entity_id, plan.channel_preconditions.get(str(raw_id), {})))
        payload = write.args[1] if len(write.args) > 1 else None
        if method == "update_channel" and isinstance(payload, dict) and "streams" in payload:
            old_streams = set(before.get("streams", []) or [])
            new_streams = set(payload.get("streams", []) or [])
            for stream_id in sorted(new_streams - old_streams):
                append("merge_stream", entity_id, before.get("name"),
                       {"stream_ids": sorted(old_streams)}, {"stream_id": stream_id},
                       f"Planned pipeline attached stream {stream_id} to channel {entity_id}")
            for stream_id in sorted(old_streams - new_streams):
                append("remove_stream", entity_id, before.get("name"),
                       {"stream_id": stream_id}, {"stream_ids": sorted(new_streams)},
                       f"Planned pipeline removed stream {stream_id} from channel {entity_id}")
            remaining = {key: value for key, value in payload.items() if key != "streams"}
            if remaining:
                append(method, entity_id, before.get("name"), before, remaining,
                       f"Planned pipeline updated channel {entity_id}")
        else:
            append(method, entity_id, before.get("name"), before, payload,
                   f"Planned pipeline executed {method} for {entity_id}")
        if method == "update_channel" and isinstance(payload, dict):
            updated = copy.deepcopy(before)
            updated.update(copy.deepcopy(payload))
            shadow[entity_id] = updated
        elif method == "delete_channel":
            shadow.pop(entity_id, None)
    return entries
