"""Current, name-bound Event Sync stream health decisions."""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cache import Cache
from models import StreamStats
from services.event_sync_stream_health import (
    _fresh_flow_state,
    _probe_and_collect_failures,
    collect_stream_flow,
    find_dead_streams,
    find_working_streams,
)
from services.pipeline_write_plan import PlanningDispatcharrClient
from stream_prober import StreamProber
from tests.unit.test_event_sync_promotion import _clock


_NOW = datetime(2026, 7, 11, 20, 0, tzinfo=timezone.utc)
_START = _NOW - timedelta(minutes=2)
_CHECKED = _NOW - timedelta(minutes=4)


@pytest.fixture
def fixed_consumer_clock():
    with patch("services.event_sync_stream_health.datetime", _clock(_NOW)):
        yield


def _make_prober(client=None, **kwargs) -> StreamProber:
    with patch.object(StreamProber, "_load_probe_history"):
        return StreamProber(
            client=client if client is not None else AsyncMock(),
            **kwargs,
        )


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"


def _stat(
    stream_id: int,
    *,
    name: str | None = None,
    measured=4_000_000,
    dark: bool | None = False,
    status: str = "success",
    failures: int = 0,
    probed_at: datetime = _NOW,
    checked_at: datetime | None = _NOW,
    declared: int | None = None,
) -> dict:
    stat = {
        "stream_id": stream_id,
        "stream_name": name or f"s{stream_id}",
        "probe_status": status,
        "consecutive_failures": failures,
        "measured_bitrate": measured,
        "video_bitrate": declared,
        "last_probed": _iso(probed_at),
    }
    if dark is not None:
        stat["is_black_screen"] = dark
    if checked_at is not None:
        stat["black_screen_checked_at"] = _iso(checked_at)
    return stat


@pytest.mark.parametrize(
    "measured,dark,expected",
    [
        (4_000_000, True, False),
        (4_000_000, False, True),
        (4_000_000, None, None),
        (0, True, False),
        (0, False, False),
        (0, None, False),
        (None, True, False),
        (None, False, None),
        (None, None, None),
    ],
)
def test_current_playability_table(measured, dark, expected):
    stat = _stat(1, measured=measured, dark=dark)
    if dark is None:
        stat.pop("black_screen_checked_at", None)

    assert _fresh_flow_state(
        stat,
        _CHECKED,
        stream_name="s1",
        now=_NOW,
    ) is expected


@pytest.mark.parametrize("measured", [True, -1, float("nan"), float("inf")])
def test_invalid_measurements_are_unknown(measured):
    assert _fresh_flow_state(
        _stat(1, measured=measured, dark=False),
        _CHECKED,
        stream_name="s1",
        now=_NOW,
    ) is None


def test_current_hard_failure_is_false_without_a_measurement():
    assert _fresh_flow_state(
        _stat(1, measured=None, dark=None, status="timeout"),
        _CHECKED,
        stream_name="s1",
        now=_NOW,
    ) is False


@pytest.mark.parametrize(
    "stat,name,now",
    [
        (None, "s1", _NOW),
        (_stat(1, name="other"), "s1", _NOW),
        (_stat(1, probed_at=_NOW - timedelta(minutes=6)), "s1", _NOW),
        (_stat(1, probed_at=_NOW + timedelta(minutes=1)), "s1", _NOW),
    ],
)
def test_missing_stale_future_and_other_name_rows_are_unknown(stat, name, now):
    assert _fresh_flow_state(
        stat,
        _NOW - timedelta(minutes=5),
        stream_name=name,
        now=now,
    ) is None


@pytest.mark.parametrize(
    "measured,dark",
    [
        (4_000_000, True),
        (4_000_000, False),
        (4_000_000, None),
        (0, True),
        (0, False),
        (0, None),
        (None, True),
        (None, False),
        (None, None),
    ],
)
@pytest.mark.parametrize("invalid", ["missing", "stale", "future", "name"])
def test_every_table_pair_rejects_invalid_evidence(measured, dark, invalid):
    stat = _stat(1, measured=measured, dark=dark)
    expected_name = "s1"
    if invalid == "missing":
        stat = None
    elif invalid == "stale":
        stat["last_probed"] = _iso(_NOW - timedelta(minutes=6))
        stat["black_screen_checked_at"] = _iso(_NOW - timedelta(minutes=6))
    elif invalid == "future":
        stat["last_probed"] = _iso(_NOW + timedelta(minutes=1))
        stat["black_screen_checked_at"] = _iso(_NOW + timedelta(minutes=1))
    else:
        expected_name = "renamed"

    assert _fresh_flow_state(
        stat,
        _NOW - timedelta(minutes=5),
        stream_name=expected_name,
        now=_NOW,
    ) is None


@pytest.mark.parametrize(
    "measured,dark,expected",
    [
        (4_000_000, True, None),
        (4_000_000, False, None),
        (4_000_000, None, None),
        (0, True, False),
        (0, False, False),
        (0, None, False),
        (None, True, None),
        (None, False, None),
        (None, None, None),
    ],
)
def test_every_table_pair_rejects_content_from_before_the_probe(
    measured, dark, expected,
):
    stat = _stat(
        1,
        measured=measured,
        dark=dark,
        probed_at=_NOW,
        checked_at=_NOW - timedelta(seconds=1),
    )

    assert _fresh_flow_state(
        stat,
        _CHECKED,
        stream_name="s1",
        now=_NOW,
    ) is expected


def test_content_before_a_newer_probe_cannot_complete_that_probe():
    stat = _stat(
        1,
        measured=4_000_000,
        dark=False,
        probed_at=_NOW,
        checked_at=_NOW - timedelta(seconds=1),
    )
    assert _fresh_flow_state(
        stat, _CHECKED, stream_name="s1", now=_NOW,
    ) is None


def test_a_new_dark_scan_can_classify_old_transport():
    stat = _stat(
        1,
        measured=4_000_000,
        dark=True,
        probed_at=_NOW - timedelta(minutes=10),
        checked_at=_NOW,
    )
    assert _fresh_flow_state(
        stat, _CHECKED, stream_name="s1", now=_NOW,
    ) is False


@pytest.mark.asyncio
async def test_collector_uses_event_time_name_and_complete_evidence(
    fixed_consumer_clock,
):
    stats = {
        1: _stat(1, measured=12_000, dark=False, declared=1),
        2: _stat(2, measured=4_000_000, dark=None),
        3: _stat(3, measured=0, dark=None),
        4: _stat(4, measured=4_000_000, dark=False, name="old"),
    }
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value=stats),
    ):
        result = await collect_stream_flow(
            [1, 2, 3, 4, 5],
            client=None,
            checked_after=_CHECKED,
            event_start_by_stream={
                1: _START,
                2: _START,
                3: _START,
                4: _START,
                5: _NOW + timedelta(hours=1),
            },
            stream_names={sid: f"s{sid}" for sid in range(1, 6)},
            expires_at=None,
        )

    assert result == {1: True, 2: None, 3: False, 4: None, 5: None}


@pytest.mark.asyncio
async def test_collector_read_only_path_never_probes(fixed_consumer_clock):
    probe = AsyncMock()
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value={}),
    ), patch(
        "services.event_sync_stream_health._probe_and_collect_failures", probe,
    ):
        result = await collect_stream_flow(
            [1],
            client=MagicMock(),
            checked_after=_CHECKED,
            event_start_by_stream={1: _START},
            stream_names={1: "s1"},
            expires_at=None,
        )

    assert result == {1: None}
    probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_collector_reloads_after_targeted_probe(fixed_consumer_clock):
    expires_at = _NOW + timedelta(minutes=1)
    fresh = _stat(
        1,
        probed_at=_NOW,
        checked_at=_NOW,
    )
    load = AsyncMock(side_effect=[{}, {1: fresh}])
    async def confirm(*args, **kwargs):
        kwargs["confirmed"].update(args[1])
        return set()

    probe = AsyncMock(side_effect=confirm)
    client = MagicMock()
    with patch("services.event_sync_stream_health._load_stats", load), patch(
        "services.event_sync_stream_health._probe_and_collect_failures", probe,
    ):
        result = await collect_stream_flow(
            [1],
            client=client,
            checked_after=_CHECKED,
            event_start_by_stream={1: _START},
            stream_names={1: "s1"},
            expires_at=expires_at,
            probe_missing=True,
            probe_while_busy=True,
        )

    assert result == {1: True}
    probe.assert_awaited_once_with(
        client,
        [1],
        expires_at=expires_at,
        event_start_by_stream={1: _START},
        stream_names={1: "s1"},
        cancelled=None,
        stop_when_playable=False,
        confirmed={1},
        event_streams=None,
        checked_after=_CHECKED,
    )


@pytest.mark.asyncio
async def test_collector_skips_future_and_unscoped_candidates(
    fixed_consumer_clock,
):
    probe = AsyncMock()
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value={}),
    ), patch(
        "services.event_sync_stream_health._probe_and_collect_failures", probe,
    ):
        result = await collect_stream_flow(
            [1, 2],
            client=MagicMock(),
            checked_after=_CHECKED,
            event_start_by_stream={1: _NOW + timedelta(hours=1)},
            stream_names={1: "s1"},
            expires_at=_NOW + timedelta(minutes=1),
            probe_missing=True,
            probe_while_busy=True,
        )

    assert result == {1: None, 2: None}
    probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_collector_uses_unprobed_then_oldest_order(fixed_consumer_clock):
    stats = {
        1: _stat(1, measured=None, dark=None, probed_at=_NOW - timedelta(minutes=4)),
        3: _stat(3, measured=None, dark=None, probed_at=_NOW - timedelta(minutes=1)),
    }
    probe = AsyncMock(return_value=set())
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(side_effect=[stats, {}]),
    ), patch(
        "services.event_sync_stream_health._probe_and_collect_failures", probe,
    ):
        await collect_stream_flow(
            [1, 2, 3],
            client=MagicMock(),
            checked_after=_NOW - timedelta(minutes=5),
            event_start_by_stream={1: _START, 2: _START, 3: _START},
            stream_names={1: "s1", 2: "s2", 3: "s3"},
            expires_at=_NOW + timedelta(minutes=1),
            probe_missing=True,
            probe_while_busy=True,
        )

    assert probe.await_args.args[1] == [2, 1, 3]


@pytest.mark.asyncio
async def test_collector_respects_active_bulk_probe_by_default(
    fixed_consumer_clock,
):
    probe = AsyncMock()
    prober = MagicMock(_probing_in_progress=True)
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value={}),
    ), patch(
        "services.event_sync_stream_health._probe_and_collect_failures", probe,
    ), patch("stream_prober.get_prober", return_value=prober):
        result = await collect_stream_flow(
            [1],
            client=MagicMock(),
            checked_after=_CHECKED,
            event_start_by_stream={1: _START},
            stream_names={1: "s1"},
            expires_at=_NOW + timedelta(minutes=1),
            probe_missing=True,
        )

    assert result == {1: None}
    probe.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [1, 2])
async def test_targeted_probe_respects_total_and_account_limits(limit):
    prober = _make_prober(max_concurrent_probes=limit)
    prober.account_probe_limits = {2: 1, 18: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    accounts = {1: 2, 2: 2, 3: 18, 4: 18}
    active = 0
    peak = 0
    by_account: dict[int, int] = {}
    stats = {}

    async def probe(stream_id, url, name, *, content, expires_at):
        nonlocal active, peak
        account = accounts[stream_id]
        active += 1
        peak = max(peak, active)
        by_account[account] = by_account.get(account, 0) + 1
        try:
            assert content is True
            assert by_account[account] == 1
            await asyncio.sleep(0.01)
            checked = datetime.now(timezone.utc)
            stats[stream_id] = _stat(
                stream_id,
                probed_at=checked,
                checked_at=checked,
            )
            return stats[stream_id]
        finally:
            active -= 1
            by_account[account] -= 1

    prober.probe_stream = AsyncMock(side_effect=probe)
    urls = {
        sid: (f"http://example.com/{sid}", f"s{sid}", account, 70 + account)
        for sid, account in accounts.items()
    }
    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health._probe_urls",
        AsyncMock(side_effect=[urls, urls]),
    ), patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(side_effect=lambda ids: {sid: stats[sid] for sid in ids}),
    ):
        result = await _probe_and_collect_failures(
            MagicMock(),
            list(accounts),
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream={sid: _START for sid in accounts},
            stream_names={sid: f"s{sid}" for sid in accounts},
        )

    assert result == set()
    assert peak <= limit


@pytest.mark.asyncio
async def test_targeted_probe_expiry_cancels_work_and_releases_permit():
    prober = _make_prober(max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    cancelled = asyncio.Event()

    async def probe(*_args, **_kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    prober.probe_stream = AsyncMock(side_effect=probe)
    urls = {1: ("http://example.com/1", "s1", 2, 72)}
    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health._probe_urls",
        AsyncMock(return_value=urls),
    ):
        result = await _probe_and_collect_failures(
            MagicMock(),
            [1],
            expires_at=datetime.now(timezone.utc) + timedelta(milliseconds=20),
            event_start_by_stream={1: _START},
            stream_names={1: "s1"},
        )

    assert result == set()
    assert cancelled.is_set()
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
async def test_targeted_probe_callback_cancels_work_and_releases_permit():
    prober = _make_prober(max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    stopped = asyncio.Event()
    state = {"cancelled": False}

    async def probe(*_args, **_kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    async def cancel():
        await asyncio.sleep(0.01)
        state["cancelled"] = True

    prober.probe_stream = AsyncMock(side_effect=probe)
    urls = {1: ("http://example.com/1", "s1", 2, 72)}
    cancel_task = asyncio.create_task(cancel())
    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health._probe_urls",
        AsyncMock(return_value=urls),
    ):
        result = await _probe_and_collect_failures(
            MagicMock(),
            [1],
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream={1: _START},
            stream_names={1: "s1"},
            cancelled=lambda: state["cancelled"],
        )
    await cancel_task

    assert result == set()
    assert stopped.is_set()
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
async def test_targeted_probe_cap_leaves_later_candidates_unprobed():
    prober = _make_prober(max_concurrent_probes=1)
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    urls = AsyncMock(return_value={})
    cache = Cache()
    client = MagicMock(base_url="http://dispatcharr.test")
    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", 1,
    ), patch(
        "services.event_sync_stream_health.get_cache", return_value=cache,
    ), patch(
        "services.event_sync_stream_health._probe_urls", urls,
    ), patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value={}),
    ):
        results = []
        for _ in range(3):
            results.append(await collect_stream_flow(
                [3, 4],
                client=client,
                checked_after=_CHECKED,
                event_start_by_stream={3: _START, 4: _START},
                stream_names={3: "s3", 4: "s4"},
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
                probe_missing=True,
                probe_while_busy=True,
            ))

    assert results == [
        {3: None, 4: None},
        {3: None, 4: None},
        {3: None, 4: None},
    ]
    assert [call.args[1] for call in urls.await_args_list] == [[3], [4], [3]]
    prober.probe_stream.assert_not_awaited()


@pytest.mark.asyncio
async def test_targeted_probe_covers_more_than_two_batches():
    prober = _make_prober(max_concurrent_probes=2)
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    urls = AsyncMock(return_value={})
    cache = Cache()
    client = MagicMock(base_url="http://dispatcharr.test")
    starts = {sid: _START for sid in range(1, 6)}
    names = {sid: f"s{sid}" for sid in range(1, 6)}

    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", 2,
    ), patch(
        "services.event_sync_stream_health.get_cache", return_value=cache,
    ), patch("services.event_sync_stream_health._probe_urls", urls):
        for _ in range(3):
            assert await _probe_and_collect_failures(
                client,
                list(starts),
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
                event_start_by_stream=starts,
                stream_names=names,
            ) == set()

    assert [call.args[1] for call in urls.await_args_list] == [
        [1, 2], [3, 4], [5, 1],
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cap,candidates,expected",
    [
        (0, [1, 2], []),
        (2, [1], [[1]]),
        (2, [1, 2], [[1, 2]]),
        (2, [1, 2, 3], [[1, 2]]),
    ],
)
async def test_targeted_probe_cap_boundaries(cap, candidates, expected):
    prober = _make_prober(max_concurrent_probes=2)
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    urls = AsyncMock(return_value={})
    cache = Cache()
    starts = {sid: _START for sid in candidates}
    names = {sid: f"s{sid}" for sid in candidates}
    client = MagicMock(base_url="http://dispatcharr.test")

    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", cap,
    ), patch(
        "services.event_sync_stream_health.get_cache", return_value=cache,
    ) as cache_get, patch(
        "services.event_sync_stream_health._probe_urls", urls,
    ):
        result = await _probe_and_collect_failures(
            client,
            candidates,
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream=starts,
            stream_names=names,
        )

    assert result == set()
    assert [call.args[1] for call in urls.await_args_list] == expected
    if len(candidates) <= cap or cap <= 0:
        cache_get.assert_not_called()
    else:
        cache_get.assert_called_once_with()


@pytest.mark.asyncio
async def test_overlapping_targeted_probes_reserve_different_candidates():
    prober = _make_prober(max_concurrent_probes=1)
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    cache = Cache()
    client = MagicMock(base_url="http://dispatcharr.test")
    first_lookup_started = asyncio.Event()
    release_first_lookup = asyncio.Event()
    batches = []

    async def lookup(_client, stream_ids, *, stream_names):
        batches.append(list(stream_ids))
        if len(batches) == 1:
            first_lookup_started.set()
            await release_first_lookup.wait()
        return {}

    async def run_probe():
        return await _probe_and_collect_failures(
            client,
            [3, 4],
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream={3: _START, 4: _START},
            stream_names={3: "s3", 4: "s4"},
        )

    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", 1,
    ), patch(
        "services.event_sync_stream_health.get_cache", return_value=cache,
    ), patch("services.event_sync_stream_health._probe_urls", side_effect=lookup):
        first = asyncio.create_task(run_probe())
        await first_lookup_started.wait()
        second = asyncio.create_task(run_probe())
        assert await second == set()
        release_first_lookup.set()
        assert await first == set()

    assert batches == [[3], [4]]


@pytest.mark.asyncio
async def test_targeted_probe_position_uses_stable_scope_identity():
    prober = _make_prober(max_concurrent_probes=1)
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    cache = Cache()
    urls = AsyncMock(return_value={})
    first_client = MagicMock(base_url="https://user:secret@dispatcharr.test:8443/api?token=x")
    proxy_client = PlanningDispatcharrClient(MagicMock(
        base_url="https://other:hidden@dispatcharr.test:8443/api?token=y",
    ))
    equivalent_start = _START.astimezone(timezone(timedelta(hours=-5)))

    async def run(client, *, starts=None, names=None, ids=None, seconds=60):
        selected = ids or [3, 4]
        return await _probe_and_collect_failures(
            client,
            selected,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=seconds),
            event_start_by_stream=starts or {3: _START, 4: _START},
            stream_names=names or {3: "s3", 4: "s4"},
        )

    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", 1,
    ), patch(
        "services.event_sync_stream_health.get_cache", return_value=cache,
    ), patch("services.event_sync_stream_health._probe_urls", urls):
        await run(first_client)
        await run(proxy_client, starts={3: equivalent_start, 4: equivalent_start})
        await run(first_client, seconds=30)
        await run(first_client, seconds=45)
        await run(MagicMock(base_url="https://dispatcharr.other:8443/api"))
        await run(first_client, names={3: "renamed", 4: "s4"})
        await run(first_client, starts={3: _START - timedelta(seconds=1), 4: _START})
        await run(first_client, ids=[3, 4, 5], starts={3: _START, 4: _START, 5: _START},
                  names={3: "s3", 4: "s4", 5: "s5"})

    assert [call.args[1] for call in urls.await_args_list] == [
        [3], [4], [3], [4], [3], [3], [3], [3],
    ]
    positions = cache.get("event_sync_health_positions", ttl=86400)
    assert all(len(key) == 64 for key in positions)
    assert all(set(value) == {"expires_at", "stream_id"} for value in positions.values())


@pytest.mark.asyncio
async def test_targeted_probe_lookup_timeout_keeps_reserved_progress():
    prober = _make_prober(max_concurrent_probes=1)
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    cache = Cache()
    client = MagicMock(base_url="http://dispatcharr.test")
    batches = []

    async def lookup(_client, stream_ids, *, stream_names):
        batches.append(list(stream_ids))
        if len(batches) == 1:
            await asyncio.Event().wait()
        return {}

    async def run(seconds):
        return await _probe_and_collect_failures(
            client,
            [3, 4],
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=seconds),
            event_start_by_stream={3: _START, 4: _START},
            stream_names={3: "s3", 4: "s4"},
        )

    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", 1,
    ), patch(
        "services.event_sync_stream_health.get_cache", return_value=cache,
    ), patch("services.event_sync_stream_health._probe_urls", side_effect=lookup):
        assert await run(0.02) == set()
        assert await run(60) == set()

    assert batches == [[3], [4]]
    prober.probe_stream.assert_not_awaited()


@pytest.mark.asyncio
async def test_targeted_probe_permit_timeout_keeps_reserved_progress():
    prober = _make_prober(max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    cache = Cache()
    client = MagicMock(base_url="http://dispatcharr.test")
    batches = []

    async def lookup(_client, stream_ids, *, stream_names):
        batches.append(list(stream_ids))
        if stream_ids == [3]:
            return {3: ("http://example.com/3", "s3", 2, 72)}
        return {}

    held = prober.semaphore_for_account(2)
    await held.__aenter__()
    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", 1,
    ), patch(
        "services.event_sync_stream_health.get_cache", return_value=cache,
    ), patch("services.event_sync_stream_health._probe_urls", side_effect=lookup):
        assert await _probe_and_collect_failures(
            client,
            [3, 4],
            expires_at=datetime.now(timezone.utc) + timedelta(milliseconds=20),
            event_start_by_stream={3: _START, 4: _START},
            stream_names={3: "s3", 4: "s4"},
        ) == set()
        await held.__aexit__(None, None, None)
        assert await _probe_and_collect_failures(
            client,
            [3, 4],
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream={3: _START, 4: _START},
            stream_names={3: "s3", 4: "s4"},
        ) == set()

    assert batches == [[3], [4]]
    prober.probe_stream.assert_not_awaited()
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
async def test_cancelled_targeted_probe_keeps_reserved_progress():
    prober = _make_prober(max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    cache = Cache()
    client = MagicMock(base_url="http://dispatcharr.test")
    batches = []
    probe_started = asyncio.Event()
    probe_stopped = asyncio.Event()
    state = {"cancelled": False}

    async def lookup(_client, stream_ids, *, stream_names):
        batches.append(list(stream_ids))
        if stream_ids == [3]:
            return {3: ("http://example.com/3", "s3", 2, 72)}
        return {}

    async def probe(*_args, **_kwargs):
        probe_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            probe_stopped.set()

    async def cancel():
        await probe_started.wait()
        state["cancelled"] = True

    prober.probe_stream = AsyncMock(side_effect=probe)
    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", 1,
    ), patch(
        "services.event_sync_stream_health.get_cache", return_value=cache,
    ), patch("services.event_sync_stream_health._probe_urls", side_effect=lookup):
        cancel_task = asyncio.create_task(cancel())
        assert await _probe_and_collect_failures(
            client,
            [3, 4],
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream={3: _START, 4: _START},
            stream_names={3: "s3", 4: "s4"},
            cancelled=lambda: state["cancelled"],
        ) == set()
        await cancel_task
        state["cancelled"] = False
        assert await _probe_and_collect_failures(
            client,
            [3, 4],
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream={3: _START, 4: _START},
            stream_names={3: "s3", 4: "s4"},
            cancelled=lambda: state["cancelled"],
        ) == set()

    assert batches == [[3], [4]]
    assert probe_stopped.is_set()
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [1, 2])
async def test_overlapping_targeted_calls_share_event_limit(limit):
    prober = _make_prober(max_concurrent_probes=limit)
    prober.account_probe_limits = {2: 1, 18: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    active = 0
    peak = 0
    first_started = asyncio.Event()
    both_started = asyncio.Event()
    release = asyncio.Event()

    async def probe(*_args, **_kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        first_started.set()
        if active == 2:
            both_started.set()
        try:
            await release.wait()
            return {}
        finally:
            active -= 1

    async def lookup(_client, stream_ids, *, stream_names):
        stream_id = stream_ids[0]
        account = 2 if stream_id == 1 else 18
        return {
            stream_id: (
                f"http://example.com/{stream_id}",
                f"s{stream_id}",
                account,
                70 + account,
            ),
        }

    prober.probe_stream = AsyncMock(side_effect=probe)

    async def run(stream_id):
        return await _probe_and_collect_failures(
            MagicMock(base_url="http://dispatcharr.test"),
            [stream_id],
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream={stream_id: _START},
            stream_names={stream_id: f"s{stream_id}"},
        )

    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health._probe_urls", side_effect=lookup,
    ), patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value={}),
    ):
        first = asyncio.create_task(run(1))
        await first_started.wait()
        second = asyncio.create_task(run(2))
        if limit == 1:
            for _ in range(100):
                if prober._probe_condition._waiters:
                    break
                await asyncio.sleep(0)
            assert prober._probe_condition._waiters
            assert not both_started.is_set()
        else:
            await asyncio.wait_for(both_started.wait(), timeout=1)
        release.set()
        assert await asyncio.gather(first, second) == [set(), set()]

    assert peak == limit
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
async def test_targeted_probe_position_retention_is_fixed_and_bounded():
    prober = _make_prober(max_concurrent_probes=1)
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    cache = Cache()
    urls = AsyncMock(return_value={})

    async def run(endpoint):
        return await _probe_and_collect_failures(
            MagicMock(base_url=endpoint),
            [1, 2],
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream={1: _START, 2: _START},
            stream_names={1: "s1", 2: "s2"},
        )

    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", 1,
    ), patch(
        "services.event_sync_stream_health.get_cache", return_value=cache,
    ), patch("services.event_sync_stream_health._probe_urls", urls):
        await run("http://dispatcharr.test")
        first_positions = cache.get("event_sync_health_positions", ttl=86400)
        first_expiry = next(iter(first_positions.values()))["expires_at"]
        await run("http://dispatcharr.test")
        second_positions = cache.get("event_sync_health_positions", ttl=86400)
        assert next(iter(second_positions.values()))["expires_at"] == first_expiry

        for index in range(257):
            await run(f"http://dispatcharr-{index}.test")

    positions = cache.get("event_sync_health_positions", ttl=86400)
    assert len(positions) == 256
    assert all(set(value) == {"expires_at", "stream_id"} for value in positions.values())


@pytest.mark.asyncio
async def test_targeted_probe_prunes_positions_only_on_capped_admission():
    prober = _make_prober(max_concurrent_probes=2)
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    cache = Cache()
    cache.set("event_sync_health_positions", {
        "expired": {"expires_at": -1.0, "stream_id": 99},
    })
    urls = AsyncMock(return_value={})
    client = MagicMock(base_url="http://dispatcharr.test")

    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health.get_cache", return_value=cache,
    ), patch("services.event_sync_stream_health._probe_urls", urls):
        with patch(
            "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", 2,
        ):
            await _probe_and_collect_failures(
                client,
                [1],
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
                event_start_by_stream={1: _START},
                stream_names={1: "s1"},
            )
        assert "expired" in cache.get("event_sync_health_positions", ttl=86400)

        with patch(
            "services.event_sync_stream_health.MAX_HEALTH_PROBES_PER_RUN", 1,
        ):
            await _probe_and_collect_failures(
                client,
                [1, 2],
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
                event_start_by_stream={1: _START, 2: _START},
                stream_names={1: "s1", 2: "s2"},
            )

    assert "expired" not in cache.get("event_sync_health_positions", ttl=86400)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [
    {1: ("http://example.com/1", "renamed", 2, 72)},
    {1: ("http://example.com/1", "s1", 2, 99)},
])
async def test_targeted_probe_rejects_changed_stream(changed):
    prober = MagicMock()
    prober.max_concurrent_probes = 1
    prober.refresh_account_probe_limits = AsyncMock()
    prober.semaphore_for_account.return_value = asyncio.Semaphore(1)
    prober.probe_stream = AsyncMock(return_value={})
    initial = {1: ("http://example.com/1", "s1", 2, 72)}
    with patch("stream_prober.ensure_prober", return_value=prober), patch(
        "services.event_sync_stream_health._probe_urls",
        AsyncMock(side_effect=[initial, changed]),
    ), patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value={1: _stat(1, measured=0, dark=False)}),
    ):
        result = await _probe_and_collect_failures(
            MagicMock(),
            [1],
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            event_start_by_stream={1: _START},
            stream_names={1: "s1"},
        )

    assert result == set()


@pytest.mark.asyncio
async def test_retirement_requires_repeated_current_hard_failure(
    fixed_consumer_clock,
):
    stats = {
        1: _stat(1, measured=0, dark=True, status="success", failures=9),
        2: _stat(2, measured=None, dark=None, status="failed", failures=1),
        3: _stat(3, measured=None, dark=None, status="timeout", failures=2),
        4: _stat(4, measured=None, dark=None, status="success", failures=9),
    }
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value=stats),
    ), patch("services.event_sync_stream_health._strike_threshold", return_value=3):
        result = await find_dead_streams(
            stats,
            stream_names={sid: f"s{sid}" for sid in stats},
            event_start_by_stream={sid: _START for sid in stats},
        )

    assert result == {3}


@pytest.mark.asyncio
async def test_retirement_requires_exact_name_and_current_time(
    fixed_consumer_clock,
):
    stats = {
        1: _stat(1, name="old", status="failed", failures=3),
        2: _stat(
            2,
            status="failed",
            failures=3,
            probed_at=_NOW - timedelta(minutes=10),
        ),
        3: _stat(
            3,
            status="failed",
            failures=3,
            probed_at=_NOW + timedelta(minutes=1),
        ),
    }
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value=stats),
    ), patch("services.event_sync_stream_health._strike_threshold", return_value=2):
        result = await find_dead_streams(
            stats,
            stream_names={1: "s1", 2: "s2", 3: "s3"},
            event_start_by_stream={1: _START, 2: _START, 3: _START},
            probe_before=_NOW - timedelta(minutes=5),
        )

    assert result == set()


@pytest.mark.asyncio
async def test_optional_probe_reloads_counter_before_retirement(
    fixed_consumer_clock,
):
    first_failure = _stat(
        1,
        measured=None,
        dark=None,
        status="failed",
        failures=1,
        probed_at=_NOW,
    )
    probe = AsyncMock(return_value={1})
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(side_effect=[{}, {1: first_failure}]),
    ), patch(
        "services.event_sync_stream_health._probe_and_collect_failures", probe,
    ), patch("services.event_sync_stream_health._strike_threshold", return_value=3):
        result = await find_dead_streams(
            [1],
            stream_names={1: "s1"},
            client=MagicMock(),
            probe_missing=True,
            event_start_by_stream={1: _START},
            probe_before=_NOW - timedelta(minutes=1),
            expires_at=_NOW + timedelta(minutes=1),
        )

    assert result == set()
    assert probe.await_args.kwargs["event_start_by_stream"] == {1: _START}


@pytest.mark.asyncio
async def test_optional_probe_accepts_reloaded_repeated_failure(
    fixed_consumer_clock,
):
    repeated = _stat(
        1,
        measured=None,
        dark=None,
        status="failed",
        failures=2,
        probed_at=_NOW,
    )
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(side_effect=[{}, {1: repeated}]),
    ), patch(
        "services.event_sync_stream_health._probe_and_collect_failures",
        AsyncMock(return_value={1}),
    ), patch("services.event_sync_stream_health._strike_threshold", return_value=3):
        result = await find_dead_streams(
            [1],
            stream_names={1: "s1"},
            client=MagicMock(),
            probe_missing=True,
            event_start_by_stream={1: _START},
            expires_at=_NOW + timedelta(minutes=1),
        )

    assert result == {1}


@pytest.mark.asyncio
async def test_delisted_future_stream_is_preserved(fixed_consumer_clock):
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value={}),
    ):
        result = await find_dead_streams(
            [1, 2],
            stream_names={1: "s1", 2: "s2"},
            stale_stream_ids={1, 2},
            event_start_by_stream={
                1: _START,
                2: _NOW + timedelta(hours=1),
            },
        )

    assert result == {1}


@pytest.mark.asyncio
async def test_unreadable_health_keeps_only_scoped_delisting(
    fixed_consumer_clock,
):
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(side_effect=RuntimeError("unavailable")),
    ):
        result = await find_dead_streams(
            [1, 2],
            stream_names={1: "s1"},
            stale_stream_ids={1, 2},
            event_start_by_stream={1: _START, 2: _START},
        )

    assert result == {1}


@pytest.mark.asyncio
async def test_working_set_uses_only_collector_true(fixed_consumer_clock):
    stats = {
        1: _stat(1),
        2: _stat(2, measured=4_000_000, dark=None),
        3: _stat(3, measured=0, dark=False),
    }
    with patch(
        "services.event_sync_stream_health._load_stats",
        AsyncMock(return_value=stats),
    ):
        result = await find_working_streams(
            [1, 2, 3],
            event_start_by_stream={1: _START, 2: _START, 3: _START},
            stream_names={1: "s1", 2: "s2", 3: "s3"},
            checked_after=_CHECKED,
        )

    assert result == {1}


def test_reloaded_stats_preserve_exact_stream_name(test_session):
    test_session.add(StreamStats(stream_id=91, stream_name="exact scoped name"))
    test_session.commit()

    with patch("stream_prober.get_session", return_value=test_session):
        rows = StreamProber.get_stats_by_stream_ids([91])

    assert rows[91]["stream_name"] == "exact scoped name"


@pytest.mark.asyncio
async def test_account_wait_keeps_loading_until_admitted():
    current = [_NOW]
    prober = _make_prober(max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    stats = {}

    async def probe(sid, url, name, *, content, expires_at):
        assert expires_at is None
        stats[sid] = _stat(sid, probed_at=current[0], checked_at=current[0])

    prober.probe_stream = AsyncMock(side_effect=probe)
    urls = {1: ("http://example.com/1", "s1", 2, 72)}
    with patch("services.event_sync_stream_health.datetime", _clock(lambda: current[0])), \
         patch("stream_prober.ensure_prober", return_value=prober), \
         patch("stream_prober.get_prober", return_value=prober), \
         patch("services.event_sync_stream_health._probe_urls", AsyncMock(return_value=urls)), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(side_effect=lambda ids: stats.copy())):
        async with prober.semaphore_for_account(2):
            task = asyncio.create_task(collect_stream_flow(
                [1], client=MagicMock(), checked_after=_CHECKED,
                event_start_by_stream={1: _START}, stream_names={1: "s1"},
                expires_at=None, probe_missing=True, stop_when_playable=True, event_streams={"a": frozenset({1})},
            ))
            for _ in range(5):
                await asyncio.sleep(0)
            current[0] += timedelta(seconds=61)
            assert not task.done()
            prober.probe_stream.assert_not_awaited()
        result = await asyncio.wait_for(task, 2)
    assert result == {1: True}
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [2, 200, 201])
async def test_positive_probe_rotates_small_and_capped_batches(count):
    current = [_NOW]
    cache = Cache()
    prober = _make_prober(max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    stats = {}
    started = []
    stopped = asyncio.Event()

    async def probe(sid, url, name, **kwargs):
        started.append(sid)
        if sid == 1:
            current[0] += timedelta(seconds=61)
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        else:
            stats[sid] = _stat(sid, probed_at=current[0], checked_at=current[0])

    prober.probe_stream = AsyncMock(side_effect=probe)
    client = MagicMock(base_url="http://dispatcharr.test")
    client.get_streams_by_ids = AsyncMock(side_effect=lambda ids: [
        {"id": sid, "name": f"s{sid}", "url": f"http://example.com/{sid}",
         "m3u_account": 2, "channel_group_id": 72} for sid in ids
    ])
    with patch("services.event_sync_stream_health.datetime", _clock(lambda: current[0])), \
         patch("services.event_sync_stream_health.get_cache", return_value=cache), \
         patch("stream_prober.ensure_prober", return_value=prober), \
         patch("stream_prober.get_prober", return_value=prober), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(side_effect=lambda ids: stats.copy())):
        values = dict(
            client=client, checked_after=_CHECKED,
            event_start_by_stream={sid: _START for sid in range(1, count + 1)},
            stream_names={sid: f"s{sid}" for sid in range(1, count + 1)},
            probe_missing=True, stop_when_playable=True, event_streams={"a": frozenset(range(1, count + 1))},
        )
        first = await collect_stream_flow(
            range(1, count + 1), expires_at=_NOW + timedelta(seconds=60), **values,
        )
        assert set(first.values()) == {None}
        assert stopped.is_set()
        assert started == [1]
        second = await asyncio.wait_for(collect_stream_flow(
            range(1, count + 1), expires_at=None, **values,
        ), 2)
        assert second[2] is True
        assert started == [1, 2]
        positions = cache.get("event_sync_health_positions", ttl=86400)
        assert len(positions) == 1
        assert next(iter(positions.values()))["stream_id"] == 2
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
async def test_positive_probe_cancels_only_owned_siblings(fixed_consumer_clock):
    prober = _make_prober(max_concurrent_probes=4)
    prober.account_probe_limits = {2: 2, 18: 1, 19: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    started = asyncio.Event()
    stopped = asyncio.Event()
    unrelated_started = asyncio.Event()
    release = asyncio.Event()
    separate_started = asyncio.Event()
    stats = {}

    async def probe(sid, url, name, **kwargs):
        if sid == 99:
            unrelated_started.set()
            await release.wait()
        elif sid == 1:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        elif sid == 4:
            await started.wait()
            separate_started.set()
            stats[sid] = _stat(sid)
        else:
            await separate_started.wait()
            stats[sid] = _stat(sid)

    prober.probe_stream = AsyncMock(side_effect=probe)
    client = MagicMock(base_url="http://dispatcharr.test")
    client.get_streams_by_ids = AsyncMock(side_effect=lambda ids: [
        {"id": sid, "name": f"s{sid}", "url": f"http://example.com/{sid}",
         "m3u_account": 18 if sid == 99 else 19 if sid == 4 else 2} for sid in ids
    ])
    with patch("stream_prober.ensure_prober", return_value=prober), \
         patch("stream_prober.get_prober", return_value=prober), \
         patch("services.event_sync_stream_health.get_cache", return_value=Cache()), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(side_effect=lambda ids: stats.copy())):
        unrelated = asyncio.create_task(_probe_and_collect_failures(
            client, [99], expires_at=None, event_start_by_stream={99: _START},
            stream_names={99: "s99"},
        ))
        await unrelated_started.wait()
        result = await asyncio.wait_for(collect_stream_flow(
            [1, 2, 3, 4], client=client, checked_after=_CHECKED,
            event_start_by_stream={sid: _START for sid in [1, 2, 3, 4]},
            stream_names={sid: f"s{sid}" for sid in [1, 2, 3, 4]},
            expires_at=None, probe_missing=True, stop_when_playable=True, event_streams={"a": frozenset({1, 2, 3}), "b": frozenset({4})},
        ), 2)
        assert result == {1: None, 2: True, 3: None, 4: True}
        assert stopped.is_set()
        assert not unrelated.done()
        assert [call.args[0] for call in prober.probe_stream.await_args_list] == [99, 1, 4, 2]
        release.set()
        await unrelated
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["url", "name", "account", "group_id", "group", "stale"])
async def test_collector_rejects_changed_identity(change, fixed_consumer_clock):
    prober = _make_prober(max_concurrent_probes=1)
    prober.refresh_account_probe_limits = AsyncMock()
    stats = {}
    original = {"id": 1, "name": "s1", "url": "http://example.com/1",
                "m3u_account": {"id": 2}, "channel_group": {"id": 72}}
    current = original.copy()

    async def probe(*args, **kwargs):
        stats[1] = _stat(1)
        if change == "url":
            current["url"] = "http://example.com/2"
        elif change == "name":
            current["name"] = "renamed"
        elif change == "account":
            current["m3u_account"] = 18
        elif change == "group_id":
            current["channel_group_id"] = 73
        elif change == "group":
            current["channel_group"] = 73
        else:
            current["is_stale"] = True

    prober.probe_stream = AsyncMock(side_effect=probe)
    client = MagicMock()
    client.get_streams_by_ids = AsyncMock(side_effect=lambda ids: [current.copy()])
    with patch("stream_prober.ensure_prober", return_value=prober), \
         patch("stream_prober.get_prober", return_value=prober), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(side_effect=lambda ids: stats.copy())):
        result = await collect_stream_flow(
            [1], client=client, checked_after=_CHECKED,
            event_start_by_stream={1: _START}, stream_names={1: "s1"},
            expires_at=None, probe_missing=True, stop_when_playable=True, event_streams={"a": frozenset({1})},
        )
    assert result == {1: None}


@pytest.mark.asyncio
async def test_cached_positive_keeps_unknown_siblings_unprobed(fixed_consumer_clock):
    probe = AsyncMock()
    with patch("services.event_sync_stream_health._load_stats", AsyncMock(return_value={1: _stat(1)})), \
         patch("services.event_sync_stream_health._probe_and_collect_failures", probe):
        result = await collect_stream_flow(
            [1, 2], client=MagicMock(), checked_after=_CHECKED,
            event_start_by_stream={1: _START, 2: _START}, stream_names={1: "s1", 2: "s2"},
            expires_at=None, probe_missing=True, stop_when_playable=True, event_streams={"a": frozenset({1, 2})},
        )
    assert result == {1: True, 2: None}
    probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_positive_reservations_do_not_rewind(fixed_consumer_clock):
    prober = _make_prober(max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    cache = Cache()
    entered = asyncio.Event()
    release = asyncio.Event()
    lookups = []

    async def urls(client, ids, **kwargs):
        lookups.append(list(ids))
        if len(lookups) == 1:
            entered.set()
            await release.wait()
        return {sid: (f"http://example.com/{sid}", f"s{sid}", 2, 72) for sid in ids}

    async def collect():
        return await _probe_and_collect_failures(
            MagicMock(base_url="http://dispatcharr.test"), [1, 2], expires_at=None,
            event_start_by_stream={1: _START, 2: _START},
            stream_names={1: "s1", 2: "s2"}, stop_when_playable=True, event_streams={"a": frozenset({1, 2})},
        )

    with patch("stream_prober.ensure_prober", return_value=prober), \
         patch("services.event_sync_stream_health.get_cache", return_value=cache), \
         patch("services.event_sync_stream_health._probe_urls", AsyncMock(side_effect=urls)), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(return_value={1: _stat(1), 2: _stat(2)})):
        first = asyncio.create_task(collect())
        await entered.wait()
        await collect()
        position = next(iter(cache.get("event_sync_health_positions", ttl=86400).values())).copy()
        assert lookups[:2] == [[1, 2], [2, 1]]
        assert position["stream_id"] == 2
        release.set()
        await first
        assert next(iter(cache.get("event_sync_health_positions", ttl=86400).values())) == position
    assert prober._account_active == {}


@pytest.mark.asyncio
async def test_media_timeout_releases_account_without_loading_expiry(fixed_consumer_clock):
    prober = _make_prober(max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    prober._run_ffprobe = AsyncMock(side_effect=asyncio.TimeoutError)
    prober._measure_stream_bitrate = AsyncMock(return_value=None)
    prober._save_probe_result = MagicMock(return_value={"stream_id": 1, "probe_status": "timeout"})
    prober._push_stats_to_dispatcharr = AsyncMock()
    with patch("stream_prober.ensure_prober", return_value=prober), \
         patch("services.event_sync_stream_health._probe_urls", AsyncMock(return_value={
             1: ("http://example.com/1", "s1", 2, 72),
         })), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(return_value={})):
        await asyncio.wait_for(_probe_and_collect_failures(
            MagicMock(), [1], expires_at=None,
            event_start_by_stream={1: _START}, stream_names={1: "s1"},
        ), 2)
    prober._run_ffprobe.assert_awaited_once()
    assert prober._save_probe_result.call_args.args[3] == "timeout"
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("cached,unknown", [(False, False), (True, False), (False, True)])
async def test_independent_events_each_receive_health(fixed_consumer_clock, cached, unknown):
    prober = _make_prober(max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    prober.refresh_account_probe_limits = AsyncMock()
    stats = {1: _stat(1)} if cached else {}
    started = []

    async def probe(sid, url, name, **kwargs):
        started.append(sid)
        if not (unknown and sid in {1, 3}):
            stats[sid] = _stat(sid)

    prober.probe_stream = AsyncMock(side_effect=probe)
    client = MagicMock(base_url="http://dispatcharr.test")
    client.get_streams_by_ids = AsyncMock(side_effect=lambda ids: [
        {"id": sid, "name": f"s{sid}", "url": f"http://example.com/{sid}", "m3u_account": 2}
        for sid in ids
    ])
    with patch("stream_prober.ensure_prober", return_value=prober), \
         patch("stream_prober.get_prober", return_value=prober), \
         patch("services.event_sync_stream_health.get_cache", return_value=Cache()), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(side_effect=lambda ids: stats.copy())):
        result = await collect_stream_flow(
            [1, 2, 3], client=client, checked_after=_CHECKED,
            event_start_by_stream={sid: _START for sid in [1, 2, 3]},
            stream_names={sid: f"s{sid}" for sid in [1, 2, 3]},
            expires_at=None, probe_missing=True, stop_when_playable=True,
            event_streams={"a": frozenset({1, 3}), "b": frozenset({2})},
        )
    assert started == ([2] if cached else [1, 2, 3] if unknown else [1, 2])
    assert result[2] is True
    assert result[1] is (None if unknown else True)
    assert result[3] is None
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
async def test_event_selection_interleaves_one_global_cap(fixed_consumer_clock):
    prober = _make_prober(max_concurrent_probes=1)
    prober.refresh_account_probe_limits = AsyncMock()
    prober.probe_stream = AsyncMock()
    cache = Cache()
    lookups = []

    async def urls(client, ids, **kwargs):
        lookups.append(list(ids))
        return {sid: (f"http://example.com/{sid}", f"s{sid}", 2, 72) for sid in ids}

    with patch("stream_prober.ensure_prober", return_value=prober), \
         patch("stream_prober.get_prober", return_value=prober), \
         patch("services.event_sync_stream_health.get_cache", return_value=cache), \
         patch("services.event_sync_stream_health._probe_urls", AsyncMock(side_effect=urls)), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(return_value={})):
        for _ in range(2):
            await collect_stream_flow(
                range(1, 203), client=MagicMock(base_url="http://dispatcharr.test"),
                checked_after=_CHECKED, expires_at=None, probe_missing=True,
                event_start_by_stream={sid: _START for sid in range(1, 203)},
                stream_names={sid: f"s{sid}" for sid in range(1, 203)},
                stop_when_playable=True,
                event_streams={"a": frozenset(range(1, 202)), "b": frozenset({202})},
            )
    assert lookups[0][:3] == [1, 202, 2]
    assert len(lookups[0]) == 200
    assert len(set(lookups[0])) == 200
    assert lookups[2][:2] == [200, 201]
    assert len(lookups[2]) == 200
    assert prober.probe_stream.await_count == 400
    assert len(cache.get("event_sync_health_positions", ttl=86400)) == 1


@pytest.mark.asyncio
async def test_group_success_respects_the_callers_cutoff(fixed_consumer_clock):
    prober = _make_prober(max_concurrent_probes=1)
    prober.refresh_account_probe_limits = AsyncMock()
    stats = {}

    async def probe(sid, url, name, **kwargs):
        observed = _NOW - timedelta(seconds=30) if sid == 1 else _NOW
        stats[sid] = _stat(sid, probed_at=observed, checked_at=observed)

    prober.probe_stream = AsyncMock(side_effect=probe)
    client = MagicMock(base_url="http://dispatcharr.test")
    client.get_streams_by_ids = AsyncMock(side_effect=lambda ids: [
        {"id": sid, "name": f"s{sid}", "url": f"http://example.com/{sid}"} for sid in ids
    ])
    with patch("stream_prober.ensure_prober", return_value=prober), \
         patch("stream_prober.get_prober", return_value=prober), \
         patch("services.event_sync_stream_health.get_cache", return_value=Cache()), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(side_effect=lambda ids: stats.copy())):
        result = await collect_stream_flow(
            [1, 2], client=client, checked_after=_NOW,
            event_start_by_stream={1: _START, 2: _START}, stream_names={1: "s1", 2: "s2"},
            expires_at=None, probe_missing=True, stop_when_playable=True,
            event_streams={"a": frozenset({1, 2})},
        )
    assert result == {1: None, 2: True}
    assert [call.args[0] for call in prober.probe_stream.await_args_list] == [1, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize("groups", [None, {}, {"": frozenset({1})}, {"a": frozenset()}, {"a": frozenset({2})}])
async def test_early_completion_requires_explicit_groups(groups):
    load = AsyncMock()
    with patch("services.event_sync_stream_health._load_stats", load):
        with pytest.raises(ValueError, match="event_streams"):
            await collect_stream_flow(
                [1], client=MagicMock(), checked_after=_CHECKED,
                event_start_by_stream={1: _START}, stream_names={1: "s1"},
                expires_at=None, probe_missing=True, stop_when_playable=True,
                event_streams=groups,
            )
    load.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_candidate_waits_for_each_event(fixed_consumer_clock):
    prober = _make_prober(max_concurrent_probes=3)
    prober.account_probe_limits = {2: 3}
    prober.refresh_account_probe_limits = AsyncMock()
    shared_started = asyncio.Event()
    first_done = asyncio.Event()
    release = asyncio.Event()
    shared_cancelled = asyncio.Event()
    stats = {}

    async def probe(sid, url, name, **kwargs):
        if sid == 3:
            shared_started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                shared_cancelled.set()
                raise
            stats[sid] = _stat(sid)
        elif sid == 1:
            await shared_started.wait()
            stats[sid] = _stat(sid)
            first_done.set()

    prober.probe_stream = AsyncMock(side_effect=probe)
    client = MagicMock(base_url="http://dispatcharr.test")
    client.get_streams_by_ids = AsyncMock(side_effect=lambda ids: [
        {"id": sid, "name": f"s{sid}", "url": f"http://example.com/{sid}", "m3u_account": 2}
        for sid in ids
    ])
    with patch("stream_prober.ensure_prober", return_value=prober), \
         patch("stream_prober.get_prober", return_value=prober), \
         patch("services.event_sync_stream_health.get_cache", return_value=Cache()), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(side_effect=lambda ids: stats.copy())):
        task = asyncio.create_task(collect_stream_flow(
            [1, 2, 3], client=client, checked_after=_CHECKED,
            event_start_by_stream={sid: _START for sid in [1, 2, 3]},
            stream_names={sid: f"s{sid}" for sid in [1, 2, 3]},
            expires_at=None, probe_missing=True, stop_when_playable=True,
            event_streams={"a": frozenset({1, 3}), "b": frozenset({2, 3})},
        ))
        await first_done.wait()
        await asyncio.sleep(0)
        assert not shared_cancelled.is_set()
        assert not task.done()
        release.set()
        result = await task
    assert result == {1: True, 2: None, 3: True}
    assert [call.args[0] for call in prober.probe_stream.await_args_list] == [1, 2, 3]
    assert prober._account_active == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["account", "media"])
async def test_group_collection_cancellation_awaits_children(fixed_consumer_clock, phase):
    prober = _make_prober(max_concurrent_probes=1)
    prober.account_probe_limits = {2: 1}
    admitted = asyncio.Event()
    started = asyncio.Event()
    stopped = asyncio.Event()
    prober.refresh_account_probe_limits = AsyncMock(side_effect=lambda: admitted.set())

    async def probe(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    prober.probe_stream = AsyncMock(side_effect=probe)
    client = MagicMock(base_url="http://dispatcharr.test")
    client.get_streams_by_ids = AsyncMock(return_value=[
        {"id": 1, "name": "s1", "url": "http://example.com/1", "m3u_account": 2},
    ])
    permit = prober.semaphore_for_account(2)
    if phase == "account":
        await permit.__aenter__()
    try:
        with patch("stream_prober.ensure_prober", return_value=prober), \
             patch("stream_prober.get_prober", return_value=prober), \
             patch("services.event_sync_stream_health._load_stats", AsyncMock(return_value={})):
            task = asyncio.create_task(collect_stream_flow(
                [1], client=client, checked_after=_CHECKED,
                event_start_by_stream={1: _START}, stream_names={1: "s1"},
                expires_at=None, probe_missing=True, stop_when_playable=True,
                event_streams={"a": frozenset({1})},
            ))
            await (admitted.wait() if phase == "account" else started.wait())
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            if phase == "media":
                assert stopped.is_set()
            else:
                prober.probe_stream.assert_not_awaited()
                assert prober._account_active == {2: 1}
    finally:
        if phase == "account":
            await permit.__aexit__(None, None, None)
    assert prober._account_active == {}
    assert prober._event_probes == 0


@pytest.mark.asyncio
async def test_cached_proof_ages_while_another_event_waits():
    current = [_NOW]
    prober = _make_prober(max_concurrent_probes=1)
    prober.refresh_account_probe_limits = AsyncMock()
    observed = _NOW - timedelta(minutes=4)
    stats = {1: _stat(1, probed_at=observed, checked_at=observed)}

    async def probe(sid, url, name, **kwargs):
        assert sid == 2
        current[0] += timedelta(minutes=2)
        stats[sid] = _stat(sid, probed_at=current[0], checked_at=current[0])

    prober.probe_stream = AsyncMock(side_effect=probe)
    client = MagicMock(base_url="http://dispatcharr.test")
    client.get_streams_by_ids = AsyncMock(return_value=[
        {"id": 2, "name": "s2", "url": "http://example.com/2"},
    ])
    with patch("services.event_sync_stream_health.datetime", _clock(lambda: current[0])), \
         patch("stream_prober.ensure_prober", return_value=prober), \
         patch("stream_prober.get_prober", return_value=prober), \
         patch("services.event_sync_stream_health._load_stats", AsyncMock(side_effect=lambda ids: stats.copy())):
        result = await collect_stream_flow(
            [1, 2], client=client, checked_after=_CHECKED,
            event_start_by_stream={1: _NOW - timedelta(minutes=5), 2: _START},
            stream_names={1: "s1", 2: "s2"}, expires_at=None,
            probe_missing=True, stop_when_playable=True,
            event_streams={"a": frozenset({1}), "b": frozenset({2})},
        )
    assert result == {1: None, 2: True}
    assert prober.probe_stream.await_count == 1
