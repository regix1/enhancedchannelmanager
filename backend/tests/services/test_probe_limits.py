"""Per-provider probe ceilings.

The failure these exist to stop: a line that allows one connection, probed
eight wide, refuses seven and ECM records those as broken streams. [76]
"""
from unittest.mock import AsyncMock

import pytest

from services.probe_limits import account_probe_limits

XC = {"id": 18, "name": "TREX", "account_type": "XC",
      "server_url": "http://line.example.test", "username": "u", "password": "p"}
STD = {"id": 2, "name": "IPTorrents", "account_type": "STD",
       "server_url": "https://plain.example.test/list.m3u"}


def _client(accounts):
    client = AsyncMock()
    client.get_m3u_accounts = AsyncMock(return_value=accounts)
    return client


def _player_api(monkeypatch, payload, raises=None):
    """Stand in for the provider's player_api.php."""
    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return payload

    class _Client:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, params=None):
            if raises:
                raise raises
            return _Response()

    monkeypatch.setattr("services.probe_limits.httpx.AsyncClient", _Client)


@pytest.mark.asyncio
async def test_reads_the_ceiling_an_xtream_codes_line_publishes(monkeypatch):
    _player_api(monkeypatch, {"user_info": {"max_connections": "1"}})
    limits = await account_probe_limits(_client([XC]))
    assert limits == {18: 1}


@pytest.mark.asyncio
async def test_a_plain_m3u_account_publishes_nothing_and_is_left_to_the_global(monkeypatch):
    _player_api(monkeypatch, {"user_info": {"max_connections": "1"}})
    limits = await account_probe_limits(_client([STD]))
    assert limits == {}


@pytest.mark.asyncio
async def test_the_operator_override_wins_over_what_the_line_claims(monkeypatch):
    _player_api(monkeypatch, {"user_info": {"max_connections": "8"}})
    limits = await account_probe_limits(_client([XC]), overrides={"18": 2})
    assert limits == {18: 2}


@pytest.mark.asyncio
async def test_an_override_reaches_an_account_that_publishes_nothing(monkeypatch):
    _player_api(monkeypatch, {"user_info": {}})
    limits = await account_probe_limits(_client([STD]), overrides={"2": 3})
    assert limits == {2: 3}


@pytest.mark.asyncio
async def test_a_line_that_does_not_answer_keeps_the_global_limit(monkeypatch):
    import httpx
    _player_api(monkeypatch, None, raises=httpx.ConnectError("refused"))
    limits = await account_probe_limits(_client([XC]))
    assert limits == {}


@pytest.mark.asyncio
async def test_a_nonsense_ceiling_is_ignored(monkeypatch):
    """Providers have been seen reporting 0 and empty strings."""
    for value in (0, "", None, "unlimited"):
        _player_api(monkeypatch, {"user_info": {"max_connections": value}})
        assert await account_probe_limits(_client([XC])) == {}


@pytest.mark.asyncio
async def test_unreadable_accounts_do_not_stop_a_probe_run():
    client = AsyncMock()
    client.get_m3u_accounts = AsyncMock(side_effect=RuntimeError("Dispatcharr down"))
    assert await account_probe_limits(client) == {}
