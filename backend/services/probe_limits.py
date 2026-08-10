"""How many probes each provider will tolerate at once.

A provider line has its own connection ceiling, and it is usually far below
the global ``max_concurrent_probes``. Probing wider than a line allows does
not slow anything down gracefully: the provider refuses every connection past
its limit, ffprobe reports those as failures, and they land in ``StreamStats``
indistinguishable from a stream that is genuinely broken. Event Sync's
promotion gate then reads those verdicts and refuses to build channels from
streams that work.

Xtream Codes accounts publish the ceiling themselves, so it is read rather
than guessed. A plain M3U URL publishes nothing, so an operator override is
the only source for those. An account with neither keeps the global limit,
which is what every account had before this existed. [76]
"""
import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

# The provider answers in well under a second; this is only here so a line
# that never responds cannot hold up building the prober.
_PLAYER_API_TIMEOUT = 8.0


async def account_probe_limits(client, overrides: dict | None = None) -> dict[int, int]:
    """Map of m3u account id to the most probes that account will accept.

    ``overrides`` is the operator's own setting, keyed by account id as a
    string because it round-trips through JSON. It wins over whatever the
    provider reports, so a line that lies about its ceiling can still be
    pinned by hand.

    Accounts that publish nothing and have no override are absent from the
    result, and the caller applies the global limit to those.
    """
    overrides = overrides or {}
    limits: dict[int, int] = {}

    try:
        accounts = await client.get_m3u_accounts()
    except Exception as e:
        logger.warning(
            "[PROBE-LIMITS] Could not read M3U accounts (%s) — every account "
            "keeps the global probe limit", e,
        )
        return limits

    for account in accounts or []:
        account_id = account.get("id")
        if account_id is None:
            continue

        override = overrides.get(str(account_id), overrides.get(account_id))
        if override:
            limits[account_id] = max(1, int(override))
            continue

        published = await _published_max_connections(account)
        if published:
            limits[account_id] = published

    if limits:
        logger.info("[PROBE-LIMITS] Per-account probe ceilings: %s", limits)
    return limits


async def _published_max_connections(account: dict) -> int | None:
    """Read ``max_connections`` off an Xtream Codes account, else None."""
    if account.get("account_type") != "XC":
        return None
    server_url = (account.get("server_url") or "").rstrip("/")
    username = account.get("username")
    password = account.get("password")
    if not (server_url and username and password):
        return None

    try:
        async with httpx.AsyncClient(timeout=_PLAYER_API_TIMEOUT) as http:
            response = await http.get(
                f"{server_url}/player_api.php",
                params={"username": username, "password": password},
            )
            response.raise_for_status()
            user_info = (response.json() or {}).get("user_info") or {}
    except (httpx.HTTPError, ValueError, asyncio.TimeoutError) as e:
        logger.warning(
            "[PROBE-LIMITS] Account %s did not report its connection limit "
            "(%s) — it keeps the global probe limit", account.get("name"), e,
        )
        return None

    try:
        limit = int(user_info.get("max_connections"))
    except (TypeError, ValueError):
        return None
    return limit if limit > 0 else None
