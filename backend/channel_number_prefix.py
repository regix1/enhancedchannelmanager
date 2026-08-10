"""The ``"<number> <separator> "`` prefix ECM writes, and the lookup that
has to see through it.

``ActionExecutor._apply_channel_number_in_name`` prepends that prefix when
``settings.include_channel_number_in_name`` is on, using
``settings.channel_number_separator``. Everything here is the other half of
that pair: :func:`strip_channel_number_prefix` removes what the method
writes, and :func:`channel_name_to_id` builds the name map Event Sync
promotion resolves an existing channel through, keyed under both spellings
so the two cannot drift.

A channel Dispatcharr stores as ``"500 - Fury Vs Usyk @ Aug 09 08:00 PM"``
must still match the unprefixed name
``services.event_sync_promote.promoted_channel_name`` derives, or the
event reads as new: the run creates a second channel for it, and with
``skip_past_events`` on, the first one leaves the rule's managed set.

**Strip only what was written.** The caller supplies the separator the
settings currently write, and supplies ``None`` when the setting is off —
then no prefix was ever written, and stripping one would key the map on a
name nobody has. A title that legitimately opens with digits (``"2024 -
Olympics Opening @ ..."``) is left whole on every instance that does not
number its channel names.

Other modules carry their own inline prefix regexes for their own
matching surfaces (EPG matching, dedup, normalization); those have
different character classes on purpose and are not this module's job.
"""
import re
from typing import Iterable, Optional

# Every separator settings.channel_number_separator can hold. Used when a
# name has to lose whatever prefix it currently carries regardless of what
# the setting says today, which is what rewriting the prefix needs.
ANY_CHANNEL_NUMBER_SEPARATOR = '|-:'

_PREFIX_RE_BY_SEPARATORS: dict[str, re.Pattern] = {}


def _prefix_re(separators: str) -> re.Pattern:
    """Compiled ``"<number><one of separators> "`` matcher, built once per
    separator set (there are at most four in play)."""
    pattern = _PREFIX_RE_BY_SEPARATORS.get(separators)
    if pattern is None:
        pattern = re.compile(
            r'^\d+(?:\.\d+)?\s*[' + re.escape(separators) + r']\s*'
        )
        _PREFIX_RE_BY_SEPARATORS[separators] = pattern
    return pattern


def strip_channel_number_prefix(
    name: str, separators: str = ANY_CHANNEL_NUMBER_SEPARATOR,
) -> str:
    """Return ``name`` without a leading ``"<number> <separator> "``.

    ``"500 - Fury Vs Usyk"`` and ``"4000 | USA Network"`` both come back
    without the prefix. A name that carries no such prefix is returned
    unchanged, edges and all, so a caller can key both spellings.

    ``separators`` narrows which characters count, so a caller that knows
    the separator its settings write strips exactly that one and leaves a
    title that merely opens with digits alone. The default accepts all
    three, which is what a rewrite needs: the prefix already on the name
    may have been written under an earlier setting.

    A name that is *nothing but* a prefix (``"500 - "``) comes back
    unchanged rather than empty — the same rail
    :meth:`ActionExecutor._apply_channel_number_in_name` keeps, since an
    empty channel name is never a usable lookup key.
    """
    stripped, replaced = _prefix_re(separators).subn('', name)
    if not replaced:
        return name
    stripped = stripped.strip()
    return stripped or name


def channel_name_to_id(
    channels: Iterable[dict], separator: Optional[str],
) -> dict[str, int]:
    """Lowercased channel name -> id for ``channels``, both spellings.

    Every channel is keyed under its stored name and, when ``separator``
    is set, under that name with the channel-number prefix removed, so a
    derived name matches a channel Dispatcharr stored with a number in
    front of it. ``separator`` is
    ``settings.channel_number_separator`` when
    ``settings.include_channel_number_in_name`` is on, and ``None`` when
    it is off — nothing writes a prefix then, so nothing strips one.

    Lowest id wins a shared key, so the map is deterministic whichever
    order the channels arrive in. Channels with no name or no id are
    skipped.
    """
    name_to_id: dict[str, int] = {}
    for channel in channels:
        name, channel_id = channel.get("name"), channel.get("id")
        if not name or channel_id is None:
            continue
        spellings = {name.lower()}
        if separator:
            spellings.add(
                strip_channel_number_prefix(name, separator).lower()
            )
        for key in spellings:
            if key not in name_to_id or channel_id < name_to_id[key]:
                name_to_id[key] = channel_id
    return name_to_id
