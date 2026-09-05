"""Pure matching helpers for preview-first EPG guide migration."""

from __future__ import annotations

import hashlib
import hmac
import json
import io
import base64
import time
import uuid
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from typing import Any


PREVIEW_AUDIENCE = "ecm-guide-migration-apply"
PREVIEW_ISSUER = "enhanced-channel-manager"
PREVIEW_TTL_SECONDS = 300
_PREVIEW_KEY_DOMAIN = b"ecm:guide-migration:preview-token:v1"
_PREVIEW_INSTANCE_DOMAIN = b"ecm:guide-migration:instance:v1"


async def stream_xmltv(
    source: dict, *, max_download: int, max_decoded: int,
    timeout: float = 120.0, transport=None, read_timeout: float = 30.0,
    diagnostics: dict | None = None,
):
    """Yield bounded, validated XML chunks without retaining the document."""
    import asyncio
    import zlib
    from urllib.parse import urljoin

    import httpx
    from fastapi import HTTPException
    from security.ssrf import SSRFError, check_redirect_depth, get_ssrf_mode, validate_redirect
    from tasks.dbas_sync_client import _PinnedSSRFTransport

    url = source.get("url")
    if not url:
        raise HTTPException(400, "XMLTV source has no downloadable URL.")
    downloaded = decoded = 0
    guard = b""
    compressed = str(url).lower().split("?", 1)[0].endswith(".gz")
    decompressor = None
    if diagnostics is not None:
        diagnostics.update(wire_bytes=0, decoded_bytes=0, transport_complete=False)
    try:
        async with asyncio.timeout(timeout):
            started = time.monotonic()
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(read_timeout, connect=10.0),
                follow_redirects=False,
                headers={"Accept-Encoding": "gzip, identity"},
                transport=transport or _PinnedSSRFTransport(verify=True),
            ) as http_client:
                current_url = url
                depth = 0
                while True:
                    async with http_client.stream("GET", current_url) as response:
                        if response.is_redirect:
                            location = response.headers.get("location")
                            if not location:
                                raise HTTPException(502, "XMLTV source returned an invalid redirect.")
                            depth += 1
                            check_redirect_depth(depth)
                            next_url = urljoin(current_url, location)
                            validate_redirect(current_url, next_url, get_ssrf_mode())
                            current_url = next_url
                            continue
                        encoding = response.headers.get("content-encoding", "").strip().lower()
                        compressed = (compressed or encoding in {"gzip", "x-gzip"}
                                      or str(current_url).lower().split("?", 1)[0].endswith(".gz"))
                        if diagnostics is not None:
                            mime = response.headers.get("content-type", "").partition(";")[0].strip().lower()
                            transfer = response.headers.get("transfer-encoding", "").strip().lower()
                            length = response.headers.get("content-length", "").strip()
                            if 0 < len(length) <= 19 and length.isascii() and length.isdigit():
                                diagnostics["content_length"] = int(length)
                            diagnostics.update(
                                headers_ms=max(0, int((time.monotonic() - started) * 1000)),
                                transfer_encoding="chunked" if transfer == "chunked" else "other" if transfer else "absent",
                                http_version=response.http_version if response.http_version in {"HTTP/1.0", "HTTP/1.1", "HTTP/2", "HTTP/3"} else "other",
                                http_status=response.status_code,
                                content_type=("absent" if not mime else "xml" if mime in {"application/xml", "text/xml"} or mime.endswith("+xml")
                                              else "gzip" if mime in {"application/gzip", "application/x-gzip"}
                                              else "html" if mime == "text/html" else "text" if mime.startswith("text/") else "other"),
                                content_encoding=encoding if encoding in {"gzip", "identity"} else "other" if encoding else "absent",
                                compression="gzip" if compressed else "identity",
                            )
                        response.raise_for_status()
                        if compressed:
                            decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                        async for chunk in response.aiter_raw():
                            downloaded += len(chunk)
                            if diagnostics is not None:
                                diagnostics["wire_bytes"] = downloaded
                            if downloaded > max_download:
                                raise HTTPException(413, "XMLTV download exceeds its size limit.")
                            pending = chunk
                            while pending:
                                if decompressor is None:
                                    piece, pending = pending[:65536], pending[65536:]
                                else:
                                    piece = decompressor.decompress(pending, min(65536, max_decoded - decoded + 1))
                                    pending = decompressor.unconsumed_tail
                                decoded += len(piece)
                                if diagnostics is not None:
                                    diagnostics["decoded_bytes"] = decoded
                                if decoded > max_decoded:
                                    raise HTTPException(413, "XMLTV decoded content exceeds its size limit.")
                                probe = (guard + piece).lower()
                                if b"<!doctype" in probe or b"<!entity" in probe or b"\x00" in probe:
                                    raise HTTPException(422, "XMLTV DTDs, entities and non-UTF encodings are not supported.")
                                guard = probe[-16:]
                                if piece:
                                    yield piece
                            if decompressor is not None and decompressor.unused_data:
                                raise HTTPException(422, "XMLTV gzip has trailing content.")
                        if diagnostics is not None:
                            diagnostics["transport_complete"] = True
                        if decompressor is not None and not decompressor.eof:
                            if diagnostics is not None:
                                diagnostics["failure"] = "incomplete_gzip"
                            raise HTTPException(422, "XMLTV gzip is incomplete.")
                    break
    except HTTPException:
        raise
    except SSRFError as exc:
        raise HTTPException(400, "XMLTV source URL is blocked by the outbound security policy.") from exc
    except (httpx.HTTPError, TimeoutError, zlib.error) as exc:
        if diagnostics is not None and isinstance(exc, httpx.RemoteProtocolError) and downloaded:
            diagnostics["failure"] = "incomplete_body"
        raise HTTPException(502, "Could not read the configured XMLTV source.") from exc


class PreviewTokenError(ValueError):
    """A preview token is invalid, expired, or for another actor/instance."""


@dataclass(frozen=True)
class XMLTVLCNIndex:
    channel_to_lcn: dict[str, str]
    lcn_to_channels: dict[str, tuple[str, ...]]


def parse_xmltv_lcn_index(content: bytes) -> XMLTVLCNIndex:
    """Parse the XMLTV channel header, preferring ``gnid`` over legacy ``lcn``."""
    pairs: list[tuple[str, str]] = []
    stream = io.BytesIO(content)
    root = None
    for event, element in ET.iterparse(stream, events=("start", "end")):
        tag = element.tag.rsplit("}", 1)[-1]
        if event == "start" and root is None:
            root = element
        if event == "end" and tag == "channel":
            channel_id = (element.get("id") or "").strip()
            gnid = None
            lcn = None
            for child in element:
                child_tag = child.tag.rsplit("}", 1)[-1]
                value = (child.text or "").strip()
                if child_tag == "gnid" and value and gnid is None:
                    gnid = value
                elif child_tag == "lcn" and value and lcn is None:
                    lcn = value
            if channel_id and (gnid or lcn):
                pairs.append((channel_id, gnid or lcn or ""))
            if root is not None:
                root.clear()
        elif event == "end" and tag == "programme":
            break
    return build_xmltv_lcn_index(pairs)


def build_xmltv_lcn_index(channels: list[tuple[str, str]]) -> XMLTVLCNIndex:
    """Build both directions without silently choosing duplicate LCN rows."""
    channel_to_lcn: dict[str, str] = {}
    reverse: dict[str, list[str]] = defaultdict(list)
    for channel_id, lcn in channels:
        channel_id = channel_id.strip()
        lcn = lcn.strip()
        if not channel_id or not lcn:
            continue
        channel_to_lcn[channel_id] = lcn
        reverse[lcn].append(channel_id)
    return XMLTVLCNIndex(
        channel_to_lcn=channel_to_lcn,
        lcn_to_channels={key: tuple(values) for key, values in reverse.items()},
    )


def preview_migration(
    *,
    channels: list[dict[str, Any]],
    epg_data: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    target_source_id: int,
    xmltv_indexes: dict[int, XMLTVLCNIndex],
) -> list[dict[str, Any]]:
    """Classify every channel; only an unambiguous LCN mapping is ``ready``."""
    source_by_id = {source["id"]: source for source in sources}
    target = source_by_id[target_source_id]
    epg_by_id = {row["id"]: row for row in epg_data}
    target_by_tvg: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in epg_data:
        if row.get("epg_source") == target_source_id:
            target_by_tvg[str(row.get("tvg_id") or "")].append(row)

    rows: list[dict[str, Any]] = []
    for channel in channels:
        current = epg_by_id.get(channel.get("epg_data_id"))
        base = {
            "channel_id": channel["id"],
            "channel_name": channel.get("name") or f"Channel {channel['id']}",
            "current_epg_data_id": channel.get("epg_data_id"),
            "current_source_id": current.get("epg_source") if current else None,
            "current_source_name": None,
            "lcn": None,
            "target_epg_data_id": None,
            "target_name": None,
            "current_tvg_id": current.get("tvg_id") if current else None,
            "target_tvg_id": None,
        }
        if current is None:
            rows.append({**base, "status": "unassigned"})
            continue
        current_source_id = current["epg_source"]
        current_source = source_by_id.get(current_source_id)
        base["current_source_name"] = (
            current_source.get("name") if current_source else f"Source {current_source_id}"
        )
        if current_source_id == target_source_id:
            rows.append({**base, "status": "already_target"})
            continue
        if current_source is None or current_source.get("source_type") not in {
            "xmltv",
            "schedules_direct",
        }:
            rows.append({**base, "status": "unsupported_origin"})
            continue

        current_tvg = str(current.get("tvg_id") or "")
        if current_source and current_source.get("source_type") == "xmltv":
            index = xmltv_indexes.get(current_source_id)
            lcn = index.channel_to_lcn.get(current_tvg) if index else None
        else:
            # Schedules Direct imports expose the station/LCN as EPGData.tvg_id.
            lcn = current_tvg or None
        base["lcn"] = lcn
        if not lcn:
            rows.append({**base, "status": "missing_lcn"})
            continue

        if target.get("source_type") == "xmltv":
            index = xmltv_indexes.get(target_source_id)
            target_tvgs = index.lcn_to_channels.get(lcn, ()) if index else ()
        else:
            target_tvgs = (lcn,)
        candidates = [
            candidate
            for target_tvg in target_tvgs
            for candidate in target_by_tvg.get(target_tvg, ())
        ]
        if not candidates:
            rows.append({**base, "status": "missing_target"})
        elif len(candidates) > 1:
            rows.append({**base, "status": "ambiguous_target"})
        else:
            candidate = candidates[0]
            rows.append(
                {
                    **base,
                    "status": "ready",
                    "target_epg_data_id": candidate["id"],
                    "target_name": candidate.get("name"),
                    "target_tvg_id": candidate.get("tvg_id"),
                }
            )
    return rows


def _ready_identity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical = [
        {
            "channel_id": row["channel_id"],
            "current_epg_data_id": row["current_epg_data_id"],
            "current_source_id": row["current_source_id"],
            "current_tvg_id": row["current_tvg_id"],
            "lcn": row["lcn"],
            "target_epg_data_id": row["target_epg_data_id"],
            "target_tvg_id": row["target_tvg_id"],
        }
        for row in rows
    ]
    return canonical


def _preview_signing_key(secret: str) -> bytes:
    return hmac.new(secret.encode(), _PREVIEW_KEY_DOMAIN, hashlib.sha256).digest()


def _instance_binding(secret: str) -> str:
    return hmac.new(
        secret.encode(), _PREVIEW_INSTANCE_DOMAIN, hashlib.sha256
    ).hexdigest()[:32]


def create_preview_token(
    *,
    secret: str,
    issuer: str,
    actor: str,
    target_source_id: int,
    rows: list[dict[str, Any]],
    now: int | None = None,
    ttl_seconds: int = PREVIEW_TTL_SECONDS,
) -> str:
    issued = int(time.time() if now is None else now)
    envelope = {
        "v": 1,
        "iss": issuer,
        "aud": PREVIEW_AUDIENCE,
        "instance": _instance_binding(secret),
        "sub": actor,
        "iat": issued,
        "exp": issued + ttl_seconds,
        "jti": uuid.uuid4().hex,
        "target_source_id": target_source_id,
        "rows": _ready_identity(rows),
    }
    payload = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
    encoded = base64.urlsafe_b64encode(payload).rstrip(b"=")
    signature = hmac.new(_preview_signing_key(secret), encoded, hashlib.sha256).digest()
    return (
        encoded.decode()
        + "."
        + base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
    )


def verify_preview_token(
    *,
    token: str,
    secret: str,
    issuer: str,
    actor: str,
    target_source_id: int,
    rows: list[dict[str, Any]],
    now: int | None = None,
) -> dict[str, Any]:
    try:
        encoded, encoded_signature = token.split(".", 1)
        signature = base64.urlsafe_b64decode(
            encoded_signature + "=" * (-len(encoded_signature) % 4)
        )
        expected = hmac.new(
            _preview_signing_key(secret), encoded.encode(), hashlib.sha256
        ).digest()
        if not hmac.compare_digest(signature, expected):
            raise PreviewTokenError("Preview signature is invalid.")
        payload = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        envelope = json.loads(payload)
    except PreviewTokenError:
        raise
    except Exception as exc:
        raise PreviewTokenError("Preview token is malformed.") from exc

    current = int(time.time() if now is None else now)
    if envelope.get("v") != 1:
        raise PreviewTokenError("Preview token version is unsupported.")
    if envelope.get("iss") != issuer or envelope.get("aud") != PREVIEW_AUDIENCE:
        raise PreviewTokenError("Preview token belongs to another instance or audience.")
    if envelope.get("instance") != _instance_binding(secret):
        raise PreviewTokenError("Preview token belongs to another instance.")
    if envelope.get("sub") != actor:
        raise PreviewTokenError("Preview token belongs to another actor.")
    if not isinstance(envelope.get("iat"), int) or not isinstance(envelope.get("exp"), int):
        raise PreviewTokenError("Preview token timestamps are invalid.")
    if envelope["iat"] > current + 30 or envelope["exp"] <= current:
        raise PreviewTokenError("Preview token is expired or not yet valid.")
    if envelope.get("target_source_id") != target_source_id:
        raise PreviewTokenError("Preview target source changed.")
    if envelope.get("rows") != _ready_identity(rows):
        raise PreviewTokenError("Preview assignments changed.")
    return envelope
