"""
Dummy EPG router — profile CRUD, channel assignments, preview, and XMLTV output.
"""
import asyncio
import logging
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from sqlalchemy.orm import Session

from cache import get_cache
from concurrency import run_cpu_bound
from database import get_session
from dispatcharr_client import get_client
from regex_lint import (
    lint_pattern,
    lint_substitution_pairs,
    violations_to_http_detail,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dummy-epg", tags=["Dummy EPG"])

cache = get_cache()


# =============================================================================
# Pydantic models
# =============================================================================


class SubstitutionPairModel(BaseModel):
    find: str
    replace: str
    is_regex: bool = False
    enabled: bool = True


class PatternVariantModel(BaseModel):
    name: str = "Default"
    title_pattern: Optional[str] = None
    time_pattern: Optional[str] = None
    date_pattern: Optional[str] = None
    title_template: Optional[str] = None
    description_template: Optional[str] = None
    channel_logo_url_template: Optional[str] = None
    program_poster_url_template: Optional[str] = None
    pattern_builder_examples: Optional[str] = None
    upcoming_title_template: Optional[str] = None
    upcoming_description_template: Optional[str] = None
    ended_title_template: Optional[str] = None
    ended_description_template: Optional[str] = None
    fallback_title_template: Optional[str] = None
    fallback_description_template: Optional[str] = None
    # Omitted means the profile's own program_duration applies, so a variant
    # that never sets it keeps today's behaviour. The floor is 0 rather than
    # 1 because the engine honours a stored 0 instead of reading it as
    # "unset". [25]
    program_duration: Optional[int] = Field(default=None, ge=0, le=1440)


class ChannelMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")
    channel_id: Annotated[int, Field(strict=True, gt=0)]
    source_id: Annotated[int, Field(strict=True, gt=0)]
    tvg_id: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ProfileCreateRequest(BaseModel):
    name: str
    enabled: bool = True
    name_source: str = "channel"
    stream_index: int = 1
    title_pattern: Optional[str] = None
    time_pattern: Optional[str] = None
    date_pattern: Optional[str] = None
    substitution_pairs: list[SubstitutionPairModel] = []
    title_template: Optional[str] = None
    description_template: Optional[str] = None
    upcoming_title_template: Optional[str] = None
    upcoming_description_template: Optional[str] = None
    ended_title_template: Optional[str] = None
    ended_description_template: Optional[str] = None
    fallback_title_template: Optional[str] = None
    fallback_description_template: Optional[str] = None
    event_timezone: str = "US/Eastern"
    output_timezone: Optional[str] = None
    # Same range as the variant field below it: a negative duration ends the
    # programme before it starts and overlaps the two fillers around it. [62]
    program_duration: int = Field(default=180, ge=0, le=1440)
    categories: Optional[str] = None
    channel_logo_url_template: Optional[str] = None
    program_poster_url_template: Optional[str] = None
    tvg_id_template: str = "ecm-{channel_id}"
    include_date_tag: bool = False
    include_live_tag: bool = False
    include_new_tag: bool = False
    pattern_builder_examples: Optional[str] = None
    pattern_variants: Optional[list[PatternVariantModel]] = None
    channel_group_ids: Optional[list[int]] = None
    epg_source_ids: Optional[list[Annotated[int, Field(strict=True, gt=0)]]] = None
    channel_mappings: Optional[list[ChannelMapping]] = None


class ProfileUpdateRequest(BaseModel):
    name: Optional[str] = None
    enabled: Optional[bool] = None
    name_source: Optional[str] = None
    stream_index: Optional[int] = None
    title_pattern: Optional[str] = None
    time_pattern: Optional[str] = None
    date_pattern: Optional[str] = None
    substitution_pairs: Optional[list[SubstitutionPairModel]] = None
    title_template: Optional[str] = None
    description_template: Optional[str] = None
    upcoming_title_template: Optional[str] = None
    upcoming_description_template: Optional[str] = None
    ended_title_template: Optional[str] = None
    ended_description_template: Optional[str] = None
    fallback_title_template: Optional[str] = None
    fallback_description_template: Optional[str] = None
    event_timezone: Optional[str] = None
    output_timezone: Optional[str] = None
    program_duration: Optional[int] = Field(default=None, ge=0, le=1440)
    categories: Optional[str] = None
    channel_logo_url_template: Optional[str] = None
    program_poster_url_template: Optional[str] = None
    tvg_id_template: Optional[str] = None
    include_date_tag: Optional[bool] = None
    include_live_tag: Optional[bool] = None
    include_new_tag: Optional[bool] = None
    pattern_builder_examples: Optional[str] = None
    pattern_variants: Optional[list[PatternVariantModel]] = None
    channel_group_ids: Optional[list[int]] = None
    epg_source_ids: Optional[list[Annotated[int, Field(strict=True, gt=0)]]] = None
    channel_mappings: Optional[list[ChannelMapping]] = None


class ImportYAMLRequest(BaseModel):
    """Request to import profiles from YAML."""
    yaml_content: str
    overwrite: bool = False


class PreviewRequest(BaseModel):
    sample_name: str
    substitution_pairs: list[SubstitutionPairModel] = []
    title_pattern: Optional[str] = None
    time_pattern: Optional[str] = None
    date_pattern: Optional[str] = None
    title_template: Optional[str] = None
    description_template: Optional[str] = None
    upcoming_title_template: Optional[str] = None
    upcoming_description_template: Optional[str] = None
    ended_title_template: Optional[str] = None
    ended_description_template: Optional[str] = None
    fallback_title_template: Optional[str] = None
    fallback_description_template: Optional[str] = None
    event_timezone: str = "US/Eastern"
    output_timezone: Optional[str] = None
    program_duration: int = 180
    channel_logo_url_template: Optional[str] = None
    program_poster_url_template: Optional[str] = None
    pattern_variants: Optional[list[PatternVariantModel]] = None
    # When true, response includes a per-template trace of placeholders,
    # pipe transforms (input→output) and conditional branches taken —
    # powers the preview UI's expandable trace view.
    include_trace: bool = False


class BatchPreviewRequest(BaseModel):
    sample_names: list[str]
    substitution_pairs: list[SubstitutionPairModel] = []
    title_pattern: Optional[str] = None
    time_pattern: Optional[str] = None
    date_pattern: Optional[str] = None
    title_template: Optional[str] = None
    description_template: Optional[str] = None
    upcoming_title_template: Optional[str] = None
    upcoming_description_template: Optional[str] = None
    ended_title_template: Optional[str] = None
    ended_description_template: Optional[str] = None
    fallback_title_template: Optional[str] = None
    fallback_description_template: Optional[str] = None
    event_timezone: str = "US/Eastern"
    output_timezone: Optional[str] = None
    program_duration: int = 180
    channel_logo_url_template: Optional[str] = None
    program_poster_url_template: Optional[str] = None
    pattern_variants: Optional[list[PatternVariantModel]] = None


# =============================================================================
# Profile CRUD
# =============================================================================


@router.get("/profiles")
async def list_profiles(db: Session = Depends(get_session)):
    """List all profiles with assignment count."""
    logger.debug("[DUMMY-EPG] GET /profiles")
    try:
        from models import DummyEPGProfile
        profiles = db.query(DummyEPGProfile).all()
        result = []
        for profile in profiles:
            d = profile.to_dict()
            d["group_count"] = len(profile.get_channel_group_ids())
            result.append(d)
        return result
    except Exception as e:
        logger.warning("[DUMMY-EPG] Failed to list profiles: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


def _lint_dummy_epg_profile_request(req) -> None:
    """Raise HTTP 422 if any pattern field on the profile request fails
    the regex linter (bd-eio04.7).

    Lints:
      - ``title_pattern``, ``time_pattern``, ``date_pattern`` (top-level)
      - ``find`` on substitution pairs flagged ``is_regex=True``
      - Pattern fields inside each ``pattern_variants`` entry
    """
    violations = []
    for name in ("title_pattern", "time_pattern", "date_pattern"):
        violations.extend(lint_pattern(getattr(req, name, None), field=name))

    sub_pairs = getattr(req, "substitution_pairs", None)
    if sub_pairs:
        # Pydantic models need to be serialized so the helper sees dicts.
        pairs_as_dicts = [
            p.model_dump() if hasattr(p, "model_dump") else p for p in sub_pairs
        ]
        violations.extend(
            lint_substitution_pairs(pairs_as_dicts, prefix="substitution_pairs")
        )

    variants = getattr(req, "pattern_variants", None)
    if variants:
        for idx, variant in enumerate(variants):
            v = variant.model_dump() if hasattr(variant, "model_dump") else variant
            if not isinstance(v, dict):
                continue
            for name in ("title_pattern", "time_pattern", "date_pattern"):
                violations.extend(
                    lint_pattern(
                        v.get(name), field=f"pattern_variants[{idx}].{name}"
                    )
                )

    if violations:
        logger.warning(
            "[DUMMY-EPG] Rejected profile — %d lint violation(s): %s",
            len(violations),
            [(v.field, v.code) for v in violations],
        )
        raise HTTPException(
            status_code=422, detail=violations_to_http_detail(violations)
        )


async def _configure_sources(profile, fields: dict, *, snapshot: dict | None = None) -> None:
    """Validate source selection and remember explicit external guide bindings."""
    from services.epg_programmes import _resolve_group_assignments, capture_mappings, resolve_sources

    selected = fields.get("epg_source_ids", profile.get_epg_source_ids()) or []
    mappings = fields.get("channel_mappings", profile.get_channel_mappings()) or []
    if len(selected) != len(set(selected)):
        raise HTTPException(status_code=422, detail="Programme source IDs must be unique")
    if len({item["channel_id"] for item in mappings}) != len(mappings):
        raise HTTPException(status_code=422, detail="Only one source mapping is allowed per channel")
    if not selected:
        if fields.get("channel_mappings"):
            raise HTTPException(status_code=422, detail="Channel mappings require selected programme sources")
        profile.set_epg_source_ids([])
        profile.set_channel_mappings([])
        return

    client = get_client()
    try:
        snapshot = snapshot if snapshot is not None else {}
        if "sources" not in snapshot:
            snapshot["sources"] = await client.get_epg_sources()
        sources = snapshot["sources"]
        canonical = resolve_sources(selected, sources)
        allowed = set(selected) | {source["id"] for source in canonical}
        if "channel_mappings" in fields and any(item["source_id"] not in allowed for item in mappings):
            raise ValueError("Channel mappings must belong to selected programme sources")
        mappings = [item for item in mappings if item["source_id"] in allowed]
        profile.set_epg_source_ids(selected)
        profile.set_channel_mappings(mappings)
        if "channels" not in snapshot:
            snapshot["channels"] = await _fetch_all_channels()
        channel_map = snapshot["channels"]
        assignments = _resolve_group_assignments(profile.get_channel_group_ids(), channel_map)
        links = set()
        for assignment in assignments:
            channel = channel_map[assignment["channel_id"]]
            link = channel.get("epg_data_id") or channel.get("epg_data")
            if isinstance(link, int) and not isinstance(link, bool):
                links.add(link)
        rows = snapshot.setdefault("rows", {})
        slots = asyncio.Semaphore(4)
        async def read_row(link):
            async with slots:
                rows[link] = await client.get_epg_data_by_id(link)
        await asyncio.gather(*(read_row(link) for link in links if link not in rows))
        profile.set_channel_mappings(capture_mappings(profile.to_dict(), channel_map, list(rows.values()), sources))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=502, detail="Could not verify programme sources and channel mappings") from None


def _source_changes(profile, fields: dict) -> bool:
    return (
        ("epg_source_ids" in fields and (fields["epg_source_ids"] or []) != profile.get_epg_source_ids())
        or ("channel_mappings" in fields and (fields["channel_mappings"] or []) != profile.get_channel_mappings())
        or (fields.get("channel_group_ids") is not None and fields["channel_group_ids"] != profile.get_channel_group_ids())
        or (fields.get("enabled") is True and not profile.enabled)
    )


@router.post("/profiles")
async def create_profile(req: ProfileCreateRequest, db: Session = Depends(get_session)):
    """Create a new Dummy EPG profile."""
    logger.debug("[DUMMY-EPG] POST /profiles name=%s", req.name)
    try:
        from models import DummyEPGProfile
        # Lint regex patterns before any DB work (bd-eio04.7).
        _lint_dummy_epg_profile_request(req)
        # Check for duplicate name
        existing = db.query(DummyEPGProfile).filter(
            DummyEPGProfile.name == req.name
        ).first()
        if existing:
            logger.warning("[DUMMY-EPG] Profile name already exists: %s", req.name)
            raise HTTPException(status_code=409, detail=f"Profile with name '{req.name}' already exists")

        profile = DummyEPGProfile(
            name=req.name,
            enabled=req.enabled,
            name_source=req.name_source,
            stream_index=req.stream_index,
            title_pattern=req.title_pattern,
            time_pattern=req.time_pattern,
            date_pattern=req.date_pattern,
            title_template=req.title_template,
            description_template=req.description_template,
            upcoming_title_template=req.upcoming_title_template,
            upcoming_description_template=req.upcoming_description_template,
            ended_title_template=req.ended_title_template,
            ended_description_template=req.ended_description_template,
            fallback_title_template=req.fallback_title_template,
            fallback_description_template=req.fallback_description_template,
            event_timezone=req.event_timezone,
            output_timezone=req.output_timezone,
            program_duration=req.program_duration,
            categories=req.categories,
            channel_logo_url_template=req.channel_logo_url_template,
            program_poster_url_template=req.program_poster_url_template,
            tvg_id_template=req.tvg_id_template,
            include_date_tag=req.include_date_tag,
            include_live_tag=req.include_live_tag,
            include_new_tag=req.include_new_tag,
            pattern_builder_examples=req.pattern_builder_examples,
        )
        if req.substitution_pairs:
            profile.set_substitution_pairs([p.model_dump() for p in req.substitution_pairs])
        if req.pattern_variants:
            profile.set_pattern_variants([v.model_dump() for v in req.pattern_variants])
        if req.channel_group_ids is not None:
            profile.set_channel_group_ids(req.channel_group_ids)

        await _configure_sources(profile, req.model_dump(exclude_unset=True))
        db.add(profile)
        db.commit()
        db.refresh(profile)

        cache.invalidate_prefix("dummy_epg_xmltv")
        logger.info("[DUMMY-EPG] Created profile id=%s name=%s", profile.id, profile.name)
        return profile.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.warning("[DUMMY-EPG] Failed to create profile: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("/profiles/{profile_id}")
async def get_profile(profile_id: int, db: Session = Depends(get_session)):
    """Get a single profile with its assignments."""
    logger.debug("[DUMMY-EPG] GET /profiles/%s", profile_id)
    try:
        from models import DummyEPGProfile
        profile = db.query(DummyEPGProfile).filter(
            DummyEPGProfile.id == profile_id
        ).first()
        if not profile:
            logger.warning("[DUMMY-EPG] Profile not found: %s", profile_id)
            raise HTTPException(status_code=404, detail="Profile not found")
        return profile.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("[DUMMY-EPG] Failed to get profile %s: %s", profile_id, e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@router.get("/profiles/{profile_id}/coverage")
async def get_profile_coverage(profile_id: int, db: Session = Depends(get_session)):
    """Inspect the same source decisions used to render a saved profile."""
    from models import DummyEPGProfile
    from services.epg_programmes import prepare_profiles

    try:
        profile = db.query(DummyEPGProfile).filter(DummyEPGProfile.id == profile_id).first()
        if profile is None:
            raise HTTPException(status_code=404, detail="Profile not found")
        channel_map = await _fetch_all_channels()
        p_dict = profile.to_dict()
        p_dict["channel_assignments"] = _resolve_group_assignments(
            p_dict.get("channel_group_ids", []), channel_map
        )
        p_dict["channel_map"] = channel_map
        _, coverage = await prepare_profiles([p_dict], channel_map, get_client())
        return coverage
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=502, detail="Could not inspect guide coverage") from None
    finally:
        db.close()


@router.patch("/profiles/{profile_id}")
async def update_profile(profile_id: int, req: ProfileUpdateRequest, db: Session = Depends(get_session)):
    """Update a profile (partial)."""
    logger.debug("[DUMMY-EPG] PATCH /profiles/%s", profile_id)
    try:
        from models import DummyEPGProfile
        # Lint regex patterns on the update (bd-eio04.7). Only fields
        # actually present in the PATCH are linted — an operator renaming
        # a profile shouldn't hit a 422 for a pattern they didn't edit.
        _lint_dummy_epg_profile_request(req)
        profile = db.query(DummyEPGProfile).filter(
            DummyEPGProfile.id == profile_id
        ).first()
        if not profile:
            logger.warning("[DUMMY-EPG] Profile not found: %s", profile_id)
            raise HTTPException(status_code=404, detail="Profile not found")

        # Check for name conflict if name is being changed
        if req.name is not None and req.name != profile.name:
            existing = db.query(DummyEPGProfile).filter(
                DummyEPGProfile.name == req.name,
                DummyEPGProfile.id != profile_id,
            ).first()
            if existing:
                logger.warning("[DUMMY-EPG] Profile name already exists: %s", req.name)
                raise HTTPException(status_code=409, detail=f"Profile with name '{req.name}' already exists")

        update_data = req.model_dump(exclude_unset=True)
        sub_pairs = update_data.pop("substitution_pairs", None)
        pattern_variants = update_data.pop("pattern_variants", None)
        channel_group_ids = update_data.pop("channel_group_ids", None)
        source_fields = {key: update_data.pop(key) for key in ("epg_source_ids", "channel_mappings") if key in update_data}
        configure = _source_changes(profile, {**source_fields, "channel_group_ids": channel_group_ids, "enabled": req.enabled})

        for field, value in update_data.items():
            setattr(profile, field, value)

        if sub_pairs is not None:
            profile.set_substitution_pairs([p.model_dump() if hasattr(p, "model_dump") else p for p in sub_pairs])
        if pattern_variants is not None:
            profile.set_pattern_variants([v.model_dump() if hasattr(v, "model_dump") else v for v in pattern_variants])
        if channel_group_ids is not None:
            profile.set_channel_group_ids(channel_group_ids)
        if configure:
            await _configure_sources(profile, source_fields)

        db.commit()
        db.refresh(profile)

        cache.invalidate_prefix("dummy_epg_xmltv")
        logger.info("[DUMMY-EPG] Updated profile id=%s", profile_id)
        return profile.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.warning("[DUMMY-EPG] Failed to update profile %s: %s", profile_id, e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.delete("/profiles/{profile_id}", status_code=204)
async def delete_profile(profile_id: int, db: Session = Depends(get_session)):
    """Delete a profile and its assignments (cascade)."""
    logger.debug("[DUMMY-EPG] DELETE /profiles/%s", profile_id)
    try:
        from models import DummyEPGProfile
        profile = db.query(DummyEPGProfile).filter(
            DummyEPGProfile.id == profile_id
        ).first()
        if not profile:
            logger.warning("[DUMMY-EPG] Profile not found: %s", profile_id)
            raise HTTPException(status_code=404, detail="Profile not found")

        # Best-effort: clean up matching Dispatcharr EPG source(s)
        try:
            client = get_client()
            epg_sources = await client.get_epg_sources()
            suffix = f"/dummy-epg/xmltv/{profile_id}"
            for src in epg_sources:
                if (src.get("url") or "").endswith(suffix):
                    await client.delete_epg_source(src["id"])
                    logger.info("[DUMMY-EPG] Deleted Dispatcharr EPG source id=%s for profile %s", src["id"], profile_id)
        except Exception as e:
            logger.warning("[DUMMY-EPG] Could not clean up Dispatcharr EPG source for profile %s: %s", profile_id, e)

        db.delete(profile)
        db.commit()

        cache.invalidate_prefix("dummy_epg_xmltv")
        logger.info("[DUMMY-EPG] Deleted profile id=%s", profile_id)
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.warning("[DUMMY-EPG] Failed to delete profile %s: %s", profile_id, e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# =============================================================================
# Preview & Output
# =============================================================================


@router.post("/preview")
async def preview_epg(req: PreviewRequest):
    """Test the EPG pipeline with sample data. Zero-write and zero-read — the
    request carries everything the engine needs. When include_trace=true the
    response carries a `traces` dict with per-field step-by-step rendering
    (placeholders, pipes, conditionals)."""
    logger.debug("[DUMMY-EPG] POST /preview sample_name=%s trace=%s", req.sample_name, req.include_trace)
    try:
        from dummy_epg_engine import preview_pipeline
        config = _build_preview_config(req)
        # Offload template/regex rendering off event loop (bd-w3z4h)
        result = await run_cpu_bound(
            preview_pipeline,
            config, req.sample_name,
            include_trace=req.include_trace,
        )
        return result
    except Exception as e:
        logger.warning("[DUMMY-EPG] Preview failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/preview/batch")
async def preview_epg_batch(req: BatchPreviewRequest):
    """Test the EPG pipeline with multiple sample names. Zero-write and
    zero-read — the request carries everything the engine needs."""
    logger.debug("[DUMMY-EPG] POST /preview/batch count=%s", len(req.sample_names))
    try:
        from dummy_epg_engine import preview_pipeline_batch
        config = _build_preview_config(req)
        # Offload batch template/regex rendering off event loop (bd-w3z4h)
        results = await run_cpu_bound(
            preview_pipeline_batch, config, req.sample_names[:100],  # Cap at 100
        )
        _annotate_event_sync_start_validity(results, config)
        return results
    except Exception as e:
        logger.warning("[DUMMY-EPG] Batch preview failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


def _annotate_event_sync_start_validity(results: list, config: dict) -> None:
    """Stamp ``event_sync_start_valid`` onto each batch-preview result.

    bead hirm6: the Event Sync Test Patterns panel drives this endpoint, and
    its 'Parsed' verdict must mean "the Event Sync matcher would actually
    build a start time from these groups" — group PRESENCE alone lets
    'A vs B @ 45 Jul 06:00 PM ET' (day 45) or a garbage month show as
    parsed while being unmatchable at preview/run time. The flag delegates
    to ``services.event_sync_matcher.groups_would_build_start`` (the exact
    ``_groups_have_complete_time`` + ``_build_start`` semantics — valid
    month name, hour <= 23, a real calendar date, never guessed).

    Read-only annotation of an already-computed response — the endpoint
    stays zero-write. One shared ``now`` anchors year inference across the
    batch so rows are judged consistently. Imported lazily: the matcher
    module (rapidfuzz) stays off this router's import path for every other
    endpoint.
    """
    from datetime import datetime

    import pytz

    from services.event_sync_matcher import (
        DEFAULT_EVENT_TIMEZONE,
        groups_would_build_start,
    )

    event_timezone = config.get("event_timezone") or DEFAULT_EVENT_TIMEZONE
    try:
        now = datetime.now(pytz.timezone(event_timezone))
    except pytz.UnknownTimeZoneError:
        event_timezone = DEFAULT_EVENT_TIMEZONE
        now = datetime.now(pytz.timezone(event_timezone))
    for result in results:
        result["event_sync_start_valid"] = (
            bool(result.get("matched"))
            and groups_would_build_start(
                result.get("groups"),
                event_timezone=event_timezone,
                now=now,
            )
        )


def _build_preview_config(req) -> dict:
    """Build a config dict from a preview request for the engine."""
    config = {
        "substitution_pairs": [p.model_dump() for p in req.substitution_pairs],
        "title_pattern": req.title_pattern,
        "time_pattern": req.time_pattern,
        "date_pattern": req.date_pattern,
        "title_template": req.title_template,
        "description_template": req.description_template,
        "upcoming_title_template": req.upcoming_title_template,
        "upcoming_description_template": req.upcoming_description_template,
        "ended_title_template": req.ended_title_template,
        "ended_description_template": req.ended_description_template,
        "fallback_title_template": req.fallback_title_template,
        "fallback_description_template": req.fallback_description_template,
        "event_timezone": req.event_timezone,
        "output_timezone": req.output_timezone,
        "program_duration": req.program_duration,
        "channel_logo_url_template": req.channel_logo_url_template,
        "program_poster_url_template": req.program_poster_url_template,
    }
    if req.pattern_variants:
        config["pattern_variants"] = [v.model_dump() for v in req.pattern_variants]
    return config


def _resolve_group_assignments(channel_group_ids: list, channel_map: dict) -> list:
    from services.epg_programmes import _resolve_group_assignments as resolve_groups
    return resolve_groups(channel_group_ids, channel_map)


async def _fetch_all_channels() -> dict:
    from services.epg_programmes import _fetch_all_channels as fetch_channels
    return await fetch_channels(get_client())


@router.get("/xmltv")
async def get_xmltv_all(db: Session = Depends(get_session)):
    """Combined XMLTV output for all enabled profiles."""
    logger.debug("[DUMMY-EPG] GET /xmltv")
    try:
        # Check cache first
        cached = cache.get("dummy_epg_xmltv_all", ttl=300)
        if cached is not None:
            logger.debug("[DUMMY-EPG] Returning cached XMLTV (all profiles)")
            return Response(content=cached, media_type="application/xml")

        from models import DummyEPGProfile
        from dummy_epg_engine import generate_xmltv

        profiles = db.query(DummyEPGProfile).filter(
            DummyEPGProfile.enabled == True  # noqa: E712
        ).all()

        channel_map = await _fetch_all_channels()

        # Build profile data — resolve group IDs to channel assignments
        profile_data = []
        for profile in profiles:
            p_dict = profile.to_dict()
            p_dict["channel_assignments"] = _resolve_group_assignments(
                p_dict.get("channel_group_ids", []), channel_map
            )
            p_dict["channel_map"] = channel_map
            profile_data.append(p_dict)

        from services.epg_programmes import can_cache, prepare_profiles
        profile_data, coverage = await prepare_profiles(profile_data, channel_map, get_client())
        # Offload XML generation off event loop (bd-w3z4h)
        xml_string = await run_cpu_bound(generate_xmltv, profile_data, channel_map)
        if can_cache(coverage):
            cache.set("dummy_epg_xmltv_all", xml_string)

        logger.info("[DUMMY-EPG] Generated XMLTV for %s enabled profiles", len(profiles))
        return Response(content=xml_string, media_type="application/xml")
    except Exception as e:
        logger.warning("[DUMMY-EPG] Failed to generate XMLTV: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("/xmltv/{profile_id}")
async def get_xmltv_profile(profile_id: int, db: Session = Depends(get_session)):
    """XMLTV output for a single profile."""
    logger.debug("[DUMMY-EPG] GET /xmltv/%s", profile_id)
    try:
        cache_key = f"dummy_epg_xmltv_{profile_id}"
        cached = cache.get(cache_key, ttl=300)
        if cached is not None:
            logger.debug("[DUMMY-EPG] Returning cached XMLTV for profile %s", profile_id)
            return Response(content=cached, media_type="application/xml")

        from models import DummyEPGProfile
        from dummy_epg_engine import generate_xmltv

        profile = db.query(DummyEPGProfile).filter(
            DummyEPGProfile.id == profile_id
        ).first()
        if not profile:
            logger.warning("[DUMMY-EPG] Profile not found: %s", profile_id)
            raise HTTPException(status_code=404, detail="Profile not found")

        channel_map = await _fetch_all_channels()

        p_dict = profile.to_dict()
        p_dict["channel_assignments"] = _resolve_group_assignments(
            p_dict.get("channel_group_ids", []), channel_map
        )
        p_dict["channel_map"] = channel_map
        profile_data = [p_dict]

        from services.epg_programmes import can_cache, prepare_profiles
        profile_data, coverage = await prepare_profiles(profile_data, channel_map, get_client())
        # Offload XML generation off event loop (bd-w3z4h)
        xml_string = await run_cpu_bound(generate_xmltv, profile_data, channel_map)
        if can_cache(coverage):
            cache.set(cache_key, xml_string)

        logger.info("[DUMMY-EPG] Generated XMLTV for profile %s", profile_id)
        return Response(content=xml_string, media_type="application/xml")
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("[DUMMY-EPG] Failed to generate XMLTV for profile %s: %s", profile_id, e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


class GenerateProfilesRequest(BaseModel):
    profile_ids: list[int] = Field(default_factory=list, max_length=499)


@router.post("/generate")
async def force_regenerate(
    body: GenerateProfilesRequest | None = None,
    db: Session = Depends(get_session),
):
    """Regenerate the combined guide and requested profile caches."""
    try:
        from models import DummyEPGProfile
        from dummy_epg_engine import generate_xmltv
        from services.epg_programmes import can_cache, prepare_profiles

        cache.invalidate_prefix("dummy_epg_xmltv")
        profiles = db.query(DummyEPGProfile).filter(
            DummyEPGProfile.enabled == True  # noqa: E712
        ).all()
        channel_map = await _fetch_all_channels()
        profile_data = []
        for profile in profiles:
            p_dict = profile.to_dict()
            p_dict["channel_assignments"] = _resolve_group_assignments(
                p_dict.get("channel_group_ids", []), channel_map
            )
            p_dict["channel_map"] = channel_map
            profile_data.append(p_dict)
        profile_data, coverage = await prepare_profiles(
            profile_data, channel_map, get_client()
        )
        xml_string = await run_cpu_bound(generate_xmltv, profile_data, channel_map)
        if can_cache(coverage):
            cache.set("dummy_epg_xmltv_all", xml_string)

        selected = set(body.profile_ids) if body is not None else {profile.id for profile in profiles}
        generated = 0
        for p_dict in profile_data:
            if p_dict["id"] not in selected:
                continue
            per_xml = await run_cpu_bound(generate_xmltv, [p_dict], channel_map)
            if can_cache(coverage):
                cache.set(f"dummy_epg_xmltv_{p_dict['id']}", per_xml)
            generated += 1
        if can_cache(coverage):
            return {"status": "ok", "profiles_generated": generated}
        status = "error" if any(source.get("status") in {"error", "stale"} for source in coverage.get("sources", [])) else "pending"
        return {"status": status, "profiles_generated": generated, "coverage": coverage}
    except Exception:
        logger.exception("[DUMMY-EPG] Failed to regenerate XMLTV")
        raise HTTPException(status_code=500, detail="Could not regenerate the guide") from None
    finally:
        db.close()


@router.get("/profiles/export/yaml")
async def export_dummy_epg_profiles_yaml():
    """Export all Dummy EPG profiles as YAML.

    Includes portable channel_group_names alongside numeric IDs
    so profiles can be shared between ECM instances.
    """
    import yaml
    from datetime import datetime as dt
    from fastapi.responses import PlainTextResponse
    from models import DummyEPGProfile

    logger.debug("[DUMMY-EPG] GET /profiles/export/yaml")
    db = get_session()
    try:
        profiles = db.query(DummyEPGProfile).order_by(DummyEPGProfile.name).all()

        # Build group id→name lookup for portable export
        client = get_client()
        group_id_to_name = {}
        try:
            groups = await client.get_channel_groups()
            group_id_to_name = {g["id"]: g["name"] for g in groups}
        except Exception as e:
            logger.warning("[DUMMY-EPG] Could not fetch channel groups for YAML export: %s", e)

        # Fields to exclude from export (runtime/internal only)
        exclude_keys = {"id", "created_at", "updated_at", "last_generated_at"}

        export_profiles = []
        for profile in profiles:
            d = profile.to_dict()
            # Remove runtime fields
            profile_dict = {k: v for k, v in d.items() if k not in exclude_keys}
            # Add portable group names
            group_ids = d.get("channel_group_ids", []) or []
            profile_dict["channel_group_names"] = [
                group_id_to_name[gid] for gid in group_ids if gid in group_id_to_name
            ]
            export_profiles.append(profile_dict)

        export_data = {
            "version": 1,
            "exported_at": dt.utcnow().isoformat() + "Z",
            "profiles": export_profiles,
        }

        yaml_str = yaml.dump(export_data, default_flow_style=False, sort_keys=False, allow_unicode=True)
        return PlainTextResponse(
            content=yaml_str,
            media_type="text/yaml",
            headers={"Content-Disposition": "attachment; filename=dummy_epg_profiles.yaml"},
        )
    except Exception as e:
        logger.warning("[DUMMY-EPG] Failed to export profiles as YAML: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/profiles/import/yaml")
async def import_dummy_epg_profiles_yaml(request: ImportYAMLRequest):
    """Import Dummy EPG profiles from YAML.

    Resolves portable channel_group_names to local channel_group_ids.
    """
    import yaml

    logger.debug("[DUMMY-EPG] POST /profiles/import/yaml - overwrite=%s", request.overwrite)
    try:
        # Parse YAML
        try:
            data = yaml.safe_load(request.yaml_content)
        except yaml.YAMLError as e:
            raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}")

        # Accept both {"profiles": [...]} and a bare list
        if isinstance(data, list):
            data = {"profiles": data}

        if not data or "profiles" not in data:
            raise HTTPException(status_code=400, detail="YAML must contain a 'profiles' array or be a list of profiles")

        # Build group name→id lookup (case-insensitive)
        client = get_client()
        group_name_to_id = {}
        try:
            groups = await client.get_channel_groups()
            group_name_to_id = {g["name"].lower(): g["id"] for g in groups}
        except Exception as e:
            logger.warning("[DUMMY-EPG] Could not fetch channel groups for YAML import: %s", e)

        from models import DummyEPGProfile

        db = get_session()
        try:
            imported = []
            errors = []
            snapshot = {}

            for i, profile_data in enumerate(data["profiles"]):
                profile_name = profile_data.get("name", f"Profile {i}")

                # Resolve channel_group_names → channel_group_ids
                if profile_data.get("channel_group_names") and not profile_data.get("channel_group_ids"):
                    resolved_ids = []
                    for gname in profile_data["channel_group_names"]:
                        gid = group_name_to_id.get(gname.lower())
                        if gid:
                            resolved_ids.append(gid)
                    if resolved_ids:
                        profile_data["channel_group_ids"] = resolved_ids

                # Strip portable-only fields
                profile_data.pop("channel_group_names", None)

                # Validate: name is required
                if not profile_data.get("name"):
                    errors.append({
                        "profile_index": i,
                        "profile_name": profile_name,
                        "errors": ["Profile name is required"],
                    })
                    continue

                # Check for duplicate
                existing = db.query(DummyEPGProfile).filter(
                    DummyEPGProfile.name == profile_data["name"]
                ).first()

                if existing and not request.overwrite:
                    errors.append({
                        "profile_index": i,
                        "profile_name": profile_name,
                        "errors": ["Profile with this name already exists"],
                    })
                    continue

                try:
                    validated = ProfileUpdateRequest.model_validate(profile_data)
                    fields = validated.model_dump(exclude_unset=True)
                    _lint_dummy_epg_profile_request(validated)
                    candidate = DummyEPGProfile(name=profile_data["name"])
                    if existing:
                        _apply_profile_fields(candidate, existing.to_dict())
                    _apply_profile_fields(candidate, fields)
                    if not existing or _source_changes(existing, fields):
                        await _configure_sources(candidate, fields, snapshot=snapshot)
                    fields["epg_source_ids"] = candidate.get_epg_source_ids()
                    fields["channel_mappings"] = candidate.get_channel_mappings()
                except (ValueError, HTTPException):
                    errors.append({
                        "profile_index": i,
                        "profile_name": profile_name,
                        "errors": ["Profile fields or programme source mappings are invalid or unavailable"],
                    })
                    continue

                if existing:
                    _apply_profile_fields(existing, fields)
                    imported.append({"name": existing.name, "action": "updated"})
                else:
                    _apply_profile_fields(candidate, fields)
                    db.add(candidate)
                    imported.append({"name": candidate.name, "action": "created"})

            db.commit()
            cache.invalidate_prefix("dummy_epg_xmltv")

            logger.info("[DUMMY-EPG] Imported %s profiles from YAML", len(imported))
            return {
                "success": True,
                "imported": imported,
                "errors": errors,
            }
        finally:
            db.close()
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("[DUMMY-EPG] Failed to import profiles from YAML: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


def _apply_profile_fields(profile, data: dict):
    """Apply profile fields from a dict to a DummyEPGProfile model instance."""
    simple_fields = [
        "name", "enabled", "name_source", "stream_index",
        "title_pattern", "time_pattern", "date_pattern",
        "title_template", "description_template",
        "upcoming_title_template", "upcoming_description_template",
        "ended_title_template", "ended_description_template",
        "fallback_title_template", "fallback_description_template",
        "event_timezone", "output_timezone", "program_duration",
        "categories", "channel_logo_url_template", "program_poster_url_template",
        "tvg_id_template", "include_date_tag", "include_live_tag", "include_new_tag",
        "pattern_builder_examples",
    ]
    for field in simple_fields:
        if field in data:
            setattr(profile, field, data[field])

    if "substitution_pairs" in data and data["substitution_pairs"]:
        profile.set_substitution_pairs(data["substitution_pairs"])
    if "pattern_variants" in data and data["pattern_variants"]:
        profile.set_pattern_variants(data["pattern_variants"])
    if "channel_group_ids" in data and data["channel_group_ids"] is not None:
        profile.set_channel_group_ids(data["channel_group_ids"])
    if "epg_source_ids" in data:
        profile.set_epg_source_ids(data["epg_source_ids"] or [])
    if "channel_mappings" in data:
        profile.set_channel_mappings(data["channel_mappings"] or [])


# =============================================================================
# Lint findings (bd-eio04.7) — read-only view of the startup migration scan.
# =============================================================================


@router.get("/lint-findings")
async def get_dummy_epg_lint_findings(db: Session = Depends(get_session)):
    """Return the cached lint findings for dummy-EPG profiles.

    See ``routers/normalization.py::get_normalization_lint_findings`` for
    semantics. Findings are scoped to ``rule_type='dummy_epg'``.
    """
    logger.debug("[DUMMY-EPG] GET /lint-findings")
    try:
        from models import RuleLintFinding
        from tasks.rule_lint_scan import RULE_TYPE_DUMMY_EPG

        findings = db.query(RuleLintFinding).filter(
            RuleLintFinding.rule_type == RULE_TYPE_DUMMY_EPG
        ).order_by(RuleLintFinding.rule_id, RuleLintFinding.id).all()
        return {"findings": [f.to_dict() for f in findings]}
    except Exception as e:
        logger.warning("[DUMMY-EPG] Failed to get lint findings: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
