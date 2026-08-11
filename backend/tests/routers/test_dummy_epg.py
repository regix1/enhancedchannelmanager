"""
Unit tests for Dummy EPG router endpoints.

Tests: Profile CRUD (with group-based channel assignment), preview, and XMLTV output.
Mocks: _fetch_all_channels, get_client, preview_pipeline, generate_xmltv, cache.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from models import DummyEPGProfile


def _create_profile(session, **overrides):
    """Helper to create a DummyEPGProfile with sensible defaults."""
    defaults = {
        "name": "Test Profile",
        "enabled": True,
        "name_source": "channel",
        "stream_index": 1,
        "title_pattern": r"(?P<title>.+)",
        "time_pattern": None,
        "date_pattern": None,
        "title_template": "{title}",
        "description_template": "Showing {title}",
        "event_timezone": "UTC",
        "program_duration": 180,
        "tvg_id_template": "ecm-{channel_number}",
        "include_date_tag": False,
        "include_live_tag": False,
        "include_new_tag": False,
    }
    defaults.update(overrides)
    profile = DummyEPGProfile(**defaults)
    session.add(profile)
    session.commit()
    session.refresh(profile)
    return profile


# =============================================================================
# Profile CRUD
# =============================================================================


class TestListProfiles:
    """Tests for GET /api/dummy-epg/profiles."""

    @pytest.mark.asyncio
    async def test_returns_empty_list(self, async_client):
        """Returns empty list when no profiles exist."""
        response = await async_client.get("/api/dummy-epg/profiles")
        assert response.status_code == 200
        assert response.json() == []

    @pytest.mark.asyncio
    async def test_returns_profiles_with_group_count(self, async_client, test_session):
        """Returns all profiles with group_count."""
        profile = _create_profile(test_session, name="Sports Profile")
        profile.set_channel_group_ids([5, 10])
        test_session.commit()

        response = await async_client.get("/api/dummy-epg/profiles")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["name"] == "Sports Profile"
        assert data[0]["group_count"] == 2
        assert data[0]["channel_group_ids"] == [5, 10]

    @pytest.mark.asyncio
    async def test_returns_multiple_profiles(self, async_client, test_session):
        """Returns all profiles."""
        _create_profile(test_session, name="Profile A")
        _create_profile(test_session, name="Profile B")

        response = await async_client.get("/api/dummy-epg/profiles")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        names = {p["name"] for p in data}
        assert names == {"Profile A", "Profile B"}


class TestCreateProfile:
    """Tests for POST /api/dummy-epg/profiles."""

    @pytest.mark.asyncio
    async def test_creates_profile(self, async_client):
        """Creates a new profile with all fields."""
        with patch("routers.dummy_epg.cache"):
            response = await async_client.post("/api/dummy-epg/profiles", json={
                "name": "New Profile",
                "enabled": True,
                "name_source": "stream",
                "stream_index": 2,
                "title_pattern": r"(?P<title>.+)",
                "title_template": "{title}",
                "description_template": "Showing {title}",
                "event_timezone": "UTC",
                "program_duration": 120,
                "tvg_id_template": "ecm-{channel_number}",
                "include_live_tag": True,
            })

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "New Profile"
        assert data["name_source"] == "stream"
        assert data["stream_index"] == 2
        assert data["program_duration"] == 120
        assert data["include_live_tag"] is True
        assert "id" in data

    @pytest.mark.asyncio
    async def test_creates_profile_with_substitution_pairs(self, async_client):
        """Creates a profile with substitution pairs."""
        with patch("routers.dummy_epg.cache"):
            response = await async_client.post("/api/dummy-epg/profiles", json={
                "name": "Subs Profile",
                "substitution_pairs": [
                    {"find": "HD", "replace": "", "is_regex": False, "enabled": True},
                    {"find": r"\s+", "replace": " ", "is_regex": True, "enabled": True},
                ],
            })

        assert response.status_code == 200
        data = response.json()
        assert len(data["substitution_pairs"]) == 2
        assert data["substitution_pairs"][0]["find"] == "HD"

    @pytest.mark.asyncio
    async def test_stores_a_per_variant_program_duration(self, async_client):
        """A variant may carry its own duration so one sport can run longer
        than the profile default."""
        with patch("routers.dummy_epg.cache"):
            response = await async_client.post("/api/dummy-epg/profiles", json={
                "name": "Per Variant Duration",
                "program_duration": 180,
                "pattern_variants": [
                    {
                        "name": "Baseball",
                        "title_pattern": r"(?P<title>.+)",
                        "program_duration": 240,
                    },
                ],
            })

        assert response.status_code == 200
        data = response.json()
        assert data["program_duration"] == 180
        assert data["pattern_variants"][0]["program_duration"] == 240

    @pytest.mark.asyncio
    async def test_rejects_a_per_variant_duration_outside_the_range(self, async_client):
        """5000 minutes is past the 1440 ceiling the profile field uses."""
        with patch("routers.dummy_epg.cache"):
            response = await async_client.post("/api/dummy-epg/profiles", json={
                "name": "Too Long",
                "pattern_variants": [
                    {
                        "name": "Baseball",
                        "title_pattern": r"(?P<title>.+)",
                        "program_duration": 5000,
                    },
                ],
            })

        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_accepts_a_per_variant_duration_of_zero(self, async_client):
        """Zero is a value the engine honours, so the floor is 0 not 1."""
        with patch("routers.dummy_epg.cache"):
            response = await async_client.post("/api/dummy-epg/profiles", json={
                "name": "Zero Duration",
                "pattern_variants": [
                    {
                        "name": "Baseball",
                        "title_pattern": r"(?P<title>.+)",
                        "program_duration": 0,
                    },
                ],
            })

        assert response.status_code == 200
        data = response.json()
        assert data["pattern_variants"][0]["program_duration"] == 0

    @pytest.mark.asyncio
    async def test_leaves_a_variant_duration_unset_when_not_given(self, async_client):
        """Absent means the profile's own duration applies."""
        with patch("routers.dummy_epg.cache"):
            response = await async_client.post("/api/dummy-epg/profiles", json={
                "name": "Profile Duration Only",
                "program_duration": 180,
                "pattern_variants": [
                    {"name": "Baseball", "title_pattern": r"(?P<title>.+)"},
                ],
            })

        assert response.status_code == 200
        data = response.json()
        assert data["pattern_variants"][0]["program_duration"] is None

    @pytest.mark.asyncio
    async def test_creates_profile_with_channel_group_ids(self, async_client):
        """Creates a profile with channel_group_ids."""
        with patch("routers.dummy_epg.cache"):
            response = await async_client.post("/api/dummy-epg/profiles", json={
                "name": "Groups Profile",
                "channel_group_ids": [5, 10, 15],
            })

        assert response.status_code == 200
        data = response.json()
        assert data["channel_group_ids"] == [5, 10, 15]

    @pytest.mark.asyncio
    async def test_rejects_duplicate_name(self, async_client, test_session):
        """Returns 409 when name already exists."""
        _create_profile(test_session, name="Existing")

        response = await async_client.post("/api/dummy-epg/profiles", json={
            "name": "Existing",
        })
        assert response.status_code == 409
        assert "already exists" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_creates_with_defaults(self, async_client):
        """Creates a profile with only required name field."""
        with patch("routers.dummy_epg.cache"):
            response = await async_client.post("/api/dummy-epg/profiles", json={
                "name": "Minimal Profile",
            })

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Minimal Profile"
        assert data["enabled"] is True
        assert data["name_source"] == "channel"
        assert data["stream_index"] == 1
        assert data["program_duration"] == 180
        assert data["event_timezone"] == "US/Eastern"
        assert data["channel_group_ids"] == []


class TestGetProfile:
    """Tests for GET /api/dummy-epg/profiles/{profile_id}."""

    @pytest.mark.asyncio
    async def test_returns_profile_with_group_ids(self, async_client, test_session):
        """Returns profile including channel_group_ids."""
        profile = _create_profile(test_session, name="Detail Profile")
        profile.set_channel_group_ids([5, 10])
        test_session.commit()

        response = await async_client.get(f"/api/dummy-epg/profiles/{profile.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Detail Profile"
        assert data["channel_group_ids"] == [5, 10]

    @pytest.mark.asyncio
    async def test_returns_404_for_nonexistent(self, async_client):
        """Returns 404 when profile doesn't exist."""
        response = await async_client.get("/api/dummy-epg/profiles/99999")
        assert response.status_code == 404


class TestUpdateProfile:
    """Tests for PATCH /api/dummy-epg/profiles/{profile_id}."""

    @pytest.mark.asyncio
    async def test_updates_name(self, async_client, test_session):
        """Updates the profile name."""
        profile = _create_profile(test_session, name="Old Name")

        with patch("routers.dummy_epg.cache"):
            response = await async_client.patch(
                f"/api/dummy-epg/profiles/{profile.id}",
                json={"name": "New Name"},
            )

        assert response.status_code == 200
        assert response.json()["name"] == "New Name"

    @pytest.mark.asyncio
    async def test_updates_multiple_fields(self, async_client, test_session):
        """Updates multiple fields at once."""
        profile = _create_profile(test_session)

        with patch("routers.dummy_epg.cache"):
            response = await async_client.patch(
                f"/api/dummy-epg/profiles/{profile.id}",
                json={
                    "enabled": False,
                    "program_duration": 60,
                    "include_live_tag": True,
                },
            )

        assert response.status_code == 200
        data = response.json()
        assert data["enabled"] is False
        assert data["program_duration"] == 60
        assert data["include_live_tag"] is True

    @pytest.mark.asyncio
    async def test_updates_substitution_pairs(self, async_client, test_session):
        """Updates substitution pairs."""
        profile = _create_profile(test_session)

        with patch("routers.dummy_epg.cache"):
            response = await async_client.patch(
                f"/api/dummy-epg/profiles/{profile.id}",
                json={
                    "substitution_pairs": [
                        {"find": "FOO", "replace": "BAR", "is_regex": False, "enabled": True},
                    ],
                },
            )

        assert response.status_code == 200
        data = response.json()
        assert len(data["substitution_pairs"]) == 1
        assert data["substitution_pairs"][0]["find"] == "FOO"

    @pytest.mark.asyncio
    async def test_updates_channel_group_ids(self, async_client, test_session):
        """Updates channel_group_ids."""
        profile = _create_profile(test_session)
        profile.set_channel_group_ids([5])
        test_session.commit()

        with patch("routers.dummy_epg.cache"):
            response = await async_client.patch(
                f"/api/dummy-epg/profiles/{profile.id}",
                json={"channel_group_ids": [10, 20]},
            )

        assert response.status_code == 200
        assert response.json()["channel_group_ids"] == [10, 20]

    @pytest.mark.asyncio
    async def test_rejects_duplicate_name(self, async_client, test_session):
        """Returns 409 when renaming to an existing name."""
        _create_profile(test_session, name="Taken Name")
        profile = _create_profile(test_session, name="My Profile")

        response = await async_client.patch(
            f"/api/dummy-epg/profiles/{profile.id}",
            json={"name": "Taken Name"},
        )
        assert response.status_code == 409

    @pytest.mark.asyncio
    async def test_returns_404_for_nonexistent(self, async_client):
        """Returns 404 when profile doesn't exist."""
        response = await async_client.patch(
            "/api/dummy-epg/profiles/99999",
            json={"name": "Ghost"},
        )
        assert response.status_code == 404


class TestDeleteProfile:
    """Tests for DELETE /api/dummy-epg/profiles/{profile_id}."""

    @pytest.mark.asyncio
    async def test_deletes_profile(self, async_client, test_session):
        """Deletes a profile successfully."""
        profile = _create_profile(test_session, name="Delete Me")
        profile_id = profile.id

        with patch("routers.dummy_epg.cache"):
            response = await async_client.delete(f"/api/dummy-epg/profiles/{profile_id}")

        assert response.status_code == 204

        # Verify deleted from DB
        result = test_session.query(DummyEPGProfile).filter(
            DummyEPGProfile.id == profile_id
        ).first()
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_404_for_nonexistent(self, async_client):
        """Returns 404 when profile doesn't exist."""
        response = await async_client.delete("/api/dummy-epg/profiles/99999")
        assert response.status_code == 404


# =============================================================================
# Preview
# =============================================================================


class TestPreview:
    """Tests for POST /api/dummy-epg/preview."""

    @pytest.mark.asyncio
    async def test_preview_with_match(self, async_client):
        """Preview returns matched groups and rendered templates."""
        mock_result = {
            "matched": True,
            "groups": {"title": "Wolves vs Hawks"},
            "rendered_title": "Wolves vs Hawks",
            "rendered_description": "Showing Wolves vs Hawks",
        }

        with patch("dummy_epg_engine.preview_pipeline", return_value=mock_result):
            response = await async_client.post("/api/dummy-epg/preview", json={
                "sample_name": "Wolves vs Hawks HD",
                "title_pattern": r"(?P<title>.+?)\\s*HD",
                "title_template": "{title}",
                "description_template": "Showing {title}",
                "event_timezone": "UTC",
                "program_duration": 180,
            })

        assert response.status_code == 200
        data = response.json()
        assert data["matched"] is True
        assert data["groups"]["title"] == "Wolves vs Hawks"

    @pytest.mark.asyncio
    async def test_preview_no_match_fallback(self, async_client):
        """Preview returns fallback when pattern doesn't match."""
        mock_result = {
            "matched": False,
            "groups": {},
            "rendered_title": "Fallback Title",
            "rendered_description": "Fallback Desc",
        }

        with patch("dummy_epg_engine.preview_pipeline", return_value=mock_result):
            response = await async_client.post("/api/dummy-epg/preview", json={
                "sample_name": "Random Channel Name",
                "title_pattern": r"(?P<title>NEVER_MATCHES)",
                "fallback_title_template": "Fallback Title",
                "fallback_description_template": "Fallback Desc",
                "event_timezone": "UTC",
                "program_duration": 180,
            })

        assert response.status_code == 200
        data = response.json()
        assert data["matched"] is False
        assert data["rendered_title"] == "Fallback Title"

    @pytest.mark.asyncio
    async def test_preview_with_substitution_pairs(self, async_client):
        """Preview processes substitution pairs before pattern matching."""
        mock_result = {
            "matched": True,
            "groups": {"title": "Wolves vs Hawks"},
            "rendered_title": "Wolves vs Hawks",
        }

        with patch("dummy_epg_engine.preview_pipeline", return_value=mock_result):
            response = await async_client.post("/api/dummy-epg/preview", json={
                "sample_name": "Wolves vs Hawks HD 1080p",
                "substitution_pairs": [
                    {"find": " HD 1080p", "replace": "", "is_regex": False, "enabled": True},
                ],
                "title_pattern": r"(?P<title>.+)",
                "title_template": "{title}",
                "event_timezone": "UTC",
                "program_duration": 180,
            })

        assert response.status_code == 200
        assert response.json()["matched"] is True

    @pytest.mark.asyncio
    async def test_preview_engine_error(self, async_client):
        """Returns 500 when preview engine raises."""
        with patch("dummy_epg_engine.preview_pipeline", side_effect=Exception("Engine error")):
            response = await async_client.post("/api/dummy-epg/preview", json={
                "sample_name": "Test",
                "event_timezone": "UTC",
                "program_duration": 180,
            })

        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_preview_with_inline_lookup(self, async_client):
        """Inline lookup table on the request resolves through the template engine."""
        response = await async_client.post("/api/dummy-epg/preview", json={
            "sample_name": "ESPN",
            "title_pattern": r"(?P<callsign>.+)",
            "title_template": "{callsign|lookup:stations}",
            "inline_lookups": {
                "stations": {"ESPN": "Entertainment Sports Programming Network"},
            },
            "event_timezone": "UTC",
            "program_duration": 180,
        })

        assert response.status_code == 200, response.json()
        body = response.json()
        assert body["matched"] is True
        assert body["rendered"]["title"] == "Entertainment Sports Programming Network"

    @pytest.mark.asyncio
    async def test_preview_with_global_lookup_id(self, async_client):
        """A lookup table created via /api/lookup-tables can be referenced by ID."""
        created = (await async_client.post(
            "/api/lookup-tables",
            json={"name": "countries", "entries": {"US": "United States"}},
        )).json()

        response = await async_client.post("/api/dummy-epg/preview", json={
            "sample_name": "US",
            "title_pattern": r"(?P<code>.+)",
            "title_template": "{code|lookup:countries}",
            "global_lookup_ids": [created["id"]],
            "event_timezone": "UTC",
            "program_duration": 180,
        })

        assert response.status_code == 200, response.json()
        assert response.json()["rendered"]["title"] == "United States"

    @pytest.mark.asyncio
    async def test_preview_inline_overrides_global_lookup(self, async_client):
        """Inline table with the same name as a global one takes precedence."""
        created = (await async_client.post(
            "/api/lookup-tables",
            json={"name": "codes", "entries": {"A": "from-global"}},
        )).json()

        response = await async_client.post("/api/dummy-epg/preview", json={
            "sample_name": "A",
            "title_pattern": r"(?P<k>.+)",
            "title_template": "{k|lookup:codes}",
            "global_lookup_ids": [created["id"]],
            "inline_lookups": {"codes": {"A": "from-inline"}},
            "event_timezone": "UTC",
            "program_duration": 180,
        })

        assert response.json()["rendered"]["title"] == "from-inline"

    @pytest.mark.asyncio
    async def test_preview_supports_pipe_transforms(self, async_client):
        """New template engine's pipe transforms work through the preview endpoint."""
        response = await async_client.post("/api/dummy-epg/preview", json={
            "sample_name": "nfl",
            "title_pattern": r"(?P<league>.+)",
            "title_template": "{league|uppercase}",
            "event_timezone": "UTC",
            "program_duration": 180,
        })

        assert response.status_code == 200
        assert response.json()["rendered"]["title"] == "NFL"

    @pytest.mark.asyncio
    async def test_preview_supports_conditionals(self, async_client):
        """{if:group}...{/if} and {if:group=value}...{/if} work end-to-end."""
        response = await async_client.post("/api/dummy-epg/preview", json={
            "sample_name": "ESPN2 HD",
            "title_pattern": r"(?P<callsign>\S+)\s+(?P<quality>HD|SD)?",
            "title_template": "{callsign}{if:quality=HD} [HD]{/if}",
            "event_timezone": "UTC",
            "program_duration": 180,
        })

        assert response.status_code == 200
        assert response.json()["rendered"]["title"] == "ESPN2 [HD]"

    @pytest.mark.asyncio
    async def test_preview_include_trace_returns_step_by_step(self, async_client):
        """include_trace=True returns a per-field trace describing pipes,
        conditional branches, and lookup resolutions."""
        response = await async_client.post("/api/dummy-epg/preview", json={
            "sample_name": "nfl-chiefs",
            "title_pattern": r"(?P<league>\w+)-(?P<team>\w+)",
            "title_template": "{league|uppercase}: {if:team}{team|titlecase}{/if}",
            "event_timezone": "UTC",
            "program_duration": 180,
            "include_trace": True,
        })

        assert response.status_code == 200, response.json()
        body = response.json()
        assert body["rendered"]["title"] == "NFL: Chiefs"
        assert "traces" in body
        title_trace = body["traces"]["title_template"]

        placeholder = next(t for t in title_trace if t["kind"] == "placeholder")
        assert placeholder["group_name"] == "league"
        assert placeholder["initial_value"] == "nfl"
        assert placeholder["final_value"] == "NFL"
        assert placeholder["pipes"][0]["transform"] == "uppercase"

        conditional = next(t for t in title_trace if t["kind"] == "conditional")
        assert conditional["taken"] is True
        assert conditional["kind_detail"] == "truthy"

    @pytest.mark.asyncio
    async def test_preview_trace_records_lookup_miss(self, async_client):
        """Trace's matched flag distinguishes lookup hits from misses."""
        response = await async_client.post("/api/dummy-epg/preview", json={
            "sample_name": "ZZ",
            "title_pattern": r"(?P<code>.+)",
            "title_template": "{code|lookup:countries}",
            "inline_lookups": {"countries": {"US": "United States"}},
            "event_timezone": "UTC",
            "program_duration": 180,
            "include_trace": True,
        })

        assert response.status_code == 200
        pipe = response.json()["traces"]["title_template"][0]["pipes"][0]
        assert pipe["transform"] == "lookup"
        assert pipe["source"] == "countries"
        assert pipe["matched"] is False


# =============================================================================
# Batch preview — matcher-level validity flag (bead hirm6)
# =============================================================================


class TestPreviewBatchEventSyncStartValid:
    """POST /api/dummy-epg/preview/batch annotates every result with
    ``event_sync_start_valid`` — True ONLY when the Event Sync matcher would
    actually build a start time from the captured groups (valid month name,
    hour <= 23 after am/pm, a real calendar date). Group-presence alone
    ('45 Jul' captured a day and a month) is NOT enough: the Test Patterns
    panel must never show 'Parsed' for a name the matcher would record as a
    parse failure. Runs the REAL extraction machinery end to end (no engine
    mock) and stays zero-write."""

    # The shipped 'slot-title-day-first-date' pattern set VERBATIM
    # (frontend/src/components/channelPipeline/eventSyncShippedPatterns.json)
    # — exactly what the panel sends for its default selection.
    _PATTERNS = {
        "title_pattern": (
            r"^(?:[^@:]{0,40}?(?<!\d)\d{2}\s*:\s*)?\s*(?P<title>.+?)\s*"
            r"(?:@\s*(?:\d{1,2}\s+[A-Za-z]{3,9}\s+\d{1,2}:\d{2}"
            r"|[A-Za-z]{3,9}\.?\s+\d{1,2}(?:\s*,?\s*\d{4})?\s+\d{1,2}:\d{2})"
            r".*)?$"
        ),
        "time_pattern": (
            r"(?P<hour>\d{1,2}):(?P<minute>\d{2})"
            r"(?:\s*(?P<ampm>[AaPp])\.?[Mm]?\.?)?\s*(?:E[SD]?T)?\s*$"
        ),
        "date_pattern": r"@\s*(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]{3,9})\s+\d{1,2}:\d{2}",
    }

    async def _batch(self, async_client, sample_names, patterns=None):
        response = await async_client.post("/api/dummy-epg/preview/batch", json={
            "sample_names": sample_names,
            # The matcher's own default zone: the request-model default
            # ("US/Eastern") is a legacy tzdata alias that not every
            # environment ships (this CI image resolves only canonical
            # names), and this suite runs the REAL engine unmocked.
            "event_timezone": "America/New_York",
            **(patterns or self._PATTERNS),
        })
        assert response.status_code == 200
        return response.json()

    @pytest.mark.asyncio
    async def test_valid_complete_date_time_is_flagged_true(self, async_client):
        results = await self._batch(
            async_client, ["A vs B @ 11 Jul 06:00 PM ET"]
        )
        assert results[0]["matched"] is True
        assert results[0]["event_sync_start_valid"] is True

    @pytest.mark.asyncio
    async def test_garbage_day_45_jul_is_flagged_false(self, async_client):
        """The PR #614 review case: all groups captured, but July 45 is not
        a real date — the matcher would never get a start time."""
        results = await self._batch(
            async_client, ["A vs B @ 45 Jul 06:00 PM ET"]
        )
        assert results[0]["matched"] is True
        groups = results[0]["groups"]
        assert groups["day"] == "45" and groups["month"] == "Jul"
        assert results[0]["event_sync_start_valid"] is False

    @pytest.mark.asyncio
    async def test_garbage_month_name_is_flagged_false(self, async_client):
        results = await self._batch(
            async_client, ["A vs B @ 11 Julx 06:00 PM ET"]
        )
        assert results[0]["event_sync_start_valid"] is False

    @pytest.mark.asyncio
    async def test_incomplete_title_only_match_is_flagged_false(
        self, async_client
    ):
        """The shipped pattern title-matches a dateless name; no date/time
        groups → the matcher never guesses a start."""
        results = await self._batch(async_client, ["Big Fight Night"])
        assert results[0]["matched"] is True
        assert results[0]["event_sync_start_valid"] is False

    @pytest.mark.asyncio
    async def test_no_match_is_flagged_false(self, async_client):
        results = await self._batch(
            async_client, ["no separator here"],
            patterns={
                "title_pattern":
                    r"^(?P<title>.+?)\s*@\s*\d{1,2}\s+[A-Za-z]{3,9}",
            },
        )
        assert results[0]["matched"] is False
        assert results[0]["event_sync_start_valid"] is False

    @pytest.mark.asyncio
    async def test_flag_is_per_row_across_the_batch(self, async_client):
        results = await self._batch(async_client, [
            "A vs B @ 11 Jul 06:00 PM ET",
            "A vs B @ 45 Jul 06:00 PM ET",
        ])
        assert [r["event_sync_start_valid"] for r in results] == [True, False]


# =============================================================================
# XMLTV Output
# =============================================================================


class TestGetXmltvAll:
    """Tests for GET /api/dummy-epg/xmltv."""

    @pytest.mark.asyncio
    async def test_returns_xml_content_type(self, async_client, test_session):
        """Returns response with application/xml content type."""
        # Profile is created so the endpoint has a row to render (side effect on test_session).
        _create_profile(test_session, name="XMLTV Test")
        xml_output = '<?xml version="1.0"?><tv></tv>'

        mock_cache = MagicMock()
        mock_cache.get.return_value = None
        mock_cache.set = MagicMock()

        with patch("routers.dummy_epg._fetch_all_channels", new_callable=AsyncMock, return_value={}), \
             patch("dummy_epg_engine.generate_xmltv", return_value=xml_output), \
             patch("routers.dummy_epg.cache", mock_cache):
            response = await async_client.get("/api/dummy-epg/xmltv")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/xml"
        assert "<?xml" in response.text

    @pytest.mark.asyncio
    async def test_returns_cached_response(self, async_client, test_session):
        """Returns cached XMLTV without regenerating."""
        cached_xml = '<?xml version="1.0"?><tv><cached/></tv>'
        mock_cache = MagicMock()
        mock_cache.get.return_value = cached_xml

        with patch("routers.dummy_epg.cache", mock_cache):
            response = await async_client.get("/api/dummy-epg/xmltv")

        assert response.status_code == 200
        assert "<cached/>" in response.text

    @pytest.mark.asyncio
    async def test_only_includes_enabled_profiles(self, async_client, test_session):
        """Only enabled profiles are included in XMLTV output."""
        _create_profile(test_session, name="Enabled", enabled=True)
        _create_profile(test_session, name="Disabled", enabled=False)

        xml_output = '<?xml version="1.0"?><tv></tv>'
        mock_cache = MagicMock()
        mock_cache.get.return_value = None
        mock_cache.set = MagicMock()

        with patch("routers.dummy_epg._fetch_all_channels", new_callable=AsyncMock, return_value={}), \
             patch("dummy_epg_engine.generate_xmltv", return_value=xml_output) as mock_gen, \
             patch("routers.dummy_epg.cache", mock_cache):
            response = await async_client.get("/api/dummy-epg/xmltv")

        assert response.status_code == 200
        # Verify generate_xmltv was called with only 1 profile (the enabled one)
        call_args = mock_gen.call_args
        profile_data = call_args[0][0]
        assert len(profile_data) == 1
        assert profile_data[0]["name"] == "Enabled"

    @pytest.mark.asyncio
    async def test_resolves_group_ids_to_assignments(self, async_client, test_session):
        """XMLTV endpoint resolves channel_group_ids into channel_assignments."""
        profile = _create_profile(test_session, name="Group Profile")
        profile.set_channel_group_ids([5])
        test_session.commit()

        channel_map = {
            1: {"id": 1, "name": "Sports One", "channel_group_id": 5},
            2: {"id": 2, "name": "Sports Plus", "channel_group_id": 5},
            3: {"id": 3, "name": "News 24", "channel_group_id": 10},
        }

        xml_output = '<?xml version="1.0"?><tv></tv>'
        mock_cache = MagicMock()
        mock_cache.get.return_value = None
        mock_cache.set = MagicMock()

        with patch("routers.dummy_epg._fetch_all_channels", new_callable=AsyncMock, return_value=channel_map), \
             patch("dummy_epg_engine.generate_xmltv", return_value=xml_output) as mock_gen, \
             patch("routers.dummy_epg.cache", mock_cache):
            response = await async_client.get("/api/dummy-epg/xmltv")

        assert response.status_code == 200
        call_args = mock_gen.call_args
        profile_data = call_args[0][0]
        assert len(profile_data) == 1
        assignments = profile_data[0]["channel_assignments"]
        assert len(assignments) == 2
        assigned_ids = {a["channel_id"] for a in assignments}
        assert assigned_ids == {1, 2}


class TestGetXmltvProfile:
    """Tests for GET /api/dummy-epg/xmltv/{profile_id}."""

    @pytest.mark.asyncio
    async def test_returns_xml_for_single_profile(self, async_client, test_session):
        """Returns XMLTV for a specific profile."""
        profile = _create_profile(test_session, name="Single Profile")
        xml_output = '<?xml version="1.0"?><tv><channel/></tv>'

        mock_cache = MagicMock()
        mock_cache.get.return_value = None
        mock_cache.set = MagicMock()

        with patch("routers.dummy_epg._fetch_all_channels", new_callable=AsyncMock, return_value={}), \
             patch("dummy_epg_engine.generate_xmltv", return_value=xml_output), \
             patch("routers.dummy_epg.cache", mock_cache):
            response = await async_client.get(f"/api/dummy-epg/xmltv/{profile.id}")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/xml"
        assert "<channel/>" in response.text

    @pytest.mark.asyncio
    async def test_returns_404_for_nonexistent_profile(self, async_client):
        """Returns 404 when profile doesn't exist."""
        mock_cache = MagicMock()
        mock_cache.get.return_value = None

        with patch("routers.dummy_epg.cache", mock_cache):
            response = await async_client.get("/api/dummy-epg/xmltv/99999")

        assert response.status_code == 404


# =============================================================================
# Force Regenerate
# =============================================================================


class TestForceRegenerate:
    """Tests for POST /api/dummy-epg/generate."""

    @pytest.mark.asyncio
    async def test_regenerates_all(self, async_client, test_session):
        """Force-regenerates XMLTV for all enabled profiles."""
        _create_profile(test_session, name="Regen Profile", enabled=True)
        xml_output = '<?xml version="1.0"?><tv></tv>'

        mock_cache = MagicMock()

        with patch("routers.dummy_epg._fetch_all_channels", new_callable=AsyncMock, return_value={}), \
             patch("dummy_epg_engine.generate_xmltv", return_value=xml_output), \
             patch("routers.dummy_epg.cache", mock_cache):
            response = await async_client.post("/api/dummy-epg/generate")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["profiles_generated"] == 1

        # Verify cache was invalidated and set
        mock_cache.invalidate_prefix.assert_called_with("dummy_epg_xmltv")
        assert mock_cache.set.call_count >= 1


# =============================================================================
# Auth posture — the XMLTV reads answer without credentials
# =============================================================================


class _AuthOn:
    """Auth settings that force the global middleware to demand a token."""
    require_auth = True
    setup_complete = True


class TestXmltvUnauthenticatedAccess:
    """Dispatcharr registers /api/dummy-epg/xmltv/<profile_id> as an XMLTV
    source and cannot send an ECM bearer token, so both XMLTV reads must
    answer with auth turned on. Everything else under /api/dummy-epg/ must
    still be rejected.
    """

    @pytest.mark.asyncio
    async def test_combined_xmltv_answers_without_credentials(
        self, async_client, test_session
    ):
        """GET /api/dummy-epg/xmltv returns the guide with auth enabled."""
        _create_profile(test_session, name="Open Read")
        cached_xml = '<?xml version="1.0"?><tv><channel id="ecm-1"/></tv>'
        mock_cache = MagicMock()
        mock_cache.get.return_value = cached_xml

        with patch("main.get_auth_settings", return_value=_AuthOn()), \
             patch("routers.dummy_epg.cache", mock_cache):
            response = await async_client.get("/api/dummy-epg/xmltv")

        assert response.status_code == 200, response.text
        assert 'id="ecm-1"' in response.text

    @pytest.mark.asyncio
    async def test_profile_xmltv_answers_without_credentials(
        self, async_client, test_session
    ):
        """The per-profile URL is the one pasted into Dispatcharr, so the
        variable {profile_id} segment must be exempt too.
        """
        profile = _create_profile(test_session, name="Open Profile Read")
        cached_xml = '<?xml version="1.0"?><tv><channel id="ecm-7"/></tv>'
        mock_cache = MagicMock()
        mock_cache.get.return_value = cached_xml

        with patch("main.get_auth_settings", return_value=_AuthOn()), \
             patch("routers.dummy_epg.cache", mock_cache):
            response = await async_client.get(f"/api/dummy-epg/xmltv/{profile.id}")

        assert response.status_code == 200, response.text
        assert 'id="ecm-7"' in response.text

    @pytest.mark.asyncio
    async def test_generate_still_requires_a_token(self, async_client):
        """POST /api/dummy-epg/generate shares the dummy-epg prefix but
        mutates cache state, so it must stay behind auth.
        """
        with patch("main.get_auth_settings", return_value=_AuthOn()):
            response = await async_client.post("/api/dummy-epg/generate")

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    @pytest.mark.asyncio
    async def test_profile_list_still_requires_a_token(self, async_client):
        """Opening the XMLTV reads must not open the rest of the router."""
        with patch("main.get_auth_settings", return_value=_AuthOn()):
            response = await async_client.get("/api/dummy-epg/profiles")

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    @pytest.mark.asyncio
    async def test_unrelated_api_route_still_requires_a_token(self, async_client):
        """The prefix must not widen past dummy-epg."""
        with patch("main.get_auth_settings", return_value=_AuthOn()):
            response = await async_client.get("/api/channels")

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    @pytest.mark.asyncio
    async def test_non_get_on_the_xmltv_path_still_requires_a_token(
        self, async_client
    ):
        """Only GET/HEAD is exempt. A POST to the same URL must be rejected
        by the middleware before routing, so adding a mutating route under
        the xmltv prefix later cannot inherit the exemption.
        """
        with patch("main.get_auth_settings", return_value=_AuthOn()):
            response = await async_client.post("/api/dummy-epg/xmltv")

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    def test_exempt_prefixes_cover_only_the_guide_reads(self):
        """Structural check on the constant — catches an accidental
        widening of the prefix during a refactor.

        Both entries exist for the same reason: Dispatcharr registers them as
        XMLTV sources and its fetcher has nowhere to put an ECM bearer token.
        The artwork proxy returns an upstream guide that is already public at
        its own URL, with the programme artwork repointed.
        """
        from main import AUTH_EXEMPT_GET_PREFIXES

        assert AUTH_EXEMPT_GET_PREFIXES == (
            "/api/dummy-epg/xmltv",
            "/api/epg/artwork-proxy",
        )
