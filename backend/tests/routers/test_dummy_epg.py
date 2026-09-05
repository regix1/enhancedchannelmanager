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
        """include_trace=True returns a per-field trace describing pipes
        and conditional branches."""
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

    @pytest.mark.asyncio
    async def test_source_generation_uses_existing_background_task(self, async_client, test_session):
        profile = _create_profile(test_session)
        profile.set_epg_source_ids([42])
        test_session.commit()
        engine = MagicMock()
        engine.run_task = AsyncMock()
        with patch("task_engine.get_engine", return_value=engine), patch(
            "routers.dummy_epg._fetch_all_channels", AsyncMock(return_value={})
        ) as fetch_channels:
            response = await async_client.post("/api/dummy-epg/generate")
        assert response.status_code == 200
        assert response.json() == {"status": "pending", "profiles_generated": 0, "task_id": "dummy_epg_refresh"}
        engine.run_task.assert_awaited_once_with("dummy_epg_refresh")
        fetch_channels.assert_not_awaited()


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


class TestProgrammeSources:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("link_field", ["epg_data_id", "epg_data"])
    async def test_captures_numeric_identity_and_preserves_it_on_sparse_edit(self, async_client, test_session, link_field):
        client = AsyncMock()
        client.get_epg_sources.return_value = [
            {"id": 42, "source_type": "xmltv", "is_active": True, "url": "https://guide.example/xml"},
            {"id": 49, "source_type": "xmltv", "is_active": True, "url": "https://ecm.example/api/epg/artwork-proxy/42"},
        ]
        rows = [{"id": 4982526, "epg_source": 49, "tvg_id": "32645"}]
        client.get_epg_data.side_effect = AssertionError("Only linked rows are needed")
        client.get_epg_data_by_id.return_value = rows[0]
        channels = {2950: {"id": 2950, "name": "ESPN", "tvg_id": "ESPN.us", "epg_data_id": None, link_field: 4982526, "channel_group": 68}}
        with patch("routers.dummy_epg.get_client", return_value=client), patch("routers.dummy_epg._fetch_all_channels", AsyncMock(return_value=channels)):
            response = await async_client.post("/api/dummy-epg/profiles", json={
                "name": "Universal", "channel_group_ids": [68], "epg_source_ids": [49],
                "program_poster_url_template": "/mlb/{away}/{home}/cover?style=4&fallback=true",
            })
        assert response.status_code == 200, response.text
        saved = response.json()
        assert saved["epg_source_ids"] == [49]
        client.get_epg_data_by_id.assert_awaited_once_with(4982526)
        assert saved["channel_mappings"] == [{"channel_id": 2950, "source_id": 42, "tvg_id": "32645"}]
        with patch("routers.dummy_epg.get_client") as get_client:
            response = await async_client.patch(f"/api/dummy-epg/profiles/{saved['id']}", json={"title_template": "{title}"})
        assert response.status_code == 200
        assert response.json()["channel_mappings"] == saved["channel_mappings"]
        assert response.json()["program_poster_url_template"] == saved["program_poster_url_template"]
        get_client.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("fields", [
        {"epg_source_ids": [0]}, {"epg_source_ids": [True]}, {"epg_source_ids": [1, 1]},
        {"epg_source_ids": [1], "channel_mappings": [{"channel_id": 2, "source_id": 1, "tvg_id": " "}]},
        {"epg_source_ids": [1], "channel_mappings": [{"channel_id": 2, "source_id": 1, "tvg_id": "A"}, {"channel_id": 2, "source_id": 1, "tvg_id": "B"}]},
        {"channel_mappings": [{"channel_id": 2, "source_id": 1, "tvg_id": "A"}]},
    ])
    async def test_invalid_selection_does_not_create_profile(self, async_client, test_session, fields):
        response = await async_client.post("/api/dummy-epg/profiles", json={"name": "Invalid", **fields})
        assert response.status_code == 422
        assert test_session.query(DummyEPGProfile).count() == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("source", [
        {"id": 1, "source_type": "xmltv", "is_active": False, "url": "https://guide.example/secret"},
        {"id": 1, "source_type": "xmltv", "is_active": True, "url": "https://ecm.example/api/dummy-epg/xmltv/1"},
        {"id": 1, "source_type": "schedulesdirect", "is_active": True, "url": "https://guide.example/secret"},
    ])
    async def test_unusable_source_is_rejected_without_url_disclosure(self, async_client, source):
        client = AsyncMock()
        client.get_epg_sources.return_value = [source]
        with patch("routers.dummy_epg.get_client", return_value=client):
            response = await async_client.post("/api/dummy-epg/profiles", json={"name": "Invalid", "epg_source_ids": [1]})
        assert response.status_code == 422
        assert "https://" not in response.text
        assert "secret" not in response.text

    @pytest.mark.asyncio
    async def test_explicit_empty_sources_returns_to_legacy_without_external_reads(self, async_client, test_session):
        profile = _create_profile(test_session)
        profile.set_epg_source_ids([49])
        profile.set_channel_mappings([{"channel_id": 2950, "source_id": 42, "tvg_id": "32645"}])
        test_session.commit()
        with patch("routers.dummy_epg.get_client") as get_client:
            response = await async_client.patch(f"/api/dummy-epg/profiles/{profile.id}", json={"epg_source_ids": []})
        assert response.status_code == 200
        assert response.json()["epg_source_ids"] == []
        assert response.json()["channel_mappings"] == []
        get_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_coverage_is_private_and_read_only(self, async_client, test_session):
        profile = _create_profile(test_session)
        with patch("main.get_auth_settings", return_value=_AuthOn()), patch("services.epg_programmes.prepare_profiles", new_callable=AsyncMock) as prepare:
            denied = await async_client.get(f"/api/dummy-epg/profiles/{profile.id}/coverage")
        assert denied.status_code == 401
        prepare.assert_not_awaited()
        coverage = {"generated_at": "2026-09-05T05:00:00Z", "window_start": "2026-09-05T00:00:00Z", "window_stop": "2026-09-07T00:00:00Z", "sources": [
            {"source_id": 51, "status": "pending", "last_success": None, "error": None},
            {"source_id": 42, "status": "error", "last_success": None, "error": "Malformed XML.", "diagnostics": {
                "wire_bytes": 512, "decoded_bytes": 4096, "http_status": 200, "content_type": "absent", "content_encoding": "gzip",
                "compression": "gzip", "transport_complete": True, "xml_complete": False, "root": "tv",
                "parser_code": 4, "parser_line": 1, "parser_column": 3902, "failure": "invalid_utf8"}},
        ], "channels": []}
        before = profile.to_dict()
        with patch("routers.dummy_epg._fetch_all_channels", AsyncMock(return_value={})), patch("routers.dummy_epg.get_client", return_value=AsyncMock()) as client, patch("services.epg_programmes.prepare_profiles", AsyncMock(return_value=([before], coverage))) as prepare:
            response = await async_client.get(f"/api/dummy-epg/profiles/{profile.id}/coverage")
        assert response.status_code == 200
        assert response.json() == coverage
        assert prepare.await_args.kwargs == {}
        client.return_value.refresh_epg_source.assert_not_awaited()
        client.return_value.update_channel.assert_not_awaited()
        assert profile.to_dict() == before

    @pytest.mark.asyncio
    async def test_targeted_generation_keeps_all_profiles_in_combined_cache(self, async_client, test_session):
        first = _create_profile(test_session, name="First")
        second = _create_profile(test_session, name="Second")
        first.set_channel_group_ids([1])
        second.set_channel_group_ids([2])
        test_session.commit()
        channels = {1: {"id": 1, "name": "One", "channel_group": 1}, 2: {"id": 2, "name": "Two", "channel_group_id": 2}}
        with patch("routers.dummy_epg._fetch_all_channels", AsyncMock(return_value=channels)), patch("routers.dummy_epg.cache") as cache:
            response = await async_client.post("/api/dummy-epg/generate", json={"profile_ids": [first.id]})
        assert response.status_code == 200
        assert response.json()["profiles_generated"] == 1
        combined = next(call.args[1] for call in cache.set.call_args_list if call.args[0] == "dummy_epg_xmltv_all")
        assert "One" in combined and "Two" in combined
        assert any(call.args[0] == f"dummy_epg_xmltv_{first.id}" for call in cache.set.call_args_list)
        assert not any(call.args[0] == f"dummy_epg_xmltv_{second.id}" for call in cache.set.call_args_list)


    @pytest.mark.asyncio
    async def test_coverage_accepts_bound_private_service_claim(self, async_client, test_session):
        from types import SimpleNamespace
        from auth.mcp_service import MCP_CLAIM_HEADER, MCPServiceCredentials, issue_test_claim

        profile = _create_profile(test_session)
        path = f"/api/dummy-epg/profiles/{profile.id}/coverage"
        credentials = MCPServiceCredentials("private-coverage-key", "private-coverage-confirmation")
        claim = issue_test_claim(credentials, "GET", path, None)
        with (
            patch("main.get_auth_settings", return_value=_AuthOn()),
            patch("main.get_settings", return_value=SimpleNamespace(mcp_api_key="public-listener-key")),
            patch("main.load_mcp_service_credentials", return_value=credentials),
            patch("routers.dummy_epg._fetch_all_channels", AsyncMock(return_value={})),
            patch("services.epg_programmes.prepare_profiles", AsyncMock(return_value=([], {"sources": [], "channels": []}))),
        ):
            response = await async_client.get(path, headers={"Authorization": "Bearer private-coverage-key", MCP_CLAIM_HEADER: claim})
            refused = await async_client.get(path, headers={"Authorization": "Bearer public-listener-key"})
        assert response.status_code == 200, response.text
        assert refused.status_code == 403

    @pytest.mark.asyncio
    async def test_yaml_round_trip_and_sparse_overwrite_keep_sources_and_artwork(self, async_client, test_session):
        import yaml

        profile = _create_profile(test_session, program_poster_url_template="/mlb/{away}/{home}/cover?style=4&fallback=true")
        profile.set_epg_source_ids([51])
        profile.set_channel_mappings([{"channel_id": 2950, "source_id": 51, "tvg_id": "32645"}])
        test_session.commit()
        client = AsyncMock()
        client.get_channel_groups.return_value = []
        client.get_epg_sources.return_value = [{"id": 51, "source_type": "xmltv", "url": "https://guide.example/xml", "is_active": True}]
        client.get_epg_data.return_value = []
        with patch("routers.dummy_epg.get_session", return_value=test_session), patch("routers.dummy_epg.get_client", return_value=client), patch("routers.dummy_epg._fetch_all_channels", AsyncMock(return_value={})):
            exported = await async_client.get("/api/dummy-epg/profiles/export/yaml")
            document = yaml.safe_load(exported.text)
            original = document["profiles"][0]
            assert original["epg_source_ids"] == [51]
            assert original["channel_mappings"][0]["tvg_id"] == "32645"
            original["name"] = "Copy"
            copied = await async_client.post("/api/dummy-epg/profiles/import/yaml", json={"yaml_content": yaml.safe_dump(document)})
            assert copied.status_code == 200, copied.text
            assert copied.json()["errors"] == []
            sparse = await async_client.post("/api/dummy-epg/profiles/import/yaml", json={"overwrite": True, "yaml_content": yaml.safe_dump({"profiles": [{"name": "Copy", "title_template": "Updated"}]})})
        assert sparse.status_code == 200, sparse.text
        saved = test_session.query(DummyEPGProfile).filter_by(name="Copy").one().to_dict()
        assert saved["channel_mappings"] == original["channel_mappings"]
        assert saved["epg_source_ids"] == [51]
        assert saved["program_poster_url_template"] == original["program_poster_url_template"]


    @pytest.mark.asyncio
    async def test_full_yaml_template_overwrite_retains_unchanged_sources_offline(self, async_client, test_session):
        import yaml
        profile = _create_profile(test_session)
        profile.set_epg_source_ids([51])
        profile.set_channel_group_ids([65])
        profile.set_channel_mappings([{"channel_id": 10, "source_id": 51, "tvg_id": "111"}])
        test_session.commit()
        fields = profile.to_dict()
        fields["title_template"] = "Updated {title}"
        upstream = AsyncMock()
        upstream.get_epg_sources.side_effect = RuntimeError("unavailable")
        with patch("routers.dummy_epg.get_session", return_value=test_session), patch("routers.dummy_epg.get_client", return_value=upstream):
            response = await async_client.post("/api/dummy-epg/profiles/import/yaml", json={"overwrite": True, "yaml_content": yaml.safe_dump({"profiles": [fields]})})
        assert response.status_code == 200, response.text
        assert response.json()["errors"] == []
        assert len(response.json()["imported"]) == 1
        assert profile.title_template == "Updated {title}"
        assert profile.get_channel_mappings() == fields["channel_mappings"]
        upstream.get_epg_sources.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_yaml_invalid_sources_leave_existing_profile_unchanged(self, async_client, test_session):
        profile = _create_profile(test_session)
        with patch("routers.dummy_epg.get_session", return_value=test_session), patch("routers.dummy_epg.get_client", return_value=AsyncMock()):
            response = await async_client.post("/api/dummy-epg/profiles/import/yaml", json={"overwrite": True, "yaml_content": "profiles:\n  - name: Test Profile\n    title_template: Changed\n    epg_source_ids: [0]\n"})
        assert response.status_code == 200
        assert len(response.json()["errors"]) == 1
        saved = test_session.query(DummyEPGProfile).filter_by(id=profile.id).one()
        assert saved.title_template == "{title}"


    @pytest.mark.asyncio
    @pytest.mark.parametrize("single_profile", [False, True])
    @pytest.mark.parametrize("source_status", ["ready", "pending", "error", "stale", "artwork"])
    async def test_http_guide_uses_prepared_source_programmes(self, async_client, test_session, single_profile, source_status):
        from datetime import datetime, timezone
        from xml.etree.ElementTree import Element, SubElement

        profile = _create_profile(test_session, tvg_id_template="ecm-{channel_id}")
        profile.set_epg_source_ids([51])
        profile.set_channel_group_ids([65])
        test_session.commit()
        channels = {10: {"id": 10, "name": "PPV 16", "channel_group": 65}}
        programme = Element("programme", channel="PPV10.art", start="20260905010000 +0000", stop="20260905050000 +0000")
        SubElement(programme, "title").text = "ONE Fight Night 47"
        prepared = profile.to_dict()
        prepared.update({
            "channel_assignments": [{"channel_id": 10, "channel_name": "PPV 16"}],
            "source_programmes": {10: [programme]},
            "guide_start": datetime(2026, 9, 5, tzinfo=timezone.utc),
            "guide_stop": datetime(2026, 9, 7, tzinfo=timezone.utc),
        })
        cache = MagicMock()
        cache.get.return_value = None
        with patch("routers.dummy_epg._fetch_all_channels", AsyncMock(return_value=channels)), patch("routers.dummy_epg.cache", cache), patch("services.epg_programmes.prepare_profiles", AsyncMock(return_value=([prepared], {"sources": [{"status": "ready" if source_status == "artwork" else source_status}], "artwork_pending": source_status == "artwork"}))) as prepare:
            path = f"/api/dummy-epg/xmltv/{profile.id}" if single_profile else "/api/dummy-epg/xmltv"
            response = await async_client.get(path)
        assert response.status_code == 200, response.text
        assert "ONE Fight Night 47" in response.text
        assert 'channel="ecm-10"' in response.text
        assert "20260905050000 +0000" in response.text
        if source_status in {"ready", "artwork"}:
            cache.set.assert_called_once()
        else:
            cache.set.assert_not_called()
        prepare.assert_awaited_once()
        assert prepare.await_args.kwargs == {}

    @pytest.mark.asyncio
    async def test_name_preview_never_fetches_programme_sources(self, async_client):
        with patch("routers.dummy_epg.get_client") as client, patch("services.epg_programmes.prepare_profiles", new_callable=AsyncMock) as prepare:
            response = await async_client.post("/api/dummy-epg/preview", json={"sample_name": "ESPN", "title_pattern": "(?P<title>.+)", "title_template": "{title}"})
        assert response.status_code == 200, response.text
        client.assert_not_called()
        prepare.assert_not_awaited()


    @pytest.mark.asyncio
    async def test_unchanged_ui_selection_saves_templates_when_dispatcharr_is_unavailable(self, async_client, test_session):
        profile = _create_profile(test_session)
        profile.set_epg_source_ids([49])
        profile.set_channel_group_ids([68])
        profile.set_channel_mappings([{"channel_id": 2950, "source_id": 42, "tvg_id": "32645"}])
        test_session.commit()
        client = AsyncMock()
        client.get_epg_sources.side_effect = TimeoutError
        with patch("routers.dummy_epg.get_client", return_value=client) as get_client:
            response = await async_client.patch(f"/api/dummy-epg/profiles/{profile.id}", json={
                "enabled": True, "channel_group_ids": [68], "epg_source_ids": [49],
                "title_template": "Updated title",
            })
        assert response.status_code == 200, response.text
        assert response.json()["title_template"] == "Updated title"
        assert response.json()["channel_mappings"] == [{"channel_id": 2950, "source_id": 42, "tvg_id": "32645"}]
        get_client.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("change", [
        {"epg_source_ids": [43]},
        {"channel_group_ids": [69]},
        {"channel_mappings": [{"channel_id": 2950, "source_id": 42, "tvg_id": "replacement"}]},
        {"enabled": True},
    ])
    async def test_changed_selection_or_enable_requires_source_validation(self, async_client, test_session, change):
        profile = _create_profile(test_session)
        profile.set_epg_source_ids([42])
        profile.set_channel_group_ids([68])
        profile.set_channel_mappings([{"channel_id": 2950, "source_id": 42, "tvg_id": "32645"}])
        if "enabled" in change:
            profile.enabled = False
        test_session.commit()
        client = AsyncMock()
        client.get_epg_sources.side_effect = TimeoutError
        with patch("routers.dummy_epg.get_client", return_value=client):
            response = await async_client.patch(f"/api/dummy-epg/profiles/{profile.id}", json=change)
        assert response.status_code == 502, response.text
        client.get_epg_sources.assert_awaited_once()
        saved = (await async_client.get(f"/api/dummy-epg/profiles/{profile.id}")).json()
        assert saved["epg_source_ids"] == [42]
        assert saved["channel_group_ids"] == [68]
        assert saved["channel_mappings"] == [{"channel_id": 2950, "source_id": 42, "tvg_id": "32645"}]
        assert saved["enabled"] == ("enabled" not in change)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stalled", ["catalogue", "source"])
    async def test_generation_returns_pending_then_publishes_loaded_schedule(self, async_client, test_session, monkeypatch, stalled):
        import asyncio
        from datetime import datetime, timedelta, timezone
        from xml.etree import ElementTree as ET
        from services import epg_programmes as guides
        from tasks.dummy_epg_refresh import DummyEPGRefreshTask

        for name in ("_CATALOGUE_CACHE", "_CATALOGUE_LOADS", "_SOURCE_CACHE", "_SOURCE_LOADS"):
            monkeypatch.setattr(guides, name, {})
        profile = _create_profile(test_session)
        profile.set_epg_source_ids([42])
        profile.set_channel_group_ids([68])
        test_session.commit()
        channels = {1: {"id": 1, "name": "ESPN", "channel_number": 1, "channel_group_id": 68, "tvg_id": "ESPN.us", "streams": []}}
        sources = [{"id": 42, "source_type": "xmltv", "is_active": True, "url": "https://guide.invalid/guide.xml"}]
        release, started = asyncio.Event(), asyncio.Event()
        now = datetime.now(timezone.utc).replace(microsecond=0)
        programme = ET.Element("programme", {
            "channel": "ESPN.us", "start": now.strftime("%Y%m%d%H%M%S %z"),
            "stop": (now + timedelta(hours=1)).strftime("%Y%m%d%H%M%S %z"),
        })
        ET.SubElement(programme, "title").text = "Fixture schedule"

        async def catalogue():
            if stalled == "catalogue":
                started.set()
                await release.wait()
            return sources

        async def read(*_):
            if stalled == "source":
                started.set()
                await release.wait()
            return {"headers": {}, "rows": {"ESPN.us": [programme]}, "warnings": [], "size": 100}

        client = AsyncMock()
        client.get_epg_sources.side_effect = catalogue
        monkeypatch.setattr(guides, "_read_source", read)
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [profile]
        engine = MagicMock()
        engine.run_task = AsyncMock()
        with patch("task_engine.get_engine", return_value=engine), patch(
            "tasks.dummy_epg_refresh.get_client", return_value=client
        ), patch("database.get_session", return_value=db), patch(
            "services.epg_programmes._fetch_all_channels", AsyncMock(return_value=channels)
        ), patch("cache.get_cache") as cache, patch("routers.dummy_epg.cache", cache.return_value):
            response = await async_client.post("/api/dummy-epg/generate")
            assert response.json() == {"status": "pending", "profiles_generated": 0, "task_id": "dummy_epg_refresh"}
            cache.return_value.set.assert_not_called()
            refresh = asyncio.create_task(DummyEPGRefreshTask()._regenerate_xmltv())
            try:
                await asyncio.wait_for(started.wait(), timeout=2)
                cache.return_value.set.assert_not_called()
                release.set()
                assert await refresh == 1
                published = {call.args[0]: call.args[1] for call in cache.return_value.set.call_args_list}
                assert "Fixture schedule" in published["dummy_epg_xmltv_all"]
                assert "Fixture schedule" in published[f"dummy_epg_xmltv_{profile.id}"]
                engine.run_task.assert_awaited_once_with("dummy_epg_refresh")
                client.get_epg_sources.assert_awaited_once()
            finally:
                release.set()
                await refresh

    @pytest.mark.asyncio
    async def test_background_guide_is_cached_while_portraits_are_pending(self, async_client, test_session, monkeypatch, tmp_path):
        import asyncio
        from datetime import datetime, timedelta, timezone
        from xml.etree import ElementTree as ET
        from cache import Cache
        from services import epg_artwork, epg_programmes as guides
        from tasks.dummy_epg_refresh import DummyEPGRefreshTask

        for name in ("_CATALOGUE_CACHE", "_CATALOGUE_LOADS", "_SOURCE_CACHE", "_SOURCE_LOADS"):
            monkeypatch.setattr(guides, name, {})
        monkeypatch.setattr(guides, "_ARTWORK_LOAD", None)
        monkeypatch.setattr(guides, "_ARTWORK_CHECKED", 0)
        monkeypatch.setattr("config.CONFIG_DIR", tmp_path)
        profile = _create_profile(test_session, tvg_id_template="ecm-{channel_id}")
        profile.set_epg_source_ids([42])
        profile.set_channel_group_ids([68])
        test_session.commit()
        channels = {1: {"id": 1, "name": "ESPN", "channel_number": 1, "channel_group_id": 68, "tvg_id": "ESPN.us", "streams": []}}
        sources = [{"id": 42, "source_type": "xmltv", "is_active": True, "url": "https://guide.invalid/guide.xml"}]
        now = datetime.now(timezone.utc).replace(microsecond=0)
        programme = ET.Element("programme", {
            "channel": "ESPN.us", "start": now.strftime("%Y%m%d%H%M%S %z"),
            "stop": (now + timedelta(hours=1)).strftime("%Y%m%d%H%M%S %z"),
        })
        ET.SubElement(programme, "title").text = "Complete schedule"
        landscape = "https://tmsimg.com/assets/p12345_b_h3_aa.jpg"
        portrait = "https://tmsimg.com/assets/p12345_b_v12_aa.jpg"
        ET.SubElement(programme, "icon", {"src": landscape})
        started, release = asyncio.Event(), asyncio.Event()

        async def probe(artwork_cache, unknown):
            started.set()
            await release.wait()
            for key in unknown:
                artwork_cache.put(key, "v12")
            artwork_cache.save()
            return len(unknown)

        client = AsyncMock()
        client.get_epg_sources.return_value = sources
        read = AsyncMock(return_value={"headers": {}, "rows": {"ESPN.us": [programme]}, "warnings": [], "size": 100})
        monkeypatch.setattr(guides, "_read_source", read)
        monkeypatch.setattr(epg_artwork, "probe_unknown", probe)
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [profile]
        cache = Cache()
        with patch("tasks.dummy_epg_refresh.get_client", return_value=client), patch(
            "routers.dummy_epg.get_client", return_value=client
        ), patch("database.get_session", return_value=db), patch(
            "services.epg_programmes._fetch_all_channels", AsyncMock(return_value=channels)
        ), patch("routers.dummy_epg._fetch_all_channels", AsyncMock(return_value=channels)) as fetch, patch(
            "cache.get_cache", return_value=cache
        ), patch.object(guides, "get_cache", return_value=cache), patch("routers.dummy_epg.cache", cache):
            task = None
            try:
                assert await DummyEPGRefreshTask()._regenerate_xmltv() == 1
                await asyncio.wait_for(started.wait(), timeout=1)
                task = guides._ARTWORK_LOAD
                for key in ("dummy_epg_xmltv_all", f"dummy_epg_xmltv_{profile.id}"):
                    xml = cache.get(key, ttl=300)
                    assert xml is not None
                    assert landscape in xml
                    ET.fromstring(xml)
                response = await async_client.get(f"/api/dummy-epg/xmltv/{profile.id}")
                assert response.status_code == 200
                assert landscape in response.text
                assert "Complete schedule" in response.text
                fetch.assert_not_awaited()
                release.set()
                await task
                assert cache.get("dummy_epg_xmltv_all", ttl=300) is None
                assert cache.get(f"dummy_epg_xmltv_{profile.id}", ttl=300) is None
                response = await async_client.get(f"/api/dummy-epg/xmltv/{profile.id}")
                assert response.status_code == 200
                assert portrait in response.text
                assert landscape not in response.text
                assert cache.get(f"dummy_epg_xmltv_{profile.id}", ttl=300) == response.text
                assert fetch.await_count == 1
                combined = await async_client.get("/api/dummy-epg/xmltv")
                assert combined.status_code == 200
                assert portrait in combined.text
                assert cache.get("dummy_epg_xmltv_all", ttl=300) == combined.text
                read.assert_awaited_once()
            finally:
                release.set()
                task = task or guides._ARTWORK_LOAD
                if task is not None:
                    await asyncio.gather(task, return_exceptions=True)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("source_status", "artwork_pending", "status"), [
        ("error", False, "error"), ("stale", False, "error"),
        ("pending", False, "pending"), ("pending", True, "pending"),
    ])
    async def test_generation_reports_unready_coverage_without_publishing(self, async_client, test_session, source_status, artwork_pending, status):
        profile = _create_profile(test_session)
        coverage = {
            "sources": [{"source_id": 42, "status": source_status}],
            "channels": [], "artwork_pending": artwork_pending,
        }
        with patch("services.epg_programmes.prepare_profiles", AsyncMock(return_value=([profile.to_dict()], coverage))), patch(
            "routers.dummy_epg._fetch_all_channels", AsyncMock(return_value={})
        ), patch("routers.dummy_epg.cache") as cache:
            response = await async_client.post("/api/dummy-epg/generate")
        assert response.status_code == 200, response.text
        assert response.json() == {"status": status, "profiles_generated": 1, "coverage": coverage}
        cache.set.assert_not_called()
