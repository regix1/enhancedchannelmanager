"""POST /api/settings must save what the form sends and reset nothing else.

The handler used to build a DispatcharrSettings from a fixed list of named
kwargs, so every model field missing from that list came back as its default and
overwrote the stored value on save. Three of those fields are load-bearing: the
post-refresh auto-creation latch, the MCP bulk-operation caps, and the
refresh/consumed timestamp pair that gates a pending auto-creation run.

The two halves are now pinned separately. A field both models carry is saved as
sent, whether or not anyone remembered to write a line for it, and a field only
the settings model carries keeps its stored value.
"""
from unittest.mock import MagicMock, patch

import pytest

from config import DispatcharrSettings
from routers.settings import SettingsRequest


def _stored_settings(**overrides) -> DispatcharrSettings:
    """A real settings model, not a mock — these tests are about the fields
    the handler never names, which a mock stand-in cannot represent.
    """
    return DispatcharrSettings(
        url="http://dispatcharr:8000",
        username="admin",
        password="secret",
        **overrides,
    )


def _distinct_request_values() -> dict[str, object]:
    """A non-default value for every field both models carry.

    Built from the model's own defaults, so a field the handler drops shows up
    as its default coming back instead of the value that was sent. api_key is
    left out: it is folded into the canonical key by the handler (bd-jmi1c).
    """
    values: dict[str, object] = {}
    for name, field in DispatcharrSettings.model_fields.items():
        if name not in SettingsRequest.model_fields or name == "api_key":
            continue
        default = field.get_default(call_default_factory=True)
        if isinstance(default, bool):
            values[name] = not default
        elif isinstance(default, int):
            values[name] = default + 7
        elif isinstance(default, float):
            # dedup_threshold is clamped to [CONFIDENCE_FLOOR, 1.0].
            values[name] = 0.93
        elif isinstance(default, str):
            values[name] = (default or "") + "-sent"
        # Shaped containers (list[int], list[dict]) a generic value cannot fill
        # are covered by the falsy-value cases below.
    # auth_method is a closed set, and api_key mode needs the canonical key,
    # which this body sends.
    values["auth_method"] = "api_key"
    return values


class TestEveryFieldBothModelsCarryIsSavedAsSent:
    @pytest.mark.asyncio
    async def test_no_shared_field_is_dropped_on_the_way_to_save(self, async_client):
        """The saved settings carry the request's value for every field the two
        models share. This is what stops a field being forgotten: it holds
        without anyone writing a line per field, and it fails the moment one
        field stops being routed through.
        """
        current = _stored_settings()
        sent = _distinct_request_values()

        with patch("routers.settings.get_settings", return_value=current), \
             patch("routers.settings.save_settings") as mock_save, \
             patch("routers.settings.clear_settings_cache"), \
             patch("routers.settings.reset_client"), \
             patch("routers.settings.get_prober", return_value=None), \
             patch("routers.settings.get_cache", return_value=MagicMock()), \
             patch("routers.settings._validate_discord_webhook_on_save"), \
             patch(
                 "routers.settings._validate_outbound_base_url_on_save",
                 side_effect=lambda label, url: url,
             ):
            response = await async_client.post("/api/settings", json=sent)

        assert response.status_code == 200, response.json()
        saved = mock_save.call_args[0][0]
        dropped = {
            name: (value, getattr(saved, name))
            for name, value in sent.items()
            if getattr(saved, name) != value
        }
        assert dropped == {}, f"fields not saved as sent: {dropped}"


class TestUnnamedFieldsSurviveASave:
    @pytest.mark.asyncio
    async def test_auto_creation_refresh_latch_survives(self, async_client):
        """task_engine sets this True after an abandoned run so the
        post-refresh auto-creation cannot fire again. A settings save that
        clears it re-arms a run the system deliberately stopped.
        """
        current = _stored_settings(auto_creation_run_on_refresh_disabled=True)

        with patch("routers.settings.get_settings", return_value=current), \
             patch("routers.settings.save_settings") as mock_save, \
             patch("routers.settings.clear_settings_cache"), \
             patch("routers.settings.reset_client"), \
             patch("routers.settings.get_prober", return_value=None), \
             patch("routers.settings.get_cache", return_value=MagicMock()):
            response = await async_client.post("/api/settings", json={
                "url": current.url,
                "username": current.username,
                "theme": "light",
            })

        assert response.status_code == 200, response.json()
        saved = mock_save.call_args[0][0]
        assert saved.auto_creation_run_on_refresh_disabled is True

    @pytest.mark.asyncio
    async def test_mcp_bulk_hard_caps_survive(self, async_client):
        """The caps guard destructive bulk deletes and merges. Resetting them
        widens a tightened guardrail without the operator asking.
        """
        current = _stored_settings(
            mcp_bulk_delete_hard_cap=50,
            mcp_bulk_merge_hard_cap=25,
        )

        with patch("routers.settings.get_settings", return_value=current), \
             patch("routers.settings.save_settings") as mock_save, \
             patch("routers.settings.clear_settings_cache"), \
             patch("routers.settings.reset_client"), \
             patch("routers.settings.get_prober", return_value=None), \
             patch("routers.settings.get_cache", return_value=MagicMock()):
            response = await async_client.post("/api/settings", json={
                "url": current.url,
                "username": current.username,
            })

        assert response.status_code == 200, response.json()
        saved = mock_save.call_args[0][0]
        assert saved.mcp_bulk_delete_hard_cap == 50
        assert saved.mcp_bulk_merge_hard_cap == 25

    @pytest.mark.asyncio
    async def test_refresh_and_consumed_timestamps_survive(self, async_client):
        """Auto-creation runs when refresh_at > consumed_at. Zeroing both makes
        the comparison false and silently drops a pending run.
        """
        current = _stored_settings(
            last_m3u_refresh_completed_at="2026-08-11T02:58:23",
            last_auto_creation_consumed_refresh_at="2026-08-10T02:58:23",
        )

        with patch("routers.settings.get_settings", return_value=current), \
             patch("routers.settings.save_settings") as mock_save, \
             patch("routers.settings.clear_settings_cache"), \
             patch("routers.settings.reset_client"), \
             patch("routers.settings.get_prober", return_value=None), \
             patch("routers.settings.get_cache", return_value=MagicMock()):
            response = await async_client.post("/api/settings", json={
                "url": current.url,
                "username": current.username,
            })

        assert response.status_code == 200, response.json()
        saved = mock_save.call_args[0][0]
        assert saved.last_m3u_refresh_completed_at == "2026-08-11T02:58:23"
        assert saved.last_auto_creation_consumed_refresh_at == "2026-08-10T02:58:23"


class TestRequestedFalsyValuesStillOverwrite:
    """Preserving unnamed fields must not spill over into fields the request
    does name. A field sent as false, empty or cleared is an instruction, not
    an omission.
    """

    @pytest.mark.asyncio
    async def test_false_overwrites_a_stored_true(self, async_client):
        current = _stored_settings(show_stream_urls=True, hide_epg_urls=True)

        with patch("routers.settings.get_settings", return_value=current), \
             patch("routers.settings.save_settings") as mock_save, \
             patch("routers.settings.clear_settings_cache"), \
             patch("routers.settings.reset_client"), \
             patch("routers.settings.get_prober", return_value=None), \
             patch("routers.settings.get_cache", return_value=MagicMock()):
            response = await async_client.post("/api/settings", json={
                "url": current.url,
                "username": current.username,
                "show_stream_urls": False,
                "hide_epg_urls": False,
            })

        assert response.status_code == 200, response.json()
        saved = mock_save.call_args[0][0]
        assert saved.show_stream_urls is False
        assert saved.hide_epg_urls is False

    @pytest.mark.asyncio
    async def test_empty_string_overwrites_a_stored_value(self, async_client):
        current = _stored_settings(smtp_host="mail.example.com")

        with patch("routers.settings.get_settings", return_value=current), \
             patch("routers.settings.save_settings") as mock_save, \
             patch("routers.settings.clear_settings_cache"), \
             patch("routers.settings.reset_client"), \
             patch("routers.settings.get_prober", return_value=None), \
             patch("routers.settings.get_cache", return_value=MagicMock()):
            response = await async_client.post("/api/settings", json={
                "url": current.url,
                "username": current.username,
                "smtp_host": "",
            })

        assert response.status_code == 200, response.json()
        assert mock_save.call_args[0][0].smtp_host == ""

    @pytest.mark.asyncio
    async def test_empty_list_clears_trusted_media_networks(self, async_client):
        """None means preserve, [] means clear — that contract predates this
        handler's field list and must still hold.
        """
        current = _stored_settings(trusted_media_networks=["10.0.0.0/8"])

        with patch("routers.settings.get_settings", return_value=current), \
             patch("routers.settings.save_settings") as mock_save, \
             patch("routers.settings.clear_settings_cache"), \
             patch("routers.settings.reset_client"), \
             patch("routers.settings.get_prober", return_value=None), \
             patch("routers.settings.get_cache", return_value=MagicMock()):
            response = await async_client.post("/api/settings", json={
                "url": current.url,
                "username": current.username,
                "trusted_media_networks": [],
            })

        assert response.status_code == 200, response.json()
        assert mock_save.call_args[0][0].trusted_media_networks == []


class TestLegacyApiKeyMirror:
    @pytest.mark.asyncio
    async def test_legacy_api_key_still_tracks_the_canonical_key(
        self, async_client, tmp_path
    ):
        """The legacy api_key field is now carried over from stored settings
        instead of rebuilt empty, so the real save_settings must still mirror
        the canonical dispatcharr_api_key over it.
        """
        import config

        current = _stored_settings(
            dispatcharr_api_key="canonical-key",
            api_key="stale-legacy-key",
        )

        with patch("routers.settings.get_settings", return_value=current), \
             patch.object(config, "CONFIG_DIR", tmp_path), \
             patch.object(config, "CONFIG_FILE", tmp_path / "settings.json"), \
             patch("routers.settings.reset_client"), \
             patch("routers.settings.get_prober", return_value=None), \
             patch("routers.settings.get_cache", return_value=MagicMock()):
            response = await async_client.post("/api/settings", json={
                "url": current.url,
                "username": current.username,
            })

        assert response.status_code == 200, response.json()
        written = (tmp_path / "settings.json").read_text()
        assert '"api_key": "canonical-key"' in written
        assert "stale-legacy-key" not in written
