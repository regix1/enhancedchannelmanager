"""Revision 0056 preserves profiles and adds durable guide publications."""
from datetime import datetime
import json

from alembic import command
from alembic.config import Config
import pytest
from sqlalchemy import MetaData, Table, create_engine, inspect, select

import database


def _config(url: str) -> Config:
    config = Config(str(database.ALEMBIC_INI_PATH))
    config.set_main_option("sqlalchemy.url", url)
    return config


def _insert_profile(connection, table, profile_id: int, name: str, groups: str):
    now = datetime.utcnow()
    connection.execute(table.insert().values(
        id=profile_id,
        name=name,
        enabled=True,
        name_source="channel",
        stream_index=1,
        event_timezone="America/New_York",
        output_timezone="UTC",
        program_duration=0,
        tvg_id_template="custom-{channel_id}",
        include_date_tag=False,
        include_live_tag=True,
        include_new_tag=False,
        pattern_variants='[{"name":"Saved","title_pattern":"(?P<title>.+)"}]',
        channel_group_ids="[65]",
        epg_source_ids="[51]",
        channel_mappings='[{"channel_id":1,"source_id":51,"tvg_id":"saved"}]',
        hide_empty_group_ids="[65]",
        stream_match_group_ids=groups,
        created_at=now,
        updated_at=now,
    ))


@pytest.mark.integration
def test_0055_to_0056_round_trip_and_compatibility(tmp_path):
    url = f"sqlite:///{tmp_path / 'events.db'}"
    config = _config(url)
    command.upgrade(config, "0055")
    engine = create_engine(url)
    try:
        profiles = Table("dummy_epg_profiles", MetaData(), autoload_with=engine)
        with engine.begin() as connection:
            _insert_profile(connection, profiles, 1, "ESPN Events", "[1558, 1557, 1558]")
            _insert_profile(connection, profiles, 2, "UFC Events", "[2462]")
            _insert_profile(connection, profiles, 3, "Generic Events", "[]")

        command.upgrade(config, "0056")
        command.upgrade(config, "0056")

        columns = {
            column["name"]: column
            for column in inspect(engine).get_columns("dummy_epg_profiles")
        }
        assert columns["event_sync_config"]["nullable"] is True
        assert "dummy_epg_publications" in inspect(engine).get_table_names()
        publication_columns = {
            column["name"]
            for column in inspect(engine).get_columns("dummy_epg_publications")
        }
        assert publication_columns == {"scope", "xmltv", "state", "revision"}

        upgraded = Table("dummy_epg_profiles", MetaData(), autoload_with=engine)
        with engine.connect() as connection:
            rows = {
                row["id"]: row
                for row in connection.execute(select(upgraded)).mappings()
            }
        espn = json.loads(rows[1]["event_sync_config"])
        ufc = json.loads(rows[2]["event_sync_config"])
        generic = json.loads(rows[3]["event_sync_config"])
        assert [scope["group_id"] for scope in espn["secondary"]] == [1558, 1557]
        assert espn["slot_patterns"][0]["bootstrap"] is False
        assert espn["slot_patterns"][0]["name"] == "espn"
        assert ufc["slot_patterns"][0]["bootstrap"] is True
        assert len(ufc["slot_patterns"][0]["event_patterns"]) == 3
        assert generic["slot_patterns"] == []
        for row in rows.values():
            assert row["program_duration"] == 0
            assert row["tvg_id_template"] == "custom-{channel_id}"
            assert row["channel_group_ids"] == "[65]"
            assert row["epg_source_ids"] == "[51]"
            assert row["channel_mappings"] == '[{"channel_id":1,"source_id":51,"tvg_id":"saved"}]'

        command.downgrade(config, "0055")
        assert "dummy_epg_publications" not in inspect(engine).get_table_names()
        assert "event_sync_config" not in {
            column["name"]
            for column in inspect(engine).get_columns("dummy_epg_profiles")
        }
        restored = Table("dummy_epg_profiles", MetaData(), autoload_with=engine)
        with engine.connect() as connection:
            assert connection.execute(select(restored.c.id)).scalars().all() == [1, 2, 3]

        command.upgrade(config, "0056")
        rebuilt = Table("dummy_epg_profiles", MetaData(), autoload_with=engine)
        with engine.connect() as connection:
            assert all(
                value is not None
                for value in connection.execute(
                    select(rebuilt.c.event_sync_config)
                ).scalars()
            )
    finally:
        engine.dispose()
