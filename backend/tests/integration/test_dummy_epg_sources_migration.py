"""Existing profiles survive the additive programme-source migration."""
from datetime import datetime

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import MetaData, Table, create_engine, inspect, select

import database


@pytest.mark.integration
def test_source_columns_upgrade_and_downgrade_preserve_profile(tmp_path):
    url = f"sqlite:///{tmp_path / 'guide.db'}"
    config = Config(str(database.ALEMBIC_INI_PATH))
    config.set_main_option("sqlalchemy.url", url)
    command.upgrade(config, "0051")
    engine = create_engine(url)
    try:
        table = Table("dummy_epg_profiles", MetaData(), autoload_with=engine)
        now = datetime.utcnow()
        with engine.begin() as connection:
            connection.execute(table.insert().values(
                id=1, name="Sports", enabled=True, name_source="channel", stream_index=1,
                event_timezone="US/Eastern", program_duration=180, tvg_id_template="ecm-{channel_id}",
                include_date_tag=False, include_live_tag=True, include_new_tag=False,
                pattern_variants='[{"name":"MLB","program_poster_url_template":"/cover?style=4&fallback=true"}]',
                channel_group_ids="[65]", created_at=now, updated_at=now,
            ))
            before = dict(connection.execute(select(table)).mappings().one())
        command.upgrade(config, "0052")
        columns = {column["name"]: column for column in inspect(engine).get_columns("dummy_epg_profiles")}
        assert columns["epg_source_ids"]["nullable"]
        assert columns["channel_mappings"]["nullable"]
        upgraded = Table("dummy_epg_profiles", MetaData(), autoload_with=engine)
        with engine.connect() as connection:
            after = dict(connection.execute(select(upgraded)).mappings().one())
        assert after.pop("epg_source_ids") is None
        assert after.pop("channel_mappings") is None
        assert after == before
        command.downgrade(config, "0051")
        restored = Table("dummy_epg_profiles", MetaData(), autoload_with=engine)
        with engine.connect() as connection:
            assert dict(connection.execute(select(restored)).mappings().one()) == before
    finally:
        engine.dispose()
