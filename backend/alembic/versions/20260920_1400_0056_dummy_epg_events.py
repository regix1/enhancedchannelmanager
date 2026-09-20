"""Persist profile event configuration and complete guide publications."""
import json

from alembic import op
import sqlalchemy as sa


revision = "0056"
down_revision = "0055"
branch_labels = None
depends_on = None


def _ordered_group_ids(raw_value):
    try:
        values = json.loads(raw_value) if raw_value else []
    except (TypeError, ValueError):
        values = []
    result = []
    for value in values if isinstance(values, list) else []:
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and value > 0
            and value not in result
        ):
            result.append(value)
    return result


def _compatibility_config(name, raw_group_ids):
    slot_patterns = []
    folded_name = (name or "").casefold()
    if "espn" in folded_name:
        slot_patterns.append({
            "name": "espn",
            "channel_pattern": r"ESPN\+\s*(?P<slot>\d+)",
            "fallback_pattern": r"ESPN PLUS\s+(?P<slot>\d+):?",
            "event_patterns": [],
            "bootstrap": False,
        })
    if "ufc" in folded_name:
        slot_patterns.append({
            "name": "ufc",
            "channel_pattern": r"UFC\s*(?P<slot>\d+)",
            "fallback_pattern": r"UFC\s*(?:INT\s*)?(?P<slot>\d+):?",
            "event_patterns": [
                r"LIVE\s+EVENT\s+(?P<slot>\d{1,2})(?=\s|:|-|\|).*\bUFC\b.*",
                r"UFC\s*(?:INT\s*)?(?P<slot>\d{1,2})\s*:\s*\S.*",
                r"US\s+\(UFC(?:\s+INT)?\s*(?P<slot>\d{1,2})\)\s*\|.*",
            ],
            "bootstrap": True,
        })
    return {
        "secondary": [
            {"group_id": group_id, "m3u_account_id": None}
            for group_id in _ordered_group_ids(raw_group_ids)
        ],
        "time_window_minutes": 30,
        "enforce_time_window": True,
        "attach_threshold": 0.8,
        "assume_current_date": True,
        "demote_stale_dateless": True,
        "use_default_patterns": True,
        "slot_patterns": slot_patterns,
    }


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {
        column["name"]
        for column in inspector.get_columns("dummy_epg_profiles")
    }
    if "event_sync_config" not in columns:
        op.add_column(
            "dummy_epg_profiles",
            sa.Column("event_sync_config", sa.Text(), nullable=True),
        )

    inspector = sa.inspect(bind)
    if "dummy_epg_publications" not in inspector.get_table_names():
        op.create_table(
            "dummy_epg_publications",
            sa.Column("scope", sa.String(length=255), primary_key=True),
            sa.Column("xmltv", sa.Text(), nullable=True),
            sa.Column(
                "state", sa.Text(), nullable=False,
                server_default=sa.text("'{}'"),
            ),
            sa.Column(
                "revision", sa.Integer(), nullable=False,
                server_default=sa.text("0"),
            ),
        )

    rows = bind.execute(sa.text(
        "SELECT id, name, stream_match_group_ids "
        "FROM dummy_epg_profiles WHERE event_sync_config IS NULL"
    )).mappings()
    for row in rows:
        config = _compatibility_config(
            row["name"], row["stream_match_group_ids"],
        )
        bind.execute(
            sa.text(
                "UPDATE dummy_epg_profiles SET event_sync_config = :config "
                "WHERE id = :profile_id AND event_sync_config IS NULL"
            ),
            {"config": json.dumps(config), "profile_id": row["id"]},
        )


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "dummy_epg_publications" in inspector.get_table_names():
        op.drop_table("dummy_epg_publications")

    columns = {
        column["name"]
        for column in sa.inspect(bind).get_columns("dummy_epg_profiles")
    }
    if "event_sync_config" in columns:
        op.drop_column("dummy_epg_profiles", "event_sync_config")
