"""Let event guide profiles prioritize matching stream groups."""
from alembic import op
import sqlalchemy as sa


revision = "0055"
down_revision = "0054"
branch_labels = None
depends_on = None


def upgrade():
    columns = {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns("dummy_epg_profiles")
    }
    if "stream_match_group_ids" not in columns:
        op.add_column(
            "dummy_epg_profiles",
            sa.Column("stream_match_group_ids", sa.Text(), nullable=True),
        )


def downgrade():
    columns = {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns("dummy_epg_profiles")
    }
    if "stream_match_group_ids" in columns:
        op.drop_column("dummy_epg_profiles", "stream_match_group_ids")
