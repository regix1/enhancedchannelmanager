"""Let a dummy EPG profile name groups whose channels hide while empty."""
from alembic import op
import sqlalchemy as sa


revision = "0054"
down_revision = "0053"
branch_labels = None
depends_on = None


def upgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("dummy_epg_profiles")}
    if "hide_empty_group_ids" not in columns:
        op.add_column("dummy_epg_profiles", sa.Column("hide_empty_group_ids", sa.Text(), nullable=True))


def downgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("dummy_epg_profiles")}
    if "hide_empty_group_ids" in columns:
        op.drop_column("dummy_epg_profiles", "hide_empty_group_ids")
