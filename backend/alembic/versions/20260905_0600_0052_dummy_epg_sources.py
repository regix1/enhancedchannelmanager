"""Remember programme sources and original channel guide bindings."""
from alembic import op
import sqlalchemy as sa


revision = "0052"
down_revision = "0051"
branch_labels = None
depends_on = None


def upgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("dummy_epg_profiles")}
    for name in ("epg_source_ids", "channel_mappings"):
        if name not in columns:
            op.add_column("dummy_epg_profiles", sa.Column(name, sa.Text(), nullable=True))


def downgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("dummy_epg_profiles")}
    for name in ("channel_mappings", "epg_source_ids"):
        if name in columns:
            op.drop_column("dummy_epg_profiles", name)
