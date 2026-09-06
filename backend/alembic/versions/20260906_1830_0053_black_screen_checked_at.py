"""Record when a black screen verdict was taken."""
from alembic import op
import sqlalchemy as sa


revision = "0053"
down_revision = "0052"
branch_labels = None
depends_on = None


def upgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("stream_stats")}
    if "black_screen_checked_at" not in columns:
        op.add_column("stream_stats", sa.Column("black_screen_checked_at", sa.DateTime(), nullable=True))


def downgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("stream_stats")}
    if "black_screen_checked_at" in columns:
        op.drop_column("stream_stats", "black_screen_checked_at")
