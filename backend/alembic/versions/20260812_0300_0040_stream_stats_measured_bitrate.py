"""Add stream_stats.measured_bitrate for sampled throughput.

Revision ID: 0040
Revises: 0039

``StreamStats.measured_bitrate`` holds the throughput actually read off a
stream, kept separate from ``video_bitrate`` (what ffprobe declares). The two
must not share a column: a provider serving a slate placeholder or nothing at
all still returns a valid ffprobe header, so the declared figure cannot tell a
live stream from a dead one while the sampled figure can.

This column was first added by an in-process ``ALTER TABLE`` in
``database._run_migrations``. That path cannot work for a NEW column, because
``init_db`` runs ``_assert_schema_matches_models`` BEFORE ``_run_migrations``:
on any database that already exists, the assertion sees a model column with no
physical counterpart and aborts startup before the in-process add is reached.
``Base.metadata.create_all`` does not close the gap either, since it creates
missing tables and never adds columns to existing ones. Alembic runs ahead of
the assertion, so a revision is the only ordering that works. The in-process
helper was removed with this revision so the column has a single owner.

Nullable with no server default: rows probed before this lands have no sample,
and ``None`` means "never measured", which the health check treats differently
from a measured zero.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = "0040"
down_revision: Union[str, Sequence[str], None] = "0039"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "stream_stats"
COLUMN = "measured_bitrate"


def _has_column() -> bool:
    """Return True when the column is already physically present.

    Databases that booted while the in-process helper still ran already carry
    the column, so both directions are guarded to stay re-runnable.
    """
    return any(col["name"] == COLUMN for col in inspect(op.get_bind()).get_columns(TABLE))


def upgrade() -> None:
    if _has_column():
        return
    op.add_column(TABLE, sa.Column(COLUMN, sa.BigInteger(), nullable=True))


def downgrade() -> None:
    if not _has_column():
        return
    # SQLite cannot drop a column in place before 3.35 — batch mode recreates
    # the table and reflects its indexes, per docs/database_migrations.md.
    with op.batch_alter_table(TABLE) as batch_op:
        batch_op.drop_column(COLUMN)
