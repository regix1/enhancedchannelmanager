"""Record the selected schedule on each new task execution."""

from alembic import op
import sqlalchemy as sa


revision = "0057"
down_revision = "0056"
branch_labels = None
depends_on = None


def upgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("task_executions")}
    if "schedule_id" not in columns:
        op.add_column("task_executions", sa.Column("schedule_id", sa.Integer(), nullable=True))


def downgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("task_executions")}
    if "schedule_id" in columns:
        with op.batch_alter_table("task_executions") as batch_op:
            batch_op.drop_column("schedule_id")
