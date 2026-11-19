"""add ingestion runs

Revision ID: b43a19f72c6e
Revises: 3d6d1fd6ccfa
Create Date: 2026-11-20

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b43a19f72c6e"
down_revision: Union[str, None] = "3d6d1fd6ccfa"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ingestion_runs",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("source_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("current_step", sa.Text(), server_default=sa.text("'INIT'"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), server_default=sa.text("0")),
        sa.CheckConstraint(
            "status IN ('PENDING', 'FETCHING', 'NORMALIZING', "
            "'COMPLETED', 'RETRYING', 'DEAD_LETTER')",
            name="check_ingestion_run_status",
        ),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("ingestion_runs")
