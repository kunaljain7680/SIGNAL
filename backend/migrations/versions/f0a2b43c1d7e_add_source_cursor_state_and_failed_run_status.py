"""add source cursor state and failed ingestion run status

Revision ID: f0a2b43c1d7e
Revises: c7d91a4e2f60
Create Date: 2026-11-21

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "f0a2b43c1d7e"
down_revision: Union[str, None] = "c7d91a4e2f60"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "sources",
        sa.Column(
            "cursor_state",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
    )

    op.drop_constraint(
        "check_ingestion_run_status",
        "ingestion_runs",
        type_="check",
    )
    op.create_check_constraint(
        "check_ingestion_run_status",
        "ingestion_runs",
        "status IN ('PENDING', 'FETCHING', 'NORMALIZING', "
        "'COMPLETED', 'FAILED', 'RETRYING', 'DEAD_LETTER')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "check_ingestion_run_status",
        "ingestion_runs",
        type_="check",
    )
    op.create_check_constraint(
        "check_ingestion_run_status",
        "ingestion_runs",
        "status IN ('PENDING', 'FETCHING', 'NORMALIZING', "
        "'COMPLETED', 'RETRYING', 'DEAD_LETTER')",
    )
    op.drop_column("sources", "cursor_state")
