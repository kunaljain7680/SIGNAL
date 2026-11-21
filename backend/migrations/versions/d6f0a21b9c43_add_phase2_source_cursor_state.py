"""add Phase 2 source cursor state and seed sources

Revision ID: d6f0a21b9c43
Revises: c7d91a4e2f60
Create Date: 2026-11-22

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "d6f0a21b9c43"
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

    op.execute(
        """
        INSERT INTO sources (name, type, config)
        VALUES
            ('GitHub', 'github', '{"schedule_interval_s": 86400}'::jsonb),
            ('Remotive', 'jobs', '{"schedule_interval_s": 86400}'::jsonb)
        """
    )

    # Merge the schedule into existing HN config without replacing its keys.
    op.execute(
        """
        UPDATE sources
        SET config = config || '{"schedule_interval_s": 21600}'::jsonb
        WHERE name = 'Hacker News' AND type = 'hn'
        """
    )


def downgrade() -> None:
    # FK restrictions intentionally prevent deleting a source with persisted events.
    op.execute(
        """
        DELETE FROM sources
        WHERE name IN ('GitHub', 'Remotive')
          AND type IN ('github', 'jobs')
        """
    )

    op.execute(
        """
        UPDATE sources
        SET config = config - 'schedule_interval_s'
        WHERE name = 'Hacker News' AND type = 'hn'
        """
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
        "'COMPLETED', 'RETRYING', 'DEAD_LETTER')",
    )

    op.drop_column("sources", "cursor_state")
