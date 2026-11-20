"""seed GitHub and Remotive source records

Revision ID: 7e4c2b19a6d0
Revises: f0a2b43c1d7e
Create Date: 2026-11-21

"""

from typing import Sequence, Union

from alembic import op


revision: str = "7e4c2b19a6d0"
down_revision: Union[str, None] = "f0a2b43c1d7e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO sources (name, type)
        VALUES
            ('GitHub', 'github'),
            ('Remotive Jobs', 'jobs')
        ON CONFLICT (name) DO NOTHING
        """
    )


def downgrade() -> None:
    # Preserve source rows if any raw evidence or run history references them.
    op.execute(
        """
        DELETE FROM sources AS source
        WHERE (source.name, source.type) IN (
            ('GitHub', 'github'),
            ('Remotive Jobs', 'jobs')
        )
        AND NOT EXISTS (
            SELECT 1 FROM source_events
            WHERE source_events.source_id = source.id
        )
        AND NOT EXISTS (
            SELECT 1 FROM ingestion_runs
            WHERE ingestion_runs.source_id = source.id
        )
        """
    )
