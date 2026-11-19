"""add technology aliases

Revision ID: c7d91a4e2f60
Revises: b43a19f72c6e
Create Date: 2026-11-20

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c7d91a4e2f60"
down_revision: Union[str, None] = "b43a19f72c6e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.create_table(
        "technology_aliases",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("technology_id", sa.UUID(), nullable=False),
        sa.Column("alias_text", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["technology_id"],
            ["technologies.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("alias_text", name="uq_technology_alias_text"),
    )
    op.create_index(
        "idx_alias_trgm",
        "technology_aliases",
        ["alias_text"],
        postgresql_using="gin",
        postgresql_ops={"alias_text": "gin_trgm_ops"},
    )


def downgrade() -> None:
    op.drop_index("idx_alias_trgm", table_name="technology_aliases")
    op.drop_table("technology_aliases")
