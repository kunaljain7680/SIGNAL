"""seed hacker news source

Revision ID: 0dc3ee276aee
Revises: 330807ff30b8
Create Date: 2026-09-22 01:47:20.851230

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


revision: str = '0dc3ee276aee'
down_revision: Union[str, None] = '330807ff30b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO sources (name, type)
        VALUES ('Hacker News', 'hn')
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM sources
        WHERE name = 'Hacker News' AND type = 'hn'
        """
    )