import uuid

from sqlalchemy import ForeignKey, Index, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class TechnologyAlias(Base):
    __tablename__ = "technology_aliases"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    technology_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("technologies.id", ondelete="CASCADE"),
        nullable=False,
    )
    alias_text: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("alias_text", name="uq_technology_alias_text"),
        Index(
            "idx_alias_trgm",
            "alias_text",
            postgresql_using="gin",
            postgresql_ops={"alias_text": "gin_trgm_ops"},
        ),
    )
