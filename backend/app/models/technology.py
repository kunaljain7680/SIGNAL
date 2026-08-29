import uuid
from datetime import datetime

from sqlalchemy import Text, Integer, CheckConstraint, text
from sqlalchemy.dialects.postgresql import UUID, ARRAY, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class Technology(Base):
    __tablename__ = "technologies"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )

    canonical_name: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        unique=True,
    )

    slug: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        unique=True,
    )

    description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    domain_tags: Mapped[list[str]] = mapped_column(
        ARRAY(Text),
        nullable=False,
        server_default="{}",
    )

    learning_effort_hours_estimate: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )

    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="active",
    )

    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'candidate', 'archived')",
            name="check_technology_status",
        ),
    )