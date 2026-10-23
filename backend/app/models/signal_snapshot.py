import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class SignalSnapshot(Base):
    __tablename__ = "signal_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )

    technology_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("technologies.id"),
        nullable=False,
    )

    window_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    window_end: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    formula_version: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'v1'"),
    )

    raw_features: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    component_scores: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    momentum: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
    )

    hype_score: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
    )

    confidence: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
    )

    final_score: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )

    __table_args__ = (
        UniqueConstraint(
            "technology_id",
            "window_end",
            "formula_version",
            name="uq_signal_snapshots_technology_window_version",
        ),
    )