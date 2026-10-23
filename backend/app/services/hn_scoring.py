
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.normalized_event import NormalizedEvent
from app.models.source import Source
from app.models.source_event import SourceEvent


def calculate_hn_v0_score(event_count: int) -> int:
    """Map a 7-day HN event count to the Phase 1 v0 score."""
    if event_count <= 0:
        return 0
    if event_count <= 2:
        return 25
    if event_count <= 5:
        return 50
    if event_count <= 9:
        return 75
    return 100


async def count_recent_hn_events(
    db: AsyncSession,
    technology_id: uuid.UUID,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> int:
    """Count this technology's HN events within the supplied time window."""
    if window_end is None:
        window_end = datetime.now(timezone.utc)

    if window_start is None:
        window_start = window_end - timedelta(days=7)

    stmt = (
        select(func.count(NormalizedEvent.id))
        .select_from(NormalizedEvent)
        .join(
            SourceEvent,
            NormalizedEvent.source_event_id == SourceEvent.id,
        )
        .join(
            Source,
            SourceEvent.source_id == Source.id,
        )
        .where(
            NormalizedEvent.technology_id == technology_id,
            NormalizedEvent.occurred_at >= window_start,
            NormalizedEvent.occurred_at < window_end,
            Source.type == "hn",
        )
    )

    result = await db.scalar(stmt)
    return int(result or 0)
