import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.normalized_event import NormalizedEvent
from app.models.source_event import SourceEvent


def normalize_hn_event(
    source_event: SourceEvent,
    technology_id: uuid.UUID,
) -> NormalizedEvent | None:
    """
    Transforms a raw Hacker News SourceEvent into a NormalizedEvent
    for a specific technology.
    """
    raw = source_event.raw_payload

    # Reject items without a usable title.
    title = raw.get("title")
    if not isinstance(title, str) or not title.strip():
        return None

    clean_title = title.strip()

    # Parse the HN creation timestamp into a timezone-aware datetime.
    created_at_str = raw["created_at"]
    if created_at_str.endswith("Z"):
        created_at_str = created_at_str.replace("Z", "+00:00")

    occurred_at = datetime.fromisoformat(created_at_str)

    return NormalizedEvent(
        source_event_id=source_event.id,
        technology_id=technology_id,
        event_type="discussion_mention",
        occurred_at=occurred_at,
        summary_text=clean_title,
        source_url=raw.get("url"),
    )


async def normalize_hn_events(
    db: AsyncSession,
    technology_id: uuid.UUID,
    source_event_ids: list[uuid.UUID],
) -> int:
    """
    Normalizes the specified Hacker News SourceEvents and
    safely persists the resulting NormalizedEvents.

    Returns the number of newly inserted rows.
    """
    if not source_event_ids:
        return 0

    result = await db.execute(
        select(SourceEvent).where(
            SourceEvent.id.in_(source_event_ids)
        )
    )
    source_events = result.scalars().all()

    normalized_records = []

    for event in source_events:
        normalized_event = normalize_hn_event(
            event,
            technology_id,
        )

        if normalized_event:
            normalized_records.append(
                {
                    "source_event_id": normalized_event.source_event_id,
                    "technology_id": normalized_event.technology_id,
                    "event_type": normalized_event.event_type,
                    "occurred_at": normalized_event.occurred_at,
                    "summary_text": normalized_event.summary_text,
                    "source_url": normalized_event.source_url,
                }
            )

    if not normalized_records:
        return 0

    stmt = insert(NormalizedEvent).values(normalized_records)

    stmt = stmt.on_conflict_do_nothing(
        index_elements=["source_event_id"]
    )

    result = await db.execute(stmt)
    await db.commit()

    return result.rowcount