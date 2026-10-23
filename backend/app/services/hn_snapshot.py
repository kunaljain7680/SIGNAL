
import uuid
from datetime import datetime

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.signal_snapshot import SignalSnapshot


async def save_hn_snapshot(
    db: AsyncSession,
    technology_id: uuid.UUID,
    window_start: datetime,
    window_end: datetime,
    event_count: int,
    score: int,
) -> bool:
    """
    Persist a Phase 1 HN score snapshot.

    Returns True if a new snapshot was inserted,
    or False if the same snapshot already exists.
    """
    window_days = round(
        (window_end - window_start).total_seconds() / 86400,
        2,
    )

    stmt = insert(SignalSnapshot).values(
        technology_id=technology_id,
        window_start=window_start,
        window_end=window_end,
        formula_version="hn_v0",
        raw_features={
            "hn_event_count": event_count,
            "window_days": window_days,
        },
        component_scores={
            "hn_v0": score,
        },
        momentum=None,
        hype_score=None,
        confidence=None,
        final_score=float(score),
    )

    stmt = stmt.on_conflict_do_nothing(
        index_elements=[
            "technology_id",
            "window_end",
            "formula_version",
        ]
    )

    result = await db.execute(stmt)
    await db.commit()

    return result.rowcount == 1
