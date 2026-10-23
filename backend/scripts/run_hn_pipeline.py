import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any

# Ensure `app` is importable when running `python scripts/run_hn_pipeline.py` from `backend/`
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.models.technology import Technology
from app.services.hn_ingestion import ingest_hn_search
from app.services.hn_normalization import normalize_hn_events
from app.services.hn_scoring import calculate_hn_v0_score, count_recent_hn_events
from app.services.hn_snapshot import save_hn_snapshot


async def run_hn_pipeline(
    db: AsyncSession,
    canonical_name: str = "FastAPI",
    window_end: datetime | None = None,
    hits_per_page: int = 100,
) -> dict[str, Any]:
    """
    Executes the Phase 1 Hacker News pipeline for a single technology:
    1. Resolves the Technology row by canonical_name.
    2. Defines a single timezone-aware 7-day UTC window.
    3. Ingests raw HN search hits and normalizes them.
    4. Counts 7-day normalized HN events and calculates the hn_v0 score.
    5. Persists a SignalSnapshot row.
    """
    if window_end is None:
        window_end = datetime.now(timezone.utc)
    elif window_end.tzinfo is None:
        raise ValueError("window_end must be timezone-aware.")

    window_start = window_end - timedelta(days=7)

    result = await db.execute(
        select(Technology).where(Technology.canonical_name == canonical_name)
    )
    technology = result.scalar_one_or_none()
    if technology is None:
        raise ValueError(f"Technology '{canonical_name}' not found in the database.")

    # Extract scalar attributes before downstream services call db.commit()
    technology_id = technology.id
    technology_name = technology.canonical_name

    source_event_ids = await ingest_hn_search(
        db=db,
        query=technology_name,
        start_timestamp=int(window_start.timestamp()),
        end_timestamp=int(window_end.timestamp()),
        hits_per_page=hits_per_page,
    )

    normalized_inserted = await normalize_hn_events(
        db=db,
        technology_id=technology_id,
        source_event_ids=source_event_ids,
    )

    event_count = await count_recent_hn_events(
        db=db,
        technology_id=technology_id,
        window_start=window_start,
        window_end=window_end,
    )

    score = calculate_hn_v0_score(event_count)

    snapshot_created = await save_hn_snapshot(
        db=db,
        technology_id=technology_id,
        window_start=window_start,
        window_end=window_end,
        event_count=event_count,
        score=score,
    )

    return {
        "technology_id": technology_id,
        "canonical_name": technology_name,
        "window_start": window_start,
        "window_end": window_end,
        "source_event_count": len(source_event_ids),
        "normalized_inserted": normalized_inserted,
        "hn_event_count_7d": event_count,
        "score": score,
        "formula_version": "hn_v0",
        "snapshot_created": snapshot_created,
    }


async def main() -> None:
    async with AsyncSessionLocal() as db:
        summary = await run_hn_pipeline(db=db, canonical_name="FastAPI")

    print(
        f"Completed HN pipeline for {summary['canonical_name']} "
        f"({summary['technology_id']}):\n"
        f"  Window:              {summary['window_start'].isoformat()} -> {summary['window_end'].isoformat()}\n"
        f"  Raw HN hits matched: {summary['source_event_count']}\n"
        f"  Normalized inserted: {summary['normalized_inserted']}\n"
        f"  7d HN event count:   {summary['hn_event_count_7d']}\n"
        f"  hn_v0 score:         {summary['score']}\n"
        f"  Snapshot inserted:   {summary['snapshot_created']}"
    )


if __name__ == "__main__":
    asyncio.run(main())