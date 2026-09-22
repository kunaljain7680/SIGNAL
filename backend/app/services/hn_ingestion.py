from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.source import Source
from app.models.source_event import SourceEvent
from app.clients.hn_client import HackerNewsClient

async def ingest_top_hn_stories(db: AsyncSession, limit: int = 10) -> None:
    # Look up the Hacker News source dynamically
    result = await db.execute(
        select(Source).where(Source.name == "Hacker News", Source.type == "hn")
    )
    hn_source = result.scalar_one_or_none()

    if not hn_source:
        raise ValueError("Hacker News source not found in the database.")

    client = HackerNewsClient()
    events_to_insert = []

    try:
        story_ids = await client.fetch_top_story_ids(limit=limit)

        for story_id in story_ids:
            item_data = await client.fetch_item(story_id)

            if item_data is None:
                continue

            # Create dictionaries instead of ORM objects for the bulk insert
            events_to_insert.append({
                "source_id": hn_source.id,
                "source_native_id": str(item_data["id"]),
                "raw_payload": item_data
            })

        if events_to_insert:
            # Construct PostgreSQL-specific INSERT ... ON CONFLICT DO NOTHING statement
            stmt = insert(SourceEvent).values(events_to_insert)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['source_id', 'source_native_id']
            )

            await db.execute(stmt)
            await db.commit()

    finally:
        await client.aclose()