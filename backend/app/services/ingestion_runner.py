from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable
import uuid

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.ingestion.contracts import Cursor, RawEvent, SourceConnector
from app.ingestion.github_connector import GitHubConnector
from app.ingestion.hn_connector import HackerNewsConnector
from app.ingestion.remotive_connector import RemotiveConnector
from app.models.ingestion_run import IngestionRun
from app.models.source import Source
from app.models.source_event import SourceEvent
from app.models.technology import Technology


@dataclass(frozen=True)
class IngestionOutcome:
    source_id: uuid.UUID
    source_name: str
    status: str
    fetched_count: int
    inserted_count: int
    ingestion_run_id: uuid.UUID
    error_message: str | None = None


class CursorAdvanceConflict(RuntimeError):
    """Another run advanced the same source cursor before this run committed."""


async def run_source_ingestion(
    session_factory: async_sessionmaker[AsyncSession],
    connector: SourceConnector,
) -> IngestionOutcome:
    """
    Run one source with persistent lifecycle tracking.

    The raw-event insert, cursor compare-and-swap, source health update, and
    COMPLETED transition share one transaction. A failure rolls all of them
    back, then records FAILED in a separate transaction.
    """
    run_id: uuid.UUID | None = None
    source_name = str(connector.source_id)
    source_id = connector.source_id
    original_cursor_state: dict = {}
    fetched_count = 0
    step = "INITIALIZE"

    try:
        async with session_factory() as session:
            source = await session.get(Source, source_id)
            if source is None:
                raise ValueError(f"Source {source_id} was not found.")
            if not source.is_active:
                raise ValueError(f"Source {source.name!r} is inactive.")

            source_name = source.name
            original_cursor_state = deepcopy(source.cursor_state or {})

            ingestion_run = IngestionRun(
                source_id=source_id,
                status="PENDING",
                current_step="INIT",
            )
            session.add(ingestion_run)
            await session.commit()
            run_id = ingestion_run.id

        async with session_factory() as session:
            ingestion_run = await session.get(IngestionRun, run_id)
            if ingestion_run is None:
                raise RuntimeError("The new ingestion run could not be reloaded.")
            ingestion_run.status = "FETCHING"
            ingestion_run.current_step = "FETCH"
            await session.commit()

        step = "FETCH"
        cursor = Cursor(state=deepcopy(original_cursor_state))
        events = await connector.fetch_since(cursor)
        if not isinstance(events, list):
            raise TypeError("SourceConnector.fetch_since must return a list.")
        for event in events:
            if not isinstance(event, RawEvent):
                raise TypeError("Connectors must return RawEvent objects.")
        fetched_count = len(events)

        step = "PERSIST"
        now = datetime.now(timezone.utc)

        async with session_factory() as session:
            async with session.begin():
                inserted_count = 0
                if events:
                    statement = insert(SourceEvent).values(
                        [
                            {
                                "source_id": source_id,
                                "source_native_id": event.source_native_id,
                                "raw_payload": event.raw_payload,
                            }
                            for event in events
                        ]
                    )
                    statement = statement.on_conflict_do_nothing(
                        index_elements=["source_id", "source_native_id"]
                    )
                    insert_result = await session.execute(statement)
                    inserted_count = max(insert_result.rowcount or 0, 0)

                cursor_result = await session.execute(
                    update(Source)
                    .where(
                        Source.id == source_id,
                        Source.cursor_state == original_cursor_state,
                    )
                    .values(
                        cursor_state=cursor.state,
                        last_run_at=now,
                        health_status="healthy",
                    )
                )
                if cursor_result.rowcount != 1:
                    raise CursorAdvanceConflict(
                        f"Cursor for source {source_name!r} changed during fetch; "
                        "raw events and cursor progress were rolled back."
                    )

                await session.execute(
                    update(IngestionRun)
                    .where(IngestionRun.id == run_id)
                    .values(
                        status="COMPLETED",
                        current_step="DONE",
                        completed_at=now,
                        error_message=None,
                    )
                )

        return IngestionOutcome(
            source_id=source_id,
            source_name=source_name,
            status="COMPLETED",
            fetched_count=fetched_count,
            inserted_count=inserted_count,
            ingestion_run_id=run_id,
        )

    except Exception as error:
        if run_id is None:
            raise

        error_message = f"{type(error).__name__}: {error}"[:2000]
        tracking_error = None
        try:
            now = datetime.now(timezone.utc)
            async with session_factory() as session:
                async with session.begin():
                    await session.execute(
                        update(IngestionRun)
                        .where(IngestionRun.id == run_id)
                        .values(
                            status="FAILED",
                            current_step=step,
                            completed_at=now,
                            error_message=error_message,
                        )
                    )
                    await session.execute(
                        update(Source)
                        .where(Source.id == source_id)
                        .values(
                            last_run_at=now,
                            health_status="degraded",
                        )
                    )
        except Exception as tracking_exception:
            tracking_error = (
                f" Failed to persist FAILED state: "
                f"{type(tracking_exception).__name__}: {tracking_exception}"
            )

        return IngestionOutcome(
            source_id=source_id,
            source_name=source_name,
            status="FAILED",
            fetched_count=fetched_count,
            inserted_count=0,
            ingestion_run_id=run_id,
            error_message=error_message + (tracking_error or ""),
        )

    finally:
        close = getattr(connector, "aclose", None)
        if callable(close):
            await close()


async def run_connectors_independently(
    session_factory: async_sessionmaker[AsyncSession],
    connectors: Iterable[SourceConnector],
) -> list[IngestionOutcome]:
    """Run sources sequentially; a recorded source failure does not stop later sources."""
    outcomes: list[IngestionOutcome] = []
    for connector in connectors:
        outcomes.append(
            await run_source_ingestion(session_factory, connector)
        )
    return outcomes


async def run_enabled_ingestion(
    source_type: str | None = None,
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionLocal,
) -> list[IngestionOutcome]:
    """
    Build connectors from active sources and the active tracked-technology set,
    then execute them independently.
    """
    async with session_factory() as session:
        source_result = await session.execute(
            select(Source)
            .where(Source.is_active.is_(True))
            .order_by(Source.type, Source.name)
        )
        sources = [
            (source.id, source.type, source.name)
            for source in source_result.scalars().all()
        ]

        technology_result = await session.execute(
            select(Technology)
            .where(Technology.status == "active")
            .order_by(Technology.canonical_name)
        )
        technologies = [
            (technology.canonical_name, technology.slug)
            for technology in technology_result.scalars().all()
        ]

    if source_type is not None:
        sources = [source for source in sources if source[1] == source_type]

    names = [name for name, _slug in technologies]
    slugs = [slug for _name, slug in technologies]
    github_token = (
        settings.GITHUB_TOKEN.get_secret_value()
        if settings.GITHUB_TOKEN is not None
        else None
    )

    connectors: list[SourceConnector] = []
    for source_id, kind, _name in sources:
        if kind == "hn":
            connectors.append(
                HackerNewsConnector(
                    source_id=source_id,
                    keywords=names,
                )
            )
        elif kind == "github":
            connectors.append(
                GitHubConnector(
                    source_id=source_id,
                    technology_topics=slugs,
                    token=github_token,
                )
            )
        elif kind == "jobs":
            connectors.append(
                RemotiveConnector(
                    source_id=source_id,
                    tracked_technologies=names,
                )
            )

    return await run_connectors_independently(
        session_factory=session_factory,
        connectors=connectors,
    )
