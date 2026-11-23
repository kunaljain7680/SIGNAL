"""Run registered SIGNAL source connectors on their configured intervals."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.database import AsyncSessionLocal
from app.ingestion.registry import CONNECTORS, CONNECTOR_SOURCE_IDENTITY
from app.models.ingestion_run import IngestionRun
from app.models.source import Source
from app.workers.run_once import (
    RunOutcome,
    health_from_recent_statuses,
    run_source_once,
)


logger = logging.getLogger(__name__)

SCHEDULER_POLL_INTERVAL_SECONDS = 60


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware.")
    return value.astimezone(timezone.utc)


def _source_is_due(
    last_run_at: datetime | None,
    schedule_interval_s: int,
    now: datetime,
) -> bool:
    if last_run_at is None:
        return True
    elapsed = (now - _as_utc(last_run_at, "Source last_run_at")).total_seconds()
    return elapsed >= schedule_interval_s


async def _get_due_connector_names(
    now: datetime,
    session_factory: async_sessionmaker[AsyncSession],
) -> list[str]:
    """Return registered active connectors whose source schedule has elapsed."""
    current_time = _as_utc(now, "Scheduler clock")
    async with session_factory() as session:
        result = await session.scalars(select(Source))
        source_rows = list(result)

    sources_by_identity = {
        (source.name, source.type): source
        for source in source_rows
    }
    due: list[str] = []

    # Preserve registry order so all provider work remains sequential and predictable.
    for connector_name in CONNECTORS:
        source_identity = CONNECTOR_SOURCE_IDENTITY.get(connector_name)
        if source_identity is None:
            logger.error(
                "Skipping connector %s because it has no database source identity.",
                connector_name,
            )
            continue

        source = sources_by_identity.get(source_identity)
        if source is None or not source.is_active:
            continue

        config: Any = source.config
        if not isinstance(config, dict):
            logger.error(
                "Skipping source %s because config is not a JSON object.",
                source.name,
            )
            continue

        interval = config.get("schedule_interval_s")
        if (
            not isinstance(interval, int)
            or isinstance(interval, bool)
            or interval <= 0
        ):
            logger.error(
                "Skipping source %s because config.schedule_interval_s "
                "must be a positive integer.",
                source.name,
            )
            continue

        try:
            if _source_is_due(source.last_run_at, interval, current_time):
                due.append(connector_name)
        except ValueError:
            logger.exception(
                "Skipping source %s because last_run_at is invalid.",
                source.name,
            )

    return due


async def _record_scheduler_failure(
    connector_name: str,
    source_identity: tuple[str, str],
    attempt_started_at: datetime,
    error: Exception,
    session_factory: async_sessionmaker[AsyncSession],
    now_factory: Callable[[], datetime],
) -> None:
    """Persist an unexpected escaped exception as a FAILED ingestion run."""
    started_at = _as_utc(attempt_started_at, "Scheduler attempt start")
    completed_at = _as_utc(now_factory(), "Scheduler completion clock")
    error_message = (
        f"Unexpected scheduler exception ({type(error).__name__}): {error}"
    )

    async with session_factory() as session:
        async with session.begin():
            source = await session.scalar(
                select(Source)
                .where(
                    Source.name == source_identity[0],
                    Source.type == source_identity[1],
                )
                .with_for_update()
            )
            if source is None:
                raise LookupError(
                    f"Cannot record scheduler failure: source "
                    f"{source_identity[0]!r} was not found."
                )

            # If run_source_once created a PENDING/FETCHING row before the
            # unexpected exception escaped, finalize that same attempt.
            in_progress = await session.scalar(
                select(IngestionRun)
                .where(
                    IngestionRun.source_id == source.id,
                    IngestionRun.status.in_(("PENDING", "FETCHING")),
                    IngestionRun.started_at >= started_at,
                )
                .order_by(
                    IngestionRun.started_at.desc(),
                    IngestionRun.id.desc(),
                )
                .limit(1)
                .with_for_update()
            )

            if in_progress is None:
                failed_run = IngestionRun(
                    id=uuid.uuid4(),
                    source_id=source.id,
                    status="FAILED",
                    current_step="FAILED",
                    started_at=started_at,
                    completed_at=completed_at,
                    error_message=error_message,
                    retry_count=0,
                )
                session.add(failed_run)
            else:
                in_progress.status = "FAILED"
                in_progress.current_step = "SCHEDULER"
                in_progress.completed_at = completed_at
                in_progress.error_message = error_message
                in_progress.retry_count = 0

            source.last_run_at = completed_at
            await session.flush()

            recent_statuses = list(
                await session.scalars(
                    select(IngestionRun.status)
                    .where(IngestionRun.source_id == source.id)
                    .order_by(
                        IngestionRun.started_at.desc(),
                        IngestionRun.id.desc(),
                    )
                    .limit(2)
                )
            )
            source.health_status = health_from_recent_statuses(recent_statuses)

    logger.error(
        "%s scheduler attempt was recorded as FAILED: %s",
        connector_name,
        error_message,
    )


async def run_due_sources_once(
    *,
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionLocal,
    now_factory: Callable[[], datetime] = _utcnow,
) -> None:
    """Run every due registered source sequentially, isolating each source failure."""
    now = _as_utc(now_factory(), "Scheduler clock")
    due_connectors = await _get_due_connector_names(now, session_factory)

    for connector_name in due_connectors:
        source_identity = CONNECTOR_SOURCE_IDENTITY[connector_name]
        attempt_started_at = _as_utc(now_factory(), "Scheduler attempt start")
        try:
            outcome: RunOutcome = await run_source_once(
                connector_name,
                session_factory=session_factory,
                now_factory=now_factory,
            )
            logger.info(
                "Scheduled source %s finished with status %s.",
                connector_name,
                outcome.status,
            )
        except Exception as exc:
            logger.exception(
                "Unexpected exception escaped scheduled source %s.",
                connector_name,
            )
            try:
                await _record_scheduler_failure(
                    connector_name=connector_name,
                    source_identity=source_identity,
                    attempt_started_at=attempt_started_at,
                    error=exc,
                    session_factory=session_factory,
                    now_factory=now_factory,
                )
            except Exception:
                logger.exception(
                    "Could not persist scheduler failure for source %s.",
                    connector_name,
                )


async def run_scheduler_forever(
    *,
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionLocal,
    now_factory: Callable[[], datetime] = _utcnow,
    sleep_factory: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Poll due sources every 60 seconds until the process is stopped."""
    while True:
        try:
            await run_due_sources_once(
                session_factory=session_factory,
                now_factory=now_factory,
            )
        except Exception:
            logger.exception("Scheduler tick failed; it will retry on the next poll.")

        await sleep_factory(SCHEDULER_POLL_INTERVAL_SECONDS)


async def _async_main() -> None:
    await run_scheduler_forever()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        logger.info("SIGNAL local scheduler stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
