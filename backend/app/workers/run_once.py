"""Run one registered SIGNAL source connector and persist its raw events."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Mapping

from sqlalchemy import func, literal, select, update
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.database import AsyncSessionLocal
from app.ingestion.contracts import FetchResult, RawEvent, SourceConnector
from app.ingestion.registry import CONNECTORS, CONNECTOR_SOURCE_IDENTITY, get_connector
from app.models.ingestion_run import IngestionRun
from app.models.source import Source
from app.models.source_event import SourceEvent


logger = logging.getLogger(__name__)

REMOTIVE_CONNECTOR_NAME = "remotive"
REMOTIVE_FEED_SCOPE = "feed"


@dataclass(frozen=True)
class RunOutcome:
    """Summary returned by one attempted connector run."""

    connector_name: str
    source_name: str
    status: str
    run_id: uuid.UUID | None
    inserted_event_count: int = 0
    skipped_native_id_count: int = 0
    error_message: str | None = None


@dataclass(frozen=True)
class _SourceContext:
    id: uuid.UUID
    name: str
    cursor_state: dict[str, Any]
    last_run_at: datetime | None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_aware_datetime(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO datetime.") from exc
    else:
        raise ValueError(f"{field_name} must be an ISO datetime.")

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone.")
    return parsed.astimezone(timezone.utc)


def _minimum_interval_block(
    connector_name: str,
    interval_seconds: int,
    cursor_state: dict[str, Any],
    last_run_at: datetime | None,
    now: datetime,
) -> str | None:
    """Return a guard message without changing source or run state."""
    if interval_seconds <= 0:
        return None

    last_successful_fetch: datetime | None = None
    if connector_name == REMOTIVE_CONNECTOR_NAME:
        feed_cursor = cursor_state.get(REMOTIVE_FEED_SCOPE, {})
        if not isinstance(feed_cursor, dict):
            raise ValueError("Remotive feed cursor must be a JSON object.")
        last_fetched_at = feed_cursor.get("last_fetched_at")
        if last_fetched_at is not None:
            last_successful_fetch = _parse_aware_datetime(
                last_fetched_at,
                "Remotive feed last_fetched_at cursor",
            )
    elif last_run_at is not None:
        last_successful_fetch = _parse_aware_datetime(
            last_run_at,
            "Source last_run_at",
        )

    if last_successful_fetch is None:
        return None

    elapsed = (now - last_successful_fetch).total_seconds()
    remaining = interval_seconds - elapsed
    if remaining > 0:
        return (
            "Minimum run interval has not elapsed; "
            f"try again in approximately {int(remaining + 0.999)} seconds."
        )
    return None


async def _load_source_context(
    source_identity: tuple[str, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> _SourceContext:
    source_name, source_type = source_identity
    async with session_factory() as session:
        source = await session.scalar(
            select(Source).where(
                Source.name == source_name,
                Source.type == source_type,
            )
        )
        if source is None:
            raise LookupError(
                f"Database source row not found for {source_name!r} ({source_type!r})."
            )
        if not source.is_active:
            raise ValueError(f"Source {source_name!r} is inactive.")

        cursor_state = source.cursor_state
        if not isinstance(cursor_state, dict):
            raise ValueError(f"Source {source_name!r} cursor_state must be a JSON object.")

        return _SourceContext(
            id=source.id,
            name=source.name,
            cursor_state=dict(cursor_state),
            last_run_at=source.last_run_at,
        )


async def _create_pending_run(
    source_id: uuid.UUID,
    started_at: datetime,
    session_factory: async_sessionmaker[AsyncSession],
) -> uuid.UUID:
    run_id = uuid.uuid4()
    async with session_factory() as session:
        async with session.begin():
            source = await session.get(Source, source_id)
            if source is None:
                raise LookupError("Source disappeared before the ingestion run was created.")
            source.last_run_at = started_at
            session.add(
                IngestionRun(
                    id=run_id,
                    source_id=source_id,
                    status="PENDING",
                    current_step="PREFLIGHT",
                    started_at=started_at,
                    retry_count=0,
                )
            )
    return run_id


async def _set_fetching(
    run_id: uuid.UUID,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        async with session.begin():
            run = await session.get(IngestionRun, run_id)
            if run is None:
                raise LookupError("Ingestion run disappeared before fetching.")
            run.status = "FETCHING"
            run.current_step = "FETCHING"
            run.retry_count = 0


def _validated_events(
    result: FetchResult,
    scope_key: str,
) -> tuple[list[RawEvent], int]:
    if not isinstance(result, FetchResult):
        raise TypeError("Connector must return a FetchResult.")
    if not isinstance(result.events, list):
        raise ValueError(f"Connector events for scope {scope_key!r} must be a list.")
    if not isinstance(result.next_cursor, dict):
        raise ValueError(f"Next cursor for scope {scope_key!r} must be a JSON object.")

    try:
        json.dumps(result.next_cursor, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Next cursor for scope {scope_key!r} is not JSON-serializable."
        ) from exc

    valid_events: list[RawEvent] = []
    skipped_native_ids = 0
    for event in result.events:
        if not isinstance(event, RawEvent):
            raise TypeError(
                f"Connector returned a non-RawEvent item for scope {scope_key!r}."
            )

        native_id = event.source_native_id
        if not isinstance(native_id, str) or not native_id.strip():
            skipped_native_ids += 1
            continue
        if not isinstance(event.raw_payload, dict):
            raise ValueError(
                f"Raw payload for event {native_id!r} must be a JSON object."
            )
        try:
            json.dumps(event.raw_payload, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Raw payload for event {native_id!r} is not JSON-serializable."
            ) from exc
        valid_events.append(event)

    if skipped_native_ids:
        logger.warning(
            "Skipped %d event(s) without a usable native ID in scope %s.",
            skipped_native_ids,
            scope_key,
        )
    return valid_events, skipped_native_ids


async def _persist_scope(
    source_id: uuid.UUID,
    scope_key: str,
    events: list[RawEvent],
    next_cursor: dict[str, Any],
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    """Atomically persist one scope's events and merge only its cursor."""
    inserted_count = 0
    async with session_factory() as session:
        async with session.begin():
            source = await session.scalar(
                select(Source)
                .where(Source.id == source_id)
                .with_for_update()
            )
            if source is None:
                raise LookupError("Source disappeared before scope persistence.")

            if events:
                statement = pg_insert(SourceEvent).values(
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
                    index_elements=[
                        SourceEvent.source_id,
                        SourceEvent.source_native_id,
                    ]
                )
                insert_result = await session.execute(statement)
                inserted_count = max(0, int(insert_result.rowcount or 0))

            # Merge in PostgreSQL rather than replacing a potentially stale
            # in-memory cursor_state map read before the HTTP request.
            cursor_merge = Source.cursor_state.op("||")(
                func.jsonb_build_object(
                    scope_key,
                    literal(next_cursor, type_=JSONB),
                )
            )
            await session.execute(
                update(Source)
                .where(Source.id == source_id)
                .values(cursor_state=cursor_merge)
            )

    return inserted_count


def health_from_recent_statuses(statuses: list[str]) -> str:
    consecutive_failures = 0
    for status in statuses:
        if status != "FAILED":
            break
        consecutive_failures += 1

    if consecutive_failures >= 2:
        return "down"
    if consecutive_failures == 1:
        return "degraded"
    return "healthy"


async def _finalize_run(
    run_id: uuid.UUID,
    source_id: uuid.UUID,
    status: str,
    current_step: str,
    error_message: str | None,
    completed_at: datetime,
    session_factory: async_sessionmaker[AsyncSession],
) -> str:
    async with session_factory() as session:
        async with session.begin():
            run = await session.get(IngestionRun, run_id)
            source = await session.get(Source, source_id)
            if run is None or source is None:
                raise LookupError("Run or source disappeared during finalization.")

            run.status = status
            run.current_step = current_step
            run.completed_at = completed_at
            run.error_message = error_message
            run.retry_count = 0

            await session.flush()
            recent_statuses = list(
                await session.scalars(
                    select(IngestionRun.status)
                    .where(IngestionRun.source_id == source_id)
                    .order_by(
                        IngestionRun.started_at.desc(),
                        IngestionRun.id.desc(),
                    )
                    .limit(2)
                )
            )
            source.health_status = health_from_recent_statuses(recent_statuses)
            return source.health_status


async def run_source_once(
    connector_name: str,
    *,
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionLocal,
    env: Mapping[str, str] | None = None,
    now_factory: Callable[[], datetime] = _utcnow,
    sleep_factory: Callable[[float], Awaitable[None]] = asyncio.sleep,
    connector: SourceConnector | None = None,
    source_identity: tuple[str, str] | None = None,
) -> RunOutcome:
    """Run one connector once; successful scopes commit independently."""
    resolved_connector = connector or get_connector(connector_name)
    resolved_identity = source_identity or CONNECTOR_SOURCE_IDENTITY.get(
        connector_name
    )
    if resolved_identity is None:
        raise ValueError(
            f"No database source identity is registered for {connector_name!r}."
        )

    started_at = _parse_aware_datetime(now_factory(), "Runner clock")
    context = await _load_source_context(resolved_identity, session_factory)
    guard_message = _minimum_interval_block(
        connector_name=connector_name,
        interval_seconds=resolved_connector.rate_limit.min_run_interval_s,
        cursor_state=context.cursor_state,
        last_run_at=context.last_run_at,
        now=started_at,
    )
    if guard_message is not None:
        logger.info("Skipping %s run: %s", connector_name, guard_message)
        return RunOutcome(
            connector_name=connector_name,
            source_name=context.name,
            status="SKIPPED",
            run_id=None,
            error_message=guard_message,
        )

    run_id = await _create_pending_run(
        source_id=context.id,
        started_at=started_at,
        session_factory=session_factory,
    )

    environment = os.environ if env is None else env
    missing_environment = sorted(
        name
        for name in resolved_connector.auth.required_env
        if not environment.get(name)
    )
    if missing_environment:
        error_message = (
            "Missing required environment variables: "
            + ", ".join(missing_environment)
        )
        health = await _finalize_run(
            run_id=run_id,
            source_id=context.id,
            status="FAILED",
            current_step="PREFLIGHT",
            error_message=error_message,
            completed_at=_parse_aware_datetime(now_factory(), "Runner clock"),
            session_factory=session_factory,
        )
        logger.error("%s preflight failed: %s", connector_name, error_message)
        return RunOutcome(
            connector_name=connector_name,
            source_name=context.name,
            status="FAILED",
            run_id=run_id,
            error_message=error_message,
        )

    inserted_total = 0
    skipped_total = 0
    failure_message: str | None = None
    run_failed = False
    preflight_failed = False

    # Resolve and validate connector scopes before marking the run FETCHING.
    # This makes configuration guards (such as an unapproved GitHub inventory)
    # fail visibly at PREFLIGHT without beginning provider fetch work.
    try:
        scope_keys = await resolved_connector.scopes()
        if not isinstance(scope_keys, list):
            raise TypeError("Connector scopes() must return a list.")
        if not scope_keys:
            raise ValueError("Connector returned no scopes.")
        if any(not isinstance(key, str) or not key.strip() for key in scope_keys):
            raise ValueError("Connector scope keys must be non-empty strings.")
        if len(set(scope_keys)) != len(scope_keys):
            raise ValueError("Connector returned duplicate scope keys.")
    except Exception as exc:
        preflight_failed = True
        run_failed = True
        failure_message = (
            f"Connector preflight failed ({type(exc).__name__}): {exc}"
        )

    if not preflight_failed:
        try:
            await _set_fetching(run_id, session_factory)

            for index, scope_key in enumerate(scope_keys):
                try:
                    # Scope calls are sequential. Keep at least the configured
                    # interval between consecutive scope fetch operations.
                    if (
                        index
                        and resolved_connector.rate_limit.min_request_interval_s > 0
                    ):
                        await sleep_factory(
                            resolved_connector.rate_limit.min_request_interval_s
                        )

                    scope_cursor = context.cursor_state.get(scope_key, {})
                    if not isinstance(scope_cursor, dict):
                        raise ValueError(
                            f"Persisted cursor for scope {scope_key!r} "
                            "must be a JSON object."
                        )

                    fetch_result = await resolved_connector.fetch_since(
                        scope_key,
                        dict(scope_cursor),
                    )
                    valid_events, skipped_count = _validated_events(
                        fetch_result,
                        scope_key,
                    )
                    inserted_count = await _persist_scope(
                        source_id=context.id,
                        scope_key=scope_key,
                        events=valid_events,
                        next_cursor=fetch_result.next_cursor,
                        session_factory=session_factory,
                    )
                    inserted_total += inserted_count
                    skipped_total += skipped_count
                except Exception as exc:
                    remaining_scopes = scope_keys[index + 1 :]
                    details = [
                        f"Scope {scope_key!r} failed ({type(exc).__name__}): {exc}"
                    ]
                    if remaining_scopes:
                        details.append(
                            "Unprocessed scopes: " + ", ".join(remaining_scopes)
                        )
                    failure_message = " ".join(details)
                    run_failed = True
                    break
        except Exception as exc:
            run_failed = True
            failure_message = (
                f"Runner execution failed ({type(exc).__name__}): {exc}"
            )

    if skipped_total:
        skipped_message = (
            f"Skipped {skipped_total} event(s) without a usable native ID."
        )
        failure_message = (
            f"{failure_message} {skipped_message}"
            if failure_message
            else skipped_message
        )

    final_status = "FAILED" if run_failed else "COMPLETED"
    if preflight_failed:
        final_step = "PREFLIGHT"
    else:
        final_step = "FAILED" if final_status == "FAILED" else "COMPLETED"
    await _finalize_run(
        run_id=run_id,
        source_id=context.id,
        status=final_status,
        current_step=final_step,
        error_message=failure_message,
        completed_at=_parse_aware_datetime(now_factory(), "Runner clock"),
        session_factory=session_factory,
    )

    logger.info(
        "%s run %s finished with status %s (%d inserted, %d skipped).",
        connector_name,
        run_id,
        final_status,
        inserted_total,
        skipped_total,
    )
    return RunOutcome(
        connector_name=connector_name,
        source_name=context.name,
        status=final_status,
        run_id=run_id,
        inserted_event_count=inserted_total,
        skipped_native_id_count=skipped_total,
        error_message=failure_message,
    )


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one registered SIGNAL source connector once."
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=sorted(CONNECTORS),
        help="Registered connector name (for example: hn_algolia or remotive).",
    )
    return parser


async def _async_main(connector_name: str) -> int:
    outcome = await run_source_once(connector_name)
    print(
        f"source={outcome.source_name} connector={outcome.connector_name} "
        f"status={outcome.status} run_id={outcome.run_id} "
        f"inserted_events={outcome.inserted_event_count} "
        f"skipped_native_ids={outcome.skipped_native_id_count}"
    )
    if outcome.error_message:
        print(outcome.error_message)
    return 0 if outcome.status in {"COMPLETED", "SKIPPED"} else 1


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _build_argument_parser().parse_args(argv)
    try:
        return asyncio.run(_async_main(args.source))
    except Exception:
        logger.exception("Unable to run SIGNAL source %s.", args.source)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
