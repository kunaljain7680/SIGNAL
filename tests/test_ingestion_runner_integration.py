import unittest
import uuid
from unittest import skip

from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal, engine
from app.ingestion.contracts import Cursor, RawEvent
from app.services.ingestion_runner import (
    run_connectors_independently,
    run_source_ingestion,
)
from app.models.ingestion_run import IngestionRun
from app.models.source import Source
from app.models.source_event import SourceEvent


class _StaticConnector:
    def __init__(self, source_id, events, fail=False):
        self.source_id = source_id
        self.events = events
        self.fail = fail
        self.closed = False

    async def fetch_since(self, cursor: Cursor) -> list[RawEvent]:
        if self.fail:
            raise TimeoutError("simulated provider timeout")
        cursor.state = {"test": {"watermark": "advanced"}}
        return self.events

    def health_check(self):
        from app.ingestion.contracts import SourceHealth
        return SourceHealth(status="healthy")

    def rate_limit_config(self):
        from app.ingestion.contracts import RateLimitConfig
        return RateLimitConfig()

    def auth_config(self):
        from app.ingestion.contracts import AuthConfig
        return AuthConfig(required=False)

    async def aclose(self):
        self.closed = True


class TestIngestionRunnerDatabaseIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        except Exception as error:
            self.skipTest(f"PostgreSQL is not available for integration tests: {error}")

        self.source_ids = []

    async def asyncTearDown(self):
        if not self.source_ids:
            return
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    delete(SourceEvent).where(
                        SourceEvent.source_id.in_(self.source_ids)
                    )
                )
                await session.execute(
                    delete(IngestionRun).where(
                        IngestionRun.source_id.in_(self.source_ids)
                    )
                )
                await session.execute(
                    delete(Source).where(Source.id.in_(self.source_ids))
                )

    async def _create_source(self, suffix: str):
        async with AsyncSessionLocal() as session:
            source = Source(
                name=f"Phase2 integration {suffix} {uuid.uuid4()}",
                type="hn",
                cursor_state={},
            )
            session.add(source)
            await session.commit()
            await session.refresh(source)
            self.source_ids.append(source.id)
            return source.id

    async def test_double_fetch_does_not_duplicate_raw_source_events(self):
        source_id = await self._create_source("idempotency")
        events = [
            RawEvent("external-1", {"id": 1, "title": "first"}),
            RawEvent("external-2", {"id": 2, "title": "second"}),
        ]

        first = await run_source_ingestion(
            AsyncSessionLocal,
            _StaticConnector(source_id, events),
        )
        second = await run_source_ingestion(
            AsyncSessionLocal,
            _StaticConnector(source_id, events),
        )

        self.assertEqual(first.status, "COMPLETED")
        self.assertEqual(first.inserted_count, 2)
        self.assertEqual(second.status, "COMPLETED")
        self.assertEqual(second.fetched_count, 2)
        self.assertEqual(second.inserted_count, 0)

        async with AsyncSessionLocal() as session:
            event_count = await session.scalar(
                select(func.count(SourceEvent.id)).where(
                    SourceEvent.source_id == source_id
                )
            )
            runs = (
                await session.execute(
                    select(IngestionRun)
                    .where(IngestionRun.source_id == source_id)
                    .order_by(IngestionRun.started_at)
                )
            ).scalars().all()
            source = await session.get(Source, source_id)

        self.assertEqual(event_count, 2)
        self.assertEqual([run.status for run in runs], ["COMPLETED", "COMPLETED"])
        self.assertEqual(source.cursor_state, {"test": {"watermark": "advanced"}})

    async def test_one_source_failure_does_not_stop_the_next_source(self):
        failing_source_id = await self._create_source("failure")
        healthy_source_id = await self._create_source("healthy")

        outcomes = await run_connectors_independently(
            AsyncSessionLocal,
            [
                _StaticConnector(failing_source_id, [], fail=True),
                _StaticConnector(
                    healthy_source_id,
                    [RawEvent("healthy-1", {"id": "healthy-1"})],
                ),
            ],
        )

        self.assertEqual([outcome.status for outcome in outcomes], ["FAILED", "COMPLETED"])

        async with AsyncSessionLocal() as session:
            failed_run = (
                await session.execute(
                    select(IngestionRun).where(
                        IngestionRun.source_id == failing_source_id
                    )
                )
            ).scalar_one()
            healthy_count = await session.scalar(
                select(func.count(SourceEvent.id)).where(
                    SourceEvent.source_id == healthy_source_id
                )
            )

        self.assertEqual(failed_run.status, "FAILED")
        self.assertIn("TimeoutError", failed_run.error_message)
        self.assertEqual(healthy_count, 1)


if __name__ == "__main__":
    unittest.main()
