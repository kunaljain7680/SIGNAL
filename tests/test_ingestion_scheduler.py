import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from app.models.ingestion_run import IngestionRun
from app.models.source import Source
from app.workers.run_once import RunOutcome
from app.workers.scheduler import (
    _get_due_connector_names,
    _record_scheduler_failure,
    run_due_sources_once,
    run_scheduler_forever,
)


NOW = datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)


class _FakeRows(list):
    pass


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _FakeStore:
    def __init__(self, sources: list[Source], runs: list[IngestionRun] | None = None):
        self.sources = sources
        self.runs = runs or []

    def factory(self):
        return _FakeSession(self)


class _FakeSession:
    def __init__(self, store: _FakeStore):
        self.store = store

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    def begin(self):
        return _FakeTransaction()

    async def scalars(self, statement):
        entity = statement.column_descriptions[0].get("entity")
        if entity is Source:
            return _FakeRows(self.store.sources)
        if entity is IngestionRun:
            statuses = [
                run.status
                for run in sorted(
                    self.store.runs,
                    key=lambda item: (item.started_at, str(item.id)),
                    reverse=True,
                )
            ]
            return _FakeRows(statuses[:2])
        raise AssertionError(f"Unexpected scalar query: {statement}")

    async def scalar(self, statement):
        entity = statement.column_descriptions[0].get("entity")
        if entity is Source:
            return self.store.sources[0] if self.store.sources else None
        if entity is IngestionRun:
            in_progress = [
                run
                for run in self.store.runs
                if run.status in {"PENDING", "FETCHING"}
            ]
            return max(
                in_progress,
                key=lambda item: (item.started_at, str(item.id)),
                default=None,
            )
        raise AssertionError(f"Unexpected scalar query: {statement}")

    def add(self, instance):
        if isinstance(instance, IngestionRun):
            self.store.runs.append(instance)
            return
        raise AssertionError(f"Unexpected ORM object: {instance}")

    async def flush(self):
        return None


def _source(
    name: str,
    source_type: str,
    schedule_interval_s: int,
    last_run_at: datetime | None = None,
    is_active: bool = True,
) -> Source:
    return Source(
        id=uuid.uuid4(),
        name=name,
        type=source_type,
        config={"schedule_interval_s": schedule_interval_s},
        cursor_state={},
        is_active=is_active,
        last_run_at=last_run_at,
        health_status="unknown",
    )


class IngestionSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_never_run_source_is_due(self):
        store = _FakeStore(
            [
                _source("Hacker News", "hn", 6 * 60 * 60),
                _source(
                    "Remotive",
                    "jobs",
                    24 * 60 * 60,
                    last_run_at=NOW - timedelta(hours=1),
                ),
            ]
        )

        due = await _get_due_connector_names(NOW, store.factory)

        self.assertEqual(due, ["hn_algolia"])

    async def test_source_becomes_due_at_exact_interval_boundary(self):
        source = _source(
            "Hacker News",
            "hn",
            6 * 60 * 60,
            last_run_at=NOW - timedelta(seconds=(6 * 60 * 60) - 1),
        )
        store = _FakeStore([source])

        not_due = await _get_due_connector_names(NOW, store.factory)
        self.assertEqual(not_due, [])

        source.last_run_at = NOW - timedelta(hours=6)
        due = await _get_due_connector_names(NOW, store.factory)
        self.assertEqual(due, ["hn_algolia"])

    async def test_inactive_and_unregistered_sources_are_not_scheduled(self):
        store = _FakeStore(
            [
                _source("Hacker News", "hn", 6 * 60 * 60, is_active=False),
                _source("GitHub", "github", 24 * 60 * 60),
            ]
        )

        due = await _get_due_connector_names(NOW, store.factory)

        self.assertEqual(due, [])

    async def test_due_sources_run_sequentially_and_exception_is_isolated(self):
        store = _FakeStore(
            [
                _source("Hacker News", "hn", 6 * 60 * 60),
                _source("Remotive", "jobs", 24 * 60 * 60),
            ]
        )
        calls: list[str] = []

        async def fake_run(connector_name, **kwargs):
            calls.append(connector_name)
            if connector_name == "hn_algolia":
                raise RuntimeError("unexpected runner failure")
            return RunOutcome(
                connector_name="remotive",
                source_name="Remotive",
                status="COMPLETED",
                run_id=uuid.uuid4(),
            )

        with (
            patch("app.workers.scheduler.run_source_once", side_effect=fake_run),
            patch(
                "app.workers.scheduler._record_scheduler_failure",
                new_callable=AsyncMock,
            ) as record_failure,
        ):
            await run_due_sources_once(
                session_factory=store.factory,
                now_factory=lambda: NOW,
            )

        self.assertEqual(calls, ["hn_algolia", "remotive"])
        record_failure.assert_awaited_once()
        self.assertEqual(
            record_failure.await_args.kwargs["connector_name"],
            "hn_algolia",
        )
        self.assertEqual(
            record_failure.await_args.kwargs["source_identity"],
            ("Hacker News", "hn"),
        )

    async def test_escaped_exception_is_recorded_and_health_is_updated(self):
        source = _source("Hacker News", "hn", 6 * 60 * 60)
        store = _FakeStore([source])

        await _record_scheduler_failure(
            connector_name="hn_algolia",
            source_identity=("Hacker News", "hn"),
            attempt_started_at=NOW - timedelta(seconds=1),
            error=RuntimeError("unexpected failure"),
            session_factory=store.factory,
            now_factory=lambda: NOW,
        )

        self.assertEqual(len(store.runs), 1)
        run = store.runs[0]
        self.assertEqual(run.status, "FAILED")
        self.assertEqual(run.current_step, "SCHEDULER")
        self.assertIn("unexpected failure", run.error_message)
        self.assertEqual(source.last_run_at, NOW)
        self.assertEqual(source.health_status, "degraded")

    async def test_existing_in_progress_attempt_is_finalized_without_duplicate(self):
        source = _source("Hacker News", "hn", 6 * 60 * 60)
        pending_run = IngestionRun(
            id=uuid.uuid4(),
            source_id=source.id,
            status="FETCHING",
            current_step="FETCHING",
            started_at=NOW - timedelta(seconds=1),
            retry_count=0,
        )
        store = _FakeStore([source], [pending_run])

        await _record_scheduler_failure(
            connector_name="hn_algolia",
            source_identity=("Hacker News", "hn"),
            attempt_started_at=NOW - timedelta(seconds=2),
            error=RuntimeError("escaped after run creation"),
            session_factory=store.factory,
            now_factory=lambda: NOW,
        )

        self.assertEqual(len(store.runs), 1)
        self.assertEqual(pending_run.status, "FAILED")
        self.assertEqual(pending_run.current_step, "SCHEDULER")
        self.assertEqual(pending_run.completed_at, NOW)
        self.assertEqual(source.health_status, "degraded")

    async def test_failed_attempt_waits_for_the_full_schedule_interval(self):
        source = _source("Hacker News", "hn", 6 * 60 * 60)
        store = _FakeStore([source])

        await _record_scheduler_failure(
            connector_name="hn_algolia",
            source_identity=("Hacker News", "hn"),
            attempt_started_at=NOW,
            error=RuntimeError("provider path escaped"),
            session_factory=store.factory,
            now_factory=lambda: NOW,
        )

        due = await _get_due_connector_names(
            NOW + timedelta(seconds=59),
            store.factory,
        )
        self.assertEqual(due, [])

    async def test_scheduler_catches_tick_error_and_polls_after_sixty_seconds(self):
        class StopScheduler(Exception):
            pass

        sleep_calls: list[float] = []

        async def stop_after_sleep(seconds: float):
            sleep_calls.append(seconds)
            raise StopScheduler()

        with (
            patch(
                "app.workers.scheduler.run_due_sources_once",
                new_callable=AsyncMock,
                side_effect=RuntimeError("temporary scheduler tick error"),
            ) as run_tick,
            self.assertRaises(StopScheduler),
        ):
            await run_scheduler_forever(
                session_factory=_FakeStore([]).factory,
                now_factory=lambda: NOW,
                sleep_factory=stop_after_sleep,
            )

        run_tick.assert_awaited_once()
        self.assertEqual(sleep_calls, [60])


if __name__ == "__main__":
    unittest.main()
