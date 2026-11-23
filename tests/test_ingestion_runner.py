import unittest
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock

from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.dml import Insert as PostgreSQLInsert
from sqlalchemy.sql.dml import Update

from app.ingestion.contracts import AuthConfig, FetchResult, RateLimitConfig, RawEvent
from app.models.ingestion_run import IngestionRun
from app.models.source import Source
from app.models.source_event import SourceEvent
from app.workers.run_once import run_source_once


NOW = datetime(2026, 11, 24, 1, 0, tzinfo=timezone.utc)


class _FakeResult:
    def __init__(self, rowcount: int = 0) -> None:
        self.rowcount = rowcount


class _FakeScalarRows:
    def __init__(self, values: list[Any]) -> None:
        self._values = values

    def __iter__(self):
        return iter(self._values)


class _FakeStore:
    def __init__(
        self,
        source_name: str = "Hacker News",
        source_type: str = "hn",
        cursor_state: dict[str, Any] | None = None,
        last_run_at: datetime | None = None,
        fail_commit_at: int | None = None,
    ) -> None:
        self.source = Source(
            id=uuid.uuid4(),
            name=source_name,
            type=source_type,
            config={},
            cursor_state=cursor_state or {},
            is_active=True,
            last_run_at=last_run_at,
            health_status="unknown",
        )
        self.runs: dict[uuid.UUID, IngestionRun] = {}
        self.run_order: list[uuid.UUID] = []
        self.events: dict[tuple[uuid.UUID, str], dict[str, Any]] = {}
        self.statements: list[Any] = []
        self.commit_attempts = 0
        self.fail_commit_at = fail_commit_at

    def factory(self):
        return _FakeSession(self)

    def snapshot(self) -> dict[str, Any]:
        return {
            "cursor_state": dict(self.source.cursor_state),
            "last_run_at": self.source.last_run_at,
            "health_status": self.source.health_status,
            "events": dict(self.events),
            "runs": dict(self.runs),
            "run_order": list(self.run_order),
            "run_fields": {
                run_id: (
                    run.status,
                    run.current_step,
                    run.completed_at,
                    run.error_message,
                    run.retry_count,
                )
                for run_id, run in self.runs.items()
            },
        }

    def restore(self, snapshot: dict[str, Any]) -> None:
        self.source.cursor_state = snapshot["cursor_state"]
        self.source.last_run_at = snapshot["last_run_at"]
        self.source.health_status = snapshot["health_status"]
        self.events = snapshot["events"]
        self.runs = snapshot["runs"]
        self.run_order = snapshot["run_order"]
        for run_id, fields in snapshot["run_fields"].items():
            run = self.runs[run_id]
            (
                run.status,
                run.current_step,
                run.completed_at,
                run.error_message,
                run.retry_count,
            ) = fields


class _FakeTransaction:
    def __init__(self, session: "_FakeSession") -> None:
        self.session = session
        self.snapshot: dict[str, Any] | None = None

    async def __aenter__(self):
        self.snapshot = self.session.store.snapshot()
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        if exc_type is not None:
            self.session.store.restore(self.snapshot or {})
            return False

        store = self.session.store
        store.commit_attempts += 1
        if store.fail_commit_at == store.commit_attempts:
            store.restore(self.snapshot or {})
            raise RuntimeError("simulated database commit failure")
        return False


class _FakeSession:
    def __init__(self, store: _FakeStore) -> None:
        self.store = store

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    def begin(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    async def scalar(self, statement):
        entity = statement.column_descriptions[0].get("entity")
        if entity is Source:
            return self.store.source
        raise AssertionError(f"Unexpected scalar query: {statement}")

    async def scalars(self, statement):
        statuses = [
            self.store.runs[run_id].status
            for run_id in reversed(self.store.run_order)
        ]
        return _FakeScalarRows(statuses[:2])

    async def get(self, model, identity):
        if model is Source:
            return self.store.source if self.store.source.id == identity else None
        if model is IngestionRun:
            return self.store.runs.get(identity)
        raise AssertionError(f"Unexpected model lookup: {model}")

    def add(self, instance) -> None:
        if isinstance(instance, IngestionRun):
            self.store.runs[instance.id] = instance
            self.store.run_order.append(instance.id)
            return
        raise AssertionError(f"Unexpected ORM object: {instance}")

    async def execute(self, statement):
        self.store.statements.append(statement)
        if isinstance(statement, PostgreSQLInsert):
            self.assert_event_insert(statement)
            inserted = 0
            for values in statement._multi_values[0]:
                key = (values["source_id"], values["source_native_id"])
                if key not in self.store.events:
                    self.store.events[key] = values["raw_payload"]
                    inserted += 1
            return _FakeResult(inserted)

        if isinstance(statement, Update) and statement.table.name == Source.__tablename__:
            compiled = statement.compile(dialect=postgresql.dialect())
            scope_key = compiled.params["jsonb_build_object_1"]
            next_cursor = compiled.params["param_1"]
            self.store.source.cursor_state = {
                **self.store.source.cursor_state,
                scope_key: next_cursor,
            }
            return _FakeResult(1)

        raise AssertionError(f"Unexpected SQL statement: {statement}")

    @staticmethod
    def assert_event_insert(statement) -> None:
        sql = str(statement.compile(dialect=postgresql.dialect()))
        if "ON CONFLICT" not in sql or "DO NOTHING" not in sql:
            raise AssertionError("Source event insert must use ON CONFLICT DO NOTHING.")

    async def flush(self) -> None:
        return None


class _FakeConnector:
    name = "fake"
    rate_limit = RateLimitConfig(
        min_run_interval_s=0,
        min_request_interval_s=0.0,
    )
    auth = AuthConfig(required_env=())

    def __init__(
        self,
        results: dict[str, FetchResult | Exception],
        scopes: list[str] | None = None,
        rate_limit: RateLimitConfig | None = None,
        auth: AuthConfig | None = None,
    ) -> None:
        self.results = results
        self.scope_keys = scopes or list(results)
        if rate_limit is not None:
            self.rate_limit = rate_limit
        if auth is not None:
            self.auth = auth
        self.fetch_calls: list[str] = []

    async def scopes(self) -> list[str]:
        return list(self.scope_keys)

    async def fetch_since(self, scope_key: str, cursor: dict[str, Any]) -> FetchResult:
        self.fetch_calls.append(scope_key)
        result = self.results[scope_key]
        if isinstance(result, Exception):
            raise result
        return result


class IngestionRunnerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.store = _FakeStore()
        self.sleep = AsyncMock()

    async def run_fake(self, connector: _FakeConnector):
        return await run_source_once(
            "fake",
            session_factory=self.store.factory,
            env={},
            now_factory=lambda: NOW,
            sleep_factory=self.sleep,
            connector=connector,
            source_identity=("Hacker News", "hn"),
        )

    async def test_success_persists_events_and_merges_each_scope_cursor(self):
        connector = _FakeConnector(
            {
                "scope-a": FetchResult(
                    [RawEvent("a-1", {"id": "a-1", "raw": True})],
                    {"watermark": 1},
                ),
                "scope-b": FetchResult(
                    [RawEvent("b-1", {"id": "b-1", "raw": True})],
                    {"watermark": 2},
                ),
            }
        )

        outcome = await self.run_fake(connector)

        self.assertEqual(outcome.status, "COMPLETED")
        self.assertEqual(outcome.inserted_event_count, 2)
        self.assertEqual(set(connector.fetch_calls), {"scope-a", "scope-b"})
        self.assertEqual(
            self.store.source.cursor_state,
            {
                "scope-a": {"watermark": 1},
                "scope-b": {"watermark": 2},
            },
        )
        self.assertEqual(len(self.store.events), 2)
        run = self.store.runs[outcome.run_id]
        self.assertEqual(run.status, "COMPLETED")
        self.assertEqual(run.retry_count, 0)
        self.assertEqual(self.store.source.health_status, "healthy")

        lock_queries = [
            statement
            for statement in self.store.statements
            if getattr(statement, "_for_update_arg", None) is not None
        ]
        self.assertEqual(len(lock_queries), 2)

    async def test_duplicate_events_are_ignored_by_database_conflict_rule(self):
        repeated = FetchResult(
            [RawEvent("same-id", {"id": "same-id"})],
            {"watermark": 10},
        )
        connector = _FakeConnector({"scope-a": repeated}, scopes=["scope-a"])

        first = await self.run_fake(connector)
        second = await self.run_fake(connector)

        self.assertEqual(first.inserted_event_count, 1)
        self.assertEqual(second.inserted_event_count, 0)
        self.assertEqual(len(self.store.events), 1)
        self.assertEqual(second.status, "COMPLETED")

    async def test_failure_preserves_committed_scope_and_reports_unprocessed_scopes(self):
        connector = _FakeConnector(
            {
                "scope-a": FetchResult(
                    [RawEvent("a-1", {"id": "a-1"})],
                    {"watermark": 1},
                ),
                "scope-b": RuntimeError("provider unavailable"),
                "scope-c": FetchResult([], {"watermark": 3}),
            },
            scopes=["scope-a", "scope-b", "scope-c"],
        )

        outcome = await self.run_fake(connector)

        self.assertEqual(outcome.status, "FAILED")
        self.assertEqual(connector.fetch_calls, ["scope-a", "scope-b"])
        self.assertIn("scope-b", outcome.error_message)
        self.assertIn("scope-c", outcome.error_message)
        self.assertEqual(self.store.source.cursor_state, {"scope-a": {"watermark": 1}})
        self.assertEqual(len(self.store.events), 1)
        self.assertEqual(self.store.source.health_status, "degraded")

    async def test_preflight_failure_creates_failed_run_without_fetching(self):
        connector = _FakeConnector(
            {},
            scopes=[],
            auth=AuthConfig(required_env=("SIGNAL_TEST_TOKEN",)),
        )

        outcome = await run_source_once(
            "fake",
            session_factory=self.store.factory,
            env={"SIGNAL_TEST_TOKEN": ""},
            now_factory=lambda: NOW,
            connector=connector,
            source_identity=("Hacker News", "hn"),
        )

        self.assertEqual(outcome.status, "FAILED")
        self.assertIn("SIGNAL_TEST_TOKEN", outcome.error_message)
        self.assertNotIn("secret-value", outcome.error_message)
        self.assertEqual(connector.fetch_calls, [])
        run = self.store.runs[outcome.run_id]
        self.assertEqual(run.current_step, "PREFLIGHT")
        self.assertEqual(run.status, "FAILED")
        self.assertEqual(self.store.source.last_run_at, NOW)
        self.assertEqual(self.store.source.health_status, "degraded")

    async def test_remotive_guard_uses_last_successful_fetch_and_creates_no_run(self):
        last_fetch = NOW - timedelta(hours=2)
        self.store = _FakeStore(
            source_name="Remotive",
            source_type="jobs",
            cursor_state={
                "feed": {"last_fetched_at": last_fetch.isoformat()}
            },
            last_run_at=NOW - timedelta(hours=2),
        )
        connector = _FakeConnector(
            {},
            scopes=[],
            rate_limit=RateLimitConfig(6 * 60 * 60, 0.0),
        )

        outcome = await run_source_once(
            "remotive",
            session_factory=self.store.factory,
            env={},
            now_factory=lambda: NOW,
            connector=connector,
            source_identity=("Remotive", "jobs"),
        )

        self.assertEqual(outcome.status, "SKIPPED")
        self.assertIsNone(outcome.run_id)
        self.assertEqual(self.store.runs, {})
        self.assertEqual(
            self.store.source.last_run_at,
            NOW - timedelta(hours=2),
        )
        self.assertEqual(connector.fetch_calls, [])

    async def test_runner_counts_and_reports_invalid_native_ids_it_receives(self):
        connector = _FakeConnector(
            {
                "scope-a": FetchResult(
                    [
                        RawEvent(None, {"value": "missing"}),
                        RawEvent("   ", {"value": "blank"}),
                        RawEvent("valid", {"value": "valid"}),
                    ],
                    {"watermark": 1},
                )
            }
        )

        outcome = await self.run_fake(connector)

        self.assertEqual(outcome.status, "COMPLETED")
        self.assertEqual(outcome.skipped_native_id_count, 2)
        self.assertEqual(outcome.inserted_event_count, 1)
        self.assertIn("Skipped 2 event(s)", outcome.error_message)
        self.assertEqual(len(self.store.events), 1)

    async def test_failed_scope_transaction_rolls_back_events_and_cursor_together(self):
        self.store.fail_commit_at = 3
        connector = _FakeConnector(
            {
                "scope-a": FetchResult(
                    [RawEvent("a-1", {"id": "a-1"})],
                    {"watermark": 1},
                )
            }
        )

        outcome = await self.run_fake(connector)

        self.assertEqual(outcome.status, "FAILED")
        self.assertIn("simulated database commit failure", outcome.error_message)
        self.assertEqual(self.store.events, {})
        self.assertEqual(self.store.source.cursor_state, {})
        self.assertEqual(self.store.source.health_status, "degraded")

    async def test_two_consecutive_failed_runs_mark_source_down(self):
        previous = IngestionRun(
            id=uuid.uuid4(),
            source_id=self.store.source.id,
            status="FAILED",
            current_step="FETCHING",
            started_at=NOW - timedelta(minutes=1),
            retry_count=0,
        )
        self.store.runs[previous.id] = previous
        self.store.run_order.append(previous.id)
        connector = _FakeConnector(
            {"scope-a": RuntimeError("source failed")},
            scopes=["scope-a"],
        )

        outcome = await self.run_fake(connector)

        self.assertEqual(outcome.status, "FAILED")
        self.assertEqual(self.store.source.health_status, "down")

    async def test_request_interval_is_applied_between_scope_fetches(self):
        connector = _FakeConnector(
            {
                "scope-a": FetchResult([], {"watermark": 1}),
                "scope-b": FetchResult([], {"watermark": 2}),
            },
            rate_limit=RateLimitConfig(0, 0.75),
        )

        outcome = await self.run_fake(connector)

        self.assertEqual(outcome.status, "COMPLETED")
        self.sleep.assert_awaited_once_with(0.75)

    async def test_malformed_scope_result_does_not_advance_cursor(self):
        connector = _FakeConnector(
            {
                "scope-a": FetchResult([], {"not_json": object()})
            }
        )

        outcome = await self.run_fake(connector)

        self.assertEqual(outcome.status, "FAILED")
        self.assertIn("JSON-serializable", outcome.error_message)
        self.assertEqual(self.store.source.cursor_state, {})
        self.assertEqual(self.store.events, {})


if __name__ == "__main__":
    unittest.main()
