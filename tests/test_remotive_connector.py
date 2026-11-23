import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.ingestion.connectors.remotive import (
    CURSOR_LAST_FETCHED_AT,
    CURSOR_MAX_ID_SEEN,
    CURSOR_MAX_PUBLICATION_DATE_SEEN,
    FEED_SCOPE,
    RemotiveConnector,
)


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
FEED_FIXTURE = json.loads(
    (FIXTURES_DIR / "remotive_feed.json").read_text(encoding="utf-8")
)
EMPTY_FEED_FIXTURE = json.loads(
    (FIXTURES_DIR / "remotive_empty_feed.json").read_text(encoding="utf-8")
)
FIXED_NOW = datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)


class FakeRemotiveClient:
    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error
        self.calls = []
        self.closed = False

    async def fetch_jobs(self, category):
        self.calls.append({"category": category})
        if self.error is not None:
            raise self.error
        return self.payload

    async def aclose(self):
        self.closed = True


class RemotiveConnectorTests(unittest.IsolatedAsyncioTestCase):
    def make_connector(self, client, now=FIXED_NOW):
        return RemotiveConnector(
            client_factory=lambda: client,
            now_factory=lambda: now,
        )

    async def test_metadata_and_single_feed_scope_match_approved_decisions(self):
        client = FakeRemotiveClient(payload=EMPTY_FEED_FIXTURE)
        connector = self.make_connector(client)

        self.assertEqual(connector.name, "remotive")
        self.assertEqual(connector.rate_limit.min_run_interval_s, 21600)
        self.assertEqual(connector.rate_limit.min_request_interval_s, 0.0)
        self.assertEqual(connector.auth.required_env, ())
        self.assertEqual(await connector.scopes(), [FEED_SCOPE])

    async def test_fetch_preserves_raw_jobs_and_updates_observational_cursor(self):
        client = FakeRemotiveClient(payload=FEED_FIXTURE)
        connector = self.make_connector(client)

        result = await connector.fetch_since(FEED_SCOPE, {})

        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0], {"category": "software-dev"})
        self.assertEqual(
            [event.source_native_id for event in result.events],
            ["481234", "481235"],
        )
        self.assertIs(result.events[0].raw_payload, FEED_FIXTURE["jobs"][0])
        self.assertIs(result.events[1].raw_payload, FEED_FIXTURE["jobs"][1])
        self.assertEqual(
            result.next_cursor[CURSOR_LAST_FETCHED_AT],
            "2026-11-24T12:00:00+00:00",
        )
        self.assertEqual(result.next_cursor[CURSOR_MAX_ID_SEEN], 481235)
        self.assertEqual(
            result.next_cursor[CURSOR_MAX_PUBLICATION_DATE_SEEN],
            "2026-11-24T08:15:00+00:00",
        )
        self.assertTrue(client.closed)

    async def test_existing_high_water_values_do_not_move_backwards_or_filter_jobs(self):
        client = FakeRemotiveClient(payload=FEED_FIXTURE)
        connector = self.make_connector(client)
        cursor = {
            CURSOR_MAX_ID_SEEN: 999999,
            CURSOR_MAX_PUBLICATION_DATE_SEEN: "2026-12-01T00:00:00Z",
        }

        result = await connector.fetch_since(FEED_SCOPE, cursor)

        self.assertEqual(len(result.events), 2)
        self.assertEqual(result.next_cursor[CURSOR_MAX_ID_SEEN], 999999)
        self.assertEqual(
            result.next_cursor[CURSOR_MAX_PUBLICATION_DATE_SEEN],
            "2026-12-01T00:00:00Z",
        )
        self.assertEqual(len(client.calls), 1)

    async def test_empty_feed_is_a_valid_fetch_and_preserves_existing_watermarks(self):
        client = FakeRemotiveClient(payload=EMPTY_FEED_FIXTURE)
        connector = self.make_connector(client)
        cursor = {
            CURSOR_MAX_ID_SEEN: 50,
            CURSOR_MAX_PUBLICATION_DATE_SEEN: "2026-11-20T00:00:00",
        }

        result = await connector.fetch_since(FEED_SCOPE, cursor)

        self.assertEqual(result.events, [])
        self.assertEqual(result.next_cursor[CURSOR_MAX_ID_SEEN], 50)
        self.assertEqual(
            result.next_cursor[CURSOR_MAX_PUBLICATION_DATE_SEEN],
            "2026-11-20T00:00:00",
        )
        self.assertIn(CURSOR_LAST_FETCHED_AT, result.next_cursor)

    async def test_jobs_without_usable_native_ids_are_skipped_and_counted(self):
        payload = {
            "job-count": 4,
            "jobs": [
                {"title": "Missing ID"},
                {"id": None, "title": "Null ID"},
                {"id": "   ", "title": "Blank ID"},
                {"id": 481240, "title": "Valid job"},
            ],
        }
        client = FakeRemotiveClient(payload=payload)
        connector = self.make_connector(client)

        with self.assertLogs(
            "app.ingestion.connectors.remotive",
            level="WARNING",
        ) as captured:
            result = await connector.fetch_since(FEED_SCOPE, {})

        self.assertEqual(
            [event.source_native_id for event in result.events],
            ["481240"],
        )
        self.assertTrue(
            any("Skipped 3 Remotive job(s)" in message for message in captured.output)
        )
        self.assertEqual(result.next_cursor[CURSOR_MAX_ID_SEEN], 481240)

    async def test_invalid_feed_envelopes_fail_without_returning_a_cursor(self):
        invalid_payloads = [
            None,
            [],
            {},
            {"jobs": None, "job-count": 0},
            {"jobs": {}, "job-count": 0},
            {"jobs": [], "job-count": "0"},
        ]

        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                client = FakeRemotiveClient(payload=payload)
                connector = self.make_connector(client)

                with self.assertRaises(ValueError):
                    await connector.fetch_since(FEED_SCOPE, {})

                self.assertTrue(client.closed)

    async def test_job_count_mismatch_fails_instead_of_checkpointing_partial_feed(self):
        client = FakeRemotiveClient(
            payload={"job-count": 2, "jobs": [{"id": 1}]}
        )
        connector = self.make_connector(client)

        with self.assertRaisesRegex(ValueError, "does not match"):
            await connector.fetch_since(FEED_SCOPE, {})

        self.assertTrue(client.closed)

    async def test_non_object_job_fails_instead_of_silently_dropping_feed_data(self):
        client = FakeRemotiveClient(
            payload={"job-count": 2, "jobs": [{"id": 1}, "bad-row"]}
        )
        connector = self.make_connector(client)

        with self.assertRaisesRegex(ValueError, "only JSON objects"):
            await connector.fetch_since(FEED_SCOPE, {})

        self.assertTrue(client.closed)

    async def test_provider_error_closes_client_and_does_not_return_cursor(self):
        client = FakeRemotiveClient(error=RuntimeError("provider unavailable"))
        connector = self.make_connector(client)

        with self.assertRaisesRegex(RuntimeError, "provider unavailable"):
            await connector.fetch_since(FEED_SCOPE, {})

        self.assertTrue(client.closed)

    async def test_unknown_scope_and_invalid_cursor_are_rejected(self):
        client = FakeRemotiveClient(payload=EMPTY_FEED_FIXTURE)
        connector = self.make_connector(client)

        with self.assertRaisesRegex(ValueError, "Unknown Remotive scope"):
            await connector.fetch_since("other", {})
        with self.assertRaisesRegex(ValueError, "JSON object"):
            await connector.fetch_since(FEED_SCOPE, [])

        self.assertEqual(client.calls, [])
        self.assertFalse(client.closed)

    async def test_invalid_existing_cursor_is_rejected_before_provider_request(self):
        client = FakeRemotiveClient(payload=EMPTY_FEED_FIXTURE)
        connector = self.make_connector(client)

        with self.assertRaisesRegex(ValueError, "max_id_seen"):
            await connector.fetch_since(
                FEED_SCOPE,
                {CURSOR_MAX_ID_SEEN: "not-a-number"},
            )

        self.assertEqual(client.calls, [])
        self.assertFalse(client.closed)

    async def test_timezone_naive_clock_is_rejected_before_provider_request(self):
        client = FakeRemotiveClient(payload=EMPTY_FEED_FIXTURE)
        connector = self.make_connector(
            client,
            now=datetime(2026, 11, 24, 12, 0),
        )

        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            await connector.fetch_since(FEED_SCOPE, {})

        self.assertEqual(client.calls, [])
        self.assertFalse(client.closed)

    async def test_invalid_job_publication_date_does_not_corrupt_raw_event(self):
        payload = {
            "job-count": 1,
            "jobs": [
                {
                    "id": 481250,
                    "publication_date": "not-a-date",
                    "title": "Raw job retained",
                }
            ],
        }
        client = FakeRemotiveClient(payload=payload)
        connector = self.make_connector(client)

        with self.assertLogs(
            "app.ingestion.connectors.remotive",
            level="WARNING",
        ):
            result = await connector.fetch_since(FEED_SCOPE, {})

        self.assertEqual(len(result.events), 1)
        self.assertEqual(result.events[0].raw_payload, payload["jobs"][0])
        self.assertNotIn(
            CURSOR_MAX_PUBLICATION_DATE_SEEN,
            result.next_cursor,
        )


if __name__ == "__main__":
    unittest.main()
