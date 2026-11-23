import unittest
from datetime import datetime, timedelta, timezone

from app.ingestion.connectors.hn_algolia import (
    ALGOLIA_MAX_HITS_PER_QUERY,
    CURSOR_INITIAL_BACKFILL_COMPLETE,
    CURSOR_INITIAL_BACKFILL_COMPLETED_AT,
    CURSOR_LAST_CREATED_AT,
    HNAlgoliaConnector,
)


FIXED_NOW = datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)
FIXED_NOW_TS = int(FIXED_NOW.timestamp())


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class FakeSession:
    def __init__(self, rows):
        self._rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def execute(self, statement):
        return FakeResult(self._rows)


class FakeSessionFactory:
    def __init__(self, rows):
        self.rows = rows

    def __call__(self):
        return FakeSession(self.rows)


class FakeHNClient:
    def __init__(self, responder):
        self.responder = responder
        self.calls = []
        self.closed = False

    async def search_stories(
        self,
        query,
        start_timestamp,
        end_timestamp,
        hits_per_page=100,
    ):
        call = {
            "query": query,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "hits_per_page": hits_per_page,
        }
        self.calls.append(call)
        return self.responder(call)

    async def aclose(self):
        self.closed = True


class HNAlgoliaConnectorTests(unittest.IsolatedAsyncioTestCase):
    def make_connector(self, client, rows=None):
        connector = HNAlgoliaConnector(
            session_factory=FakeSessionFactory(
                rows or [("fastapi", "FastAPI")]
            ),
            client_factory=lambda: client,
            now_factory=lambda: FIXED_NOW,
        )
        return connector

    async def test_metadata_matches_phase_two_source_configuration(self):
        connector = self.make_connector(FakeHNClient(lambda call: {
            "hits": [],
            "nbHits": 0,
        }))

        self.assertEqual(connector.name, "hn_algolia")
        self.assertEqual(connector.rate_limit.min_run_interval_s, 21600)
        self.assertEqual(connector.rate_limit.min_request_interval_s, 1.0)
        self.assertEqual(connector.auth.required_env, ())

    async def test_scopes_use_active_technology_slugs_and_phase_one_names(self):
        connector = self.make_connector(
            FakeHNClient(lambda call: {"hits": [], "nbHits": 0}),
            rows=[
                ("fastapi", "FastAPI"),
                ("node-js", "Node.js"),
            ],
        )

        scopes = await connector.scopes()

        self.assertEqual(scopes, ["fastapi", "node-js"])
        self.assertEqual(connector._query_by_scope["fastapi"], "FastAPI")
        self.assertEqual(connector._query_by_scope["node-js"], "Node.js")

    async def test_empty_cursor_runs_six_seven_day_backfill_slices(self):
        client = FakeHNClient(lambda call: {"hits": [], "nbHits": 0})
        connector = self.make_connector(client)
        await connector.scopes()

        result = await connector.fetch_since("fastapi", {})

        self.assertEqual(len(client.calls), 6)
        self.assertEqual(
            client.calls[0]["start_timestamp"],
            FIXED_NOW_TS - 42 * 24 * 60 * 60,
        )
        self.assertEqual(client.calls[0]["end_timestamp"] - client.calls[0]["start_timestamp"], 7 * 24 * 60 * 60)
        self.assertEqual(client.calls[-1]["end_timestamp"], FIXED_NOW_TS)
        self.assertTrue(result.next_cursor[CURSOR_INITIAL_BACKFILL_COMPLETE])
        self.assertEqual(
            result.next_cursor[CURSOR_INITIAL_BACKFILL_COMPLETED_AT],
            FIXED_NOW_TS,
        )
        self.assertNotIn(CURSOR_LAST_CREATED_AT, result.next_cursor)
        self.assertTrue(client.closed)

    async def test_initial_backfill_preserves_algolia_hit_and_native_id(self):
        hit = {
            "objectID": "hn-101",
            "created_at_i": FIXED_NOW_TS - 100,
            "created_at": "2026-11-24T11:58:20Z",
            "title": "FastAPI release discussion",
            "url": "https://example.com/story",
            "points": 12,
        }

        def responder(call):
            if call["start_timestamp"] == FIXED_NOW_TS - 42 * 24 * 60 * 60:
                return {"hits": [hit], "nbHits": 1}
            return {"hits": [], "nbHits": 0}

        client = FakeHNClient(responder)
        connector = self.make_connector(client)
        await connector.scopes()

        result = await connector.fetch_since("fastapi", {})

        self.assertEqual(len(result.events), 1)
        self.assertEqual(result.events[0].source_native_id, "hn-101")
        self.assertIs(result.events[0].raw_payload, hit)
        self.assertEqual(
            result.next_cursor[CURSOR_LAST_CREATED_AT],
            hit["created_at_i"],
        )
        self.assertTrue(client.closed)

    async def test_incremental_fetch_uses_one_hour_overlap_and_existing_query_term(self):
        watermark = FIXED_NOW_TS - 600
        hit = {
            "objectID": "hn-202",
            "created_at_i": FIXED_NOW_TS - 100,
            "created_at": "2026-11-24T11:58:20Z",
            "title": "FastAPI update",
        }
        client = FakeHNClient(lambda call: {"hits": [hit], "nbHits": 1})
        connector = self.make_connector(client)
        await connector.scopes()

        result = await connector.fetch_since(
            "fastapi",
            {
                CURSOR_INITIAL_BACKFILL_COMPLETE: True,
                CURSOR_INITIAL_BACKFILL_COMPLETED_AT: FIXED_NOW_TS - 42 * 86400,
                CURSOR_LAST_CREATED_AT: watermark,
            },
        )

        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["query"], "FastAPI")
        self.assertEqual(
            client.calls[0]["start_timestamp"],
            watermark - 60 * 60,
        )
        self.assertEqual(client.calls[0]["end_timestamp"], FIXED_NOW_TS)
        self.assertEqual(
            client.calls[0]["hits_per_page"],
            ALGOLIA_MAX_HITS_PER_QUERY,
        )
        self.assertEqual(result.events[0].source_native_id, "hn-202")
        self.assertEqual(
            result.next_cursor[CURSOR_LAST_CREATED_AT],
            hit["created_at_i"],
        )
        self.assertTrue(client.closed)

    async def test_incomplete_algolia_slice_is_split_until_all_hits_are_returned(self):
        calls = 0
        first_hit = {
            "objectID": "hn-left",
            "created_at_i": FIXED_NOW_TS - 7000,
            "title": "Left result",
        }
        second_hit = {
            "objectID": "hn-right",
            "created_at_i": FIXED_NOW_TS - 1000,
            "title": "Right result",
        }

        def responder(call):
            nonlocal calls
            calls += 1
            if calls == 1:
                # Simulate Algolia's 1,000-hit retrieval cap.
                return {
                    "hits": [{"objectID": str(index)} for index in range(1000)],
                    "nbHits": 1001,
                }
            if calls == 2:
                return {"hits": [first_hit], "nbHits": 1}
            return {"hits": [second_hit], "nbHits": 1}

        client = FakeHNClient(responder)
        connector = self.make_connector(client)
        await connector.scopes()

        watermark = FIXED_NOW_TS - 2 * 60 * 60
        result = await connector.fetch_since(
            "fastapi",
            {
                CURSOR_INITIAL_BACKFILL_COMPLETE: True,
                CURSOR_INITIAL_BACKFILL_COMPLETED_AT: FIXED_NOW_TS - 42 * 86400,
                CURSOR_LAST_CREATED_AT: watermark,
            },
        )

        self.assertEqual(calls, 3)
        self.assertEqual(
            [event.source_native_id for event in result.events],
            ["hn-left", "hn-right"],
        )
        self.assertEqual(
            result.next_cursor[CURSOR_LAST_CREATED_AT],
            second_hit["created_at_i"],
        )

    async def test_incomplete_one_second_window_fails_instead_of_truncating(self):
        client = FakeHNClient(lambda call: {"hits": [], "nbHits": 1})
        connector = self.make_connector(client)

        with self.assertRaisesRegex(RuntimeError, "one-second"):
            await connector._fetch_complete_window(
                client=client,
                query="FastAPI",
                start_timestamp=100,
                end_timestamp=101,
            )

    async def test_missing_native_id_is_skipped_and_does_not_advance_cursor(self):
        hit_without_id = {
            "created_at_i": FIXED_NOW_TS - 50,
            "title": "Malformed provider hit",
        }
        client = FakeHNClient(lambda call: {
            "hits": [hit_without_id],
            "nbHits": 1,
        })
        connector = self.make_connector(client)
        await connector.scopes()

        result = await connector.fetch_since("fastapi", {})

        self.assertEqual(result.events, [])
        self.assertNotIn(CURSOR_LAST_CREATED_AT, result.next_cursor)
        self.assertTrue(result.next_cursor[CURSOR_INITIAL_BACKFILL_COMPLETE])

    async def test_fetch_requires_scope_discovery_first(self):
        connector = self.make_connector(FakeHNClient(lambda call: {
            "hits": [],
            "nbHits": 0,
        }))

        with self.assertRaisesRegex(ValueError, "Call scopes"):
            await connector.fetch_since("fastapi", {})

    async def test_client_closes_when_provider_response_is_invalid(self):
        client = FakeHNClient(lambda call: {"hits": "not-a-list", "nbHits": 0})
        connector = self.make_connector(client)
        await connector.scopes()

        with self.assertRaisesRegex(ValueError, "invalid hits/nbHits"):
            await connector.fetch_since("fastapi", {})

        self.assertTrue(client.closed)


if __name__ == "__main__":
    unittest.main()
