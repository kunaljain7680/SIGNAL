import unittest
import uuid
from unittest.mock import AsyncMock

from app.ingestion.contracts import Cursor
from app.ingestion.hn_connector import HackerNewsConnector


class TestHackerNewsConnector(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_since_paginates_and_deduplicates_keyword_overlap(self) -> None:
        source_id = uuid.uuid4()
        client = AsyncMock()
        client.search_stories_by_date.side_effect = [
            {
                "hits": [
                    {"objectID": "1", "title": "FastAPI release"},
                    {"objectID": "2", "title": "Python framework"},
                ],
                "nbPages": 2,
            },
            {
                "hits": [
                    {"objectID": "3", "title": "FastAPI update"},
                ],
                "nbPages": 2,
            },
            {
                "hits": [
                    {"objectID": "2", "title": "Python framework"},
                    {"objectID": "4", "title": "Python news"},
                ],
                "nbPages": 1,
            },
        ]
        connector = HackerNewsConnector(
            source_id=source_id,
            keywords=["FastAPI", "Python"],
            client=client,
            hits_per_page=2,
        )
        cursor = Cursor(
            state={
                "hn": {
                    "keywords": {
                        "FastAPI": {"last_search_timestamp": 1_700_000_000}
                    }
                }
            }
        )

        events = await connector.fetch_since(cursor)

        self.assertEqual(
            [event.source_native_id for event in events],
            ["1", "2", "3", "4"],
        )
        self.assertEqual(client.search_stories_by_date.await_count, 3)
        self.assertIn("Python", cursor.state["hn"]["keywords"])
        self.assertGreater(
            cursor.state["hn"]["keywords"]["Python"]["last_search_timestamp"],
            1_700_000_000,
        )

    async def test_cursor_does_not_advance_when_any_keyword_fails(self) -> None:
        client = AsyncMock()
        client.search_stories_by_date.side_effect = [
            {"hits": [], "nbPages": 0},
            TimeoutError("provider timeout"),
        ]
        original_state = {"hn": {"keywords": {}}}
        cursor = Cursor(state=original_state)
        connector = HackerNewsConnector(
            source_id=uuid.uuid4(),
            keywords=["FastAPI", "Python"],
            client=client,
        )

        with self.assertRaises(TimeoutError):
            await connector.fetch_since(cursor)

        self.assertEqual(cursor.state, original_state)

    def test_connector_metadata(self) -> None:
        connector = HackerNewsConnector(
            source_id=uuid.uuid4(),
            keywords=["FastAPI"],
            client=AsyncMock(),
        )
        self.assertFalse(connector.auth_config().required)
        self.assertGreaterEqual(
            connector.rate_limit_config().minimum_interval_seconds,
            0,
        )


if __name__ == "__main__":
    unittest.main()
