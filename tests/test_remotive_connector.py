import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

from app.ingestion.contracts import Cursor
from app.ingestion.remotive_connector import RemotiveConnector


def _job(job_id: int, title: str, description: str, publication_date: str):
    return {
        "id": job_id,
        "title": title,
        "description": description,
        "publication_date": publication_date,
        "url": f"https://remotive.com/remote-jobs/software-dev/job-{job_id}",
    }


class TestRemotiveConnector(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_filters_tracked_technologies_and_sets_watermark(self) -> None:
        now = datetime.now(timezone.utc)
        recent = (now - timedelta(days=1)).isoformat()
        older = (now - timedelta(days=20)).isoformat()
        client = AsyncMock()
        client.fetch_active_jobs.return_value = {
            "job-count": 3,
            "jobs": [
                _job(1, "Python Backend Engineer", "FastAPI APIs", recent),
                _job(2, "Frontend Engineer", "React and TypeScript", recent),
                _job(3, "Python Developer", "Django services", older),
            ],
        }
        connector = RemotiveConnector(
            source_id=uuid.uuid4(),
            tracked_technologies=["Python", "FastAPI"],
            client=client,
        )
        cursor = Cursor()

        events = await connector.fetch_since(cursor)

        self.assertEqual([event.source_native_id for event in events], ["job:1"])
        self.assertEqual(events[0].raw_payload["id"], 1)
        self.assertIn("last_publication_date", cursor.state["remotive"])
        self.assertEqual(cursor.state["remotive"]["last_job_count"], 3)

    async def test_daily_guard_avoids_repeated_provider_requests(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        client = AsyncMock()
        connector = RemotiveConnector(
            source_id=uuid.uuid4(),
            tracked_technologies=["Python"],
            client=client,
        )
        cursor = Cursor(
            state={"remotive": {"last_fetch_at": now}}
        )

        events = await connector.fetch_since(cursor)

        self.assertEqual(events, [])
        client.fetch_active_jobs.assert_not_awaited()

    async def test_malformed_response_does_not_advance_cursor(self) -> None:
        client = AsyncMock()
        client.fetch_active_jobs.return_value = {"unexpected": []}
        original = {"remotive": {"last_publication_date": "2026-11-01T00:00:00+00:00"}}
        cursor = Cursor(state=original)
        connector = RemotiveConnector(
            source_id=uuid.uuid4(),
            tracked_technologies=["Python"],
            client=client,
        )

        with self.assertRaises(ValueError):
            await connector.fetch_since(cursor)

        self.assertEqual(cursor.state, original)

    def test_go_matching_requires_language_context(self) -> None:
        connector = RemotiveConnector(
            source_id=uuid.uuid4(),
            tracked_technologies=["Go"],
            client=AsyncMock(),
        )

        self.assertFalse(
            connector._matches_tracked_technology(
                {"title": "Where should we go next?", "description": "Join our team"}
            )
        )
        self.assertTrue(
            connector._matches_tracked_technology(
                {"title": "Golang backend engineer", "description": "Build APIs"}
            )
        )

    def test_connector_metadata(self) -> None:
        connector = RemotiveConnector(
            source_id=uuid.uuid4(),
            tracked_technologies=["Python"],
            client=AsyncMock(),
        )
        self.assertFalse(connector.auth_config().required)
        self.assertEqual(
            connector.rate_limit_config().requests_per_window,
            4,
        )


if __name__ == "__main__":
    unittest.main()
