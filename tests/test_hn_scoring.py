import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock
import uuid

from sqlalchemy.dialects import postgresql

from app.services.hn_scoring import calculate_hn_v0_score, count_recent_hn_events


class TestHNScoring(unittest.TestCase):
    def test_calculate_hn_v0_score_boundaries(self) -> None:
        # event_count <= 0 -> 0
        self.assertEqual(calculate_hn_v0_score(-1), 0)
        self.assertEqual(calculate_hn_v0_score(0), 0)

        # 1–2 -> 25
        self.assertEqual(calculate_hn_v0_score(1), 25)
        self.assertEqual(calculate_hn_v0_score(2), 25)

        # 3–5 -> 50
        self.assertEqual(calculate_hn_v0_score(3), 50)
        self.assertEqual(calculate_hn_v0_score(5), 50)

        # 6–9 -> 75
        self.assertEqual(calculate_hn_v0_score(6), 75)
        self.assertEqual(calculate_hn_v0_score(9), 75)

        # 10 or more -> 100
        self.assertEqual(calculate_hn_v0_score(10), 100)
        self.assertEqual(calculate_hn_v0_score(15), 100)


class TestHNScoringSourceIsolation(unittest.IsolatedAsyncioTestCase):
    async def test_recent_event_count_only_includes_hn_source_events(self) -> None:
        mock_db = AsyncMock()
        mock_db.scalar.return_value = 4

        event_count = await count_recent_hn_events(
            db=mock_db,
            technology_id=uuid.uuid4(),
            window_start=datetime(2026, 11, 17, tzinfo=timezone.utc),
            window_end=datetime(2026, 11, 24, tzinfo=timezone.utc),
        )

        self.assertEqual(event_count, 4)
        mock_db.scalar.assert_awaited_once()

        statement = mock_db.scalar.await_args.args[0]
        compiled = statement.compile(dialect=postgresql.dialect())
        sql = str(compiled).lower()

        self.assertIn("join sources", sql)
        self.assertIn("sources.type", sql)
        self.assertIn("source_events", sql)
        self.assertIn("hn", compiled.params.values())


if __name__ == "__main__":
    unittest.main()
