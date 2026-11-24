import unittest
from unittest.mock import AsyncMock, MagicMock
import uuid

from sqlalchemy.dialects import postgresql

from app.services.hn_normalization import normalize_hn_events


class TestHNNormalizationSourceIsolation(unittest.IsolatedAsyncioTestCase):
    async def test_normalization_query_only_selects_hn_source_events(self) -> None:
        mock_db = AsyncMock()

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_db.execute.return_value = mock_result

        inserted = await normalize_hn_events(
            db=mock_db,
            technology_id=uuid.uuid4(),
            source_event_ids=[uuid.uuid4()],
        )

        self.assertEqual(inserted, 0)
        mock_db.execute.assert_awaited_once()

        statement = mock_db.execute.await_args.args[0]
        compiled = statement.compile(dialect=postgresql.dialect())
        sql = str(compiled).lower()

        self.assertIn("join sources", sql)
        self.assertIn("sources.type", sql)
        self.assertIn("source_events.id in", sql)
        self.assertIn("hn", compiled.params.values())
        mock_db.commit.assert_not_awaited()

    async def test_empty_event_ids_do_not_query_database(self) -> None:
        mock_db = AsyncMock()

        inserted = await normalize_hn_events(
            db=mock_db,
            technology_id=uuid.uuid4(),
            source_event_ids=[],
        )

        self.assertEqual(inserted, 0)
        mock_db.execute.assert_not_awaited()
        mock_db.commit.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
