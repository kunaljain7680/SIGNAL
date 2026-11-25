import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

from sqlalchemy.dialects import postgresql

from app.services.hn_normalization import normalize_hn_scoped_events


class TestHNScopedNormalization(unittest.IsolatedAsyncioTestCase):
    async def test_first_recorded_scope_wins_for_a_multi_scope_event(self) -> None:
        mock_db = AsyncMock()
        source_event_id = uuid.uuid4()
        first_technology_id = uuid.uuid4()
        later_technology_id = uuid.uuid4()

        source_event = MagicMock()
        source_event.id = source_event_id
        source_event.raw_payload = {
            "title": "A useful HN story",
            "created_at": "2026-11-25T12:00:00Z",
            "url": "https://example.com/story",
        }

        selection_result = MagicMock()
        # The SQL query orders rows by recorded_at, then scope_key. The same
        # event therefore appears first with the technology discovered first.
        selection_result.all.return_value = [
            (source_event, first_technology_id),
            (source_event, later_technology_id),
        ]
        insert_result = MagicMock()
        insert_result.rowcount = 1
        mock_db.execute.side_effect = [selection_result, insert_result]

        inserted = await normalize_hn_scoped_events(mock_db)

        self.assertEqual(inserted, 1)
        self.assertEqual(mock_db.execute.await_count, 2)
        mock_db.commit.assert_awaited_once()

        select_statement = mock_db.execute.await_args_list[0].args[0]
        compiled_select = select_statement.compile(dialect=postgresql.dialect())
        sql = str(compiled_select).lower()

        self.assertIn("join source_event_scopes", sql)
        self.assertIn("join technologies", sql)
        self.assertIn("join sources", sql)
        self.assertIn("left outer join normalized_events", sql)
        self.assertIn("source_event_scopes.recorded_at asc", sql)
        self.assertIn("source_event_scopes.scope_key asc", sql)
        self.assertIn("hn", compiled_select.params.values())
        self.assertIn("active", compiled_select.params.values())

        insert_statement = mock_db.execute.await_args_list[1].args[0]
        compiled_insert = insert_statement.compile(dialect=postgresql.dialect())
        self.assertIn(
            "on conflict (source_event_id) do nothing",
            str(compiled_insert).lower(),
        )

        insert_params = compiled_insert.params
        self.assertIn(source_event_id, insert_params.values())
        self.assertIn(first_technology_id, insert_params.values())
        self.assertIn("discussion_mention", insert_params.values())

    async def test_large_pending_set_is_inserted_in_bounded_batches(self) -> None:
        mock_db = AsyncMock()
        technology_id = uuid.uuid4()
        source_events = []

        for _ in range(3):
            source_event = MagicMock()
            source_event.id = uuid.uuid4()
            source_event.raw_payload = {
                "title": "A useful HN story",
                "created_at": "2026-11-25T12:00:00Z",
                "url": "https://example.com/story",
            }
            source_events.append((source_event, technology_id))

        selection_result = MagicMock()
        selection_result.all.return_value = source_events

        first_insert_result = MagicMock()
        first_insert_result.rowcount = 2
        second_insert_result = MagicMock()
        second_insert_result.rowcount = 1
        mock_db.execute.side_effect = [
            selection_result,
            first_insert_result,
            second_insert_result,
        ]

        # Use a small batch size here to test the chunking path without
        # constructing thousands of fixture events.
        with patch(
            "app.services.hn_normalization.NORMALIZATION_INSERT_BATCH_SIZE",
            2,
        ):
            inserted = await normalize_hn_scoped_events(mock_db)

        self.assertEqual(inserted, 3)
        self.assertEqual(mock_db.execute.await_count, 3)
        mock_db.commit.assert_awaited_once()

        first_insert = mock_db.execute.await_args_list[1].args[0]
        second_insert = mock_db.execute.await_args_list[2].args[0]
        first_params = first_insert.compile(dialect=postgresql.dialect()).params
        second_params = second_insert.compile(dialect=postgresql.dialect()).params
        self.assertEqual(len(first_params), 12)
        self.assertEqual(len(second_params), 6)

    async def test_no_scoped_pending_hn_events_does_not_commit(self) -> None:
        mock_db = AsyncMock()
        selection_result = MagicMock()
        selection_result.all.return_value = []
        mock_db.execute.return_value = selection_result

        inserted = await normalize_hn_scoped_events(mock_db)

        self.assertEqual(inserted, 0)
        mock_db.execute.assert_awaited_once()
        mock_db.commit.assert_not_awaited()

    async def test_first_scope_is_not_replaced_when_its_payload_is_not_normalizable(self) -> None:
        mock_db = AsyncMock()
        source_event = MagicMock()
        source_event.id = uuid.uuid4()
        source_event.raw_payload = {
            "title": "   ",
            "created_at": "2026-11-25T12:00:00Z",
        }

        selection_result = MagicMock()
        selection_result.all.return_value = [
            (source_event, uuid.uuid4()),
            (source_event, uuid.uuid4()),
        ]
        mock_db.execute.return_value = selection_result

        inserted = await normalize_hn_scoped_events(mock_db)

        self.assertEqual(inserted, 0)
        mock_db.execute.assert_awaited_once()
        mock_db.commit.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
