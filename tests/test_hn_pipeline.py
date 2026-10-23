from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

from scripts.run_hn_pipeline import run_hn_pipeline


class TestHNPipeline(unittest.IsolatedAsyncioTestCase):
    async def test_run_hn_pipeline_shares_window_and_calls_services_in_order(self) -> None:
        mock_db = AsyncMock()
        tech_id = uuid.uuid4()
        event_ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]

        mock_tech = MagicMock()
        mock_tech.id = tech_id
        mock_tech.canonical_name = "FastAPI"

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_tech
        mock_db.execute.return_value = mock_result

        fixed_end = datetime(2026, 10, 24, 12, 0, 0, tzinfo=timezone.utc)
        expected_start = fixed_end - timedelta(days=7)

        with (
            patch(
                "scripts.run_hn_pipeline.ingest_hn_search",
                new_callable=AsyncMock,
                return_value=event_ids,
            ) as mock_ingest,
            patch(
                "scripts.run_hn_pipeline.normalize_hn_events",
                new_callable=AsyncMock,
                return_value=2,
            ) as mock_normalize,
            patch(
                "scripts.run_hn_pipeline.count_recent_hn_events",
                new_callable=AsyncMock,
                return_value=3,
            ) as mock_count,
            patch(
                "scripts.run_hn_pipeline.save_hn_snapshot",
                new_callable=AsyncMock,
                return_value=True,
            ) as mock_snapshot,
        ):
            summary = await run_hn_pipeline(
                db=mock_db,
                canonical_name="FastAPI",
                window_end=fixed_end,
                hits_per_page=50,
            )

        mock_ingest.assert_awaited_once_with(
            db=mock_db,
            query="FastAPI",
            start_timestamp=int(expected_start.timestamp()),
            end_timestamp=int(fixed_end.timestamp()),
            hits_per_page=50,
        )
        mock_normalize.assert_awaited_once_with(
            db=mock_db,
            technology_id=tech_id,
            source_event_ids=event_ids,
        )
        mock_count.assert_awaited_once_with(
            db=mock_db,
            technology_id=tech_id,
            window_start=expected_start,
            window_end=fixed_end,
        )
        mock_snapshot.assert_awaited_once_with(
            db=mock_db,
            technology_id=tech_id,
            window_start=expected_start,
            window_end=fixed_end,
            event_count=3,
            score=50,
        )

        self.assertEqual(summary["technology_id"], tech_id)
        self.assertEqual(summary["window_start"], expected_start)
        self.assertEqual(summary["window_end"], fixed_end)
        self.assertEqual(summary["source_event_count"], 3)
        self.assertEqual(summary["normalized_inserted"], 2)
        self.assertEqual(summary["hn_event_count_7d"], 3)
        self.assertEqual(summary["score"], 50)
        self.assertEqual(summary["formula_version"], "hn_v0")
        self.assertTrue(summary["snapshot_created"])

    async def test_run_hn_pipeline_raises_when_technology_missing(self) -> None:
        mock_db = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute.return_value = mock_result

        with self.assertRaises(ValueError):
            await run_hn_pipeline(db=mock_db, canonical_name="FastAPI")


if __name__ == "__main__":
    unittest.main()