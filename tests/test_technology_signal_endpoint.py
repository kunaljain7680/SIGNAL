from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock
import uuid

# Ensure `backend/` is on sys.path when running tests from the repository root
REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = REPO_ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi.testclient import TestClient

from app.core.database import get_db
from app.main import app


class TestTechnologySignalEndpoint(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_db = AsyncMock()

        async def override_get_db():
            yield self.mock_db

        app.dependency_overrides[get_db] = override_get_db
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.pop(get_db, None)

    def test_get_technology_signal_success(self) -> None:
        tech_id = uuid.uuid4()
        window_start = datetime(2026, 10, 17, 12, 0, 0, tzinfo=timezone.utc)
        window_end = datetime(2026, 10, 24, 12, 0, 0, tzinfo=timezone.utc)
        computed_at = datetime(2026, 10, 24, 12, 5, 0, tzinfo=timezone.utc)

        mock_tech = MagicMock()
        mock_tech.id = tech_id
        mock_tech.canonical_name = "FastAPI"

        mock_snapshot = MagicMock()
        mock_snapshot.technology_id = tech_id
        mock_snapshot.final_score = 75.0
        mock_snapshot.formula_version = "hn_v0"
        mock_snapshot.raw_features = {"hn_event_count": 7, "window_days": 7.0}
        mock_snapshot.component_scores = {"hn_v0": 75}
        mock_snapshot.window_start = window_start
        mock_snapshot.window_end = window_end
        mock_snapshot.computed_at = computed_at

        tech_query_result = MagicMock()
        tech_query_result.scalar_one_or_none.return_value = mock_tech

        snapshot_query_result = MagicMock()
        snapshot_query_result.scalar_one_or_none.return_value = mock_snapshot

        self.mock_db.execute.side_effect = [
            tech_query_result,
            snapshot_query_result,
        ]

        response = self.client.get(f"/technologies/{tech_id}/signal")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["technology_id"], str(tech_id))
        self.assertEqual(data["canonical_name"], "FastAPI")
        self.assertEqual(data["final_score"], 75.0)
        self.assertEqual(data["formula_version"], "hn_v0")
        self.assertEqual(
            data["raw_features"],
            {"hn_event_count": 7, "window_days": 7.0},
        )
        self.assertEqual(data["component_scores"], {"hn_v0": 75})
        self.assertEqual(data["window_start"], "2026-10-17T12:00:00Z")
        self.assertEqual(data["window_end"], "2026-10-24T12:00:00Z")
        self.assertEqual(data["computed_at"], "2026-10-24T12:05:00Z")
        self.assertEqual(self.mock_db.execute.await_count, 2)

    def test_get_technology_signal_missing_technology_returns_404(self) -> None:
        tech_id = uuid.uuid4()

        tech_query_result = MagicMock()
        tech_query_result.scalar_one_or_none.return_value = None
        self.mock_db.execute.return_value = tech_query_result

        response = self.client.get(f"/technologies/{tech_id}/signal")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"], "Technology not found.")
        self.assertEqual(self.mock_db.execute.await_count, 1)

    def test_get_technology_signal_missing_snapshot_returns_404(self) -> None:
        tech_id = uuid.uuid4()

        mock_tech = MagicMock()
        mock_tech.id = tech_id
        mock_tech.canonical_name = "FastAPI"

        tech_query_result = MagicMock()
        tech_query_result.scalar_one_or_none.return_value = mock_tech

        snapshot_query_result = MagicMock()
        snapshot_query_result.scalar_one_or_none.return_value = None

        self.mock_db.execute.side_effect = [
            tech_query_result,
            snapshot_query_result,
        ]

        response = self.client.get(f"/technologies/{tech_id}/signal")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(
            response.json()["detail"],
            "No signal snapshot found for this technology.",
        )
        self.assertEqual(self.mock_db.execute.await_count, 2)


if __name__ == "__main__":
    unittest.main()