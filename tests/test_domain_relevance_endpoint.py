from pathlib import Path
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import uuid

# Ensure backend/ is importable when tests run from the repository root.
REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = REPO_ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi.testclient import TestClient

from app.core.database import get_db
from app.main import app


class TestDomainRelevanceEndpoint(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_db = AsyncMock()

        async def override_get_db():
            yield self.mock_db

        app.dependency_overrides[get_db] = override_get_db
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.pop(get_db, None)

    def test_returns_domain_relevance_for_active_technologies(self) -> None:
        backend_id = uuid.uuid4()
        ai_id = uuid.uuid4()

        technologies_result = MagicMock()
        technologies_result.scalars.return_value.all.return_value = [
            SimpleNamespace(
                id=backend_id,
                canonical_name="FastAPI",
                domain_tags=["backend", "cloud"],
            ),
            SimpleNamespace(
                id=ai_id,
                canonical_name="PyTorch",
                domain_tags=["ai", "machine-learning"],
            ),
        ]
        self.mock_db.execute.return_value = technologies_result

        response = self.client.post(
            "/technologies/domain-relevance",
            json={
                "target_domains": ["backend"],
                "interests": ["cloud"],
            },
        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["technology_id"], str(backend_id))
        self.assertEqual(data[0]["canonical_name"], "FastAPI")
        self.assertEqual(data[0]["domain_tags"], ["backend", "cloud"])
        self.assertEqual(data[0]["domain_relevance"], 1.0)
        self.assertEqual(data[1]["technology_id"], str(ai_id))
        self.assertEqual(data[1]["domain_relevance"], 0.0)
        self.assertEqual(self.mock_db.execute.await_count, 1)

    def test_empty_profile_input_returns_zero_for_nonempty_tags(self) -> None:
        technology_id = uuid.uuid4()
        technologies_result = MagicMock()
        technologies_result.scalars.return_value.all.return_value = [
            SimpleNamespace(
                id=technology_id,
                canonical_name="FastAPI",
                domain_tags=["backend"],
            )
        ]
        self.mock_db.execute.return_value = technologies_result

        response = self.client.post(
            "/technologies/domain-relevance",
            json={},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()[0]["domain_relevance"], 0.0)

    def test_rejects_fields_outside_phase_one_profile_contract(self) -> None:
        response = self.client.post(
            "/technologies/domain-relevance",
            json={
                "target_domains": ["backend"],
                "interests": ["cloud"],
                "current_skills": ["Python"],
            },
        )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(self.mock_db.execute.await_count, 0)


if __name__ == "__main__":
    unittest.main()
