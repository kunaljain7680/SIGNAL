from datetime import datetime, timezone
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

from sqlalchemy.dialects import postgresql

from app.core.database import get_db
from app.main import app


class TestTechnologyEvidenceEndpoint(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_db = AsyncMock()

        async def override_get_db():
            yield self.mock_db

        app.dependency_overrides[get_db] = override_get_db
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.pop(get_db, None)

    def test_get_technology_evidence_success(self) -> None:
        technology_id = uuid.uuid4()
        normalized_event_id = uuid.uuid4()
        occurred_at = datetime(2026, 11, 19, 10, 0, tzinfo=timezone.utc)
        fetched_at = datetime(2026, 11, 19, 10, 1, tzinfo=timezone.utc)

        technology_result = MagicMock()
        technology_result.scalar_one_or_none.return_value = technology_id

        normalized_event = SimpleNamespace(
            id=normalized_event_id,
            summary_text="FastAPI adds a new feature",
            source_url="https://example.com/fastapi",
            occurred_at=occurred_at,
        )
        source_event = SimpleNamespace(
            source_native_id="123456",
            fetched_at=fetched_at,
        )
        source = SimpleNamespace(name="Hacker News", type="hn")

        evidence_result = MagicMock()
        evidence_result.all.return_value = [
            (normalized_event, source_event, source)
        ]
        self.mock_db.execute.side_effect = [
            technology_result,
            evidence_result,
        ]

        response = self.client.get(
            f"/technologies/{technology_id}/evidence?limit=10&offset=0"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()), 1)
        item = response.json()[0]
        self.assertEqual(item["normalized_event_id"], str(normalized_event_id))
        self.assertEqual(item["source_name"], "Hacker News")
        self.assertEqual(item["source_type"], "hn")
        self.assertEqual(item["source_native_id"], "123456")
        self.assertEqual(item["summary_text"], "FastAPI adds a new feature")
        self.assertEqual(item["source_url"], "https://example.com/fastapi")
        self.assertEqual(item["occurred_at"], "2026-11-19T10:00:00Z")
        self.assertEqual(item["fetched_at"], "2026-11-19T10:01:00Z")
        self.assertEqual(self.mock_db.execute.await_count, 2)


    def test_get_technology_evidence_returns_all_source_types(self) -> None:
        technology_id = uuid.uuid4()
        occurred_at = datetime(2026, 11, 25, 10, 0, tzinfo=timezone.utc)
        fetched_at = datetime(2026, 11, 25, 10, 5, tzinfo=timezone.utc)

        technology_result = MagicMock()
        technology_result.scalar_one_or_none.return_value = technology_id

        source_examples = [
            (
                "Hacker News",
                "hn",
                "hn-story-1001",
                "FastAPI is discussed on Hacker News",
                "https://news.ycombinator.com/item?id=1001",
            ),
            (
                "GitHub",
                "github",
                "github-release-2001",
                "FastAPI release v1",
                "https://github.com/fastapi/fastapi/releases/tag/1",
            ),
            (
                "Remotive",
                "jobs",
                "remotive-job-3001",
                "Backend role mentions FastAPI",
                "https://remotive.com/remote-jobs/software-dev/3001",
            ),
        ]

        evidence_rows = []
        for source_name, source_type, native_id, summary, source_url in source_examples:
            normalized_event = SimpleNamespace(
                id=uuid.uuid4(),
                summary_text=summary,
                source_url=source_url,
                occurred_at=occurred_at,
            )
            source_event = SimpleNamespace(
                source_native_id=native_id,
                fetched_at=fetched_at,
            )
            source = SimpleNamespace(name=source_name, type=source_type)
            evidence_rows.append((normalized_event, source_event, source))

        evidence_result = MagicMock()
        evidence_result.all.return_value = evidence_rows
        self.mock_db.execute.side_effect = [
            technology_result,
            evidence_result,
        ]

        response = self.client.get(
            f"/technologies/{technology_id}/evidence?limit=10&offset=0"
        )

        self.assertEqual(response.status_code, 200)
        response_items = response.json()
        self.assertEqual(
            {item["source_type"] for item in response_items},
            {"hn", "github", "jobs"},
        )
        self.assertEqual(len(response_items), 3)

        items_by_source = {item["source_type"]: item for item in response_items}
        for source_name, source_type, native_id, summary, source_url in source_examples:
            item = items_by_source[source_type]
            self.assertEqual(item["source_name"], source_name)
            self.assertEqual(item["source_native_id"], native_id)
            self.assertEqual(item["summary_text"], summary)
            self.assertEqual(item["source_url"], source_url)

        self.assertEqual(self.mock_db.execute.await_count, 2)
        evidence_statement = self.mock_db.execute.await_args_list[1].args[0]
        where_clause = evidence_statement.whereclause
        compiled_where = where_clause.compile(dialect=postgresql.dialect())
        where_sql = str(compiled_where).lower()

        self.assertIn("normalized_events.technology_id", where_sql)
        self.assertNotIn("sources.type", where_sql)
        self.assertIn(technology_id, compiled_where.params.values())

    def test_get_technology_evidence_missing_technology_returns_404(self) -> None:
        technology_id = uuid.uuid4()
        technology_result = MagicMock()
        technology_result.scalar_one_or_none.return_value = None
        self.mock_db.execute.return_value = technology_result

        response = self.client.get(f"/technologies/{technology_id}/evidence")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"], "Technology not found.")
        self.assertEqual(self.mock_db.execute.await_count, 1)

    def test_get_technology_evidence_returns_empty_list_when_no_events(self) -> None:
        technology_id = uuid.uuid4()

        technology_result = MagicMock()
        technology_result.scalar_one_or_none.return_value = technology_id

        evidence_result = MagicMock()
        evidence_result.all.return_value = []
        self.mock_db.execute.side_effect = [
            technology_result,
            evidence_result,
        ]

        response = self.client.get(f"/technologies/{technology_id}/evidence")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [])
        self.assertEqual(self.mock_db.execute.await_count, 2)


if __name__ == "__main__":
    unittest.main()
