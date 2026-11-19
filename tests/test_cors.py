from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = REPO_ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi.testclient import TestClient
from app.main import app


class TestLocalDashboardCORS(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_domain_relevance_preflight_allows_local_dashboard(self) -> None:
        response = self.client.options(
            "/technologies/domain-relevance",
            headers={
                "Origin": "http://127.0.0.1:5500",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get("access-control-allow-origin"),
            "http://127.0.0.1:5500",
        )


if __name__ == "__main__":
    unittest.main()
