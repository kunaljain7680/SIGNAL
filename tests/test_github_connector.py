import unittest
import uuid
from unittest.mock import AsyncMock

from app.ingestion.contracts import Cursor
from app.ingestion.github_connector import GitHubConnector


class _Response:
    def __init__(self, status_code: int, data=None, headers=None) -> None:
        self.status_code = status_code
        self._data = data
        self.headers = headers or {}

    def json(self):
        return self._data


class TestGitHubConnector(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_saves_repository_and_release_with_etags(self) -> None:
        client = AsyncMock()
        client.search_repositories.return_value = _Response(
            200,
            {
                "items": [
                    {"id": 10, "full_name": "org/project"},
                ]
            },
        )
        client.get.side_effect = [
            _Response(
                200,
                {
                    "id": 10,
                    "full_name": "org/project",
                    "stargazers_count": 42,
                    "forks_count": 7,
                    "updated_at": "2026-11-20T10:00:00Z",
                },
                {"ETag": '"repo-v1"'},
            ),
            _Response(
                200,
                [{"id": 77, "tag_name": "v1.0.0"}],
                {"ETag": '"releases-v1"'},
            ),
        ]
        connector = GitHubConnector(
            source_id=uuid.uuid4(),
            technology_topics=["fastapi"],
            token="test-token",
            client=client,
        )
        cursor = Cursor()

        events = await connector.fetch_since(cursor)

        self.assertEqual(
            {event.source_native_id for event in events},
            {'repository:10:"repo-v1"', "release:77"},
        )
        self.assertEqual(
            cursor.state["github"]["repositories"]["10"]["repository_etag"],
            '"repo-v1"',
        )
        self.assertEqual(
            cursor.state["github"]["repositories"]["10"]["releases_etag"],
            '"releases-v1"',
        )
        self.assertIn(
            "last_checked_at",
            cursor.state["github"]["topics"]["fastapi"],
        )
        self.assertEqual(client.get.await_count, 2)

    async def test_304_responses_produce_no_duplicate_events(self) -> None:
        client = AsyncMock()
        client.search_repositories.return_value = _Response(
            200,
            {"items": [{"id": 10, "full_name": "org/project"}]},
        )
        client.get.side_effect = [
            _Response(304),
            _Response(304),
        ]
        connector = GitHubConnector(
            source_id=uuid.uuid4(),
            technology_topics=["fastapi"],
            token="test-token",
            client=client,
        )
        cursor = Cursor(
            state={
                "github": {
                    "repositories": {
                        "10": {
                            "repository_etag": '"repo-v1"',
                            "releases_etag": '"releases-v1"',
                        }
                    }
                }
            }
        )

        events = await connector.fetch_since(cursor)

        self.assertEqual(events, [])
        self.assertEqual(client.get.await_count, 2)

    async def test_invalid_search_response_does_not_advance_cursor(self) -> None:
        client = AsyncMock()
        client.search_repositories.return_value = _Response(
            200,
            {"unexpected": []},
        )
        cursor = Cursor(state={"github": {"topics": {}}})
        connector = GitHubConnector(
            source_id=uuid.uuid4(),
            technology_topics=["fastapi"],
            token="test-token",
            client=client,
        )

        with self.assertRaises(ValueError):
            await connector.fetch_since(cursor)

        self.assertEqual(cursor.state, {"github": {"topics": {}}})

    def test_connector_requires_authenticated_access(self) -> None:
        connector = GitHubConnector(
            source_id=uuid.uuid4(),
            technology_topics=["fastapi"],
            token="test-token",
            client=AsyncMock(),
        )
        self.assertTrue(connector.auth_config().required)
        self.assertEqual(
            connector.auth_config().environment_variable,
            "GITHUB_TOKEN",
        )


if __name__ == "__main__":
    unittest.main()
