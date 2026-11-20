import unittest
from unittest.mock import AsyncMock, MagicMock

from app.clients.hn_client import HackerNewsClient


class TestHackerNewsClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = HackerNewsClient()
        self.client.client = AsyncMock()
        response = MagicMock()
        response.json.return_value = {"hits": [], "nbPages": 1}
        self.client.client.get.return_value = response

    async def test_phase_one_search_keeps_relevance_endpoint_and_filters(self) -> None:
        await self.client.search_stories(
            query="FastAPI",
            start_timestamp=100,
            end_timestamp=200,
            hits_per_page=50,
        )

        self.client.client.get.assert_awaited_once()
        url = self.client.client.get.await_args.args[0]
        params = self.client.client.get.await_args.kwargs["params"]
        self.assertEqual(url, "https://hn.algolia.com/api/v1/search")
        self.assertEqual(params["query"], "FastAPI")
        self.assertEqual(params["tags"], "story")
        self.assertEqual(params["numericFilters"], "created_at_i>=100,created_at_i<200")
        self.assertEqual(params["hitsPerPage"], 50)
        self.assertNotIn("page", params)

    async def test_phase_two_date_search_exposes_page_parameter(self) -> None:
        await self.client.search_stories_by_date(
            query="Python",
            start_timestamp=100,
            end_timestamp=200,
            hits_per_page=25,
            page=3,
        )

        self.client.client.get.assert_awaited_once()
        url = self.client.client.get.await_args.args[0]
        params = self.client.client.get.await_args.kwargs["params"]
        self.assertEqual(url, "https://hn.algolia.com/api/v1/search_by_date")
        self.assertEqual(params["page"], 3)
        self.assertEqual(params["hitsPerPage"], 25)


if __name__ == "__main__":
    unittest.main()
