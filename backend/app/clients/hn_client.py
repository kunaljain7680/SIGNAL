import httpx
from typing import Any, Optional


class HackerNewsClient:
    """Async clients for the Hacker News Firebase and Algolia Search APIs."""

    BASE_URL = "https://hacker-news.firebaseio.com/v0"
    ALGOLIA_BASE_URL = "https://hn.algolia.com/api/v1"

    def __init__(self):
        self.client = httpx.AsyncClient(base_url=self.BASE_URL)

    async def fetch_top_story_ids(self, limit: int = 10) -> list[int]:
        """Fetch current top story IDs."""
        response = await self.client.get("/topstories.json")
        response.raise_for_status()
        story_ids = response.json()
        return story_ids[:limit]

    async def fetch_item(self, item_id: int) -> Optional[dict[str, Any]]:
        """Fetch one raw Hacker News item, or None when unavailable."""
        response = await self.client.get(f"/item/{item_id}.json")
        response.raise_for_status()
        return response.json()

    async def search_stories(
        self,
        query: str,
        start_timestamp: int,
        end_timestamp: int,
        hits_per_page: int = 100,
    ) -> dict[str, Any]:
        """
        Preserve the Phase 1 relevance-sorted search behavior.
        """
        params = {
            "query": query,
            "tags": "story",
            "numericFilters": (
                f"created_at_i>={start_timestamp},"
                f"created_at_i<{end_timestamp}"
            ),
            "hitsPerPage": hits_per_page,
        }
        response = await self.client.get(
            f"{self.ALGOLIA_BASE_URL}/search",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def search_stories_by_date(
        self,
        query: str,
        start_timestamp: int,
        end_timestamp: int,
        hits_per_page: int = 100,
        page: int = 0,
    ) -> dict[str, Any]:
        """
        Fetch one page of date-sorted stories for incremental ingestion.

        Pagination metadata is returned unchanged so the connector can fetch
        every page in the timestamp window. This is separate from the Phase 1
        search_stories method and preserves its existing behavior.
        """
        params = {
            "query": query,
            "tags": "story",
            "numericFilters": (
                f"created_at_i>={start_timestamp},"
                f"created_at_i<{end_timestamp}"
            ),
            "hitsPerPage": hits_per_page,
            "page": page,
        }
        response = await self.client.get(
            f"{self.ALGOLIA_BASE_URL}/search_by_date",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def aclose(self):
        """Close the underlying HTTP connections."""
        await self.client.aclose()
