import httpx
from typing import Any, Optional


class HackerNewsClient:
    """
    A minimal, async client for the official Hacker News Firebase API.
    """
    BASE_URL = "https://hacker-news.firebaseio.com/v0"

    def __init__(self):
        self.client = httpx.AsyncClient(base_url=self.BASE_URL)

    async def fetch_top_story_ids(self, limit: int = 10) -> list[int]:
        """
        Fetches the current top story IDs.
        Returns a list of integer IDs, truncated to the specified limit.
        """
        response = await self.client.get("/topstories.json")
        response.raise_for_status()

        story_ids = response.json()
        return story_ids[:limit]

    async def fetch_item(self, item_id: int) -> Optional[dict[str, Any]]:
        """
        Fetches the raw JSON payload for a single Hacker News item.
        Returns None if the item is unavailable.
        """
        response = await self.client.get(f"/item/{item_id}.json")
        response.raise_for_status()

        return response.json()

    async def aclose(self):
        """
        Properly closes the underlying HTTP connections.
        """
        await self.client.aclose()