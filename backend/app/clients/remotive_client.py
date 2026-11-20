from __future__ import annotations

from typing import Any

import httpx


class RemotiveClient:
    """Small async client for the documented public Remotive jobs endpoint."""

    API_URL = "https://remotive.com/api/remote-jobs"

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(timeout=30.0)

    async def fetch_active_jobs(self) -> dict[str, Any]:
        """
        Fetch the active remote-jobs feed once.

        The public API documents no since_id or page cursor. The connector
        applies its local publication watermark to this bounded daily feed.
        """
        response = await self.client.get(self.API_URL)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Remotive returned a non-object response.")
        return payload

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()
