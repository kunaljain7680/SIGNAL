from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx


class GitHubQuotaLowError(RuntimeError):
    """Raised when the authenticated GitHub quota falls below the safe floor."""


class GitHubClient:
    API_BASE_URL = "https://api.github.com"
    API_VERSION = "2022-11-28"
    SEARCH_MIN_INTERVAL_SECONDS = 2.1

    def __init__(
        self,
        token: str,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not token or not token.strip():
            raise ValueError("A GitHub token is required for authenticated access.")

        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": self.API_VERSION,
        }
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            base_url=self.API_BASE_URL,
            headers=headers,
            timeout=20.0,
        )
        self._last_search_at: float | None = None
        self._search_lock = asyncio.Lock()
        self._quota_low = False
        self.last_rate_limit: dict[str, Any] = {}

    async def get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        etag: str | None = None,
    ) -> httpx.Response:
        if self._quota_low:
            raise GitHubQuotaLowError(
                "GitHub quota is below 10% of its advertised limit; "
                "the connector stopped making requests."
            )

        headers = {"If-None-Match": etag} if etag else None
        response = await self.client.get(path, params=params, headers=headers)
        self._record_rate_limit(response)

        # 304 is a successful conditional-GET outcome, not an error.
        if response.status_code != 304:
            response.raise_for_status()
        return response

    async def search_repositories(
        self,
        topic: str,
        per_page: int = 10,
    ) -> httpx.Response:
        async with self._search_lock:
            if self._last_search_at is not None:
                elapsed = time.monotonic() - self._last_search_at
                delay = self.SEARCH_MIN_INTERVAL_SECONDS - elapsed
                if delay > 0:
                    await asyncio.sleep(delay)

            response = await self.get(
                "/search/repositories",
                params={
                    "q": f"topic:{topic}",
                    "sort": "updated",
                    "order": "desc",
                    "per_page": per_page,
                    "page": 1,
                },
            )
            self._last_search_at = time.monotonic()
            return response

    def _record_rate_limit(self, response: httpx.Response) -> None:
        headers = response.headers
        try:
            limit = int(headers.get("X-RateLimit-Limit", "0"))
            remaining = int(headers.get("X-RateLimit-Remaining", "-1"))
            reset = int(headers.get("X-RateLimit-Reset", "0"))
        except ValueError:
            return

        resource = headers.get("X-RateLimit-Resource", "unknown")
        self.last_rate_limit = {
            "limit": limit,
            "remaining": remaining,
            "reset_at": reset,
            "resource": resource,
        }

        if limit > 0 and remaining >= 0 and remaining / limit < 0.10:
            self._quota_low = True

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()
