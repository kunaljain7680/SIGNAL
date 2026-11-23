from typing import Any

import httpx


class RemotiveAPIError(RuntimeError):
    """Raised when the Remotive public API cannot provide a valid response."""


class RemotiveClient:
    BASE_URL = "https://remotive.com"
    JOBS_PATH = "/api/remote-jobs"

    def __init__(
        self,
        http_client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
    ) -> None:
        self._client = http_client or httpx.AsyncClient(
            base_url=self.BASE_URL,
            timeout=timeout,
        )
        self._owns_client = http_client is None

    async def fetch_jobs(self, category: str = "software-dev") -> Any:
        """Fetch one complete category feed from Remotive."""
        try:
            response = await self._client.get(
                self.JOBS_PATH,
                params={"category": category},
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            raise RemotiveAPIError(
                f"Remotive API request failed (HTTP {status_code})."
            ) from exc
        except httpx.RequestError as exc:
            raise RemotiveAPIError(
                f"Remotive API request failed ({exc.__class__.__name__})."
            ) from exc

        try:
            return response.json()
        except ValueError as exc:
            raise RemotiveAPIError(
                "Remotive API returned invalid JSON."
            ) from exc

    async def aclose(self) -> None:
        """Close the HTTP client only when this wrapper created it."""
        if self._owns_client:
            await self._client.aclose()
