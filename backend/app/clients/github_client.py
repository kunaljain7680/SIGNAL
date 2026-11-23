import os
from typing import Any

import httpx


class GitHubRateLimitError(RuntimeError):
    pass


class GitHubAPIError(RuntimeError):
    pass


class GitHubClient:
    BASE_URL = "https://api.github.com"

    def __init__(
        self,
        token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
    ):
        self.token = token if token is not None else os.getenv("SIGNAL_GITHUB_PAT")
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        self._client = http_client or httpx.AsyncClient(
            base_url=self.BASE_URL, headers=headers, timeout=timeout
        )
        self._owns_client = http_client is None

    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        etag: str | None = None,
    ):
        response = await self._client.get(
            path,
            params=params,
            headers={"If-None-Match": etag} if etag else None,
        )

        # Normalize headers because real httpx headers are case-insensitive,
        # while test doubles and other adapters may provide ordinary dicts.
        headers = {key.lower(): value for key, value in response.headers.items()}
        remaining = None
        try:
            remaining = int(headers.get("x-ratelimit-remaining", ""))
        except (TypeError, ValueError):
            pass

        if response.status_code == 429 or (
            response.status_code == 403 and remaining is not None and remaining < 100
        ):
            raise GitHubRateLimitError(
                "GitHub rate limit response; stop without sleeping."
            )
        if remaining is not None and remaining < 100:
            raise GitHubRateLimitError(
                f"GitHub quota safety threshold reached ({remaining} remaining)."
            )

        if response.status_code == 304:
            return 304, headers, None
        if response.status_code >= 400:
            message = "GitHub API request failed"
            try:
                payload = response.json()
                provider_message = (
                    payload.get("message", "")
                    if isinstance(payload, dict)
                    else ""
                )
                if self.token and isinstance(provider_message, str):
                    provider_message = provider_message.replace(
                        self.token,
                        "[REDACTED]",
                    )
                if provider_message:
                    message += f": {provider_message}"
            except (ValueError, AttributeError):
                pass
            raise GitHubAPIError(f"{message} (HTTP {response.status_code})")

        try:
            payload = response.json()
        except ValueError as exc:
            raise GitHubAPIError("GitHub API returned invalid JSON.") from exc
        return response.status_code, headers, payload

    async def aclose(self):
        if self._owns_client:
            await self._client.aclose()
