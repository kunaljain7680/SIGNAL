import unittest
from unittest.mock import AsyncMock

import httpx

from app.clients.github_client import GitHubClient, GitHubQuotaLowError


def _httpx_response(status_code: int, headers=None, json_data=None):
    request = httpx.Request("GET", "https://api.github.com/repos/org/project")
    return httpx.Response(
        status_code,
        headers=headers or {},
        json=json_data,
        request=request,
    )


class TestGitHubClient(unittest.IsolatedAsyncioTestCase):
    async def test_conditional_get_sends_if_none_match(self) -> None:
        transport = AsyncMock()
        transport.get.return_value = _httpx_response(304)
        client = GitHubClient(token="token", client=transport)

        response = await client.get(
            "/repos/org/project",
            etag='"cached-etag"',
        )

        self.assertEqual(response.status_code, 304)
        transport.get.assert_awaited_once()
        self.assertEqual(
            transport.get.await_args.kwargs["headers"],
            {"If-None-Match": '"cached-etag"'},
        )

    async def test_quota_below_ten_percent_stops_following_requests(self) -> None:
        transport = AsyncMock()
        transport.get.return_value = _httpx_response(
            200,
            headers={
                "X-RateLimit-Limit": "5000",
                "X-RateLimit-Remaining": "400",
                "X-RateLimit-Reset": "1795000000",
                "X-RateLimit-Resource": "core",
            },
            json_data={"ok": True},
        )
        client = GitHubClient(token="token", client=transport)

        await client.get("/repos/org/project")
        with self.assertRaises(GitHubQuotaLowError):
            await client.get("/repos/org/project/releases")

        self.assertEqual(transport.get.await_count, 1)


if __name__ == "__main__":
    unittest.main()
